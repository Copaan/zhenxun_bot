from __future__ import annotations

from copy import deepcopy
import hashlib
import json
from pathlib import Path

from filelock import FileLock, Timeout

from zhenxun.utils.atomic_json import write_json_locked

from .analysis_process import run_analysis
from .configuration import bound_environment, confirmed_environment
from .errors import MigrationError
from .paths import contained_path
from .restore_phases import _read
from .tasks import PREFLIGHT_TTL, MigrationBudget, TaskStore


class MigrationApplication:
    """Shared package, preflight and confirmation contract for HTTP and CLI."""

    def __init__(self, project: Path):
        self.project = project.absolute()
        self.store = TaskStore(self.project)

    async def preview_export(self, *, offset=0, limit=100):
        return await run_analysis(
            self.project, "export_preview", options={"offset": offset, "limit": limit}
        )

    async def import_package(self, session: str, path: Path):
        return await run_analysis(
            self.project,
            "import_package",
            options={"path": str(path.absolute())},
            private={"session": session},
        )

    async def restore_offline(
        self, session: str, archive: Path, *, options: dict, private: dict
    ):
        import secrets
        import uuid

        from zhenxun.services.lifecycle.kernel import LifecycleKernel
        from zhenxun.services.lifecycle.launcher import LauncherSupervisor

        from .launcher import install_launcher_migration
        from .lease import InstanceLease
        from .maintenance_app import ManagementSnapshot
        from .snapshot import assert_offline
        from .transaction import RestoreTransaction

        # Taking the instance lease before package work prevents offline races
        # with an independent worker, another CLI or the launcher.
        with InstanceLease(self.project, role="launcher") as lease:
            assert_offline(self.project)
            if self.store.active():
                raise MigrationError("migration_operation_in_progress", status=409)
            uploaded = await self.import_package(session, archive)
            preflight = await self.preflight(
                session, upload_id=uploaded["id"], options=options, private=private
            )
            job = await self.confirm(
                session,
                preflight["id"],
                private=private,
                replacement_confirmed=True,
                submit=False,
            )
            supervisor = LauncherSupervisor(LifecycleKernel())
            supervisor.boot_id = uuid.uuid4().hex
            await supervisor.run_recovery(lambda: None)
            service = await install_launcher_migration(lease, supervisor)
            if service is None:
                raise MigrationError("migration_control_unavailable")
            management = ManagementSnapshot.parse(
                {
                    "username": "offline-migration",
                    "password": secrets.token_urlsafe(48),
                    "secret": secrets.token_urlsafe(48),
                }
            )
            try:
                transaction = RestoreTransaction(
                    service, job["id"], management, private=private, offline=True
                )
                await transaction.execute()
                return self.store.public(self.store.read("jobs", job["id"]))
            finally:
                await service.close()
                await supervisor.shutdown()

    async def export(self, session: str, options, *, password=None):
        from .submission import submit_export

        return await submit_export(self.project, session, options, password=password)

    async def preflight(
        self, session: str, *, upload_id: str, options: dict, private: dict
    ):
        if set(options) - {
            "first_deployment",
            "source_trusted",
            "database",
            "legacy_selection",
        }:
            raise MigrationError("migration_restore_options_invalid")
        if type(options.get("first_deployment")) is not bool:
            raise MigrationError("migration_restore_mode_required")
        if options.get("source_trusted") is not True:
            raise MigrationError("migration_source_trust_required")
        configuration = confirmed_environment(private.get("configuration"))
        administrator = private.get("administrator")
        if administrator is not None and (
            not isinstance(administrator, dict)
            or set(administrator) != {"username", "password"}
            or any(
                not isinstance(value, str) or not 1 <= len(value) <= 4096
                for value in administrator.values()
            )
        ):
            raise MigrationError("migration_administrator_confirmation_required")
        if not {"HOST", "PORT"} <= configuration.keys():
            raise MigrationError("migration_network_confirmation_required")
        if options.get("database") and not configuration.get("DB_URL"):
            raise MigrationError("migration_database_connection_confirmation_required")
        upload = self.store.read("uploads", upload_id, session=session)
        if upload["stage"] != "sealed" or upload["expires_at"] <= self.store.clock():
            raise MigrationError("migration_upload_not_ready", status=409)
        path = contained_path(
            self.store.path("uploads", upload_id).parent,
            "archive.zx.part",
            regular=True,
        )
        values = {
            **deepcopy(options),
            "replacement_confirmed": True,
            "archive_path": path.relative_to(self.project).as_posix(),
            "archive_sha256": upload["sha256"],
            "configuration_keys": sorted(configuration),
            "configuration_sha256": hashlib.sha256(
                json.dumps(configuration, sort_keys=True).encode()
            ).hexdigest(),
        }
        self.store._initialize()
        lock = FileLock(
            str(self.store.root / "analysis.lock"), timeout=0, thread_local=False
        )
        try:
            with lock:
                if self.store.active():
                    raise MigrationError("migration_operation_in_progress", status=409)
                record = self.store.create_preflight(
                    session,
                    upload_id=upload_id,
                    target_revision="0" * 64,
                    options=values,
                )
                if administrator:
                    from zhenxun.utils.passwords import hash_password

                    from .archive import file_hash

                    admin_path = (
                        self.store.path("preflights", record["id"]).parent
                        / "administrator.json"
                    )
                    write_json_locked(
                        admin_path,
                        {
                            "USERNAME": administrator["username"],
                            "PASSWORD": hash_password(administrator["password"]),
                        },
                    )
                    record["options"]["administrator_sha256"] = file_hash(admin_path)
                    write_json_locked(
                        self.store.path("preflights", record["id"]), record
                    )
                try:
                    result = await run_analysis(
                        self.project,
                        "preflight",
                        identity=record["id"],
                        private=private,
                        budget=MigrationBudget.start(3600),
                    )
                except BaseException:
                    record["stage"] = "failed"
                    write_json_locked(
                        self.store.path("preflights", record["id"]), record
                    )
                    raise
                record.update(
                    stage="awaiting_confirmation",
                    target_revision=result["revision"],
                    expires_at=self.store.clock() + PREFLIGHT_TTL,
                    summary=result,
                )
                write_json_locked(self.store.path("preflights", record["id"]), record)
                return {**self.store.public(record), "summary": result}
        except Timeout:
            raise MigrationError("migration_inspection_busy", status=409) from None

    def details(
        self, session: str, identity: str, *, section="files", offset=0, limit=100
    ):
        if section not in {
            "files",
            "directories",
            "dependencies",
            "dependency_issues",
            "skipped",
        }:
            raise MigrationError("migration_preflight_section_invalid")
        if offset < 0 or not 1 <= limit <= 100:
            raise MigrationError("migration_pagination_invalid")
        record = self.store.read("preflights", identity, session=session)
        plan = _read(self.store.path("preflights", identity).parent / "analysis.json")
        if section == "files":
            entries = [
                {key: item[key] for key in ("path", "action", "size")}
                for item in plan["files"]["actions"]
            ]
        elif section == "directories":
            entries = [
                {"path": item["path"], "action": "remove"}
                for item in plan["files"]["removed_directories"]
            ]
        elif section == "skipped":
            entries = plan["files"]["skipped"]
        else:
            entries = plan[section]
        return {
            "id": identity,
            "target_revision": record["target_revision"],
            "items": entries[offset : offset + limit],
            "total": len(entries),
        }

    async def confirm(
        self,
        session: str,
        identity: str,
        *,
        private: dict,
        replacement_confirmed: bool,
        submit=True,
    ):
        record = self.store.read("preflights", identity, session=session)
        if record.get("job_id"):
            job = self.store.read("jobs", record["job_id"], session=session)
            if submit:
                return await self._submit_existing(session, job, private)
            return job
        if replacement_confirmed is not True:
            raise MigrationError("migration_destructive_confirmation_required")
        if record.get("stage") != "awaiting_confirmation":
            raise MigrationError("migration_preflight_not_ready", status=409)
        if record["expires_at"] <= self.store.clock():
            raise MigrationError("migration_preflight_expired", status=410)
        bound_environment(record["options"], private.get("configuration"))
        await run_analysis(self.project, "recheck", identity=identity, private=private)
        job = self.store.confirm_preflight(
            identity, session, target_revision=record["target_revision"], reserve=False
        )
        if submit:
            return await self._submit_existing(session, job, private)
        return job

    async def _submit_existing(self, session, job, private):
        from zhenxun.utils.atomic_json import read_json_locked

        from .commit import options_digest
        from .submission import submit_restore

        receipt = read_json_locked(
            self.store.path("jobs", job["id"]).parent / "handoff.json", None
        )
        if receipt is not None:
            if receipt.get("job_id") != job["id"] or receipt.get(
                "options_sha256"
            ) != options_digest(job["options"]):
                raise MigrationError("migration_handoff_unconfirmed", status=409)
            return {**self.store.public(job), "handoff": "accepted"}
        result = await submit_restore(
            self.project, session, job["options"], private=private, identity=job["id"]
        )
        return {**result, "handoff": "accepted"}
