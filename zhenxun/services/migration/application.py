from __future__ import annotations

import asyncio
from copy import deepcopy
import hashlib
import json
from pathlib import Path

from filelock import FileLock, Timeout

from zhenxun.utils.atomic_json import write_json_locked

from .analysis_process import run_analysis
from .configuration import bound_environment, confirmed_environment
from .errors import MigrationError
from .inspection import WORKER_GENERATION
from .paths import contained_path
from .restore_phases import _read
from .tasks import PREFLIGHT_TTL, MigrationBudget, TaskStore


class MigrationApplication:
    """Shared package preparation and launcher handoff contract for the WebUI."""

    def __init__(self, project: Path):
        self.project = project.absolute()
        self.store = TaskStore(self.project)

    def _private_binding(self, private):
        from .candidate_database import connection_binding
        from .database_capabilities import CAPABILITY_POLICY_VERSION

        connections = {
            key: connection_binding(url, self.project)
            for key, url in (private.get("database") or {}).items()
            if key in {"target_url", "candidate_url"} and url
        }
        return self.store.private_fingerprint(
            {
                "private": private,
                "connections": connections,
                "policy_version": CAPABILITY_POLICY_VERSION,
                "generation": WORKER_GENERATION,
            }
        )

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

    async def export(
        self,
        session: str,
        options,
        *,
        password=None,
        task_id=None,
        connection_fingerprint=None,
    ):
        from .submission import submit_export

        return await submit_export(
            self.project,
            session,
            options,
            password=password,
            task_id=task_id,
            connection_fingerprint=connection_fingerprint,
        )

    async def preflight(
        self,
        session: str,
        *,
        upload_id: str,
        options: dict,
        private: dict,
        flow_job: str | None = None,
        budget: MigrationBudget | None = None,
    ):
        if set(options) - {
            "first_deployment",
            "source_trusted",
            "database",
            "legacy_selection",
            "request_id",
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
        if flow_job is not None:
            job = self.store.read("jobs", flow_job, session=session)
            if (
                not job.get("flow")
                or job["stage"] != "preflight"
                or options.get("request_id") != flow_job
            ):
                raise MigrationError("migration_restore_preview_changed", status=409)
            values["administrator_mode"] = "resolved"
        request_id = options.get("request_id")
        private_binding = self._private_binding(private)
        binding = self.store.private_fingerprint(
            {
                "private_binding": private_binding,
                "options": options,
                "upload_id": upload_id,
            }
        )
        self.store._initialize()
        lock = FileLock(
            str(self.store.root / "analysis.lock"), timeout=0, thread_local=False
        )
        try:
            with lock:
                if request_id and self.store.path("preflights", request_id).exists():
                    previous = self.store.read(
                        "preflights", request_id, session=session
                    )
                    if previous.get("request_binding") != binding:
                        raise MigrationError(
                            "migration_operation_id_conflict", status=409
                        )
                    if previous.get("expires_at", 0) <= self.store.clock():
                        raise MigrationError("migration_preflight_expired", status=410)
                    if previous["stage"] == "awaiting_confirmation":
                        return {
                            **self.store.public(previous),
                            "summary": previous["summary"],
                        }
                    failure = dict(previous.get("failure") or {})
                    code = failure.pop("code", "migration_operation_in_progress")
                    raise MigrationError(
                        code,
                        status=409,
                        details={**failure, "preflight_id": request_id},
                    )
                active = self.store.active()
                if active and active["id"] != flow_job:
                    raise MigrationError("migration_operation_in_progress", status=409)
                record = self.store.create_preflight(
                    session,
                    upload_id=upload_id,
                    target_revision="0" * 64,
                    options=values,
                    identity=request_id,
                )
                record["request_binding"] = binding
                record["private_binding"] = private_binding
                write_json_locked(self.store.path("preflights", record["id"]), record)
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
                            "PASSWORD": administrator["password"]
                            if flow_job
                            else hash_password(administrator["password"]),
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
                        budget=budget or MigrationBudget.start(3600),
                    )
                except BaseException as error:
                    record["stage"] = "failed"
                    from .candidate_database import public_resource

                    record["failure"] = {
                        "code": getattr(
                            error,
                            "code",
                            "migration_cancelled"
                            if isinstance(error, asyncio.CancelledError)
                            else "migration_analysis_failed",
                        )
                    }
                    if isinstance(error, MigrationError):
                        error.details["preflight_id"] = record["id"]
                        record["failure"] = error.public()
                    try:
                        record["candidate_resource"] = public_resource(
                            self.store, record["id"]
                        )
                        if isinstance(error, MigrationError):
                            error.details["candidate_resource"] = record[
                                "candidate_resource"
                            ]
                            record["failure"] = error.public()
                        write_json_locked(
                            self.store.path("preflights", record["id"]), record
                        )
                    except Exception:
                        pass
                    raise
                record.update(
                    stage="awaiting_confirmation",
                    target_revision=result["revision"],
                    expires_at=self.store.clock() + PREFLIGHT_TTL,
                    summary=result,
                )
                from .candidate_database import public_resource

                record["candidate_resource"] = public_resource(self.store, record["id"])
                write_json_locked(self.store.path("preflights", record["id"]), record)
                return {**self.store.public(record), "summary": result}
        except Timeout:
            details = {"operation": "preflight", "resource": "analysis"}
            if request_id and self.store.path("preflights", request_id).exists():
                previous = self.store.read("preflights", request_id, session=session)
                if previous.get("request_binding") != binding:
                    raise MigrationError(
                        "migration_operation_id_conflict", status=409
                    ) from None
                details.update(preflight_id=request_id, stage=previous["stage"])
            raise MigrationError(
                "migration_operation_in_progress",
                status=409,
                details=details,
            ) from None

    def details(
        self, session: str, identity: str, *, section="files", offset=0, limit=100
    ):
        if section not in {
            "files",
            "directories",
            "dependencies",
            "dependency_issues",
            "skipped",
            "conflicts",
            "status",
        }:
            raise MigrationError("migration_preflight_section_invalid")
        if offset < 0 or not 1 <= limit <= 100:
            raise MigrationError("migration_pagination_invalid")
        record = self.store.read("preflights", identity, session=session)
        if section == "status":
            from .candidate_database import public_resource

            return {
                **self.store.public(record),
                "summary": record.get("summary"),
                "candidate_resource": public_resource(self.store, identity),
            }
        if section == "conflicts":
            from zhenxun.utils.atomic_json import read_json_locked

            entries = read_json_locked(
                self.store.path("preflights", identity).parent / "conflicts.json",
                {"items": []},
            )["items"]
            return {
                "id": identity,
                "stage": record["stage"],
                "failure": record.get("failure"),
                "items": entries[offset : offset + limit],
                "total": len(entries),
            }
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
        self, session, identity, *, private, replacement_confirmed, submit=True
    ):
        lock = FileLock(
            str(self.store.root / "analysis.lock"), timeout=0, thread_local=False
        )
        try:
            with lock:
                return await self._confirm(
                    session,
                    identity,
                    private=private,
                    replacement_confirmed=replacement_confirmed,
                    submit=submit,
                )
        except Timeout:
            raise MigrationError(
                "migration_operation_in_progress", status=409
            ) from None

    async def _confirm(
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
        from .candidate_database import resolved_private

        if record.get("private_binding") != self._private_binding(private):
            raise MigrationError("migration_database_preflight_stale", status=409)
        private = resolved_private(self.store, identity, private)
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

    async def cleanup_candidate(
        self, session, identity, private, *, authorized_reclaim=False
    ):
        from .candidate_database import cleanup_candidate, public_resource

        self.store.read(
            "preflights", identity, session=None if authorized_reclaim else session
        )
        lock = FileLock(
            str(self.store.root / "analysis.lock"), timeout=0, thread_local=False
        )
        try:
            with lock:
                if self.store.active():
                    raise MigrationError("migration_operation_in_progress", status=409)
                try:
                    return await cleanup_candidate(
                        self.store, identity, private, MigrationBudget.start(30)
                    )
                finally:
                    try:
                        record = self.store.read(
                            "preflights",
                            identity,
                            session=None if authorized_reclaim else session,
                        )
                        record["candidate_resource"] = public_resource(
                            self.store, identity
                        )
                        if (
                            not record.get("job_id")
                            and record["candidate_resource"].get("state") == "cleaned"
                        ):
                            record["stage"] = "cancelled"
                        write_json_locked(
                            self.store.path("preflights", identity), record
                        )
                    except Exception:
                        # The resource receipt, not this summary, owns cleanup state.
                        pass
        except Timeout:
            raise MigrationError(
                "migration_operation_in_progress", status=409
            ) from None

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
        from .candidate_database import resolved_private

        private = resolved_private(self.store, job["options"]["preflight_id"], private)
        result = await submit_restore(
            self.project, session, job["options"], private=private, identity=job["id"]
        )
        return {**result, "handoff": "accepted"}
