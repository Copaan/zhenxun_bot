from __future__ import annotations

import asyncio
import hashlib
import json
from pathlib import Path

from zhenxun.utils.atomic_json import read_json_locked, write_json_locked

from .access import private_directory
from .archive import MANIFEST_LIMIT, extract_verified, file_hash, verify_archive
from .errors import MigrationError
from .paths import contained_path
from .replacement_database import (
    apply_sqlite_replacement,
    prepare_sqlite_replacement,
    rollback_sqlite_replacement,
)
from .restore import apply_files, prepare_files, recheck_files, rollback_files
from .selection import ReplacementSelection
from .snapshot import _database_configuration, assert_offline
from .tasks import MigrationBudget, TaskStore


def _read(path: Path) -> dict:
    contained_path(path.parent, path.name, regular=True)
    if path.stat().st_size > MANIFEST_LIMIT:
        raise MigrationError("migration_restore_plan_limit")
    value = read_json_locked(path, None)
    if not isinstance(value, dict):
        raise MigrationError("migration_restore_plan_invalid")
    return value


def _digest(value: dict) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True).encode()).hexdigest()


def database_selection(
    manifest: dict, mapping: dict | None
) -> tuple[dict, dict] | None:
    entries = [entry for entry in manifest["files"] if entry["category"] == "database"]
    if not entries:
        if mapping:
            raise MigrationError("migration_database_payload_missing")
        return None
    if len(entries) != 1:
        raise MigrationError("migration_multiple_databases_require_mapping")
    if not isinstance(mapping, dict) or mapping.get("engine") not in {
        "sqlite",
        "mysql",
        "postgres",
    }:
        raise MigrationError("migration_database_mapping_required")
    entry = entries[0]
    primary = manifest.get("source", {}).get("primary_database")
    if primary and (
        primary.get("root") != entry["root"]
        or primary.get("path") != entry["path"]
        or primary.get("engine") != mapping["engine"]
    ):
        raise MigrationError("migration_primary_database_mismatch")
    if mapping.get("source_path") != entry["path"]:
        raise MigrationError("migration_database_source_confirmation_required")
    descriptions = [
        item
        for item in manifest.get("source", {}).get("databases", [])
        if item.get("root") == entry["root"] and item.get("path") == entry["path"]
    ]
    if len(descriptions) != 1 or descriptions[0].get("engine") != mapping["engine"]:
        raise MigrationError("migration_database_metadata_missing")
    if mapping["engine"] != "sqlite" and (
        not primary or descriptions[0].get("sha256") != entry["sha256"]
    ):
        raise MigrationError("migration_database_metadata_missing")
    return entry, descriptions[0]


def require_target_connection(project, mapping, private):
    value = _database_configuration(project)
    if not value:
        raise MigrationError("migration_database_connection_confirmation_required")
    if mapping["engine"] in {"mysql", "postgres"}:
        from .native_database import DatabaseEndpoint

        if not private.get("target_url"):
            raise MigrationError("migration_database_credentials_required")
        expected = DatabaseEndpoint.parse(private["target_url"])
        observed = DatabaseEndpoint.parse(value)
        matches = expected.identity() == observed.identity()
    else:
        from zhenxun.configs.database import sqlite_path_from_url

        try:
            observed = sqlite_path_from_url(value, root=project)
        except ValueError:
            raise MigrationError("migration_database_cross_engine_forbidden") from None
        matches = observed == contained_path(project, mapping["target_path"])
    if not matches:
        raise MigrationError("migration_database_connection_confirmation_required")


def execute_restore_phase(project: Path, request: dict, *, lease) -> dict:
    from .configuration import bound_environment, confirmed_process_environment

    values = request.get("private_input", {}).get("configuration")
    if request["phase"] not in {"restore_publish", "restore_rollback"} or values:
        options = TaskStore(project).read("jobs", request["job_id"])["options"]
        values = bound_environment(options, values)
    with confirmed_process_environment(values):
        return _execute_restore_phase(project, request, lease=lease)


def _execute_restore_phase(project: Path, request: dict, *, lease) -> dict:
    store = TaskStore(project)
    identity = request["job_id"]
    job = store.read("jobs", identity)
    phase = request["phase"]
    directory = store.path("jobs", identity).parent
    budget = MigrationBudget.start(min(float(request["remaining_seconds"]), 3600))

    def check():
        budget.checkpoint()
        lease.require_held()
        if (
            phase != "restore_rollback"
            and store.read("jobs", identity)["cancel_requested"]
        ):
            raise MigrationError("migration_cancelled")

    check()
    if not job["options"].get("source_trusted"):
        raise MigrationError("migration_source_trust_required")
    if phase == "restore_publish":
        from .publication import publish_restore

        return publish_restore(project, identity, lease=lease, checkpoint=check)
    if phase == "restore_dependencies":
        from .generation import stage_generation

        archive = contained_path(project, job["options"]["archive_path"], regular=True)
        password = request.get("private_input", {}).get("archive_password")
        if file_hash(archive, check) != job["options"]["archive_sha256"]:
            raise MigrationError("migration_archive_changed", status=409)
        manifest = verify_archive(
            archive, password=password.encode() if password else None, checkpoint=check
        )
        if file_hash(archive, check) != job["options"]["archive_sha256"]:
            raise MigrationError("migration_archive_changed", status=409)

        async def restore_dependencies():
            from zhenxun.nonebot_store.dependencies import (
                FORBIDDEN_LAYER_PACKAGES,
                base_installed_inventory,
                project_closure,
            )
            from zhenxun.services.lifecycle.deadline import (
                ShutdownBudget,
                current_budget,
            )
            from zhenxun.services.lifecycle.kernel import LifecycleKernel
            from zhenxun.services.lifecycle.launcher import LauncherSupervisor

            from .dependencies import DependencyRestorer

            supervisor = LauncherSupervisor(LifecycleKernel())
            core = project_closure()
            if not core:
                raise MigrationError("migration_core_constraints_unavailable")
            installed = base_installed_inventory()
            core.update(
                {
                    name: installed[name]
                    for name in FORBIDDEN_LAYER_PACKAGES
                    if name in installed
                }
            )
            token = current_budget.set(ShutdownBudget(budget.deadline))
            private = request.get("private_input", {}).get("dependencies") or {}
            try:
                restorer = DependencyRestorer(
                    directory / "dependencies",
                    supervisor,
                    core=core,
                    index_url=private.get("index_url"),
                    find_links=Path(private["find_links"])
                    if private.get("find_links")
                    else None,
                    declarations=tuple(private.get("declarations", ())),
                )
                result = await restorer.restore(
                    manifest["source"], budget=budget, checkpoint=check
                )
                candidate = contained_path(restorer.directory, result["candidate"])
                generation = stage_generation(
                    project,
                    identity,
                    candidate,
                    core=core,
                    lease=lease,
                    checkpoint=check,
                )
                result["generation"] = generation["generation"]
                result.pop("candidate")
                write_json_locked(directory / "dependency-result.json", result)
                return {
                    "state": result["state"],
                    "generation": generation["generation"],
                    "missing_count": len(result["missing"]),
                    "relaxed_count": len(result["relaxed"]),
                }
            finally:
                try:
                    supervisor.shutdown_deadline = ShutdownBudget(budget.deadline)
                    await supervisor.shutdown()
                finally:
                    current_budget.reset(token)

        return asyncio.run(restore_dependencies())
    assert_offline(project, management_peers=getattr(lease, "management_peers", ()))
    if phase == "restore_prepare":
        approved = None
        administrator = None
        preflight_id = job["options"].get("preflight_id")
        if preflight_id:
            from .analysis import recheck_preflight
            from .configuration import preflight_administrator

            administrator = preflight_administrator(
                store, preflight_id, request.get("private_input", {})
            )

            recheck_preflight(
                project, preflight_id, request.get("private_input", {}), budget
            )
            approved = _read(
                store.path("preflights", preflight_id).parent / "analysis.json"
            )
            if approved["revision"] != job["options"].get("target_revision"):
                raise MigrationError("migration_preflight_receipt_invalid", status=409)
        archive = contained_path(project, job["options"]["archive_path"], regular=True)
        digest = job["options"]["archive_sha256"]
        if file_hash(archive, check) != digest:
            raise MigrationError("migration_archive_changed", status=409)
        password = request.get("private_input", {}).get("archive_password")
        if password is not None and (
            not isinstance(password, str) or len(password.encode()) > 4096
        ):
            raise MigrationError("migration_password_invalid")
        stage = directory / "restore-stage"
        manifest = extract_verified(
            archive,
            stage,
            password=password.encode() if password else None,
            checkpoint=check,
        )
        if file_hash(archive, check) != digest:
            raise MigrationError("migration_archive_changed", status=409)
        selection = ReplacementSelection.from_manifest(
            manifest, legacy=job["options"].get("legacy_selection")
        )
        mapping = job["options"].get("database")
        database = database_selection(manifest, mapping)
        files = prepare_files(
            project,
            stage,
            manifest,
            selection.restore_options(),
            checkpoint=check,
            configuration_overrides=request.get("private_input", {}).get(
                "configuration"
            ),
            administrator_overrides=administrator,
        )
        if files["conflicts"]:
            raise MigrationError("migration_conflicts_unresolved", status=409)
        if (
            approved
            and files["target_revision"] != approved["files"]["target_revision"]
        ):
            raise MigrationError("migration_target_changed", status=409)
        database_plan = None
        if database and mapping["engine"] != "sqlite":
            from .native_database import DatabaseEndpoint
            from .native_replacement import prepare_native

            entry, description = database
            private = request.get("private_input", {}).get("database") or {}
            if not private.get("target_url") or not _database_configuration(project):
                raise MigrationError("migration_database_credentials_required")
            selected = DatabaseEndpoint.parse(private["target_url"])
            configured = DatabaseEndpoint.parse(_database_configuration(project))
            if selected.identity() != configured.identity():
                raise MigrationError(
                    "migration_database_connection_confirmation_required"
                )
            database_stage = private_directory(directory / "database-stage")
            if approved:
                from .restore import _atomic_copy

                database_plan = approved["database"]
                _atomic_copy(
                    contained_path(
                        store.path("preflights", preflight_id).parent,
                        "database-stage/database-target.backup",
                        regular=True,
                    ),
                    database_stage / "database-target.backup",
                    checkpoint=check,
                )
            else:
                database_plan = asyncio.run(
                    prepare_native(
                        database_stage,
                        contained_path(stage, entry["payload"], regular=True),
                        description,
                        private=private,
                        first_deployment=job["options"].get("first_deployment") is True,
                        source_trusted=True,
                        budget=budget,
                        checkpoint=check,
                    )
                )
            database_plan["source_payload"] = entry["payload"]
        elif database:
            entry, description = database
            from zhenxun.configs.database import sqlite_path_from_url

            target_connection = _database_configuration(project)
            if target_connection:
                try:
                    connected = sqlite_path_from_url(target_connection, root=project)
                except ValueError:
                    raise MigrationError(
                        "migration_database_cross_engine_forbidden"
                    ) from None
                if connected != contained_path(project, mapping["target_path"]):
                    raise MigrationError(
                        "migration_database_connection_confirmation_required"
                    )
            else:
                raise MigrationError(
                    "migration_database_connection_confirmation_required"
                )
            database_stage = private_directory(directory / "database-stage")
            database_plan = prepare_sqlite_replacement(
                project,
                database_stage,
                contained_path(stage, entry["payload"], regular=True),
                target_path=mapping["target_path"],
                source_sha256=entry["sha256"],
                source_version=description["engine_version"],
                package_id=manifest["package_id"],
                first_deployment=job["options"].get("first_deployment") is True,
                deadline=budget.deadline,
                cancel=lambda: (check() or False),
            )
        plan = {
            "schema": 1,
            "job_id": identity,
            "package_id": manifest["package_id"],
            "archive_sha256": digest,
            "files": files,
            "database": database_plan,
        }
        plan["revision"] = _digest(plan)
        write_json_locked(directory / "restore-plan.json", plan)
        return {
            "revision": plan["revision"],
            "file_actions": len(files["actions"]),
            "removed_directories": len(files["removed_directories"]),
            "database_prepared": database_plan is not None,
            "validation": "not_started",
        }
    plan = _read(directory / "restore-plan.json")
    digest = _digest({key: value for key, value in plan.items() if key != "revision"})
    if plan.get("job_id") != identity or plan.get("revision") != digest:
        raise MigrationError("migration_restore_plan_changed")
    stage = directory / "restore-stage"
    database_stage = directory / "database-stage"
    files_journal = stage / "files-journal.json"
    database_journal = database_stage / "database-journal.json"
    if phase == "restore_apply":
        if request.get("private_input", {}).get("expected_revision") != digest:
            raise MigrationError("migration_target_changed", status=409)
        recheck_files(project, stage, plan["files"], checkpoint=check)
        mapping = job["options"].get("database") or {}
        if (
            plan["database"]
            and mapping.get("confirmed_name")
            != plan["database"]["database"]["target_name"]
        ):
            raise MigrationError("migration_database_confirmation_required")
        # Check external data before the first file write, then again before DDL.
        if plan["database"] and plan["database"].get("engine") in {"mysql", "postgres"}:
            from .native_replacement import recheck_native

            asyncio.run(
                recheck_native(
                    database_stage,
                    contained_path(
                        stage, plan["database"]["source_payload"], regular=True
                    ),
                    plan["database"],
                    private=request.get("private_input", {}).get("database") or {},
                    budget=budget,
                    checkpoint=check,
                )
            )
        # One durable intent covers both resources before either is modified.
        write_json_locked(
            directory / "restore-application.json",
            {
                "job_id": identity,
                "revision": digest,
                "state": "applying",
            },
        )
        apply_files(
            project,
            stage,
            plan["files"],
            files_journal,
            expected_revision=plan["files"]["target_revision"],
            destructive_confirmed=job["options"].get("replacement_confirmed") is True,
            checkpoint=check,
            rollback_checkpoint=budget.checkpoint,
        )
        if plan["database"]:
            # Restored configuration must not send validation to the source DB.
            require_target_connection(
                project,
                mapping,
                request.get("private_input", {}).get("database") or {},
            )
        if plan["database"] and plan["database"].get("engine") in {"mysql", "postgres"}:
            from .native_replacement import apply_native

            asyncio.run(
                apply_native(
                    database_stage,
                    contained_path(
                        stage, plan["database"]["source_payload"], regular=True
                    ),
                    plan["database"],
                    database_journal,
                    private=request.get("private_input", {}).get("database") or {},
                    confirmed_name=mapping["confirmed_name"],
                    budget=budget,
                    checkpoint=check,
                )
            )
        elif plan["database"]:
            apply_sqlite_replacement(
                project,
                database_stage,
                plan["database"],
                database_journal,
                lease=lease,
                expected_revision=plan["database"]["target_revision"],
                source_trusted=True,
                confirmed_database_name=mapping["confirmed_name"],
                deadline=budget.deadline,
                rollback_deadline=budget.deadline,
                cancel=lambda: (check() or False),
            )
        write_json_locked(
            directory / "restore-application.json",
            {
                "job_id": identity,
                "revision": digest,
                "state": "applied_unverified",
            },
        )
        return {"state": "applied_unverified", "revision": digest}
    if phase == "restore_rollback":
        if (directory / "restore-commit.json").exists():
            raise MigrationError("migration_committed_rollback_forbidden")
        results = {}
        failures = []
        for kind, journal in (("database", database_journal), ("files", files_journal)):
            check()
            if (
                not journal.exists()
                and not journal.with_name(journal.name + ".events").exists()
            ):
                results[kind] = "not_applied"
                continue
            try:
                if kind == "database":
                    if plan["database"].get("engine") in {"mysql", "postgres"}:
                        from .native_replacement import rollback_native

                        asyncio.run(
                            rollback_native(
                                database_stage,
                                plan["database"],
                                journal,
                                private=request.get("private_input", {}).get("database")
                                or {},
                                budget=budget,
                                checkpoint=budget.checkpoint,
                            )
                        )
                    else:
                        rollback_sqlite_replacement(
                            project,
                            database_stage,
                            plan["database"],
                            journal,
                            lease=lease,
                            deadline=budget.deadline,
                        )
                else:
                    rollback_files(project, stage, journal, checkpoint=check)
                results[kind] = "rolled_back"
            except MigrationError as error:
                results[kind] = error.code
                failures.append(error.code)
        write_json_locked(
            directory / "restore-rollback.json",
            {
                "job_id": identity,
                "revision": digest,
                "resources": results,
            },
        )
        if failures:
            raise MigrationError("migration_restore_rollback_incomplete")
        return {"state": "rolled_back", "resources": results}
    raise MigrationError("migration_phase_invalid")
