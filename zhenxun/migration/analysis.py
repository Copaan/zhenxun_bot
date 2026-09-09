from __future__ import annotations

import asyncio
from pathlib import Path
import uuid

from zhenxun.utils.atomic_json import write_json_locked

from .access import private_directory
from .archive import extract_verified, file_hash
from .configuration import bound_environment, confirmed_process_environment
from .database import snapshot_sqlite
from .dependencies import effective_requests
from .discovery import scan_project
from .errors import MigrationError
from .paths import contained_path
from .replacement_database import (
    prepare_sqlite_replacement,
    sqlite_content_revision,
)
from .restore import prepare_files, recheck_files
from .restore_phases import _digest, _read, database_selection
from .selection import ReplacementSelection
from .tasks import MigrationBudget, TaskStore


def analyse_preflight(project: Path, identity: str, private: dict, budget) -> dict:
    store = TaskStore(project)
    record = store.read("preflights", identity)
    options = record["options"]
    if not options.get("source_trusted"):
        raise MigrationError("migration_source_trust_required")
    configuration = bound_environment(options, private.get("configuration"))
    from .configuration import preflight_administrator

    administrator = preflight_administrator(store, identity, private)
    directory = store.path("preflights", identity).parent
    archive = contained_path(project, options["archive_path"], regular=True)
    if file_hash(archive, budget.checkpoint) != record["sha256"]:
        raise MigrationError("migration_archive_changed", status=409)
    password = private.get("archive_password")
    stage = directory / "stage"
    manifest = extract_verified(
        archive,
        stage,
        password=password.encode() if password else None,
        checkpoint=budget.checkpoint,
    )
    if file_hash(archive, budget.checkpoint) != record["sha256"]:
        raise MigrationError("migration_archive_changed", status=409)
    selection = ReplacementSelection.from_manifest(
        manifest, legacy=options.get("legacy_selection")
    )
    with confirmed_process_environment(configuration):
        files = prepare_files(
            project,
            stage,
            manifest,
            selection.restore_options(),
            configuration_overrides=configuration,
            administrator_overrides=administrator,
            checkpoint=budget.checkpoint,
        )
    if files["conflicts"]:
        raise MigrationError("migration_conflicts_unresolved", status=409)
    selected = database_selection(manifest, options.get("database"))
    database = None
    if selected:
        entry, description = selected
        mapping = options["database"]
        db_stage = private_directory(directory / "database-stage")
        source = contained_path(stage, entry["payload"], regular=True)
        if mapping["engine"] == "sqlite":
            database = prepare_sqlite_replacement(
                project,
                db_stage,
                source,
                target_path=mapping["target_path"],
                source_sha256=entry["sha256"],
                source_version=description["engine_version"],
                package_id=manifest["package_id"],
                first_deployment=options["first_deployment"],
                deadline=budget.deadline,
                cancel=lambda: (budget.checkpoint() or False),
                online=True,
            )
        else:
            from .native_replacement import prepare_native

            database = asyncio.run(
                prepare_native(
                    db_stage,
                    source,
                    description,
                    private=private.get("database") or {},
                    first_deployment=options["first_deployment"],
                    source_trusted=True,
                    live_analysis=True,
                    budget=budget,
                    checkpoint=budget.checkpoint,
                )
            )
            database["source_payload"] = entry["payload"]
        if mapping.get("confirmed_name") != database["database"]["target_name"]:
            raise MigrationError("migration_database_confirmation_required")
    dependencies, issues = effective_requests(manifest["source"], {})
    plan = {
        "schema": 1,
        "preflight_id": identity,
        "archive_sha256": record["sha256"],
        "files": files,
        "database": database,
        "dependencies": dependencies,
        "dependency_issues": issues,
    }
    plan["revision"] = _digest(plan)
    write_json_locked(directory / "analysis.json", plan)
    return {"revision": plan["revision"], **preflight_summary(plan)}


def preflight_summary(plan: dict) -> dict:
    counts = {name: 0 for name in ("add", "replace", "remove")}
    for action in plan["files"]["actions"]:
        counts[action["action"]] += 1
    return {
        "files": counts,
        "removed_directories": len(plan["files"]["removed_directories"]),
        "dependencies": len(plan["dependencies"]),
        "dependency_issues": len(plan["dependency_issues"]),
        "database": (plan.get("database") or {}).get("database"),
        "source_trust_verified": False,
        "plugin_side_effects_reversible": False,
    }


def recheck_preflight(
    project: Path, identity: str, private: dict, budget, *, live_analysis=False
) -> dict:
    store = TaskStore(project)
    record = store.read("preflights", identity)
    bound_environment(record["options"], private.get("configuration"))
    from .configuration import preflight_administrator

    preflight_administrator(store, identity, private)
    directory = store.path("preflights", identity).parent
    plan = _read(directory / "analysis.json")
    if plan["revision"] != _digest({k: v for k, v in plan.items() if k != "revision"}):
        raise MigrationError("migration_preflight_receipt_invalid", status=409)
    if plan["revision"] != record["target_revision"]:
        raise MigrationError("migration_preflight_receipt_invalid", status=409)
    archive = contained_path(project, record["options"]["archive_path"], regular=True)
    if file_hash(archive, budget.checkpoint) != record["sha256"]:
        raise MigrationError("migration_archive_changed", status=409)
    recheck_files(
        project, directory / "stage", plan["files"], checkpoint=budget.checkpoint
    )
    database = plan.get("database")
    if database and database.get("engine") in {"mysql", "postgres"}:
        from .native_replacement import recheck_native

        asyncio.run(
            recheck_native(
                directory / "database-stage",
                contained_path(
                    directory / "stage", database["source_payload"], regular=True
                ),
                database,
                private=private.get("database") or {},
                live_analysis=live_analysis,
                budget=budget,
                checkpoint=budget.checkpoint,
            )
        )
    elif database:
        target = contained_path(project, database["database"]["target_path"])
        expected = database["database"]["target_content_revision"]
        observed = None
        if target.exists():
            snapshot = directory / "database-stage" / (uuid.uuid4().hex + ".db")
            try:
                snapshot_sqlite(target, snapshot, deadline=budget.deadline)
                observed = sqlite_content_revision(
                    snapshot, checkpoint=budget.checkpoint
                )
            finally:
                snapshot.unlink(missing_ok=True)
        if observed != expected:
            raise MigrationError("migration_database_target_changed", status=409)
    return {"revision": plan["revision"]}


def export_preview(project: Path, *, offset=0, limit=100, budget) -> dict:
    if offset < 0 or not 1 <= limit <= 100:
        raise MigrationError("migration_pagination_invalid")
    scan = scan_project(project, checkpoint=budget.checkpoint).public()
    entries = scan.pop("files")
    excluded = scan.pop("excluded")
    return {
        **scan,
        "items": entries[offset : offset + limit],
        "total": len(entries),
        "excluded": excluded[offset : offset + limit],
        "excluded_total": len(excluded),
    }


def execute_analysis(project: Path, request: dict) -> dict:
    budget = MigrationBudget.start(request["remaining_seconds"])
    operation = request["operation"]
    if operation == "retention":
        from .retention import expire_assets

        return expire_assets(project, budget=budget)
    if operation == "import_package":
        from .tasks import UPLOAD_CHUNK

        path = Path(request["options"]["path"]).absolute()
        path = contained_path(path.parent, path.name, regular=True)
        if path.suffix.casefold() != ".zx":
            raise MigrationError("migration_archive_extension_invalid")
        session = request["private"]["session"]
        store = TaskStore(project)
        upload = store.create_upload(session, total=path.stat().st_size)
        import hashlib

        digest = hashlib.sha256()
        offset = 0
        with path.open("rb") as stream:
            while chunk := stream.read(UPLOAD_CHUNK):
                budget.checkpoint()
                digest.update(chunk)
                store.append_chunk(
                    upload["id"],
                    session,
                    offset=offset,
                    data=chunk,
                    digest=hashlib.sha256(chunk).hexdigest(),
                )
                offset += len(chunk)
        return store.public(
            store.seal_upload(
                upload["id"], session, digest.hexdigest(), checkpoint=budget.checkpoint
            )
        )
    if operation == "export_preview":
        return export_preview(project, budget=budget, **request.get("options", {}))
    if operation == "preflight":
        return analyse_preflight(
            project, request["identity"], request["private"], budget
        )
    if operation == "recheck":
        return recheck_preflight(
            project, request["identity"], request["private"], budget, live_analysis=True
        )
    raise MigrationError("migration_analysis_operation_invalid")
