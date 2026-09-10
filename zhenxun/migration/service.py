from __future__ import annotations

from pathlib import Path
import re
import shutil

from .archive import Limits, encryption_available, file_hash, verify_archive
from .database import database_capabilities
from .errors import MigrationError
from .inventory import _safe_text
from .paths import contained_path, is_link
from .selection import ReplacementSelection
from .tasks import MigrationBudget


def recovery_requirements(store, identity: str) -> dict:
    from zhenxun.utils.atomic_json import read_json_locked

    from .archive import MANIFEST_LIMIT

    job = store.read("jobs", identity)
    if job["stage"] not in {
        "awaiting_credentials",
        "recovery_required",
    }:
        raise MigrationError("migration_recovery_stage_invalid", status=409)
    if job["action"] == "export":
        return {
            "task_id": identity,
            "engine": None,
            "target": {},
            "credentials_required": False,
        }
    path = contained_path(
        store.path("jobs", identity).parent, "restore-plan.json", regular=True
    )
    if path.stat().st_size > MANIFEST_LIMIT:
        raise MigrationError("migration_restore_plan_limit")
    plan = read_json_locked(path, None)
    if not isinstance(plan, dict) or plan.get("job_id") != identity:
        raise MigrationError("migration_recovery_receipt_invalid", status=409)
    database = plan.get("database") or {}
    engine = database.get("engine")
    target = database.get("target") or {}
    return {
        "task_id": identity,
        "engine": engine,
        "target": {
            key: target[key] for key in ("host", "port", "database") if key in target
        },
        "credentials_required": engine in {"mysql", "postgres"},
        "committed": bool(job.get("committed_at")),
        "retry_revalidates_ownership": True,
    }


def capabilities() -> dict:
    databases = database_capabilities()
    databases["sqlite"].pop("candidate_incremental", None)
    for engine, entry in databases.items():
        blockers = []
        if shutil.which("uv") is None:
            blockers.append("migration_dependency_tool_missing")
        if engine != "sqlite" and not (
            entry.get("dump_tool")
            and entry.get("restore_tool")
            and (engine != "postgres" or shutil.which("psql"))
        ):
            blockers.append("migration_database_tool_missing")
        entry["offline_restore_available"] = not blockers
        entry["restore_available"] = not blockers
        entry["offline_restore_blockers"] = blockers
        entry["online_restore_blockers"] = list(blockers)
    return {
        "format": "zhenxun-instance",
        "schema": 1,
        "offline_export": True,
        "inspect": True,
        "online_export": True,
        "offline_restore": any(
            entry["offline_restore_available"] for entry in databases.values()
        ),
        "preflight": True,
        "restore": any(entry["restore_available"] for entry in databases.values()),
        "restore_blockers": []
        if any(entry["restore_available"] for entry in databases.values())
        else ["migration_runtime_prerequisites_missing"],
        "first_deployment_requires_console_authorization": True,
        "acceptance": "internal_integration_simulation",
        "encryption": encryption_available(),
        "databases": databases,
    }


def discover_packages(project: Path, *, maximum: int = 1000) -> list[dict]:
    if not 1 <= maximum <= 1000:
        raise MigrationError("migration_discovery_limit_invalid")
    result = []
    inspected = 0
    for directory in (project, contained_path(project, "migration/inbox")):
        if not directory.exists():
            continue
        for entry in directory.iterdir():
            inspected += 1
            if inspected > 10_000:
                raise MigrationError("migration_scan_work_limit")
            if entry.suffix.casefold() != ".zx":
                continue
            if is_link(entry) or not entry.is_file():
                continue
            path = entry.relative_to(project).as_posix()
            contained_path(project, path, regular=True)
            result.append({"path": path, "size": entry.stat().st_size})
            if len(result) > maximum:
                raise MigrationError("migration_discovery_limit")
    return sorted(result, key=lambda item: item["path"].casefold())


def inspect_package(
    path: Path,
    *,
    password: bytes | None = None,
    limits: Limits = Limits(),
    offset: int = 0,
    limit: int = 100,
    budget: MigrationBudget | None = None,
) -> dict:
    if path.suffix.casefold() != ".zx":
        raise MigrationError("migration_archive_extension_invalid")
    return inspect_archive(
        path,
        password=password,
        limits=limits,
        offset=offset,
        limit=limit,
        budget=budget,
    )


def inspect_archive(
    path: Path,
    *,
    password: bytes | None = None,
    limits: Limits = Limits(),
    offset: int = 0,
    limit: int = 100,
    budget: MigrationBudget | None = None,
) -> dict:
    """Inspect a service-owned artifact, including a sealed upload's .part file."""
    if offset < 0 or not 1 <= limit <= 100:
        raise MigrationError("migration_pagination_invalid")
    path = contained_path(path.parent, path.name, regular=True)
    budget = budget or MigrationBudget.start()
    before = file_hash(path, budget.checkpoint)
    manifest = verify_archive(
        path, password=password, limits=limits, checkpoint=budget.checkpoint
    )
    if before != file_hash(path, budget.checkpoint):
        raise MigrationError("migration_archive_changed", status=409)
    source = manifest["source"]
    summary = {}
    for key in ("core_version", "python", "system", "architecture", "implementation"):
        value = _safe_text(source.get(key))
        if value:
            summary[key] = value
    revision = source.get("git_revision")
    if isinstance(revision, str) and re.fullmatch(r"[a-fA-F0-9]{40,64}", revision):
        summary["git_revision"] = revision
    categories = {}
    for entry in manifest["files"]:
        category = categories.setdefault(entry["category"], {"count": 0, "bytes": 0})
        category["count"] += 1
        category["bytes"] += entry["size"]
    blockers = []
    selection = None
    try:
        selection = ReplacementSelection.from_manifest(manifest).public()
    except MigrationError as error:
        blockers.append(error.code)
    return {
        "package_id": manifest["package_id"],
        "database": {
            key: manifest["source"]["primary_database"][key]
            for key in ("engine", "path")
            if key in manifest["source"]["primary_database"]
        }
        if isinstance(manifest["source"].get("primary_database"), dict)
        else None,
        "sha256": before,
        "source": summary,
        "categories": categories,
        "selection": selection,
        "replacement_blockers": blockers,
        "total": len(manifest["files"]),
        "items": [
            {key: entry[key] for key in ("root", "path", "category", "size", "sha256")}
            for entry in manifest["files"][offset : offset + limit]
        ],
        "integrity_verified": True,
        "source_trust_verified": False,
        "restore_available": not blockers and capabilities()["restore"],
        "preflight_required": True,
    }
