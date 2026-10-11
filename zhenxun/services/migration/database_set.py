"""Auxiliary SQLite databases participating in the instance restore transaction."""

from __future__ import annotations

import hashlib
import uuid

from .access import private_directory
from .database import snapshot_sqlite
from .errors import MigrationError
from .paths import contained_path
from .replacement_database import (
    apply_sqlite_replacement,
    prepare_sqlite_replacement,
    rollback_sqlite_replacement,
    sqlite_content_revision,
)


def auxiliary_entries(manifest, mapping):
    primary = manifest.get("source", {}).get("primary_database") or {}
    primary_path = primary.get("path") or (mapping or {}).get("source_path")
    descriptions = manifest.get("source", {}).get("databases", [])
    result = []
    for entry in manifest["files"]:
        if entry["category"] != "database" or entry["path"] == primary_path:
            continue
        matches = [
            d
            for d in descriptions
            if (d.get("root"), d.get("path")) == (entry["root"], entry["path"])
        ]
        if len(matches) != 1 or matches[0].get("engine") != "sqlite":
            raise MigrationError(
                "migration_database_metadata_missing", path=entry["path"]
            )
        if entry["root"] != "project":
            raise MigrationError(
                "migration_external_root_unsupported", path=entry["path"]
            )
        if (mapping or {}).get("target_path", "").casefold() == entry[
            "path"
        ].casefold():
            raise MigrationError(
                "migration_database_target_duplicate", path=entry["path"]
            )
        result.append((entry, matches[0]))
    return sorted(result, key=lambda pair: pair[0]["path"])


def prepare_auxiliary(
    project,
    directory,
    stage,
    manifest,
    mapping,
    *,
    first_deployment,
    budget,
    online=False,
):
    result = []
    for entry, description in auxiliary_entries(manifest, mapping):
        budget.checkpoint()
        resource_id = hashlib.sha256(entry["path"].encode()).hexdigest()[:32]
        relative = f"database-stage/auxiliary/{resource_id}"
        db_stage = private_directory(contained_path(directory, relative))
        plan = prepare_sqlite_replacement(
            project,
            db_stage,
            contained_path(stage, entry["payload"], regular=True),
            target_path=entry["path"],
            source_sha256=entry["sha256"],
            source_version=description["engine_version"],
            package_id=manifest["package_id"],
            first_deployment=first_deployment,
            deadline=budget.deadline,
            cancel=lambda: (budget.checkpoint() or False),
            online=online,
        )
        result.append(
            {"id": resource_id, "role": "auxiliary", "stage": relative, "plan": plan}
        )
    return result


def recheck_auxiliary(project, directory, items, budget):
    for item in items:
        metadata = item["plan"]["database"]
        target = contained_path(project, metadata["target_path"])
        observed = None
        if target.exists():
            stage = contained_path(directory, item["stage"])
            snapshot = stage / (uuid.uuid4().hex + ".db")
            try:
                snapshot_sqlite(target, snapshot, deadline=budget.deadline)
                observed = sqlite_content_revision(
                    snapshot,
                    checkpoint=budget.checkpoint,
                    algorithm=metadata.get("revision_algorithm", "sqlite-content-v1"),
                )
            finally:
                snapshot.unlink(missing_ok=True)
        if observed != metadata["target_content_revision"]:
            raise MigrationError(
                "migration_database_target_changed",
                path=metadata["target_path"],
                status=409,
            )


def apply_auxiliary(
    project, directory, items, *, lease, budget, record=lambda value: None
):
    for item in items:
        budget.checkpoint()
        stage = contained_path(directory, item["stage"])
        plan = item["plan"]
        record(resource_state(item, "applying"))
        apply_sqlite_replacement(
            project,
            stage,
            plan,
            stage / "database-journal.json",
            lease=lease,
            expected_revision=plan["target_revision"],
            source_trusted=True,
            confirmed_database_name=plan["database"]["target_name"],
            deadline=budget.deadline,
            rollback_deadline=budget.deadline,
            cancel=lambda: (budget.checkpoint() or False),
        )
        record(resource_state(item, "applied_unverified"))


def rollback_auxiliary(
    project, directory, items, *, lease, budget, record=lambda value: None
):
    results = {}
    for item in reversed(items):
        stage = contained_path(directory, item["stage"])
        journal = stage / "database-journal.json"
        if (
            not journal.exists()
            and not journal.with_name(journal.name + ".events").exists()
        ):
            results[item["id"]] = "not_applied"
            continue
        try:
            rollback_sqlite_replacement(
                project,
                stage,
                item["plan"],
                journal,
                lease=lease,
                deadline=budget.deadline,
            )
            results[item["id"]] = "rolled_back"
            record(resource_state(item, "rolled_back"))
        except MigrationError as error:
            results[item["id"]] = error.code
            record(
                {**resource_state(item, "recovery_required"), "error_code": error.code}
            )
    return results


def resource_state(item, state):
    metadata = item["plan"]["database"]
    return {
        "id": item["id"],
        "role": item.get("role", "primary"),
        "engine": item["plan"].get("engine", metadata.get("engine", "sqlite")),
        "path": metadata.get("target_path"),
        "target_name": metadata.get("target_name"),
        "state": state,
    }


def public_databases(primary, auxiliary):
    values = []
    if primary:
        values.append(
            {
                "role": "primary",
                "state": "prepared",
                **primary.get("database", {}),
                "engine": primary.get(
                    "engine", primary.get("database", {}).get("engine")
                ),
            }
        )
    values.extend(
        {
            "id": item["id"],
            "role": "auxiliary",
            "state": "prepared",
            **item["plan"]["database"],
        }
        for item in auxiliary
    )
    return values
