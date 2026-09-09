from __future__ import annotations

from contextlib import closing
import hashlib
import json
from pathlib import Path
import sqlite3

from packaging.version import InvalidVersion, Version

from .archive import file_hash
from .database import _check_deadline, _quote, _readonly, _tables, snapshot_sqlite
from .discovery import SourceBoundary, category_for
from .errors import MigrationError
from .lease import InstanceLease
from .paths import contained_path, logical_path
from .restore import RestoreOptions, apply_files, prepare_files, rollback_files
from .snapshot import assert_offline

_ENGINES = {"sqlite", "mysql", "postgres"}


def require_same_engine(source: str, target: str) -> None:
    if source not in _ENGINES or target not in _ENGINES:
        raise MigrationError("migration_database_engine_invalid")
    if source != target:
        raise MigrationError("migration_database_cross_engine_forbidden")


def _sqlite_version(source: str) -> None:
    try:
        version = Version(source)
    except (InvalidVersion, TypeError):
        raise MigrationError("migration_database_version_invalid") from None
    if version.major != 3 or version > Version(sqlite3.sqlite_version):
        raise MigrationError("migration_database_version_unsupported")


def _quiet_target(root: Path, relative: str, *, online: bool = False) -> Path:
    logical_path(relative)
    if SourceBoundary.read(root).protected(relative):
        raise MigrationError("migration_target_protected", path=relative)
    category = category_for(relative)
    if category != "data" and not (
        category is None
        and "/" not in relative
        and Path(relative).suffix.lower() in {".db", ".sqlite", ".sqlite3"}
    ):
        raise MigrationError("migration_database_target_invalid")
    path = contained_path(root, relative)
    for suffix in ("-wal", "-shm", "-journal"):
        companion = contained_path(root, relative + suffix)
        if companion.exists() and not online:
            # Replacing only the main file while a journal exists can discard
            # committed pages or leave another connection on the old database.
            raise MigrationError("migration_database_writers_unconfirmed")
    return path


def sqlite_content_revision(path: Path, *, checkpoint=lambda: None) -> str:
    """Hash a consistent snapshot without WAL/checkpoint-specific file headers."""
    digest = hashlib.sha256()
    with closing(_readonly(path, immutable=True)) as connection:

        def progress():
            try:
                checkpoint()
                return 0
            except MigrationError:
                return 1

        connection.set_progress_handler(progress, 1000)
        rows = 0
        for table in _tables(connection):
            checkpoint()
            quoted = _quote(table)
            columns = connection.execute(f"PRAGMA table_info({quoted})").fetchall()
            if len(columns) > 512:
                raise MigrationError("migration_database_analysis_limit")
            # iterdump materializes one SQL INSERT at a time. Bound that row
            # inside SQLite before asking Python to receive its contents.
            lengths = "+".join(
                f"coalesce(length(CAST({_quote(c[1])} AS BLOB)),0)" for c in columns
            )
            if (
                lengths
                and connection.execute(
                    f"SELECT 1 FROM {quoted} WHERE {lengths}>8388608 LIMIT 1"
                ).fetchone()
            ):
                raise MigrationError("migration_database_analysis_limit")
            rows += connection.execute(f"SELECT COUNT(*) FROM {quoted}").fetchone()[0]
            if rows > 2_000_000:
                raise MigrationError("migration_database_analysis_limit")
        for statement in connection.iterdump():
            checkpoint()
            digest.update(statement.encode("utf-8"))
            digest.update(b"\0")
        for pragma in ("user_version", "application_id"):
            digest.update(
                str(connection.execute(f"PRAGMA {pragma}").fetchone()).encode()
            )
    return digest.hexdigest()


def _validate_candidate(path: Path, *, deadline: float, cancel=None) -> dict:
    with closing(_readonly(path)) as connection:

        def progress():
            try:
                _check_deadline(deadline, cancel)
                return 0
            except MigrationError:
                return 1

        connection.set_progress_handler(progress, 1000)
        try:
            tables = _tables(connection)
            if connection.execute("PRAGMA integrity_check").fetchone() != ("ok",):
                raise MigrationError("migration_database_integrity_failed")
            if connection.execute("PRAGMA foreign_key_check").fetchone() is not None:
                raise MigrationError("migration_database_foreign_key_conflict")
            objects = connection.execute(
                "SELECT type, COUNT(*) FROM sqlite_master "
                "WHERE type IN ('trigger', 'view') GROUP BY type"
            ).fetchall()
            _check_deadline(deadline, cancel)
            return {"tables": len(tables), "executable_objects": dict(objects)}
        except sqlite3.Error:
            _check_deadline(deadline, cancel)
            raise MigrationError("migration_database_validation_failed") from None


def prepare_sqlite_replacement(
    root: Path,
    staging: Path,
    source: Path,
    *,
    target_path: str,
    source_sha256: str,
    source_version: str,
    package_id: str,
    first_deployment: bool,
    deadline: float,
    cancel=None,
    online: bool = False,
) -> dict:
    """Prepare an isolated full replacement; no source SQL scripts are executed.

    This is a stopped-instance phase, not an online migration entry point.
    The caller composes it with the file/configuration and validation transaction.
    """
    _sqlite_version(source_version)

    def check():
        _check_deadline(deadline, cancel)

    check()
    target = _quiet_target(root, target_path, online=online)
    if source.resolve() == target.resolve():
        raise MigrationError("migration_database_source_is_target")
    if file_hash(source, check) != source_sha256:
        raise MigrationError("migration_payload_hash_mismatch")
    # Use the common file undo contract, including identity-checked crash replay.
    plan = prepare_files(
        root,
        staging,
        {"package_id": package_id, "source": {}, "files": []},
        RestoreOptions(categories=frozenset()),
        checkpoint=check,
    )
    before = file_hash(target, check) if target.exists() else None
    target_summary = {"tables": 0, "executable_objects": {}}
    if before is not None:
        backup = staging / "database-target.db"
        # _quiet_target proved that the stopped target has no WAL or journal.
        # Immutable read avoids creating companion files during preflight.
        snapshot_sqlite(
            target, backup, deadline=deadline, cancel=cancel, immutable=not online
        )
        target_summary = _validate_candidate(backup, deadline=deadline, cancel=cancel)
        if not online and file_hash(target, check) != before:
            raise MigrationError("migration_target_changed", status=409)
    if first_deployment and target_summary["tables"]:
        raise MigrationError(
            "migration_first_deployment_database_not_empty", status=409
        )
    candidate = staging / "database-candidate.db"
    snapshot_sqlite(source, candidate, deadline=deadline, cancel=cancel, immutable=True)
    source_summary = _validate_candidate(candidate, deadline=deadline, cancel=cancel)
    if file_hash(source, check) != source_sha256:
        raise MigrationError("migration_payload_hash_mismatch")
    plan["actions"] = [
        {
            "path": target_path,
            "action": "replace" if before is not None else "add",
            "before": before,
            "after": file_hash(candidate, check),
            "candidate": candidate.relative_to(staging).as_posix(),
            "size": candidate.stat().st_size,
        }
    ]
    plan["database"] = {
        "engine": "sqlite",
        "mode": "replace",
        "target_path": target_path,
        "target_name": target.name,
        "first_deployment": first_deployment,
        "source_version": source_version,
        "target_version": sqlite3.sqlite_version,
        "source": source_summary,
        "target": target_summary,
        "target_content_revision": sqlite_content_revision(backup, checkpoint=check)
        if before is not None
        else None,
    }
    plan.pop("target_revision", None)
    plan["target_revision"] = hashlib.sha256(
        json.dumps(plan, sort_keys=True).encode()
    ).hexdigest()
    return plan


def _require_stopped(root: Path, lease: InstanceLease) -> None:
    lease.require_held()
    if lease.project.resolve() != root.resolve():
        raise MigrationError("migration_instance_lease_mismatch")
    if lease.role not in {"migration", "launcher"}:
        raise MigrationError("migration_database_requires_maintenance")
    assert_offline(root, management_peers=getattr(lease, "management_peers", ()))


def apply_sqlite_replacement(
    root: Path,
    staging: Path,
    plan: dict,
    journal: Path,
    *,
    lease: InstanceLease,
    expected_revision: str,
    source_trusted: bool,
    confirmed_database_name: str,
    deadline: float,
    rollback_deadline: float,
    cancel=None,
) -> dict:
    _require_stopped(root, lease)
    if not source_trusted:
        raise MigrationError("migration_source_trust_required")
    database = plan["database"]
    require_same_engine(database["engine"], "sqlite")
    if confirmed_database_name != database["target_name"]:
        raise MigrationError("migration_database_confirmation_required")
    _quiet_target(root, database["target_path"])
    return apply_files(
        root,
        staging,
        plan,
        journal,
        expected_revision=expected_revision,
        destructive_confirmed=True,
        checkpoint=lambda: (_check_deadline(deadline, cancel), lease.require_held()),
        rollback_checkpoint=lambda: _check_deadline(rollback_deadline),
    )


def rollback_sqlite_replacement(
    root: Path,
    staging: Path,
    plan: dict,
    journal: Path,
    *,
    lease: InstanceLease,
    deadline: float,
) -> dict:
    _require_stopped(root, lease)
    _quiet_target(root, plan["database"]["target_path"])
    return rollback_files(
        root,
        staging,
        journal,
        checkpoint=lambda: (_check_deadline(deadline), lease.require_held()),
    )
