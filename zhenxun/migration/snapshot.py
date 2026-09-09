from __future__ import annotations

import asyncio
from dataclasses import asdict, dataclass
import hashlib
import os
from pathlib import Path
import time

from dotenv import dotenv_values
import psutil

from .access import private_directory
from .archive import CHUNK, Limits, build_archive, file_hash, require_space
from .database import snapshot_sqlite
from .discovery import CATEGORIES, FileEntry, scan_project
from .errors import MigrationError
from .inventory import environment_inventory, plugin_descriptors
from .lease import InstanceLease
from .paths import contained_path
from .restore import _configuration_read, _database_companion, _database_file
from .selection import export_selection
from .tasks import MigrationBudget, TaskStore


@dataclass(frozen=True)
class ExportOptions:
    categories: frozenset[str] = CATEGORIES
    plaintext_confirmed: bool = False
    dependencies: bool = True
    budget_seconds: int = 6 * 3600

    def __post_init__(self) -> None:
        if not self.categories or not self.categories <= CATEGORIES:
            raise MigrationError("migration_categories_invalid")
        MigrationBudget.start(self.budget_seconds)


def assert_offline(project: Path, *, management_peers: tuple = ()) -> None:
    ancestors = {p.pid for p in psutil.Process().parents()} | {os.getpid()}
    for process in psutil.process_iter(["pid", "name", "cwd"], ad_value=None):
        if process.pid in ancestors:
            continue
        if any(
            peer.get("pid") == process.pid
            and peer.get("created_at") == process.create_time()
            for peer in management_peers
        ):
            continue
        name = str(process.info.get("name") or "").lower()
        if not any(
            name.startswith(prefix) for prefix in ("python", "pypy", "zx", "uv.")
        ):
            continue
        cwd = process.info.get("cwd")
        if cwd and Path(cwd).resolve() == project.resolve():
            raise MigrationError("migration_instance_running", status=409)
        if not cwd and name.startswith(("python", "pypy")):
            raise MigrationError("migration_process_identity_unobserved", status=409)


def _database_configuration(project: Path) -> str | None:
    values = {}
    for name in (".env", ".env.dev"):
        path = contained_path(project, name)
        if path.exists():
            from io import StringIO

            values.update(
                {
                    str(k).upper(): v
                    for k, v in dotenv_values(
                        stream=StringIO(_configuration_read(path).decode("utf-8-sig")),
                        interpolate=True,
                    ).items()
                }
            )
    # NoneBot expands dotenv references and lets even an empty environment value
    # override the files. Truthiness here would select a different database.
    environment = {key.upper(): value for key, value in os.environ.items()}
    return environment.get("DB_URL", values.get("DB_URL"))


def capture_snapshot(
    project: Path,
    destination: Path,
    *,
    options: ExportOptions,
    lease: InstanceLease,
    budget: MigrationBudget,
    limits: Limits = Limits(),
    checkpoint=lambda: None,
) -> tuple[list[FileEntry], dict]:
    lease.require_held()
    budget.checkpoint()

    def check():
        budget.checkpoint()
        checkpoint()

    initial = scan_project(project, max_entries=limits.entries, checkpoint=check)

    def stable_files(scan):
        # SQLite readers update SHM and may create an empty WAL. Verify WAL
        # payload separately; these transient files are never archive entries.
        return [
            entry
            for entry in scan.files
            if not _database_companion(contained_path(project, entry.path))
        ]

    def wal_hash(source):
        wal = contained_path(project, source.relative_to(project).as_posix() + "-wal")
        return file_hash(wal, check) if wal.exists() and wal.stat().st_size else None

    wal_hashes = {}
    files = [entry for entry in initial.files if entry.category in options.categories]
    if sum(entry.size for entry in files) > limits.expanded:
        raise MigrationError("migration_expanded_limit")
    database_url = _database_configuration(project)
    primary_database = None
    native_endpoint = None
    if "data" in options.categories and database_url:
        if not database_url.lower().startswith("sqlite:"):
            from .native_database import DatabaseEndpoint

            native_endpoint = DatabaseEndpoint.parse(database_url)
        else:
            from zhenxun.configs.database import (
                is_sqlite_memory_url,
                sqlite_path_from_url,
            )

            if is_sqlite_memory_url(database_url):
                raise MigrationError("migration_volatile_database_unsupported")
            configured = sqlite_path_from_url(database_url, root=project)
            if configured is None or not configured.is_relative_to(project):
                raise MigrationError("migration_database_root_mapping_required")
            if configured.relative_to(project).as_posix() not in {
                entry.path for entry in files
            }:
                raise MigrationError("migration_configured_database_not_in_snapshot")
            primary_database = {
                "root": "project",
                "path": configured.relative_to(project).as_posix(),
                "engine": "sqlite",
            }
    require_space(destination.parent, sum(entry.size for entry in files) * 2)
    private_directory(destination)
    captured = []
    databases = []
    identities = {}
    hashes = {}
    total = 0
    for entry in files:
        checkpoint()
        budget.checkpoint()
        lease.require_held()
        source = contained_path(project, entry.path, regular=True)
        if _database_companion(source):
            continue
        target = contained_path(destination, entry.path)
        target.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        before = source.stat()
        identities[entry.path] = (before.st_dev, before.st_ino, before.st_ctime_ns)
        if (before.st_size, before.st_mtime_ns) != (entry.size, entry.mtime_ns):
            raise MigrationError("migration_source_changed", path=entry.path)
        category = entry.category
        if _database_file(source):
            wal_hashes[entry.path] = wal_hash(source)
            hashes[entry.path] = file_hash(source, check)
            details = snapshot_sqlite(
                source,
                target,
                deadline=budget.deadline,
                cancel=lambda: (checkpoint() or False),
            )
            category = "database"
            databases.append({"root": entry.root, "path": entry.path, **details})
        else:
            with source.open("rb") as reader, target.open("xb") as writer:
                opened = os.fstat(reader.fileno())
                if (opened.st_dev, opened.st_ino) != (before.st_dev, before.st_ino):
                    raise MigrationError("migration_source_changed", path=entry.path)
                copied = 0
                digest = hashlib.sha256()
                while data := reader.read(CHUNK):
                    checkpoint()
                    budget.checkpoint()
                    copied += len(data)
                    if copied > entry.size:
                        raise MigrationError(
                            "migration_source_changed", path=entry.path
                        )
                    writer.write(data)
                    digest.update(data)
                writer.flush()
                os.fsync(writer.fileno())
            hashes[entry.path] = digest.hexdigest()
            after = source.stat()
            if (before.st_ino, before.st_mtime_ns, before.st_size) != (
                after.st_ino,
                after.st_mtime_ns,
                after.st_size,
            ) or copied != entry.size:
                raise MigrationError("migration_source_changed", path=entry.path)
        info = target.stat()
        total += info.st_size
        if total > limits.expanded:
            raise MigrationError("migration_expanded_limit")
        captured.append(
            FileEntry(entry.root, entry.path, category, info.st_size, info.st_mtime_ns)
        )
    current = scan_project(project, max_entries=limits.entries, checkpoint=check)
    if stable_files(initial) != stable_files(current):
        raise MigrationError("migration_source_changed")
    if native_endpoint is not None:
        from .native_replacement import snapshot_native

        relative = f"migration/database/{native_endpoint.engine}.backup"
        payload = contained_path(destination, relative)
        payload.parent.mkdir(parents=True, exist_ok=True)
        details = asyncio.run(
            snapshot_native(
                native_endpoint,
                destination.parent,
                payload,
                budget=budget,
                checkpoint=check,
                maximum=limits.expanded - total,
            )
        )
        primary_database = {
            "root": "project",
            "path": relative,
            "engine": native_endpoint.engine,
        }
        databases.append({**primary_database, **details})
        info = payload.stat()
        captured.append(
            FileEntry("project", relative, "database", info.st_size, info.st_mtime_ns)
        )
        # File and database snapshots must describe the same stopped instance.
        if stable_files(initial) != stable_files(
            scan_project(project, max_entries=limits.entries, checkpoint=check)
        ):
            raise MigrationError("migration_source_changed")
    # Native dumps may take hours; verify file content after the database snapshot.
    for relative, identity in identities.items():
        check()
        info = contained_path(project, relative, regular=True).stat()
        if (info.st_dev, info.st_ino, info.st_ctime_ns) != identity:
            raise MigrationError("migration_source_changed", path=relative)
        if (
            file_hash(contained_path(project, relative, regular=True), check)
            != hashes[relative]
        ):
            raise MigrationError("migration_source_changed", path=relative)
        if (
            relative in wal_hashes
            and wal_hash(project / relative) != wal_hashes[relative]
        ):
            raise MigrationError("migration_source_changed", path=relative)
    metadata = environment_inventory(
        revision=initial.boundary.revision,
        project=project if options.dependencies else None,
        paths=None if options.dependencies else [],
        checkpoint=check,
    )
    if not options.dependencies:
        metadata.pop("distributions", None)
        metadata["dependency_inventory_scope"] = "not_selected"
    metadata.update(
        snapshot_at=time.time(),
        source_boundary=initial.boundary.source,
        exclusions=initial.excluded,
        databases=databases,
        primary_database=primary_database,
        plugin_installations=plugin_descriptors(project),
        replacement_selection=export_selection(
            options.categories, [entry.path for entry in files]
        ).public(),
    )
    return captured, metadata


def export_offline(
    project: Path,
    destination: Path,
    *,
    options: ExportOptions,
    password: bytes | None = None,
    limits: Limits = Limits(),
) -> dict:
    if not password and not options.plaintext_confirmed:
        raise MigrationError("migration_plaintext_confirmation_required")
    project = project.absolute()
    store = TaskStore(project)
    budget = MigrationBudget.start(options.budget_seconds)
    with InstanceLease(project, role="migration") as lease:
        assert_offline(project)
        if store.active():
            raise MigrationError("migration_operation_in_progress", status=409)
        public_options = {**asdict(options), "categories": sorted(options.categories)}
        job = store.create_job("local-console", "export", public_options)
        identity = job["id"]
        store.reserve(identity)

        def checkpoint():
            budget.checkpoint()
            if store.read("jobs", identity)["cancel_requested"]:
                raise MigrationError("migration_cancelled")

        try:
            store.transition(identity, "preparing")
            store.transition(identity, "quiescing")
            store.transition(identity, "snapshotting")
            snapshot = store.path("jobs", identity).parent / "snapshot"
            files, metadata = capture_snapshot(
                project,
                snapshot,
                options=options,
                lease=lease,
                budget=budget.phase(),
                limits=limits,
                checkpoint=checkpoint,
            )
            store.transition(identity, "resuming")
            # Offline export never starts a process that was not running before it.
            store.transition(identity, "compressing")
            result = build_archive(
                snapshot,
                files,
                destination,
                metadata=metadata,
                password=password,
                plaintext_confirmed=options.plaintext_confirmed,
                limits=limits,
                checkpoint=checkpoint,
                publish=lambda src, dst, outcome: store.publish_export(
                    identity, src, dst, outcome
                ),
            )
            return {
                "task": store.public(store.read("jobs", identity)),
                "archive": result,
            }
        except BaseException as error:
            code = (
                error.code
                if isinstance(error, MigrationError)
                else "migration_export_failed"
            )
            stage = "cancelled" if code == "migration_cancelled" else "failed"
            if (store.path("jobs", identity).parent / "publication.json").exists():
                stage = "recovery_required"
                # A published file may already exist even when the final record
                # write failed. Recovery must verify it, never call this cancelled.
                if store.read("jobs", identity)["stage"] == "completed":
                    raise
            store.transition(identity, stage, error_code=code)
            raise
