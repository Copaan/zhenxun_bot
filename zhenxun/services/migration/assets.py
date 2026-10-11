from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime
import logging
from pathlib import Path
import re
import tempfile

from filelock import FileLock, Timeout
from starlette.responses import FileResponse

from zhenxun.utils.atomic_json import mutate_json_locked, read_json_locked

from .archive import file_hash
from .errors import MigrationError
from .paths import contained_path


def asset_lock(store, identity):
    return FileLock(
        str(store.path("jobs", identity).parent / "asset.lock"),
        timeout=0,
        thread_local=False,
    )


def export_work_lock(store, identity):
    """Protect private snapshots and compression files independently of downloads."""
    return FileLock(
        str(store.path("jobs", identity).parent / "export-work.lock"),
        timeout=0,
        thread_local=False,
    )


def export_destination(store, job):
    """Resolve a stable package name relative to this instance's project."""
    if job.get("export_prepared"):
        return publication_path(
            store,
            job["id"],
            {
                "schema": 2,
                "job_id": job["id"],
                "destination": job["export_file"]["path"],
            },
        )
    stamp = datetime.fromtimestamp(job["created_at"]).strftime("%Y%m%d-%H%M%S")
    name = f"zhenxun-{stamp}-{job['id'][:8]}.zx"
    return contained_path(store.project, name)


def publication_path(store, identity, receipt):
    """Resolve only the recorded instance-owned publication, including v1 receipts."""
    if not isinstance(receipt, dict) or receipt.get("job_id") != identity:
        raise MigrationError("migration_publication_receipt_invalid")
    destination = receipt.get("destination")
    if not isinstance(destination, str):
        raise MigrationError("migration_publication_receipt_invalid")
    if receipt.get("schema") == 2:
        if not re.fullmatch(
            rf"zhenxun-[0-9]{{8}}-[0-9]{{6}}-{identity[:8]}\.zx", destination
        ):
            raise MigrationError("migration_publication_receipt_invalid")
        return contained_path(store.project, destination)
    if receipt.get("schema") != 1:
        raise MigrationError("migration_publication_receipt_invalid")
    path = Path(destination)
    # Legacy automatic exports were always private task assets.
    expected = store.path("jobs", identity).parent / "export.zx"
    if path.absolute() != expected.absolute():
        raise MigrationError("migration_publication_conflict", status=409)
    return contained_path(expected.parent, expected.name)


def verify_publication(path, receipt, *, checkpoint=lambda: None, digest=True):
    if (
        not isinstance(receipt.get("result"), dict)
        or not {"size", "sha256"} <= receipt["result"].keys()
        or not {"device", "inode"} <= receipt.keys()
    ):
        raise MigrationError("migration_publication_receipt_invalid")
    if not path.is_file():
        raise MigrationError("migration_export_expired", status=410)
    info = path.stat()
    result = receipt["result"]
    if (
        (info.st_dev, info.st_ino) != (receipt["device"], receipt["inode"])
        or info.st_size != result["size"]
        or (digest and file_hash(path, checkpoint) != result["sha256"])
    ):
        raise MigrationError("migration_publication_conflict", status=409)


def publication_exists(store, identity):
    """Keep a possibly committed artifact for reconciliation, not failed cleanup."""
    receipt = read_json_locked(
        store.path("jobs", identity).parent / "publication.json", None
    )
    if receipt is None:
        return False
    try:
        return publication_path(store, identity, receipt).exists()
    except MigrationError:
        return True


def _record_cleanup(store, identity, result, *, removed):
    def update(value):
        value["export_cleanup"] = result
        if removed:
            value["export_file"] = {
                **value.get("export_file", {}),
                "state": "removed",
                "downloadable": False,
                "removed_at": store.clock(),
            }
        return dict(value)

    mutate_json_locked(store.path("jobs", identity), None, update)


def cleanup_export(store, identity, *, remove_archive, budget):
    """Clean a reaped export's assets while keeping its outcome and evidence."""
    from .retention import _remove

    removed = False
    result = {"state": "cleaned", "remove_archive": remove_archive}
    try:
        with export_work_lock(store, identity):
            directory = store.path("jobs", identity).parent
            if remove_archive:
                with asset_lock(store, identity):
                    job = store.read("jobs", identity)
                    if job.get("export_file", {}).get("state") != "removed":
                        receipt = read_json_locked(directory / "publication.json", None)
                        if receipt is not None:
                            path = publication_path(store, identity, receipt)
                            if path.exists():
                                verify_publication(
                                    path, receipt, checkpoint=budget.checkpoint
                                )
                                path.unlink()
                        removed = True
            for name in ("snapshot", "package.zx"):
                _remove(directory, directory / name, budget)
            for temporary in directory.glob(".zx-export-*"):
                _remove(directory, temporary, budget)
    except Exception as error:
        result = {
            "state": "failed",
            "remove_archive": remove_archive,
            "exception_type": type(error).__name__,
            "code": "migration_download_busy"
            if isinstance(error, Timeout)
            else getattr(error, "code", "migration_export_cleanup_failed"),
        }
    try:
        _record_cleanup(store, identity, result, removed=removed)
    except Exception as error:
        result = {
            **result,
            "state": "failed",
            "code": "migration_export_cleanup_record_failed",
        }
        logging.getLogger(__name__).warning(
            "Export cleanup receipt unavailable: job=%s exception=%s",
            identity,
            type(error).__name__,
        )
    return result


def prepare_export(store, identity, *, budget):
    """Remove earlier owned packages before a newly reserved export starts."""
    try:
        with FileLock(str(store.root / "export-files.lock"), timeout=0):
            return _prepare_export(store, identity, budget=budget)
    except Timeout:
        raise MigrationError("migration_operation_in_progress", status=409) from None


def _prepare_export(store, identity, *, budget):
    from .retention import _records

    job = store.read("jobs", identity)
    if job.get("export_prepared"):
        return export_destination(store, job)
    for old in _records(store, "jobs"):
        budget.checkpoint()
        if old["id"] == identity or old["action"] != "export":
            continue
        if old["stage"] not in {"completed", "failed", "cancelled", "partial"}:
            raise MigrationError("migration_operation_in_progress", status=409)
        result = cleanup_export(store, old["id"], remove_archive=True, budget=budget)
        if result["state"] != "cleaned":
            raise MigrationError(result["code"], status=409)
    path = export_destination(store, job)
    if path.exists():
        raise MigrationError("migration_output_exists", status=409)
    try:
        with tempfile.TemporaryFile(dir=store.project) as probe:
            probe.write(b"export")
            probe.flush()
    except OSError as error:
        raise MigrationError("migration_export_directory_unwritable") from error

    def prepared(value):
        value["export_prepared"] = True
        value["export_file"] = {
            "policy_version": 2,
            "path": path.relative_to(store.project).as_posix(),
            "name": path.name,
            "state": "pending",
            "downloadable": False,
        }
        return dict(value)

    mutate_json_locked(store.path("jobs", identity), None, prepared)
    return path


@contextmanager
def lease_package(store, path):
    """Hold the export lease while copying a discovered package into an upload."""
    from .retention import _records

    for job in _records(store, "jobs"):
        if job["action"] != "export":
            continue
        directory = store.path("jobs", job["id"]).parent
        receipt = read_json_locked(directory / "publication.json", None)
        if receipt is None:
            continue
        try:
            owned = publication_path(store, job["id"], receipt)
        except MigrationError:
            continue
        if owned.absolute() == path.absolute():
            try:
                with asset_lock(store, job["id"]):
                    verify_publication(path, receipt)
                    yield
                return
            except Timeout:
                raise MigrationError("migration_download_busy", status=409) from None
    yield


class LeasedDownload(FileResponse):
    """Keep the asset lease until ASGI has finished or the client disconnected."""

    def __init__(self, store, identity):
        job = store.read("jobs", identity)
        if job["action"] != "export" or job["stage"] != "completed":
            raise MigrationError("migration_export_not_ready", status=409)
        if job.get("export_file", {}).get("state") == "removed":
            raise MigrationError("migration_export_expired", status=410)
        if (
            job.get("export_file", {}).get("policy_version") != 2
            and job.get("expires_at", 0) <= store.clock()
        ):
            raise MigrationError("migration_export_expired", status=410)
        directory = store.path("jobs", identity).parent
        self.lease = asset_lock(store, identity)
        try:
            self.lease.acquire()
        except Timeout:
            raise MigrationError("migration_download_busy", status=409) from None
        try:
            receipt = read_json_locked(directory / "publication.json", None)
            path = publication_path(store, identity, receipt)
            if not path.is_file():
                raise MigrationError("migration_export_expired", status=410)
            verify_publication(path, receipt, digest=False)
            super().__init__(
                path,
                filename=path.name,
                media_type="application/octet-stream",
                headers={
                    "Cache-Control": "no-store",
                    "X-Content-Type-Options": "nosniff",
                },
            )
        except BaseException:
            self.lease.release()
            raise

    async def __call__(self, scope, receive, send):
        try:
            await super().__call__(scope, receive, send)
        finally:
            self.lease.release()
