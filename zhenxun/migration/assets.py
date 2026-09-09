from __future__ import annotations

from filelock import FileLock, Timeout
from starlette.responses import FileResponse

from zhenxun.utils.atomic_json import read_json_locked

from .errors import MigrationError
from .paths import contained_path


class LeasedDownload(FileResponse):
    """Keep the asset lease until ASGI has finished or the client disconnected."""

    def __init__(self, store, identity):
        job = store.read("jobs", identity)
        if job["action"] != "export" or job["stage"] != "completed":
            raise MigrationError("migration_export_not_ready", status=409)
        if job.get("expires_at", 0) <= store.clock():
            raise MigrationError("migration_export_expired", status=410)
        directory = store.path("jobs", identity).parent
        self.lease = FileLock(
            str(directory / "asset.lock"), timeout=0, thread_local=False
        )
        try:
            self.lease.acquire()
        except Timeout:
            raise MigrationError("migration_download_busy", status=409) from None
        try:
            receipt = read_json_locked(directory / "publication.json", None)
            path = contained_path(directory, "export.zx", regular=True)
            info = path.stat()
            if (
                not isinstance(receipt, dict)
                or receipt.get("job_id") != identity
                or (info.st_dev, info.st_ino)
                != (receipt.get("device"), receipt.get("inode"))
                or info.st_size != job["progress"].get("size")
            ):
                raise MigrationError("migration_publication_conflict", status=409)
            super().__init__(
                path,
                filename=f"instance-{identity}.zx",
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
