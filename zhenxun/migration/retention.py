from __future__ import annotations

import os
import re

from filelock import FileLock, Timeout

from zhenxun.utils.atomic_json import write_json_locked

from .errors import MigrationError
from .paths import contained_path, is_link
from .tasks import TaskStore


def _remove(root, path, budget):
    path = contained_path(root, path.relative_to(root).as_posix())
    if not path.exists():
        return
    if path.is_file():
        path.unlink()
        return
    count = 0
    directories = []
    for current, dirs, names in os.walk(path, topdown=True, followlinks=False):
        for name in dirs:
            target = type(path)(current) / name
            if is_link(target):
                raise MigrationError("migration_link_forbidden")
            contained_path(root, target.relative_to(root).as_posix())
            directories.append(target)
        for name in names + dirs:
            budget.checkpoint()
            count += 1
            if count > 600_000:
                raise MigrationError("migration_retention_work_limit")
            target = contained_path(
                root, (type(path)(current) / name).relative_to(root).as_posix()
            )
            if is_link(target):
                raise MigrationError("migration_link_forbidden")
            if not target.is_dir():
                target.unlink()
    for directory in reversed(directories):
        budget.checkpoint()
        contained_path(root, directory.relative_to(root).as_posix()).rmdir()
    path.rmdir()


def expire_assets(project, *, budget):
    store = TaskStore(project)
    store._initialize()
    removed, deferred, errors = [], [], []
    try:
        with FileLock(str(store.root / "analysis.lock"), timeout=0):
            if store.active():
                return {"removed": [], "deferred": ["active_migration"], "errors": []}
            protected_uploads = set()
            for item in _records(store, "preflights"):
                if (
                    item.get("job_id")
                    or item.get("expires_at", 0) > store.clock()
                    or item.get("stage") == "failed"
                ):
                    protected_uploads.add(item["upload_id"])
            for kind in ("uploads", "jobs"):
                for record in _records(store, kind):
                    budget.checkpoint()
                    identity = record["id"]
                    if record.get(
                        "expires_at", float("inf")
                    ) > store.clock() or record.get("assets_expired_at"):
                        continue
                    if kind == "uploads":
                        if identity in protected_uploads:
                            continue
                        names = ["archive.zx.part"]
                    elif record["stage"] not in {"completed", "partial"}:
                        continue
                    elif record["action"] == "export":
                        names = ["export.zx", "snapshot", "package.zx"]
                    else:
                        names = ["restore-stage", "database-stage"]
                    directory = store.path(kind, identity).parent
                    try:
                        with FileLock(
                            str(
                                directory
                                / ("upload.lock" if kind == "uploads" else "asset.lock")
                            ),
                            timeout=0,
                        ):
                            for name in names:
                                _remove(directory, directory / name, budget)
                            record["assets_expired_at"] = store.clock()
                            write_json_locked(store.path(kind, identity), record)
                            removed.append(identity)
                    except Timeout:
                        deferred.append(identity)
                    except (MigrationError, OSError) as error:
                        if (
                            isinstance(error, MigrationError)
                            and error.code == "migration_budget_exhausted"
                        ):
                            raise
                        errors.append(
                            {
                                "id": identity,
                                "code": getattr(
                                    error, "code", "migration_retention_cleanup_failed"
                                ),
                            }
                        )
    except Timeout:
        deferred.append("analysis_busy")
    result = {"removed": removed, "deferred": deferred, "errors": errors}
    write_json_locked(store.root / "retention-result.json", result)
    return result


def _records(store, kind):
    directory = contained_path(store.root, kind)
    if not directory.exists():
        return
    for count, child in enumerate(directory.iterdir()):
        if count >= 10_000:
            raise MigrationError("migration_task_history_limit")
        if (
            re.fullmatch(r"[a-f0-9]{32}", child.name)
            and (child / "record.json").is_file()
        ):
            yield store.read(kind, child.name)
