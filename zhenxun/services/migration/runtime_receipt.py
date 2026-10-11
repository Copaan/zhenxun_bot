from __future__ import annotations

from pathlib import Path
import time

from zhenxun.utils.atomic_json import read_json_locked, write_json_locked

from .archive import file_hash
from .paths import contained_path

_SCRIPTS = (
    "zhenxun/builtin_plugins/init/manager.py",
    "zhenxun/services/db_context/__init__.py",
)
CONFIGURATION_PATHS = frozenset(
    {"data/config.yaml", "data/configs/plugins2config.yaml"}
)


def begin_validation_window(project: Path, directory: Path, authorization: dict):
    """Bind startup mutations to the applied plan and actual database file."""
    plan = read_json_locked(directory / "restore-plan.json", {})
    database = plan.get("database") or {}
    metadata = database.get("database", {})
    target = None
    if metadata.get("engine") == "sqlite":
        path = contained_path(project, metadata["target_path"], regular=True)
        info = path.stat()
        target = {
            "path": metadata["target_path"],
            "identity": [info.st_dev, info.st_ino],
            "revision": metadata.get("source_content_revision"),
        }
    scripts = {
        relative: file_hash(contained_path(project, relative, regular=True))
        for relative in _SCRIPTS
    }
    write_json_locked(
        directory / "validation-window.json",
        {
            **authorization,
            "version": 1,
            "phase": "before_worker_start",
            "started_at": time.time(),
            "plan_revision": plan.get("revision"),
            "target": target,
            "scripts": scripts,
        },
    )


def failed_runtime_receipt(directory: Path) -> dict | None:
    """Return only an uncommitted, released validation worker's failure evidence."""
    if (directory / "restore-commit.json").exists():
        return None
    runtime = read_json_locked(directory / "validation-runtime.json", {})
    state = read_json_locked(directory / "validation-state.json", {})
    window = read_json_locked(directory / "validation-window.json", {})
    authorization = read_json_locked(directory / "validation-authorization.json", {})
    release = read_json_locked(directory / "validation-release.json", {})
    if (
        runtime.get("version") != 2
        or runtime.get("state") != "failed"
        or not runtime.get("runtime_started")
        or state.get("state") != "failed"
        or state.get("business_opened") is not False
        or release.get("result") != "confirmed"
        or release.get("forced") is not False
        or release.get("process_tree_released") is not True
        or not window.get("plan_revision")
        or runtime.get("plan_revision") != window.get("plan_revision")
    ):
        return None
    for key in ("job_id", "startup_id", "launcher_boot_id"):
        if not runtime.get(key) or any(
            item.get(key) != runtime[key] for item in (state, window, authorization)
        ):
            return None
    boot_id = runtime.get("worker_boot_id")
    released_identity = release.get("identity", {})
    if (
        not boot_id
        or state.get("worker_boot_id") != boot_id
        or released_identity.get("boot_id") != boot_id
        or released_identity.get("startup_id") != runtime["startup_id"]
        or released_identity.get("pid") != runtime.get("pid")
        or released_identity.get("launcher_boot_id") != runtime["launcher_boot_id"]
    ):
        return None
    return {**runtime, "window": window}


def configuration_rollback_mutations(directory: Path, plan: dict) -> dict:
    """Use verified configuration bytes; later writes never inherit authorization."""
    receipt = failed_runtime_receipt(directory)
    if not receipt or receipt["plan_revision"] != plan.get("revision"):
        return {}
    actions = {item["path"]: item for item in plan["files"]["actions"]}
    result = {}
    for mutation in receipt.get("file_mutations", []):
        path = mutation.get("path")
        action = actions.get(path)
        if (
            path in CONFIGURATION_PATHS
            and action
            and mutation.get("expected_sha256") == action.get("after")
            and isinstance(mutation.get("current_sha256"), str)
            and len(mutation["current_sha256"]) == 64
            and len(mutation.get("identity", [])) == 2
        ):
            result[path] = {
                **mutation,
                "startup_id": receipt["startup_id"],
                "worker_boot_id": receipt["worker_boot_id"],
            }
    return result
