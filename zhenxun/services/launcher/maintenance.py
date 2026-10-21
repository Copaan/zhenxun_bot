"""launcher 在 worker 启动前串行执行的维护子进程（事务应用、更新、依赖同步）"""

import asyncio
from functools import partial
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
from typing import Any
import uuid

from zhenxun.utils.atomic_json import read_json_locked, write_json_locked

from .supervisor import launcher_log, launcher_supervisor


def execute(action: str) -> None:
    from .lease import validate_delegation

    if os.getenv("ZHENXUN_MAINTENANCE_CHILD") != "1":
        raise RuntimeError("maintenance_requires_launcher")
    launcher = os.environ.get("ZHENXUN_LAUNCHER_PID", "")
    identity = os.environ.get("ZHENXUN_INSTANCE_LEASE_ID")
    if not launcher.isdecimal() or not identity:
        raise RuntimeError("maintenance_requires_launcher_identity")
    validate_delegation(Path.cwd(), int(launcher), identity)

    interruption_requested = False

    def interrupted(_signum, _frame):
        nonlocal interruption_requested
        if interruption_requested:
            return
        interruption_requested = True
        raise KeyboardInterrupt

    for name in ("SIGINT", "SIGTERM", "SIGBREAK"):
        if hasattr(signal, name):
            signal.signal(getattr(signal, name), interrupted)
    result: dict[str, Any] = {
        "startup_id": os.environ["ZHENXUN_MAINTENANCE_STARTUP_ID"]
    }
    try:
        if action == "nonebot-apply":
            from zhenxun.services.nonebot_store.runtime import apply_pending_transaction

            result["value"] = apply_pending_transaction()
        elif action == "nonebot-resync":
            from zhenxun.services.nonebot_store.runtime import stage_environment_resync

            result["value"] = stage_environment_resync()
        elif action == "source-apply":
            from zhenxun.services.plugin_store.plugin_store_transaction import (
                apply_pending_transaction,
            )

            result["value"] = apply_pending_transaction()
        elif action == "update-apply":
            from zhenxun.services.update.update_service import apply_pending_update

            result["value"] = apply_pending_update(Path.cwd())
        elif action == "sync-core":
            from zhenxun.services.nonebot_store.dependencies import guard_orm_sync
            from zhenxun.services.update.update_service import _sync_dependencies

            guard_orm_sync()
            _sync_dependencies()
            result["value"] = True
        elif action == "plugin-requirements":
            from zhenxun.services.nonebot_store.manual import stage_requirement_files

            paths = json.loads(os.environ.get("ZHENXUN_MAINTENANCE_PATHS", "[]"))
            result["value"] = stage_requirement_files([Path(path) for path in paths])
        elif action == "restore-apply":
            from zhenxun.services.migration.restore import apply_pending

            result["value"] = apply_pending()
        elif action == "verify-dependencies":
            from zhenxun.services.nonebot_store.environment import (
                verify_effective_environment,
            )

            verify_effective_environment()
            result["value"] = True
        else:
            raise ValueError("maintenance_action_invalid")
        result["completed"] = True
    except BaseException as error:
        result["error_code"] = getattr(error, "code", type(error).__name__)
        result["details"] = getattr(error, "details", None)
        raise
    finally:
        write_json_locked(Path(os.environ["ZHENXUN_MAINTENANCE_RESULT_PATH"]), result)


async def run(action: str, paths: list[str] | None = None) -> bool:
    if launcher_supervisor.shutdown_deadline is not None:
        raise asyncio.CancelledError("maintenance_startup_interrupted")
    startup_id = uuid.uuid4().hex
    path = Path("data/runtime/launcher-maintenance-result-v1.json").resolve()
    process = await launcher_supervisor.start_process(
        "maintenance",
        partial(
            subprocess.Popen,
            [sys.executable, "-m", "zhenxun.cli", "run-maintenance", action],
            env={
                **os.environ,
                "ZHENXUN_MAINTENANCE_CHILD": "1",
                "ZHENXUN_LAUNCHER_PID": str(os.getpid()),
                "ZHENXUN_MAINTENANCE_STARTUP_ID": startup_id,
                "ZHENXUN_MAINTENANCE_RESULT_PATH": str(path),
                "ZHENXUN_MAINTENANCE_PATHS": json.dumps(paths or []),
            },
            creationflags=subprocess.CREATE_NEW_PROCESS_GROUP if os.name == "nt" else 0,
        ),
        startup_id=startup_id,
    )
    try:
        while process.poll() is None:
            if launcher_supervisor.shutdown_deadline is not None:
                raise asyncio.CancelledError("maintenance_interrupted")
            await asyncio.sleep(0.1)
        result = read_json_locked(path, {})
        if (
            process.returncode != 0
            or result.get("startup_id") != startup_id
            or not result.get("completed")
        ):
            if (
                action == "verify-dependencies"
                and result.get("startup_id") == startup_id
            ):
                from zhenxun.services.nonebot_store.dependencies import (
                    DependencyAnalysisError,
                )

                raise DependencyAnalysisError(
                    result.get("error_code", "dependency_verification_failed"),
                    details=result.get("details"),
                )
            raise RuntimeError("maintenance_failed")
        return bool(result.get("value"))
    finally:
        await launcher_supervisor.stop_process(process, allow_force=True)


async def sync_dependencies(paths: list[str]) -> None:
    """按变更文件同步核心依赖或插件依赖，结果写入依赖同步状态

    核心依赖同步后依赖层随之重新同步；依赖层需要重建时，由重建后的启动验证
    代替依赖检查。

    参数:
        paths: 发生变化的依赖文件
    """
    from zhenxun.services.nonebot_store.environment import REQUIREMENT_FILES
    from zhenxun.services.nonebot_store.storage import (
        dependency_sync_status,
        save_dependency_sync_status,
    )

    context = {
        key: value
        for key, value in dependency_sync_status().items()
        if key not in {"code", "details"}
    }
    save_dependency_sync_status({**context, "status": "running", "paths": paths})
    try:
        resynced = False
        if any(path in {"pyproject.toml", "uv.lock"} for path in paths):
            await run("sync-core")
            resynced = await run("nonebot-resync")
        requirement_paths = [
            path for path in paths if Path(path).name in REQUIREMENT_FILES
        ]
        if requirement_paths:
            await run("plugin-requirements", requirement_paths)
        if not resynced:
            await run("verify-dependencies")
    except Exception as error:
        launcher_log(
            "dependency sync failed; restarting worker with the "
            f"current environment ({type(error).__name__})"
        )
        save_dependency_sync_status(
            {
                **context,
                "status": "failed",
                "code": getattr(error, "code", "dependency_sync_failed"),
                "details": getattr(error, "details", None),
                "paths": paths,
            }
        )
    else:
        save_dependency_sync_status({**context, "status": "succeeded", "paths": paths})
