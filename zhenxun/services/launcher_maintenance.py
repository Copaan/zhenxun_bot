"""Serialized pre-worker mutations run outside the launcher event loop."""

import asyncio
from functools import partial
import os
from pathlib import Path
import signal
import subprocess
import sys
import uuid

from zhenxun.utils.atomic_json import read_json_locked, write_json_locked


def execute(action: str) -> None:
    from zhenxun.migration.lease import validate_delegation

    if os.getenv("ZHENXUN_MAINTENANCE_CHILD") != "1":
        raise RuntimeError("maintenance_requires_launcher")
    launcher = os.environ.get("ZHENXUN_LAUNCHER_PID", "")
    identity = os.environ.get("ZHENXUN_INSTANCE_LEASE_ID")
    if not launcher.isdecimal() or not identity:
        raise RuntimeError("maintenance_requires_launcher_identity")
    validate_delegation(Path.cwd(), int(launcher), identity=identity)

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
    result = {"startup_id": os.environ["ZHENXUN_MAINTENANCE_STARTUP_ID"]}
    try:
        if action == "nonebot-apply":
            from zhenxun.nonebot_store.runtime import apply_pending_transaction

            result["value"] = apply_pending_transaction()
        elif action == "source-apply":
            from zhenxun.plugin_store_transaction import apply_pending_transaction

            result["value"] = apply_pending_transaction()
        elif action == "update-apply":
            from zhenxun.update_service import apply_pending_update

            result["value"] = apply_pending_update(Path.cwd())
        elif action == "sync-core":
            from zhenxun.update_service import _sync_dependencies

            _sync_dependencies(preserve_extras=True)
            result["value"] = True
        else:
            raise ValueError("maintenance_action_invalid")
        result["completed"] = True
    except BaseException as error:
        result["error_code"] = type(error).__name__
        raise
    finally:
        write_json_locked(Path(os.environ["ZHENXUN_MAINTENANCE_RESULT_PATH"]), result)


async def run(action: str) -> bool:
    from zhenxun.services.lifecycle.launcher import launcher_supervisor

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
            },
            creationflags=subprocess.CREATE_NEW_PROCESS_GROUP if os.name == "nt" else 0,
        ),
        startup_id=startup_id,
    )
    try:
        while process.poll() is None:
            launcher_supervisor._publish_processes()
            if launcher_supervisor.shutdown_deadline is not None:
                raise asyncio.CancelledError("maintenance_interrupted")
            await asyncio.sleep(0.1)
        result = read_json_locked(path, {})
        if (
            process.returncode != 0
            or result.get("startup_id") != startup_id
            or not result.get("completed")
        ):
            raise RuntimeError("maintenance_failed")
        return bool(result.get("value"))
    except BaseException:
        launcher_supervisor.update_metadata(
            maintenance={
                "action": action,
                "state": "recovery_required",
                "code": "maintenance_interrupted_or_failed",
            }
        )
        raise
    finally:
        await launcher_supervisor.stop_process(process)
