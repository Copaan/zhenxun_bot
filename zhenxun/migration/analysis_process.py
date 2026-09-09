from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path
import subprocess
import sys
import time
import uuid

from zhenxun.utils.atomic_json import read_json_locked

from .access import private_directory
from .errors import MigrationError
from .paths import contained_path
from .tasks import MigrationBudget


async def run_analysis(
    project: Path,
    operation: str,
    *,
    identity=None,
    private=None,
    options=None,
    budget=None,
) -> dict:
    from zhenxun.services.lifecycle.deadline import ShutdownBudget
    from zhenxun.services.lifecycle.kernel import LifecycleKernel
    from zhenxun.services.lifecycle.launcher import LauncherSupervisor

    budget = budget or MigrationBudget.start(3600)
    budget.checkpoint()
    invocation = uuid.uuid4().hex
    directory = private_directory(
        contained_path(project, f"migration/analysis/{invocation}")
    )
    encoded = json.dumps(
        {
            "invocation": invocation,
            "operation": operation,
            "identity": identity,
            "private": private or {},
            "options": options or {},
            "remaining_seconds": max(0, budget.deadline - time.monotonic()),
        }
    ).encode()
    if len(encoded) > 64 * 1024:
        raise MigrationError("migration_private_input_limit", status=413)
    supervisor = LauncherSupervisor(LifecycleKernel())
    supervisor.boot_id = uuid.uuid4().hex
    process = None
    sender = None

    def spawn():
        return subprocess.Popen(
            [sys.executable, "-m", "zhenxun.migration.analysis_worker"],
            cwd=project,
            stdin=subprocess.PIPE,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
            env={
                **os.environ,
                "PYTHONPATH": str(Path(__file__).resolve().parents[2]),
                "PYTHONDONTWRITEBYTECODE": "1",
            },
        )

    try:
        process = await supervisor.start_process(
            "migration_analysis", spawn, completion_expected=True
        )
        while True:
            budget.checkpoint()
            sender = asyncio.create_task(
                asyncio.to_thread(
                    process.communicate,
                    encoded,
                    timeout=min(1, max(0.001, budget.deadline - time.monotonic())),
                )
            )
            try:
                await asyncio.shield(sender)
                break
            except subprocess.TimeoutExpired:
                # Popen retains partially written input across communicate calls.
                encoded = None
        budget.checkpoint()
        path = contained_path(directory, "result.json", regular=True)
        if path.stat().st_size > 1024 * 1024:
            raise MigrationError("migration_analysis_result_limit")
        result = read_json_locked(path, None)
        if (
            process.returncode
            or not isinstance(result, dict)
            or result.get("invocation") != invocation
        ):
            raise MigrationError("migration_analysis_result_unconfirmed")
        if "error" in result:
            raise MigrationError(result["error"], status=result.get("status", 500))
        return result["result"]
    finally:
        supervisor.shutdown_deadline = ShutdownBudget(budget.deadline)
        try:
            if process is not None:
                await supervisor.stop_process(process)
        finally:
            await supervisor.shutdown()
            if sender is not None:
                await asyncio.gather(sender, return_exceptions=True)
