from __future__ import annotations

import asyncio
import os
from pathlib import Path
import signal
import time

from .control import request_control
from .errors import MigrationError
from .validation import ValidationIngress, authorized_validation, validation_gate

_request: dict | None = None


def authorize(project: Path, identity: str) -> None:
    global _request
    deadline = time.monotonic() + 10
    while True:
        try:
            _request = authorized_validation(project, identity)
            break
        except MigrationError as error:
            if (
                error.code != "migration_validation_identity_unconfirmed"
                or time.monotonic() >= deadline
            ):
                raise
            time.sleep(0.02)
    validation_gate.configure(identity, _request["startup_id"])
    from .configuration import bound_environment
    from .tasks import TaskStore

    overrides = bound_environment(
        TaskStore(project).read("jobs", identity)["options"],
        _request.get("private", {}).get("configuration"),
    )
    for key in list(os.environ):
        if key.upper() in overrides:
            del os.environ[key]
    os.environ.update(overrides)


def install(driver) -> None:
    if _request is None:
        return
    validation_gate.install(driver)


def register(driver) -> None:
    if _request is None:
        return
    import nonebot

    from zhenxun.services.lifecycle import lifecycle_kernel
    from zhenxun.services.startup import startup_coordinator
    from zhenxun.services.startup_load import startup_load_planner
    from zhenxun.utils.manager.priority_manager import PriorityLifecycle

    from .generation import activated_generation
    from .maintenance_app import ManagementSnapshot, create_maintenance_app
    from .restore_phases import require_target_connection
    from .tasks import TaskStore

    project = Path.cwd()
    request = _request
    identity = {
        "job_id": request["job_id"],
        "task_id": request["job_id"],
        "startup_id": request["startup_id"],
        "launcher_boot_id": request["launcher_boot_id"],
        "pid": os.getpid(),
        "worker_boot_id": startup_coordinator.boot_id,
        "boot_id": startup_coordinator.boot_id,
    }
    management = create_maintenance_app(
        project, ManagementSnapshot.parse(request["management"]), identity
    )
    nonebot.get_app().add_middleware(ValidationIngress, management=management)
    deadline = time.monotonic() + request["remaining_seconds"]

    def check_validation() -> dict:
        status = startup_coordinator.snapshot()
        base = {**identity, "business_opened": validation_gate.business_allowed}
        if status["state"] == "failed" or status.get("setup_required"):
            return {
                **base,
                "state": "failed",
                "error_code": "migration_core_initialization_failed",
            }
        if (
            status["state"] not in {"warmup_ready", "degraded"}
            or not status["server_bound"]
        ):
            return {**base, "state": "starting"}
        if lifecycle_kernel.status()["recovery_required"]:
            raise MigrationError("migration_validation_resources_unresolved")
        failed = sorted(startup_load_planner.failed_plugins)
        if any(startup_load_planner.is_core_plugin(p) for p in failed):
            raise MigrationError("migration_core_initialization_failed")
        job = TaskStore(project).read("jobs", request["job_id"])
        if mapping := job["options"].get("database"):
            require_target_connection(
                project, mapping, request.get("private", {}).get("database") or {}
            )
        generation = activated_generation(request["job_id"])
        return {
            **base,
            "state": "validated",
            "generation": generation["generation"],
            "failed_plugins": failed,
            "checks": {
                key: True
                for key in (
                    "configuration",
                    "database",
                    "core",
                    "plugins",
                    "dependencies",
                    "listener",
                )
            },
        }

    async def monitor():
        sequence = 0
        try:
            report = None
            while time.monotonic() < deadline:
                if report is None or report["state"] == "starting":
                    try:
                        report = check_validation()
                    except MigrationError as error:
                        report = {
                            **identity,
                            "state": "failed",
                            "business_opened": False,
                            "error_code": error.code,
                        }
                response = await asyncio.to_thread(
                    request_control,
                    project,
                    "validation_state",
                    {
                        "task_id": request["job_id"],
                        "startup_id": request["startup_id"],
                        "sequence": sequence,
                        "state": report,
                    },
                )
                sequence += 1
                if (
                    response.get("publication_ready")
                    and response.get("decision", {}).get("mode") == "online"
                ):
                    from zhenxun.configs.config import Config

                    from .mutation import resuming_startup

                    with resuming_startup(project):
                        Config.reload(strict=True)
                        await validation_gate.promote(response["decision"])
                    report = {**identity, "state": "promoted", "business_opened": True}
                    await asyncio.to_thread(
                        request_control,
                        project,
                        "validation_state",
                        {
                            "task_id": request["job_id"],
                            "startup_id": request["startup_id"],
                            "sequence": sequence,
                            "state": report,
                        },
                    )
                    return
                await asyncio.sleep(0.2)
            raise MigrationError("migration_budget_exhausted")
        except asyncio.CancelledError:
            raise
        except BaseException:
            validation_gate.stop()
            signal.raise_signal(signal.SIGINT)

    @PriorityLifecycle.on_startup(
        priority=-90,
        stage="management",
        component_id="migration:validation_control",
        scope="worker",
        pass_context=True,
    )
    async def start_monitor(context):
        context.spawn_task(
            monitor(), name="migration-validation-control", persistent=False
        )

    @driver.on_shutdown
    async def stop_gate():
        validation_gate.stop()
