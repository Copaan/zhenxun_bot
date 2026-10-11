from __future__ import annotations

import asyncio
import os
from pathlib import Path
import signal
import time
import traceback

from .control import request_control
from .errors import MigrationError
from .validation import ValidationIngress, authorized_validation, validation_gate

_request: dict | None = None


def failure_diagnostic(error: Exception) -> dict:
    """Keep exception locations without credentials, source lines or local values."""
    return {
        **(
            error.public()
            if isinstance(error, MigrationError)
            else {"code": "migration_validation_internal_error"}
        ),
        "exception_type": type(error).__name__,
        "errno": getattr(error, "errno", None),
        "winerror": getattr(error, "winerror", None),
        "frames": [
            {
                "file": Path(frame.filename).name,
                "line": frame.lineno,
                "function": frame.name,
            }
            for frame in traceback.extract_tb(error.__traceback__)[-12:]
        ],
    }


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


def register(driver, *, managed_status: dict | None = None) -> None:
    if _request is None:
        return
    import nonebot

    from zhenxun.services.lifecycle import lifecycle_kernel
    from zhenxun.services.startup import startup_coordinator
    from zhenxun.services.startup_load import startup_load_planner
    from zhenxun.utils.atomic_json import read_json_locked, write_json_locked
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
    runtime_receipt = (
        TaskStore(project).path("jobs", request["job_id"]).parent
        / "validation-runtime.json"
    )
    runtime_phase = "startup"
    runtime_started = False
    runtime_database_revision = None
    runtime_file_mutations = []
    runtime_evidence = {}

    def record_runtime_started(database_revision, file_mutations):
        nonlocal runtime_started, runtime_database_revision, runtime_file_mutations
        payload = {
            **identity,
            "version": 2,
            "state": "running",
            "runtime_started": True,
            "database_revision": database_revision,
            "file_mutations": file_mutations,
            "plan_revision": runtime_evidence.get("plan_revision"),
            "generation": runtime_evidence.get("generation"),
            "recorded_at": time.time(),
        }
        try:
            write_json_locked(runtime_receipt, payload)
        except Exception:
            return False
        runtime_started = True
        runtime_database_revision = database_revision
        runtime_file_mutations = file_mutations
        return True

    def record_runtime_failure(error):
        diagnostic = failure_diagnostic(error)
        try:
            previous = read_json_locked(runtime_receipt, None)
            if (
                isinstance(previous, dict)
                and previous.get("state") == "failed"
                and previous.get("startup_id") == identity["startup_id"]
                and previous.get("worker_boot_id") == identity["worker_boot_id"]
            ):
                return
            write_json_locked(
                runtime_receipt,
                {
                    **identity,
                    "version": 2,
                    "state": "failed",
                    "runtime_started": runtime_started,
                    "database_revision": runtime_database_revision,
                    "file_mutations": runtime_file_mutations,
                    "plan_revision": runtime_evidence.get("plan_revision"),
                    "generation": runtime_evidence.get("generation"),
                    "phase": runtime_phase,
                    "diagnostic": diagnostic,
                    "failed_at": time.time(),
                },
            )
        except Exception:
            pass

    async def check_validation() -> dict:
        nonlocal runtime_phase, runtime_evidence
        runtime_phase = "startup"
        status = startup_coordinator.snapshot()
        base = {**identity, "business_opened": validation_gate.business_allowed}
        if status["state"] == "failed" or status.get("setup_required"):
            runtime_phase = "startup"
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
            runtime_phase = "resource_check"
            raise MigrationError("migration_validation_resources_unresolved")
        failed = sorted(startup_load_planner.failed_plugins)
        if failed or (managed_status or {}).get("failed"):
            runtime_phase = "plugin_check"
            raise MigrationError("migration_selected_plugin_initialization_failed")
        job = TaskStore(project).read("jobs", request["job_id"])
        if mapping := job["options"].get("database"):
            require_target_connection(
                project, mapping, request.get("private", {}).get("database") or {}
            )
        generation = activated_generation(request["job_id"])
        from .verification import (
            verify_restore,
            verify_runtime_configuration,
            verify_runtime_database,
            verify_runtime_dependencies,
        )

        runtime_phase = "restore_verify"
        verify_runtime_configuration(project)
        evidence = await asyncio.to_thread(verify_restore, project, request["job_id"])
        if generation["generation"] != evidence["generation"]:
            raise MigrationError("migration_dependency_generation_changed")
        runtime_evidence = evidence
        file_mutations = evidence.get("configuration_mutations", [])
        if not record_runtime_started(
            evidence.get("database_revision"), file_mutations
        ):
            runtime_phase = "runtime_receipt"
            raise MigrationError("migration_validation_receipt_unavailable")
        runtime_phase = "database_verify"
        database = await verify_runtime_database()
        runtime_phase = "dependency_verify"
        await asyncio.to_thread(verify_runtime_dependencies, project, request["job_id"])
        runtime_phase = "validated"
        return {
            **base,
            "state": "validated",
            "generation": generation["generation"],
            "failed_plugins": failed,
            "evidence": evidence,
            "database_evidence": database,
            "checks": {
                "configuration": not status.get("setup_required", False),
                "database": database["ready"],
                "core": status["state"] in {"warmup_ready", "degraded"},
                "plugins": not failed and not (managed_status or {}).get("failed"),
                "dependencies": evidence["generation"] == generation["generation"],
                "listener": status["server_bound"],
                "files": evidence["schema"] == 2,
            },
        }

    async def monitor():
        sequence = 0
        try:
            report = None
            while time.monotonic() < deadline:
                if report is None or report["state"] == "starting":
                    try:
                        report = await check_validation()
                        if report.get("state") == "failed":
                            record_runtime_failure(
                                MigrationError(
                                    report.get(
                                        "error_code",
                                        "migration_core_initialization_failed",
                                    )
                                )
                            )
                    except Exception as error:
                        diagnostic = failure_diagnostic(error)
                        record_runtime_failure(error)
                        report = {
                            **identity,
                            "state": "failed",
                            "business_opened": False,
                            "error_code": diagnostic["code"],
                            "diagnostic": diagnostic,
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
        except Exception as error:
            record_runtime_failure(error)
            try:
                write_json_locked(
                    TaskStore(project).path("jobs", request["job_id"]).parent
                    / "validation-control-failure.json",
                    {**identity, "diagnostic": failure_diagnostic(error)},
                )
            except Exception:
                pass
            validation_gate.stop()
            signal.raise_signal(signal.SIGINT)

    @PriorityLifecycle.on_startup(
        priority=-90,
        stage="management",
        component_id="migration:validation_control",
        pass_context=True,
    )
    async def start_monitor(context):
        context.spawn_task(
            monitor(), name="migration-validation-control", degrade_on_error=False
        )

    @driver.on_shutdown
    async def stop_gate():
        validation_gate.stop()
