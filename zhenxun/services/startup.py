from __future__ import annotations

import asyncio
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import os
from pathlib import Path
import platform
import sys
from threading import RLock
import time
from typing import Any, Literal
import uuid

from zhenxun.utils.atomic_json import write_json_locked

StartupState = Literal[
    "starting",
    "management_ready",
    "runtime_ready",
    "warmup_ready",
    "degraded",
    "failed",
]
StartupStage = Literal["management", "runtime", "warmup"]

_REPORT_PATH = Path("data/runtime/startup-profile-v1.json")


@dataclass(slots=True)
class OperationRecord:
    name: str
    stage: str
    state: str
    duration_ms: float
    priority: int | None = None
    error_code: str | None = None
    details: dict[str, Any] | None = None


class StartupCoordinator:
    def __init__(self) -> None:
        self.boot_id = uuid.uuid4().hex
        self.pid = os.getpid()
        self.started_at = datetime.now(timezone.utc).isoformat()
        self._started_monotonic = time.monotonic()
        self._finished_monotonic: float | None = None
        self._state: StartupState = "starting"
        self._stage_started: dict[str, float] = {}
        self._stage_completed_wall: dict[str, float] = {}
        self._stages: dict[str, dict[str, Any]] = {}
        self._operations: list[OperationRecord] = []
        self._errors: list[dict[str, str]] = []
        self._degraded_reasons: list[dict[str, str]] = []
        self._current_operation: dict[str, Any] | None = None
        self._load_planner: Any | None = None
        self._management_complete = False
        self._server_bound = False
        self._last_persist_monotonic = 0.0
        self._lock = RLock()
        self._server_bound_event: asyncio.Event | None = None
        self._runtime_event: asyncio.Event | None = None
        self._warmup_event: asyncio.Event | None = None
        self._loop: asyncio.AbstractEventLoop | None = None

    @property
    def state(self) -> StartupState:
        with self._lock:
            return self._state

    @property
    def runtime_ready(self) -> bool:
        return self.state in {"runtime_ready", "warmup_ready", "degraded"}

    def begin_stage(self, stage: StartupStage) -> None:
        with self._lock:
            self._stage_started[stage] = time.monotonic()
            self._stages[stage] = {"state": "running", "duration_ms": None}
        self.persist()

    def finish_stage(self, stage: StartupStage) -> None:
        with self._lock:
            started = self._stage_started.get(stage, time.monotonic())
            self._stages[stage] = {
                "state": "completed",
                "duration_ms": round((time.monotonic() - started) * 1000, 2),
            }
            self._stage_completed_wall[stage] = time.time()
            if stage == "management":
                self._management_complete = True
                if self._server_bound:
                    self._state = "management_ready"
            elif stage == "runtime":
                self._state = "runtime_ready"
                self._set_event(self._runtime_event)
            else:
                self._state = "warmup_ready" if not self._errors else "degraded"
                if self._finished_monotonic is None:
                    self._finished_monotonic = time.monotonic()
                self._set_event(self._warmup_event)
        self.persist()

    def fail_stage(
        self,
        stage: StartupStage,
        error_code: str,
        *,
        fatal: bool = True,
        source_type: str = "stage",
        source_id: str | None = None,
        display_name: str | None = None,
    ) -> None:
        added = False
        with self._lock:
            started = self._stage_started.get(stage, time.monotonic())
            self._stages[stage] = {
                "state": "failed",
                "duration_ms": round((time.monotonic() - started) * 1000, 2),
                "error_code": error_code,
            }
            self._stage_completed_wall[stage] = time.time()
            error = {"stage": stage, "code": error_code}
            if error not in self._errors:
                self._errors.append(error)
            added = self._append_degraded_reason_locked(
                stage,
                error_code,
                source_type=source_type,
                source_id=source_id,
                display_name=display_name,
            )
            self._state = "failed" if fatal else "degraded"
            if self._finished_monotonic is None:
                self._finished_monotonic = time.monotonic()
            if stage == "runtime":
                self._set_event(self._runtime_event)
            if stage == "warmup":
                self._set_event(self._warmup_event)
        reason = dict(self._degraded_reasons[-1]) if added else None
        self.persist()
        if added:
            self._log_degraded_reason(reason or {})

    def record_operation(
        self,
        name: str,
        stage: str,
        state: str,
        duration_ms: float,
        *,
        priority: int | None = None,
        error_code: str | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        record = OperationRecord(
            name=name,
            stage=stage,
            state=state,
            duration_ms=round(duration_ms, 2),
            priority=priority,
            error_code=error_code,
            details=details,
        )
        with self._lock:
            self._operations.append(record)
            self._operations = self._operations[-1000:]
            if self._current_operation and self._current_operation.get("name") == name:
                self._current_operation = None
        self._persist_throttled(force=duration_ms >= 250 or state == "failed")

    def begin_operation(self, name: str, stage: str, **details: Any) -> None:
        with self._lock:
            self._current_operation = {
                "name": name,
                "stage": stage,
                "started_at": datetime.now(timezone.utc).isoformat(),
                "details": details,
            }
        self._persist_throttled()

    def set_load_plan(self, planner: Any) -> None:
        with self._lock:
            self._load_planner = planner
        self._persist_throttled(force=True)

    def record_error(
        self,
        stage: str,
        error_code: str,
        *,
        source_type: str = "operation",
        source_id: str | None = None,
        display_name: str | None = None,
    ) -> None:
        added = False
        with self._lock:
            item = {"stage": stage, "code": error_code}
            if item not in self._errors:
                self._errors.append(item)
            added = self._append_degraded_reason_locked(
                stage,
                error_code,
                source_type=source_type,
                source_id=source_id,
                display_name=display_name,
            )
        reason = dict(self._degraded_reasons[-1]) if added else None
        if added:
            self._log_degraded_reason(reason or {})
            self._persist_throttled(force=True)

    def _append_degraded_reason_locked(
        self,
        stage: str,
        error_code: str,
        *,
        source_type: str,
        source_id: str | None,
        display_name: str | None,
    ) -> bool:
        normalized_source_id = str(source_id or "").strip() or "unknown"
        reason = {
            "stage": str(stage),
            "source_type": str(source_type or "operation"),
            "source_id": normalized_source_id,
            "code": str(error_code),
            "display_name": str(display_name or normalized_source_id),
        }
        identity = (
            reason["stage"],
            reason["source_type"],
            reason["source_id"],
            reason["code"],
        )
        if any(
            (
                item["stage"],
                item["source_type"],
                item["source_id"],
                item["code"],
            )
            == identity
            for item in self._degraded_reasons
        ):
            return False
        self._degraded_reasons.append(reason)
        return True

    @staticmethod
    def _log_degraded_reason(reason: dict[str, str]) -> None:
        try:
            from zhenxun.services.log import logger

            logger.warning(
                "启动能力降级 | "
                f"stage={reason['stage']} | source={reason['source_type']}:"
                f"{reason['source_id']} | code={reason['code']}",
                "Startup",
            )
        except Exception:
            pass

    def record_lifecycle_event(self, event: dict[str, Any]) -> None:
        component = event.get("component") or {}
        component_id = str(component.get("component_id") or "unknown")
        state = str(component.get("state") or event.get("event") or "unknown")
        with self._lock:
            self._current_operation = (
                {
                    "name": component_id,
                    "stage": str(component.get("stage") or "runtime"),
                    "state": state,
                }
                if state in {"starting", "quiescing", "stopping"}
                else None
            )
        if state in {"degraded", "failed"}:
            error_code = str(
                component.get("error_code")
                or event.get("error_code")
                or f"component_{state}"
            )
            self.record_error(
                str(component.get("stage") or "runtime"),
                error_code,
                source_type="component",
                source_id=component_id,
                display_name=component_id,
            )
        self._persist_throttled(force=state in {"failed", "degraded"})

    def mark_server_bound(self) -> None:
        with self._lock:
            self._server_bound = True
            if self._management_complete and self._state == "starting":
                self._state = "management_ready"
            self._set_event(self._server_bound_event)
        self.persist()

    async def wait_server_bound(self) -> None:
        await self._event("server").wait()

    async def wait_runtime_ready(self) -> None:
        await self._event("runtime").wait()

    async def wait_final_available(self) -> bool:
        """Wait until startup reaches its final user-facing usable state."""
        await self.wait_runtime_ready()
        with self._lock:
            if self._stages.get("runtime", {}).get("state") != "completed":
                return False
        await self._event("warmup").wait()
        with self._lock:
            return self._state in {"warmup_ready", "degraded"}

    def _event(self, kind: str) -> asyncio.Event:
        with self._lock:
            self._loop = asyncio.get_running_loop()
            if kind == "server":
                if self._server_bound_event is None:
                    self._server_bound_event = asyncio.Event()
                    if self._server_bound:
                        self._server_bound_event.set()
                return self._server_bound_event
            if kind == "runtime":
                if self._runtime_event is None:
                    self._runtime_event = asyncio.Event()
                    if self._stages.get("runtime", {}).get("state") in {
                        "completed",
                        "failed",
                    }:
                        self._runtime_event.set()
                return self._runtime_event
            if self._warmup_event is None:
                self._warmup_event = asyncio.Event()
                if self._stages.get("warmup", {}).get("state") in {
                    "completed",
                    "failed",
                }:
                    self._warmup_event.set()
            return self._warmup_event

    def _set_event(self, event: asyncio.Event | None) -> None:
        if event is not None:
            loop = self._loop
            try:
                current_loop = asyncio.get_running_loop()
            except RuntimeError:
                current_loop = None
            if loop is not None and current_loop is not loop and not loop.is_closed():
                loop.call_soon_threadsafe(event.set)
            else:
                event.set()

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            operations = [asdict(item) for item in self._operations]
            slow = sorted(
                operations, key=lambda item: float(item["duration_ms"]), reverse=True
            )[:20]
            result = {
                "boot_id": self.boot_id,
                "pid": self.pid,
                "state": self._state,
                "started_at": self.started_at,
                "elapsed_ms": round(
                    (
                        (self._finished_monotonic or time.monotonic())
                        - self._started_monotonic
                    )
                    * 1000,
                    2,
                ),
                "stages": dict(self._stages),
                "errors": list(self._errors),
                "degraded_reasons": list(self._degraded_reasons),
                "slow_operations": slow,
                "current_operation": dict(self._current_operation)
                if self._current_operation
                else None,
                "server_bound": self._server_bound,
                "operating_mode": (
                    "management_only" if self._state == "failed" else "normal"
                ),
                "accepts_bot_events": self.runtime_ready,
                "failure_terminal": self._state == "failed",
                "python": {
                    "version": platform.python_version(),
                    "implementation": platform.python_implementation(),
                },
            }
            if self._load_planner is not None:
                result["load_plan"] = self._load_planner.summary()
            try:
                from zhenxun.services.lifecycle import lifecycle_kernel

                lifecycle = lifecycle_kernel.status()
                result["lifecycle"] = {
                    key: value
                    for key, value in lifecycle.items()
                    if key != "components"
                }
            except Exception:
                pass
            try:
                from zhenxun.services.message_load import (
                    db_unhealthy_reason,
                    is_db_unhealthy,
                )

                cache_module = sys.modules.get("zhenxun.services.cache.runtime_cache")
                watchdog_module = sys.modules.get(
                    "zhenxun.services.db_context.watchdog"
                )
                result["database_health"] = {
                    "unhealthy": is_db_unhealthy(),
                    "reason": db_unhealthy_reason(),
                    "runtime_cache": (
                        cache_module.refresh_coordinator_snapshot()
                        if cache_module is not None
                        else {"running": False, "state": "not_loaded"}
                    ),
                    "watchdog": (
                        watchdog_module.watchdog_snapshot()
                        if watchdog_module is not None
                        else {"running": False, "state": "not_loaded"}
                    ),
                }
            except Exception:
                pass
            for stage, completed_at in self._stage_completed_wall.items():
                for key, prefix in (
                    ("ZHENXUN_LAUNCHER_STARTED_AT", "launcher"),
                    ("ZHENXUN_WORKER_SPAWNED_AT", "worker"),
                ):
                    try:
                        started_at = float(os.getenv(key, ""))
                    except ValueError:
                        continue
                    result[f"{prefix}_{stage}_ready_ms"] = round(
                        (completed_at - started_at) * 1000, 2
                    )
            return result

    def report(self) -> dict[str, Any]:
        result = self.snapshot()
        with self._lock:
            result["operations"] = [asdict(item) for item in self._operations]
            if self._load_planner is not None:
                result["load_plan"] = self._load_planner.summary(detail=True)
        try:
            from zhenxun.services.lifecycle import lifecycle_kernel

            result["lifecycle"] = lifecycle_kernel.status()
        except Exception:
            pass
        return result

    def _persist_throttled(self, *, force: bool = False) -> None:
        now = time.monotonic()
        with self._lock:
            if not force and now - self._last_persist_monotonic < 0.25:
                return
            self._last_persist_monotonic = now
        self.persist()

    def persist(self) -> None:
        try:
            write_json_locked(_REPORT_PATH, self.snapshot())
        except OSError:
            pass


startup_coordinator = StartupCoordinator()

try:
    from zhenxun.services.lifecycle import lifecycle_kernel

    lifecycle_kernel.add_observer(startup_coordinator.record_lifecycle_event)
except Exception:
    pass

__all__ = ["StartupCoordinator", "startup_coordinator"]
