from __future__ import annotations

import asyncio
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import os
from pathlib import Path
import platform
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
        self._state: StartupState = "starting"
        self._stage_started: dict[str, float] = {}
        self._stage_completed_wall: dict[str, float] = {}
        self._stages: dict[str, dict[str, Any]] = {}
        self._operations: list[OperationRecord] = []
        self._errors: list[dict[str, str]] = []
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
                self._set_event(self._warmup_event)
        self.persist()

    def fail_stage(
        self, stage: StartupStage, error_code: str, *, fatal: bool = True
    ) -> None:
        with self._lock:
            started = self._stage_started.get(stage, time.monotonic())
            self._stages[stage] = {
                "state": "failed",
                "duration_ms": round((time.monotonic() - started) * 1000, 2),
                "error_code": error_code,
            }
            self._stage_completed_wall[stage] = time.time()
            self._errors.append({"stage": stage, "code": error_code})
            self._state = "failed" if fatal else "degraded"
            if stage == "runtime":
                self._set_event(self._runtime_event)
            if stage == "warmup":
                self._set_event(self._warmup_event)
        self.persist()

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

    def record_error(self, stage: str, error_code: str) -> None:
        with self._lock:
            item = {"stage": stage, "code": error_code}
            if item not in self._errors:
                self._errors.append(item)

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
                    (time.monotonic() - self._started_monotonic) * 1000, 2
                ),
                "stages": dict(self._stages),
                "errors": list(self._errors),
                "slow_operations": slow,
                "current_operation": dict(self._current_operation)
                if self._current_operation
                else None,
                "server_bound": self._server_bound,
                "python": {
                    "version": platform.python_version(),
                    "implementation": platform.python_implementation(),
                },
            }
            if self._load_planner is not None:
                result["load_plan"] = self._load_planner.summary()
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

__all__ = ["StartupCoordinator", "startup_coordinator"]
