from __future__ import annotations

import asyncio
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import os
from pathlib import Path
import platform
import re
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


@dataclass(slots=True)
class StartupDiagnostic:
    diagnostic_id: str
    occurred_at: str
    stage: str
    source_type: str
    source_id: str
    code: str
    error_type: str
    summary: str
    details: dict[str, Any]


_WINDOWS_PATH_PATTERN = re.compile(r"(?i)(?:[a-z]:[\\/]|\\\\)[^\r\n\t\"'<>|]+")
_POSIX_PATH_PATTERN = re.compile(r"(?<![:\w])/(?:[^/\s]+/)+[^\s:]+")
_SECRET_PATTERN = re.compile(
    r"(?i)\b(token|api[_-]?key|secret|password|authorization)\b\s*[:=]\s*[^\s,;]+"
)


def _sanitize_diagnostic_text(value: object, *, limit: int = 240) -> str:
    text = " ".join(str(value or "").split())
    text = _WINDOWS_PATH_PATTERN.sub("[path]", text)
    text = _POSIX_PATH_PATTERN.sub("[path]", text)
    text = _SECRET_PATTERN.sub(lambda match: f"{match.group(1)}=[redacted]", text)
    return text[:limit]


def _diagnostic_payload(
    error: BaseException | None, code: str
) -> tuple[str, str, dict[str, Any]]:
    if error is None:
        return "StartupDegradation", f"启动能力降级：{code}", {}
    error_type = type(error).__name__
    if isinstance(error, ModuleNotFoundError):
        module = _sanitize_diagnostic_text(error.name or "unknown", limit=120)
        return error_type, f"缺少 Python 模块：{module}", {"missing_module": module}
    if isinstance(error, SyntaxError):
        filename = Path(str(error.filename or "")).name or None
        details = {
            key: value
            for key, value in {
                "filename": filename,
                "line": error.lineno,
                "column": error.offset,
            }.items()
            if value is not None
        }
        summary = _sanitize_diagnostic_text(error.msg or "Python 语法错误")
        return error_type, summary, details
    errors = getattr(error, "errors", None)
    if callable(errors) and error_type == "ValidationError":
        fields: list[str] = []
        kinds: list[str] = []
        try:
            for item in errors(include_url=False, include_input=False)[:20]:
                location = ".".join(str(part) for part in item.get("loc", ()))
                if location:
                    fields.append(_sanitize_diagnostic_text(location, limit=120))
                if item.get("type"):
                    kinds.append(_sanitize_diagnostic_text(item["type"], limit=80))
        except (TypeError, ValueError):
            pass
        details = {"fields": fields, "validation_types": kinds}
        summary = "配置校验失败"
        if fields:
            summary += f"：{', '.join(fields[:5])}"
        return error_type, summary, details
    return error_type, f"{error_type} 导致启动能力降级，请使用诊断ID查看本地日志", {}


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
        self._diagnostics: list[StartupDiagnostic] = []
        self._lifecycle_diagnostics: list[dict[str, Any]] = []
        self._current_operation: dict[str, Any] | None = None
        self._load_planner: Any | None = None
        self._management_complete = False
        self._server_bound = False
        self._setup_required = False
        self._last_persist_monotonic = 0.0
        self._lock = RLock()
        self._server_bound_event: asyncio.Event | None = None
        self._runtime_event: asyncio.Event | None = None
        self._warmup_event: asyncio.Event | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._state_writer = None

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

    def enter_setup_mode(self) -> None:
        """Keep the minimal management surface alive until first setup restarts."""
        with self._lock:
            self._setup_required = True
            skipped = {
                "state": "skipped",
                "duration_ms": 0.0,
                "reason_code": "initial_setup_required",
            }
            self._stages.setdefault("runtime", dict(skipped))
            self._stages.setdefault("warmup", dict(skipped))
            if self._management_complete and self._server_bound:
                self._state = "management_ready"
                if self._finished_monotonic is None:
                    self._finished_monotonic = time.monotonic()
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
        error: BaseException | None = None,
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
                error=error,
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
            self._log_degraded_reason(reason or {}, error=error)

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

    def mark_plugins_recovered(self, owners: set[str], generation: int) -> None:
        with self._lock:
            for diagnostic in self._diagnostics:
                if (
                    diagnostic.source_type == "plugin"
                    and diagnostic.source_id in owners
                ):
                    diagnostic.details.update(
                        recovered=True, recovered_generation=generation
                    )
            for reason in self._degraded_reasons:
                if (
                    reason.get("source_type") == "plugin"
                    and reason.get("source_id") in owners
                ):
                    reason.update(recovered=True, recovered_generation=generation)

    def record_error(
        self,
        stage: str,
        error_code: str,
        *,
        source_type: str = "operation",
        source_id: str | None = None,
        display_name: str | None = None,
        error: BaseException | None = None,
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
                error=error,
            )
        reason = dict(self._degraded_reasons[-1]) if added else None
        if added:
            self._log_degraded_reason(reason or {}, error=error)
            self._persist_throttled(force=True)

    def _append_degraded_reason_locked(
        self,
        stage: str,
        error_code: str,
        *,
        source_type: str,
        source_id: str | None,
        display_name: str | None,
        error: BaseException | None,
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
        occurred_at = datetime.now(timezone.utc).isoformat()
        diagnostic_id = f"diag-{uuid.uuid4().hex[:12]}"
        error_type, summary, details = _diagnostic_payload(error, str(error_code))
        reason["diagnostic_id"] = diagnostic_id
        reason["occurred_at"] = occurred_at
        self._degraded_reasons.append(reason)
        self._diagnostics.append(
            StartupDiagnostic(
                diagnostic_id=diagnostic_id,
                occurred_at=occurred_at,
                stage=str(stage),
                source_type=str(source_type or "operation"),
                source_id=normalized_source_id,
                code=str(error_code),
                error_type=error_type,
                summary=summary,
                details=details,
            )
        )
        self._diagnostics = self._diagnostics[-100:]
        return True

    @staticmethod
    def _log_degraded_reason(
        reason: dict[str, str], *, error: BaseException | None = None
    ) -> None:
        try:
            from zhenxun.services.log import logger

            logger.error(
                "启动能力降级 | "
                f"diagnostic_id={reason.get('diagnostic_id', 'unknown')} | "
                f"stage={reason['stage']} | source={reason['source_type']}:"
                f"{reason['source_id']} | code={reason['code']}",
                "Startup",
                e=error if isinstance(error, Exception) else None,
            )
        except Exception:
            pass

    def record_lifecycle_event(self, event: dict[str, Any]) -> None:
        component = event.get("component") or {}
        component_id = str(component.get("component_id") or "unknown")
        state = str(component.get("state") or event.get("event") or "unknown")
        action = (event.get("operation") or {}).get("action")
        if action in {"stop", "health_check", "rebuild"}:
            if state in {"failed", "degraded"}:
                self._record_lifecycle_diagnostic(event)
            return
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

    def _record_lifecycle_diagnostic(self, event: dict[str, Any]) -> None:
        component = event.get("component") or {}
        operation = event.get("operation") or {}
        diagnostic = (component.get("metadata") or {}).get("stop_diagnostic") or {}
        source = _sanitize_diagnostic_text(
            component.get("component_id", "unknown"), limit=120
        )
        code = _sanitize_diagnostic_text(
            component.get("error_code", "component_failed"), limit=120
        )
        operation_id = operation.get("operation_id") or diagnostic.get("operation_id")
        identity = (source, code, operation.get("action"), operation_id)
        with self._lock:
            if any(
                tuple(item["identity"]) == identity
                for item in self._lifecycle_diagnostics
            ):
                return
            item = {
                "identity": list(identity),
                "diagnostic_id": diagnostic.get("diagnostic_id")
                or f"diag-{uuid.uuid4().hex[:12]}",
                "occurred_at": diagnostic.get("occurred_at")
                or datetime.now(timezone.utc).isoformat(),
                "action": operation.get("action"),
                "component_id": source,
                "code": code,
                "failure_stage": _sanitize_diagnostic_text(
                    event.get("failure_stage") or "unknown"
                ),
                "resources": {
                    _sanitize_diagnostic_text(key, limit=100): value
                    for key, value in (diagnostic.get("resources") or {}).items()
                },
            }
            self._lifecycle_diagnostics.append(item)
            self._lifecycle_diagnostics = self._lifecycle_diagnostics[-100:]
        try:
            from zhenxun.services.log import logger

            label = (
                "生命周期关闭异常" if item["action"] == "stop" else "生命周期运行异常"
            )
            logger.error(
                f"{label} | diagnostic_id={item['diagnostic_id']} | "
                f"component={source} | stage={item['failure_stage']} | "
                f"code={code} | resources={item['resources']}",
                "Lifecycle",
            )
        except Exception:
            pass

    def mark_server_bound(self) -> None:
        with self._lock:
            self._server_bound = True
            if self._management_complete and self._state == "starting":
                self._state = "management_ready"
                if self._setup_required and self._finished_monotonic is None:
                    self._finished_monotonic = time.monotonic()
            self._set_event(self._server_bound_event)
        self.persist()

    async def wait_server_bound(self) -> None:
        await self._event("server").wait()

    async def wait_runtime_ready(self) -> None:
        await self._event("runtime").wait()

    async def wait_final_available(self) -> bool:
        """Wait until startup reaches its final user-facing usable state."""
        with self._lock:
            setup_required = self._setup_required
        if setup_required:
            await self.wait_server_bound()
            with self._lock:
                return (
                    self._management_complete
                    and self._server_bound
                    and self._state == "management_ready"
                )
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
                "launcher_boot_id": os.getenv("ZHENXUN_LAUNCHER_BOOT_ID", ""),
                "startup_id": os.getenv("ZHENXUN_WORKER_STARTUP_ID", ""),
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
                "persistence": self.persistence_status(),
                "server_bound": self._server_bound,
                "operating_mode": (
                    "management_only"
                    if self._state == "failed"
                    else "setup_only"
                    if self._setup_required
                    else "normal"
                ),
                "accepts_bot_events": self.runtime_ready,
                "failure_terminal": self._state == "failed",
                "setup_required": self._setup_required,
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
            result["diagnostics"] = [asdict(item) for item in self._diagnostics]
            result["lifecycle_diagnostics"] = [
                dict(item) for item in self._lifecycle_diagnostics
            ]
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
        if self._state_writer is not None:
            self._state_writer.mark_dirty()
            return
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            pass
        else:
            from zhenxun.services.lifecycle.diagnostics import LifecycleStateWriter

            self._state_writer = LifecycleStateWriter(_REPORT_PATH, self.snapshot)
            self._state_writer.mark_dirty()
            return
        try:
            write_json_locked(_REPORT_PATH, self.snapshot())
        except OSError:
            pass

    def persistence_status(self) -> dict[str, Any]:
        if self._state_writer is None:
            return {"mode": "synchronous", "pending": False}
        return {"mode": "managed", **self._state_writer.status()}

    async def finish_persistence(self, timeout: float) -> bool:
        if self._state_writer is None:
            return True
        return await self._state_writer.close(timeout)


startup_coordinator = StartupCoordinator()

try:
    from zhenxun.services.lifecycle import lifecycle_kernel

    lifecycle_kernel.add_observer(startup_coordinator.record_lifecycle_event)
except Exception:
    pass

__all__ = ["StartupCoordinator", "StartupDiagnostic", "startup_coordinator"]
