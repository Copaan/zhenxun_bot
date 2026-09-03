from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Coroutine
from copy import deepcopy
from dataclasses import dataclass, field
from datetime import datetime, timezone
import inspect
import os
from pathlib import Path
from threading import RLock
from typing import Any, Literal
import uuid

from strenum import StrEnum

from zhenxun.utils.atomic_json import read_json_locked, write_json_locked

from .kernel import LifecycleContext, LifecycleError, LifecycleKernel, lifecycle_kernel

OperationRecoveryPolicy = Literal["resume", "restart", "rollback", "discard"]
CheckpointCallback = Callable[[], Any | Awaitable[Any]]
RecoveryCallback = Callable[[dict[str, Any]], Coroutine[Any, Any, Any] | None]

_DEFAULT_STATE_PATH = Path("data/runtime/lifecycle-operations-v2.json")
_TERMINAL_STATES = {"completed", "failed", "cancelled"}
_SENSITIVE_KEY_PARTS = (
    "token",
    "secret",
    "password",
    "cookie",
    "api_key",
    "apikey",
)


class OperationState(StrEnum):
    QUEUED = "queued"
    RUNNING = "running"
    CHECKPOINTING = "checkpointing"
    CHECKPOINTED = "checkpointed"
    COMMIT_CRITICAL = "commit_critical"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    RECOVERY_REQUIRED = "recovery_required"


@dataclass(slots=True)
class OperationRecord:
    operation_id: str
    kind: str
    owner_component_id: str
    owner_boot_id: str | None = None
    state: OperationState = OperationState.QUEUED
    progress: int = 0
    public_input: dict[str, Any] = field(default_factory=dict)
    checkpoint: dict[str, Any] = field(default_factory=dict)
    recovery_policy: OperationRecoveryPolicy = "restart"
    phase: str = "queued"
    error_code: str | None = None
    scope_id: str | None = None
    created_at: str = field(default_factory=lambda: _now())
    updated_at: str = field(default_factory=lambda: _now())
    completed_at: str | None = None

    def public_dict(self) -> dict[str, Any]:
        return {
            "operation_id": self.operation_id,
            "kind": self.kind,
            "owner_component_id": self.owner_component_id,
            "owner_boot_id": self.owner_boot_id,
            "state": self.state.value,
            "progress": self.progress,
            "public_input": deepcopy(self.public_input),
            "checkpoint": deepcopy(self.checkpoint),
            "recovery_policy": self.recovery_policy,
            "phase": self.phase,
            "error_code": self.error_code,
            "scope_id": self.scope_id,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "completed_at": self.completed_at,
        }


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _public_value(value: Any) -> Any:
    if value is None or isinstance(value, bool | int | float):
        return value
    if isinstance(value, str):
        return value[:500]
    if isinstance(value, dict):
        return {
            str(key)[:100]: (
                "configured"
                if any(part in str(key).lower() for part in _SENSITIVE_KEY_PARTS)
                else _public_value(item)
            )
            for key, item in list(value.items())[:100]
        }
    if isinstance(value, list | tuple | set):
        return [_public_value(item) for item in list(value)[:100]]
    return type(value).__name__


class OperationRegistry:
    def __init__(
        self,
        kernel: LifecycleKernel,
        state_path: Path | None = None,
    ) -> None:
        self._kernel = kernel
        self._state_path = state_path
        self._lock = RLock()
        self._records: dict[str, OperationRecord] = {}
        self._tasks: dict[str, asyncio.Task[Any]] = {}
        self._scopes: dict[str, LifecycleContext] = {}
        self._checkpoint_callbacks: dict[str, CheckpointCallback] = {}
        self._recovery_handlers: dict[str, RecoveryCallback] = {}
        self._context: LifecycleContext | None = None
        self._accepting = True
        self._load()

    @property
    def accepting(self) -> bool:
        return self._accepting

    def bind(self, context: LifecycleContext) -> None:
        self._context = context
        self._accepting = True

    def register_recovery_handler(self, kind: str, callback: RecoveryCallback) -> None:
        self._recovery_handlers[kind] = callback

    def start(
        self,
        kind: str,
        coroutine: Coroutine[Any, Any, Any],
        *,
        operation_id: str | None = None,
        owner_component_id: str = "management:operations",
        public_input: dict[str, Any] | None = None,
        recovery_policy: OperationRecoveryPolicy = "restart",
        checkpoint: CheckpointCallback | None = None,
        name: str | None = None,
        _resume: bool = False,
    ) -> tuple[OperationRecord, asyncio.Task[Any]]:
        if not self._accepting or self._context is None:
            coroutine.close()
            raise LifecycleError("operation_registry_not_accepting")
        operation_id = operation_id or uuid.uuid4().hex
        with self._lock:
            existing = self._records.get(operation_id)
            if existing:
                resumable = existing.state in {
                    OperationState.CHECKPOINTED,
                    OperationState.RECOVERY_REQUIRED,
                }
                if not _resume or not resumable:
                    coroutine.close()
                    raise LifecycleError(f"operation_duplicate:{operation_id}")
            record = OperationRecord(
                operation_id=operation_id,
                kind=kind,
                owner_component_id=owner_component_id,
                owner_boot_id=str(
                    self._kernel.status().get("process", {}).get("boot_id") or ""
                )
                or None,
                public_input=_public_value(public_input or {}),
                recovery_policy=recovery_policy,
                progress=existing.progress if existing else 0,
                checkpoint=deepcopy(existing.checkpoint) if existing else {},
                created_at=existing.created_at if existing else _now(),
            )
            scope = self._context.create_child_scope("operation", operation_id)
            record.scope_id = scope.scope_id
            self._records[operation_id] = record
            self._scopes[operation_id] = scope
            if checkpoint is not None:
                self._checkpoint_callbacks[operation_id] = checkpoint
            self._persist()
        task = scope.spawn_task(
            self._run(record, coroutine),
            name=name or f"operation:{kind}:{operation_id[:8]}",
            persistent=False,
        )
        self._tasks[operation_id] = task
        task.add_done_callback(
            lambda _task, op_id=operation_id: self._operation_done(op_id)
        )
        return record, task

    async def _run(
        self,
        record: OperationRecord,
        coroutine: Coroutine[Any, Any, Any],
    ) -> Any:
        self.update(record.operation_id, state=OperationState.RUNNING, phase="running")
        try:
            result = await coroutine
        except asyncio.CancelledError:
            current = self._records[record.operation_id]
            if current.state not in {
                OperationState.CHECKPOINTED,
                OperationState.RECOVERY_REQUIRED,
            }:
                self.update(
                    record.operation_id,
                    state=OperationState.CANCELLED,
                    phase="cancelled",
                )
            raise
        except BaseException as error:
            self.update(
                record.operation_id,
                state=OperationState.FAILED,
                phase="failed",
                error_code=type(error).__name__,
            )
            raise
        self.update(
            record.operation_id,
            state=OperationState.COMPLETED,
            phase="completed",
            progress=100,
        )
        return result

    def update(
        self,
        operation_id: str,
        *,
        state: OperationState | None = None,
        phase: str | None = None,
        progress: int | None = None,
        checkpoint: dict[str, Any] | None = None,
        error_code: str | None = None,
    ) -> OperationRecord:
        with self._lock:
            record = self._records[operation_id]
            if state is not None:
                record.state = state
            if phase is not None:
                record.phase = phase
            if progress is not None:
                record.progress = max(0, min(100, int(progress)))
            if checkpoint is not None:
                record.checkpoint = _public_value(checkpoint)
            if error_code is not None:
                record.error_code = error_code
            record.updated_at = _now()
            if record.state.value in _TERMINAL_STATES:
                record.completed_at = record.updated_at
            self._persist()
            return record

    def mark_commit_critical(self, operation_id: str, phase: str) -> None:
        self.update(
            operation_id,
            state=OperationState.COMMIT_CRITICAL,
            phase=phase,
        )

    def get(self, operation_id: str) -> dict[str, Any] | None:
        with self._lock:
            record = self._records.get(operation_id)
            return record.public_dict() if record else None

    def list(self, *, active_only: bool = False) -> list[dict[str, Any]]:
        with self._lock:
            records = sorted(self._records.values(), key=lambda item: item.created_at)
            if active_only:
                records = [
                    item for item in records if item.state.value not in _TERMINAL_STATES
                ]
            return [item.public_dict() for item in records]

    def status(self) -> dict[str, Any]:
        operations = self.list()
        active = [item for item in operations if item["state"] not in _TERMINAL_STATES]
        return {
            "accepting": self._accepting,
            "active_count": len(active),
            "recovery_required_count": sum(
                item["state"] == OperationState.RECOVERY_REQUIRED for item in operations
            ),
            "operations": operations[-200:],
        }

    async def shutdown(
        self,
        *,
        checkpoint_timeout: float = 2.0,
        commit_timeout: float = 8.0,
    ) -> None:
        self._accepting = False
        active_ids = [
            operation_id
            for operation_id, task in self._tasks.items()
            if not task.done()
        ]
        active_tasks = {
            operation_id: self._tasks[operation_id] for operation_id in active_ids
        }
        checkpoint_ids = [
            operation_id
            for operation_id in active_ids
            if self._records[operation_id].state is not OperationState.COMMIT_CRITICAL
        ]
        commit_ids = [item for item in active_ids if item not in checkpoint_ids]

        async def checkpoint_one(operation_id: str) -> None:
            self.update(
                operation_id,
                state=OperationState.CHECKPOINTING,
                phase="shutdown_checkpoint",
            )
            callback = self._checkpoint_callbacks.get(operation_id)
            checkpoint_value = deepcopy(self._records[operation_id].checkpoint)
            if callback is not None:
                value = callback()
                if inspect.isawaitable(value):
                    value = await value
                if isinstance(value, dict):
                    checkpoint_value = value
            self.update(
                operation_id,
                state=OperationState.CHECKPOINTED,
                phase="checkpointed",
                checkpoint=checkpoint_value,
            )

        if checkpoint_ids:
            try:
                checkpoint_results = await asyncio.wait_for(
                    asyncio.gather(
                        *(checkpoint_one(item) for item in checkpoint_ids),
                        return_exceptions=True,
                    ),
                    timeout=checkpoint_timeout,
                )
                for operation_id, result in zip(checkpoint_ids, checkpoint_results):
                    if not isinstance(result, BaseException):
                        continue
                    self.update(
                        operation_id,
                        state=OperationState.RECOVERY_REQUIRED,
                        phase="checkpoint_failed",
                        error_code=(
                            "operation_checkpoint_failed:" f"{type(result).__name__}"
                        ),
                    )
            except TimeoutError:
                for operation_id in checkpoint_ids:
                    if (
                        self._records[operation_id].state
                        is OperationState.CHECKPOINTING
                    ):
                        self.update(
                            operation_id,
                            state=OperationState.RECOVERY_REQUIRED,
                            phase="checkpoint_timeout",
                            error_code="operation_checkpoint_timeout",
                        )
            for operation_id in checkpoint_ids:
                active_tasks[operation_id].cancel()

        if commit_ids:
            pending = {active_tasks[item] for item in commit_ids}
            _, still_pending = await asyncio.wait(pending, timeout=commit_timeout)
            if still_pending:
                for operation_id in commit_ids:
                    task = active_tasks[operation_id]
                    if task not in still_pending:
                        continue
                    self.update(
                        operation_id,
                        state=OperationState.RECOVERY_REQUIRED,
                        phase="commit_timeout",
                        error_code="operation_commit_timeout",
                    )
                    task.cancel()

        remaining = [task for task in active_tasks.values() if not task.done()]
        if remaining:
            await asyncio.gather(*remaining, return_exceptions=True)
        scopes = list(self._scopes.values())
        for scope in reversed(scopes):
            await scope.close()
        await self._kernel.drain_scope_cleanups()
        self._context = None
        self._persist()

    async def recover_pending(self) -> None:
        if not self._accepting:
            return
        pending = [
            record
            for record in self._records.values()
            if record.state
            in {OperationState.CHECKPOINTED, OperationState.RECOVERY_REQUIRED}
        ]
        for record in pending:
            handler = self._recovery_handlers.get(record.kind)
            if handler is None or record.recovery_policy in {"rollback", "discard"}:
                continue
            coroutine = handler(record.public_dict())
            if coroutine is None:
                continue
            self.start(
                record.kind,
                coroutine,
                operation_id=record.operation_id,
                public_input=record.public_input,
                recovery_policy=record.recovery_policy,
                _resume=True,
            )

    def _operation_done(self, operation_id: str) -> None:
        self._tasks.pop(operation_id, None)
        self._checkpoint_callbacks.pop(operation_id, None)
        scope = self._scopes.pop(operation_id, None)
        if scope is not None:
            self._kernel._schedule_scope_close(scope)
        self._persist()

    def _load(self) -> None:
        if self._state_path is None:
            return
        state = read_json_locked(
            self._state_path, {"version": 2, "operations": []}, quarantine_corrupt=True
        )
        values = state.get("operations", []) if isinstance(state, dict) else []
        if not isinstance(values, list):
            return
        for value in values[-200:]:
            if not isinstance(value, dict):
                continue
            try:
                state_value = OperationState(str(value.get("state", "failed")))
                if state_value in {
                    OperationState.QUEUED,
                    OperationState.RUNNING,
                    OperationState.CHECKPOINTING,
                }:
                    state_value = OperationState.CHECKPOINTED
                elif state_value is OperationState.COMMIT_CRITICAL:
                    state_value = OperationState.RECOVERY_REQUIRED
                recovery_policy = str(value.get("recovery_policy") or "restart")
                if recovery_policy not in {
                    "resume",
                    "restart",
                    "rollback",
                    "discard",
                }:
                    recovery_policy = "restart"
                record = OperationRecord(
                    operation_id=str(value["operation_id"]),
                    kind=str(value["kind"]),
                    owner_component_id=str(
                        value.get("owner_component_id") or "management:operations"
                    ),
                    owner_boot_id=value.get("owner_boot_id"),
                    state=state_value,
                    progress=int(value.get("progress") or 0),
                    public_input=_public_value(value.get("public_input") or {}),
                    checkpoint=_public_value(value.get("checkpoint") or {}),
                    recovery_policy=recovery_policy,  # type: ignore[arg-type]
                    phase=str(value.get("phase") or state_value.value),
                    error_code=value.get("error_code"),
                    scope_id=None,
                    created_at=str(value.get("created_at") or _now()),
                    updated_at=str(value.get("updated_at") or _now()),
                    completed_at=value.get("completed_at"),
                )
            except (KeyError, TypeError, ValueError):
                continue
            self._records[record.operation_id] = record

    def _persist(self) -> None:
        if self._state_path is None or (
            self._state_path == _DEFAULT_STATE_PATH and os.getenv("PYTEST_CURRENT_TEST")
        ):
            return
        records = sorted(self._records.values(), key=lambda item: item.created_at)[
            -200:
        ]
        write_json_locked(
            self._state_path,
            {"version": 2, "operations": [item.public_dict() for item in records]},
        )


def _state_path_from_environment() -> Path:
    value = os.getenv("ZHENXUN_OPERATION_STATE_PATH", "").strip()
    return Path(value) if value else _DEFAULT_STATE_PATH


operation_registry = OperationRegistry(lifecycle_kernel, _state_path_from_environment())

__all__ = [
    "OperationRecord",
    "OperationRegistry",
    "OperationState",
    "operation_registry",
]
