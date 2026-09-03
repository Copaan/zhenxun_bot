from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from contextvars import ContextVar
from datetime import datetime, timezone
from typing import Any
import uuid


class RuntimeMutationBusyError(RuntimeError):
    pass


class RuntimeMutationCoordinator:
    """Serialize state-changing runtime operations with task-local reentrancy."""

    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._depth: ContextVar[int] = ContextVar("runtime_mutation_depth", default=0)
        self._operation_id: ContextVar[str | None] = ContextVar(
            "runtime_mutation_operation_id", default=None
        )
        self._current: dict[str, Any] | None = None
        self._state = "open"
        self._idle = asyncio.Event()
        self._idle.set()
        self._cancel_event: asyncio.Event | None = None

    @property
    def locked(self) -> bool:
        return self._lock.locked()

    @property
    def reentrant(self) -> bool:
        return self._depth.get() > 0

    def status(self) -> dict[str, Any] | None:
        if self._current:
            return {"coordinator_state": self._state, **dict(self._current)}
        return None if self._state == "open" else {"coordinator_state": self._state}

    @property
    def accepting(self) -> bool:
        return self._state == "open"

    @property
    def current_operation_id(self) -> str | None:
        return self._operation_id.get()

    @property
    def cancellation_requested(self) -> bool:
        return bool(self._cancel_event and self._cancel_event.is_set())

    def set_phase(self, phase: str) -> None:
        if self._current is not None and self.reentrant:
            self._current["phase"] = phase

    def associate(self, operation_id: str, owner: str | None = None) -> None:
        if self._current is None or not self.reentrant:
            return
        self._operation_id.set(operation_id)
        self._current["operation_id"] = operation_id
        if owner is not None:
            self._current["owner"] = owner

    def request_cancel(self) -> None:
        if self._cancel_event is not None:
            self._cancel_event.set()

    async def quiesce(self, timeout: float = 10.0) -> bool:
        self._state = "quiescing"
        if self._lock.locked():
            try:
                await asyncio.wait_for(self._idle.wait(), timeout=timeout)
            except TimeoutError:
                self.request_cancel()
                return False
        return True

    def close(self) -> None:
        self._state = "closed"

    def reopen(self) -> None:
        self._state = "open"

    @asynccontextmanager
    async def operation(
        self,
        kind: str,
        *,
        fail_if_busy: bool = False,
        operation_id: str | None = None,
        owner: str | None = None,
        phase: str = "running",
    ) -> AsyncIterator[None]:
        depth = self._depth.get()
        if depth:
            token = self._depth.set(depth + 1)
            try:
                yield
            finally:
                self._depth.reset(token)
            return
        if self._state != "open":
            raise RuntimeMutationBusyError(f"runtime_mutation_{self._state}")
        if fail_if_busy and self._lock.locked():
            raise RuntimeMutationBusyError("runtime_mutation_in_progress")
        async with self._lock:
            if self._state != "open":
                raise RuntimeMutationBusyError(f"runtime_mutation_{self._state}")
            token = self._depth.set(1)
            operation_id = operation_id or uuid.uuid4().hex
            operation_token = self._operation_id.set(operation_id)
            self._idle.clear()
            self._cancel_event = asyncio.Event()
            self._current = {
                "kind": kind,
                "operation_id": operation_id,
                "owner": owner,
                "phase": phase,
                "started_at": datetime.now(timezone.utc).isoformat(),
            }
            try:
                yield
            finally:
                self._current = None
                self._cancel_event = None
                self._idle.set()
                self._operation_id.reset(operation_token)
                self._depth.reset(token)


runtime_mutation_coordinator = RuntimeMutationCoordinator()

__all__ = [
    "RuntimeMutationBusyError",
    "RuntimeMutationCoordinator",
    "runtime_mutation_coordinator",
]
