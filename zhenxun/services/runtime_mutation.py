from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Callable, Coroutine
from contextlib import asynccontextmanager
from contextvars import ContextVar
from datetime import datetime, timezone
from functools import wraps
from pathlib import Path
from typing import Any
import uuid


class RuntimeMutationBusyError(RuntimeError):
    pass


class MutationCancelled(asyncio.CancelledError):
    """Cooperative cancellation at a transaction-owned safe point."""


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
        self._owner_task: asyncio.Task[Any] | None = None
        self._transaction: object | None = None
        self._active_budget: Any = None
        self._inherited_transaction: ContextVar[object | None] = ContextVar(
            "runtime_mutation_transaction", default=None
        )

    @property
    def locked(self) -> bool:
        return self._lock.locked()

    @property
    def reentrant(self) -> bool:
        try:
            task = asyncio.current_task()
        except RuntimeError:
            return False
        return task is not None and task is self._owner_task

    def status(self) -> dict[str, Any] | None:
        if self._current:
            return {"coordinator_state": self._state, **dict(self._current)}
        return None if self._state == "open" else {"coordinator_state": self._state}

    @property
    def accepting(self) -> bool:
        return self._state == "open"

    @property
    def current_operation_id(self) -> str | None:
        return self._operation_id.get() if self.reentrant else None

    @property
    def cancellation_requested(self) -> bool:
        return bool(self._cancel_event and self._cancel_event.is_set())

    def set_phase(self, phase: str) -> None:
        if self._current is not None and self.reentrant:
            self._current["phase"] = phase
            from zhenxun.services.lifecycle.operations import operation_registry

            operation_id = self.current_operation_id
            record = operation_registry.get(operation_id) if operation_id else None
            if record and record["state"] not in {
                "completed",
                "failed",
                "cancelled",
                "recovery_required",
            }:
                operation_registry.update(operation_id, phase=phase)

    def checkpoint(self) -> None:
        if not self.reentrant:
            raise RuntimeMutationBusyError("mutation_not_owner")
        if self.cancellation_requested:
            raise MutationCancelled()

    async def run_owned(
        self,
        kind: str,
        callback: Callable[[], Coroutine[Any, Any, Any]],
        *,
        fail_if_busy: bool = False,
    ) -> Any:
        if self.reentrant:
            return await callback()
        if self._transaction is not None and (
            self._inherited_transaction.get() is self._transaction
        ):
            raise RuntimeMutationBusyError("mutation_cross_task_reentry")
        if not self.accepting:
            raise RuntimeMutationBusyError(f"runtime_mutation_{self._state}")
        if fail_if_busy and self.locked:
            raise RuntimeMutationBusyError("runtime_mutation_in_progress")
        from zhenxun.services.lifecycle.deadline import ShutdownBudget, current_budget
        from zhenxun.services.lifecycle.operations import operation_registry

        cancel = asyncio.Event()
        budget = ShutdownBudget(float("inf"))
        if parent := current_budget.get():
            budget.tighten(parent)
        operation_id = uuid.uuid4().hex

        def request_cancel() -> None:
            if parent := current_budget.get():
                budget.tighten(parent)
            cancel.set()

        async def execute() -> Any:
            token = current_budget.set(budget)
            try:
                async with self.operation(kind, operation_id=operation_id):
                    self._cancel_event = cancel
                    self._active_budget = budget
                    self.checkpoint()
                    return await callback()
            finally:
                current_budget.reset(token)

        _, task = operation_registry.start(
            kind,
            execute(),
            operation_id=operation_id,
            public_input={"source": kind},
            cancel=request_cancel,
        )
        # The caller can abandon its wait, but cannot interrupt the lock owner.
        while True:
            try:
                return await asyncio.shield(task)
            except asyncio.CancelledError:
                if task.done():
                    return task.result()
                request_cancel()

    def associate(self, operation_id: str, owner: str | None = None) -> None:
        if self._current is None or not self.reentrant:
            return
        self._operation_id.set(operation_id)
        self._current["operation_id"] = operation_id
        if owner is not None:
            self._current["owner"] = owner

    def request_cancel(self) -> None:
        if not self.reentrant:
            raise RuntimeMutationBusyError("mutation_not_owner")
        self._request_cancel()

    def _request_cancel(self) -> None:
        from zhenxun.services.lifecycle.deadline import current_budget

        parent = current_budget.get()
        if self._active_budget is not None and parent is not None:
            self._active_budget.tighten(parent)
        if self._cancel_event is not None:
            self._cancel_event.set()

    async def quiesce(self, timeout: float = 10.0) -> bool:
        self._state = "quiescing"
        self._request_cancel()
        if self._lock.locked():
            try:
                await asyncio.wait_for(self._idle.wait(), timeout=timeout)
            except asyncio.TimeoutError:
                self._request_cancel()
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
        if self.reentrant:
            token = self._depth.set(depth + 1)
            try:
                yield
            finally:
                self._depth.reset(token)
            return
        if (
            self._transaction is not None
            and self._inherited_transaction.get() is self._transaction
        ):
            raise RuntimeMutationBusyError("mutation_cross_task_reentry")
        if self._state != "open":
            raise RuntimeMutationBusyError(f"runtime_mutation_{self._state}")
        if fail_if_busy and self._lock.locked():
            raise RuntimeMutationBusyError("runtime_mutation_in_progress")
        async with self._lock:
            if self._state != "open":
                raise RuntimeMutationBusyError(f"runtime_mutation_{self._state}")
            from zhenxun.migration.errors import MigrationError
            from zhenxun.migration.mutation import require_mutation_available

            try:
                require_mutation_available(Path.cwd())
            except MigrationError as error:
                raise RuntimeMutationBusyError(error.code) from None
            token = self._depth.set(1)
            self._owner_task = asyncio.current_task()
            self._transaction = object()
            transaction_token = self._inherited_transaction.set(self._transaction)
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
                self._owner_task = None
                self._transaction = None
                self._active_budget = None
                self._inherited_transaction.reset(transaction_token)
                self._idle.set()
                self._operation_id.reset(operation_token)
                self._depth.reset(token)


runtime_mutation_coordinator = RuntimeMutationCoordinator()


def managed_mutation(kind: str):
    def decorate(func):
        @wraps(func)
        async def wrapped(*args, **kwargs):
            return await runtime_mutation_coordinator.run_owned(
                kind, lambda: func(*args, **kwargs)
            )

        return wrapped

    return decorate


__all__ = [
    "RuntimeMutationBusyError",
    "RuntimeMutationCoordinator",
    "runtime_mutation_coordinator",
]
