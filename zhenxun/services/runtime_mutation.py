from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Callable, Coroutine
from contextlib import asynccontextmanager, contextmanager
from contextvars import ContextVar
from datetime import datetime, timezone
from functools import wraps
import os
from pathlib import Path
import threading
from typing import Any, ParamSpec, TypeVar
import uuid

from filelock import FileLock, Timeout

from zhenxun.utils.atomic_json import (
    AtomicJsonRecordError,
    read_json_locked,
    write_json_locked,
)

P = ParamSpec("P")
R = TypeVar("R")
INSTANCE_ROOT = Path(os.environ.get("ZHENXUN_PROJECT_ROOT") or Path.cwd()).resolve()
OWNER_FILE = INSTANCE_ROOT / "data/runtime/mutation-owner-v1.json"
_ownership: ContextVar[str | None] = ContextVar("runtime_mutation_owner", default=None)
_owner_locks: dict[Path, FileLock] = {}
_owner_locks_guard = threading.Lock()


class RuntimeMutationBusyError(RuntimeError):
    pass


class StoreOperationBusyError(RuntimeError):
    pass


class MutationCancelled(asyncio.CancelledError):
    """Cooperative cancellation at a transaction-owned safe point."""


def mutation_owner() -> dict[str, Any] | None:
    """读取跨进程变更归属，损坏记录保留并拒绝新的写入"""
    value = read_json_locked(OWNER_FILE, None, strict=True)
    if value is not None and (
        not isinstance(value, dict) or not value.get("operation_id")
    ):
        raise AtomicJsonRecordError("mutation_owner_invalid")
    return value


@contextmanager
def mutation_scope(operation_id: str | None):
    """在受控内部调用期间绑定父任务，不向普通管理请求继承授权"""
    token = _ownership.set(operation_id)
    try:
        yield
    finally:
        _ownership.reset(token)


@contextmanager
def mutation_guard_lock():
    """短时间持有跨进程归属锁，不校验归属"""
    path = OWNER_FILE.with_name(".mutation.lock").resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    with _owner_locks_guard:
        lock = _owner_locks.setdefault(path, FileLock(path, thread_local=True))
    try:
        lock.acquire(timeout=2)
    except Timeout as error:
        raise RuntimeMutationBusyError("runtime_mutation_in_progress") from error
    try:
        yield
    finally:
        lock.release()


@contextmanager
def mutation_guard(operation_id: str | None = None):
    """短时间持有跨进程归属锁，校验当前写入属于已登记任务"""
    with mutation_guard_lock():
        owner = mutation_owner()
        identity = operation_id or _ownership.get()
        if owner and owner["operation_id"] != identity:
            raise RuntimeMutationBusyError("migration_operation_in_progress")
        yield owner


def clear_stale_runtime_owner() -> bool:
    """清除登记进程已不存在的运行时变更归属，恢复与切换任务的归属保持不动

    返回:
        bool: 是否清除了记录
    """
    import psutil

    with mutation_guard_lock():
        owner = mutation_owner()
        if not owner or owner.get("kind") != "runtime":
            return False
        executor = owner.get("executor") or {}
        try:
            process = psutil.Process(int(executor.get("pid") or 0))
            if abs(process.create_time() - float(executor.get("created_at") or 0)) < 1:
                return False
        except (psutil.Error, ValueError, TypeError):
            pass
        write_json_locked(OWNER_FILE, None)
        return True


def release_mutation(operation_id: str) -> None:
    """仅释放本任务登记的归属，调用前必须持久化终态或提交证据"""
    with mutation_guard(operation_id) as owner:
        if owner:
            write_json_locked(OWNER_FILE, None)


class RuntimeMutationCoordinator:
    """Serialize state-changing runtime operations with task-local reentrancy."""

    def __init__(self) -> None:
        self._lock = asyncio.Lock()
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
    def reentrant(self) -> bool:
        try:
            task = asyncio.current_task()
        except RuntimeError:
            return False
        return task is not None and task is self._owner_task

    def status(self) -> dict[str, Any] | None:
        if self._current:
            return {"coordinator_state": self._state, **dict(self._current)}
        if persisted := mutation_owner():
            return {
                "coordinator_state": "reserved",
                "operation_id": persisted["operation_id"],
                "kind": persisted.get("kind"),
                "phase": persisted.get("phase", "recovery_required"),
            }
        return None if self._state == "open" else {"coordinator_state": self._state}

    @property
    def cancellation_requested(self) -> bool:
        return bool(self._cancel_event and self._cancel_event.is_set())

    def checkpoint(self) -> None:
        if not self.reentrant:
            raise RuntimeMutationBusyError("mutation_not_owner")
        if self.cancellation_requested:
            raise MutationCancelled()

    def _check_enter(self, fail_if_busy: bool) -> None:
        if (
            self._transaction is not None
            and self._inherited_transaction.get() is self._transaction
        ):
            raise RuntimeMutationBusyError("mutation_cross_task_reentry")
        if self._state != "open":
            raise RuntimeMutationBusyError(f"runtime_mutation_{self._state}")
        if fail_if_busy and self._lock.locked():
            raise RuntimeMutationBusyError("runtime_mutation_in_progress")

    async def run_owned(
        self,
        kind: str,
        callback: Callable[[], Coroutine[Any, Any, Any]],
        *,
        fail_if_busy: bool = False,
    ) -> Any:
        """在独立任务中持锁执行变更，调用方取消等待时只请求变更在安全点停止

        参数:
            kind: 变更类型
            callback: 变更内容
            fail_if_busy: 已有变更时是否直接失败

        返回:
            Any: 变更结果
        """
        if self.reentrant:
            return await callback()
        self._check_enter(fail_if_busy)
        from zhenxun.services.lifecycle.deadline import ShutdownBudget, current_budget

        cancel = asyncio.Event()
        budget = ShutdownBudget(float("inf"))
        if parent := current_budget.get():
            budget.tighten(parent)

        def request_cancel() -> None:
            if parent := current_budget.get():
                budget.tighten(parent)
            cancel.set()

        async def execute() -> Any:
            token = current_budget.set(budget)
            try:
                async with self.operation(kind):
                    self._cancel_event = cancel
                    self._active_budget = budget
                    self.checkpoint()
                    return await callback()
            finally:
                current_budget.reset(token)

        task = asyncio.create_task(execute(), name=f"runtime-mutation:{kind}")
        while True:
            try:
                return await asyncio.shield(task)
            except asyncio.CancelledError:
                if task.done():
                    return task.result()
                request_cancel()

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
        if self.reentrant:
            yield
            return
        self._check_enter(fail_if_busy)
        async with self._lock:
            if self._state != "open":
                raise RuntimeMutationBusyError(f"runtime_mutation_{self._state}")
            import psutil

            inherited = _ownership.get()
            identity = inherited or operation_id or uuid.uuid4().hex
            registered = False
            with mutation_guard(inherited):
                if not inherited:
                    write_json_locked(
                        OWNER_FILE,
                        {
                            "operation_id": identity,
                            "kind": "runtime",
                            "operation_kind": kind,
                            "phase": phase,
                            "revision": 1,
                            "executor": {
                                "pid": os.getpid(),
                                "created_at": psutil.Process().create_time(),
                                "lease_id": os.environ.get("ZHENXUN_INSTANCE_LEASE_ID"),
                            },
                        },
                    )
                    registered = True
            ownership_token = _ownership.set(identity)
            self._owner_task = asyncio.current_task()
            self._transaction = object()
            transaction_token = self._inherited_transaction.set(self._transaction)
            self._idle.clear()
            self._cancel_event = asyncio.Event()
            self._current = {
                "kind": kind,
                "operation_id": identity,
                "owner": owner,
                "phase": phase,
                "started_at": datetime.now(timezone.utc).isoformat(),
            }
            try:
                yield
            finally:
                try:
                    if registered:
                        release_mutation(identity)
                finally:
                    _ownership.reset(ownership_token)
                    self._current = None
                    self._cancel_event = None
                    self._owner_task = None
                    self._transaction = None
                    self._active_budget = None
                    self._inherited_transaction.reset(transaction_token)
                    self._idle.set()


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


@asynccontextmanager
async def store_operation(
    *, operation_id: str | None = None, owner: str | None = None
) -> AsyncIterator[None]:
    """持有插件商店变更锁，已有其他变更时直接失败

    参数:
        operation_id: 操作 ID
        owner: 操作来源
    """
    try:
        async with runtime_mutation_coordinator.operation(
            "plugin_store",
            fail_if_busy=True,
            operation_id=operation_id,
            owner=owner,
        ):
            yield
    except RuntimeMutationBusyError as error:
        raise StoreOperationBusyError("plugin_operation_in_progress") from error


def coordinated_store_operation(
    func: Callable[P, Coroutine[Any, Any, R]],
) -> Callable[P, Coroutine[Any, Any, R]]:
    """以插件商店变更锁运行函数，已有其他变更时直接失败

    参数:
        func: 商店操作函数

    返回:
        Callable[P, Coroutine[Any, Any, R]]: 包装后的函数
    """

    @wraps(func)
    async def wrapped(*args: P.args, **kwargs: P.kwargs) -> R:
        try:
            return await runtime_mutation_coordinator.run_owned(
                "plugin_store", lambda: func(*args, **kwargs), fail_if_busy=True
            )
        except RuntimeMutationBusyError as error:
            raise StoreOperationBusyError("plugin_operation_in_progress") from error

    return wrapped


__all__ = [
    "RuntimeMutationBusyError",
    "RuntimeMutationCoordinator",
    "StoreOperationBusyError",
    "coordinated_store_operation",
    "managed_mutation",
    "runtime_mutation_coordinator",
    "store_operation",
]
