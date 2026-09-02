from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Callable, Coroutine
from contextlib import asynccontextmanager
from contextvars import ContextVar
from functools import wraps
from typing import Any, ParamSpec, TypeVar

P = ParamSpec("P")
R = TypeVar("R")


class StoreOperationBusyError(RuntimeError):
    pass


class PluginStoreOperationCoordinator:
    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._depth: ContextVar[int] = ContextVar(
            "plugin_store_operation_depth", default=0
        )

    @asynccontextmanager
    async def operation(self) -> AsyncIterator[None]:
        depth = self._depth.get()
        if depth:
            token = self._depth.set(depth + 1)
            try:
                yield
            finally:
                self._depth.reset(token)
            return
        if self._lock.locked():
            raise StoreOperationBusyError("plugin_operation_in_progress")
        async with self._lock:
            token = self._depth.set(1)
            try:
                yield
            finally:
                self._depth.reset(token)


plugin_store_operation_coordinator = PluginStoreOperationCoordinator()


def coordinated_store_operation(
    func: Callable[P, Coroutine[Any, Any, R]],
) -> Callable[P, Coroutine[Any, Any, R]]:
    @wraps(func)
    async def wrapped(*args: P.args, **kwargs: P.kwargs) -> R:
        async with plugin_store_operation_coordinator.operation():
            return await func(*args, **kwargs)

    return wrapped


__all__ = [
    "StoreOperationBusyError",
    "coordinated_store_operation",
    "plugin_store_operation_coordinator",
]
