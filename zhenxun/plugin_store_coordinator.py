from __future__ import annotations

from collections.abc import AsyncIterator, Callable, Coroutine
from contextlib import asynccontextmanager
from functools import wraps
from typing import Any, ParamSpec, TypeVar

from zhenxun.services.runtime_mutation import (
    RuntimeMutationBusyError,
    runtime_mutation_coordinator,
)

P = ParamSpec("P")
R = TypeVar("R")


class StoreOperationBusyError(RuntimeError):
    pass


class PluginStoreOperationCoordinator:
    def __init__(self) -> None:
        self._coordinator = runtime_mutation_coordinator

    @asynccontextmanager
    async def operation(
        self,
        *,
        operation_id: str | None = None,
        owner: str | None = None,
    ) -> AsyncIterator[None]:
        try:
            async with self._coordinator.operation(
                "plugin_store",
                fail_if_busy=True,
                operation_id=operation_id,
                owner=owner,
            ):
                yield
        except RuntimeMutationBusyError as error:
            raise StoreOperationBusyError("plugin_operation_in_progress") from error


plugin_store_operation_coordinator = PluginStoreOperationCoordinator()


def coordinated_store_operation(
    func: Callable[P, Coroutine[Any, Any, R]],
) -> Callable[P, Coroutine[Any, Any, R]]:
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
    "StoreOperationBusyError",
    "coordinated_store_operation",
    "plugin_store_operation_coordinator",
]
