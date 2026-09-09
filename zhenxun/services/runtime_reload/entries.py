from __future__ import annotations

from contextlib import AsyncExitStack
from dataclasses import dataclass, field
from typing import Any

from nonebot.dependencies import Dependent

from .ownership import lifecycle_work_context, owner_context


class DependencyStack(AsyncExitStack):
    """Register cleanup on the original event stack, retaining its owner lease."""

    def __init__(self, stack, manager, owner, incarnation):
        super().__init__()
        self.stack = stack
        self.manager = manager
        self.owner = owner
        self.incarnation = incarnation

    async def enter_async_context(self, cm):
        return await self.stack.enter_async_context(
            DependencyResource(cm, self.manager, self.owner, self.incarnation)
        )


class DependencyResource:
    def __init__(self, cm, manager, owner, incarnation):
        self.cm = cm
        self.manager = manager
        self.owner = owner
        self.incarnation = incarnation
        self.release = None
        self.closed = False

    async def __aenter__(self):
        self.release = self.manager._retain_activity(self.owner)
        try:
            with owner_context(self.owner):
                return await self.cm.__aenter__()
        except BaseException:
            self.release()
            raise

    async def __aexit__(self, *exc):
        from zhenxun.services.lifecycle import lifecycle_kernel

        async def close_resource():
            result = await self.cm.__aexit__(*exc)
            self.release()
            self.closed = True
            return result

        try:
            with (
                owner_context(self.owner),
                lifecycle_work_context(self.owner, self.incarnation, "on_shutdown"),
            ):
                return await lifecycle_kernel._run_cleanup(
                    close_resource(),
                    owner=self.owner,
                    stage="dependency_exit",
                    timeout=lifecycle_kernel.shutdown_remaining(15.0),
                    grace=0.05,
                )
        except BaseException:
            if self.closed:
                raise
            root = self.manager._root_owner(self.owner) or self.owner
            self.manager._integrity_failures.add(root)
            lifecycle_kernel.require_recovery(f"plugin:{root}")
            if unit := self.manager.units.get(root):
                unit.draining = True
                unit.last_error = "dependency_release_unconfirmed"
            raise


@dataclass(frozen=True)
class ManagedDependent(Dependent):
    manager: Any = field(default=None, compare=False, repr=False)
    owner: str = ""
    incarnation: str = ""
    kind: str = ""

    async def __call__(self, **kwargs):
        import asyncio

        task = asyncio.current_task()
        connection = self.kind in {"on_bot_connect", "on_bot_disconnect"}
        if connection:
            self.manager._connection_tasks.add(task)
        try:
            return await self._invoke(**kwargs)
        except asyncio.CancelledError as error:
            if connection and self.manager.consume_connection_cancellation(task, error):
                return None
            raise
        finally:
            if connection:
                self.manager._connection_tasks.discard(task)

    async def _invoke(self, **kwargs):
        with self.manager._entry_admission(
            self.owner,
            self.incarnation,
            business=self.kind
            not in {"on_startup", "on_ready", "on_shutdown", "on_bot_disconnect"},
        ) as admitted:
            if not admitted:
                return None
            stack = kwargs.get("stack")
            if stack is not None:
                kwargs["stack"] = DependencyStack(
                    stack, self.manager, self.owner, self.incarnation
                )
            return await super().__call__(**kwargs)


def manage_registration(registry, before, manager, owner, incarnation, *, kind=""):
    for dependent in list(registry):
        if id(dependent) in before:
            continue
        managed = ManagedDependent(
            call=dependent.call,
            params=dependent.params,
            parameterless=dependent.parameterless,
            manager=manager,
            owner=owner,
            incarnation=incarnation,
            kind=kind,
        )
        registry.remove(dependent)
        registry.add(managed)
