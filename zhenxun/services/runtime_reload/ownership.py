from __future__ import annotations

import asyncio
from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import Context, ContextVar
from dataclasses import dataclass, field
import threading
from typing import TYPE_CHECKING
import weakref

if TYPE_CHECKING:
    from zhenxun.services.lifecycle.deadline import ShutdownBudget

operation_owner: ContextVar[str | None] = ContextVar(
    "zhenxun_operation_owner", default=None
)
callback_owner: ContextVar[str | None] = ContextVar(
    "zhenxun_callback_owner", default=None
)
resource_owner: ContextVar[str | None] = ContextVar(
    "zhenxun_resource_owner", default=None
)
_lifecycle_work = ContextVar("zhenxun_lifecycle_work", default=None)
initialization_retainer = ContextVar("plugin_initialization_retainer", default=None)


@dataclass
class LifecycleWork:
    owner: str
    incarnation_id: str
    executor: weakref.ReferenceType
    phase: str
    budget: ShutdownBudget
    active: bool = True
    children: weakref.WeakSet = field(default_factory=weakref.WeakSet)
    supervisor: weakref.ReferenceType | None = None
    retained: bool = False
    expired: bool = False
    deadline_handle: asyncio.Handle | None = None
    loop: asyncio.AbstractEventLoop | None = None

    def valid(self) -> bool:
        executor = self.supervisor() if self.supervisor else self.executor()
        return bool(
            self.active
            and executor is not None
            and not (isinstance(executor, asyncio.Task) and executor.done())
            and self.budget.remaining() > 0
        )


def _execution_owner():
    try:
        return asyncio.current_task() or threading.current_thread()
    except RuntimeError:
        return threading.current_thread()


@contextmanager
def lifecycle_work_context(owner: str, incarnation_id: str, phase: str):
    from zhenxun.services.lifecycle import lifecycle_kernel
    from zhenxun.services.lifecycle.deadline import shutdown_budget

    maximum = (
        lifecycle_kernel.shutdown_remaining(15.0) if phase == "on_shutdown" else 60.0
    )
    with shutdown_budget(maximum) as budget:
        budget.check()
        value = LifecycleWork(
            owner, incarnation_id, weakref.ref(_execution_owner()), phase, budget
        )
        if phase in {"on_startup", "on_ready"} and (
            retain := initialization_retainer.get()
        ):
            retain(value)
        token = _lifecycle_work.set(value)
        executor = value.executor()
        prior_cancellations = (
            executor.cancelling() if hasattr(executor, "cancelling") else 0
        )
        timeout_handle = None
        timed_out = False

        def expire():
            nonlocal timed_out
            if (
                value.active
                and isinstance(executor, asyncio.Task)
                and not executor.done()
            ):
                timed_out = True
                executor.cancel("plugin_lifecycle_deadline")

        if isinstance(executor, asyncio.Task):
            # Deadline enforcement is infrastructure work, not a plugin timer.
            timeout_handle = Context().run(
                asyncio.get_running_loop().call_later, budget.remaining(), expire
            )
        try:
            yield value
            if timed_out or budget.remaining() <= 0:
                raise TimeoutError("plugin_lifecycle_budget_exhausted")
        except asyncio.CancelledError as error:
            if timed_out and error.args == ("plugin_lifecycle_deadline",):
                if hasattr(executor, "uncancel"):
                    if executor.uncancel() > prior_cancellations:
                        raise
                raise TimeoutError("plugin_lifecycle_budget_exhausted") from error
            raise
        finally:
            if timeout_handle is not None:
                timeout_handle.cancel()
            if value.retained:

                def expire_retained():
                    value.expired = True
                    value.active = False
                    for child in list(value.children):
                        if not child.done():
                            child.cancel("plugin_initialization_budget_exhausted")

                def schedule():
                    if value.active:
                        value.deadline_handle = value.loop.call_later(
                            budget.remaining(), expire_retained
                        )

                Context().run(value.loop.call_soon_threadsafe, schedule)
            else:
                value.active = False
            _lifecycle_work.reset(token)


def lifecycle_work_phase(owner: str, incarnation_id: str) -> LifecycleWork | None:
    value = _lifecycle_work.get()
    if (
        value
        and (value.owner, value.incarnation_id) == (owner, incarnation_id)
        and (
            value.executor() is _execution_owner()
            or _execution_owner() in value.children
        )
        and value.valid()
    ):
        return value
    return None


def current_owner() -> str | None:
    if owner := resource_owner.get():
        return owner
    if owner := import_owner():
        return owner
    if owner := callback_owner.get():
        return owner
    return operation_owner.get()


def import_owner() -> str | None:
    try:
        from nonebot.plugin import _current_plugin

        plugin = _current_plugin.get()
    except (ImportError, LookupError):
        return None
    return plugin.id_ if plugin else None


@contextmanager
def owner_context(owner: str | None) -> Iterator[None]:
    token = callback_owner.set(owner)
    try:
        yield
    finally:
        callback_owner.reset(token)


@contextmanager
def operation_context(owner: str | None) -> Iterator[None]:
    token = operation_owner.set(owner)
    try:
        yield
    finally:
        operation_owner.reset(token)


@contextmanager
def resource_context(owner: str | None) -> Iterator[None]:
    token = resource_owner.set(owner)
    try:
        yield
    finally:
        resource_owner.reset(token)
