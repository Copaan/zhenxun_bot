from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar

operation_owner: ContextVar[str | None] = ContextVar(
    "zhenxun_operation_owner", default=None
)
callback_owner: ContextVar[str | None] = ContextVar(
    "zhenxun_callback_owner", default=None
)
resource_owner: ContextVar[str | None] = ContextVar(
    "zhenxun_resource_owner", default=None
)


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
