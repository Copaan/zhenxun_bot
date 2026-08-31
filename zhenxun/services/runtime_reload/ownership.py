from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar

runtime_owner: ContextVar[str | None] = ContextVar(
    "zhenxun_runtime_owner", default=None
)


def current_owner() -> str | None:
    owner = runtime_owner.get()
    if owner:
        return owner
    try:
        from nonebot.plugin import _current_plugin

        plugin = _current_plugin.get()
    except (ImportError, LookupError):
        return None
    return plugin.id_ if plugin else None


def import_owner() -> str | None:
    try:
        from nonebot.plugin import _current_plugin

        plugin = _current_plugin.get()
    except (ImportError, LookupError):
        return None
    return plugin.id_ if plugin else None


@contextmanager
def owner_context(owner: str | None) -> Iterator[None]:
    token = runtime_owner.set(owner)
    try:
        yield
    finally:
        runtime_owner.reset(token)
