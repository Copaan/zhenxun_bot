"""Immediate admission switch, separate from a plugin's new-group default."""

import asyncio
from contextlib import contextmanager
from contextvars import ContextVar
from functools import wraps

_admitted = ContextVar("ai_chat_admitted_request", default=None)


def module_identity(owner: str) -> str:
    value = owner.split(":", 1)[0]
    for prefix in ("zhenxun.plugins.", "zhenxun.builtin_plugins."):
        if value.startswith(prefix):
            return value[len(prefix) :]
    return value


def switch_values() -> dict[str, bool] | None:
    from zhenxun.services.plugin_policy import (
        PluginPolicyNotReady,
        plugin_policy_service,
    )

    try:
        return plugin_policy_service.global_switch_values()
    except PluginPolicyNotReady:
        # Keep admission closed while startup is still loading policy.  The
        # caller can skip work without logging a normal startup race as ERROR.
        return None


def chat_plugin_enabled(owner: str | None, values: dict | None = None) -> bool:
    current_values = switch_values() if values is None else values
    if current_values is None:
        return False
    if not owner:
        return True
    identity = module_identity(owner)
    return not any(
        enabled is False
        and (
            identity == module_identity(module)
            or identity.startswith(module_identity(module) + ".")
        )
        for module, enabled in current_values.items()
    )


def require_chat_enabled(owner: str | None = None) -> None:
    from zhenxun.services.ai.core.exceptions import ConfigurationException
    from zhenxun.services.runtime_reload.ownership import current_owner

    if _admitted.get() is asyncio.current_task():
        return
    if not chat_plugin_enabled(owner or current_owner()):
        raise ConfigurationException(
            "ai_chat_plugin_disabled: 当前 AI 聊天插件已停用。"
        )


@contextmanager
def admitted_ai_request():
    require_chat_enabled()
    token = _admitted.set(asyncio.current_task())
    try:
        yield
    finally:
        _admitted.reset(token)


def admit_ai_call(function):
    @wraps(function)
    async def invoke(*args, **kwargs):
        with admitted_ai_request():
            return await function(*args, **kwargs)

    return invoke
