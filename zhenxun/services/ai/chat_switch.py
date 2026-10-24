"""Immediate admission switch, separate from a plugin's new-group default."""

import asyncio
from contextlib import contextmanager
from contextvars import ContextVar
from functools import wraps
from pathlib import Path
from types import ModuleType

from nonebot.plugin import Plugin

AI_CHAT_CAPABILITY = "ai_chat"
"""AI 聊天插件的能力标记"""
_AI_IMPORTS = ("zhenxun.services.ai",)
_MESSAGE_LISTENERS = frozenset(
    {"nonebot.on_message", "nonebot.plugin.on_message", "nonebot.plugin.on.on_message"}
)

_admitted = ContextVar("ai_chat_admitted_request", default=None)
_scene_admitted = ContextVar("plugin_scene_admitted", default=False)


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
    from nonebot.matcher import current_matcher

    from zhenxun.services.ai.core.exceptions import ConfigurationException
    from zhenxun.services.plugin_host import current_owner

    if _admitted.get() is asyncio.current_task():
        return
    if _scene_admitted.get() or current_matcher.get(None) is not None:
        return
    if not chat_plugin_enabled(owner or current_owner()):
        raise ConfigurationException(
            "ai_chat_plugin_disabled: 当前 AI 聊天插件已停用。"
        )


@contextmanager
def scene_admitted():
    """入口已按场景判定插件可用，期间的 AI 调用不再按全局开关拦截"""
    token = _scene_admitted.set(True)
    try:
        yield
    finally:
        _scene_admitted.reset(token)


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


def declared_capabilities(plugin: Plugin) -> set[str]:
    """插件在 PluginExtraData.capabilities 中声明的能力

    参数:
        plugin: 插件

    返回:
        set[str]: 能力标记
    """
    extra = plugin.metadata.extra if plugin.metadata else None
    values = extra.get("capabilities") if isinstance(extra, dict) else None
    return {str(item).strip() for item in values or () if str(item).strip()}


def detect_ai_chat(module_name: str, root: Path) -> bool:
    """按源码导入识别 AI 聊天插件：使用宿主 AI 接口且注册了不限指令的消息响应器

    参数:
        module_name: 插件模块名
        root: 插件包目录或模块文件

    返回:
        bool: 是否为 AI 聊天插件
    """
    from zhenxun.services.plugin_host import plugin_host

    imports = plugin_host.classifier.source_imports(module_name, root)
    uses_ai = any(
        name == prefix or name.startswith(f"{prefix}.")
        for name in imports
        for prefix in _AI_IMPORTS
    )
    return uses_ai and not imports.isdisjoint(_MESSAGE_LISTENERS)


def plugin_capabilities(module_name: str, root: Path | None = None) -> set[str]:
    """插件声明的能力与按源码识别出的能力

    参数:
        module_name: 插件 id 或模块名
        root: 插件包目录或模块文件，为空时取已加载插件的源码位置

    返回:
        set[str]: 能力标记
    """
    from zhenxun.services.plugin_host.classifier import find_plugin, plugin_root

    plugin = find_plugin(module_name)
    capabilities = declared_capabilities(plugin) if plugin is not None else set()
    if plugin is not None:
        module_name = plugin.module_name
        root = root or plugin_root(plugin)
    if (
        AI_CHAT_CAPABILITY not in capabilities
        and root is not None
        and root.exists()
        and detect_ai_chat(module_name, root)
    ):
        capabilities.add(AI_CHAT_CAPABILITY)
    return capabilities


def loaded_ai_chat_plugins() -> list[Plugin]:
    """已加载的扩展 AI 聊天插件

    返回:
        list[Plugin]: 顶层插件
    """
    import nonebot

    return [
        plugin
        for plugin in nonebot.get_loaded_plugins()
        if plugin.parent_plugin is None
        and not plugin.module_name.startswith("zhenxun.builtin_plugins.")
        and AI_CHAT_CAPABILITY in plugin_capabilities(plugin.module_name)
    ]


def persona_provider() -> ModuleType | None:
    """提供默认人设读写的 AI 聊天插件模块，优先取当前启用的插件

    插件顶层模块需提供 ``get_default_persona() -> dict | None`` 与
    ``save_default_persona(data: dict) -> dict``，人设字段为 name、prompt、
    style、tone_examples、preset_dialogues、enabled。

    返回:
        ModuleType | None: 插件模块
    """
    modules = [
        plugin.module
        for plugin in loaded_ai_chat_plugins()
        if callable(getattr(plugin.module, "get_default_persona", None))
        and callable(getattr(plugin.module, "save_default_persona", None))
    ]
    values = switch_values() or {}
    enabled = [m for m in modules if chat_plugin_enabled(m.__name__, values)]
    return next(iter(enabled or modules), None)
