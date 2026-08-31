from __future__ import annotations

from collections.abc import Iterable
import contextlib
from typing import Any


class NoneBotCompatibilityError(RuntimeError):
    pass


def verify_nonebot_compatibility() -> None:
    try:
        from nonebot.matcher import matchers
        import nonebot.message as message
        import nonebot.plugin as plugin
        from nonebot.rule import TrieRule

        required = (
            plugin._plugins,
            plugin._managers,
            matchers,
            TrieRule.prefix,
            message._event_preprocessors,
            message._event_postprocessors,
            message._run_preprocessors,
            message._run_postprocessors,
        )
    except (AttributeError, ImportError) as e:
        raise NoneBotCompatibilityError("nonebot_private_api_missing") from e
    if not all(container is not None for container in required):
        raise NoneBotCompatibilityError("nonebot_private_api_invalid")


def _dependent_module(item: Any) -> str:
    call = getattr(item, "call", None)
    return str(getattr(call, "__module__", ""))


def remove_processors(module_names: set[str]) -> None:
    import nonebot.message as message

    for name in (
        "_event_preprocessors",
        "_event_postprocessors",
        "_run_preprocessors",
        "_run_postprocessors",
    ):
        registry = getattr(message, name)
        for item in list(registry):
            if _dependent_module(item) in module_names:
                registry.discard(item)


def remove_driver_hooks(driver: Any, module_names: set[str]) -> None:
    lifespan = driver._lifespan
    for name in ("_startup_funcs", "_ready_funcs", "_shutdown_funcs"):
        registry = getattr(lifespan, name)
        for item in list(registry):
            func = getattr(item, "func", item)
            if getattr(func, "__module__", "") in module_names:
                with contextlib.suppress(ValueError, KeyError):
                    registry.remove(item)
    for name in ("_bot_connection_hook", "_bot_disconnection_hook"):
        registry = getattr(driver, name)
        for item in list(registry):
            if _dependent_module(item) in module_names:
                registry.discard(item)


def remove_priority_hooks(module_names: set[str]) -> None:
    from zhenxun.utils.manager.priority_manager import PriorityLifecycle

    for priority_map in PriorityLifecycle._data.values():
        for priority, funcs in list(priority_map.items()):
            priority_map[priority] = [
                func
                for func in funcs
                if getattr(func, "__module__", "") not in module_names
            ]
            if not priority_map[priority]:
                del priority_map[priority]


def remove_plugin_init(module_names: set[str]) -> None:
    from zhenxun.services.plugin_init import PluginInitManager

    for module_name in list(PluginInitManager.plugins):
        if module_name in module_names:
            PluginInitManager.plugins.pop(module_name, None)


def clean_matchers(matchers: Iterable[type]) -> None:
    for matcher in matchers:
        clean = getattr(matcher, "clean", None)
        try:
            if callable(clean):
                clean()
            else:
                matcher.destroy()
        except (KeyError, ValueError):
            continue


def remove_plugins(plugins: Iterable[Any]) -> None:
    import nonebot.plugin as plugin_module

    for plugin in sorted(plugins, key=lambda item: item.id_.count(":"), reverse=True):
        if plugin.parent_plugin:
            plugin.parent_plugin.sub_plugins.discard(plugin)
        plugin_module._plugins.pop(plugin.id_, None)
        with contextlib.suppress(AttributeError):
            plugin.module.__dict__.pop("__plugin__", None)


def remove_nested_managers(managers: Iterable[Any]) -> None:
    import nonebot.plugin as plugin_module

    for manager in managers:
        with contextlib.suppress(ValueError):
            plugin_module._managers.remove(manager)
