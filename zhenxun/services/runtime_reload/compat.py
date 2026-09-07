from __future__ import annotations

from collections.abc import Iterable
import contextlib
import inspect
from typing import Any


class NoneBotCompatibilityError(RuntimeError):
    pass


def verify_nonebot_compatibility() -> None:
    try:
        from dataclasses import fields

        import nonebot
        from nonebot.dependencies import Dependent
        from nonebot.dependencies.utils import get_typed_annotation
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
            get_typed_annotation,
        )
        if not {"call", "params", "parameterless"}.issubset(
            {field.name for field in fields(Dependent)}
        ):
            raise NoneBotCompatibilityError("nonebot_dependent_incompatible")
        if not inspect.iscoroutinefunction(Dependent.__call__) or not callable(
            get_typed_annotation
        ):
            raise NoneBotCompatibilityError("nonebot_dependent_incompatible")
        for name in (
            "_event_preprocessors",
            "_event_postprocessors",
            "_run_preprocessors",
            "_run_postprocessors",
        ):
            if not isinstance(getattr(message, name, None), set):
                raise NoneBotCompatibilityError("nonebot_hook_registry_incompatible")
        driver = nonebot.get_driver()
        for name in ("_bot_connection_hook", "_bot_disconnection_hook"):
            if not isinstance(getattr(driver, name, None), set):
                raise NoneBotCompatibilityError("nonebot_hook_registry_incompatible")
        for name in ("_startup_funcs", "_ready_funcs", "_shutdown_funcs"):
            if not isinstance(getattr(driver._lifespan, name, None), list):
                raise NoneBotCompatibilityError("nonebot_lifespan_incompatible")
    except (AttributeError, ImportError, TypeError, ValueError) as e:
        raise NoneBotCompatibilityError("nonebot_private_api_missing") from e
    if not all(container is not None for container in required):
        raise NoneBotCompatibilityError("nonebot_private_api_invalid")
    dispatch = getattr(message, "check_and_run_matcher", None)
    if not callable(dispatch) or not {"Matcher", "bot", "event", "state"}.issubset(
        inspect.signature(dispatch).parameters
    ):
        raise NoneBotCompatibilityError("nonebot_matcher_dispatch_incompatible")


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


def remove_bot_api_hooks(module_names: set[str]) -> None:
    try:
        from nonebot.internal.adapter import Bot
    except ImportError:
        return
    for name in ("_calling_api_hook", "_called_api_hook"):
        registry = getattr(Bot, name, None)
        if registry is None:
            continue
        for item in list(registry):
            if _dependent_module(item) in module_names:
                registry.discard(item)


def remove_priority_hooks(module_names: set[str]) -> None:
    from zhenxun.utils.manager.priority_manager import PriorityLifecycle

    for priority_map in PriorityLifecycle._data.values():
        for priority, funcs in list(priority_map.items()):
            removed = {
                func
                for func in funcs
                if getattr(func, "__module__", "") in module_names
            }
            priority_map[priority] = [
                func
                for func in funcs
                if getattr(func, "__module__", "") not in module_names
            ]
            for func in removed:
                PriorityLifecycle._metadata.pop(func, None)
            if not priority_map[priority]:
                del priority_map[priority]


def remove_plugin_init(module_names: set[str]) -> None:
    from zhenxun.services.plugin_init import PluginInitManager

    PluginInitManager.remove_registrations(module_names)


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
