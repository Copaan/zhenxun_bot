"""Public service facade with lazy imports.

Importing a lightweight service such as ``logger`` must not initialize the AI,
renderer, database, and scheduler stacks. Keep this module side-effect free;
NoneBot library plugins are bootstrapped explicitly by the worker.
"""

from __future__ import annotations

from importlib import import_module
from typing import Any

_EXPORTS: dict[str, tuple[str, str]] = {
    "ExecutionPolicy": ("zhenxun.services.scheduler", "ExecutionPolicy"),
    "Model": ("zhenxun.services.db_context", "Model"),
    "PluginInit": ("zhenxun.services.plugin_init", "PluginInit"),
    "PluginInitManager": ("zhenxun.services.plugin_init", "PluginInitManager"),
    "ScheduleContext": ("zhenxun.services.scheduler", "ScheduleContext"),
    "Trigger": ("zhenxun.services.scheduler", "Trigger"),
    "avatar_service": ("zhenxun.services.avatar_service", "avatar_service"),
    "chat": ("zhenxun.services.ai.llm.api", "chat"),
    "disconnect": ("zhenxun.services.db_context", "disconnect"),
    "group_settings_service": (
        "zhenxun.services.group_settings_service",
        "group_settings_service",
    ),
    "logger": ("zhenxun.services.log", "logger"),
    "renderer_service": ("zhenxun.services.renderer", "renderer_service"),
    "scheduler_manager": ("zhenxun.services.scheduler", "scheduler_manager"),
    "with_db_timeout": ("zhenxun.services.db_context", "with_db_timeout"),
}

__all__ = list(_EXPORTS)


def __getattr__(name: str) -> Any:
    try:
        module_name, attribute = _EXPORTS[name]
    except KeyError as error:
        raise AttributeError(name) from error
    value = getattr(import_module(module_name), attribute)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    return sorted({*globals(), *__all__})
