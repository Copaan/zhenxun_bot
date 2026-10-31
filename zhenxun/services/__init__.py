"""Public service facade with lazy imports.

Importing a lightweight service such as ``logger`` must not initialize the AI,
renderer, database, and scheduler stacks. Keep this module side-effect free;
NoneBot library plugins are bootstrapped explicitly by the worker.
"""

from __future__ import annotations

from importlib import import_module
from importlib.abc import Loader, MetaPathFinder
from importlib.util import spec_from_loader
import sys
from typing import Any

_EXPORTS: dict[str, tuple[str, str]] = {
    "resolve_business_identity": (
        "zhenxun.services.business_identity",
        "resolve_business_identity",
    ),
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


_LLM_SOURCES = (
    "zhenxun.services.ai.compat",
    "zhenxun.services.ai.llm",
    "zhenxun.services.ai.core.messages",
    "zhenxun.services.ai.core.options",
    "zhenxun.services.ai.core.exceptions",
    "zhenxun.services.ai.core.models",
)
_COMPAT = ("zhenxun.services.ai.compat",)
_LEGACY_MODULES: dict[str, tuple[str, ...]] = {
    "zhenxun.services.llm": _LLM_SOURCES,
    "zhenxun.services.llm.types": _LLM_SOURCES,
    "zhenxun.services.llm.types.exceptions": ("zhenxun.services.ai.core.exceptions",),
    "zhenxun.services.llm.types.models": ("zhenxun.services.ai.core.models",),
    "zhenxun.services.ai.context.memory.builder": _COMPAT,
    "zhenxun.services.ai.context.memory.manager": _COMPAT,
    "zhenxun.services.ai.llm.builder": _COMPAT,
    "zhenxun.services.ai.llm.engine": _COMPAT,
    "zhenxun.services.ai.llm.engine.router": _COMPAT,
    "zhenxun.services.ai.llm.system": _COMPAT,
    "zhenxun.services.ai.llm.system.capabilities": _COMPAT,
}
"""外部 AI 插件使用的旧模块路径，到提供其名称的现有模块"""
_MISSING = object()


class _LegacyModules(MetaPathFinder, Loader):
    """把外部 AI 插件使用的旧模块路径映射到现有实现，名称在使用时才导入"""

    def find_spec(self, fullname: str, path: Any, target: Any = None):
        if fullname in _LEGACY_MODULES:
            return spec_from_loader(fullname, self, is_package=True)
        return None

    def create_module(self, spec: Any) -> None:
        return None

    def exec_module(self, module: Any) -> None:
        sources = _LEGACY_MODULES[module.__name__]

        def __getattr__(name: str) -> Any:
            if name.startswith("__"):
                raise AttributeError(name)
            for source in sources:
                value = getattr(import_module(source), name, _MISSING)
                if value is not _MISSING:
                    return value
            raise AttributeError(name)

        module.__getattr__ = __getattr__


sys.meta_path[:] = [
    finder
    for finder in sys.meta_path
    if type(finder).__qualname__ != _LegacyModules.__qualname__
    or type(finder).__module__ != __name__
]
sys.meta_path.insert(0, _LegacyModules())
