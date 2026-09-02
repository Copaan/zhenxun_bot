"""Lazy public facade for AI tools."""

from importlib import import_module
from typing import Any

_EXPORTS = {
    "BaseToolkit": ("zhenxun.services.ai.tools.core.toolkit", "BaseToolkit"),
    "Native": ("zhenxun.services.ai.tools.providers.builtin.native", "Native"),
    "Rules": ("zhenxun.services.ai.tools.core.decorators", "Rules"),
    "ToolOptions": ("zhenxun.services.ai.tools.models", "ToolOptions"),
    "ToolResult": ("zhenxun.services.ai.tools.models", "ToolResult"),
    "bind_matcher": (
        "zhenxun.services.ai.tools.bridges.matcher_bridge",
        "bind_matcher",
    ),
    "tool": ("zhenxun.services.ai.tools.core.decorators", "tool"),
    "toolkit": ("zhenxun.services.ai.tools.core.decorators", "toolkit"),
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
