from importlib import import_module
from typing import Any

_EXPORTS = {
    "DelegateTool": ("zhenxun.services.ai.tools.bridges.delegate", "DelegateTool"),
    "HandoffTool": ("zhenxun.services.ai.tools.bridges.handoff", "HandoffTool"),
    "MatcherTool": (
        "zhenxun.services.ai.tools.bridges.matcher_bridge",
        "MatcherTool",
    ),
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
