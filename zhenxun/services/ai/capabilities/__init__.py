"""Lazy public facade for AI capabilities."""

from importlib import import_module
from typing import Any

_EXPORTS = {
    "AbstractCapability": (
        "zhenxun.services.ai.capabilities.base",
        "AbstractCapability",
    ),
    "CapabilityQuery": ("zhenxun.services.ai.capabilities.manager", "CapabilityQuery"),
    "CapabilitySource": (
        "zhenxun.services.ai.capabilities.manager",
        "CapabilitySource",
    ),
    "CombinedCapability": (
        "zhenxun.services.ai.capabilities.wrappers",
        "CombinedCapability",
    ),
    "DynamicCapability": (
        "zhenxun.services.ai.capabilities.wrappers",
        "DynamicCapability",
    ),
    "WrapModelRequestHandler": (
        "zhenxun.services.ai.capabilities.base",
        "WrapModelRequestHandler",
    ),
    "WrapRunHandler": ("zhenxun.services.ai.capabilities.base", "WrapRunHandler"),
    "WrapToolExecuteHandler": (
        "zhenxun.services.ai.capabilities.base",
        "WrapToolExecuteHandler",
    ),
    "WrapToolValidateHandler": (
        "zhenxun.services.ai.capabilities.base",
        "WrapToolValidateHandler",
    ),
    "WrapperCapability": (
        "zhenxun.services.ai.capabilities.wrappers",
        "WrapperCapability",
    ),
    "capability": ("zhenxun.services.ai.capabilities.manager", "capability"),
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
