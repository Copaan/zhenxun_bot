"""Lazy public facade for the AI runtime."""

from __future__ import annotations

from importlib import import_module
from typing import Any

_EXPORTS: dict[str, tuple[str, str]] = {
    "Agent": ("zhenxun.services.ai.flow", "Agent"),
    "Inject": ("zhenxun.services.ai.run", "Inject"),
    "IntentBuilder": ("zhenxun.services.ai.llm", "IntentBuilder"),
    "LLMMessage": ("zhenxun.services.ai.core.messages", "LLMMessage"),
    "Rules": ("zhenxun.services.ai.tools", "Rules"),
    "RunContext": ("zhenxun.services.ai.run", "RunContext"),
    "Team": ("zhenxun.services.ai.flow", "Team"),
    "Workflow": ("zhenxun.services.ai.flow", "Workflow"),
    "chat": ("zhenxun.services.ai.llm.api", "chat"),
    "generate_structured": (
        "zhenxun.services.ai.llm.api",
        "generate_structured",
    ),
    "tool": ("zhenxun.services.ai.tools", "tool"),
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
