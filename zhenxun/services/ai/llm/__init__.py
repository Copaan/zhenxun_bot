"""Lazy public facade for LLM APIs and contracts."""

from __future__ import annotations

from importlib import import_module
from typing import Any

_EXPORTS: dict[str, tuple[str, str]] = {
    "AudioResponse": ("zhenxun.services.ai.core.messages", "AudioResponse"),
    "ChatResponse": ("zhenxun.services.ai.core.messages", "ChatResponse"),
    "IntentBuilder": ("zhenxun.services.ai.llm.builder", "IntentBuilder"),
    "LLMContentPart": ("zhenxun.services.ai.core.messages", "LLMContentPart"),
    "LLMException": ("zhenxun.services.ai.core.exceptions", "LLMException"),
    "LLMMessage": ("zhenxun.services.ai.core.messages", "LLMMessage"),
    "TTSConfig": ("zhenxun.services.ai.core.options", "TTSConfig"),
    "chat": ("zhenxun.services.ai.llm.api", "chat"),
    "create_image": ("zhenxun.services.ai.llm.api", "create_image"),
    "create_speech": ("zhenxun.services.ai.llm.api", "create_speech"),
    "embed": ("zhenxun.services.ai.llm.api", "embed"),
    "generate": ("zhenxun.services.ai.llm.api", "generate"),
    "generate_structured": (
        "zhenxun.services.ai.llm.api",
        "generate_structured",
    ),
    "get_default_model": ("zhenxun.services.ai.llm.manager", "get_default_model"),
    "rerank": ("zhenxun.services.ai.llm.api", "rerank"),
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
