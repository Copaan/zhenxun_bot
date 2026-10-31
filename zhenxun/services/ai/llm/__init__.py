"""大模型调用接口，按需导入以避免循环依赖"""

from __future__ import annotations

from importlib import import_module
from typing import Any

_EXPORTS: dict[str, tuple[str, str]] = {
    "LLMException": ("zhenxun.services.ai.core.exceptions", "LLMException"),
    "LLMMessage": ("zhenxun.services.ai.core.messages", "LLMMessage"),
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
    "list_available_models": (
        "zhenxun.services.ai.llm.manager",
        "list_available_models",
    ),
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
