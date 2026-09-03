from importlib import import_module
from typing import Any

_EXPORTS = {
    "AgentEvent": (".messages", "AgentEvent"),
    "AgentMessage": (".messages", "AgentMessage"),
    "GenerationConfig": (".options", "GenerationConfig"),
    "HandoffEvent": (".messages", "HandoffEvent"),
    "LLMException": (".exceptions", "LLMException"),
    "LLMMessage": (".messages", "LLMMessage"),
    "PromptTemplate": (".templates", "PromptTemplate"),
    "TaskLifecycleEvent": (".messages", "TaskLifecycleEvent"),
}


def __getattr__(name: str) -> Any:
    target = _EXPORTS.get(name)
    if target is None:
        raise AttributeError(name)
    module_name, attribute = target
    value = getattr(import_module(module_name, __name__), attribute)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    return sorted({*globals(), *_EXPORTS})


__all__ = [
    "AgentEvent",
    "AgentMessage",
    "GenerationConfig",
    "HandoffEvent",
    "LLMException",
    "LLMMessage",
    "PromptTemplate",
    "TaskLifecycleEvent",
]
