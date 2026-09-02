"""Lazy public facade for AI run state."""

from importlib import import_module
from typing import Any

_EXPORTS = {
    "AgentRunResult": ("zhenxun.services.ai.run.models", "AgentRunResult"),
    "AgentTask": ("zhenxun.services.ai.run.models", "AgentTask"),
    "BlackboardManager": ("zhenxun.services.ai.run.blackboard", "BlackboardManager"),
    "CancellationToken": ("zhenxun.services.ai.core.models", "CancellationToken"),
    "HITLController": ("zhenxun.services.ai.run.hitl", "HITLController"),
    "Hidden": ("zhenxun.services.ai.run.di", "Hidden"),
    "Hooks": ("zhenxun.services.ai.run.hooks", "Hooks"),
    "Inject": ("zhenxun.services.ai.run.di", "Inject"),
    "NoneBotDeps": ("zhenxun.services.ai.run.context", "NoneBotDeps"),
    "RunContext": ("zhenxun.services.ai.run.context", "RunContext"),
    "RunIntent": ("zhenxun.services.ai.run.models", "RunIntent"),
    "StreamedRunResult": ("zhenxun.services.ai.run.models", "StreamedRunResult"),
    "UIController": ("zhenxun.services.ai.run.ui", "UIController"),
    "get_current_run_context": (
        "zhenxun.services.ai.run.context",
        "get_current_run_context",
    ),
    "session_manager": ("zhenxun.services.ai.run.session", "session_manager"),
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
