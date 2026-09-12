"""Event-local durable identities, independent of login or connection epochs."""

from contextvars import ContextVar
from dataclasses import dataclass, field
from hashlib import sha256


@dataclass
class MessageExecution:
    identity: str
    received_at: float | None = None
    operations: dict = field(default_factory=dict)
    errors: list = field(default_factory=list)
    deliveries: dict = field(default_factory=dict)
    handlers_started: int = 0


class MessageExecutionUnavailable(RuntimeError):
    """A durable message no longer has the capability needed to execute."""


current_execution: ContextVar[MessageExecution | None] = ContextVar(
    "message_execution", default=None
)
current_dispatch_lease: ContextVar[object | None] = ContextVar(
    "dispatch_lease", default=None
)


def operation_key(kind: str, user_id: str) -> str | None:
    execution = current_execution.get()
    if execution is None:
        return None
    from nonebot.matcher import current_matcher

    matcher = current_matcher.get(None)
    owner = (str(getattr(matcher, "module", "")), str(getattr(matcher, "lineno", "")))
    key = owner, kind, str(user_id)
    ordinal = execution.operations.get(key, 0)
    execution.operations[key] = ordinal + 1
    return sha256(repr((execution.identity, key, ordinal)).encode()).hexdigest()
