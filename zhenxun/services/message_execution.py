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
    business_identities: dict = field(default_factory=dict)
    deferred_reason: str | None = None
    retry_blocked: bool = False
    preconditions: list = field(default_factory=list)

    @property
    def can_retry(self) -> bool:
        return bool(
            self.deferred_reason
            and not self.handlers_started
            and not self.operations
            and not self.deliveries
            and not self.errors
            and not self.retry_blocked
            and all(item.retry_safe for item in self.preconditions)
        )


class MessageExecutionDeferred(RuntimeError):
    """A temporary dependency failure; never an authorization exemption."""


def defer_execution(reason: str) -> None:
    from .cache.diagnostics import record_availability_fallback

    record_availability_fallback("permission_deferred")
    execution = current_execution.get()
    if execution is not None and execution.deferred_reason is None:
        execution.deferred_reason = reason


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
