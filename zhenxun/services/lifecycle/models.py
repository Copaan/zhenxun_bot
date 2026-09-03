from __future__ import annotations

from collections.abc import Awaitable
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Literal, Protocol, runtime_checkable
import uuid

from strenum import StrEnum

LifecycleStage = Literal["management", "runtime", "warmup"]
FailurePolicy = Literal["fatal", "degrade"]
RestartPolicy = Literal["in_place", "component", "worker"]
LifecycleScope = Literal[
    "launcher",
    "worker",
    "infrastructure",
    "plugin",
    "bot_connection",
    "operation",
    "event",
    "request",
    "task",
]

SCOPE_DEPTH: dict[str, int] = {
    "launcher": 0,
    "worker": 1,
    "infrastructure": 2,
    "plugin": 2,
    "bot_connection": 3,
    "operation": 3,
    "event": 4,
    "request": 4,
    "task": 5,
}


class ComponentState(StrEnum):
    DECLARED = "declared"
    STARTING = "starting"
    READY = "ready"
    DEGRADED = "degraded"
    QUIESCING = "quiescing"
    STOPPING = "stopping"
    STOPPED = "stopped"
    FAILED = "failed"


class LeaseState(StrEnum):
    PREPARED = "prepared"
    ACTIVE = "active"
    REVOKING = "revoking"
    REVOKED = "revoked"
    FAILED = "failed"


class ScopeState(StrEnum):
    OPEN = "open"
    QUIESCING = "quiescing"
    CLOSING = "closing"
    CLOSED = "closed"
    FAILED = "failed"


@dataclass(frozen=True, slots=True)
class ComponentSpec:
    component_id: str
    scope: LifecycleScope = "infrastructure"
    stage: LifecycleStage = "runtime"
    depends_on: tuple[str, ...] = ()
    provides: tuple[str, ...] = ()
    resource_group: str | None = None
    timeout: float | None = None
    drain_timeout: float = 10.0
    cancel_timeout: float = 5.0
    finalizer_timeout: float = 10.0
    failure_policy: FailurePolicy = "fatal"
    restart_policy: RestartPolicy = "worker"
    config_keys: tuple[str, ...] = ()
    priority: int = 0
    stop_priority: int | None = None
    parallel_safe: bool = False
    source: str = "native"

    def __post_init__(self) -> None:
        if not self.component_id or self.component_id.strip() != self.component_id:
            raise ValueError("component_id_invalid")
        if self.component_id in self.depends_on:
            raise ValueError("component_self_dependency")
        if self.timeout is not None and self.timeout <= 0:
            raise ValueError("component_timeout_invalid")
        if min(self.drain_timeout, self.cancel_timeout, self.finalizer_timeout) <= 0:
            raise ValueError("component_cleanup_timeout_invalid")
        if self.scope not in SCOPE_DEPTH:
            raise ValueError("component_scope_invalid")

    def public_dict(self) -> dict[str, Any]:
        return {
            "component_id": self.component_id,
            "scope": self.scope,
            "stage": self.stage,
            "depends_on": list(self.depends_on),
            "provides": list(self.provides),
            "resource_group": self.resource_group,
            "timeout": self.timeout,
            "drain_timeout": self.drain_timeout,
            "cancel_timeout": self.cancel_timeout,
            "finalizer_timeout": self.finalizer_timeout,
            "failure_policy": self.failure_policy,
            "restart_policy": self.restart_policy,
            "config_keys": list(self.config_keys),
            "priority": self.priority,
            "stop_priority": self.stop_priority,
            "parallel_safe": self.parallel_safe,
            "source": self.source,
        }


@dataclass(slots=True)
class ResourceReceipt:
    receipt_id: str
    provider: str
    resource_type: str
    owner_id: str
    incarnation_id: str | None = None
    ownership: str = "exclusive"
    reversible: bool = True
    state: str = "active"
    created_at: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )
    completed_at: str | None = None
    error_code: str | None = None
    detail: dict[str, Any] = field(default_factory=dict)

    def public_dict(self) -> dict[str, Any]:
        return {
            "receipt_id": self.receipt_id,
            "provider": self.provider,
            "resource_type": self.resource_type,
            "owner_id": self.owner_id,
            "incarnation_id": self.incarnation_id,
            "ownership": self.ownership,
            "reversible": self.reversible,
            "state": self.state,
            "created_at": self.created_at,
            "completed_at": self.completed_at,
            "error_code": self.error_code,
            "detail": dict(self.detail),
        }


@runtime_checkable
class CompositeHandle(Protocol):
    """A bounded lifecycle contract for services that own internal resources."""

    async def quiesce(self) -> None: ...

    async def close(self) -> None: ...

    def health(self) -> Any | Awaitable[Any]: ...

    def snapshot(self) -> dict[str, Any] | Awaitable[dict[str, Any]]: ...

    def resource_snapshot(
        self,
    ) -> list[ResourceReceipt] | Awaitable[list[ResourceReceipt]]: ...


@dataclass(slots=True)
class RuntimeHandle:
    value: Any = None
    health: str = "healthy"
    metadata: dict[str, Any] = field(default_factory=dict)
    controller: CompositeHandle | None = None


@dataclass(slots=True)
class ScopeRecord:
    scope_id: str
    scope: LifecycleScope
    parent_id: str
    owner_id: str
    generation: int
    state: ScopeState = ScopeState.OPEN
    active_activities: int = 0
    created_order: int = 0
    created_at: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )
    closed_at: str | None = None
    error_code: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
    resources: list[ResourceReceipt] = field(default_factory=list)

    def public_dict(self) -> dict[str, Any]:
        return {
            "scope_id": self.scope_id,
            "scope": self.scope,
            "parent_id": self.parent_id,
            "owner_id": self.owner_id,
            "generation": self.generation,
            "state": self.state.value,
            "active_activities": self.active_activities,
            "created_order": self.created_order,
            "created_at": self.created_at,
            "closed_at": self.closed_at,
            "error_code": self.error_code,
            "metadata": dict(self.metadata),
            "resource_count": len(self.resources),
            "resource_counts": _resource_counts(self.resources),
        }


@dataclass(slots=True)
class PluginIncarnation:
    plugin_id: str
    boot_id: str
    source_digest: str = ""
    dependency_generation: int | None = None
    incarnation_id: str = field(default_factory=lambda: uuid.uuid4().hex)
    lease_state: LeaseState = LeaseState.PREPARED
    created_at: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )

    @property
    def accepts_work(self) -> bool:
        return self.lease_state is LeaseState.ACTIVE

    def public_dict(self) -> dict[str, Any]:
        return {
            "plugin_id": self.plugin_id,
            "incarnation_id": self.incarnation_id,
            "boot_id": self.boot_id,
            "source_digest": self.source_digest[:12],
            "dependency_generation": self.dependency_generation,
            "lease_state": self.lease_state.value,
            "created_at": self.created_at,
        }


@dataclass(slots=True)
class ComponentRuntime:
    spec: ComponentSpec
    state: ComponentState = ComponentState.DECLARED
    runtime_generation: int = 0
    observed_generation: int = 0
    started_at: str | None = None
    stopped_at: str | None = None
    duration_ms: float | None = None
    health: str = "unknown"
    error_code: str | None = None
    last_health_checked_at: str | None = None
    consecutive_health_failures: int = 0
    active_activities: int = 0
    metadata: dict[str, Any] = field(default_factory=dict)
    resources: list[ResourceReceipt] = field(default_factory=list)

    def public_dict(self) -> dict[str, Any]:
        return {
            **self.spec.public_dict(),
            "state": self.state.value,
            "runtime_generation": self.runtime_generation,
            "observed_generation": self.observed_generation,
            "started_at": self.started_at,
            "stopped_at": self.stopped_at,
            "duration_ms": self.duration_ms,
            "health": self.health,
            "error_code": self.error_code,
            "last_health_checked_at": self.last_health_checked_at,
            "consecutive_health_failures": self.consecutive_health_failures,
            "active_activities": self.active_activities,
            "metadata": dict(self.metadata),
            "resource_count": len(self.resources),
            "resource_counts": _resource_counts(self.resources),
        }


def _resource_counts(resources: list[ResourceReceipt]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for receipt in resources:
        key = f"{receipt.provider}:{receipt.resource_type}:{receipt.state}"
        counts[key] = counts.get(key, 0) + 1
    return dict(sorted(counts.items()))


@dataclass(slots=True)
class LifecycleOperationResult:
    apply_effect: str
    affected_components: list[str]
    reason_codes: list[str] = field(default_factory=list)
    rollback_state: str = "none"
    runtime_generation: int = 0

    def public_dict(self) -> dict[str, Any]:
        return {
            "apply_effect": self.apply_effect,
            "affected_components": list(self.affected_components),
            "reason_codes": list(self.reason_codes),
            "rollback_state": self.rollback_state,
            "runtime_generation": self.runtime_generation,
        }
