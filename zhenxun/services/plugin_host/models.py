from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from strenum import StrEnum


class ReloadClassification(StrEnum):
    HOT_RELOADABLE = "hot_reloadable"
    RESTART_REQUIRED = "restart_required"


class ApplyMode(StrEnum):
    HOT_RELOADED = "hot_reloaded"
    RESTART_REQUESTED = "restart_requested"
    RESTART_PENDING = "restart_pending"
    FAILED = "failed"
    CONFIG_RELOADED = "config_reloaded"
    COMPONENT_RESTARTED = "component_restarted"


@dataclass(slots=True)
class RuntimeOperation:
    """插件或配置变更的应用结果"""

    mode: ApplyMode
    status: str
    changed: list[str]
    reason: str | None = None
    generation: int = 0
    config_keys: list[str] = field(default_factory=list)
    component_effects: dict[str, str] = field(default_factory=dict)
    affected_components: list[str] = field(default_factory=list)
    rollback_state: str = "none"
    notes: list[str] = field(default_factory=list)

    def public_dict(self) -> dict[str, Any]:
        return {
            "apply_mode": self.mode.value,
            "status": self.status,
            "changed": self.changed,
            "reason": self.reason,
            "generation": self.generation,
            "config_keys": self.config_keys,
            "component_effects": dict(self.component_effects),
            "affected_components": list(self.affected_components),
            "rollback_state": self.rollback_state,
            "notes": list(self.notes),
            "reason_codes": [self.reason] if self.reason else [],
        }
