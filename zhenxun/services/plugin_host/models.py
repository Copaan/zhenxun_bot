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
    notes: list[str] = field(default_factory=list)
    stopped: list[str] = field(default_factory=list)
    """加载失败而停用、可修复后重新加载的插件"""

    def public_dict(self) -> dict[str, Any]:
        return {
            "apply_mode": self.mode.value,
            "status": self.status,
            "changed": self.changed,
            "reason": self.reason,
            "generation": self.generation,
            "config_keys": self.config_keys,
            "notes": list(self.notes),
            "stopped": list(self.stopped),
            "reason_codes": [self.reason] if self.reason else [],
        }
