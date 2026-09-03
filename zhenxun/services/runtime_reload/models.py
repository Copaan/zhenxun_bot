from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from strenum import StrEnum


class ReloadClassification(StrEnum):
    HOT_RELOADABLE = "hot_reloadable"
    RESTART_REQUIRED = "restart_required"
    FAILED = "failed"


class ApplyMode(StrEnum):
    HOT_RELOADED = "hot_reloaded"
    RESTART_REQUESTED = "restart_requested"
    RESTART_PENDING = "restart_pending"
    FAILED = "failed"
    CONFIG_RELOADED = "config_reloaded"
    WEBUI_REFRESH = "webui_refresh"
    COMPONENT_RESTARTED = "component_restarted"
    ROLLED_BACK = "rolled_back"


@dataclass(slots=True)
class PluginUnit:
    plugin_id: str
    module_name: str
    manager: Any
    root: Path | None
    nested_managers: list[Any] = field(default_factory=list)
    module_names: set[str] = field(default_factory=set)
    files: set[Path] = field(default_factory=set)
    model_files: set[Path] = field(default_factory=set)
    dependencies: set[str] = field(default_factory=set)
    config_dependencies: set[tuple[str, str]] = field(default_factory=set)
    env_dependencies: set[str] = field(default_factory=set)
    imported_modules: set[str] = field(default_factory=set)
    import_time_dependency_calls: set[str] = field(default_factory=set)
    reasons: set[str] = field(default_factory=set)
    classification: ReloadClassification = ReloadClassification.HOT_RELOADABLE
    fingerprint: str = ""
    file_cache: dict[str, dict[str, Any]] = field(default_factory=dict)
    in_flight: int = 0
    draining: bool = False
    last_error: str | None = None
    incarnation_id: str | None = None
    resource_receipts: list[Any] = field(default_factory=list)

    def public_dict(self) -> dict[str, Any]:
        return {
            "plugin_id": self.plugin_id,
            "module": self.module_name,
            "classification": self.classification.value,
            "reasons": sorted(self.reasons),
            "dependencies": sorted(self.dependencies),
            "env_dependencies": sorted(self.env_dependencies),
            "fingerprint": self.fingerprint[:12],
            "last_error": self.last_error,
            "incarnation_id": self.incarnation_id,
            "resource_count": len(self.resource_receipts),
            "resource_providers": sorted(
                {receipt.provider for receipt in self.resource_receipts}
            ),
        }


@dataclass(slots=True)
class RuntimeOperation:
    mode: ApplyMode
    status: str
    changed: list[str]
    reason: str | None = None
    generation: int = 0
    config_keys: list[str] = field(default_factory=list)
    component_effects: dict[str, str] = field(default_factory=dict)
    affected_components: list[str] = field(default_factory=list)
    rollback_state: str = "none"

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
        }
