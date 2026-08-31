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
    reasons: set[str] = field(default_factory=set)
    classification: ReloadClassification = ReloadClassification.HOT_RELOADABLE
    fingerprint: str = ""
    in_flight: int = 0
    draining: bool = False
    last_error: str | None = None

    def public_dict(self) -> dict[str, Any]:
        return {
            "plugin_id": self.plugin_id,
            "module": self.module_name,
            "classification": self.classification.value,
            "reasons": sorted(self.reasons),
            "dependencies": sorted(self.dependencies),
            "fingerprint": self.fingerprint[:12],
            "last_error": self.last_error,
        }


@dataclass(slots=True)
class RuntimeOperation:
    mode: ApplyMode
    status: str
    changed: list[str]
    reason: str | None = None
    generation: int = 0
    config_keys: list[str] = field(default_factory=list)

    def public_dict(self) -> dict[str, Any]:
        return {
            "apply_mode": self.mode.value,
            "status": self.status,
            "changed": self.changed,
            "reason": self.reason,
            "generation": self.generation,
            "config_keys": self.config_keys,
        }
