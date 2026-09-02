from __future__ import annotations

from io import StringIO
import os
from pathlib import Path
from typing import Any

from dotenv import dotenv_values

from zhenxun.utils._restart_utils import (
    clear_restart_pending,
    issue_restart_ticket,
    mark_restart_pending,
)

APPLY_NO_CHANGE = "no_change"
APPLY_CONFIG_RELOADED = "config_reloaded"
APPLY_HOT_RELOADED = "hot_reloaded"
APPLY_NEW_SESSION = "new_session"
APPLY_RESTART_PENDING = "restart_pending"
APPLY_RESTART_REQUESTED = "restart_requested"
APPLY_FAILED = "failed"

_ENV_SOURCE = Path(".env.dev") if Path(".env.dev").exists() else Path(".env")


def normalize_env(content: str) -> dict[str, str | None]:
    return {
        str(key): None if value is None else str(value)
        for key, value in dotenv_values(stream=StringIO(content)).items()
    }


def _startup_env() -> dict[str, str | None]:
    try:
        return normalize_env(_ENV_SOURCE.read_text(encoding="utf-8"))
    except OSError:
        return {}


_STARTUP_ENV_VALUES = _startup_env()


def env_restart_impact(content: str) -> tuple[bool, list[str]]:
    candidate = normalize_env(content)
    changed = sorted(
        key
        for key in _STARTUP_ENV_VALUES.keys() | candidate.keys()
        if _STARTUP_ENV_VALUES.get(key) != candidate.get(key)
    )
    return bool(changed), changed


def env_change_impact(
    before: str,
    after: str,
    managed_keys: set[str] | list[str] | None = None,
) -> tuple[list[str], list[str]]:
    previous = normalize_env(before)
    candidate = normalize_env(after)
    allowed = {str(key) for key in managed_keys} if managed_keys is not None else None
    request_changed = sorted(
        key
        for key in previous.keys() | candidate.keys()
        if previous.get(key) != candidate.get(key)
        and (allowed is None or key in allowed)
    )
    pending = sorted(
        key
        for key in _STARTUP_ENV_VALUES.keys() | candidate.keys()
        if _STARTUP_ENV_VALUES.get(key) != candidate.get(key)
        and (allowed is None or key in allowed)
    )
    return request_changed, pending


def update_pending_restart(
    source: str,
    reasons: list[str] | set[str],
    *,
    issue_ticket: bool = True,
) -> bool:
    normalized = sorted({str(reason) for reason in reasons if str(reason).strip()})
    if not normalized:
        clear_restart_pending(source)
        return False
    mark_restart_pending(source, normalized)
    launcher_managed = bool(os.getenv("ZHENXUN_LAUNCHER_PID"))
    if launcher_managed and issue_ticket:
        issue_restart_ticket(source, ttl_seconds=10 * 60)
    return launcher_managed


def apply_result_data(
    *,
    apply_mode: str,
    changed_keys: list[str] | None = None,
    restart_required: bool = False,
    hot_reloaded: bool | None = None,
    reason_codes: list[str] | None = None,
    access_urls: list[str] | None = None,
    access_targets: list[dict[str, str]] | None = None,
    **extra: Any,
) -> dict[str, Any]:
    return {
        **extra,
        "apply_mode": apply_mode,
        "changed_keys": changed_keys or [],
        "hot_reloaded": (
            apply_mode in {APPLY_CONFIG_RELOADED, APPLY_HOT_RELOADED}
            if hot_reloaded is None
            else hot_reloaded
        ),
        "restart_required": restart_required,
        "restart_available": restart_required
        and bool(os.getenv("ZHENXUN_LAUNCHER_PID")),
        "reason_codes": reason_codes or [],
        "access_urls": access_urls or [],
        "access_targets": access_targets or [],
    }


__all__ = [
    "APPLY_CONFIG_RELOADED",
    "APPLY_FAILED",
    "APPLY_HOT_RELOADED",
    "APPLY_NEW_SESSION",
    "APPLY_NO_CHANGE",
    "APPLY_RESTART_PENDING",
    "APPLY_RESTART_REQUESTED",
    "apply_result_data",
    "env_change_impact",
    "env_restart_impact",
    "normalize_env",
    "update_pending_restart",
]
