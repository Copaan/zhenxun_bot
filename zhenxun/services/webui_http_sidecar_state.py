from __future__ import annotations

from datetime import datetime, timezone
import errno
import os
from pathlib import Path
from typing import Any

import psutil

from zhenxun.utils.atomic_json import mutate_json_locked, read_json_locked

_ERROR_CODES = frozenset(
    {
        "sidecar_error",
        "address_in_use",
        "address_unavailable",
        "permission_denied",
        "listener_bind_failed",
        "listener_identity_unverified",
        "upstream_certificate_mismatch",
        "upstream_timeout",
        "upstream_unavailable",
        "https_worker_not_ready",
        "startup_timeout",
        "sidecar_startup_failed",
        "sidecar_spawn_failed",
        "sidecar_startup_interrupted",
        "sidecar_unexpected_exit",
    }
)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _state_path() -> Path:
    return Path(
        os.getenv(
            "ZHENXUN_HTTP_SIDECAR_STATE_PATH",
            "data/runtime/webui-http-sidecar-v1.json",
        )
    )


def sanitized_sidecar_error(error: BaseException | str | None) -> str | None:
    if error is None:
        return None
    if isinstance(error, BaseException):
        name = type(error).__name__
        code = getattr(error, "errno", None) or getattr(error, "winerror", None)
        return f"{name}:{code}" if code is not None else name
    value = str(error).strip().casefold()
    return value if value in _ERROR_CODES else "sidecar_error"


def sidecar_error_details(error: BaseException | str, *, stage: str) -> dict[str, Any]:
    number = getattr(error, "errno", None)
    windows_number = getattr(error, "winerror", None)
    code = "sidecar_error"
    if number in {errno.EADDRINUSE, 10048} or windows_number == 10048:
        code = "address_in_use"
    elif number in {errno.EADDRNOTAVAIL, 10049} or windows_number == 10049:
        code = "address_unavailable"
    elif number in {errno.EACCES, errno.EPERM, 10013} or windows_number == 10013:
        code = "permission_denied"
    elif type(error).__name__ == "ServerFingerprintMismatch":
        code = "upstream_certificate_mismatch"
    elif isinstance(error, TimeoutError):
        code = "upstream_timeout" if stage.startswith("upstream") else "startup_timeout"
    elif isinstance(error, str):
        code = sanitized_sidecar_error(error)
    elif stage == "bind":
        code = "listener_bind_failed"
    elif stage.startswith("upstream"):
        code = "upstream_unavailable"
    elif stage == "spawn":
        code = "sidecar_spawn_failed"
    return {
        "error_code": code,
        "stage": stage,
        "errno": number if number is not None else windows_number,
        "last_error": sanitized_sidecar_error(error),
    }


def write_http_sidecar_state(**changes: Any) -> dict[str, Any]:
    result: dict[str, Any] = {}

    def update(current: dict[str, Any]) -> None:
        if changes.get("launcher_boot_id") not in {
            None,
            current.get("launcher_boot_id"),
        }:
            current.clear()
        previous_retries = current.get("retry_count") or 0
        current.setdefault("total_retries", previous_retries)
        if "retry_count" in changes:
            current["total_retries"] += max(
                0, (changes["retry_count"] or 0) - previous_retries
            )
        if changes.get("last_error") and (
            changes.get("last_error") != current.get("last_error")
            or changes.get("error_code") != current.get("error_code")
            or changes.get("startup_id", current.get("startup_id"))
            != current.get("startup_id")
        ):
            current.setdefault("first_error", changes["last_error"])
            if not current.get("first_error"):
                current["first_error"] = changes["last_error"]
            current["first_error_at"] = current.get("first_error_at") or _now()
            current["latest_error"] = changes["last_error"]
            current["latest_error_at"] = _now()
        current.update(changes)
        if changes.get("state") in {"ready", "disabled"}:
            current.update(retry_in_seconds=0, next_retry_at=None, retry_count=0)
        if changes.get("state") == "disabled":
            current.update(
                error_code=None,
                stage=None,
                errno=None,
                last_error=None,
                first_error=None,
                first_error_at=None,
                latest_error=None,
                latest_error_at=None,
                diagnostic_identity_verified=None,
                unverified_child_diagnostic=None,
            )
        current["version"] = 1
        current["updated_at"] = _now()
        result.update(current)

    mutate_json_locked(
        _state_path(),
        {},
        update,
        quarantine_corrupt=True,
    )
    return result


def read_http_sidecar_state() -> dict[str, Any]:
    expected_boot = os.getenv("ZHENXUN_LAUNCHER_BOOT_ID", "")
    if not expected_boot:
        return {"state": "disabled", "mode": "disabled"}
    value = read_json_locked(_state_path(), {}, quarantine_corrupt=True)
    if not isinstance(value, dict):
        return {}
    if value.get("launcher_boot_id") != expected_boot:
        return {"state": "unknown", "last_error": "listener_identity_unverified"}
    if value.get("state") in {"starting", "ready"}:
        if not listener_identity_matches(value):
            value = {
                **value,
                "state": "degraded",
                "active_connections": 0,
                **(
                    {}
                    if value.get("last_error")
                    else sidecar_error_details(
                        "listener_identity_unverified", stage="identity"
                    )
                ),
            }
    return value


def listener_identity() -> dict[str, Any]:
    return {
        "pid": os.getpid(),
        "process_created_at": psutil.Process().create_time(),
        "launcher_boot_id": os.getenv("ZHENXUN_LAUNCHER_BOOT_ID", ""),
    }


def listener_identity_matches(value: dict[str, Any]) -> bool:
    expected_boot = os.getenv("ZHENXUN_LAUNCHER_BOOT_ID", "")
    if (
        not expected_boot
        or value.get("launcher_boot_id") != expected_boot
        or not value.get("startup_id")
    ):
        return False
    try:
        pid = int(value["pid"])
        created = float(value["process_created_at"])
        return pid > 0 and abs(psutil.Process(pid).create_time() - created) < 0.01
    except (psutil.Error, KeyError, ValueError, TypeError):
        return False


def reset_http_sidecar_state(*, mode: str, port: int | None) -> None:
    write_http_sidecar_state(
        mode=mode,
        port=port,
        pid=None,
        state="disabled" if mode == "disabled" else "stopped",
        active_connections=0,
        proxy_failures=0,
        last_error=None,
    )


__all__ = [
    "read_http_sidecar_state",
    "reset_http_sidecar_state",
    "sanitized_sidecar_error",
    "write_http_sidecar_state",
]
