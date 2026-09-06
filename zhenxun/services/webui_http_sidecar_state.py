from __future__ import annotations

from datetime import datetime, timezone
import os
from pathlib import Path
from typing import Any

import psutil

from zhenxun.utils.atomic_json import mutate_json_locked, read_json_locked


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
    value = str(error).strip().casefold().replace(" ", "_")
    return value[:96] or None


def write_http_sidecar_state(**changes: Any) -> dict[str, Any]:
    result: dict[str, Any] = {}

    def update(current: dict[str, Any]) -> None:
        current.update(changes)
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
    value = read_json_locked(_state_path(), {}, quarantine_corrupt=True)
    if not isinstance(value, dict):
        return {}
    pid = value.get("pid")
    if value.get("state") in {"starting", "ready"} and isinstance(pid, int):
        if not psutil.pid_exists(pid):
            value = {
                **value,
                "state": "degraded",
                "active_connections": 0,
                "last_error": value.get("last_error") or "process_not_running",
            }
    return value


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
