from __future__ import annotations

import os
from pathlib import Path

from zhenxun.services.webui_http_sidecar_state import (
    listener_identity,
    listener_identity_matches,
)
from zhenxun.utils.atomic_json import mutate_json_locked, read_json_locked


def _path() -> Path:
    return Path(
        os.getenv("ZHENXUN_QQ_INGRESS_STATE_PATH", "data/runtime/qq-ingress-v1.json")
    )


def publish_ingress_state(state: str, *, port: int) -> None:
    value = {
        **listener_identity(),
        "startup_id": os.getenv("ZHENXUN_QQ_INGRESS_STARTUP_ID", ""),
        "state": state,
        "port": port,
    }
    mutate_json_locked(_path(), {}, lambda current: current.update(value))


def read_ingress_state() -> dict:
    value = read_json_locked(_path(), {})
    if not isinstance(value, dict):
        return {"state": "unknown"}
    if not listener_identity_matches(value):
        return {"state": "unknown", "last_error": "listener_identity_unverified"}
    return value
