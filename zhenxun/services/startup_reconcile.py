from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

from zhenxun.configs.config import BotConfig
from zhenxun.utils.atomic_json import mutate_json_locked, read_json_locked

_STATE_PATH = Path("data/runtime/startup-reconcile-v1.json")


def payload_fingerprint(payload: Any) -> str:
    encoded = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    ).encode()
    return hashlib.sha256(encoded).hexdigest()


def _database_identity() -> str:
    return hashlib.sha256(str(BotConfig.db_url or "").encode()).hexdigest()


def matches(section: str, fingerprint: str) -> bool:
    state = read_json_locked(_STATE_PATH, {})
    return bool(
        state.get("version") == 1
        and state.get("database") == _database_identity()
        and state.get("sections", {}).get(section) == fingerprint
    )


def commit(section: str, fingerprint: str) -> None:
    def update(state: dict[str, Any]) -> None:
        if state.get("database") != _database_identity():
            state.clear()
        state["version"] = 1
        state["database"] = _database_identity()
        sections = state.setdefault("sections", {})
        sections[section] = fingerprint

    mutate_json_locked(_STATE_PATH, {}, update, quarantine_corrupt=True)


__all__ = ["commit", "matches", "payload_fingerprint"]
