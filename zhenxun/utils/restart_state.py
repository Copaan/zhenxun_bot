from __future__ import annotations

import json
from pathlib import Path
import time
from typing import Any

_RESTART_STATE_FILE = Path() / "data" / ".restart_state.json"
_LAUNCHER_ACTION_KEY = "launcher_action"
_ACTION_RESTART = "restart"
_ACTION_SYNC_DEPENDENCIES = "sync_dependencies_restart"
_LAUNCHER_NOT_BEFORE_KEY = "launcher_not_before"
_DEPENDENCY_PATHS_KEY = "dependency_paths"


def _ensure_state_parent() -> None:
    _RESTART_STATE_FILE.parent.mkdir(parents=True, exist_ok=True)


def read_restart_state() -> dict[str, Any]:
    if not _RESTART_STATE_FILE.exists():
        return {}
    try:
        data = json.loads(_RESTART_STATE_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return data if isinstance(data, dict) else {}


def write_restart_state(state: dict[str, Any]) -> None:
    if not state:
        if _RESTART_STATE_FILE.exists():
            _RESTART_STATE_FILE.unlink()
        return
    _ensure_state_parent()
    temp_file = _RESTART_STATE_FILE.with_name(f"{_RESTART_STATE_FILE.name}.tmp")
    temp_file.write_text(
        json.dumps(state, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    temp_file.replace(_RESTART_STATE_FILE)


def consume_launcher_restart_signal() -> bool:
    return consume_launcher_action() is not None


def consume_launcher_action() -> tuple[str, list[str]] | None:
    state = read_restart_state()
    action = state.get(_LAUNCHER_ACTION_KEY)
    if action not in {_ACTION_RESTART, _ACTION_SYNC_DEPENDENCIES}:
        return None
    if time.time() < float(state.get(_LAUNCHER_NOT_BEFORE_KEY, 0)):
        return None
    paths = state.get(_DEPENDENCY_PATHS_KEY, [])
    if not isinstance(paths, list) or not all(isinstance(item, str) for item in paths):
        paths = []
    state.pop(_LAUNCHER_ACTION_KEY, None)
    state.pop(_LAUNCHER_NOT_BEFORE_KEY, None)
    state.pop(_DEPENDENCY_PATHS_KEY, None)
    write_restart_state(state)
    return str(action), paths


def clear_launcher_restart_signal() -> None:
    state = read_restart_state()
    if _LAUNCHER_ACTION_KEY not in state:
        return
    state.pop(_LAUNCHER_ACTION_KEY, None)
    state.pop(_LAUNCHER_NOT_BEFORE_KEY, None)
    state.pop(_DEPENDENCY_PATHS_KEY, None)
    write_restart_state(state)
