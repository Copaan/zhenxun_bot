from __future__ import annotations

from collections.abc import Callable
import logging
from pathlib import Path
import time
from typing import Any

from .atomic_json import (
    AtomicJsonRecordError,
    mutate_json_locked,
    read_json_locked,
    write_json_locked,
)

_RESTART_STATE_FILE = Path() / "data" / ".restart_state.json"
_LAUNCHER_ACTION_KEY = "launcher_action"
_ACTION_RESTART = "restart"
_ACTION_SYNC_DEPENDENCIES = "sync_dependencies_restart"
_LAUNCHER_NOT_BEFORE_KEY = "launcher_not_before"
_DEPENDENCY_PATHS_KEY = "dependency_paths"
_PENDING_RESTARTS_KEY = "pending_restarts"
_RESTART_TICKET_KEY = "restart_ticket"
_RESTART_TICKETS_KEY = "restart_tickets"


def read_restart_state() -> dict[str, Any]:
    value = read_json_locked(_RESTART_STATE_FILE, {}, strict=True)
    _validate_restart_state(value)
    return value


def _validate_restart_state(value: Any) -> None:
    if not isinstance(value, dict):
        raise AtomicJsonRecordError("restart_state_invalid")
    pending = value.get(_PENDING_RESTARTS_KEY, {})
    if not isinstance(pending, dict) or any(
        not isinstance(item, dict) for item in pending.values()
    ):
        raise AtomicJsonRecordError("restart_pending_invalid")


def bind_restart_worker(startup_id: str) -> None:
    """将已有变更绑定到即将启动的 worker，后续变更保持独立。"""

    def bind(state: dict[str, Any]) -> None:
        state["verification_startup_id"] = startup_id
        for source, item in state.get(_PENDING_RESTARTS_KEY, {}).items():
            if source in {
                "webui.environment",
                "webui.protocol",
                "webui.config",
                "webui.ai",
            }:
                item["verification_startup_id"] = startup_id

    mutate_restart_state(bind)


def write_restart_state(state: dict[str, Any]) -> None:
    write_json_locked(_RESTART_STATE_FILE, state)


def restart_state_signature() -> tuple[int, int] | None:
    """重启状态文件的修改时间与大小，用于判断文件是否变化

    返回:
        tuple[int, int] | None: 修改时间与大小，文件不存在时为 None
    """
    try:
        stat = _RESTART_STATE_FILE.stat()
    except OSError:
        return None
    return stat.st_mtime_ns, stat.st_size


def mutate_restart_state(
    mutator, *, write_if: Callable[[Any], bool] | None = None
) -> Any:
    def checked(state):
        _validate_restart_state(state)
        return mutator(state)

    return mutate_json_locked(
        _RESTART_STATE_FILE,
        {},
        checked,
        strict=True,
        write_if=write_if,
    )


def _ticket_map(state: dict[str, Any]) -> dict[str, dict[str, Any]]:
    tickets = state.get(_RESTART_TICKETS_KEY)
    if not isinstance(tickets, dict):
        tickets = {}
    legacy = state.pop(_RESTART_TICKET_KEY, None)
    if isinstance(legacy, dict) and legacy.get("source"):
        tickets.setdefault(str(legacy["source"]), legacy)
    state[_RESTART_TICKETS_KEY] = tickets
    return tickets


def issue_restart_ticket(source: str, *, ttl_seconds: int = 600) -> None:
    now = time.time()

    def update(state: dict[str, Any]) -> None:
        tickets = _ticket_map(state)
        tickets[source] = {
            "source": source,
            "issued_at": now,
            "expires_at": now + ttl_seconds,
        }

    mutate_restart_state(update)


def consume_restart_ticket(state: dict[str, Any], source: str) -> tuple[bool, str]:
    tickets = _ticket_map(state)
    ticket = tickets.get(source)
    if not isinstance(ticket, dict):
        return False, "重启标志不存在..."
    if time.time() > float(ticket.get("expires_at", 0)):
        tickets.pop(source, None)
        return False, "重启标志已过期，请重新设置配置。"
    tickets.pop(source, None)
    if not tickets:
        state.pop(_RESTART_TICKETS_KEY, None)
    return True, ""


def clear_restart_tickets(state: dict[str, Any]) -> None:
    state.pop(_RESTART_TICKET_KEY, None)
    state.pop(_RESTART_TICKETS_KEY, None)


def consume_launcher_action() -> tuple[str, list[str]] | None:
    def consume(state: dict[str, Any]) -> tuple[str, list[str]] | None:
        action = state.get(_LAUNCHER_ACTION_KEY)
        if action not in {_ACTION_RESTART, _ACTION_SYNC_DEPENDENCIES}:
            return None
        if time.time() < float(state.get(_LAUNCHER_NOT_BEFORE_KEY, 0)):
            return None
        paths = state.get(_DEPENDENCY_PATHS_KEY, [])
        if not isinstance(paths, list) or not all(
            isinstance(item, str) for item in paths
        ):
            paths = []
        state.pop(_LAUNCHER_ACTION_KEY, None)
        state.pop(_LAUNCHER_NOT_BEFORE_KEY, None)
        state.pop(_DEPENDENCY_PATHS_KEY, None)
        return str(action), paths

    try:
        return mutate_restart_state(consume, write_if=lambda action: action is not None)
    except (OSError, ValueError, RuntimeError):
        return None


def clear_launcher_restart_signal() -> None:
    def clear(state: dict[str, Any]) -> None:
        state.pop(_LAUNCHER_ACTION_KEY, None)
        state.pop(_LAUNCHER_NOT_BEFORE_KEY, None)
        state.pop(_DEPENDENCY_PATHS_KEY, None)

    try:
        mutate_restart_state(clear)
    except (OSError, ValueError, RuntimeError) as error:
        logging.getLogger(__name__).warning(
            "重启记录待核验，未清理信号: %s", type(error).__name__
        )


def clear_pending_restart_state(
    source: str, *, expected: dict[str, Any] | None = None
) -> None:
    def clear(state: dict[str, Any]) -> None:
        pending = state.get(_PENDING_RESTARTS_KEY)
        if not isinstance(pending, dict):
            return
        if expected is not None and pending.get(source) != expected:
            return
        pending.pop(source, None)
        if pending:
            state[_PENDING_RESTARTS_KEY] = pending
        else:
            state.pop(_PENDING_RESTARTS_KEY, None)
            clear_restart_tickets(state)

    mutate_restart_state(clear)


__all__ = [
    "_ACTION_RESTART",
    "_ACTION_SYNC_DEPENDENCIES",
    "_DEPENDENCY_PATHS_KEY",
    "_LAUNCHER_ACTION_KEY",
    "_LAUNCHER_NOT_BEFORE_KEY",
    "_PENDING_RESTARTS_KEY",
    "_RESTART_STATE_FILE",
    "bind_restart_worker",
    "clear_launcher_restart_signal",
    "clear_pending_restart_state",
    "clear_restart_tickets",
    "consume_launcher_action",
    "consume_restart_ticket",
    "issue_restart_ticket",
    "mutate_restart_state",
    "read_restart_state",
    "restart_state_signature",
    "write_restart_state",
]
