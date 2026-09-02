from __future__ import annotations

from pathlib import Path
import time
from typing import Any

from .atomic_json import mutate_json_locked, read_json_locked, write_json_locked

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
    value = read_json_locked(_RESTART_STATE_FILE, {}, quarantine_corrupt=True)
    return value if isinstance(value, dict) else {}


def write_restart_state(state: dict[str, Any]) -> None:
    write_json_locked(_RESTART_STATE_FILE, state)


def mutate_restart_state(mutator) -> Any:
    return mutate_json_locked(
        _RESTART_STATE_FILE,
        {},
        mutator,
        quarantine_corrupt=True,
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


def consume_launcher_restart_signal() -> bool:
    return consume_launcher_action() is not None


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

    return mutate_restart_state(consume)


def clear_launcher_restart_signal() -> None:
    def clear(state: dict[str, Any]) -> None:
        state.pop(_LAUNCHER_ACTION_KEY, None)
        state.pop(_LAUNCHER_NOT_BEFORE_KEY, None)
        state.pop(_DEPENDENCY_PATHS_KEY, None)

    mutate_restart_state(clear)


def clear_pending_restart_state(source: str) -> None:
    def clear(state: dict[str, Any]) -> None:
        pending = state.get(_PENDING_RESTARTS_KEY)
        if not isinstance(pending, dict):
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
    "clear_launcher_restart_signal",
    "clear_pending_restart_state",
    "clear_restart_tickets",
    "consume_launcher_action",
    "consume_launcher_restart_signal",
    "consume_restart_ticket",
    "issue_restart_ticket",
    "mutate_restart_state",
    "read_restart_state",
    "write_restart_state",
]
