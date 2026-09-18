from __future__ import annotations

import time

_OVERLOAD_UNTIL = 0.0
_DB_UNHEALTHY_UNTIL = 0.0
_DB_UNHEALTHY_REASON = ""
_LAST_ACTIVITY = time.monotonic()


def mark_activity() -> None:
    """Record lightweight runtime activity for idle-only maintenance jobs."""
    global _LAST_ACTIVITY
    _LAST_ACTIVITY = time.monotonic()


def idle_seconds() -> float:
    return max(0.0, time.monotonic() - _LAST_ACTIVITY)


def signal_overload(duration: float = 5.0) -> None:
    """Mark the system as overloaded for a short time window."""
    global _OVERLOAD_UNTIL
    if duration <= 0:
        return
    now = time.monotonic()
    until = now + duration
    if until > _OVERLOAD_UNTIL:
        _OVERLOAD_UNTIL = until


def is_overloaded() -> bool:
    return time.monotonic() < _OVERLOAD_UNTIL


def signal_db_unhealthy(duration: float = 30.0, reason: str = "") -> None:
    """Mark database-dependent low-priority tasks as unsafe to run briefly."""
    global _DB_UNHEALTHY_REASON, _DB_UNHEALTHY_UNTIL
    if duration <= 0:
        return
    now = time.monotonic()
    until = now + duration
    if until > _DB_UNHEALTHY_UNTIL:
        _DB_UNHEALTHY_UNTIL = until
        _DB_UNHEALTHY_REASON = str(reason or "")[:200]


def signal_db_recovered(reason: str = "") -> bool:
    """在确认数据库恢复后提前结束降级窗口。

    窗口本身是阻尼设计，所以调用方必须先自己确认"连续成功"，这里只负责清除。
    没有这个入口时，窗口是纯时间驱动的：数据库一秒后就恢复了，权限判定仍要
    fail-open 满 30 秒。
    """
    global _DB_UNHEALTHY_REASON, _DB_UNHEALTHY_UNTIL
    if time.monotonic() >= _DB_UNHEALTHY_UNTIL:
        return False
    _DB_UNHEALTHY_UNTIL = 0.0
    _DB_UNHEALTHY_REASON = ""
    del reason
    return True


def is_db_unhealthy() -> bool:
    return time.monotonic() < _DB_UNHEALTHY_UNTIL


def db_unhealthy_reason() -> str:
    if not is_db_unhealthy():
        return ""
    return _DB_UNHEALTHY_REASON


def should_pause_tasks() -> bool:
    return is_overloaded() or is_db_unhealthy()


def should_pause_db_tasks() -> bool:
    return is_db_unhealthy()
