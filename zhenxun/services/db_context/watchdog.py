from __future__ import annotations

import asyncio
import contextlib
import time

from tortoise import Tortoise
from tortoise.connection import connections

from zhenxun.services.log import logger
from zhenxun.services.low_priority_writer import low_priority_writer_active_count
from zhenxun.services.message_load import (
    signal_db_unhealthy,
)
from zhenxun.utils.manager.priority_manager import PriorityLifecycle

from .config import LOG_COMMAND
from .utils import (
    managed_db_operation_count,
    sqlite_exclusive_access,
    sqlite_operation_locked,
)

_CHECK_INTERVAL_SECONDS = 15.0
_CHECK_TIMEOUT_SECONDS = 2.0
_FAIL_THRESHOLD = 3
_UNHEALTHY_SECONDS = 60.0
_RECONNECT_COOLDOWN_SECONDS = 60.0
_RECONNECT_WAIT_IDLE_SECONDS = 3.0

_WATCHDOG_TASK: asyncio.Task[None] | None = None
_RECONNECT_LOCK = asyncio.Lock()
_LAST_RECONNECT_AT = 0.0
_CONSECUTIVE_FAILURES = 0
_RECOVERY_STATE = "idle"
_LAST_FAILURE = ""


def db_watchdog_healthy(_value=None) -> bool:
    return _WATCHDOG_TASK is not None and not _WATCHDOG_TASK.done()


def _is_sqlite_connection() -> bool:
    with contextlib.suppress(Exception):
        connection = Tortoise.get_connection("default")
        capabilities = getattr(connection, "capabilities", None)
        dialect = str(getattr(capabilities, "dialect", "") or "").lower()
        return dialect.startswith("sqlite")
    return False


async def _select_one() -> None:
    connection = Tortoise.get_connection("default")
    await connection.execute_query("SELECT 1")


async def _select_one_when_idle() -> bool:
    if low_priority_writer_active_count() > 0 or sqlite_operation_locked():
        return False
    acquired = False
    try:
        async with sqlite_exclusive_access(0.1):
            acquired = True
            await asyncio.wait_for(_select_one(), timeout=_CHECK_TIMEOUT_SECONDS)
    except asyncio.TimeoutError:
        if not acquired:
            return False
        raise
    return True


async def _try_reconnect(reason: str) -> None:
    global _LAST_RECONNECT_AT, _RECOVERY_STATE
    now = time.monotonic()
    if now - _LAST_RECONNECT_AT < _RECONNECT_COOLDOWN_SECONDS:
        return
    if low_priority_writer_active_count() > 0:
        _RECOVERY_STATE = "recovery_required"
        return
    async with _RECONNECT_LOCK:
        now = time.monotonic()
        if now - _LAST_RECONNECT_AT < _RECONNECT_COOLDOWN_SECONDS:
            return
        try:
            async with sqlite_exclusive_access(_RECONNECT_WAIT_IDLE_SECONDS):
                if (
                    low_priority_writer_active_count() > 0
                    or managed_db_operation_count() > 0
                ):
                    _RECOVERY_STATE = "recovery_required"
                    return
                _RECOVERY_STATE = "reconnecting"
                await connections.close_all(discard=True)
                # ConnectionHandler lazily recreates default connection from db_config.
                Tortoise.get_connection("default")
            _LAST_RECONNECT_AT = time.monotonic()
            _RECOVERY_STATE = "recovered"
            logger.warning(
                f"SQLite watchdog rebuilt default connection: {reason}",
                LOG_COMMAND,
            )
        except Exception as exc:
            _LAST_RECONNECT_AT = time.monotonic()
            _RECOVERY_STATE = "recovery_required"
            signal_db_unhealthy(
                _UNHEALTHY_SECONDS,
                reason=f"watchdog reconnect:{reason}",
            )
            logger.warning("SQLite watchdog reconnect failed", LOG_COMMAND, e=exc)


async def _run_watchdog_probe() -> None:
    global _CONSECUTIVE_FAILURES, _LAST_FAILURE, _RECOVERY_STATE
    if not _is_sqlite_connection():
        _CONSECUTIVE_FAILURES = 0
        return
    try:
        if not await _select_one_when_idle():
            return
        _CONSECUTIVE_FAILURES = 0
        _LAST_FAILURE = ""
        if _RECOVERY_STATE != "reconnecting":
            _RECOVERY_STATE = "idle"
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        _CONSECUTIVE_FAILURES += 1
        reason = (
            "sqlite watchdog SELECT 1 failed "
            f"x{_CONSECUTIVE_FAILURES}: {type(exc).__name__}"
        )
        _LAST_FAILURE = reason
        logger.warning(reason, LOG_COMMAND)
        if _CONSECUTIVE_FAILURES >= _FAIL_THRESHOLD:
            signal_db_unhealthy(_UNHEALTHY_SECONDS, reason=reason)
            await _try_reconnect(reason)


async def _watchdog_loop() -> None:
    while True:
        await asyncio.sleep(_CHECK_INTERVAL_SECONDS)
        await _run_watchdog_probe()


def watchdog_snapshot() -> dict[str, object]:
    return {
        "running": db_watchdog_healthy(),
        "consecutive_failures": _CONSECUTIVE_FAILURES,
        "last_failure": _LAST_FAILURE,
        "recovery_state": _RECOVERY_STATE,
        "managed_operations": managed_db_operation_count(),
        "sqlite_operation_locked": sqlite_operation_locked(),
    }


def start_db_watchdog(context=None) -> None:
    global _WATCHDOG_TASK
    if _WATCHDOG_TASK is not None and not _WATCHDOG_TASK.done():
        return
    _WATCHDOG_TASK = (
        context.spawn_task(_watchdog_loop(), name="database-watchdog")
        if context is not None
        else asyncio.create_task(_watchdog_loop(), name="database-watchdog")
    )


async def stop_db_watchdog() -> None:
    global _WATCHDOG_TASK
    task = _WATCHDOG_TASK
    _WATCHDOG_TASK = None
    if task is None:
        return
    task.cancel()
    with contextlib.suppress(BaseException):
        await task


@PriorityLifecycle.on_startup(
    priority=8,
    component_id="runtime:database_watchdog",
    depends_on=("management:database",),
    pass_context=True,
    health=db_watchdog_healthy,
)
async def _start_db_watchdog(context) -> None:
    start_db_watchdog(context)


@PriorityLifecycle.on_shutdown(priority=10, component_id="runtime:database_watchdog")
async def _stop_db_watchdog() -> None:
    await stop_db_watchdog()
