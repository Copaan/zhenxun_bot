import asyncio
import contextlib
from contextvars import ContextVar
import inspect
from typing import Any, Literal

from zhenxun.services.log import logger
from zhenxun.services.message_load import (
    is_db_unhealthy,
    signal_db_recovered,
    signal_db_unhealthy,
)

from .config import (
    DB_TIMEOUT_SECONDS,
    LOG_COMMAND,
    SLOW_QUERY_THRESHOLD,
)

Dialect = Literal["sqlite", "postgres", "mysql", "unknown"]

_DB_DEADLINE: ContextVar[tuple[object, float] | None] = ContextVar(
    "db_deadline", default=None
)
_DB_UNHEALTHY_TIMEOUT_SECONDS = 30.0
_SQLITE_STALL_TIMEOUT_SECONDS = 60.0
_DB_RECOVERY_STREAK = 0
_DB_RECOVERY_STREAK_REQUIRED = 3
_SQLITE_LOCK_PATTERNS = (
    "database is locked",
    "database is busy",
    "database table is locked",
    "database table is busy",
)


def db_dialect(connection: Any = None) -> Dialect:
    """获取数据库方言

    参数:
        connection: 数据库连接，为空时使用默认连接

    返回:
        Dialect: 数据库方言
    """
    with contextlib.suppress(Exception):
        if connection is None:
            from tortoise import Tortoise

            connection = Tortoise.get_connection("default")
        capabilities = getattr(connection, "capabilities", None)
        raw = str(getattr(capabilities, "dialect", "") or "").lower()
        for dialect in ("sqlite", "postgres", "mysql"):
            if raw.startswith(dialect):
                return dialect
    return "unknown"


def _note_db_success(elapsed: float, timeout: float) -> None:
    """降级窗口内累计快速成功，够数就提前解除。

    只认明显快于超时阈值的操作，避免"勉强没超时"被当成恢复；一旦再次
    signal_db_unhealthy，streak 随之清零。
    """
    global _DB_RECOVERY_STREAK
    if not is_db_unhealthy():
        _DB_RECOVERY_STREAK = 0
        return
    if timeout <= 0 or elapsed > timeout / 4:
        return
    _DB_RECOVERY_STREAK += 1
    if _DB_RECOVERY_STREAK < _DB_RECOVERY_STREAK_REQUIRED:
        return
    _DB_RECOVERY_STREAK = 0
    if signal_db_recovered("db operation streak recovered"):
        logger.info("数据库连续操作成功，提前解除降级窗口", LOG_COMMAND)


def _signal_unhealthy(duration: float, reason: str, source: str | None) -> None:
    global _DB_RECOVERY_STREAK
    if source != "runtime_cache":
        _DB_RECOVERY_STREAK = 0
        signal_db_unhealthy(duration, reason=reason)


async def with_db_timeout(
    coro,
    timeout: float = DB_TIMEOUT_SECONDS,
    operation: str | None = None,
    source: str | None = None,
):
    """带超时控制的数据库操作"""
    loop = asyncio.get_running_loop()
    task = asyncio.current_task()
    start_time = loop.time()
    inherited = _DB_DEADLINE.get()
    deadline = start_time + max(0.0, timeout)
    owns_deadline = True
    if inherited and inherited[0] is task:
        owns_deadline = deadline < inherited[1]
        deadline = min(deadline, inherited[1])
    token = _DB_DEADLINE.set((task, deadline))
    expired = False

    def expire():
        nonlocal expired
        expired = True
        task.cancel()

    cancellation_count = task.cancelling() if hasattr(task, "cancelling") else 0
    timer = loop.call_at(deadline, expire) if owns_deadline else None
    try:
        try:
            if start_time >= deadline:
                raise asyncio.TimeoutError
            result = await coro
        except asyncio.CancelledError:
            if not expired or (
                hasattr(task, "cancelling")
                and task.cancelling() > cancellation_count + 1
            ):
                raise
            raise asyncio.TimeoutError from None
        elapsed = loop.time() - start_time
        if elapsed > SLOW_QUERY_THRESHOLD and operation:
            logger.warning(
                f"数据库操作耗时过高: {operation} 耗时 {elapsed:.3f}s 来源 {source}",
                LOG_COMMAND,
            )
        _note_db_success(elapsed, timeout)
        return result
    except asyncio.TimeoutError:
        reason = f"{operation or 'database_operation'} from {source or '-'}"
        if db_dialect() == "sqlite":
            _signal_unhealthy(_SQLITE_STALL_TIMEOUT_SECONDS, reason, source)
            logger.warning(
                "SQLite 数据库操作超时，疑似 aiosqlite worker/连接被锁等待卡住",
                LOG_COMMAND,
            )
        else:
            _signal_unhealthy(_DB_UNHEALTHY_TIMEOUT_SECONDS, reason, source)
        if operation:
            logger.error(
                f"数据库操作超时: {operation} (>{timeout}s) 来源: {source}",
                LOG_COMMAND,
            )
        raise
    except Exception as exc:
        message = str(exc).casefold()
        if db_dialect() == "sqlite" and any(
            pattern in message for pattern in _SQLITE_LOCK_PATTERNS
        ):
            _signal_unhealthy(
                _SQLITE_STALL_TIMEOUT_SECONDS,
                f"{operation or 'database_operation'} sqlite_lock",
                source,
            )
            logger.warning(
                "SQLite 数据库锁等待失败，已暂停低优先级数据库任务",
                LOG_COMMAND,
                e=exc,
            )
        raise
    finally:
        if timer is not None:
            timer.cancel()
        if expired and hasattr(task, "uncancel"):
            task.uncancel()
        _DB_DEADLINE.reset(token)
        if inspect.iscoroutine(coro):
            coro.close()
