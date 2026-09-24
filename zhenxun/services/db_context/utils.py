import asyncio
from collections.abc import AsyncIterator
import contextlib
from contextlib import asynccontextmanager
from contextvars import ContextVar
import inspect
import time

from zhenxun.services.log import logger
from zhenxun.services.message_load import (
    is_db_unhealthy,
    signal_db_recovered,
    signal_db_unhealthy,
)
from zhenxun.services.pipeline_metrics import pipeline_metrics

from .config import (
    DB_TIMEOUT_SECONDS,
    LOG_COMMAND,
    SLOW_QUERY_THRESHOLD,
)

_SQLITE_STALL_UNTIL = 0.0
_SQLITE_STALL_REASON = ""
_SQLITE_OPERATION_LOCK = asyncio.Lock()
_ACTIVE_MANAGED_DB_OPERATIONS = 0
_OPERATION_OWNER = None
_DB_DEADLINE: ContextVar[tuple[object, float] | None] = ContextVar(
    "db_deadline", default=None
)
_DB_TIMING = {
    "operations": 0,
    "queue_timeouts": 0,
    "execution_timeouts": 0,
    "queue_wait_ms": 0.0,
    "execution_ms": 0.0,
}


def db_timing_snapshot():
    return dict(_DB_TIMING)


_DB_UNHEALTHY_TIMEOUT_SECONDS = 30.0
_SQLITE_STALL_TIMEOUT_SECONDS = 60.0
_DB_RECOVERY_STREAK = 0
_DB_RECOVERY_STREAK_REQUIRED = 3
"""降级窗口内需要连续多少次快速成功才提前解除。"""


def _note_db_success(elapsed: float, timeout: float) -> None:
    """降级窗口内累计快速成功，够数就提前解除。

    只认明显快于超时阈值的操作，避免"勉强没超时"被当成恢复；一旦再次
    signal_db_unhealthy，streak 由 _reset_db_recovery_streak 清零。
    """
    global _DB_RECOVERY_STREAK
    if not is_db_unhealthy():
        _DB_RECOVERY_STREAK = 0
        return
    if timeout <= 0 or elapsed > timeout / 4:
        return
    if _SQLITE_STALL_UNTIL > time.monotonic():
        # SQLite 卡死窗口另有判定，不在这里抢先解除。
        return
    _DB_RECOVERY_STREAK += 1
    if _DB_RECOVERY_STREAK < _DB_RECOVERY_STREAK_REQUIRED:
        return
    _DB_RECOVERY_STREAK = 0
    if signal_db_recovered("db operation streak recovered"):
        logger.info("数据库连续操作成功，提前解除降级窗口", LOG_COMMAND)


def _reset_db_recovery_streak() -> None:
    global _DB_RECOVERY_STREAK
    _DB_RECOVERY_STREAK = 0


_SQLITE_LOCK_PATTERNS = (
    "database is locked",
    "database is busy",
    "database table is locked",
    "database table is busy",
)


def _is_sqlite_connection() -> bool:
    with contextlib.suppress(Exception):
        from tortoise import Tortoise

        connection = Tortoise.get_connection("default")
        capabilities = getattr(connection, "capabilities", None)
        dialect = str(getattr(capabilities, "dialect", "") or "").lower()
        return dialect.startswith("sqlite")
    return False


def _mark_sqlite_stall(reason: str, duration: float) -> None:
    global _SQLITE_STALL_REASON, _SQLITE_STALL_UNTIL
    until = time.monotonic() + max(duration, 0.0)
    if until > _SQLITE_STALL_UNTIL:
        _SQLITE_STALL_UNTIL = until
        _SQLITE_STALL_REASON = str(reason or "")[:200]


def is_sqlite_stall_suspected() -> bool:
    return time.monotonic() < _SQLITE_STALL_UNTIL


def sqlite_stall_reason() -> str:
    if not is_sqlite_stall_suspected():
        return ""
    return _SQLITE_STALL_REASON


def _is_sqlite_lock_error(exc: BaseException) -> bool:
    if not _is_sqlite_connection():
        return False
    message = str(exc).casefold()
    return any(pattern in message for pattern in _SQLITE_LOCK_PATTERNS)


def _mark_sqlite_lock_unhealthy(
    exc: BaseException, operation: str | None, source: str | None
) -> None:
    reason = f"{operation or 'database_operation'} sqlite_lock"
    _mark_sqlite_stall(reason, _SQLITE_STALL_TIMEOUT_SECONDS)
    if source != "runtime_cache":
        _reset_db_recovery_streak()
        signal_db_unhealthy(_SQLITE_STALL_TIMEOUT_SECONDS, reason=reason)
    logger.warning(
        "SQLite 数据库锁等待失败，已暂停低优先级数据库任务",
        LOG_COMMAND,
        e=exc if isinstance(exc, Exception) else None,
    )


def managed_db_operation_count() -> int:
    return _ACTIVE_MANAGED_DB_OPERATIONS


def sqlite_operation_locked() -> bool:
    return _SQLITE_OPERATION_LOCK.locked()


@asynccontextmanager
async def _managed_db_operation() -> AsyncIterator[None]:
    global _ACTIVE_MANAGED_DB_OPERATIONS, _OPERATION_OWNER
    task = asyncio.current_task()
    if _OPERATION_OWNER is task:
        yield
        return
    lock = _SQLITE_OPERATION_LOCK if _is_sqlite_connection() else None
    if lock is not None:
        from zhenxun.services.cache.write import in_write_transaction

        # The transaction already holds the ORM connection lock. Waiting for
        # a reader holding our admission gate would invert the two locks.
        if in_write_transaction():
            lock = None
    if lock is not None:
        await lock.acquire()
        _OPERATION_OWNER = task
    _ACTIVE_MANAGED_DB_OPERATIONS += 1
    try:
        yield
    finally:
        _ACTIVE_MANAGED_DB_OPERATIONS = max(0, _ACTIVE_MANAGED_DB_OPERATIONS - 1)
        if lock is not None:
            _OPERATION_OWNER = None
            lock.release()


@asynccontextmanager
async def sqlite_exclusive_access(timeout: float) -> AsyncIterator[None]:
    """Block new managed SQLite operations while recovery inspects the connection."""
    if not _is_sqlite_connection():
        yield
        return
    await asyncio.wait_for(_SQLITE_OPERATION_LOCK.acquire(), timeout=timeout)
    try:
        yield
    finally:
        _SQLITE_OPERATION_LOCK.release()


async def with_db_timeout(
    coro,
    timeout: float = DB_TIMEOUT_SECONDS,
    operation: str | None = None,
    source: str | None = None,
    timing: dict | None = None,
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
    entered = False
    queued = start_time
    _DB_TIMING["operations"] += 1

    def expire():
        nonlocal expired
        expired = True
        task.cancel()

    cancellation_count = task.cancelling() if hasattr(task, "cancelling") else 0
    timer = loop.call_at(deadline, expire) if owns_deadline else None
    try:
        try:
            async with _managed_db_operation():
                queued = loop.time()
                _DB_TIMING["queue_wait_ms"] += (queued - start_time) * 1000
                entered = True
                if queued >= deadline:
                    if inspect.iscoroutine(coro):
                        coro.close()
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
        execution_elapsed = loop.time() - queued if entered else 0.0
        row_count = len(result) if isinstance(result, list | tuple) else "未知"
        if elapsed > SLOW_QUERY_THRESHOLD and operation:
            logger.warning(
                f"数据库操作耗时过高: {operation} 总耗时 {elapsed:.3f}s "
                f"(排队 {max(0.0, queued - start_time):.3f}s, "
                f"查询往返（含驱动等待） {execution_elapsed:.3f}s)"
                + (
                    f" 来源 {timing.get('source', source)}，" f"行数 {row_count}"
                    if timing is not None
                    else ""
                ),
                LOG_COMMAND,
            )
        if entered:
            _note_db_success(execution_elapsed, timeout)
        return result
    except asyncio.TimeoutError:
        if not entered:
            _DB_TIMING["queue_timeouts"] += 1
            _DB_TIMING["queue_wait_ms"] += (loop.time() - start_time) * 1000
            raise
        _DB_TIMING["execution_timeouts"] += 1
        timeout_reason = f"{operation or 'database_operation'} from {source or '-'}"
        unhealthy_duration = _DB_UNHEALTHY_TIMEOUT_SECONDS
        if _is_sqlite_connection():
            unhealthy_duration = _SQLITE_STALL_TIMEOUT_SECONDS
            _mark_sqlite_stall(timeout_reason, unhealthy_duration)
            if source == "runtime_cache":
                logger.warning(
                    "SQLite RuntimeCache 刷新超时；已对该缓存退避，"
                    "数据库全局健康状态保持不变",
                    LOG_COMMAND,
                )
            else:
                logger.warning(
                    "SQLite 数据库操作超时，疑似 aiosqlite worker/连接被锁等待"
                    "卡住；已暂停低优先级数据库任务",
                    LOG_COMMAND,
                )
        if source != "runtime_cache":
            _reset_db_recovery_streak()
            signal_db_unhealthy(unhealthy_duration, reason=timeout_reason)
        if operation:
            logger.error(
                f"数据库操作超时: {operation} (>{timeout}s) 来源: {source}",
                LOG_COMMAND,
            )
        raise
    except Exception as exc:
        if _is_sqlite_lock_error(exc):
            _mark_sqlite_lock_unhealthy(exc, operation, source)
        raise

    finally:
        if timing is not None:
            timing.update(
                queue_wait_ms=round(
                    ((queued if entered else loop.time()) - start_time) * 1000, 3
                ),
                query_roundtrip_ms=round((loop.time() - queued) * 1000, 3)
                if entered
                else 0,
            )
        if timer is not None:
            timer.cancel()
        if expired and hasattr(task, "uncancel"):
            task.uncancel()
        _DB_DEADLINE.reset(token)
        pipeline_metrics.observe(
            "queue_wait_ms", (queued if entered else loop.time()) - start_time
        )
        if entered:
            _DB_TIMING["execution_ms"] += (loop.time() - queued) * 1000
            pipeline_metrics.observe("database_operation_ms", loop.time() - queued)
        elif inspect.iscoroutine(coro):
            coro.close()
