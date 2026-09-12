import asyncio
from collections.abc import AsyncIterator
import contextlib
from contextlib import asynccontextmanager
from contextvars import ContextVar
import inspect
import time

from zhenxun.services.log import logger
from zhenxun.services.message_load import signal_db_unhealthy
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
        if elapsed > SLOW_QUERY_THRESHOLD and operation:
            logger.warning(f"慢查询: {operation} 耗时 {elapsed:.3f}s", LOG_COMMAND)
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
