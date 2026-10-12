"""Coalesce cache effects and publish only after the database commit succeeds."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from contextlib import asynccontextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from functools import wraps
import inspect
import time
from typing import Any

from tortoise.backends.base.client import TransactionContext, TransactionContextPooled
from tortoise.connection import connections

from zhenxun.utils.exception import DependencyUnavailable

Effect = Callable[[], Awaitable[Any]]
_scope: ContextVar[WriteBatch | None] = ContextVar("cache_write_batch", default=None)
_connection: ContextVar[Any] = ContextVar("cache_write_connection", default=None)
_flushing: ContextVar[bool] = ContextVar("cache_write_flushing", default=False)
_RECONCILERS: list[Callable[[tuple | None], None]] = []
_UNCERTAIN_INVALIDATIONS = {"group_settings", "group_settings_all"}


class CacheUnavailable(DependencyUnavailable):
    """No authoritative value is available; this is not a cache miss."""


class CachePublicationError(CacheUnavailable):
    pass


def require_publication(result: Any) -> None:
    if result is False:
        raise CachePublicationError("cache_invalidation_returned_false")


def on_publication_failed(
    handler: Callable[[tuple | None], None],
) -> Callable[[tuple | None], None]:
    """注册缓存发布失败时的和解回调

    参数:
        handler: 回调，接收失败的缓存效果键，无法确定时为 None

    返回:
        Callable: 原回调
    """
    _RECONCILERS.append(handler)
    return handler


def _request_reconciliation(key: tuple | None = None) -> None:
    for handler in tuple(_RECONCILERS):
        handler(key)


@dataclass
class WriteBatch:
    owner: Any = None
    effects: dict[tuple, Effect] = field(default_factory=dict)

    def add(self, key: tuple, effect: Effect) -> None:
        self.effects.pop(key, None)
        self.effects[key] = effect

    async def flush(self) -> None:
        token = _flushing.set(True)
        try:
            pending, self.effects = self.effects, {}
            for key, effect in tuple(pending.items()):
                try:
                    require_publication(await effect())
                except asyncio.CancelledError:
                    self.effects.update(pending)
                    for pending_key in pending:
                        _request_reconciliation(pending_key)
                    raise
                except Exception as error:
                    # The database is committed. A cache error must not make the
                    # ORM skip releasing its transaction/connection resources.
                    _request_reconciliation(key)
                    from zhenxun.services.log import logger

                    logger.error(
                        f"Committed cache publication failed: {key[:2]}", e=error
                    )
                pending.pop(key)
        finally:
            _flushing.reset(token)


async def _transaction_failed(batch: WriteBatch | None, committing: bool) -> None:
    """事务提交或回滚失败时请求和解，提交结果未知时先清除可能过期的缓存

    参数:
        batch: 事务内暂存的缓存效果
        committing: 是否为提交阶段失败
    """
    if batch is None:
        _request_reconciliation()
        return
    effects, batch.effects = batch.effects, {}
    if not committing:
        return
    for key in effects:
        _request_reconciliation(key)
    invalidations = WriteBatch(
        effects={
            key: effect
            for key, effect in effects.items()
            if key[0] in _UNCERTAIN_INVALIDATIONS
        }
    )
    try:
        await invalidations.flush()
    except BaseException:
        pass


def _install_transaction_hooks() -> None:
    """在 Tortoise 事务退出后发布缓存效果，并在提交/回滚失败时回收连接"""
    for context_type in (TransactionContext, TransactionContextPooled):
        pooled = context_type is TransactionContextPooled
        original_enter, original_exit = context_type.__aenter__, context_type.__aexit__
        if getattr(original_exit, "_zx_cache_exit", False):
            continue

        def wrap_enter(enter_method, pooled):
            @wraps(enter_method)
            async def enter(context):
                try:
                    if getattr(
                        context.connection._parent,
                        "_zx_transaction_cleanup_failed",
                        False,
                    ):
                        raise RuntimeError("transaction_connection_quarantined")
                    return await enter_method(context)
                except BaseException:
                    if connections.get(context.connection_name) is context.connection:
                        await _recover_transaction_context(context, pooled)
                    raise

            return enter

        def wrap_exit(exit_method, pooled):
            @wraps(exit_method)
            async def exit_(context, exc_type, *args):
                connection = context.connection
                finalized = connection._finalized
                try:
                    await exit_method(context, exc_type, *args)
                except BaseException:
                    # Tortoise doesn't reset the context or release its
                    # connection when commit/rollback raises.
                    if connections.get(context.connection_name) is connection:
                        await _recover_transaction_context(context, pooled)
                    await _transaction_failed(
                        connection.__dict__.pop("_zx_cache_batch", None),
                        committing=not finalized and exc_type is None,
                    )
                    raise
                batch = connection.__dict__.pop("_zx_cache_batch", None)
                if batch is not None and not finalized and exc_type is None:
                    # Publish after ORM connection reset/pool release, so reads
                    # cannot reuse a finalized transaction or deadlock its lock.
                    await batch.flush()

            setattr(exit_, "_zx_cache_exit", True)
            return exit_

        setattr(context_type, "__aenter__", wrap_enter(original_enter, pooled))
        setattr(context_type, "__aexit__", wrap_exit(original_exit, pooled))


async def _recover_transaction_context(context, pooled):
    from zhenxun.services.lifecycle.deadline import remaining_timeout
    from zhenxun.services.log import logger

    connection = context.connection
    raw = connection._connection
    deadline = time.monotonic() + remaining_timeout(15)

    def remaining():
        return max(0, deadline - time.monotonic())

    try:
        if not connection._finalized and raw is not None:
            try:
                await asyncio.wait_for(connection.rollback(), remaining())
            except BaseException as error:
                # Never return an uncertain open transaction for reuse. Closing
                # a pooled connection lets its pool create a replacement.
                try:
                    close = raw.close()
                    if inspect.isawaitable(close):
                        await asyncio.wait_for(close, remaining())
                    if not pooled:
                        connection._parent._connection = None
                except BaseException:
                    _quarantine_connection_parent(connection._parent)
                    logger.error("Transaction connection cleanup unresolved", e=error)
        if pooled and raw is not None and connection._parent._pool:
            try:
                await asyncio.wait_for(
                    connection._parent._pool.release(raw), remaining()
                )
            except BaseException as error:
                _quarantine_connection_parent(connection._parent)
                logger.error("Transaction pool release unresolved", e=error)
    finally:
        # ContextVar tokens belong to the calling task, never a cleanup task.
        connections.reset(context.token)
        if not pooled:
            context.lock.release()


def _quarantine_connection_parent(parent):
    parent._zx_transaction_cleanup_failed = True

    def reject_acquisition(*args, **kwargs):
        raise RuntimeError("transaction_connection_quarantined")

    # Keep close() available for lifecycle cleanup, but don't allow another
    # request to use a connection whose rollback/release couldn't be verified.
    parent.acquire_connection = reject_acquisition


def current_connection():
    explicit = _connection.get()
    if explicit is not None:
        return explicit
    try:
        return connections.get("default")
    except Exception:
        return None


def in_write_transaction() -> bool:
    return getattr(current_connection(), "_finalized", None) is False


def defer(key: tuple, effect: Effect) -> bool:
    if _flushing.get():
        return False
    connection = current_connection()
    if getattr(connection, "_finalized", None) is False:
        batch = connection.__dict__.get("_zx_cache_batch")
        if batch is None:
            batch = connection.__dict__["_zx_cache_batch"] = WriteBatch()
    else:
        batch = _scope.get()
        if batch is None or batch.owner is not asyncio.current_task():
            return False
    batch.add(key, effect)
    return True


@asynccontextmanager
async def write_scope(connection=None):
    token = _connection.set(connection if connection is not None else _connection.get())
    existing = _scope.get()
    owner = asyncio.current_task()
    own = existing is None or existing.owner is not owner
    batch = WriteBatch(owner) if own else existing
    scope_token = _scope.set(batch)
    try:
        yield
    finally:
        try:
            if own:
                # Autocommitted writes remain committed even if a later hook
                # fails; transaction effects are held by their connection instead.
                await batch.flush()
        finally:
            _scope.reset(scope_token)
            _connection.reset(token)


def write_boundary(function):
    if getattr(function, "_zx_write_boundary", False):
        return function
    signature = inspect.signature(function)

    @wraps(function)
    async def wrapped(*args, **kwargs):
        connection = kwargs.get("using_db")
        if connection is None and "using_db" in signature.parameters:
            connection = signature.bind_partial(*args, **kwargs).arguments.get(
                "using_db"
            )
        async with write_scope(connection):
            return await function(*args, **kwargs)

    setattr(wrapped, "_zx_write_boundary", True)
    return wrapped


def deferred_mutation(function):
    @wraps(function)
    async def wrapped(cls, *args, **kwargs):
        if not _flushing.get() and defer(
            ("runtime", cls.__name__, function.__name__, repr(args), repr(kwargs)),
            lambda: function(cls, *args, **kwargs),
        ):
            return
        try:
            result = await function(cls, *args, **kwargs)
            require_publication(result)
            return result
        except BaseException:
            if not _flushing.get():
                _request_reconciliation(("runtime", cls.__name__))
            raise

    return wrapped


async def notify_bulk_write(model) -> None:
    table = model._meta.db_table

    async def reconcile():
        from zhenxun.services.cache.runtime_sync import RUNTIME_CACHES

        cache = next((c for c in RUNTIME_CACHES.values() if c.table == table), None)
        if cache is not None:
            await cache.reconcile()
        elif table == "group_info_users":
            from zhenxun.services.hot_query_cache import (
                invalidate_group_members,
                invalidate_member_names,
            )

            await invalidate_group_members()
            await invalidate_member_names()
        elif table == "group_plugin_settings":
            from zhenxun.services.group_settings_service import group_settings_service

            await group_settings_service.invalidate_all()

    key = ("bulk", table)
    if not defer(key, reconcile):
        try:
            require_publication(await reconcile())
        except BaseException:
            _request_reconciliation(key)
            raise


class WriteQuery:
    """Preserve lazy ORM execution while observing QuerySet writes."""

    def __init__(self, query, model):
        self.query = query
        self.model = model

    def __getattr__(self, name):
        return getattr(self.query, name)

    def __await__(self):
        return self._execute().__await__()

    async def _execute(self):
        async with write_scope(getattr(self.query, "_db", None)):
            result = await self.query
            await notify_bulk_write(self.model)
            return result


_install_transaction_hooks()
