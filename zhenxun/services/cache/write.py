"""Coalesce cache effects and publish only after the database commit succeeds."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from contextlib import asynccontextmanager
from contextvars import ContextVar
from copy import deepcopy
from dataclasses import dataclass, field
from functools import wraps
import inspect
import sys
import time
from typing import Any

from tortoise.connection import connections

from zhenxun.services.pipeline_metrics import pipeline_metrics

Effect = Callable[[], Awaitable[Any]]
_scope: ContextVar[WriteBatch | None] = ContextVar("cache_write_batch", default=None)
_connection: ContextVar[Any] = ContextVar("cache_write_connection", default=None)
_flushing: ContextVar[bool] = ContextVar("cache_write_flushing", default=False)
_exiting: ContextVar[bool] = ContextVar("cache_transaction_exiting", default=False)
_stats = {
    "queued": 0,
    "coalesced": 0,
    "published": 0,
    "discarded": 0,
    "failed": 0,
    "reconciliation_requests": 0,
    "transaction_exit_failures": 0,
    "transaction_cleanup_failures": 0,
}


def _request_reconciliation() -> None:
    runtime = sys.modules.get("zhenxun.services.cache.runtime_cache")
    if runtime is not None:
        coordinator = runtime.runtime_cache_refresh_coordinator
        for cache, _ in coordinator._specs().values():
            coordinator.request_refresh(cache)
        _stats["reconciliation_requests"] += 1


@dataclass
class WriteBatch:
    owner: Any = None
    effects: dict[tuple, Effect] = field(default_factory=dict)
    committed: bool = False
    uncertain_invalidations: dict[tuple, Effect] = field(default_factory=dict)

    def add(self, key: tuple, effect: Effect) -> None:
        _stats["queued"] += 1
        if key in self.effects:
            _stats["coalesced"] += 1
            self.effects.pop(key)
        self.effects[key] = effect

    async def flush(self) -> None:
        token = _flushing.set(True)
        try:
            pending, self.effects = self.effects, {}
            for key, effect in tuple(pending.items()):
                try:
                    await effect()
                    _stats["published"] += 1
                except asyncio.CancelledError:
                    self.effects.update(pending)
                    _stats["failed"] += 1
                    _request_reconciliation()
                    raise
                except Exception as error:
                    # The database is committed. A cache error must not make the
                    # ORM skip releasing its transaction/connection resources.
                    _stats["failed"] += 1
                    _request_reconciliation()
                    from zhenxun.services.log import logger

                    logger.error("Committed cache publication failed", e=error)
                pending.pop(key)
        finally:
            _flushing.reset(token)

    def discard(self) -> None:
        _stats["discarded"] += len(self.effects)
        self.effects.clear()


def _observe_transaction_exit() -> None:
    from tortoise.backends.base.client import (
        TransactionContext,
        TransactionContextPooled,
    )

    for context_type in (TransactionContext, TransactionContextPooled):
        original = context_type.__aexit__
        if getattr(original, "_zx_cache_exit", False):
            continue

        def wrap(exit_method, pooled):
            @wraps(exit_method)
            async def exit_with_publication(context, *args):
                token = _exiting.set(True)
                try:
                    await exit_method(context, *args)
                except BaseException:
                    _stats["transaction_exit_failures"] += 1
                    # Tortoise's locked version doesn't reset the context or
                    # release its connection when commit/rollback raises.
                    if connections.get(context.connection_name) is context.connection:
                        await _recover_transaction_context(context, pooled)
                    batch = getattr(context.connection, "_zx_cache_batch", None)
                    if batch is not None and batch.uncertain_invalidations:
                        invalidations = WriteBatch(
                            effects=batch.uncertain_invalidations
                        )
                        batch.uncertain_invalidations = {}
                        try:
                            await invalidations.flush()
                        except BaseException:
                            _stats["failed"] += 1
                    _request_reconciliation()
                    raise
                finally:
                    _exiting.reset(token)
                batch = getattr(context.connection, "_zx_cache_batch", None)
                if batch is not None and batch.committed:
                    # Publish after ORM connection reset/pool release, so reads
                    # cannot reuse a finalized transaction or deadlock its lock.
                    await batch.flush()

            exit_with_publication._zx_cache_exit = True
            return exit_with_publication

        context_type.__aexit__ = wrap(
            original, context_type is TransactionContextPooled
        )
        original_enter = context_type.__aenter__

        def wrap_enter(enter_method, pooled):
            @wraps(enter_method)
            async def enter_with_cleanup(context):
                started = time.monotonic()
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
                finally:
                    pipeline_metrics.observe(
                        "transaction_enter_ms", time.monotonic() - started
                    )

            return enter_with_cleanup

        context_type.__aenter__ = wrap_enter(
            original_enter, context_type is TransactionContextPooled
        )


async def _recover_transaction_context(context, pooled):
    from zhenxun.services.lifecycle.deadline import remaining_timeout

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
                _stats["transaction_cleanup_failures"] += 1
                # Never return an uncertain open transaction for reuse. Closing
                # a pooled connection lets its pool create a replacement.
                try:
                    close = raw.close()
                    if inspect.isawaitable(close):
                        await asyncio.wait_for(close, remaining())
                    if not pooled:
                        connection._parent._connection = None
                except BaseException:
                    from zhenxun.services.log import logger

                    _quarantine_connection_parent(connection._parent)
                    logger.error("Transaction connection cleanup unresolved", e=error)
        if pooled and raw is not None and connection._parent._pool:
            try:
                await asyncio.wait_for(
                    connection._parent._pool.release(raw), remaining()
                )
            except BaseException as error:
                _stats["transaction_cleanup_failures"] += 1
                from zhenxun.services.log import logger

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


def _transaction_batch(connection) -> WriteBatch | None:
    if connection is None or getattr(connection, "_finalized", None) is not False:
        return None
    batch = getattr(connection, "_zx_cache_batch", None)
    if batch is not None:
        return batch
    _observe_transaction_exit()
    batch = WriteBatch()
    original_commit, original_rollback = connection.commit, connection.rollback
    client_timed = bool(getattr(connection, "_zx_pipeline_timing", False))

    async def commit():
        started = time.monotonic()
        try:
            await original_commit()
        except BaseException:
            # Evict stale reads even if a commit's result is unknown. Never
            # publish captured model values from an unconfirmed transaction.
            batch.uncertain_invalidations = {
                key: effect
                for key, effect in batch.effects.items()
                if key[0]
                in {"model_invalidate", "group_settings", "group_settings_all"}
            }
            batch.discard()
            raise
        finally:
            if not client_timed:
                pipeline_metrics.observe("commit_ms", time.monotonic() - started)
        batch.committed = True
        if not _exiting.get():
            await batch.flush()

    async def rollback():
        started = time.monotonic()
        try:
            await original_rollback()
        finally:
            if not client_timed:
                pipeline_metrics.observe("rollback_ms", time.monotonic() - started)
            batch.discard()

    # Bind to this transaction instance, including caller-supplied connections;
    # leave driver classes, other instances and ORM global configuration alone.
    connection.commit, connection.rollback = commit, rollback
    connection._zx_cache_batch = batch
    return batch


def defer(key: tuple, effect: Effect) -> bool:
    if _flushing.get():
        return False
    batch = _transaction_batch(current_connection())
    if batch is None:
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
    _observe_transaction_exit()
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

    wrapped._zx_write_boundary = True
    return wrapped


def deferred_mutation(function):
    @wraps(function)
    async def wrapped(cls, *args, **kwargs):
        if not _flushing.get():
            copied_args, copied_kwargs = deepcopy(args), deepcopy(kwargs)
            identity = (
                ("id", getattr(args[0], "pk", None))
                if args and hasattr(args[0], "pk")
                else (repr(args), repr(sorted(kwargs.items())))
            )
            if defer(
                ("runtime", cls.__name__, *identity),
                lambda: function(cls, *copied_args, **copied_kwargs),
            ):
                return
        return await function(cls, *args, **kwargs)

    return wrapped


def publication_snapshot() -> dict[str, int]:
    return dict(_stats)


async def notify_bulk_write(model) -> None:
    table = model._meta.db_table

    async def reconcile():
        from zhenxun.services.cache import CacheRoot
        from zhenxun.services.cache import runtime_cache as runtime

        cache_type = model.get_cache_type()
        if cache_type:
            await CacheRoot.invalidate_cache(cache_type)
        mapping = {
            "plugin_info": (runtime.PluginInfoMemoryCache, "plugin"),
            "bot_console": (runtime.BotMemoryCache, "bot"),
            "group_console": (runtime.GroupMemoryCache, "group"),
            "level_users": (runtime.LevelUserMemoryCache, "level"),
            "task_info": (runtime.TaskInfoMemoryCache, "task"),
            "plugin_limit": (runtime.PluginLimitMemoryCache, "plugin_limit"),
            "ban_console": (runtime.BanMemoryCache, "ban"),
        }
        if table in mapping:
            cache, label = mapping[table]
            await cache.refresh()
            if getattr(cache, "_last_error", None):
                raise RuntimeError(f"Cache reconciliation failed: {label}")
            runtime.RuntimeCacheMutation.publish(label, "refresh", {})
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

    if not defer(("bulk", table), reconcile):
        await reconcile()


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
