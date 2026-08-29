from __future__ import annotations

import asyncio
import contextlib
from datetime import datetime, timezone

from nonebot.message import event_preprocessor

from zhenxun.utils.manager.priority_manager import PriorityLifecycle

from .cache import clear_qq_official_caches
from .context import activate_event_official_context
from .models import QQWebhookReceipt

RECEIPT_CLEANUP_INTERVAL = 60 * 60
RECEIPT_CLEANUP_BATCH = 1000

_cleanup_task: asyncio.Task[None] | None = None
_adapters: set[object] = set()
_database_ready = False
_connection_lock: asyncio.Lock | None = None


def register_adapter_runtime(adapter: object) -> None:
    _adapters.add(adapter)


def unregister_adapter_runtime(adapter: object) -> None:
    _adapters.discard(adapter)


async def connect_prepared_adapter(adapter: object) -> None:
    global _connection_lock
    if not _database_ready:
        return
    if _connection_lock is None:
        _connection_lock = asyncio.Lock()
    async with _connection_lock:
        if not _database_ready:
            return
        connect = getattr(adapter, "connect_prepared_bots", None)
        if callable(connect):
            await connect()


@event_preprocessor
async def _activate_qq_official_context(event) -> None:
    activate_event_official_context(event)


async def cleanup_expired_receipts() -> int:
    rows = (
        await QQWebhookReceipt.filter(expires_at__lte=datetime.now(timezone.utc))
        .limit(RECEIPT_CLEANUP_BATCH)
        .values_list("id", flat=True)
    )
    if not rows:
        return 0
    return await QQWebhookReceipt.filter(id__in=rows).delete()


async def _drain_expired_receipts() -> None:
    deleted = await cleanup_expired_receipts()
    while deleted >= RECEIPT_CLEANUP_BATCH:
        deleted = await cleanup_expired_receipts()


async def _cleanup_loop() -> None:
    while True:
        await asyncio.sleep(RECEIPT_CLEANUP_INTERVAL)
        await _drain_expired_receipts()


@PriorityLifecycle.on_startup(priority=2)
async def start_qq_official_runtime() -> None:
    global _cleanup_task, _database_ready
    await _drain_expired_receipts()
    _database_ready = True
    for adapter in tuple(_adapters):
        await connect_prepared_adapter(adapter)
    if _cleanup_task is None or _cleanup_task.done():
        _cleanup_task = asyncio.create_task(
            _cleanup_loop(), name="qq-official-receipt-cleanup"
        )


@PriorityLifecycle.on_shutdown(priority=40)
async def stop_qq_official_runtime() -> None:
    global _cleanup_task, _connection_lock, _database_ready
    _database_ready = False
    task = _cleanup_task
    _cleanup_task = None
    if task is not None and not task.done():
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
    await clear_qq_official_caches()
    _adapters.clear()
    _connection_lock = None


__all__ = [
    "cleanup_expired_receipts",
    "connect_prepared_adapter",
    "register_adapter_runtime",
    "start_qq_official_runtime",
    "stop_qq_official_runtime",
    "unregister_adapter_runtime",
]
