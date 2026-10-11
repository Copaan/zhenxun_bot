from __future__ import annotations

import asyncio
import contextlib
from datetime import datetime, timezone
from typing import Any

from zhenxun.utils.manager.priority_manager import PriorityLifecycle

from .cache import clear_qq_official_caches
from .models import QQWebhookReceipt

RECEIPT_CLEANUP_INTERVAL = 60 * 60
RECEIPT_CLEANUP_BATCH = 1000

_cleanup_task: asyncio.Task[None] | None = None
_adapters: set[Any] = set()
_database_ready = False
_connection_lock: asyncio.Lock | None = None


def database_ready() -> bool:
    return _database_ready


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


@PriorityLifecycle.on_startup(
    priority=2,
    component_id="runtime:qq_official",
    depends_on=("management:database",),
    restart_policy="worker",
    config_keys=("QQ_ADAPTER_LOAD", "QQ_BOTS", "QQ_WEBHOOK"),
    pass_context=True,
    # 单个平台适配器出问题不应让整个 runtime 阶段失败、把 worker 打回
    # management/setup_only（那会连带关掉其它平台的入口）。降级即可，
    # 原因会记进 degraded_reasons 供 WebUI 展示。
    failure_policy="degrade",
)
async def start_qq_official_runtime(context=None) -> None:
    global _cleanup_task, _database_ready
    await _drain_expired_receipts()
    _database_ready = True
    for adapter in tuple(_adapters):
        await connect_prepared_adapter(adapter)
    if _cleanup_task is None or _cleanup_task.done():
        coroutine = _cleanup_loop()
        _cleanup_task = (
            context.spawn_task(coroutine, name="qq-official-receipt-cleanup")
            if context is not None
            else asyncio.create_task(coroutine, name="qq-official-receipt-cleanup")
        )


@PriorityLifecycle.on_shutdown(priority=40, component_id="runtime:qq_official")
async def stop_qq_official_runtime() -> None:
    global _cleanup_task, _connection_lock, _database_ready
    _database_ready = False
    for adapter in tuple(_adapters):
        await adapter._dispatcher.stop()
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
    "database_ready",
    "register_adapter_runtime",
    "start_qq_official_runtime",
    "stop_qq_official_runtime",
    "unregister_adapter_runtime",
]
