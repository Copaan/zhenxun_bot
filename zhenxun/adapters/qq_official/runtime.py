from __future__ import annotations

import asyncio
import contextlib
from datetime import datetime, timezone

from nonebot.message import event_preprocessor

from zhenxun.services.lifecycle import ResourceReceipt, RuntimeHandle
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


class QQOfficialRuntimeHandle:
    def __init__(self, adapters: tuple[object, ...]) -> None:
        self._adapters = adapters

    async def quiesce(self) -> None:
        for adapter in self._adapters:
            dispatcher = getattr(adapter, "_dispatcher", None)
            stop = getattr(dispatcher, "stop", None)
            if callable(stop):
                await stop()

    async def close(self) -> None:
        return None

    async def health(self) -> dict[str, object]:
        return {
            "healthy": runtime_ready() or not self._adapters,
            "adapter_count": len(self._adapters),
        }

    async def snapshot(self) -> dict[str, object]:
        return await runtime_diagnostics()

    async def resource_snapshot(self) -> list[ResourceReceipt]:
        receipts: list[ResourceReceipt] = []
        for index, adapter in enumerate(self._adapters):
            dispatcher = getattr(adapter, "_dispatcher", None)
            snapshot = await dispatcher.snapshot() if dispatcher is not None else {}
            receipts.append(
                ResourceReceipt(
                    receipt_id=f"qq-dispatcher:{id(dispatcher)}",
                    provider="qq_official",
                    resource_type="dispatcher",
                    owner_id="runtime:qq_official",
                    detail={
                        "index": index,
                        "queue_depth": snapshot.get("queue_depth", 0),
                        "in_flight": snapshot.get("in_flight", 0),
                        "shards": snapshot.get("shards", 0),
                    },
                )
            )
            receipts.extend(
                ResourceReceipt(
                    receipt_id=f"task:{id(task)}",
                    provider="qq_official",
                    resource_type="task",
                    owner_id="runtime:qq_official",
                    detail={"name": task.get_name(), "dispatcher": index},
                )
                for task in getattr(dispatcher, "_workers", ())
                if not task.done()
            )
        return receipts


def database_ready() -> bool:
    return _database_ready


def runtime_ready() -> bool:
    if not _database_ready or not _adapters:
        return False
    return all(
        bool(getattr(adapter, "is_ready", lambda: False)()) for adapter in _adapters
    )


async def runtime_diagnostics() -> dict[str, object]:
    adapters = []
    for adapter in tuple(_adapters):
        diagnostics = getattr(adapter, "diagnostics", None)
        if callable(diagnostics):
            adapters.append(await diagnostics())
    return {
        "database_ready": _database_ready,
        "ready": runtime_ready(),
        "adapters": adapters,
    }


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


@PriorityLifecycle.on_startup(
    priority=2,
    component_id="runtime:qq_official",
    scope="bot_connection",
    depends_on=("management:database",),
    restart_policy="worker",
    config_keys=("QQ_ADAPTER_LOAD", "QQ_BOTS", "QQ_WEBHOOK"),
    pass_context=True,
)
async def start_qq_official_runtime(context=None) -> RuntimeHandle:
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
    return RuntimeHandle(
        controller=QQOfficialRuntimeHandle(tuple(_adapters)),
        metadata={"ownership": "composite"},
    )


@PriorityLifecycle.on_shutdown(priority=40, component_id="runtime:qq_official")
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
    "database_ready",
    "register_adapter_runtime",
    "runtime_diagnostics",
    "runtime_ready",
    "start_qq_official_runtime",
    "stop_qq_official_runtime",
    "unregister_adapter_runtime",
]
