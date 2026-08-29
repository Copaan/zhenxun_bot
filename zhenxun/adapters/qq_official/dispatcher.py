from __future__ import annotations

import asyncio
from collections import Counter
from collections.abc import Awaitable, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass
import hashlib
from typing import Any

from zhenxun.services.log import logger

SHARD_COUNT = 32
TOTAL_QUEUE_CAPACITY = 2048
SHUTDOWN_DRAIN_SECONDS = 15.0


@dataclass(frozen=True, slots=True)
class DispatchItem:
    handler: Callable[[Any], Awaitable[None]]
    event: Any


class QQWebhookDispatcher:
    """Bounded sharded executor preserving per-conversation event order."""

    def __init__(
        self,
        *,
        shard_count: int = SHARD_COUNT,
        capacity: int = TOTAL_QUEUE_CAPACITY,
    ) -> None:
        self._shard_count = max(1, shard_count)
        self._capacity = max(1, capacity)
        self._queues = [asyncio.Queue[DispatchItem]() for _ in range(self._shard_count)]
        self._ordering_locks = [asyncio.Lock() for _ in range(self._shard_count)]
        self._workers: list[asyncio.Task[None]] = []
        self._admitted = 0
        self._in_flight = 0
        self._accepting = False
        self._lock = asyncio.Lock()
        self._counts: Counter[str] = Counter()

    @property
    def accepting(self) -> bool:
        return self._accepting

    async def start(self) -> None:
        async with self._lock:
            if self._accepting:
                return
            self._accepting = True
            self._workers = [
                asyncio.create_task(self._worker(index), name=f"qq-dispatch-{index}")
                for index in range(self._shard_count)
            ]

    async def reserve(self) -> bool:
        async with self._lock:
            if not self._accepting or self._admitted >= self._capacity:
                self._counts["overloaded"] += 1
                return False
            self._admitted += 1
            return True

    async def release(self) -> None:
        async with self._lock:
            self._admitted = max(0, self._admitted - 1)

    async def enqueue(
        self,
        routing_key: str,
        handler: Callable[[Any], Awaitable[None]],
        event: Any,
    ) -> bool:
        async with self._lock:
            if not self._accepting:
                self._admitted = max(0, self._admitted - 1)
                self._counts["stopped_before_enqueue"] += 1
                return False
            index = self._shard_index(routing_key)
            self._queues[index].put_nowait(DispatchItem(handler=handler, event=event))
            self._counts["enqueued"] += 1
            return True

    def _shard_index(self, routing_key: str) -> int:
        digest = hashlib.blake2s(routing_key.encode("utf-8"), digest_size=4).digest()
        return int.from_bytes(digest, "big") % self._shard_count

    @asynccontextmanager
    async def ordering(self, routing_key: str):
        async with self._ordering_locks[self._shard_index(routing_key)]:
            yield

    async def _worker(self, index: int) -> None:
        queue = self._queues[index]
        while True:
            item = await queue.get()
            async with self._lock:
                self._in_flight += 1
            try:
                await item.handler(item.event)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self._counts["handler_failed"] += 1
                logger.error(
                    "QQ Webhook 分片事件执行失败",
                    "QQOfficialWebhook",
                    e=exc,
                )
            else:
                self._counts["handled"] += 1
            finally:
                async with self._lock:
                    self._in_flight = max(0, self._in_flight - 1)
                    self._admitted = max(0, self._admitted - 1)
                queue.task_done()

    async def stop(self) -> None:
        async with self._lock:
            self._accepting = False
            workers = self._workers
            self._workers = []
        if not workers:
            return
        try:
            await asyncio.wait_for(
                asyncio.gather(*(queue.join() for queue in self._queues)),
                timeout=SHUTDOWN_DRAIN_SECONDS,
            )
        except TimeoutError:
            self._counts["shutdown_timeout"] += 1
        for worker in workers:
            worker.cancel()
        await asyncio.gather(*workers, return_exceptions=True)
        for queue in self._queues:
            while not queue.empty():
                queue.get_nowait()
                queue.task_done()
                await self.release()

    async def snapshot(self) -> dict[str, object]:
        async with self._lock:
            return {
                "accepting": self._accepting,
                "capacity": self._capacity,
                "admitted": self._admitted,
                "in_flight": self._in_flight,
                "queue_depth": sum(queue.qsize() for queue in self._queues),
                "shards": self._shard_count,
                "counts": dict(self._counts),
            }


def dispatch_routing_key(app_id: str, event_type: str, data: object) -> str:
    payload = data if isinstance(data, dict) else {}
    group = str(payload.get("group_openid") or "")
    author = payload.get("author")
    author_data = author if isinstance(author, dict) else {}
    actor = str(
        payload.get("openid")
        or payload.get("user_openid")
        or payload.get("op_member_openid")
        or author_data.get("member_openid")
        or author_data.get("user_openid")
        or ""
    )
    scene = "group" if group or event_type.startswith("GROUP_") else "c2c"
    recipient = group or actor or event_type
    return f"{app_id}\0{scene}\0{recipient}"


__all__ = [
    "SHARD_COUNT",
    "TOTAL_QUEUE_CAPACITY",
    "QQWebhookDispatcher",
    "dispatch_routing_key",
]
