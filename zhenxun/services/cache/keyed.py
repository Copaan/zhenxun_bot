from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from dataclasses import dataclass


@dataclass
class _Entry:
    lock: asyncio.Lock
    users: int = 0


class KeyedLocks:
    """Fixed lock stripes never evict an identity with holders or waiters."""

    def __init__(self, capacity: int = 4096):
        self.capacity = capacity
        self._entries: dict[object, _Entry] = {}
        self.waits = 0

    @asynccontextmanager
    async def hold(self, key):
        stripe = hash(key) % self.capacity
        entry = self._entries.get(stripe)
        if entry is None:
            entry = self._entries[stripe] = _Entry(asyncio.Lock())
        entry.users += 1
        lock = entry.lock
        if lock.locked():
            self.waits += 1
        try:
            async with lock:
                yield
        finally:
            entry.users -= 1
