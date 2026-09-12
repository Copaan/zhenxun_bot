"""Bound queued and in-flight spool payloads until their acknowledgments finish."""

from __future__ import annotations

import asyncio
from collections import deque
import json


class PayloadCapacityError(asyncio.QueueFull):
    pass


class PayloadQueue(asyncio.Queue):
    def __init__(self, maxsize: int, max_bytes: int, payload):
        super().__init__(maxsize=maxsize)
        self.max_bytes = max_bytes
        self.payload_bytes = 0
        self.peak_bytes = 0
        self._payload = payload
        self._sizes = deque()
        self._in_flight_sizes = deque()

    def _put(self, item):
        size = 0
        remaining = self.max_bytes - self.payload_bytes
        for chunk in json.JSONEncoder(ensure_ascii=False).iterencode(
            self._payload(item)
        ):
            if len(chunk) > remaining - size:
                raise PayloadCapacityError
            size += len(chunk.encode("utf-8"))
            if size > remaining:
                raise PayloadCapacityError
        self._sizes.append(size)
        self.payload_bytes += size
        self.peak_bytes = max(self.peak_bytes, self.payload_bytes)
        super()._put(item)

    def _get(self):
        item = super()._get()
        self._in_flight_sizes.append(self._sizes.popleft())
        return item

    def task_done(self):
        super().task_done()
        self.payload_bytes -= self._in_flight_sizes.popleft()
