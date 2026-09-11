"""Serialize legacy WebSocket flow-control waiters without serializing API replies."""

import asyncio
from functools import wraps
from weakref import WeakKeyDictionary


class WebSocketFlowRuntime:
    def __init__(self):
        self.protocol = self.original = self.wrapper = None

    def install(self):
        if self.wrapper is not None:
            return
        from websockets.legacy.protocol import WebSocketCommonProtocol

        original = WebSocketCommonProtocol.drain
        locks = WeakKeyDictionary()

        @wraps(original)
        async def drain(protocol):
            lock = locks.get(protocol)
            if lock is None:
                lock = asyncio.Lock()
                locks[protocol] = lock
            async with lock:
                return await original(protocol)

        self.protocol, self.original, self.wrapper = (
            WebSocketCommonProtocol,
            original,
            drain,
        )
        WebSocketCommonProtocol.drain = drain

    def restore(self):
        if self.wrapper is not None and self.protocol.drain is self.wrapper:
            self.protocol.drain = self.original
        self.protocol = self.original = self.wrapper = None
