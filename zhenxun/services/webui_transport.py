from __future__ import annotations

import asyncio
from collections import Counter
from collections.abc import Callable
from threading import RLock
import time
from typing import Any

_UVICORN_HTTP_PREFIX = "uvicorn.protocols.http."
_UVICORN_WEBSOCKET_PREFIX = "uvicorn.protocols.websockets."
_DEBUG_INTERVAL_SECONDS = 30.0


class TransportRuntime:
    """Own the event-loop exception proxy and transport diagnostics."""

    def __init__(self) -> None:
        self._loop: asyncio.AbstractEventLoop | None = None
        self._previous_handler: Callable[..., Any] | None = None
        self._installed_handler: Callable[..., Any] | None = None
        self._counts: Counter[str] = Counter()
        self._last_debug: dict[str, float] = {}
        self._lock = RLock()

    def install(self) -> None:
        loop = asyncio.get_running_loop()
        with self._lock:
            if self._loop is loop and self._installed_handler is not None:
                return
            if self._loop is not None:
                self.restore()
            self._loop = loop
            self._previous_handler = loop.get_exception_handler()
            self._installed_handler = self._handle_exception
            loop.set_exception_handler(self._installed_handler)

    def restore(self) -> None:
        with self._lock:
            loop = self._loop
            previous = self._previous_handler
            installed = self._installed_handler
            self._loop = None
            self._previous_handler = None
            self._installed_handler = None
        if loop is None or loop.is_closed():
            return
        if loop.get_exception_handler() is installed:
            loop.set_exception_handler(previous)

    def record(self, metric: str, amount: int = 1) -> None:
        with self._lock:
            self._counts[metric] += amount

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            counts = dict(self._counts)
            installed = self._installed_handler is not None
        return {
            "exception_proxy_installed": installed,
            "http_reset_count": counts.get("http_reset", 0),
            "websocket_reset_count": counts.get("websocket_reset", 0),
            "websocket_send_failure_count": counts.get("websocket_send_failure", 0),
            "cooperative_close_count": counts.get("cooperative_close", 0),
        }

    def _handle_exception(
        self, loop: asyncio.AbstractEventLoop, context: dict[str, Any]
    ) -> None:
        error = context.get("exception")
        channel = self._uvicorn_channel(context)
        if channel and self._is_windows_connection_reset(error):
            metric = f"{channel}_reset"
            self.record(metric)
            self._debug_disconnect(metric)
            return
        self._delegate(loop, context)

    def _delegate(
        self, loop: asyncio.AbstractEventLoop, context: dict[str, Any]
    ) -> None:
        with self._lock:
            previous = self._previous_handler
        if previous is not None:
            previous(loop, context)
        else:
            loop.default_exception_handler(context)

    @staticmethod
    def _is_windows_connection_reset(error: Any) -> bool:
        return isinstance(error, ConnectionResetError) or (
            isinstance(error, OSError) and getattr(error, "winerror", None) == 10054
        )

    @staticmethod
    def _uvicorn_channel(context: dict[str, Any]) -> str | None:
        objects: list[Any] = [
            context.get("protocol"),
            context.get("transport"),
            context.get("handle"),
            context.get("callback"),
        ]
        handle = context.get("handle")
        objects.append(getattr(handle, "_callback", None))
        visited: set[int] = set()
        while objects:
            current = objects.pop()
            if current is None or id(current) in visited:
                continue
            visited.add(id(current))
            module = str(
                getattr(current, "__module__", "")
                or getattr(type(current), "__module__", "")
            )
            if module.startswith(_UVICORN_WEBSOCKET_PREFIX):
                return "websocket"
            if module.startswith(_UVICORN_HTTP_PREFIX):
                return "http"
            objects.extend(
                (
                    getattr(current, "__self__", None),
                    getattr(current, "_protocol", None),
                    getattr(current, "protocol", None),
                    getattr(current, "_transport", None),
                )
            )
        return None

    def _debug_disconnect(self, metric: str) -> None:
        now = time.monotonic()
        with self._lock:
            last = self._last_debug.get(metric, 0.0)
            if now - last < _DEBUG_INTERVAL_SECONDS:
                return
            self._last_debug[metric] = now
        try:
            from zhenxun.services.log import logger

            logger.debug(
                "已回收Uvicorn客户端重置连接 | "
                f"channel={metric.removesuffix('_reset')}",
                "WebUi",
            )
        except Exception:
            pass


transport_runtime = TransportRuntime()

__all__ = ["TransportRuntime", "transport_runtime"]
