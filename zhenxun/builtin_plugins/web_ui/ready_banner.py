from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
import contextlib
import inspect
from typing import Any

from zhenxun.utils.network import internal_connect_host

ReadyCallback = Callable[[], Awaitable[None] | None]


class UvicornReadyBanner:
    """Run one callback after the worker's listening socket accepts TCP."""

    def __init__(self) -> None:
        self._task: asyncio.Task[None] | None = None
        self._emitted = False

    def arm(
        self,
        callback: ReadyCallback,
        *,
        host: str,
        port: int,
        timeout: float = 120.0,
        context: Any | None = None,
    ) -> bool:
        if self._task is not None or self._emitted:
            return False
        coroutine = self._probe(callback, internal_connect_host(host), port, timeout)
        self._task = (
            context.spawn_task(
                coroutine,
                name="zhenxun-webui-ready-probe",
                degrade_on_error=False,
            )
            if context is not None
            else asyncio.create_task(coroutine, name="zhenxun-webui-ready-probe")
        )
        return True

    async def _probe(
        self,
        callback: ReadyCallback,
        host: str,
        port: int,
        timeout: float,
    ) -> None:
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout
        try:
            while loop.time() < deadline:
                try:
                    attempt_timeout = max(0.01, min(0.25, deadline - loop.time()))
                    _, writer = await asyncio.wait_for(
                        asyncio.open_connection(host, port), timeout=attempt_timeout
                    )
                except (OSError, asyncio.TimeoutError):
                    await asyncio.sleep(0.05)
                    continue
                writer.close()
                with contextlib.suppress(Exception):
                    await writer.wait_closed()
                self._emitted = True
                result = callback()
                if inspect.isawaitable(result):
                    await result
                return
        finally:
            self._task = None

    def reset(self) -> None:
        task = self._task
        self._task = None
        self._emitted = False
        if task is not None and not task.done():
            task.cancel()


webui_ready_banner = UvicornReadyBanner()

__all__ = ["UvicornReadyBanner", "webui_ready_banner"]
