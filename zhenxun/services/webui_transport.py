from __future__ import annotations

import asyncio
from contextlib import suppress
from functools import wraps
import time
from typing import Any
import weakref

CLOSE_GRACE_SECONDS = 0.3
SHUTDOWN_ABORT_SECONDS = 2.0
GRACEFUL_SHUTDOWN_TIMEOUT = 15
SERVER_OPTIONS: dict[str, Any] = {
    "access_log": False,
    "ws_per_message_deflate": False,
    "limit_concurrency": 512,
    "backlog": 2048,
    "timeout_keep_alive": 15,
}


def install_websocket_drain_lock() -> None:
    """串行化旧版 websockets 的流控等待，避免并发发送触发断言"""
    from websockets.legacy.protocol import WebSocketCommonProtocol

    original = WebSocketCommonProtocol.drain
    if getattr(original, "__zhenxun_drain_lock__", False):
        return
    locks: weakref.WeakKeyDictionary[Any, asyncio.Lock] = weakref.WeakKeyDictionary()

    @wraps(original)
    async def drain(protocol):
        lock = locks.get(protocol)
        if lock is None:
            lock = locks[protocol] = asyncio.Lock()
        async with lock:
            return await original(protocol)

    drain.__zhenxun_drain_lock__ = True  # type: ignore[attr-defined]
    setattr(WebSocketCommonProtocol, "drain", drain)


def install_uvicorn_shutdown_hook() -> None:
    """uvicorn 收到退出信号时立即进入关闭状态

    同时停止 HTTP 兼容入口，并回收等待关闭握手的连接。
    """
    import uvicorn

    original = uvicorn.Server.handle_exit
    if getattr(original, "__zhenxun_shutdown_hook__", False):
        return
    tasks: weakref.WeakKeyDictionary[Any, asyncio.Task] = weakref.WeakKeyDictionary()

    def start(server: Any) -> None:
        from zhenxun.services.webui_http import webui_http_compat

        webui_http_compat.stop()
        if server not in tasks:
            tasks[server] = asyncio.create_task(
                _abort_closing_connections(server), name="webui-close-connections"
            )

    @wraps(original)
    def handle_exit(server: Any, sig: int, frame: Any) -> None:
        from zhenxun.services.lifecycle import lifecycle_kernel

        lifecycle_kernel.request_shutdown()
        with suppress(RuntimeError):
            asyncio.get_running_loop().call_soon_threadsafe(start, server)
        original(server, sig, frame)

    handle_exit.__zhenxun_shutdown_hook__ = True  # type: ignore[attr-defined]
    setattr(uvicorn.Server, "handle_exit", handle_exit)


async def _abort_closing_connections(server: Any) -> None:
    """中止已进入关闭状态但迟迟收不到对端确认的连接

    参数:
        server: uvicorn.Server
    """
    from zhenxun.services.lifecycle import lifecycle_kernel

    deadline = time.monotonic() + lifecycle_kernel.shutdown_remaining(
        SHUTDOWN_ABORT_SECONDS
    )
    closing_since: weakref.WeakKeyDictionary[Any, float] = weakref.WeakKeyDictionary()
    connections = server.server_state.connections
    while time.monotonic() < deadline:
        now = time.monotonic()
        for connection in tuple(connections):
            transport = getattr(connection, "transport", None)
            if transport is None or not transport.is_closing():
                continue
            since = closing_since.setdefault(transport, now)
            if now - since >= CLOSE_GRACE_SECONDS:
                transport.abort()
        await asyncio.sleep(0.05)


__all__ = [
    "GRACEFUL_SHUTDOWN_TIMEOUT",
    "SERVER_OPTIONS",
    "install_uvicorn_shutdown_hook",
    "install_websocket_drain_lock",
]
