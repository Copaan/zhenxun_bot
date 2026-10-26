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


def install_uvicorn_pathsend(prefixes: tuple[str, ...]) -> bool:
    """为指定路径的明文 HTTP 请求开启 ASGI pathsend 扩展

    FileResponse 检测到该扩展后改为整文件交给内核发送（loop.sendfile），
    传输过程不再逐块经过事件循环。HTTPS 连接、其他路径以及未实现 sendfile 的
    事件循环（如 uvloop）不声明扩展，仍按块发送。

    参数:
        prefixes: 开启扩展的请求路径前缀

    返回:
        bool: 是否成功安装，uvicorn 内部结构不符时返回 False
    """
    try:
        from uvicorn.protocols.http.httptools_impl import RequestResponseCycle
    except ImportError:
        return False
    if getattr(RequestResponseCycle.send, "__zhenxun_pathsend__", False):
        return True
    required = (
        "transport",
        "response_started",
        "response_complete",
        "chunked_encoding",
        "expected_content_length",
        "disconnected",
    )
    if not all(
        name in RequestResponseCycle.__init__.__code__.co_names for name in required
    ):
        return False
    original_run = RequestResponseCycle.run_asgi
    original_send = RequestResponseCycle.send

    @wraps(original_run)
    async def run_asgi(cycle: Any, app: Any) -> None:
        if (
            cycle.scope["type"] == "http"
            and str(cycle.scope.get("path", "")).startswith(prefixes)
            and cycle.transport.get_extra_info("sslcontext") is None
            and _supports_sendfile()
        ):
            cycle.scope.setdefault("extensions", {})["http.response.pathsend"] = {}
        await original_run(cycle, app)

    @wraps(original_send)
    async def send(cycle: Any, message: Any) -> None:
        if message["type"] != "http.response.pathsend":
            return await original_send(cycle, message)
        if not cycle.response_started or cycle.response_complete:
            raise RuntimeError("Unexpected ASGI message 'http.response.pathsend'.")
        count = cycle.expected_content_length
        if cycle.scope["method"] != "HEAD" and not cycle.chunked_encoding and count:
            try:
                file = await asyncio.to_thread(open, message["path"], "rb")
                with file:
                    sent = await asyncio.get_running_loop().sendfile(
                        cycle.transport, file, count=count
                    )
            except (OSError, RuntimeError):
                sent = -1
            if sent != count:
                cycle.disconnected = True
                cycle.transport.close()
                return
            cycle.expected_content_length = 0
        await original_send(
            cycle, {"type": "http.response.body", "body": b"", "more_body": False}
        )

    send.__zhenxun_pathsend__ = True  # type: ignore[attr-defined]
    setattr(RequestResponseCycle, "run_asgi", run_asgi)
    setattr(RequestResponseCycle, "send", send)
    return True


def _supports_sendfile() -> bool:
    """当前事件循环是否实现 loop.sendfile

    返回:
        bool: 标准 asyncio 事件循环为 True，uvloop 等未实现的为 False
    """
    return isinstance(asyncio.get_running_loop(), asyncio.BaseEventLoop)


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
    "install_uvicorn_pathsend",
    "install_uvicorn_shutdown_hook",
    "install_websocket_drain_lock",
]
