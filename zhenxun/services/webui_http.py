"""WebUI HTTPS 开启时的 HTTP 兼容入口：跳转到 HTTPS 或以明文提供完整 WebUI"""

from __future__ import annotations

import asyncio
from contextlib import contextmanager, suppress
import errno
import ipaddress
import socket
import sys
from typing import Any

import uvicorn

from zhenxun.services.log import logger

RETRY_DELAYS = (1.0, 2.0, 5.0, 15.0, 30.0)
_ERROR_CODES = (
    ({errno.EADDRINUSE, 10048}, "address_in_use"),
    ({errno.EADDRNOTAVAIL, 10049}, "address_unavailable"),
    ({errno.EACCES, errno.EPERM, 10013}, "permission_denied"),
)

_state: dict[str, Any] = {"mode": "disabled", "port": None, "state": "disabled"}


def http_compat_state() -> dict[str, Any]:
    """HTTP 兼容入口当前状态

    返回:
        dict[str, Any]: mode、port、state 以及降级时的错误码与重试信息
    """
    return dict(_state)


def redirect_hostname(raw_host: str) -> str:
    """从请求 Host 中取出可用于跳转的主机名，非法时返回 localhost

    参数:
        raw_host: 请求头中的 Host

    返回:
        str: 主机名，IPv6 地址带方括号
    """
    value = raw_host.strip()
    if not value or any(ord(char) > 127 for char in value):
        return "localhost"
    if value.startswith("["):
        closing = value.find("]")
        if closing < 0:
            return "localhost"
        try:
            return f"[{ipaddress.IPv6Address(value[1:closing]).compressed}]"
        except ValueError:
            return "localhost"
    hostname, separator, port = value.rpartition(":")
    if separator and port.isdigit() and ":" not in hostname:
        value = hostname
    try:
        return str(ipaddress.ip_address(value))
    except ValueError:
        labels = value.rstrip(".").split(".")
        valid = value == "localhost" or all(
            label
            and len(label) <= 63
            and label[0].isalnum()
            and label[-1].isalnum()
            and all(char.isalnum() or char == "-" for char in label)
            for label in labels
        )
        return value.rstrip(".") if valid else "localhost"


def redirect_app(https_port: int):
    """生成把所有 HTTP 请求 308 跳转到 HTTPS 的 ASGI 应用

    参数:
        https_port: HTTPS 端口

    返回:
        ASGI 应用
    """

    async def app(scope, receive, send) -> None:
        if scope["type"] == "websocket":
            await send({"type": "websocket.close", "code": 1008})
            return
        if scope["type"] != "http":
            return
        headers = dict(scope["headers"])
        hostname = redirect_hostname(headers.get(b"host", b"").decode("latin-1"))
        path = (scope.get("raw_path") or scope["path"].encode()).decode("latin-1")
        if scope.get("query_string"):
            path += "?" + scope["query_string"].decode("latin-1")
        location = f"https://{hostname}:{https_port}{path}"
        await send(
            {
                "type": "http.response.start",
                "status": 308,
                "headers": [
                    (b"location", location.encode("latin-1")),
                    (b"cache-control", b"no-store"),
                    (b"content-length", b"0"),
                ],
            }
        )
        await send({"type": "http.response.body", "body": b""})

    return app


class _CompatServer(uvicorn.Server):
    """不接管进程信号的 uvicorn 实例，随 worker 主服务一起关闭"""

    @contextmanager
    def capture_signals(self):
        yield


def _bind(host: str, port: int) -> socket.socket:
    host = host.strip().strip("[]")
    sock = socket.socket(
        socket.AF_INET6 if ":" in host else socket.AF_INET, socket.SOCK_STREAM
    )
    try:
        if sys.platform != "win32":
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind((host, port))
    except OSError:
        sock.close()
        raise
    return sock


def _error_code(error: OSError) -> str:
    number = getattr(error, "winerror", None) or error.errno
    return next(
        (code for numbers, code in _ERROR_CODES if number in numbers),
        "listener_bind_failed",
    )


class WebUIHttpCompat:
    """管理 HTTP 兼容入口的监听、降级重试与关闭"""

    def __init__(self) -> None:
        self._server: _CompatServer | None = None
        self._task: asyncio.Task[None] | None = None
        self._stopping = False

    async def start(self, context, app, settings) -> None:
        """按 WebUI TLS 设置启动 HTTP 兼容入口，监听失败时降级并在后台重试

        参数:
            context: 生命周期组件上下文
            app: WebUI 的 ASGI 应用（serve 模式使用）
            settings: 当前生效的 WebUI TLS 设置
        """
        mode = settings.effective_http_mode
        _state.clear()
        _state.update(mode=mode, port=None, state="disabled")
        if mode == "disabled":
            return
        _state.update(port=settings.redirect_port, state="starting")
        self._stopping = False
        config = uvicorn.Config(
            app if mode == "serve" else redirect_app(settings.port),
            lifespan="off",
            log_config=None,
            access_log=False,
            timeout_graceful_shutdown=3,
        )
        sock = self._try_bind(settings, 0)
        self._task = context.spawn_task(
            self._run(config, settings, sock),
            name="webui-http-compat",
            cancel=self._request_stop,
        )

    def _try_bind(self, settings, retry_count: int) -> socket.socket | None:
        try:
            sock = _bind(settings.host, settings.redirect_port)
        except OSError as error:
            delay = RETRY_DELAYS[min(retry_count, len(RETRY_DELAYS) - 1)]
            _state.update(
                state="degraded",
                error_code=_error_code(error),
                errno=getattr(error, "winerror", None) or error.errno,
                retry_count=retry_count + 1,
                retry_in_seconds=delay,
            )
            if retry_count == 0:
                logger.warning(
                    f"HTTP 兼容入口监听 {settings.host}:{settings.redirect_port} "
                    f"失败({_state['error_code']})，HTTPS 不受影响，将自动重试",
                    "WebUi",
                )
            return None
        for key in ("error_code", "errno", "retry_count", "retry_in_seconds"):
            _state.pop(key, None)
        _state["state"] = "ready"
        return sock

    async def _run(self, config, settings, sock: socket.socket | None) -> None:
        retry_count = 0
        while not self._stopping:
            if sock is None:
                await asyncio.sleep(_state.get("retry_in_seconds") or 1.0)
                if self._stopping:
                    break
                retry_count += 1
                sock = self._try_bind(settings, retry_count)
                if sock is None:
                    continue
                logger.info("HTTP 兼容入口已恢复监听", "WebUi")
            retry_count = 0
            self._server = _CompatServer(config)
            try:
                await self._server.serve(sockets=[sock])
            finally:
                self._server = None
                with suppress(OSError):
                    sock.close()
                sock = None
        _state["state"] = "stopped"

    def _request_stop(self) -> None:
        self._stopping = True
        if self._server is not None:
            self._server.should_exit = True
        elif self._task is not None:
            self._task.cancel()


webui_http_compat = WebUIHttpCompat()

__all__ = ["http_compat_state", "redirect_app", "webui_http_compat"]
