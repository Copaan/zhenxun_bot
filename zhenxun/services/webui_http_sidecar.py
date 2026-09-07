from __future__ import annotations

import asyncio
from contextlib import suppress
from dataclasses import dataclass
import ipaddress
import os
import signal
import time
from typing import Any

from aiohttp import (
    ClientError,
    ClientSession,
    ClientTimeout,
    DummyCookieJar,
    Fingerprint,
    TCPConnector,
    WSMsgType,
    web,
)

from zhenxun.utils.network import is_private_client

from .webui_http_sidecar_state import (
    sanitized_sidecar_error,
    write_http_sidecar_state,
)
from .webui_transport import transport_runtime

_HOP_BY_HOP_HEADERS = {
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
    "sec-websocket-extensions",
    "sec-websocket-key",
    "sec-websocket-protocol",
    "sec-websocket-version",
}
_FORWARDED_HEADERS = {
    "forwarded",
    "x-forwarded-for",
    "x-forwarded-host",
    "x-forwarded-port",
    "x-forwarded-proto",
    "x-real-ip",
}


@dataclass(frozen=True, slots=True)
class HttpSidecarSettings:
    mode: str
    listen_host: str
    listen_port: int
    upstream_host: str
    upstream_port: int
    certificate_sha256: str

    @property
    def upstream_base(self) -> str:
        host = (
            f"[{self.upstream_host}]"
            if ":" in self.upstream_host
            else self.upstream_host
        )
        return f"https://{host}:{self.upstream_port}"


class HttpSidecar:
    def __init__(self, settings: HttpSidecarSettings) -> None:
        self.settings = settings
        self._session: ClientSession | None = None
        self._active_connections = 0
        self._proxy_failures = 0
        self._last_error: str | None = None
        self._state = "starting"
        self._state_lock = asyncio.Lock()
        self._state_task: asyncio.Task[None] | None = None
        self._closing = False
        self._requests: set[asyncio.Task[Any]] = set()
        self._websockets: set[Any] = set()
        self._stream_truncations = 0
        self._drain_started: float | None = None
        self._drain_timed_out = False

    def _fingerprint(self) -> Fingerprint:
        return Fingerprint(bytes.fromhex(self.settings.certificate_sha256))

    async def start(self, _app: web.Application) -> None:
        transport_runtime.install()
        connector = TCPConnector(ssl=self._fingerprint())
        self._session = ClientSession(
            connector=connector,
            timeout=ClientTimeout(total=None, connect=5, sock_connect=5),
            auto_decompress=False,
            cookie_jar=DummyCookieJar(),
        )
        try:
            if self.settings.mode == "serve":
                async with self._session.get(
                    f"{self.settings.upstream_base}/zhenxun/api/configure/status",
                    timeout=ClientTimeout(total=5),
                    allow_redirects=False,
                ) as response:
                    if not 200 <= response.status < 300:
                        raise ConnectionError("upstream_probe_failed")
                    response.release()
        except BaseException:
            await self._session.close()
            self._session = None
            raise
        self._write_state("starting")
        self._state_task = asyncio.create_task(
            self._publish_state_loop(),
            name="webui-http-sidecar-state",
        )

    async def shutdown(self, _app: web.Application) -> None:
        if self._closing:
            return
        self._closing = True
        self._drain_started = time.monotonic()
        if self._state_task is not None:
            self._state_task.cancel()
            with suppress(asyncio.CancelledError):
                await self._state_task
            self._state_task = None
        self._write_state("stopping")
        closers = [
            asyncio.create_task(ws.close(code=1012, message=b"service restart"))
            for ws in tuple(self._websockets)
        ]
        try:
            if closers:
                await asyncio.wait(closers, timeout=1)
            pending = set(self._requests)
            if pending:
                _, pending = await asyncio.wait(pending, timeout=2)
            self._drain_timed_out = bool(pending)
            for task in pending:
                task.cancel()
            if pending:
                await asyncio.gather(*pending, return_exceptions=True)
        finally:
            for task in closers:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*closers, return_exceptions=True)

    async def stop(self, _app: web.Application) -> None:
        transport_runtime.retain_until_loop_close()
        self._write_state("stopping")
        if self._state_task is not None:
            self._state_task.cancel()
            with suppress(asyncio.CancelledError):
                await self._state_task
            self._state_task = None
        if self._session is not None:
            await self._session.close()
            self._session = None
        self._write_state("stopped", active_connections=0)

    def _write_state(self, state: str, **changes: Any) -> None:
        from zhenxun.services.webui_http_sidecar_state import listener_identity

        self._state = state
        values = {
            **changes,
            **listener_identity(),
            "mode": self.settings.mode,
            "port": self.settings.listen_port,
            "pid": os.getpid(),
            "startup_id": os.environ.get("ZHENXUN_HTTP_SIDECAR_STARTUP_ID", ""),
            "stream_truncations": self._stream_truncations,
            "drain_timed_out": self._drain_timed_out,
            "drain_seconds": round(time.monotonic() - self._drain_started, 3)
            if self._drain_started is not None
            else None,
            "state": state,
            "active_connections": changes.get(
                "active_connections", self._active_connections
            ),
            "proxy_failures": changes.get("proxy_failures", self._proxy_failures),
            "last_error": changes.get("last_error", self._last_error),
            "transport": transport_runtime.snapshot(),
        }
        write_http_sidecar_state(**values)

    async def _publish_state_loop(self) -> None:
        while True:
            await asyncio.sleep(1)
            async with self._state_lock:
                publish = asyncio.create_task(
                    asyncio.to_thread(self._write_state, self._state)
                )
                try:
                    await asyncio.shield(publish)
                except asyncio.CancelledError:
                    await publish
                    raise

    async def _connection_opened(self) -> None:
        async with self._state_lock:
            self._active_connections += 1
            task = asyncio.current_task()
            if task is not None:
                self._requests.add(task)

    async def _connection_closed(self) -> None:
        async with self._state_lock:
            self._active_connections = max(0, self._active_connections - 1)
            self._requests.discard(asyncio.current_task())

    async def _succeeded(self) -> None:
        async with self._state_lock:
            if self._closing:
                return
            if self._state == "ready" and self._last_error is None:
                return
            self._last_error = None
            self._write_state("ready")

    async def _failed(self, error: BaseException) -> None:
        async with self._state_lock:
            self._proxy_failures += 1
            self._last_error = sanitized_sidecar_error(error)
            self._write_state("stopping" if self._closing else "degraded")

    @staticmethod
    def _request_headers(request: web.Request) -> dict[str, str]:
        connection_headers = {
            value.strip().casefold()
            for value in request.headers.get("Connection", "").split(",")
            if value.strip()
        }
        headers = {
            name: value
            for name, value in request.headers.items()
            if name.casefold()
            not in _HOP_BY_HOP_HEADERS | _FORWARDED_HEADERS | connection_headers
        }
        headers["X-Forwarded-For"] = request.remote or ""
        headers["X-Forwarded-Proto"] = "http"
        headers["X-Forwarded-Host"] = request.host
        return headers

    @staticmethod
    def _response_headers(headers: Any) -> list[tuple[str, str]]:
        connection_headers = {
            value.strip().casefold()
            for value in headers.get("Connection", "").split(",")
            if value.strip()
        }
        return [
            (name, value)
            for name, value in headers.items()
            if name.casefold() not in _HOP_BY_HOP_HEADERS | connection_headers
        ]

    @staticmethod
    def _redirect_hostname(raw_host: str) -> str:
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

    async def handle(self, request: web.Request) -> web.StreamResponse:
        if self._closing:
            raise web.HTTPServiceUnavailable(text="HTTP sidecar is stopping")
        if not is_private_client(request.remote):
            raise web.HTTPForbidden(
                text="WebUI is available only from this host or a private network"
            )
        if self.settings.mode == "redirect":
            hostname = self._redirect_hostname(request.host)
            raise web.HTTPPermanentRedirect(
                location=(
                    f"https://{hostname}:{self.settings.upstream_port}"
                    f"{request.raw_path}"
                ),
                headers={"Cache-Control": "no-store"},
            )
        if request.headers.get("Upgrade", "").casefold() == "websocket":
            return await self._proxy_websocket(request)
        return await self._proxy_http(request)

    async def _proxy_http(self, request: web.Request) -> web.StreamResponse:
        if self._session is None:
            raise web.HTTPServiceUnavailable(
                text="HTTP compatibility sidecar is starting"
            )
        await self._connection_opened()
        response: web.StreamResponse | None = None
        try:
            upstream_url = f"{self.settings.upstream_base}{request.raw_path}"
            async with self._session.request(
                request.method,
                upstream_url,
                headers=self._request_headers(request),
                data=request.content.iter_chunked(64 * 1024),
                allow_redirects=False,
            ) as upstream:
                response = web.StreamResponse(
                    status=upstream.status,
                    reason=upstream.reason,
                    headers=self._response_headers(upstream.headers),
                )
                await response.prepare(request)
                if request.method != "HEAD":
                    async for chunk in upstream.content.iter_chunked(64 * 1024):
                        await response.write(chunk)
                await response.write_eof()
                await self._succeeded()
                return response
        except (ClientError, ConnectionError, OSError, asyncio.TimeoutError) as error:
            if response is not None and response.prepared:
                self._stream_truncations += 1
                await self._failed(error)
                response.force_close()
                if request.transport is not None:
                    request.transport.abort()
                return response
            await self._failed(error)
            if isinstance(error, asyncio.TimeoutError):
                raise web.HTTPGatewayTimeout(text="HTTPS worker timed out") from error
            raise web.HTTPBadGateway(text="HTTPS worker is unavailable") from error
        finally:
            await self._connection_closed()

    async def _proxy_websocket(self, request: web.Request) -> web.StreamResponse:
        if self._session is None:
            raise web.HTTPServiceUnavailable(
                text="HTTP compatibility sidecar is starting"
            )
        protocols = [
            item.strip()
            for item in request.headers.get("Sec-WebSocket-Protocol", "").split(",")
            if item.strip()
        ]
        await self._connection_opened()
        client_ws: web.WebSocketResponse | None = None
        tasks: set[asyncio.Task[Any]] = set()
        try:
            upstream_url = f"{self.settings.upstream_base}{request.raw_path}"
            async with self._session.ws_connect(
                upstream_url,
                headers=self._request_headers(request),
                protocols=protocols,
                autoping=False,
                autoclose=False,
                timeout=1,
                heartbeat=None,
            ) as upstream_ws:
                client_ws = web.WebSocketResponse(
                    autoping=False,
                    autoclose=False,
                    timeout=1,
                    protocols=[upstream_ws.protocol] if upstream_ws.protocol else [],
                )
                await client_ws.prepare(request)
                self._websockets.update((client_ws, upstream_ws))

                async def client_to_upstream() -> tuple[int, bytes]:
                    while True:
                        message = await client_ws.receive()
                        if message.type == WSMsgType.TEXT:
                            await upstream_ws.send_str(message.data)
                        elif message.type == WSMsgType.BINARY:
                            await upstream_ws.send_bytes(message.data)
                        elif message.type == WSMsgType.PING:
                            await upstream_ws.ping(message.data)
                        elif message.type == WSMsgType.PONG:
                            await upstream_ws.pong(message.data)
                        elif message.type in {
                            WSMsgType.CLOSE,
                            WSMsgType.CLOSED,
                            WSMsgType.CLOSING,
                        }:
                            code = self._close_code(
                                message.data
                                if message.type == WSMsgType.CLOSE
                                else 1011
                            )
                            reason = (
                                str(message.extra or "").encode()
                                if message.type == WSMsgType.CLOSE
                                else b""
                            )
                            return code, reason
                        elif message.type == WSMsgType.ERROR:
                            raise client_ws.exception() or ConnectionError(
                                "client_websocket"
                            )

                async def upstream_to_client() -> tuple[int, bytes]:
                    while True:
                        message = await upstream_ws.receive()
                        if message.type == WSMsgType.TEXT:
                            await client_ws.send_str(message.data)
                        elif message.type == WSMsgType.BINARY:
                            await client_ws.send_bytes(message.data)
                        elif message.type == WSMsgType.PING:
                            await client_ws.ping(message.data)
                        elif message.type == WSMsgType.PONG:
                            await client_ws.pong(message.data)
                        elif message.type in {
                            WSMsgType.CLOSE,
                            WSMsgType.CLOSED,
                            WSMsgType.CLOSING,
                        }:
                            code = self._close_code(
                                message.data
                                if message.type == WSMsgType.CLOSE
                                else 1011
                            )
                            reason = (
                                str(message.extra or "").encode()
                                if message.type == WSMsgType.CLOSE
                                else b""
                            )
                            return code, reason
                        elif message.type == WSMsgType.ERROR:
                            raise upstream_ws.exception() or ConnectionError(
                                "upstream_websocket"
                            )

                tasks = {
                    asyncio.create_task(client_to_upstream(), name="sidecar-ws-client"),
                    asyncio.create_task(
                        upstream_to_client(), name="sidecar-ws-upstream"
                    ),
                }
                close_code, close_reason = 1011, b""
                try:
                    done, _ = await asyncio.wait(
                        tasks, return_when=asyncio.FIRST_COMPLETED
                    )
                    for task in done:
                        close_code, close_reason = task.result()
                finally:
                    for task in tasks:
                        if not task.done():
                            task.cancel()
                    await asyncio.gather(*tasks, return_exceptions=True)
                    self._websockets.difference_update((client_ws, upstream_ws))
                    await asyncio.wait_for(
                        asyncio.gather(
                            client_ws.close(code=close_code, message=close_reason),
                            upstream_ws.close(code=close_code, message=close_reason),
                            return_exceptions=True,
                        ),
                        timeout=1,
                    )
                await self._succeeded()
                return client_ws
        except (ClientError, ConnectionError, OSError, asyncio.TimeoutError) as error:
            await self._failed(error)
            if client_ws is None or not client_ws.prepared:
                raise web.HTTPBadGateway(text="HTTPS worker is unavailable") from error
            with suppress(Exception):
                await client_ws.close(code=1012, message=b"worker unavailable")
            return client_ws
        finally:
            if client_ws is not None and not client_ws.closed:
                with suppress(Exception):
                    await asyncio.wait_for(client_ws.close(code=1011), timeout=1)
            await self._connection_closed()

    @staticmethod
    def _close_code(code: Any) -> int:
        return (
            code
            if isinstance(code, int)
            and (
                code
                in {
                    1000,
                    1001,
                    1002,
                    1003,
                    1007,
                    1008,
                    1009,
                    1010,
                    1011,
                    1012,
                    1013,
                    1014,
                }
                or 3000 <= code <= 4999
            )
            else 1011
        )


def create_sidecar_app(settings: HttpSidecarSettings) -> web.Application:
    sidecar = HttpSidecar(settings)
    app = web.Application(
        client_max_size=1024**3, handler_args={"auto_decompress": False}
    )
    app[SIDECAR_KEY] = sidecar
    app.on_startup.append(sidecar.start)
    app.on_shutdown.append(sidecar.shutdown)
    app.on_cleanup.append(sidecar.stop)
    app.router.add_route("*", "/{path:.*}", sidecar.handle)
    return app


SIDECAR_KEY = web.AppKey("http_sidecar", HttpSidecar)


async def _serve_http_sidecar(settings: HttpSidecarSettings) -> None:
    app = create_sidecar_app(settings)
    sidecar = app[SIDECAR_KEY]
    runner = web.AppRunner(
        app, handle_signals=False, access_log=None, shutdown_timeout=1
    )
    loop = asyncio.get_running_loop()
    stopped = asyncio.Event()
    previous = {}

    def request_stop(_sig: int, _frame: Any) -> None:
        loop.call_soon_threadsafe(stopped.set)

    try:
        for name in ("SIGINT", "SIGTERM", "SIGBREAK"):
            sig = getattr(signal, name, None)
            if sig is not None:
                previous[sig] = signal.signal(sig, request_stop)
        await runner.setup()
        site = web.TCPSite(runner, settings.listen_host, settings.listen_port)
        await site.start()
        sidecar._write_state("ready")
        await stopped.wait()
    finally:
        try:
            await asyncio.wait_for(runner.cleanup(), timeout=5)
        except asyncio.TimeoutError:
            sidecar._drain_timed_out = True
            await sidecar.stop(app)
        finally:
            for sig, handler in previous.items():
                signal.signal(sig, handler)


def run_http_sidecar(settings: HttpSidecarSettings) -> None:
    from zhenxun.services.webui_http_sidecar_state import listener_identity

    write_http_sidecar_state(
        **listener_identity(),
        mode=settings.mode,
        port=settings.listen_port,
        state="starting",
        active_connections=0,
        proxy_failures=0,
        last_error=None,
        startup_id=os.environ.get("ZHENXUN_HTTP_SIDECAR_STARTUP_ID", ""),
    )
    try:
        asyncio.run(_serve_http_sidecar(settings))
    except BaseException as error:
        write_http_sidecar_state(
            mode=settings.mode,
            port=settings.listen_port,
            pid=os.getpid(),
            state="degraded",
            active_connections=0,
            last_error=sanitized_sidecar_error(error),
        )
        raise


__all__ = [
    "HttpSidecar",
    "HttpSidecarSettings",
    "create_sidecar_app",
    "run_http_sidecar",
]
