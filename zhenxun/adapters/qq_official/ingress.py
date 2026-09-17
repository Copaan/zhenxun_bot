from __future__ import annotations

import asyncio
from collections import Counter
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
import json
import os
import ssl
from typing import Any

import aiohttp
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse, Response
from starlette.routing import Route
import uvicorn

MAX_REQUEST_BODY = 2 * 1024 * 1024
MAX_HEADER_BYTES = 16 * 1024
MAX_CONNECTIONS = 512
MAX_IN_FLIGHT = 256
LISTEN_BACKLOG = 2048
UPSTREAM_TIMEOUT_SECONDS = 15.0
REQUEST_READ_TIMEOUT_SECONDS = 15.0

_FORWARDED_HEADERS = (
    "content-type",
    "x-bot-appid",
    "x-signature-ed25519",
    "x-signature-timestamp",
)


@dataclass(frozen=True, slots=True)
class IngressSettings:
    listen_host: str
    listen_port: int
    certfile: str
    keyfile: str
    upstream_url: str


class QQWebhookIngress:
    """Minimal public proxy that leaves verification and payload parsing to worker."""

    def __init__(self, upstream_url: str) -> None:
        self.upstream_url = upstream_url.rstrip("/")
        self._client: aiohttp.ClientSession | None = None
        self._in_flight = 0
        self._in_flight_lock = asyncio.Lock()
        self._counts: Counter[str] = Counter()

    @asynccontextmanager
    async def lifespan(self, _app: Starlette) -> AsyncIterator[None]:
        fingerprint = os.getenv("ZHENXUN_QQ_UPSTREAM_CERT_SHA256", "")
        if self.upstream_url.startswith("https://") and not fingerprint:
            raise RuntimeError("qq_upstream_certificate_pin_missing")
        connector = aiohttp.TCPConnector(
            ssl=aiohttp.Fingerprint(bytes.fromhex(fingerprint)) if fingerprint else True
        )
        self._client = aiohttp.ClientSession(
            connector=connector,
            timeout=aiohttp.ClientTimeout(total=UPSTREAM_TIMEOUT_SECONDS),
            trust_env=False,
            auto_decompress=False,
            cookie_jar=aiohttp.DummyCookieJar(),
        )
        try:
            yield
        finally:
            client = self._client
            self._client = None
            if client is not None:
                await client.close()

    def create_app(self) -> Starlette:
        return Starlette(
            debug=False,
            routes=[
                Route("/qq/webhook", self.webhook, methods=["POST"]),
                Route("/qq/healthz", self.health, methods=["GET"]),
            ],
            lifespan=self.lifespan,
        )

    async def _try_enter(self) -> bool:
        async with self._in_flight_lock:
            if self._in_flight >= MAX_IN_FLIGHT:
                self._counts["overloaded"] += 1
                return False
            self._in_flight += 1
            return True

    async def _leave(self) -> None:
        async with self._in_flight_lock:
            self._in_flight = max(0, self._in_flight - 1)

    @staticmethod
    def _header_size(request: Request) -> int:
        return sum(
            len(name) + len(value) + 4 for name, value in request.scope["headers"]
        )

    async def health(self, _request: Request) -> Response:
        client = self._client
        if client is None:
            return JSONResponse({"status": "degraded"}, status_code=503)
        try:
            response = await self._request(
                client, "GET", f"{self.upstream_url}/qq/healthz"
            )
            try:
                ready = (
                    self._response_status(response) == 200
                    and (await self._response_json(response)).get("status") == "ready"
                )
            finally:
                await self._close_response(response)
        except (aiohttp.ClientError, TimeoutError, json.JSONDecodeError, ValueError):
            ready = False
        return JSONResponse(
            {"status": "ready" if ready else "degraded"},
            status_code=200 if ready else 503,
        )

    async def webhook(self, request: Request) -> Response:
        if self._header_size(request) > MAX_HEADER_BYTES:
            self._counts["headers_too_large"] += 1
            return Response("Request headers too large", status_code=431)
        if request.headers.get("content-encoding", "identity").lower() != "identity":
            self._counts["compressed"] += 1
            return Response("Compressed requests are not supported", status_code=415)
        content_type = request.headers.get("content-type", "").split(";", 1)[0]
        if content_type.strip().lower() != "application/json":
            self._counts["content_type"] += 1
            return Response("Content-Type must be application/json", status_code=415)
        content_length = request.headers.get("content-length")
        if content_length:
            try:
                if int(content_length) > MAX_REQUEST_BODY:
                    self._counts["body_too_large"] += 1
                    return Response("Request body too large", status_code=413)
            except ValueError:
                return Response("Invalid Content-Length", status_code=400)
        if not await self._try_enter():
            return Response("Webhook ingress overloaded", status_code=503)
        try:
            try:
                body = await asyncio.wait_for(
                    self._read_bounded_body(request),
                    timeout=REQUEST_READ_TIMEOUT_SECONDS,
                )
            except TimeoutError:
                self._counts["body_timeout"] += 1
                return Response("Request body timed out", status_code=503)
            if body is None:
                self._counts["body_too_large"] += 1
                return Response("Request body too large", status_code=413)
            client = self._client
            if client is None:
                self._counts["worker_unavailable"] += 1
                return Response("Webhook worker unavailable", status_code=503)
            headers = {
                name: request.headers[name]
                for name in _FORWARDED_HEADERS
                if name in request.headers
            }
            try:
                upstream = await self._request(
                    client,
                    "POST",
                    f"{self.upstream_url}/qq/webhook",
                    data=body,
                    headers=headers,
                )
                try:
                    response_body = await self._response_read(upstream)
                    status = self._response_status(upstream)
                    response_headers = {
                        name: upstream.headers[name]
                        for name in ("Content-Type", "Content-Encoding")
                        if name in upstream.headers
                    }
                finally:
                    await self._close_response(upstream)
            except (aiohttp.ClientError, TimeoutError):
                self._counts["worker_unavailable"] += 1
                return Response("Webhook worker unavailable", status_code=503)
            self._counts[f"upstream_{status}"] += 1
            return Response(
                response_body,
                status_code=status,
                headers=response_headers,
            )
        finally:
            await self._leave()

    @staticmethod
    async def _request(client: Any, method: str, url: str, **kwargs: Any) -> Any:
        """Issue a request through aiohttp or an async test double."""

        try:
            request = getattr(client, method.lower())(
                url, allow_redirects=False, **kwargs
            )
        except TypeError:
            request = getattr(client, method.lower())(
                url, follow_redirects=False, **kwargs
            )
        return await request

    @staticmethod
    async def _close_response(response: Any) -> None:
        release = getattr(response, "release", None)
        if release is not None:
            release()
        close = getattr(response, "aclose", None)
        if close is not None:
            result = close()
            if asyncio.iscoroutine(result):
                await result

    @staticmethod
    def _response_status(response: Any) -> int:
        status = getattr(response, "status", None)
        if status is None:
            status = response.status_code
        return int(status)

    @staticmethod
    async def _response_read(response: Any) -> bytes:
        content = getattr(response, "read", None)
        if content is not None:
            result = content()
            if asyncio.iscoroutine(result):
                return await result
            return result
        return bytes(getattr(response, "content", b""))

    @staticmethod
    async def _response_json(response: Any) -> Any:
        result = response.json()
        if asyncio.iscoroutine(result):
            return await result
        return result

    @staticmethod
    async def _read_bounded_body(request: Request) -> bytes | None:
        body = bytearray()
        async for chunk in request.stream():
            if len(body) + len(chunk) > MAX_REQUEST_BODY:
                return None
            body.extend(chunk)
        return bytes(body)

    async def snapshot(self) -> dict[str, object]:
        async with self._in_flight_lock:
            return {
                "in_flight": self._in_flight,
                "limit": MAX_IN_FLIGHT,
                "counts": dict(self._counts),
            }


def create_ingress_app(upstream_url: str) -> Starlette:
    return QQWebhookIngress(upstream_url).create_app()


def build_ingress_config(settings: IngressSettings, app: Starlette) -> uvicorn.Config:
    config = uvicorn.Config(
        app,
        host=settings.listen_host,
        port=settings.listen_port,
        ssl_certfile=settings.certfile,
        ssl_keyfile=settings.keyfile,
        ssl_version=ssl.PROTOCOL_TLS_SERVER,
        limit_concurrency=MAX_CONNECTIONS,
        backlog=LISTEN_BACKLOG,
        h11_max_incomplete_event_size=MAX_HEADER_BYTES,
        timeout_graceful_shutdown=15,
        timeout_keep_alive=5,
        access_log=False,
        server_header=False,
        date_header=False,
        proxy_headers=False,
    )
    config.load()
    if not isinstance(config.ssl, ssl.SSLContext):
        raise RuntimeError("QQ Webhook TLS context was not created")
    config.ssl.minimum_version = ssl.TLSVersion.TLSv1_2
    return config


def run_ingress(settings: IngressSettings) -> None:
    from zhenxun.services.qq_ingress_state import publish_ingress_state

    class IngressServer(uvicorn.Server):
        async def startup(self, sockets=None):
            await super().startup(sockets=sockets)
            if self.started and not self.should_exit:
                publish_ingress_state("ready", port=settings.listen_port)

    proxy = QQWebhookIngress(settings.upstream_url)
    publish_ingress_state("starting", port=settings.listen_port)
    try:
        IngressServer(build_ingress_config(settings, proxy.create_app())).run()
    finally:
        publish_ingress_state("stopped", port=settings.listen_port)


__all__ = [
    "MAX_CONNECTIONS",
    "MAX_HEADER_BYTES",
    "MAX_IN_FLIGHT",
    "MAX_REQUEST_BODY",
    "IngressSettings",
    "QQWebhookIngress",
    "build_ingress_config",
    "create_ingress_app",
    "run_ingress",
]
