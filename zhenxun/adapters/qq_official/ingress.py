from __future__ import annotations

import asyncio
from collections import Counter
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
import json
import ssl

import httpx
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
        self._client: httpx.AsyncClient | None = None
        self._in_flight = 0
        self._in_flight_lock = asyncio.Lock()
        self._counts: Counter[str] = Counter()

    @asynccontextmanager
    async def lifespan(self, _app: Starlette) -> AsyncIterator[None]:
        self._client = httpx.AsyncClient(
            timeout=httpx.Timeout(UPSTREAM_TIMEOUT_SECONDS),
            trust_env=False,
            follow_redirects=False,
        )
        try:
            yield
        finally:
            client = self._client
            self._client = None
            if client is not None:
                await client.aclose()

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
            response = await client.get(f"{self.upstream_url}/qq/healthz")
            ready = (
                response.status_code == 200 and response.json().get("status") == "ready"
            )
        except (httpx.HTTPError, json.JSONDecodeError, ValueError):
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
                upstream = await client.post(
                    f"{self.upstream_url}/qq/webhook",
                    content=body,
                    headers=headers,
                )
            except httpx.TransportError:
                self._counts["worker_unavailable"] += 1
                return Response("Webhook worker unavailable", status_code=503)
            self._counts[f"upstream_{upstream.status_code}"] += 1
            response_headers = {}
            if content_type := upstream.headers.get("content-type"):
                response_headers["Content-Type"] = content_type
            return Response(
                upstream.content,
                status_code=upstream.status_code,
                headers=response_headers,
            )
        finally:
            await self._leave()

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
    proxy = QQWebhookIngress(settings.upstream_url)
    uvicorn.Server(build_ingress_config(settings, proxy.create_app())).run()


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
