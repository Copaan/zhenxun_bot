from __future__ import annotations

import asyncio
import contextlib
import ipaddress
import json
from typing import Any

from fastapi import HTTPException, Request
from fastapi.responses import PlainTextResponse
from fastapi.staticfiles import StaticFiles
from jose import JWTError, jwt
from starlette.types import Receive, Scope, Send
from starlette.websockets import WebSocket, WebSocketDisconnect

from zhenxun.configs.config import Config
from zhenxun.services.cache import BoundedTTLCache

from .console_access import console_access

PRIVATE_ACCESS_DENIED = "WebUI is available only from this host or a private network"
WEBSOCKET_AUTH_TIMEOUT = 5.0
MAX_LOGIN_FAILURES = 5
_IPV4_PRIVATE_NETWORKS = (
    ipaddress.ip_network("10.0.0.0/8"),
    ipaddress.ip_network("172.16.0.0/12"),
    ipaddress.ip_network("192.168.0.0/16"),
)
_IPV6_PRIVATE_NETWORK = ipaddress.ip_network("fc00::/7")


class LoginAttemptLimiter:
    def __init__(self) -> None:
        self._cache = BoundedTTLCache[str, int](
            "webui_login_failures", ttl_seconds=60, max_items=2048
        )
        self._lock = asyncio.Lock()

    async def reserve(self, client_key: str) -> bool:
        async with self._lock:
            count = await self._cache.get(client_key) or 0
            if count >= MAX_LOGIN_FAILURES:
                return False
            await self._cache.set(client_key, count + 1)
            return True

    async def clear(self, client_key: str) -> None:
        await self._cache.delete(client_key)


login_attempt_limiter = LoginAttemptLimiter()
_AUTHENTICATED_WEBSOCKETS: set[WebSocket] = set()


def is_private_client(host: str | None) -> bool:
    if not host:
        return False
    normalized = host.split("%", 1)[0]
    try:
        address = ipaddress.ip_address(normalized)
    except ValueError:
        return normalized.lower() == "localhost"
    if address.is_loopback or address.is_link_local:
        return True
    if isinstance(address, ipaddress.IPv4Address):
        return any(address in network for network in _IPV4_PRIVATE_NETWORKS)
    return address in _IPV6_PRIVATE_NETWORK


def is_private_scope(scope: Scope) -> bool:
    client = scope.get("client")
    return bool(client and is_private_client(str(client[0])))


def validate_access_token(token: str) -> bool:
    token = token.strip()
    if token.lower().startswith("bearer "):
        token = token[7:].strip()
    if not token:
        return False
    secret = Config.get_config("web-ui", "secret")
    configured_username = Config.get_config("web-ui", "username")
    configured_password = Config.get_config("web-ui", "password")
    if not secret or not configured_username or not configured_password:
        return False
    try:
        payload = jwt.decode(token, secret, algorithms=["HS256"])
    except (JWTError, TypeError, ValueError):
        return False
    username = payload.get("sub")
    if not username or str(username) != str(configured_username):
        return False
    if payload.get("auth_source") == "console":
        return console_access.accepts_boot_id(payload.get("boot_id"))
    return True


async def require_private_request(request: Request) -> None:
    if not is_private_scope(request.scope):
        raise HTTPException(status_code=403, detail=PRIVATE_ACCESS_DENIED)


class PrivateNetworkStaticFiles(StaticFiles):
    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if not is_private_scope(scope):
            await PlainTextResponse(PRIVATE_ACCESS_DENIED, status_code=403)(
                scope, receive, send
            )
            return
        await super().__call__(scope, receive, send)

    async def get_response(self, path: str, scope: Scope):
        response = await super().get_response(path, scope)
        filename = path.rsplit("/", 1)[-1]
        parts = filename.split(".")
        has_content_hash = any(
            len(part) >= 8
            and all(character in "0123456789abcdef" for character in part.lower())
            for part in parts[1:-1]
        )
        if has_content_hash and response.status_code == 200:
            response.headers["Cache-Control"] = "public, max-age=31536000, immutable"
        return response


async def authenticate_websocket(websocket: WebSocket) -> bool:
    if not is_private_scope(websocket.scope):
        await websocket.close(code=1008, reason=PRIVATE_ACCESS_DENIED)
        return False
    await websocket.accept()
    try:
        raw_message = await asyncio.wait_for(
            websocket.receive_text(), timeout=WEBSOCKET_AUTH_TIMEOUT
        )
        payload: Any = json.loads(raw_message)
        token = payload.get("token") if isinstance(payload, dict) else None
    except (TimeoutError, ValueError, WebSocketDisconnect):
        token = None

    if not isinstance(token, str) or not validate_access_token(token):
        await websocket.close(code=1008, reason="authentication required")
        return False
    _AUTHENTICATED_WEBSOCKETS.add(websocket)
    return True


def unregister_authenticated_websocket(websocket: WebSocket) -> None:
    _AUTHENTICATED_WEBSOCKETS.discard(websocket)


def authenticated_websocket_count() -> int:
    return len(_AUTHENTICATED_WEBSOCKETS)


async def revoke_authenticated_websockets() -> None:
    websockets = list(_AUTHENTICATED_WEBSOCKETS)
    _AUTHENTICATED_WEBSOCKETS.clear()
    for websocket in websockets:
        with contextlib.suppress(Exception):
            await asyncio.wait_for(
                websocket.close(code=1008, reason="credentials changed"),
                timeout=2,
            )


__all__ = [
    "PRIVATE_ACCESS_DENIED",
    "PrivateNetworkStaticFiles",
    "authenticate_websocket",
    "authenticated_websocket_count",
    "is_private_client",
    "is_private_scope",
    "login_attempt_limiter",
    "require_private_request",
    "revoke_authenticated_websockets",
    "unregister_authenticated_websocket",
    "validate_access_token",
]
