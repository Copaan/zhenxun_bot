from __future__ import annotations

import asyncio
from collections.abc import Coroutine
import contextlib
import ipaddress
import json
import time
from typing import Any

from fastapi import HTTPException, Request
from fastapi.responses import PlainTextResponse
from fastapi.staticfiles import StaticFiles
from jose import JWTError, jwt
from jose.exceptions import ExpiredSignatureError
from starlette.types import Receive, Scope, Send
from starlette.websockets import WebSocket, WebSocketDisconnect, WebSocketState

from zhenxun.configs.config import Config
from zhenxun.services.cache import BoundedTTLCache
from zhenxun.services.webui_transport import transport_runtime

from .console_access import console_access

PRIVATE_ACCESS_DENIED = "WebUI is available only from this host or a private network"
WEBSOCKET_AUTH_TIMEOUT = 5.0
WEBSOCKET_AUTH_EXPIRED = 4401
MAX_LOGIN_FAILURES = 5
_IPV4_PRIVATE_NETWORKS = (
    ipaddress.ip_network("10.0.0.0/8"),
    ipaddress.ip_network("172.16.0.0/12"),
    ipaddress.ip_network("192.168.0.0/16"),
)
_IPV6_PRIVATE_NETWORK = ipaddress.ip_network("fc00::/7")
_LIFECYCLE_CONTEXT: Any | None = None


def bind_lifecycle_context(context: Any | None) -> None:
    global _LIFECYCLE_CONTEXT
    _LIFECYCLE_CONTEXT = context


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


class WebSocketAuthLease:
    __slots__ = ("closing", "expires_at", "expiry_task", "send_lock", "sid")

    def __init__(
        self,
        *,
        sid: str,
        expires_at: float | None,
        send_lock: asyncio.Lock,
    ) -> None:
        self.sid = sid
        self.expires_at = expires_at
        self.send_lock = send_lock
        self.closing = False
        self.expiry_task: asyncio.Task[None] | None = None


_AUTHENTICATED_WEBSOCKETS: dict[WebSocket, WebSocketAuthLease] = {}
_WINDOWS_DISCONNECT_ERRORS = {10053, 10054}


def is_websocket_disconnect_error(error: BaseException) -> bool:
    """Return whether an exception is an expected socket disconnect at a WS boundary."""
    if isinstance(error, WebSocketDisconnect | ConnectionResetError | BrokenPipeError):
        return True
    return isinstance(error, OSError) and getattr(error, "winerror", None) in (
        _WINDOWS_DISCONNECT_ERRORS
    )


def record_websocket_disconnect(error: BaseException) -> bool:
    expected = is_websocket_disconnect_error(error)
    if expected and isinstance(error, ConnectionResetError | BrokenPipeError | OSError):
        transport_runtime.record("websocket_reset")
    return expected


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


def decode_access_token_status(
    token: str,
) -> tuple[dict[str, Any] | None, str]:
    token = token.strip()
    if token.lower().startswith("bearer "):
        token = token[7:].strip()
    if not token:
        return None, "invalid"
    secret = Config.get_config("web-ui", "secret")
    configured_username = Config.get_config("web-ui", "username")
    configured_password = Config.get_config("web-ui", "password")
    if not secret or not configured_username or not configured_password:
        return None, "invalid"
    try:
        payload = jwt.decode(token, secret, algorithms=["HS256"])
    except ExpiredSignatureError:
        return None, "expired"
    except (JWTError, TypeError, ValueError):
        return None, "invalid"
    username = payload.get("sub")
    if not username or str(username) != str(configured_username):
        return None, "invalid"
    if payload.get("auth_source") == "console":
        if not console_access.accepts_boot_id(payload.get("boot_id")):
            return None, "invalid"
    return payload, "valid"


def _decode_access_token(token: str) -> dict[str, Any] | None:
    payload, _ = decode_access_token_status(token)
    return payload


def validate_access_token(token: str) -> bool:
    return _decode_access_token(token) is not None


def _spawn_expiry_task(websocket: WebSocket, expires_at: float) -> asyncio.Task[None]:
    coroutine = _expire_websocket(websocket, expires_at)
    if _LIFECYCLE_CONTEXT is not None:
        return _LIFECYCLE_CONTEXT.spawn_detached(
            coroutine,
            scope_id=f"websocket-auth-{id(websocket)}-{time.monotonic_ns()}",
            scope="task",
            name="webui-websocket-auth-expiry",
        )
    return asyncio.create_task(coroutine, name="webui-websocket-auth-expiry")


def spawn_websocket_task(
    coroutine: Coroutine[Any, Any, None], *, scope_id: str, name: str
) -> asyncio.Task[None]:
    if _LIFECYCLE_CONTEXT is not None:
        return _LIFECYCLE_CONTEXT.spawn_detached(
            coroutine,
            scope_id=scope_id,
            scope="task",
            name=name,
        )
    return asyncio.create_task(coroutine, name=name)


async def _expire_websocket(websocket: WebSocket, expires_at: float) -> None:
    delay = max(0.0, expires_at - time.time())
    await asyncio.sleep(delay)
    lease = _AUTHENTICATED_WEBSOCKETS.get(websocket)
    if (
        lease is None
        or lease.closing
        or lease.expires_at != expires_at
        or time.time() < expires_at
    ):
        return
    await close_authenticated_websocket(
        websocket, code=WEBSOCKET_AUTH_EXPIRED, reason="authentication expired"
    )


async def close_authenticated_websocket(
    websocket: WebSocket, *, code: int = 1000, reason: str = ""
) -> bool:
    lease = _AUTHENTICATED_WEBSOCKETS.get(websocket)
    lock = lease.send_lock if lease is not None else asyncio.Lock()
    async with lock:
        if lease is not None:
            lease.closing = True
        if not _websocket_connected(websocket):
            return False
        try:
            await websocket.close(code=code, reason=reason)
        except Exception as error:
            if isinstance(error, RuntimeError) or is_websocket_disconnect_error(error):
                record_websocket_disconnect(error)
                return False
            raise
        if code == 1012:
            transport_runtime.record("cooperative_close")
        return True


async def send_authenticated_text(websocket: WebSocket, text: str) -> bool:
    lease = _AUTHENTICATED_WEBSOCKETS.get(websocket)
    if lease is None:
        return False
    async with lease.send_lock:
        if lease.closing or not _websocket_connected(websocket):
            return False
        try:
            await websocket.send_text(text)
        except Exception as error:
            if isinstance(error, RuntimeError) or is_websocket_disconnect_error(error):
                lease.closing = True
                transport_runtime.record("websocket_send_failure")
                record_websocket_disconnect(error)
                unregister_authenticated_websocket(websocket)
                return False
            raise
        return True


async def send_authenticated_json(websocket: WebSocket, data: Any) -> bool:
    lease = _AUTHENTICATED_WEBSOCKETS.get(websocket)
    if lease is None:
        return False
    async with lease.send_lock:
        if lease.closing or not _websocket_connected(websocket):
            return False
        try:
            await websocket.send_json(data)
        except Exception as error:
            if isinstance(error, RuntimeError) or is_websocket_disconnect_error(error):
                lease.closing = True
                transport_runtime.record("websocket_send_failure")
                record_websocket_disconnect(error)
                unregister_authenticated_websocket(websocket)
                return False
            raise
        return True


def renew_authenticated_websockets(sid: str, expires_at: float) -> int:
    renewed = 0
    for websocket, lease in list(_AUTHENTICATED_WEBSOCKETS.items()):
        if lease.sid != sid or lease.closing:
            continue
        task = lease.expiry_task
        if task is not None and task is not asyncio.current_task() and not task.done():
            task.cancel()
        lease.expires_at = expires_at
        lease.expiry_task = _spawn_expiry_task(websocket, expires_at)
        renewed += 1
    return renewed


def _websocket_connected(websocket: WebSocket) -> bool:
    return (
        getattr(websocket, "client_state", WebSocketState.CONNECTED)
        == WebSocketState.CONNECTED
        and getattr(websocket, "application_state", WebSocketState.CONNECTED)
        == WebSocketState.CONNECTED
    )


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
    except OSError as error:
        if not is_websocket_disconnect_error(error):
            raise
        record_websocket_disconnect(error)
        return False

    decoded, auth_status = (
        decode_access_token_status(token)
        if isinstance(token, str)
        else (None, "invalid")
    )
    valid = decoded is not None or (
        auth_status != "expired"
        and isinstance(token, str)
        and validate_access_token(token)
    )
    if not valid:
        if auth_status == "expired":
            await close_authenticated_websocket(
                websocket,
                code=WEBSOCKET_AUTH_EXPIRED,
                reason="authentication expired",
            )
        else:
            await close_authenticated_websocket(
                websocket, code=1008, reason="authentication required"
            )
        return False
    if decoded is None:
        decoded = {}
    sid = str(decoded.get("sid") or "")
    expires_at = decoded.get("exp") if decoded is not None else None
    lease = WebSocketAuthLease(
        sid=sid,
        expires_at=float(expires_at) if isinstance(expires_at, int | float) else None,
        send_lock=asyncio.Lock(),
    )
    _AUTHENTICATED_WEBSOCKETS[websocket] = lease
    if isinstance(expires_at, int | float):
        lease.expiry_task = _spawn_expiry_task(websocket, float(expires_at))
    return True


def unregister_authenticated_websocket(websocket: WebSocket) -> None:
    lease = _AUTHENTICATED_WEBSOCKETS.pop(websocket, None)
    task = lease.expiry_task if lease is not None else None
    if task is not None and task is not asyncio.current_task() and not task.done():
        task.cancel()


def authenticated_websocket_count() -> int:
    return len(_AUTHENTICATED_WEBSOCKETS)


async def revoke_authenticated_websockets() -> None:
    websockets = list(_AUTHENTICATED_WEBSOCKETS)
    for websocket in websockets:
        with contextlib.suppress(Exception, asyncio.TimeoutError):
            await asyncio.wait_for(
                close_authenticated_websocket(
                    websocket, code=1008, reason="credentials changed"
                ),
                timeout=2,
            )
        unregister_authenticated_websocket(websocket)


async def quiesce_authenticated_websockets(
    *, code: int = 1012, reason: str = "service restart", timeout: float = 2.0
) -> int:
    entries = list(_AUTHENTICATED_WEBSOCKETS.items())
    websockets = [websocket for websocket, _lease in entries]
    if not websockets:
        return 0
    for _websocket, lease in entries:
        lease.closing = True
    await asyncio.gather(
        *(
            asyncio.wait_for(
                close_authenticated_websocket(websocket, code=code, reason=reason),
                timeout=timeout,
            )
            for websocket in websockets
        ),
        return_exceptions=True,
    )
    for websocket in websockets:
        unregister_authenticated_websocket(websocket)
    return len(websockets)


__all__ = [
    "PRIVATE_ACCESS_DENIED",
    "WEBSOCKET_AUTH_EXPIRED",
    "PrivateNetworkStaticFiles",
    "authenticate_websocket",
    "authenticated_websocket_count",
    "close_authenticated_websocket",
    "decode_access_token_status",
    "is_private_client",
    "is_private_scope",
    "is_websocket_disconnect_error",
    "login_attempt_limiter",
    "quiesce_authenticated_websockets",
    "record_websocket_disconnect",
    "renew_authenticated_websockets",
    "require_private_request",
    "revoke_authenticated_websockets",
    "send_authenticated_json",
    "send_authenticated_text",
    "spawn_websocket_task",
    "unregister_authenticated_websocket",
    "validate_access_token",
]
