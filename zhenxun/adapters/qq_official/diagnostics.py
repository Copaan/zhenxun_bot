from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import re
from typing import Any, Literal
from urllib.parse import urlsplit

import httpx
from nonebot.adapters.qq.exception import ActionFailed, NetworkError

QQConnectionState = Literal[
    "authorizing",
    "gateway",
    "connecting",
    "connected",
    "reconnecting",
    "failed",
]

_SAFE_VALUE_PATTERN = re.compile(r"[^A-Za-z0-9._:-]+")


@dataclass(frozen=True, slots=True)
class QQPublicError:
    code: str
    message: str
    provider_code: str | None = None
    http_status: int | None = None
    trace_id: str | None = None
    retryable: bool = False

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class QQConnectionDiagnostic:
    app_id: str
    mode: Literal["websocket", "webhook"]
    state: QQConnectionState
    updated_at: str
    error: QQPublicError | None = None

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["error"] = self.error.to_dict() if self.error else None
        return data


_diagnostics: dict[str, QQConnectionDiagnostic] = {}


def safe_avatar_url(value: object) -> str | None:
    raw = str(value or "").strip()
    if not raw or len(raw) > 2048:
        return None
    try:
        parsed = urlsplit(raw)
    except ValueError:
        return None
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        return None
    if parsed.username or parsed.password:
        return None
    return raw


def _safe_scalar(value: object, *, limit: int = 128) -> str | None:
    if value is None:
        return None
    raw = _SAFE_VALUE_PATTERN.sub("", str(value).strip())
    return raw[:limit] or None


def _response_error(response: httpx.Response) -> tuple[str | None, str | None]:
    provider_code = None
    try:
        payload = response.json()
        if isinstance(payload, dict):
            provider_code = _safe_scalar(payload.get("code") or payload.get("retcode"))
    except Exception:
        pass
    trace_id = _safe_scalar(
        response.headers.get("X-Tps-trace-ID")
        or response.headers.get("X-Trace-ID")
        or response.headers.get("Trace-ID")
    )
    return provider_code, trace_id


def public_error_from_exception(
    error: BaseException,
    *,
    stage: Literal["credential", "gateway", "websocket_auth", "websocket"],
) -> QQPublicError:
    code_map = {
        "credential": ("qq_credential_failed", "QQ机器人凭据验证失败。"),
        "gateway": ("qq_gateway_failed", "QQ WebSocket Gateway获取失败。"),
        "websocket_auth": ("qq_websocket_auth_failed", "QQ WebSocket鉴权失败。"),
        "websocket": ("qq_websocket_failed", "QQ WebSocket连接失败。"),
    }
    code, message = code_map[stage]
    provider_code: str | None = None
    http_status: int | None = None
    trace_id: str | None = None
    retryable = False

    if isinstance(error, ActionFailed):
        provider_code = _safe_scalar(error.code)
        http_status = error.status_code
        trace_id = _safe_scalar(error.trace_id)
        retryable = http_status == 429 or http_status >= 500
    elif isinstance(error, httpx.HTTPStatusError):
        http_status = error.response.status_code
        provider_code, trace_id = _response_error(error.response)
        retryable = http_status == 429 or http_status >= 500
    elif isinstance(error, NetworkError | httpx.TimeoutException | httpx.NetworkError):
        retryable = True
    else:
        close_code = _safe_scalar(getattr(error, "code", None))
        if close_code and stage in {"websocket_auth", "websocket"}:
            provider_code = close_code

    return QQPublicError(
        code=code,
        message=message,
        provider_code=provider_code,
        http_status=http_status,
        trace_id=trace_id,
        retryable=retryable,
    )


def update_connection_diagnostic(
    app_id: str,
    mode: Literal["websocket", "webhook"],
    state: QQConnectionState,
    *,
    error: QQPublicError | None = None,
) -> QQConnectionDiagnostic:
    diagnostic = QQConnectionDiagnostic(
        app_id=str(app_id),
        mode=mode,
        state=state,
        error=error,
        updated_at=datetime.now(timezone.utc).isoformat(),
    )
    _diagnostics[diagnostic.app_id] = diagnostic
    return diagnostic


def connection_diagnostic(app_id: str) -> QQConnectionDiagnostic | None:
    return _diagnostics.get(str(app_id))


def clear_connection_diagnostics() -> None:
    _diagnostics.clear()


__all__ = [
    "QQConnectionDiagnostic",
    "QQConnectionState",
    "QQPublicError",
    "clear_connection_diagnostics",
    "connection_diagnostic",
    "public_error_from_exception",
    "safe_avatar_url",
    "update_connection_diagnostic",
]
