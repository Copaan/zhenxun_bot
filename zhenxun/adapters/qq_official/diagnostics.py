from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import re
from typing import Any, Literal
from urllib.parse import urlsplit

import httpx

try:
    from nonebot.adapters.qq.exception import ActionFailed, NetworkError
except ModuleNotFoundError:
    # The optional official adapter is not installed in every test/runtime env.
    class NetworkError(Exception):
        pass

    class ActionFailed(Exception):
        pass


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
    provider_explanation: str | None = None
    suggestion: str | None = None
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


@dataclass(frozen=True, slots=True)
class QQPublicIdentity:
    app_id: str
    bot_id: str | None
    username: str | None
    avatar_url: str | None
    updated_at: str


_diagnostics: dict[str, QQConnectionDiagnostic] = {}
_identities: dict[str, QQPublicIdentity] = {}

_PROVIDER_ERROR_GUIDANCE: dict[str, tuple[str, str]] = {
    "4004": (
        "QQ WebSocket 鉴权失败。",
        "请重新验证 AppID 和 AppSecret，确认机器人未被停用。",
    ),
    "4010": (
        "QQ WebSocket 分片参数无效。",
        "请更新真寻和 QQ 适配器后重试。",
    ),
    "4011": (
        "QQ 要求使用分片连接。",
        "请稍后重试；若持续出现，请保留 Trace ID 反馈。",
    ),
    "4012": (
        "QQ WebSocket API 版本不受支持。",
        "请更新真寻和 QQ 适配器。",
    ),
    "4013": (
        "QQ WebSocket Intent 配置无效。",
        "请检查机器人事件订阅权限后重新连接。",
    ),
    "4014": (
        "QQ 拒绝了当前未授权的事件订阅。",
        "请在 QQ 开放平台为机器人开通所需事件权限。",
    ),
}


def _provider_guidance(
    provider_code: str | None, http_status: int | None
) -> tuple[str | None, str]:
    if provider_code and provider_code in _PROVIDER_ERROR_GUIDANCE:
        return _PROVIDER_ERROR_GUIDANCE[provider_code]
    if http_status == 401:
        return (
            "QQ 拒绝了当前机器人凭据。",
            "请核对 AppID 和 AppSecret，重新验证后保存配置。",
        )
    if http_status == 403:
        return (
            "当前机器人没有访问该接口的权限。",
            "请检查 QQ 开放平台中的机器人状态和接口权限。",
        )
    if http_status == 429:
        return (
            "QQ 接口触发了频率或额度限制。",
            "请等待额度恢复后重试，避免短时间重复连接。",
        )
    if http_status is not None and http_status >= 500:
        return (
            "QQ 服务当前不可用。",
            "请稍后重试；持续失败时可携带 Trace ID 排查。",
        )
    if provider_code:
        return (
            "该 QQ 错误码当前没有更具体的公开说明。",
            "请核对机器人状态，并携带错误码和 Trace ID 查询 QQ 官方文档。",
        )
    return None, "请检查网络和机器人配置后重试。"


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
        retryable = http_status == 429 or bool(http_status and http_status >= 500)
    elif isinstance(error, httpx.HTTPStatusError):
        http_status = error.response.status_code
        provider_code, trace_id = _response_error(error.response)
        retryable = http_status == 429 or bool(http_status and http_status >= 500)
    elif isinstance(error, NetworkError | httpx.TimeoutException | httpx.NetworkError):
        retryable = True
    else:
        close_code = _safe_scalar(getattr(error, "code", None))
        if close_code and stage in {"websocket_auth", "websocket"}:
            provider_code = close_code

    provider_explanation, suggestion = _provider_guidance(provider_code, http_status)

    return QQPublicError(
        code=code,
        message=message,
        provider_code=provider_code,
        provider_explanation=provider_explanation,
        suggestion=suggestion,
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


def update_public_identity(
    app_id: str,
    *,
    bot_id: object = None,
    username: object = None,
    avatar_url: object = None,
) -> QQPublicIdentity:
    identity = QQPublicIdentity(
        app_id=str(app_id),
        bot_id=_safe_scalar(bot_id),
        username=str(username).strip()[:256] if username else None,
        avatar_url=safe_avatar_url(avatar_url),
        updated_at=datetime.now(timezone.utc).isoformat(),
    )
    _identities[identity.app_id] = identity
    return identity


def public_identity(app_id: str) -> QQPublicIdentity | None:
    return _identities.get(str(app_id))


def clear_connection_diagnostics() -> None:
    _diagnostics.clear()
    _identities.clear()


__all__ = [
    "QQConnectionDiagnostic",
    "QQConnectionState",
    "QQPublicError",
    "QQPublicIdentity",
    "clear_connection_diagnostics",
    "connection_diagnostic",
    "public_error_from_exception",
    "public_identity",
    "safe_avatar_url",
    "update_connection_diagnostic",
    "update_public_identity",
]
