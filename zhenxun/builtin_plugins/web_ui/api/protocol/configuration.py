from __future__ import annotations

import hashlib
from io import StringIO
import json
import os
from pathlib import Path
import time
from typing import Annotated, Any, Literal
from urllib.parse import urlsplit, urlunsplit

from dotenv import dotenv_values
from dotenv.parser import parse_stream
from fastapi import APIRouter, HTTPException, Query, Response
from fastapi.responses import JSONResponse
import httpx
from pydantic import BaseModel, Field

from zhenxun.adapters.qq_official.config import (
    QQLauncherSettings,
    QQOfficialBotConfig,
    QQOfficialConfig,
    QQOfficialConfigError,
    QQOfficialIntent,
    validate_builtin_ingress,
    validate_qq_config_data,
)
from zhenxun.adapters.qq_official.diagnostics import (
    public_error_from_exception,
    safe_avatar_url,
)
from zhenxun.services.log import logger
from zhenxun.utils._restart_utils import issue_restart_ticket
from zhenxun.utils.pydantic_compat import model_dump

from ...base_model import Result
from ...config_validation import (
    ConfigurationValidationError,
    validate_dotenv,
    validation_detail,
)
from ...restart_service import restart_status_data
from ...utils import authentication
from ..configure.persistence import _write_transaction

router = APIRouter()

_ENV_FILE = Path(".env.dev")
_ENV_TEMPLATE = Path(".env.example")
_AUTH_URL = "https://bots.qq.com/app/getAppAccessToken"
_ME_URL = "https://api.sgroup.qq.com/users/@me"
_TIMEOUT = httpx.Timeout(10.0, connect=5.0)


class QQCredentialProbe(BaseModel):
    id: str = Field(min_length=1, max_length=128)
    token: str | None = Field(default=None, max_length=512)
    secret: str | None = Field(default=None, max_length=512)


class QQBotForm(BaseModel):
    id: str = Field(min_length=1, max_length=128)
    token: str | None = Field(default=None, max_length=512)
    secret: str | None = Field(default=None, max_length=512)
    use_websocket: bool = False


class ProtocolConfigurationUpdate(BaseModel):
    expected_revision: str = Field(min_length=64, max_length=64)
    onebot_access_token: str | None = Field(default=None, max_length=1024)
    clear_onebot_access_token: bool = False
    qq_enabled: bool
    qq_bots: list[QQBotForm] = Field(default_factory=list)
    qq_webhook_mode: Literal["external", "builtin_https"] = "external"
    qq_webhook_public_base_url: str = ""
    qq_webhook_listen_host: str = "0.0.0.0"
    qq_webhook_listen_port: int = Field(default=443, ge=1, le=65535)
    qq_webhook_tls_certfile: str = ""
    qq_webhook_tls_keyfile: str = ""


def _source_path() -> Path:
    if _ENV_FILE.exists():
        return _ENV_FILE
    if _ENV_TEMPLATE.exists():
        return _ENV_TEMPLATE
    raise HTTPException(status_code=500, detail="环境配置文件不存在。")


def _revision(content: str) -> str:
    return hashlib.sha256(content.encode("utf-8")).hexdigest()


def _validate_env(content: str) -> None:
    try:
        validate_dotenv(content)
    except ConfigurationValidationError as exc:
        raise HTTPException(
            status_code=422,
            detail=validation_detail(exc, message="dotenv 配置校验失败。"),
        ) from exc


def _env_value(value: Any) -> str:
    if isinstance(value, bool):
        return "True" if value else "False"
    if isinstance(value, int):
        return str(value)
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def _update_env(content: str, values: dict[str, Any]) -> str:
    _validate_env(content)
    remaining = {key.upper(): value for key, value in values.items()}
    output: list[str] = []
    for binding in parse_stream(StringIO(content)):
        key = binding.key.upper() if binding.key else None
        if key in remaining:
            output.append(f"{key}={_env_value(remaining.pop(key))}\n")
        else:
            output.append(binding.original.string)
    if remaining:
        if output and not output[-1].endswith("\n"):
            output.append("\n")
        output.extend(
            f"{key}={_env_value(value)}\n" for key, value in remaining.items()
        )
    result = "".join(output)
    _validate_env(result)
    return result


def _parse_bots(raw: object) -> list[dict[str, Any]]:
    if not raw:
        return []
    try:
        value = json.loads(str(raw))
    except (TypeError, ValueError):
        return []
    return value if isinstance(value, list) else []


def _public_base_url(value: str, *, required: bool) -> str:
    value = value.strip().rstrip("/")
    if not value and not required:
        return ""
    parsed = urlsplit(value)
    if (
        parsed.scheme != "https"
        or not parsed.hostname
        or parsed.query
        or parsed.fragment
        or parsed.path not in {"", "/"}
        or parsed.username
        or parsed.password
    ):
        raise HTTPException(
            status_code=422,
            detail="公网基础地址必须是无路径、查询参数和账号信息的HTTPS地址。",
        )
    return urlunsplit(("https", parsed.netloc, "", "", ""))


def _callback_url(base_url: str) -> str | None:
    return f"{base_url.rstrip('/')}/qq/webhook" if base_url else None


def _masked_configuration(content: str) -> dict[str, Any]:
    values = dotenv_values(stream=StringIO(content))
    raw_bots = _parse_bots(values.get("QQ_BOTS"))
    bots = []
    for item in raw_bots:
        if not isinstance(item, dict):
            continue
        bots.append(
            {
                "id": str(item.get("id") or ""),
                "has_token": bool(str(item.get("token") or "").strip()),
                "has_secret": bool(str(item.get("secret") or "").strip()),
            }
        )
    bot_modes = {
        str(item.get("id") or ""): bool(item.get("use_websocket", False))
        for item in raw_bots
        if isinstance(item, dict) and str(item.get("id") or "")
    }
    base_url = str(values.get("QQ_WEBHOOK_PUBLIC_BASE_URL") or "").strip()
    return {
        "revision": _revision(content),
        "launcher_managed": bool(os.getenv("ZHENXUN_LAUNCHER_PID")),
        "onebot": {
            "has_access_token": bool(
                str(values.get("ONEBOT_ACCESS_TOKEN") or "").strip()
            )
        },
        "qq": {
            "enabled": str(values.get("QQ_ADAPTER_LOAD") or "").lower()
            in {"true", "1", "yes", "on"},
            "bots": bots,
            "bot_modes": bot_modes,
            "webhook_mode": str(values.get("QQ_WEBHOOK_MODE") or "external"),
            "public_base_url": base_url,
            "callback_url": _callback_url(base_url),
            "listen_host": str(values.get("QQ_WEBHOOK_LISTEN_HOST") or "0.0.0.0"),
            "listen_port": int(values.get("QQ_WEBHOOK_LISTEN_PORT") or 443),
            "has_tls_certfile": bool(
                str(values.get("QQ_WEBHOOK_TLS_CERTFILE") or "").strip()
            ),
            "has_tls_keyfile": bool(
                str(values.get("QQ_WEBHOOK_TLS_KEYFILE") or "").strip()
            ),
        },
    }


def _resolve_bots(
    forms: list[QQBotForm], existing: list[dict[str, Any]]
) -> list[QQOfficialBotConfig]:
    existing_by_id = {
        str(item.get("id") or ""): item for item in existing if isinstance(item, dict)
    }
    result: list[QQOfficialBotConfig] = []
    for index, form in enumerate(forms):
        app_id = form.id.strip()
        old = existing_by_id.get(app_id, {})
        token = (form.token or "").strip() or str(old.get("token") or "").strip()
        secret = (form.secret or "").strip() or str(old.get("secret") or "").strip()
        if not secret:
            raise HTTPException(
                status_code=422,
                detail=f"QQ Bot第{index + 1}项缺少Secret。",
            )
        result.append(
            QQOfficialBotConfig(
                id=app_id,
                token=token,
                secret=secret,
                use_websocket=form.use_websocket,
                intent=QQOfficialIntent(c2c_group_at_messages=True),
            )
        )
    return result


async def _probe_credential(app_id: str, secret: str) -> dict[str, str]:
    started_at = time.perf_counter()
    try:
        async with httpx.AsyncClient(timeout=_TIMEOUT, follow_redirects=True) as client:
            auth = await client.post(
                _AUTH_URL,
                json={"appId": app_id, "clientSecret": secret},
            )
            auth.raise_for_status()
            auth_data = auth.json()
            access_token = str(auth_data.get("access_token") or "")
            if not access_token:
                raise httpx.HTTPStatusError(
                    "QQ authorization response did not include an access token",
                    request=auth.request,
                    response=auth,
                )
            me = await client.get(
                _ME_URL,
                headers={
                    "Authorization": f"QQBot {access_token}",
                    "X-Union-Appid": app_id,
                },
            )
            me.raise_for_status()
            data = me.json()
    except Exception as exc:
        public_error = public_error_from_exception(exc, stage="credential")
        logger.warning(
            "QQ Bot凭据测试失败 "
            f"code={public_error.code} "
            f"provider_code={public_error.provider_code or '-'} "
            f"http_status={public_error.http_status or '-'} "
            f"trace_id={public_error.trace_id or '-'} "
            f"latency_ms={round((time.perf_counter() - started_at) * 1000)}",
            "QQOfficialProbe",
        )
        raise HTTPException(
            status_code=422,
            detail=public_error.to_dict(),
        ) from exc
    logger.info(
        "QQ Bot凭据测试成功 result=ready "
        f"latency_ms={round((time.perf_counter() - started_at) * 1000)}",
        "QQOfficialProbe",
    )
    return {
        "app_id": app_id,
        "bot_id": str(data.get("id") or ""),
        "username": str(data.get("username") or ""),
        "avatar_url": safe_avatar_url(data.get("avatar")) or "",
    }


@router.get(
    "/configuration",
    dependencies=[authentication()],
    response_model=Result,
    response_class=JSONResponse,
)
async def protocol_configuration(response: Response) -> Result:
    content = _source_path().read_text(encoding="utf-8")
    _validate_env(content)
    response.headers["Cache-Control"] = "no-store"
    return Result.ok(_masked_configuration(content))


@router.post(
    "/qq/probe",
    dependencies=[authentication()],
    response_model=Result,
    response_class=JSONResponse,
)
async def probe_qq_credential(payload: QQCredentialProbe) -> Result:
    content = _source_path().read_text(encoding="utf-8")
    values = dotenv_values(stream=StringIO(content))
    existing = {
        str(item.get("id") or ""): item
        for item in _parse_bots(values.get("QQ_BOTS"))
        if isinstance(item, dict)
    }
    app_id = payload.id.strip()
    saved = existing.get(app_id, {})
    secret = (payload.secret or "").strip() or str(saved.get("secret") or "").strip()
    if not secret:
        raise HTTPException(status_code=422, detail="QQ Bot缺少Secret。")
    result = await _probe_credential(app_id, secret)
    return Result.ok(result, info="QQ Bot凭据验证成功。")


@router.put(
    "/configuration",
    dependencies=[authentication()],
    response_model=Result,
    response_class=JSONResponse,
)
async def save_protocol_configuration(
    payload: ProtocolConfigurationUpdate,
) -> Result:
    source = _source_path()
    current = source.read_text(encoding="utf-8")
    if _revision(current) != payload.expected_revision:
        raise HTTPException(status_code=409, detail="配置已被外部修改，请重新加载。")
    values = dotenv_values(stream=StringIO(current))
    existing_bots = _parse_bots(values.get("QQ_BOTS"))
    changed: dict[str, Any] = {
        "QQ_ADAPTER_LOAD": payload.qq_enabled,
        "QQ_WEBHOOK_MODE": payload.qq_webhook_mode,
        "QQ_WEBHOOK_LISTEN_HOST": payload.qq_webhook_listen_host.strip(),
        "QQ_WEBHOOK_LISTEN_PORT": payload.qq_webhook_listen_port,
    }
    if payload.clear_onebot_access_token:
        changed["ONEBOT_ACCESS_TOKEN"] = ""
    elif payload.onebot_access_token is not None and payload.onebot_access_token:
        changed["ONEBOT_ACCESS_TOKEN"] = payload.onebot_access_token

    bots: list[QQOfficialBotConfig] = []
    base_url = ""
    if payload.qq_enabled:
        bots = _resolve_bots(payload.qq_bots, existing_bots)
        has_webhook_bots = any(not bot.use_websocket for bot in bots)
        base_url = _public_base_url(
            payload.qq_webhook_public_base_url,
            required=has_webhook_bots,
        )
        config = QQOfficialConfig(
            qq_bots=bots,
            qq_verify_webhook=True,
            qq_webhook_mode=payload.qq_webhook_mode,
            qq_webhook_listen_host=payload.qq_webhook_listen_host.strip(),
            qq_webhook_listen_port=payload.qq_webhook_listen_port,
            qq_webhook_tls_certfile=(
                payload.qq_webhook_tls_certfile.strip()
                or str(values.get("QQ_WEBHOOK_TLS_CERTFILE") or "")
            ),
            qq_webhook_tls_keyfile=(
                payload.qq_webhook_tls_keyfile.strip()
                or str(values.get("QQ_WEBHOOK_TLS_KEYFILE") or "")
            ),
            qq_webhook_public_base_url=base_url,
        )
        try:
            validate_qq_config_data(config)
            validate_builtin_ingress(
                QQLauncherSettings(
                    enabled=True,
                    config=config,
                    worker_host=str(values.get("HOST") or "127.0.0.1"),
                    worker_port=int(values.get("PORT") or 8080),
                ),
                check_port=False,
            )
        except QQOfficialConfigError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        for bot in bots:
            await _probe_credential(bot.id, bot.secret)
        changed.update(
            {
                "QQ_BOTS": [model_dump(bot) for bot in bots],
                "QQ_VERIFY_WEBHOOK": True,
                "QQ_WEBHOOK_TLS_CERTFILE": config.qq_webhook_tls_certfile,
                "QQ_WEBHOOK_TLS_KEYFILE": config.qq_webhook_tls_keyfile,
            }
        )
    else:
        base_url = str(values.get("QQ_WEBHOOK_PUBLIC_BASE_URL") or "").strip()
    changed["QQ_WEBHOOK_PUBLIC_BASE_URL"] = base_url

    updated = _update_env(current, changed)
    _write_transaction([(_ENV_FILE, updated.encode("utf-8"))])
    launcher_managed = bool(os.getenv("ZHENXUN_LAUNCHER_PID"))
    if launcher_managed:
        issue_restart_ticket("webui.settings", ttl_seconds=10 * 60)
    return Result.ok(
        {
            "revision": _revision(updated),
            "changed_keys": sorted(changed),
            "callback_url": _callback_url(base_url),
            "restart_required": True,
            "restart_available": launcher_managed,
        },
        info="协议配置已保存。",
    )


@router.delete(
    "/qq/bots/{app_id}",
    dependencies=[authentication()],
    response_model=Result,
    response_class=JSONResponse,
)
async def delete_qq_bot(
    app_id: str,
    expected_revision: Annotated[
        str,
        Query(min_length=64, max_length=64),
    ],
) -> Result:
    source = _source_path()
    current = source.read_text(encoding="utf-8")
    if _revision(current) != expected_revision:
        raise HTTPException(
            status_code=409,
            detail={
                "code": "configuration_revision_conflict",
                "message": "配置已被外部修改，请重新加载后再删除。",
            },
        )

    values = dotenv_values(stream=StringIO(current))
    bots = [
        item for item in _parse_bots(values.get("QQ_BOTS")) if isinstance(item, dict)
    ]
    normalized_app_id = app_id.strip()
    remaining = [
        item for item in bots if str(item.get("id") or "").strip() != normalized_app_id
    ]
    if len(remaining) == len(bots):
        raise HTTPException(
            status_code=404,
            detail={
                "code": "qq_bot_not_found",
                "message": "未找到要移除的QQ官方机器人。",
            },
        )

    was_enabled = str(values.get("QQ_ADAPTER_LOAD") or "").strip().lower() in {
        "true",
        "1",
        "yes",
        "on",
    }
    updated = _update_env(
        current,
        {
            "QQ_ADAPTER_LOAD": was_enabled and bool(remaining),
            "QQ_BOTS": remaining,
        },
    )
    try:
        _write_transaction([(_ENV_FILE, updated.encode("utf-8"))])
    except Exception as exc:
        logger.error(
            "QQ Bot本地配置移除失败 code=qq_bot_delete_failed",
            "QQOfficialConfiguration",
        )
        raise HTTPException(
            status_code=500,
            detail={
                "code": "qq_bot_delete_failed",
                "message": "QQ机器人本地配置移除失败，请重试。",
            },
        ) from exc

    launcher_managed = bool(os.getenv("ZHENXUN_LAUNCHER_PID"))
    if launcher_managed:
        issue_restart_ticket("webui.settings", ttl_seconds=10 * 60)
    restart = restart_status_data()
    logger.info(
        f"QQ Bot已从本地配置移除 remaining={len(remaining)}",
        "QQOfficialConfiguration",
    )
    return Result.ok(
        {
            "revision": _revision(updated),
            "remaining": len(remaining),
            "qq_enabled": was_enabled and bool(remaining),
            "restart_required": True,
            "restart_available": launcher_managed,
            "access_urls": restart["access_urls"],
            "access_targets": restart["access_targets"],
        },
        info="机器人已从真寻本地配置移除。",
    )


__all__ = ["router"]
