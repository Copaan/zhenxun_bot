from __future__ import annotations

import asyncio
import base64
from collections import OrderedDict
from dataclasses import dataclass, field
import hashlib
from io import StringIO
import secrets
import time
from typing import Any
from urllib.parse import quote

from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from dotenv import dotenv_values
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse
import httpx

from zhenxun.adapters.qq_official.config import (
    QQOfficialBotConfig,
    QQOfficialConfig,
    QQOfficialIntent,
    validate_qq_config_data,
)
from zhenxun.services.log import logger
from zhenxun.utils._restart_utils import issue_restart_ticket
from zhenxun.utils.pydantic_compat import model_dump

from ...base_model import Result
from ...restart_service import restart_status_data
from ...utils import authentication
from ..configure.persistence import _write_transaction
from .configuration import (
    _ENV_FILE,
    _parse_bots,
    _probe_credential,
    _revision,
    _source_path,
    _update_env,
)

router = APIRouter()

_CREATE_URL = "https://q.qq.com/lite/create_bind_task"
_POLL_URL = "https://q.qq.com/lite/poll_bind_result"
_CONNECT_URL = "https://q.qq.com/qqbot/openclaw/connect.html"
_TIMEOUT = httpx.Timeout(10.0, connect=5.0)
_SESSION_TTL_SECONDS = 10 * 60
_POLL_INTERVAL_SECONDS = 2
_MAX_SESSIONS = 8
_MAX_SESSIONS_PER_OWNER = 2


@dataclass(slots=True)
class _RegistrationSession:
    owner: str
    task_id: str
    key: bytes
    expires_at: float
    next_poll_at: float = 0.0
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    completed: dict[str, Any] | None = None


class _RegistrationStore:
    def __init__(self) -> None:
        self._sessions: OrderedDict[str, _RegistrationSession] = OrderedDict()
        self._lock = asyncio.Lock()

    def _purge_expired(self, now: float) -> None:
        expired = [
            registration_id
            for registration_id, session in self._sessions.items()
            if session.expires_at <= now
        ]
        for registration_id in expired:
            self._sessions.pop(registration_id, None)

    async def create(
        self, owner: str, task_id: str, key: bytes
    ) -> tuple[str, _RegistrationSession]:
        async with self._lock:
            now = time.monotonic()
            self._purge_expired(now)
            owner_count = sum(
                session.owner == owner for session in self._sessions.values()
            )
            if owner_count >= _MAX_SESSIONS_PER_OWNER:
                raise _registration_error(
                    429,
                    "registration_limit",
                    "当前账户的扫码会话过多，请先取消旧会话。",
                )
            if len(self._sessions) >= _MAX_SESSIONS:
                raise _registration_error(
                    503, "registration_busy", "扫码接入服务繁忙，请稍后重试。"
                )
            registration_id = secrets.token_urlsafe(24)
            session = _RegistrationSession(
                owner=owner,
                task_id=task_id,
                key=key,
                expires_at=now + _SESSION_TTL_SECONDS,
            )
            self._sessions[registration_id] = session
            return registration_id, session

    async def get(self, registration_id: str, owner: str) -> _RegistrationSession:
        async with self._lock:
            now = time.monotonic()
            session = self._sessions.get(registration_id)
            if session is None or session.expires_at <= now:
                self._sessions.pop(registration_id, None)
                raise _registration_error(
                    410, "registration_expired", "二维码已过期，请重新生成。"
                )
            if not secrets.compare_digest(session.owner, owner):
                raise _registration_error(
                    404, "registration_not_found", "扫码会话不存在。"
                )
            self._sessions.move_to_end(registration_id)
            return session

    async def delete(self, registration_id: str, owner: str) -> None:
        async with self._lock:
            session = self._sessions.get(registration_id)
            if session is not None and secrets.compare_digest(session.owner, owner):
                self._sessions.pop(registration_id, None)


_sessions = _RegistrationStore()
_configuration_write_lock = asyncio.Lock()


def _registration_error(status: int, code: str, message: str) -> HTTPException:
    return HTTPException(status_code=status, detail={"code": code, "message": message})


def _owner(request: Request) -> str:
    authorization = request.headers.get("authorization", "")
    client_host = request.client.host if request.client else "unknown"
    material = f"{client_host}\0{authorization}".encode("utf-8", "replace")
    return hashlib.sha256(material).hexdigest()


def _response_data(response: httpx.Response) -> dict[str, Any]:
    response.raise_for_status()
    data = response.json()
    if not isinstance(data, dict):
        raise ValueError("invalid_response")
    retcode = data.get("retcode")
    if retcode is not None:
        try:
            success = int(retcode) == 0
        except (TypeError, ValueError):
            success = False
        if not success:
            raise ValueError("provider_rejected")
    return data


def _payload(data: dict[str, Any]) -> dict[str, Any]:
    value = data.get("data")
    return value if isinstance(value, dict) else {}


def _decrypt_secret(encrypted_secret: str, key: bytes) -> str:
    try:
        raw = base64.b64decode(encrypted_secret, validate=True)
        if len(key) != 32 or len(raw) <= 28:
            raise ValueError("invalid_encrypted_secret")
        secret = AESGCM(key).decrypt(raw[:12], raw[12:], None).decode("utf-8")
    except Exception as exc:
        raise ValueError("invalid_encrypted_secret") from exc
    secret = secret.strip()
    if not secret:
        raise ValueError("invalid_encrypted_secret")
    return secret


async def _create_bind_task(key: bytes) -> str:
    encoded_key = base64.b64encode(key).decode("ascii")
    async with httpx.AsyncClient(timeout=_TIMEOUT) as client:
        response = await client.post(
            _CREATE_URL,
            json={"key": encoded_key},
            headers={"Accept": "application/json"},
        )
    task_id = str(_payload(_response_data(response)).get("task_id") or "").strip()
    if not task_id:
        raise ValueError("missing_task_id")
    return task_id


async def _poll_bind_task(task_id: str) -> dict[str, Any]:
    async with httpx.AsyncClient(timeout=_TIMEOUT) as client:
        response = await client.post(
            _POLL_URL,
            json={"task_id": task_id},
            headers={"Accept": "application/json"},
        )
    return _payload(_response_data(response))


async def _save_websocket_bot(app_id: str, secret: str) -> dict[str, Any]:
    async with _configuration_write_lock:
        source = _source_path()
        current = source.read_text(encoding="utf-8")
        values = dotenv_values(stream=StringIO(current))
        raw_bots = _parse_bots(values.get("QQ_BOTS"))
        merged: list[dict[str, Any]] = []
        replaced = False
        for item in raw_bots:
            if not isinstance(item, dict):
                continue
            if str(item.get("id") or "").strip() == app_id:
                item = {
                    "id": app_id,
                    "token": str(item.get("token") or "").strip(),
                    "secret": secret,
                    "use_websocket": True,
                    "intent": model_dump(QQOfficialIntent(c2c_group_at_messages=True)),
                }
                replaced = True
            merged.append(item)
        if not replaced:
            merged.append(
                {
                    "id": app_id,
                    "token": "",
                    "secret": secret,
                    "use_websocket": True,
                    "intent": model_dump(QQOfficialIntent(c2c_group_at_messages=True)),
                }
            )
        bots = [QQOfficialBotConfig(**item) for item in merged]
        validate_qq_config_data(QQOfficialConfig(qq_bots=bots))
        updated = _update_env(
            current,
            {
                "QQ_ADAPTER_LOAD": True,
                "QQ_BOTS": [model_dump(bot) for bot in bots],
            },
        )
        _write_transaction([(_ENV_FILE, updated.encode("utf-8"))])
        issue_restart_ticket("webui.settings", ttl_seconds=10 * 60)
        return {
            "revision": _revision(updated),
            "updated_existing": replaced,
        }


@router.post(
    "/qq/registration/start",
    dependencies=[authentication()],
    response_model=Result,
    response_class=JSONResponse,
)
async def start_qq_registration(request: Request) -> Result:
    key = secrets.token_bytes(32)
    owner = _owner(request)
    registration_id = ""
    try:
        registration_id, session = await _sessions.create(owner, "", key)
        task_id = await _create_bind_task(key)
        session.task_id = task_id
    except HTTPException:
        raise
    except Exception as exc:
        if registration_id:
            await _sessions.delete(registration_id, owner)
        logger.warning(
            f"QQ Bot扫码绑定任务创建失败 result={exc.__class__.__name__}",
            "QQOfficialRegistration",
        )
        raise _registration_error(
            502, "registration_start_failed", "无法连接 QQ 扫码服务，请稍后重试。"
        ) from exc
    qr_url = f"{_CONNECT_URL}?task_id={quote(task_id, safe='')}&_wv=2"
    logger.info("QQ Bot扫码绑定任务已创建", "QQOfficialRegistration")
    return Result.ok(
        {
            "registration_id": registration_id,
            "qr_url": qr_url,
            "interval": _POLL_INTERVAL_SECONDS,
            "expires_in": _SESSION_TTL_SECONDS,
        }
    )


@router.post(
    "/qq/registration/{registration_id}/poll",
    dependencies=[authentication()],
    response_model=Result,
    response_class=JSONResponse,
)
async def poll_qq_registration(registration_id: str, request: Request) -> Result:
    session = await _sessions.get(registration_id, _owner(request))
    async with session.lock:
        if session.completed is not None:
            return Result.ok(session.completed)
        now = time.monotonic()
        if now < session.next_poll_at:
            return Result.ok(
                {
                    "status": "pending",
                    "retry_after": max(1, round(session.next_poll_at - now)),
                }
            )
        session.next_poll_at = now + _POLL_INTERVAL_SECONDS
        try:
            payload = await _poll_bind_task(session.task_id)
            status = int(payload.get("status", 0))
        except Exception as exc:
            logger.warning(
                f"QQ Bot扫码状态查询失败 result={exc.__class__.__name__}",
                "QQOfficialRegistration",
            )
            raise _registration_error(
                502, "registration_poll_failed", "扫码状态查询失败，请稍后重试。"
            ) from exc

        if status == 3:
            session.expires_at = 0
            return Result.ok({"status": "expired"})
        if status != 2:
            return Result.ok({"status": "pending", "retry_after": 2})

        app_id = str(payload.get("bot_appid") or "").strip()
        encrypted_secret = str(payload.get("bot_encrypt_secret") or "").strip()
        if not app_id or not encrypted_secret:
            raise _registration_error(
                502, "registration_incomplete", "QQ 未返回完整的机器人凭据。"
            )
        try:
            secret = _decrypt_secret(encrypted_secret, session.key)
            identity = await _probe_credential(app_id, secret)
            saved = await _save_websocket_bot(app_id, secret)
        except HTTPException as exc:
            logger.warning(
                "QQ Bot扫码凭据验证失败 result=credential_invalid",
                "QQOfficialRegistration",
            )
            raise _registration_error(
                422, "credential_invalid", "扫码已完成，但机器人凭据验证失败。"
            ) from exc
        except Exception as exc:
            logger.warning(
                f"QQ Bot扫码配置保存失败 result={exc.__class__.__name__}",
                "QQOfficialRegistration",
            )
            raise _registration_error(
                500, "registration_save_failed", "机器人配置保存失败，请重试。"
            ) from exc

        restart = restart_status_data()
        result = {
            "status": "completed",
            "bot": {
                "app_id": app_id,
                "bot_id": identity.get("bot_id", ""),
                "username": identity.get("username", ""),
            },
            "revision": saved["revision"],
            "updated_existing": saved["updated_existing"],
            "restart_required": True,
            "restart_available": restart["launcher_managed"],
            "access_urls": restart["access_urls"],
        }
        session.completed = result
        session.key = b""
        session.task_id = ""
        logger.info("QQ Bot扫码绑定完成并已保存", "QQOfficialRegistration")
        return Result.ok(result)


@router.delete(
    "/qq/registration/{registration_id}",
    dependencies=[authentication()],
    response_model=Result,
    response_class=JSONResponse,
)
async def cancel_qq_registration(registration_id: str, request: Request) -> Result:
    await _sessions.delete(registration_id, _owner(request))
    return Result.ok(info="扫码会话已取消。")


__all__ = ["router"]
