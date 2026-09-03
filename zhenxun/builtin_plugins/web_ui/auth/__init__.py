import asyncio
from datetime import timedelta
import json
import secrets
import time

import aiofiles
from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.security import OAuth2PasswordRequestForm
import nonebot
from pydantic import BaseModel, Field

from zhenxun.configs.config import Config
from zhenxun.services.log import logger

from ..api.configure.persistence import persist_webui_credentials
from ..api.configure.setup_access import setup_access
from ..base_model import Result
from ..console_access import console_access
from ..passwords import (
    hash_password,
    is_password_hash,
    validate_new_password,
    verify_password,
)
from ..security import (
    decode_access_token_status,
    login_attempt_limiter,
    renew_authenticated_websockets,
    revoke_authenticated_websockets,
)
from ..utils import (
    ACCESS_TOKEN_ABSOLUTE_HOURS,
    ACCESS_TOKEN_EXPIRE_MINUTES,
    authentication,
    create_token,
    get_user,
    oauth2_scheme,
    token_data,
    token_file,
)

app = nonebot.get_app()


router = APIRouter()
_TOKEN_WRITE_LOCK = asyncio.Lock()


class ConsoleConnectRequest(BaseModel):
    code: str = Field(min_length=32, max_length=128)


class PasswordResetRequest(BaseModel):
    password: str = Field(min_length=8, max_length=1024)
    confirm_password: str = Field(min_length=8, max_length=1024)


class SessionActivityResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    expires_in: int
    absolute_expires_in: int


def _client_key(request: Request) -> str:
    return request.client.host if request.client else "unknown"


async def _remember_token(access_token: str) -> None:
    async with _TOKEN_WRITE_LOCK:
        token_data["token"].append(access_token)
        if len(token_data["token"]) > 3:
            token_data["token"] = token_data["token"][-3:]
        async with aiofiles.open(token_file, "w", encoding="utf8") as stream:
            await stream.write(json.dumps(token_data, ensure_ascii=False, indent=4))


@router.post("/login")
async def login_get_token(
    request: Request, form_data: OAuth2PasswordRequestForm = Depends()
):
    client_key = _client_key(request)
    if not await login_attempt_limiter.reserve(client_key):
        raise HTTPException(status_code=429, detail="登录尝试过于频繁，请稍后再试")
    username = Config.get_config("web-ui", "username")
    password = Config.get_config("web-ui", "password")
    if not username or not password:
        return Result.fail("你滴配置文件里用户名密码配置项为空", 998)
    password_value = str(password or "")
    password_matches = username == form_data.username and await asyncio.to_thread(
        verify_password, form_data.password, password_value
    )
    if not password_matches:
        return Result.fail("真笨, 账号密码都能记错!", 999)
    if not is_password_hash(password_value):
        upgraded = await asyncio.to_thread(hash_password, form_data.password)
        try:
            await asyncio.to_thread(persist_webui_credentials, str(username), upgraded)
        except Exception as error:
            logger.warning(
                f"WebUI 管理密码哈希迁移失败（{error.__class__.__name__}）",
                "WebUi",
            )
    user = get_user(form_data.username)
    if not user:
        return Result.fail("用户不存在...", 997)
    await login_attempt_limiter.clear(client_key)
    access_token = create_token(
        user=user,
        expires_delta=timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES),
    )
    await _remember_token(access_token)
    return Result.ok(
        {"access_token": access_token, "token_type": "bearer"}, "欢迎回家, 欧尼酱!"
    )


@router.post("/auth/console-connect", response_model=Result)
async def console_connect(request: Request, payload: ConsoleConnectRequest) -> Result:
    client_key = _client_key(request)
    boot_id = await console_access.claim(payload.code, client_key)
    state = setup_access.state()
    if state in {"unconfigured", "partial"}:
        setup_token, expires_in = await setup_access.create_session(client_key)
        return Result.ok(
            {
                "mode": "setup",
                "setup_token": setup_token,
                "expires_in": expires_in,
            },
            "控制台连接已授权，请继续完成首次配置。",
        )
    if state == "restart_pending":
        raise HTTPException(status_code=409, detail="配置正在等待重启。")

    username = str(Config.get_config("web-ui", "username", ""))
    user = get_user(username)
    if user is None:
        raise HTTPException(status_code=409, detail="WebUI 管理账户尚未完成配置。")
    access_token = create_token(
        user=user,
        expires_delta=timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES),
        extra_claims={"auth_source": "console", "boot_id": boot_id},
    )
    await _remember_token(access_token)
    return Result.ok(
        {
            "mode": "login",
            "access_token": access_token,
            "token_type": "bearer",
            "expires_in": ACCESS_TOKEN_EXPIRE_MINUTES * 60,
        },
        "已通过本次启动的控制台链接临时登录。",
    )


@router.post("/auth/activity", response_model=Result[SessionActivityResponse])
async def renew_session_activity(token: str = Depends(oauth2_scheme)) -> Result:
    claims, status = decode_access_token_status(token)
    if claims is None:
        raise HTTPException(
            status_code=401,
            detail=("登录会话已过期。" if status == "expired" else "登录验证失败。"),
            headers={"WWW-Authenticate": "Bearer"},
        )
    username = str(claims.get("sub") or "")
    user = get_user(username)
    if user is None:
        raise HTTPException(status_code=401, detail="登录账户已失效。")
    now = int(time.time())
    absolute_exp = int(
        claims.get("absolute_exp")
        or claims.get("auth_time", now) + ACCESS_TOKEN_ABSOLUTE_HOURS * 3600
    )
    if now >= absolute_exp:
        raise HTTPException(status_code=401, detail="登录会话已达到最长有效期。")
    retained = {
        key: claims[key]
        for key in ("sid", "auth_time", "absolute_exp", "auth_source", "boot_id")
        if key in claims
    }
    access_token = create_token(
        user=user,
        expires_delta=timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES),
        extra_claims=retained,
    )
    refreshed, _ = decode_access_token_status(access_token)
    expires_at = int((refreshed or {}).get("exp", now))
    renew_authenticated_websockets(str((refreshed or {}).get("sid") or ""), expires_at)
    await _remember_token(access_token)
    return Result.ok(
        {
            "access_token": access_token,
            "token_type": "bearer",
            "expires_in": max(0, expires_at - now),
            "absolute_expires_in": max(0, absolute_exp - now),
        }
    )


@router.post(
    "/auth/password",
    response_model=Result,
    dependencies=[authentication()],
)
async def reset_password(payload: PasswordResetRequest) -> Result:
    if payload.password != payload.confirm_password:
        raise HTTPException(status_code=422, detail="两次输入的密码不一致。")
    if password_error := validate_new_password(payload.password):
        raise HTTPException(status_code=422, detail=password_error)
    username = str(Config.get_config("web-ui", "username", ""))
    if not username:
        raise HTTPException(status_code=409, detail="WebUI 管理账户尚未完成配置。")

    password_hash = await asyncio.to_thread(hash_password, payload.password)
    new_secret = secrets.token_urlsafe(32)
    try:
        await asyncio.to_thread(
            persist_webui_credentials,
            username,
            password_hash,
            new_secret,
        )
    except Exception as error:
        logger.error(
            f"WebUI 管理密码重置失败（{error.__class__.__name__}）",
            "WebUi",
        )
        raise HTTPException(status_code=500, detail="管理密码重置失败。") from error

    token_data["token"].clear()
    try:
        async with aiofiles.open(token_file, "w", encoding="utf8") as stream:
            await stream.write(json.dumps(token_data, ensure_ascii=False, indent=4))
    except OSError as error:
        logger.warning(
            f"WebUI 旧登录记录清理失败（{error.__class__.__name__}）",
            "WebUi",
        )
    await revoke_authenticated_websockets()
    return Result.ok(info="管理密码已更新，请使用新密码重新登录。")
