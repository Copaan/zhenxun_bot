import asyncio
from datetime import timedelta
import json

import aiofiles
from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.security import OAuth2PasswordRequestForm
import nonebot

from zhenxun.configs.config import Config
from zhenxun.services.log import logger

from ..api.configure.persistence import persist_webui_credentials
from ..base_model import Result
from ..passwords import hash_password, is_password_hash, verify_password
from ..security import login_attempt_limiter
from ..utils import (
    ACCESS_TOKEN_EXPIRE_MINUTES,
    create_token,
    get_user,
    token_data,
    token_file,
)

app = nonebot.get_app()


router = APIRouter()


def _client_key(request: Request) -> str:
    return request.client.host if request.client else "unknown"


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
    token_data["token"].append(access_token)
    if len(token_data["token"]) > 3:
        token_data["token"] = token_data["token"][1:]
    async with aiofiles.open(token_file, "w", encoding="utf8") as f:
        await f.write(json.dumps(token_data, ensure_ascii=False, indent=4))
    return Result.ok(
        {"access_token": access_token, "token_type": "bearer"}, "欢迎回家, 欧尼酱!"
    )
