"""内置 AI 聊天的二次元角色识别：档位、来源设置、本地模型安装与识别测试"""

from __future__ import annotations

import time
from typing import Literal

from fastapi import APIRouter, File, HTTPException, UploadFile
from pydantic import BaseModel, Field

from zhenxun.services.log import logger

from ....base_model import Result
from ....utils import authentication

router = APIRouter()

MAX_IMAGE = 10 * 1024 * 1024
"""测试图片大小上限"""


class SourcePayload(BaseModel):
    enabled: bool
    weight: float = Field(ge=0, le=1)


class SettingsPayload(BaseModel):
    tier: Literal["off", "cloud", "lite", "full"]
    min_score: float = Field(ge=0, le=3)
    sources: dict[str, SourcePayload]
    saucenao_key: str | None = Field(default=None, max_length=200)
    """为 None 时保留原值，空字符串清除"""
    hf_endpoint: str = Field(min_length=8, max_length=200, pattern=r"^https?://")


class InstallPayload(BaseModel):
    tier: Literal["lite", "full"]
    rebuild_library: bool = False


def _status() -> dict:
    from zhenxun.builtin_plugins.ai_chat._char_local import LocalModels
    from zhenxun.builtin_plugins.ai_chat._characters import settings, tier
    from zhenxun.builtin_plugins.ai_chat._components import component_status
    from zhenxun.builtin_plugins.ai_chat._data_source import MODULE
    from zhenxun.configs.config import Config

    return {
        "tier": tier(),
        "min_score": float(Config.get_config(MODULE, "CHARACTER_MIN_SCORE") or 0.5),
        "sources": settings(),
        "saucenao_key_set": bool(Config.get_config(MODULE, "CHARACTER_SAUCENAO_KEY")),
        "hf_endpoint": Config.get_config(MODULE, "HF_ENDPOINT")
        or "https://hf-mirror.com",
        "component": component_status("characters")[0],
        "local": LocalModels.status(),
    }


@router.get(
    "/characters/status", dependencies=[authentication()], response_model=Result
)
async def character_status() -> Result:
    return Result.ok(_status())


@router.put(
    "/characters/settings", dependencies=[authentication()], response_model=Result
)
async def update_character_settings(payload: SettingsPayload) -> Result:
    from zhenxun.builtin_plugins.ai_chat._characters import SOURCES
    from zhenxun.builtin_plugins.ai_chat._data_source import MODULE
    from zhenxun.configs.config import Config

    unknown = set(payload.sources) - set(SOURCES)
    if unknown:
        raise HTTPException(422, detail=f"未知的识别来源：{'、'.join(sorted(unknown))}")
    Config.set_config(MODULE, "CHARACTER_RECOGNITION", payload.tier)
    Config.set_config(MODULE, "CHARACTER_MIN_SCORE", payload.min_score)
    Config.set_config(
        MODULE,
        "CHARACTER_SOURCES",
        {k: v.model_dump() for k, v in payload.sources.items()},
    )
    if payload.saucenao_key is not None:
        Config.set_config(
            MODULE, "CHARACTER_SAUCENAO_KEY", payload.saucenao_key.strip()
        )
    Config.set_config(MODULE, "HF_ENDPOINT", payload.hf_endpoint.rstrip("/"))
    Config.save(save_simple_data=True)
    logger.info(f"WebUI 修改了角色识别设置: 档位={payload.tier}", "WebUi")
    return Result.ok(_status(), info="角色识别设置已保存。")


@router.post(
    "/characters/install", dependencies=[authentication()], response_model=Result
)
async def install_models(payload: InstallPayload) -> Result:
    from zhenxun.builtin_plugins.ai_chat._char_local import LocalModels

    try:
        started = LocalModels.install(payload.tier, payload.rebuild_library)
    except ValueError as e:
        raise HTTPException(422, detail=str(e)) from e
    if not started:
        raise HTTPException(409, detail="已有模型下载任务在进行")
    return Result.ok(_status(), info="已开始下载，可在本页查看进度。")


@router.delete(
    "/characters/models", dependencies=[authentication()], response_model=Result
)
async def remove_models() -> Result:
    from zhenxun.builtin_plugins.ai_chat._char_local import LocalModels

    try:
        await LocalModels.remove()
    except ValueError as e:
        raise HTTPException(409, detail=str(e)) from e
    return Result.ok(_status(), info="本地模型与角色库已删除。")


@router.post("/characters/test", dependencies=[authentication()], response_model=Result)
async def test_recognition(file: UploadFile = File(...)) -> Result:
    from zhenxun.builtin_plugins.ai_chat._characters import (
        CharacterRecognizer,
        tier,
    )

    data = await file.read(MAX_IMAGE + 1)
    if len(data) > MAX_IMAGE:
        raise HTTPException(422, detail="图片不能超过 10MB")
    if tier() == "off":
        raise HTTPException(422, detail="请先选择识别档位并保存")
    started = time.monotonic()
    try:
        result = await CharacterRecognizer.recognize(data, use_cache=False)
    except Exception as e:
        raise HTTPException(422, detail=f"无法识别该图片：{e}") from e
    return Result.ok(
        {
            "candidates": [
                {"key": c.key, "work": c.work, "score": c.score, "sources": c.sources}
                for c in result.candidates
            ],
            "sources": result.sources,
            "note": CharacterRecognizer.note(result) if result.ran else None,
            "ms": round((time.monotonic() - started) * 1000),
        }
    )
