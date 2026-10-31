"""内置 AI 聊天的表情库与技能管理"""

from __future__ import annotations

import asyncio
import base64
import binascii
from typing import Any

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from ....base_model import Result
from ....utils import authentication

router = APIRouter()


class StickerUpdate(BaseModel):
    remark: str = Field(default="", max_length=64)
    tags: list[str] = Field(default_factory=list, max_length=10)
    usage: str = Field(default="", max_length=255)


class StickerUpload(StickerUpdate):
    data: str = Field(min_length=1, max_length=2_900_000)
    """图片 base64，可带 data URL 前缀"""


async def _sticker_item(sticker: Any) -> dict[str, Any]:
    from zhenxun.builtin_plugins.ai_chat._stickers import Stickers

    path = Stickers.thumb_path(sticker)
    thumb = await asyncio.to_thread(path.read_bytes) if path.exists() else b""
    return {
        "id": sticker.id,
        "remark": sticker.remark,
        "tags": sticker.tags or [],
        "usage": sticker.usage,
        "use_count": sticker.use_count,
        "last_used_at": sticker.last_used_at,
        "source": sticker.source,
        "group_id": sticker.group_id,
        "user_id": sticker.user_id,
        "created_at": sticker.created_at,
        "thumb": f"data:image/png;base64,{base64.b64encode(thumb).decode()}"
        if thumb
        else None,
    }


@router.get("/chat/stickers", dependencies=[authentication()], response_model=Result)
async def list_stickers(query: str = Query(default="", max_length=64)) -> Result:
    from zhenxun.builtin_plugins.ai_chat._data_source import MODULE
    from zhenxun.builtin_plugins.ai_chat._stickers import Stickers
    from zhenxun.configs.config import Config

    stickers = await Stickers.search(query, limit=1000)
    return Result.ok(
        {
            "items": [await _sticker_item(s) for s in stickers],
            "limit": int(Config.get_config(MODULE, "STICKER_LIMIT") or 0),
        }
    )


@router.post("/chat/stickers", dependencies=[authentication()], response_model=Result)
async def upload_sticker(payload: StickerUpload) -> Result:
    from zhenxun.builtin_plugins.ai_chat._stickers import Stickers

    try:
        data = base64.b64decode(payload.data.split(",", 1)[-1], validate=True)
    except (binascii.Error, ValueError) as e:
        raise HTTPException(422, detail="图片数据无效。") from e
    try:
        sticker, created = await Stickers.add(
            data, payload.remark, payload.tags, payload.usage, source="webui"
        )
    except ValueError as e:
        raise HTTPException(422, detail=str(e)) from e
    return Result.ok(
        await _sticker_item(sticker),
        info="已上传。" if created else "这张已经收藏过了。",
    )


@router.post(
    "/chat/stickers/import_qq",
    dependencies=[authentication()],
    response_model=Result,
)
async def import_qq_stickers() -> Result:
    from nonebot import get_bots

    from zhenxun.builtin_plugins.ai_chat._stickers import Stickers
    from zhenxun.utils.platform import PlatformUtils

    bots = [
        bot
        for bot in get_bots().values()
        if PlatformUtils.get_platform_scope(bot) == "qq_client"
    ]
    if not bots:
        raise HTTPException(422, detail="没有在线的 OneBot V11 Bot。")
    added = total = 0
    errors = []
    for bot in bots:
        try:
            got, found = await Stickers.import_qq(bot)
        except ValueError as e:
            errors.append(str(e))
            continue
        added += got
        total += found
    if errors and not total:
        raise HTTPException(422, detail=errors[0])
    return Result.ok(
        {"added": added, "total": total},
        info=f"读取到 {total} 个 QQ 收藏表情，新增 {added} 个。",
    )


@router.put(
    "/chat/stickers/{sticker_id}",
    dependencies=[authentication()],
    response_model=Result,
)
async def update_sticker(sticker_id: int, payload: StickerUpdate) -> Result:
    from zhenxun.builtin_plugins.ai_chat._stickers import Stickers

    sticker = await Stickers.update(
        sticker_id, payload.remark, payload.tags, payload.usage
    )
    if sticker is None:
        raise HTTPException(404, detail="表情不存在")
    return Result.ok(await _sticker_item(sticker), info="已保存。")


@router.delete(
    "/chat/stickers/{sticker_id}",
    dependencies=[authentication()],
    response_model=Result,
)
async def delete_sticker(sticker_id: int) -> Result:
    from zhenxun.builtin_plugins.ai_chat._stickers import Stickers

    if not await Stickers.remove(sticker_id):
        raise HTTPException(404, detail="表情不存在")
    return Result.ok(info="已删除。")


class SkillUpload(BaseModel):
    filename: str = Field(min_length=1, max_length=255)
    data: str = Field(min_length=1, max_length=7_100_000)
    """SKILL.md 或 zip 的 base64，可带 data URL 前缀"""


class SkillSwitch(BaseModel):
    enabled: bool


def _skill_item(skill: Any) -> dict[str, Any]:
    from zhenxun.builtin_plugins.ai_chat._skills import Skills

    return {
        "name": skill.name,
        "description": skill.description,
        "enabled": skill.enabled,
        "files": Skills.files(skill),
    }


@router.get("/chat/skills", dependencies=[authentication()], response_model=Result)
async def list_skills() -> Result:
    from zhenxun.builtin_plugins.ai_chat._skills import Skills

    skills = await asyncio.to_thread(Skills.all)
    return Result.ok([_skill_item(s) for s in skills])


@router.post("/chat/skills", dependencies=[authentication()], response_model=Result)
async def upload_skill(payload: SkillUpload) -> Result:
    from zhenxun.builtin_plugins.ai_chat._skills import Skills

    try:
        data = base64.b64decode(payload.data.split(",", 1)[-1], validate=True)
    except (binascii.Error, ValueError) as e:
        raise HTTPException(422, detail="文件数据无效。") from e
    try:
        skill = await asyncio.to_thread(Skills.install, payload.filename, data)
    except ValueError as e:
        raise HTTPException(422, detail=str(e)) from e
    return Result.ok(_skill_item(skill), info=f"技能 {skill.name} 已安装。")


@router.put(
    "/chat/skills/{name}", dependencies=[authentication()], response_model=Result
)
async def switch_skill(name: str, payload: SkillSwitch) -> Result:
    from zhenxun.builtin_plugins.ai_chat._skills import Skills

    if not any(s.name == name for s in await asyncio.to_thread(Skills.all)):
        raise HTTPException(404, detail="技能不存在")
    Skills.set_enabled(name, payload.enabled)
    return Result.ok(info="已启用。" if payload.enabled else "已停用。")


@router.delete(
    "/chat/skills/{name}", dependencies=[authentication()], response_model=Result
)
async def delete_skill(name: str) -> Result:
    from zhenxun.builtin_plugins.ai_chat._skills import Skills

    if not await asyncio.to_thread(Skills.remove, name):
        raise HTTPException(404, detail="技能不存在")
    return Result.ok(info="已删除。")
