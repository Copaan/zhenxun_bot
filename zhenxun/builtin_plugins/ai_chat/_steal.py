"""偷表情：从群消息里自动收集表情包，按概率直接收藏或由视觉模型判断后收藏"""

import asyncio
from datetime import date
import hashlib
import json
import random
import re
import time
from typing import Any, ClassVar
import uuid

from nonebot.adapters import Bot
from nonebot_plugin_alconna import Image, Other, Segment, UniMessage
from tortoise.functions import Count
from tortoise.timezone import localtime

from zhenxun.configs.config import Config
from zhenxun.models.ai_agent_run import AIAgentRun
from zhenxun.models.ai_sticker import AISticker
from zhenxun.services.ai.utils.utils import MediaFetchError, fetch_media
from zhenxun.services.log import logger
from zhenxun.utils.time_utils import TimeUtils

from ._data_source import MODULE
from ._proactive import group_config
from ._stickers import STICKER_MAX_BYTES, Stickers
from ._tools import image_source

MIN_CONFIDENCE = 0.7
"""视觉模型判断为表情包的最低置信度"""
REJECT_TTL = 7 * 86400
"""判断为不是表情包的图片在这段时间内不再判断（秒）"""
MAX_REJECTED = 2000
"""最多记住的不是表情包的图片数"""
STEAL_PROMPT = (
    "判断这张图是不是适合在聊天里使用的表情包（梗图、反应图、动画表情等），"
    "截图、照片、长图、广告、色情或血腥内容都不算。只输出 JSON："
    '{"is_meme": true 或 false, "confidence": 0 到 1, '
    '"remark": "几个字写它表达的意思", "tags": ["关键词", "最多 5 个"], '
    '"usage": "一句话写适合在什么场合用"}'
)
"""监听模式给视觉模型的判断提示"""
STICKER_SUMMARIES = {"动画表情", "[动画表情]", "[表情]"}
"""协议端标记表情包的图片摘要"""


def _cfg(key: str, default: Any) -> Any:
    value = Config.get_config(MODULE, key)
    return default if value is None else value


def is_sticker(segment: Segment) -> bool:
    """消息段是否为表情包：商城表情，或协议端标为表情的图片
    （NapCat 的 sub_type 为 1、带 emoji_id 或摘要为“动画表情”）

    参数:
        segment: 消息段

    返回:
        bool: 是否为表情包
    """
    if isinstance(segment, Other):
        return getattr(segment.origin, "type", "") == "mface"
    if not isinstance(segment, Image):
        return False
    data = getattr(getattr(segment, "origin", None), "data", None) or {}
    return bool(
        segment.sticker
        or data.get("emoji_id")
        or data.get("emoji_package_id")
        or str(data.get("summary") or "").strip() in STICKER_SUMMARIES
    )


class Steal:
    """偷表情的运行状态：每群上次尝试时间与今日收集数、今日判断数"""

    _last: ClassVar[dict[str, float]] = {}
    _day: ClassVar[date | None] = None
    _counts: ClassVar[dict[str, int]] = {}
    _judges: ClassVar[int] = 0
    _tasks: ClassVar[set[asyncio.Task]] = set()
    _slot: ClassVar[asyncio.Semaphore | None] = None
    _rejected: ClassVar[dict[str, float]] = {}
    """判断为不是表情包的图片 md5 与判断时间"""

    @classmethod
    async def mode(cls, group_id: str) -> str:
        """群生效的偷表情模式，群停用 AI 聊天时为 off

        参数:
            group_id: 群号

        返回:
            str: off、probability 或 listen
        """
        setting, group = await group_config(group_id)
        if setting and setting.disabled:
            return "off"
        return group.steal or str(_cfg("STICKER_STEAL", "off"))

    @classmethod
    async def _load_today(cls) -> None:
        """跨天或首次读取时从表情库与运行记录汇总今日的收集数和判断数"""
        today = TimeUtils.now().date()
        if cls._day == today:
            return
        start = localtime(TimeUtils.get_day_start())
        rows = (
            await AISticker.filter(source="steal", created_at__gte=start)
            .annotate(n=Count("id"))
            .group_by("group_id")
            .values("group_id", "n")
        )
        cls._counts = {str(r["group_id"]): int(r["n"] or 0) for r in rows}
        cls._judges = await AIAgentRun.filter(
            source="steal", created_at__gte=start
        ).count()
        cls._day = today

    @classmethod
    async def today(cls, group_id: str) -> tuple[int, int]:
        """今日在该群收集的表情数与全部群的视觉判断次数

        参数:
            group_id: 群号

        返回:
            tuple[int, int]: 收集数, 判断次数
        """
        await cls._load_today()
        return cls._counts.get(group_id, 0), cls._judges

    @classmethod
    async def observe(
        cls, bot: Bot, group_id: str, user_id: str, message: UniMessage
    ) -> asyncio.Task | None:
        """检查一条群消息里的表情包，符合模式与限额时在后台收集一张：
        冷却已过、未达本群每日收集数，监听模式还需未达每日判断次数，
        概率模式还需抽中偷表情概率。监听模式在创建任务前预留一次判断次数

        参数:
            bot: Bot
            group_id: 群号
            user_id: 发送者
            message: 消息

        返回:
            asyncio.Task | None: 后台收集任务，不收集时为空
        """
        segments = [seg for seg in message if is_sticker(seg)]
        if not segments:
            return None
        mode = await cls.mode(group_id)
        if mode not in ("probability", "listen"):
            return None
        cooldown = int(_cfg("STEAL_COOLDOWN", 0))
        if cooldown and time.time() - cls._last.get(group_id, 0) < cooldown:
            return None
        count, judges = await cls.today(group_id)
        daily = int(_cfg("STEAL_DAILY_COUNT", 0))
        if daily and count >= daily:
            return None
        if mode == "listen":
            limit = int(_cfg("STEAL_DAILY_JUDGES", 0))
            if limit and judges >= limit:
                return None
            cls._judges += 1
        elif random.random() >= float(_cfg("STEAL_PROBABILITY", 0.0)):
            return None
        cls._last[group_id] = time.time()
        task = asyncio.create_task(
            cls.collect(bot, group_id, user_id, segments[0], mode == "listen")
        )
        cls._tasks.add(task)
        task.add_done_callback(cls._tasks.discard)
        return task

    @classmethod
    async def collect(
        cls, bot: Bot, group_id: str, user_id: str, segment: Segment, judge: bool
    ) -> bool:
        """下载并收藏一张表情，来源记为 steal；监听模式先让视觉模型判断，
        不是表情包或置信度不足时放弃并记住这张图，之后一段时间内不再判断。
        全局一次处理一张；监听模式使用 observe 预留的判断次数，没有调用视觉模型
        就结束时退还（跨天后不退还）

        参数:
            bot: Bot
            group_id: 群号
            user_id: 发送者
            segment: 图片或商城表情消息段
            judge: 是否先让视觉模型判断

        返回:
            bool: 是否新收藏
        """
        if cls._slot is None:
            cls._slot = asyncio.Semaphore(1)
        day, judged = cls._day, False
        try:
            async with cls._slot:
                raw, url, summary = image_source(segment)
                if raw is None:
                    if not url:
                        return False
                    try:
                        raw = await fetch_media(
                            url, max_bytes=STICKER_MAX_BYTES, allow_private=True
                        )
                    except MediaFetchError as e:
                        logger.debug(f"偷表情下载失败: {e}", MODULE)
                        return False
                note: dict[str, Any] = {"remark": summary}
                if judge:
                    digest = hashlib.md5(raw).hexdigest()
                    if cls.rejected(digest) or await Stickers.known(raw):
                        return False
                    judged = True
                    result = await cls.judge(bot, group_id, raw)
                    if result is None:
                        cls.reject(digest)
                        return False
                    note = result
                try:
                    sticker, created = await Stickers.add(
                        raw,
                        str(note.get("remark") or ""),
                        note.get("tags"),
                        str(note.get("usage") or ""),
                        source="steal",
                        group_id=group_id,
                        user_id=user_id,
                    )
                except ValueError as e:
                    logger.debug(f"偷表情未收藏: {e}", MODULE, group_id=group_id)
                    return False
                if created:
                    cls._counts[group_id] = cls._counts.get(group_id, 0) + 1
                    await Stickers.push_qq(bot, raw, sticker.remark)
                    logger.info(
                        f"偷到表情 #{sticker.id} {sticker.remark}",
                        MODULE,
                        group_id=group_id,
                    )
                return created
        finally:
            if judge and not judged and cls._day == day:
                cls._judges = max(0, cls._judges - 1)

    @classmethod
    def rejected(cls, digest: str) -> bool:
        """图片近期是否被判断为不是表情包

        参数:
            digest: 图片 md5

        返回:
            bool: 是否判断过且未过期
        """
        at = cls._rejected.get(digest)
        if at is None:
            return False
        if time.time() - at > REJECT_TTL:
            del cls._rejected[digest]
            return False
        return True

    @classmethod
    def reject(cls, digest: str) -> None:
        """记住一张不是表情包的图片，超出 MAX_REJECTED 时忘掉最早的

        参数:
            digest: 图片 md5
        """
        cls._rejected.pop(digest, None)
        cls._rejected[digest] = time.time()
        while len(cls._rejected) > MAX_REJECTED:
            del cls._rejected[next(iter(cls._rejected))]

    @classmethod
    async def judge(cls, bot: Bot, group_id: str, data: bytes) -> dict | None:
        """让视觉模型判断图片是不是表情包并写说明，结果记为 steal 运行记录；
        判断次数由 observe 预留

        参数:
            bot: Bot
            group_id: 群号
            data: 图片内容

        返回:
            dict | None: 是表情包时为备注、标签与适用场合，否则为空
        """
        from zhenxun.services.ai.core.engine.token_counter import parse_usage_info
        from zhenxun.services.ai.core.messages.parts import ImagePart, TextPart
        from zhenxun.services.ai.llm.api import chat
        from zhenxun.services.ai.llm.manager import get_default_model

        model = str(_cfg("STEAL_MODEL", "")).strip() or get_default_model("vision")
        started = time.monotonic()
        status, output, error, tokens, note = "silent", "", "", 0, None
        try:
            response = await chat(
                [TextPart(text=STEAL_PROMPT), ImagePart(raw=data)],
                model=model,
                timeout=60,
            )
            output = response.text
            tokens = parse_usage_info(response.usage_info).total_tokens
            match = re.search(r"\{.*\}", output, re.S)
            result = json.loads(match.group(0)) if match else {}
            if (
                isinstance(result, dict)
                and result.get("is_meme") is True
                and float(result.get("confidence") or 0) >= MIN_CONFIDENCE
            ):
                status = "ok"
                tags = result.get("tags")
                note = {
                    "remark": str(result.get("remark") or ""),
                    "tags": [str(t) for t in tags] if isinstance(tags, list) else None,
                    "usage": str(result.get("usage") or ""),
                }
        except Exception as e:
            status, error = "error", f"{type(e).__name__}: {e}"
            logger.debug(f"偷表情判断失败: {e}", MODULE, group_id=group_id)
        try:
            await AIAgentRun.create(
                run_id=uuid.uuid4().hex,
                source="steal",
                session_id=f"{MODULE}:{bot.self_id}:{group_id}",
                user_id=bot.self_id,
                group_id=group_id,
                model=model or get_default_model("chat"),
                input="判断群里的一张表情",
                output=output[:4000],
                tokens=tokens,
                status=status,
                error=error[:2000],
                duration=int((time.monotonic() - started) * 1000),
            )
        except Exception as e:
            logger.warning(f"保存偷表情记录失败: {e}", MODULE)
        return note
