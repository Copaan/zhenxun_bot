import asyncio
import base64
from datetime import datetime
import hashlib
from io import BytesIO
import json
from pathlib import Path
import re
import time
from typing import ClassVar

from nonebot.adapters import Bot
from nonebot_plugin_alconna import Image, Reply, Segment, UniMessage
from PIL import Image as PILImage
from tortoise.exceptions import IntegrityError
from tortoise.expressions import F

from zhenxun.configs import path_config
from zhenxun.configs.config import Config
from zhenxun.models.ai_sticker import AISticker
from zhenxun.models.ai_sticker_block import AIStickerBlock
from zhenxun.services.ai.run.context import RunContext
from zhenxun.services.ai.utils.utils import MediaFetchError, fetch_media
from zhenxun.services.log import logger
from zhenxun.utils.platform import PlatformUtils

from ._data_source import MODULE
from ._send import Scene, SendQuota

STICKER_MAX_BYTES = 2 * 1024 * 1024
"""单个表情的大小上限"""
STICKER_MAX_PIXELS = 4096 * 4096
"""单个表情的像素上限，避免解码超大尺寸的图片"""
THUMB_SIZE = (160, 160)
"""WebUI 缩略图尺寸"""
TOP_USED = 5
"""提醒中列出的最常用表情数"""
TOP_NEW = 3
"""提醒中列出的最近收藏表情数"""
RECENT_MINUTES = 30
"""最近这么多分钟内发过的表情不列入提醒"""
MARKER = re.compile(r"[\[【]\s*表情\s*[:：]\s*#?(\d+)\s*[\]】]")
"""回复中的表情标记 [表情:#id]"""
FIELD_WEIGHTS = (3.0, 2.0, 1.0)
"""搜索时备注、标签、适用场合的权重"""
DESCRIBE_PROMPT = (
    "这是一张聊天表情图。只输出 JSON："
    '{"remark": "几个字写它表达的意思", "tags": ["关键词", "最多 5 个"], '
    '"usage": "一句话写适合在什么场合用"}'
)
_FORMATS = {"PNG": "png", "JPEG": "jpg", "GIF": "gif", "WEBP": "webp"}
SORTS = {
    "used": ("-use_count", "-id"),
    "new": ("-id",),
    "recent": ("-last_used_at", "-id"),
}
"""WebUI 表情列表的排序：最常用、最新收藏、最近使用"""


def _clean(text: str | None, size: int) -> str:
    """把模型或用户给的说明整理为单行并截断"""
    return " ".join((text or "").split())[:size]


def _grams(text: str) -> set[str]:
    """文字的二元组，单字时为其本身

    参数:
        text: 文字

    返回:
        set[str]: 二元组
    """
    grams: set[str] = set()
    for word in text.lower().split():
        grams |= {word[i : i + 2] for i in range(len(word) - 1)} or {word}
    return grams


class Stickers:
    """表情库：图片存于 data/ai_chat/stickers/，按 md5 去重"""

    _describing: ClassVar[set[asyncio.Task]] = set()
    _describe_slot: ClassVar[asyncio.Semaphore | None] = None

    @classmethod
    def folder(cls) -> Path:
        """表情文件目录"""
        return path_config.DATA_PATH / "ai_chat" / "stickers"

    @classmethod
    def path(cls, sticker: AISticker) -> Path:
        """表情图片文件

        参数:
            sticker: 表情

        返回:
            Path: 文件路径
        """
        return cls.folder() / sticker.file

    @classmethod
    def thumb_path(cls, sticker: AISticker) -> Path:
        """表情缩略图文件

        参数:
            sticker: 表情

        返回:
            Path: 文件路径
        """
        return cls.folder() / f"{sticker.md5}.thumb.png"

    @classmethod
    async def add(
        cls,
        data: bytes,
        remark: str = "",
        tags: list[str] | None = None,
        usage: str = "",
        source: str = "ai",
        group_id: str | None = None,
        user_id: str | None = None,
    ) -> tuple[AISticker, bool]:
        """收藏一张表情，已收藏过时返回原表情；设为不再收集的图片只能从 WebUI 上传，
        上传后解除不再收集。没有备注且开启 STICKER_AUTO_DESCRIBE 时在后台用视觉模型
        补写说明

        参数:
            data: 图片内容
            remark: 备注
            tags: 标签
            usage: 适用场合
            source: 来源
            group_id: 所在群
            user_id: 原图发送者

        返回:
            tuple[AISticker, bool]: 表情, 是否新收藏

        异常:
            ValueError: 图片过大、格式不支持、已设为不再收集或表情库已满
        """
        if len(data) > STICKER_MAX_BYTES:
            raise ValueError("图片超过 2MB。")
        unsupported = "不是支持的图片格式（png / jpg / gif / webp）。"
        try:
            image = PILImage.open(BytesIO(data))
            ext = _FORMATS[str(image.format)]
        except Exception as e:
            raise ValueError(unsupported) from e
        if image.width * image.height > STICKER_MAX_PIXELS:
            raise ValueError("图片尺寸过大。")
        md5 = hashlib.md5(data).hexdigest()
        if existing := await AISticker.get_or_none(md5=md5):
            return existing, False
        if source == "webui":
            await AIStickerBlock.filter(md5=md5).delete()
        elif await AIStickerBlock.exists(md5=md5):
            raise ValueError("这张图片已设为不再收集。")

        def thumbnail() -> BytesIO:
            frame = image.convert("RGBA")
            frame.thumbnail(THUMB_SIZE)
            thumb = BytesIO()
            frame.save(thumb, "PNG")
            return thumb

        try:
            thumb = await asyncio.to_thread(thumbnail)
        except Exception as e:
            raise ValueError(unsupported) from e
        limit = int(Config.get_config(MODULE, "STICKER_LIMIT") or 0)
        if limit and await AISticker.all().count() >= limit:
            raise ValueError(f"表情库已满（{limit} 个），先在 WebUI 删除一些。")
        folder = cls.folder()

        def write() -> None:
            folder.mkdir(parents=True, exist_ok=True)
            (folder / f"{md5}.{ext}").write_bytes(data)
            (folder / f"{md5}.thumb.png").write_bytes(thumb.getvalue())

        await asyncio.to_thread(write)
        try:
            sticker = await AISticker.create(
                md5=md5,
                file=f"{md5}.{ext}",
                remark=_clean(remark, 64),
                tags=[_clean(t, 16) for t in tags or [] if _clean(t, 16)][:10],
                usage=_clean(usage, 255),
                source=source,
                group_id=group_id,
                user_id=user_id,
            )
        except IntegrityError:
            return await AISticker.get(md5=md5), False
        if not sticker.remark and Config.get_config(
            MODULE, "STICKER_AUTO_DESCRIBE", False
        ):
            task = asyncio.create_task(cls.auto_describe(sticker.id))
            cls._describing.add(task)
            task.add_done_callback(cls._describing.discard)
        return sticker, True

    @classmethod
    async def known(cls, data: bytes) -> bool:
        """图片是否已收藏或已设为不再收集

        参数:
            data: 图片内容

        返回:
            bool: 是否已知
        """
        md5 = hashlib.md5(data).hexdigest()
        return await AISticker.exists(md5=md5) or await AIStickerBlock.exists(md5=md5)

    @classmethod
    async def auto_describe(cls, sticker_id: int) -> None:
        """用默认视觉模型（未配置时用对话模型）给没有备注的表情写备注、标签和适用场合，
        全局一次一张，失败时保持原样

        参数:
            sticker_id: 表情 id
        """
        from zhenxun.services.ai.core.messages.parts import ImagePart, TextPart
        from zhenxun.services.ai.llm.api import chat
        from zhenxun.services.ai.llm.manager import get_default_model

        model = get_default_model("vision")
        if cls._describe_slot is None:
            cls._describe_slot = asyncio.Semaphore(1)
        async with cls._describe_slot:
            sticker = await AISticker.get_or_none(id=sticker_id)
            if sticker is None or sticker.remark:
                return
            try:
                data = await asyncio.to_thread(cls.path(sticker).read_bytes)
                response = await chat(
                    [TextPart(text=DESCRIBE_PROMPT), ImagePart(raw=data)],
                    model=model,
                    timeout=60,
                )
                match = re.search(r"\{.*\}", response.text, re.S)
                result = json.loads(match.group(0)) if match else {}
            except Exception as e:
                logger.debug(f"表情 #{sticker_id} 自动描述失败: {e}", MODULE)
                return
            if not isinstance(result, dict) or not result.get("remark"):
                return
            tags = result.get("tags")
            await cls.update(
                sticker_id,
                str(result["remark"]),
                [str(t) for t in tags] if isinstance(tags, list) else None,
                str(result.get("usage") or ""),
            )

    @classmethod
    async def push_qq(cls, bot: Bot, data: bytes, remark: str) -> None:
        """STICKER_QQ_SYNC 开启时把新收藏的表情加入 Bot 账号的 QQ 收藏表情，
        需要协议端支持 add_custom_face，失败时忽略

        参数:
            bot: Bot
            data: 图片内容
            remark: 备注
        """
        if not Config.get_config(MODULE, "STICKER_QQ_SYNC", False):
            return
        if PlatformUtils.get_platform_scope(bot) != "qq_client":
            return
        try:
            result = await bot.call_api(
                "add_custom_face", file=f"base64://{base64.b64encode(data).decode()}"
            )
            emoji_id = result.get("emoji_id") if isinstance(result, dict) else None
            if emoji_id and remark:
                await bot.call_api("modify_custom_face", emoji_id=emoji_id, desc=remark)
        except Exception as e:
            logger.debug(f"表情同步到 QQ 收藏失败，协议端可能不支持: {e}", MODULE)

    @classmethod
    async def import_qq(cls, bot: Bot, count: int = 100) -> tuple[int, int]:
        """把 Bot 账号的 QQ 收藏表情导入表情库：优先用 fetch_custom_face_detail
        （带备注），协议端不支持时用 fetch_custom_face（只有地址）

        参数:
            bot: Bot
            count: 最多读取的数量

        返回:
            tuple[int, int]: 新增数, 读取到的表情数

        异常:
            ValueError: 不是 OneBot V11 协议端或协议端不支持读取收藏表情
        """
        if PlatformUtils.get_platform_scope(bot) != "qq_client":
            raise ValueError("只有 OneBot V11 协议端能读取 QQ 收藏表情。")
        try:
            data = await bot.call_api("fetch_custom_face_detail", count=count)
            items = [
                (str(item.get("url") or ""), str(item.get("desc") or ""))
                for item in data or []
                if isinstance(item, dict)
            ]
        except Exception:
            try:
                data = await bot.call_api("fetch_custom_face", count=count)
            except Exception as e:
                raise ValueError("协议端不支持读取 QQ 收藏表情。") from e
            items = [(url, "") for url in data or [] if isinstance(url, str)]
        added = 0
        for url, desc in items[:count]:
            if not url.startswith(("http://", "https://")):
                continue
            try:
                raw = await fetch_media(
                    url, max_bytes=STICKER_MAX_BYTES, allow_private=True
                )
                _, created = await cls.add(raw, desc, source="qq")
            except MediaFetchError as e:
                logger.debug(f"下载 QQ 收藏表情失败: {e}", MODULE)
                continue
            except ValueError as e:
                if str(e).startswith("表情库已满"):
                    break
                continue
            added += created
        return added, len(items)

    @classmethod
    async def search(
        cls, query: str = "", limit: int | None = 20, source: str = ""
    ) -> list[AISticker]:
        """按备注、标签和适用场合搜索表情：按二元组重合度加字段权重打分，
        相关度高的在前，相同时常用的在前

        参数:
            query: 关键词，为空时返回最常用的
            limit: 条数，为空时不截断
            source: 来源，为空时不限

        返回:
            list[AISticker]: 表情
        """
        rows = AISticker.filter(source=source) if source else AISticker.all()
        stickers = await rows.order_by("-use_count", "-id")
        grams = _grams(query)
        if not grams:
            return stickers[:limit]
        scored = []
        for s in stickers:
            texts = (s.remark, " ".join(s.tags or []), s.usage)
            score = sum(
                weight * len(grams & _grams(text)) / len(grams)
                for text, weight in zip(texts, FIELD_WEIGHTS)
            )
            if query.strip().lower() in s.remark.lower():
                score += 3
            if score > 0:
                scored.append((score, s))
        scored.sort(key=lambda item: -item[0])
        return [s for _, s in scored[:limit]]

    @classmethod
    async def listing(
        cls, query: str = "", source: str = "", sort: str = "used"
    ) -> list[AISticker]:
        """WebUI 的表情列表：按关键词搜索时按相关度排列，否则按指定方式排序

        参数:
            query: 关键词
            source: 来源，为空时不限
            sort: 排序，见 SORTS

        返回:
            list[AISticker]: 表情
        """
        if query.strip():
            stickers = await cls.search(query, limit=10000)
            return [s for s in stickers if not source or s.source == source]
        rows = AISticker.filter(source=source) if source else AISticker.all()
        return await rows.order_by(*SORTS.get(sort, SORTS["used"]))

    @classmethod
    async def listing_page(
        cls,
        query: str = "",
        source: str = "",
        sort: str = "used",
        page: int = 1,
        page_size: int = 30,
    ) -> tuple[list[AISticker], int]:
        """读取表情列表的一页与筛选后的总数

        参数:
            query: 关键词
            source: 来源，为空时不限
            sort: 排序方式
            page: 页码，从 1 开始
            page_size: 每页数量，最多 100

        返回:
            tuple[list[AISticker], int]: 当前页表情与匹配总数
        """
        page_size = min(100, max(1, page_size))
        offset = (max(1, page) - 1) * page_size
        if query.strip():
            stickers = await cls.search(query, limit=None, source=source)
            return stickers[offset : offset + page_size], len(stickers)
        rows = AISticker.filter(source=source) if source else AISticker.all()
        count = await rows.count()
        stickers = (
            await rows.order_by(*SORTS.get(sort, SORTS["used"]))
            .offset(offset)
            .limit(page_size)
        )
        return stickers, count

    @classmethod
    async def candidates(cls) -> list[AISticker]:
        """提醒中列出的表情：最常用的与最近收藏的，最近发过的除外

        返回:
            list[AISticker]: 表情
        """
        since = time.time() - RECENT_MINUTES * 60
        stickers = [
            s
            for s in await AISticker.all().order_by("-use_count", "-id")
            if not s.last_used_at or s.last_used_at.timestamp() < since
        ]
        picked = stickers[:TOP_USED]
        newest = sorted(stickers[TOP_USED:], key=lambda s: -s.id)[:TOP_NEW]
        return picked + newest

    @classmethod
    def describe(cls, sticker: AISticker) -> str:
        """表情的一行说明

        参数:
            sticker: 表情

        返回:
            str: 说明
        """
        text = f"#{sticker.id} {sticker.remark or '无备注'}"
        if sticker.tags:
            text += f" [{'、'.join(sticker.tags)}]"
        if sticker.usage:
            text += f" 适合：{sticker.usage}"
        return text + f"（用过 {sticker.use_count} 次）"

    @classmethod
    async def send(
        cls, scene: Scene, sticker_id: int, reply_to: str = ""
    ) -> str | None:
        """向当前会话发送表情库中的一个表情，计入发送配额并记录使用

        参数:
            scene: 会话
            sticker_id: 表情 id
            reply_to: 引用的消息 id

        返回:
            str | None: 发送失败的原因，成功时为空
        """
        sticker = await AISticker.get_or_none(id=sticker_id)
        path = cls.path(sticker) if sticker else None
        if sticker is None or path is None or not path.exists():
            return "没有这个表情。"
        if reason := SendQuota.take(scene.key):
            return reason
        data = await asyncio.to_thread(path.read_bytes)
        segments: list[Segment] = [Reply(reply_to)] if reply_to else []
        segments.append(Image(raw=data))
        if not await PlatformUtils.send_message(
            scene.bot, scene.user_id, scene.group_id, UniMessage(segments)
        ):
            SendQuota.refund(scene.key, 1)
            return "发送失败。"
        await cls.mark_used(sticker)
        scene.note(f"[发了表情 #{sticker.id} {sticker.remark}]")
        return None

    @classmethod
    async def update(
        cls,
        sticker_id: int,
        remark: str | None = None,
        tags: list[str] | None = None,
        usage: str | None = None,
    ) -> AISticker | None:
        """修改表情的备注、标签或适用场合，参数为空的不修改

        参数:
            sticker_id: 表情 id
            remark: 备注
            tags: 标签
            usage: 适用场合

        返回:
            AISticker | None: 修改后的表情，不存在时为 None
        """
        sticker = await AISticker.get_or_none(id=sticker_id)
        if sticker is None:
            return None
        if remark is not None:
            sticker.remark = _clean(remark, 64)
        if tags is not None:
            sticker.tags = [_clean(t, 16) for t in tags if _clean(t, 16)][:10]
        if usage is not None:
            sticker.usage = _clean(usage, 255)
        await sticker.save(update_fields=["remark", "tags", "usage"])
        return sticker

    @classmethod
    async def remove(cls, sticker_id: int, block: bool = False) -> bool:
        """删除表情及其文件

        参数:
            sticker_id: 表情 id
            block: 是否同时设为不再收集

        返回:
            bool: 是否删除
        """
        return bool(await cls.remove_many([sticker_id], block))

    @classmethod
    async def remove_many(cls, sticker_ids: list[int], block: bool = False) -> int:
        """批量删除表情及其文件

        参数:
            sticker_ids: 表情 id
            block: 是否同时设为不再收集，之后 AI 和自动收集都不会再收藏这些图片

        返回:
            int: 删除数
        """
        stickers = await AISticker.filter(id__in=sticker_ids)
        for sticker in stickers:
            await sticker.delete()
            if block:
                await AIStickerBlock.get_or_create(md5=sticker.md5)
            for path in (cls.path(sticker), cls.thumb_path(sticker)):
                path.unlink(missing_ok=True)
        return len(stickers)

    @classmethod
    async def merge(cls, keep_id: int, other_ids: list[int]) -> AISticker | None:
        """把重复的表情合并到一个：标签取并集，备注和适用场合为空时取被合并的，
        使用次数相加；被合并的表情删除并设为不再收集

        参数:
            keep_id: 保留的表情 id
            other_ids: 被合并的表情 id

        返回:
            AISticker | None: 合并后的表情，保留的表情不存在时为 None
        """
        keep = await AISticker.get_or_none(id=keep_id)
        if keep is None:
            return None
        others = await AISticker.filter(id__in=[i for i in other_ids if i != keep_id])
        tags = list(keep.tags or [])
        for other in others:
            keep.remark = keep.remark or other.remark
            keep.usage = keep.usage or other.usage
            keep.use_count += other.use_count
            if other.last_used_at and (
                not keep.last_used_at or other.last_used_at > keep.last_used_at
            ):
                keep.last_used_at = other.last_used_at
            tags += [t for t in other.tags or [] if t not in tags]
        keep.tags = tags[:10]
        await keep.save(
            update_fields=["remark", "usage", "use_count", "last_used_at", "tags"]
        )
        await cls.remove_many([o.id for o in others], block=True)
        return keep

    @classmethod
    async def mark_used(cls, sticker: AISticker) -> None:
        """记录一次发送

        参数:
            sticker: 表情
        """
        await AISticker.filter(id=sticker.id).update(
            use_count=F("use_count") + 1, last_used_at=datetime.now()
        )


def split_sticker(text: str) -> tuple[str, int | None]:
    """去掉回复中的表情标记 [表情:#id]，取第一个标记的表情

    参数:
        text: 回复

    返回:
        tuple[str, int | None]: 去掉标记后的文字, 表情 id
    """
    match = MARKER.search(text)
    if match is None:
        return text, None
    return MARKER.sub("", text).strip(), int(match.group(1))


async def send_marked(context: RunContext, sticker_id: int) -> None:
    """回复文字发完后发送回复中标记的表情，关闭发送工具、表情不存在或配额用完时放弃

    参数:
        context: 运行上下文
        sticker_id: 表情 id
    """
    if not Config.get_config(MODULE, "SEND_TOOLS", True):
        return
    if (scene := Scene.of(context)) and (
        reason := await Stickers.send(scene, sticker_id)
    ):
        logger.debug(f"未发送标记的表情 #{sticker_id}: {reason}", MODULE)
