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
from nonebot_plugin_alconna import Image, Other, Reply, Segment, UniMessage
from PIL import Image as PILImage
from tortoise.exceptions import IntegrityError
from tortoise.expressions import F

from zhenxun.configs import path_config
from zhenxun.configs.config import Config
from zhenxun.models.ai_sticker import AISticker
from zhenxun.services.ai.run.context import RunContext
from zhenxun.services.ai.tools.core.decorators import tool
from zhenxun.services.ai.tools.core.toolkit import BaseToolkit
from zhenxun.services.ai.tools.models import ResolvedToolPayload, ToolResult
from zhenxun.services.ai.utils.utils import MediaFetchError, fetch_media
from zhenxun.services.log import logger
from zhenxun.utils.platform import PlatformUtils

from ._data_source import MODULE
from ._send import Scene, SendQuota
from ._tools import GroupHistory

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
        """收藏一张表情，已收藏过时返回原表情；没有备注且开启 STICKER_AUTO_DESCRIBE 时
        在后台用视觉模型补写说明

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
            ValueError: 图片过大、格式不支持或表情库已满
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
    async def search(cls, query: str = "", limit: int = 20) -> list[AISticker]:
        """按备注、标签和适用场合搜索表情：按二元组重合度加字段权重打分，
        相关度高的在前，相同时常用的在前

        参数:
            query: 关键词，为空时返回最常用的
            limit: 条数

        返回:
            list[AISticker]: 表情
        """
        stickers = await AISticker.all().order_by("-use_count", "-id")
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
    async def remove(cls, sticker_id: int) -> bool:
        """删除表情及其文件

        参数:
            sticker_id: 表情 id

        返回:
            bool: 是否删除
        """
        sticker = await AISticker.get_or_none(id=sticker_id)
        if sticker is None:
            return False
        await sticker.delete()
        for path in (cls.path(sticker), cls.thumb_path(sticker)):
            path.unlink(missing_ok=True)
        return True

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


def _image_source(segment: Segment) -> tuple[bytes | None, str | None, str]:
    """取图片或商城表情消息段的内容、地址和自带说明"""
    if isinstance(segment, Image):
        raw = segment.raw if isinstance(segment.raw, bytes) else None
        return raw, segment.url, ""
    origin = getattr(segment, "origin", None)
    data = getattr(origin, "data", None) or {}
    return None, data.get("url"), str(data.get("summary") or "").strip("[]")


def _find_image(message: UniMessage) -> Segment | None:
    """找出消息中的第一张图片或商城表情"""
    for seg in message:
        if isinstance(seg, Image):
            return seg
        if isinstance(seg, Other) and getattr(seg.origin, "type", "") == "mface":
            return seg
    return None


async def _locate(
    context: RunContext, message_id: str
) -> tuple[Segment, str | None] | str:
    """找到要收藏的图片：指定消息、当前消息或当前消息引用的消息

    参数:
        context: 运行上下文
        message_id: 消息 id，为空时用当前消息

    返回:
        tuple[Segment, str | None] | str: 图片消息段与发送者，找不到时为原因
    """
    bot, group_id = context.get_bot(), context.get_group_id()
    if bot is None:
        return "当前不在会话中。"
    candidates: list[tuple[UniMessage, str | None]] = []
    if message_id:
        if not group_id:
            return "私聊中只能收藏当前消息里的图片。"
        found = await GroupHistory.message(bot, group_id, message_id)
        if found is None:
            return "当前群里没有找到这条消息。"
        candidates.append((found[3], found[1]))
    elif event := context.get_event():
        message = UniMessage.of(event.get_message(), bot)
        candidates.append((message, context.get_user_id()))
        reply = getattr(event, "reply", None)
        if reply is not None:
            sender = getattr(getattr(reply, "sender", None), "user_id", None)
            candidates.append(
                (UniMessage.of(reply.message, bot), str(sender) if sender else None)
            )
        else:
            replied = next((s for s in candidates[0][0] if isinstance(s, Reply)), None)
            if replied and group_id:
                found = await GroupHistory.message(bot, group_id, str(replied.id))
                if found:
                    candidates.append((found[3], found[1]))
    for message, owner in candidates:
        if segment := _find_image(message):
            return segment, owner
    return "这条消息里没有图片。"


class StickerToolkit(BaseToolkit):
    """AI 聊天的表情包工具：收藏、搜索、修改说明、发送"""

    default_instructions = (
        "## 表情包\n"
        "你有一个自己的表情库。看到有意思的图片或表情可以用 `collect_sticker` 收藏，"
        "写清备注（表情的意思）和适用场合。想用表情时在回复末尾写 `[表情:#id]`"
        "（最多一个，只发表情时回复只写这个标记），文字发出后会接着发这个表情，"
        "不要每次都用。提醒里列出了部分表情，找别的用 `find_stickers`；"
        "要引用某条消息发表情时用 `send_sticker`。表情说明不准时用 "
        "`update_sticker` 修改。"
    )

    async def resolve(self, context: RunContext | None = None) -> ResolvedToolPayload:
        payload = await super().resolve(context)
        hidden = set()
        if not Config.get_config(MODULE, "SEND_TOOLS", True):
            hidden.add("send_sticker")
        if not Config.get_config(MODULE, "STICKER_COLLECT", True):
            hidden.add("collect_sticker")
        top = await Stickers.candidates()
        if not top and "collect_sticker" in hidden:
            return ResolvedToolPayload()
        payload.tools = [t for t in payload.tools if t.name not in hidden]
        if top and "send_sticker" not in hidden:
            payload.reminders.append(
                "可用表情（在回复末尾写 [表情:#id] 发送）："
                + "；".join(Stickers.describe(s) for s in top)
            )
        return payload

    @tool(
        name="collect_sticker",
        description=(
            "把一张图片或表情收藏进你的表情库。message_id 为空时取当前消息"
            "（或它引用的消息）里的图片；remark 用几个字写表情的意思，"
            "tags 是关键词，usage 写适合在什么场合用。"
        ),
    )
    async def collect_sticker(
        self,
        context: RunContext,
        remark: str,
        message_id: str = "",
        tags: list[str] | None = None,
        usage: str = "",
    ) -> ToolResult:
        scene = Scene.of(context)
        if scene is None:
            return ToolResult(output="当前不在会话中。").as_error()
        if not Config.get_config(MODULE, "STICKER_COLLECT", True):
            return ToolResult(output="表情收藏已关闭。").as_error(False)
        located = await _locate(context, message_id.strip().lstrip("#"))
        if isinstance(located, str):
            return ToolResult(output=located).as_error()
        segment, owner = located
        raw, url, summary = _image_source(segment)
        if raw is None and not url:
            return ToolResult(output="读取不到这张图片。").as_error()
        if reason := SendQuota.take(("collect", scene.key[1]), 1, 2, 10):
            return ToolResult(output=reason).as_error(False)
        if raw is None:
            try:
                raw = await fetch_media(
                    url or "", max_bytes=STICKER_MAX_BYTES, allow_private=True
                )
            except MediaFetchError as e:
                SendQuota.refund(("collect", scene.key[1]), 1)
                logger.debug(f"下载表情失败: {e}", MODULE)
                return ToolResult(
                    output="图片超过 2MB。" if e.too_large else "图片下载失败。"
                ).as_error(False)
        try:
            sticker, created = await Stickers.add(
                raw,
                remark or summary,
                tags,
                usage,
                group_id=scene.group_id,
                user_id=owner,
            )
        except ValueError as e:
            SendQuota.refund(("collect", scene.key[1]), 1)
            return ToolResult(output=str(e)).as_error(False)
        if not created:
            SendQuota.refund(("collect", scene.key[1]), 1)
            return ToolResult(output=f"已经收藏过了：{Stickers.describe(sticker)}")
        await Stickers.push_qq(scene.bot, raw, sticker.remark)
        return ToolResult(output=f"已收藏：{Stickers.describe(sticker)}")

    @tool(
        name="find_stickers",
        description="按关键词搜索表情库（备注、标签、适用场合），为空时列出最常用的。",
    )
    async def find_stickers(self, context: RunContext, query: str = "") -> ToolResult:
        stickers = await Stickers.search(query)
        if not stickers:
            return ToolResult(output="没有找到合适的表情。")
        return ToolResult(output="\n".join(Stickers.describe(s) for s in stickers))

    @tool(
        name="update_sticker",
        description="修改表情的备注、标签或适用场合，不填的不修改。",
    )
    async def update_sticker(
        self,
        context: RunContext,
        sticker_id: int,
        remark: str | None = None,
        tags: list[str] | None = None,
        usage: str | None = None,
    ) -> ToolResult:
        sticker = await Stickers.update(sticker_id, remark, tags, usage)
        if sticker is None:
            return ToolResult(output="没有这个表情。").as_error()
        return ToolResult(output=f"已修改：{Stickers.describe(sticker)}")

    @tool(
        name="send_sticker",
        description=(
            "立即向当前会话发送表情库中的一个表情，reply_to 可引用本群一条消息；"
            "不需要引用时在回复末尾写 [表情:#id] 即可。"
        ),
    )
    async def send_sticker(
        self, context: RunContext, sticker_id: int, reply_to: str = ""
    ) -> ToolResult:
        scene = Scene.of(context)
        if scene is None:
            return ToolResult(output="当前不在会话中。").as_error()
        reply_to = reply_to.strip().lstrip("#")
        if reply_to and (
            not scene.group_id
            or await GroupHistory.message(scene.bot, scene.group_id, reply_to) is None
        ):
            return ToolResult(output="只能引用当前群里的消息。").as_error()
        if reason := await Stickers.send(scene, sticker_id, reply_to):
            return ToolResult(output=reason).as_error(reason == "没有这个表情。")
        return ToolResult(output="已发送。")
