"""回复的发送：清理模型输出、Markdown 清理、语音、长回复转图或合并转发、分段发送"""

import asyncio
from functools import lru_cache
import random
import re
import time

from nonebot.adapters import Bot
from nonebot.matcher import current_bot
from nonebot_plugin_alconna import Image, Reference, Reply, Text, UniMessage, Voice

from zhenxun.configs.config import BotConfig, Config
from zhenxun.services.ai.core.exceptions import LLMException
from zhenxun.services.ai.core.options import TTSConfig
from zhenxun.services.ai.llm import create_speech, get_default_model
from zhenxun.services.log import logger
from zhenxun.utils.message import MessageUtils
from zhenxun.utils.platform import PlatformUtils

from ._buffer import ChatBuffer, ChatLine
from ._data_source import MODULE
from ._safety import Safety

LONG_REPLY_MODES = ("text", "forward", "image")
"""长回复发送方式：text 直接输出，forward 合并转发，image 转图片"""
FORWARD_MAX_NODES = 20
VOICE_MAX_CHARS = 200
"""超过该长度的回复不转语音"""
_SILENT = re.compile(r"[\[【]\s*silent\s*[\]】]", re.IGNORECASE)
_THINK = re.compile(r"<(think|thinking)>.*?</\1>\s*", re.S | re.I)
_TOOL_MARKUP = re.compile(
    r"<tool_call>.*?(?:</tool_call>|$)"
    r"|<function_calls>.*?(?:</function_calls>|$)"
    r"|<\|tool_calls_section_begin\|>.*?(?:<\|tool_calls_section_end\|>|$)"
    r"|<｜tool▁calls▁begin｜>.*?(?:<｜tool▁calls▁end｜>|$)"
    r"|```tool_code\s.*?(?:```|$)",
    re.S,
)
"""模型以文本形式输出的工具调用（中转未解析时）"""
_ECHO_LABEL = re.compile(
    r"^[ \t]*(?:\[(?:\d{2}-\d{2} )?\d{1,2}:\d{2}\][ \t]*[^\n:：]{0,40}?"
    r"|你\(\d+\)(?:[ \t]*#\S+)?)[ \t]*[:：][ \t]*",
    re.M,
)
"""模型模仿群聊背景格式输出的“[时间] 发言人:”或“你(id):”前缀"""
_SENTENCE_END = re.compile(r"[。！？!?…~]")
_FENCE = re.compile(r"^\s*(```|~~~)")
_INLINE_CODE = re.compile(r"(`[^`\n]+`)")
_LINE_RULES = [
    (re.compile(r"^\s{0,3}#{1,6}\s+"), ""),
    (re.compile(r"^\s{0,3}>\s?"), ""),
    (re.compile(r"^\s{0,3}([-*_])(\s*\1){2,}\s*$"), ""),
    (re.compile(r"^(\s*)[*+]\s+"), r"\1- "),
]
_SPAN_RULES = [
    (re.compile(r"\*\*(.+?)\*\*"), r"\1"),
    (
        re.compile(r"(?<![0-9A-Za-z*])\*(?!\s)([^*\n]+?)(?<!\s)\*(?![0-9A-Za-z*])"),
        r"\1",
    ),
    (re.compile(r"!?\[([^\]\n]*)\]\((\S+?)\)"), r"\1 \2"),
]


def _segments(text: str) -> list[tuple[bool, str]]:
    """按代码块切分文本

    参数:
        text: 文本

    返回:
        list[tuple[bool, str]]: (是否代码块, 内容) 列表
    """
    result: list[tuple[bool, list[str]]] = []
    fenced = False
    for line in text.splitlines():
        is_fence = bool(_FENCE.match(line))
        if is_fence and not fenced:
            result.append((True, [line]))
            fenced = True
            continue
        if fenced:
            result[-1][1].append(line)
            fenced = not is_fence
            continue
        if result and not result[-1][0]:
            result[-1][1].append(line)
        else:
            result.append((False, [line]))
    return [(code, "\n".join(lines)) for code, lines in result]


def clean_markdown(text: str) -> str:
    """去掉 QQ 不渲染的 Markdown 痕迹：标题、引用、分隔线、粗斜体、行内代码、
    链接写法、行尾空格与多余空行，代码块保持原样

    参数:
        text: 模型回复

    返回:
        str: 清理后的文本
    """
    out = []
    for code, body in _segments(text.strip()):
        if code:
            out.append(body)
            continue
        lines = []
        for line in body.splitlines():
            for pattern, repl in _LINE_RULES:
                line = pattern.sub(repl, line)
            pieces = _INLINE_CODE.split(line)
            for i in range(0, len(pieces), 2):
                for pattern, repl in _SPAN_RULES:
                    pieces[i] = pattern.sub(repl, pieces[i])
            for i in range(1, len(pieces), 2):
                pieces[i] = pieces[i][1:-1]
            lines.append("".join(pieces).rstrip())
        out.append(re.sub(r"\n{3,}", "\n\n", "\n".join(lines)))
    return "\n".join(out).strip()


@lru_cache(maxsize=16)
def _regex(key: str, pattern: str) -> re.Pattern[str] | None:
    """编译分段配置中的正则，无效时记录一次警告并忽略

    参数:
        key: 配置项名
        pattern: 正则表达式

    返回:
        re.Pattern[str] | None: 编译结果，为空或无效时为 None
    """
    if not pattern:
        return None
    try:
        return re.compile(pattern)
    except re.error as e:
        logger.warning(f"{key} 不是有效的正则表达式，已忽略: {e}", MODULE)
        return None


def _cut(line: str, pattern: re.Pattern[str]) -> list[str]:
    """在每个匹配之后切开一行，匹配内容保留在前一片段

    参数:
        line: 一行文本
        pattern: 切分正则

    返回:
        list[str]: 片段
    """
    pieces: list[str] = []
    start = 0
    for match in pattern.finditer(line):
        if match.end() > start:
            pieces.append(line[start : match.end()])
            start = match.end()
    pieces.append(line[start:])
    return pieces


def split_reply(
    text: str,
    max_parts: int = 5,
    pattern: re.Pattern[str] | None = None,
    cleanup: re.Pattern[str] | None = None,
) -> list[str]:
    """按行与句末标点（或自定义规则）切分回复，每行单独成段，行内过短的片段
    并入前一段，含代码块时不切分

    参数:
        text: 回复文本
        max_parts: 最多段数，超出部分按行合并为最后一段
        pattern: 行内切分正则，为空时按句末标点
        cleanup: 从每段中删除的内容

    返回:
        list[str]: 分段文本
    """
    text = text.strip()
    if "```" in text:
        return [text]
    parts: list[str] = []
    for line in text.splitlines():
        pieces: list[str] = []
        for piece in _cut(line, pattern or _SENTENCE_END):
            piece = piece.strip()
            if not piece:
                continue
            if pieces and len(piece) < 4:
                pieces[-1] += piece
            else:
                pieces.append(piece)
        parts.extend(pieces)
    if len(parts) > max_parts:
        parts = [*parts[: max_parts - 1], "\n".join(parts[max_parts - 1 :])]
    if cleanup:
        parts = [part for part in (cleanup.sub("", p).strip() for p in parts) if part]
    return parts or [text]


def is_long(text: str) -> bool:
    """判断回复是否按长回复发送：超过 LONG_REPLY_CHARS 字

    参数:
        text: 回复文本

    返回:
        bool: 是否为长回复
    """
    limit = int(Config.get_config(MODULE, "LONG_REPLY_CHARS", 1500) or 0)
    return bool(limit) and len(text) > limit


def quote_reply(group_id: str | None) -> bool:
    """群聊回复是否引用触发消息（REPLY_QUOTE），私聊不引用

    参数:
        group_id: 群号，私聊为空

    返回:
        bool: 是否引用
    """
    return bool(group_id) and bool(Config.get_config(MODULE, "REPLY_QUOTE", False))


def split_delay() -> float:
    """分段发送时每段发送前等待的秒数：SPLIT_INTERVAL 为“最小,最大”时在区间内
    随机取值，为单个数字时固定等待；未开启分段回复时为 0

    返回:
        float: 等待秒数
    """
    if not Config.get_config(MODULE, "SPLIT_REPLY", False):
        return 0.0
    value = str(Config.get_config(MODULE, "SPLIT_INTERVAL", "1.5,3.5") or 0)
    try:
        bounds = [float(v) for v in value.split(",", 1)]
    except ValueError:
        return 0.0
    return max(random.uniform(min(bounds), max(bounds)), 0.0)


def note_forward(bot_id: str, group_id: str | None, text: str) -> None:
    """合并转发不经过发送回调的记录，把回复摘要记入群聊缓冲

    参数:
        bot_id: Bot id
        group_id: 群号，私聊为空时不记录
        text: 回复文本
    """
    if not group_id:
        return
    summary = clean_markdown(text).replace("\n", " ")
    if len(summary) > 200:
        summary = f"{summary[:200]}…"
    ChatBuffer.record(
        bot_id,
        group_id,
        ChatLine(
            None,
            bot_id,
            BotConfig.self_nickname or "Bot",
            f"[合并转发] {summary}",
            time.time(),
            is_bot=True,
        ),
    )


def _paragraphs(text: str) -> list[str]:
    """按空行切分段落，代码块整体作为一段，最多 FORWARD_MAX_NODES 段

    参数:
        text: 文本

    返回:
        list[str]: 段落
    """
    paragraphs: list[str] = []
    for code, body in _segments(text.strip()):
        if code:
            paragraphs.append(body)
        else:
            paragraphs += [p.strip() for p in re.split(r"\n\s*\n", body) if p.strip()]
    if len(paragraphs) > FORWARD_MAX_NODES:
        tail = "\n\n".join(paragraphs[FORWARD_MAX_NODES - 1 :])
        paragraphs = [*paragraphs[: FORWARD_MAX_NODES - 1], tail]
    return paragraphs


async def long_message(bot: Bot | None, text: str) -> UniMessage | None:
    """按 LONG_REPLY 把长回复转为图片或合并转发；QQ 官方不支持合并转发时改为图片，
    转图失败或不是长回复时返回空，由调用方按文字发送

    参数:
        bot: Bot，为空时不使用合并转发
        text: 回复文本

    返回:
        UniMessage | None: 图片或合并转发消息
    """
    mode = str(Config.get_config(MODULE, "LONG_REPLY", "forward") or "forward")
    if mode not in ("forward", "image") or not is_long(text):
        return None
    if mode == "forward" and bot and PlatformUtils.get_platform_scope(bot) != "qq_api":
        nodes = [clean_markdown(p) for p in _paragraphs(text)]
        return MessageUtils.alc_forward_msg(
            [[node] for node in nodes if node],
            bot.self_id,
            BotConfig.self_nickname or "Bot",
        )
    from zhenxun.ui import render_markdown

    try:
        return UniMessage(Image(raw=await render_markdown(text)))
    except Exception as e:
        logger.warning(f"长回复转图失败，改为文字发送: {e}", MODULE)
        return None


def text_parts(text: str) -> list[str]:
    """清理 Markdown 后按 SPLIT_REPLY 切分文字回复，超过 SPLIT_MAX_CHARS 字时
    整条发送，按 SPLIT_PATTERN 切分并用 SPLIT_CLEANUP 清理每段

    参数:
        text: 回复文本

    返回:
        list[str]: 分段文本
    """
    text = clean_markdown(text)
    if not Config.get_config(MODULE, "SPLIT_REPLY", False):
        return [text]
    limit = int(Config.get_config(MODULE, "SPLIT_MAX_CHARS", 0) or 0)
    if limit and len(text) > limit:
        return [text]
    return split_reply(
        text,
        pattern=_regex(
            "SPLIT_PATTERN", str(Config.get_config(MODULE, "SPLIT_PATTERN", "") or "")
        ),
        cleanup=_regex(
            "SPLIT_CLEANUP", str(Config.get_config(MODULE, "SPLIT_CLEANUP", "") or "")
        ),
    )


async def deliver(
    bot: Bot,
    group_id: str | None,
    user_id: str | None,
    text: str,
    reply_to: str | None = None,
    voice_mode: str | None = None,
) -> bool:
    """不依赖事件向群或私聊发送回复：语音回复设为 always 时发语音，长回复转图或
    合并转发，其余清理 Markdown 后分段发送，开启 REPLY_QUOTE 时第一段引用消息

    参数:
        bot: Bot
        group_id: 群号，私聊为空
        user_id: 私聊对象
        text: 文字
        reply_to: 引用的消息id，未开启 REPLY_QUOTE 时忽略
        voice_mode: 语音回复方式，为空使用全局配置

    返回:
        bool: 是否全部发送成功
    """
    if not quote_reply(group_id):
        reply_to = None
    text, _ = Safety.filter_output(text)
    voice = await voice_segment(clean_markdown(text), False, voice_mode, bot)
    if voice:
        return bool(
            await PlatformUtils.send_message(bot, user_id, group_id, UniMessage(voice))
        )
    if message := await long_message(bot, text):
        if reply_to and not message.has(Reference):
            message = UniMessage(Reply(reply_to)) + message
        if not await PlatformUtils.send_message(bot, user_id, group_id, message):
            return False
        if message.has(Reference):
            note_forward(bot.self_id, group_id, text)
        return True
    for index, part in enumerate(text_parts(text)):
        if delay := split_delay():
            await asyncio.sleep(delay)
        message = (
            UniMessage([Reply(reply_to), Text(part)])
            if reply_to and not index
            else UniMessage(Text(part))
        )
        if not await PlatformUtils.send_message(bot, user_id, group_id, message):
            return False
    return True


def clean_output(text: str) -> str:
    """去掉模型回复中不该发出的内容：思考过程、文本形式的工具调用、
    模仿聊天记录格式的时间与发言人前缀

    参数:
        text: 模型回复

    返回:
        str: 清理后的文本
    """
    text = _THINK.sub("", text)
    head, sep, tail = text.partition("</think>")
    if sep and "<think" not in head.lower():
        text = tail
    text = _TOOL_MARKUP.sub("", text)
    return _ECHO_LABEL.sub("", text).strip()


def parse_silent(text: str) -> str | None:
    """解析模型回复：先去掉不该发出的内容，为空、以沉默令牌开头或只有令牌时
    视为沉默，其余情况去掉令牌后返回

    参数:
        text: 模型回复

    返回:
        str | None: 要发送的文本，沉默时为空
    """
    text = clean_output(text)
    if not text or _SILENT.match(text) or text.upper() == "SILENT":
        return None
    return _SILENT.sub("", text).strip() or None


async def voice_segment(
    text: str,
    voice_input: bool,
    mode: str | None = None,
    bot: Bot | None = None,
) -> Voice | None:
    """按语音回复配置用 tts 默认模型把回复合成为语音，QQ 官方编码为 silk

    参数:
        text: 回复文本
        voice_input: 用户是否发送了语音
        mode: 语音回复方式（off、voice、always），为空使用全局配置
        bot: Bot，为空时取当前事件的 Bot

    返回:
        Voice | None: 语音，不需要或无法合成时为空
    """
    mode = mode or str(Config.get_config(MODULE, "VOICE_REPLY") or "off")
    if mode != "always" and not (mode == "voice" and voice_input):
        return None
    text = text.strip()
    if len(text) > VOICE_MAX_CHARS or "```" in text or not get_default_model("tts"):
        return None
    bot = bot or current_bot.get()
    qq_api = PlatformUtils.get_platform_scope(bot) == "qq_api"
    try:
        speech = await create_speech(
            text, config=TTSConfig(response_format="wav" if qq_api else "mp3")
        )
    except LLMException as e:
        logger.warning(f"语音回复合成失败，改为文字回复: {e}", MODULE)
        return None
    if not qq_api:
        return speech.to_voice()
    from zhenxun.services.ai.utils.media import audio_to_silk

    silk = await audio_to_silk(speech.audio_bytes, speech.audio_format)
    if not silk:
        logger.warning("语音回复无法编码为 silk，改为文字回复", MODULE)
        return None
    return Voice(raw=silk, mimetype="audio/silk", name="speech.silk")


async def send_reply(
    text: str,
    group_id: str | None,
    voice_input: bool = False,
    voice_mode: str | None = None,
) -> None:
    """按配置用语音、长回复（图片或合并转发）或分段文字发送回复，
    群聊按 REPLY_QUOTE 在第一段引用原消息

    参数:
        text: 回复文本
        group_id: 群号，私聊为空
        voice_input: 用户是否发送了语音
        voice_mode: 语音回复方式，为空使用全局配置
    """
    quote = quote_reply(group_id)
    text, _ = Safety.filter_output(text)
    voice = await voice_segment(clean_markdown(text), voice_input, voice_mode)
    if voice:
        await MessageUtils.build_message(voice).send()
        return
    bot = current_bot.get(None)
    if message := await long_message(bot, text):
        forward = message.has(Reference)
        await message.send(reply_to=quote and not forward)
        if forward and bot:
            note_forward(bot.self_id, group_id, text)
        return
    for index, part in enumerate(text_parts(text)):
        if delay := split_delay():
            await asyncio.sleep(delay)
        await MessageUtils.build_message(part).send(reply_to=quote and not index)
