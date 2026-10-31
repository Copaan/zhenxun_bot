"""AI 聊天的内容安全、每用户频率限制与每会话限流

安全词在用户输入与 AI 输出两侧检查；输出同时检查是否泄漏已配置的密钥。
"""

import asyncio
from collections import deque
import re
import time
from typing import ClassVar

from zhenxun.configs.config import Config
from zhenxun.services.log import logger

from ._data_source import MODULE

INPUT_NOTICE = "这个话题我不方便聊，换个话题吧。"
"""输入命中安全词时的回复"""
OUTPUT_NOTICE = "这条回复包含不适合发送的内容，已被拦截。"
"""输出命中安全词时替换整条回复的文字"""
LEAK_NOTICE = "这条回复可能包含密钥，已被拦截。"
"""输出疑似泄漏密钥时替换整条回复的文字"""
RATE_NOTICE = "说话太快啦，歇一会儿再找我吧。"
"""超出频率限制时的提示，每个限制周期只提示一次"""
SECRET_PATTERNS = re.compile(
    r"sk-[A-Za-z0-9_\-]{20,}"
    r"|AIza[0-9A-Za-z_\-]{35}"
    r"|gh[pousr]_[A-Za-z0-9]{36,}"
    r"|github_pat_[A-Za-z0-9_]{40,}"
    r"|xox[abprs]-[A-Za-z0-9\-]{10,}"
    r"|AKIA[0-9A-Z]{16}"
    r"|-----BEGIN [A-Z ]*PRIVATE KEY-----"
)
"""常见密钥格式"""
MIN_SECRET = 8
"""参与泄漏检查的已配置密钥最短长度"""


def _secrets() -> set[str]:
    """已配置的密钥：服务商与联网搜索密钥、服务商请求头、SauceNAO 密钥、
    MCP 服务的环境变量与请求头"""
    from zhenxun.services.ai.config import get_llm_config
    from zhenxun.services.ai.tools.providers.mcp.provider import mcp_provider

    config = get_llm_config()
    values: list[str] = list(config.web_search.api_keys)
    for provider in config.providers:
        values += provider.api_keys
        values += provider.headers.values()
    values.append(str(Config.get_config(MODULE, "CHARACTER_SAUCENAO_KEY") or ""))
    for server in mcp_provider.servers().values():
        values += (server.env or {}).values()
        values += (server.headers or {}).values()
    result = set()
    for value in values:
        value = re.sub(r"^(Bearer|Basic|Digest)\s+", "", str(value).strip())
        if len(value) >= MIN_SECRET:
            result.add(value)
    return result


class Safety:
    """安全词检查与密钥泄漏拦截"""

    _compiled: ClassVar[tuple[tuple[str, ...], list[re.Pattern[str]]]] = ((), [])

    @classmethod
    def patterns(cls) -> list[re.Pattern[str]]:
        """按 SAFETY_WORDS 编译的匹配规则：/…/ 为正则，其余为忽略大小写的关键词，
        英文关键词只匹配完整单词

        返回:
            list[re.Pattern[str]]: 匹配规则，无效的正则会被跳过
        """
        words = tuple(
            str(w).strip()
            for w in Config.get_config(MODULE, "SAFETY_WORDS") or []
            if str(w).strip()
        )
        if words == cls._compiled[0]:
            return cls._compiled[1]
        result = []
        for word in words:
            if len(word) > 2 and word.startswith("/") and word.endswith("/"):
                try:
                    result.append(re.compile(word[1:-1], re.IGNORECASE))
                except re.error as e:
                    logger.warning(f"安全词正则 {word} 无效，已跳过: {e}", MODULE)
            else:
                pattern = re.escape(word)
                if word.isascii():
                    pattern = rf"(?<![A-Za-z0-9]){pattern}(?![A-Za-z0-9])"
                result.append(re.compile(pattern, re.IGNORECASE))
        cls._compiled = (words, result)
        return result

    @classmethod
    def check_input(cls, text: str) -> bool:
        """用户输入是否命中安全词

        参数:
            text: 消息文字

        返回:
            bool: 是否命中
        """
        return any(p.search(text) for p in cls.patterns())

    @classmethod
    def filter_output(cls, text: str) -> tuple[str, bool]:
        """检查 AI 的输出：疑似泄漏密钥时整条拦截；命中安全词时按 SAFETY_MODE
        整条拦截（block）或把命中部分替换为 *（mask）

        参数:
            text: 输出文字

        返回:
            tuple[str, bool]: (处理后的文字, 是否整条拦截)
        """
        if SECRET_PATTERNS.search(text) or any(s in text for s in _secrets()):
            logger.warning("AI 输出疑似包含密钥，已拦截", MODULE)
            return LEAK_NOTICE, True
        patterns = cls.patterns()
        if not any(p.search(text) for p in patterns):
            return text, False
        if str(Config.get_config(MODULE, "SAFETY_MODE") or "block") != "mask":
            logger.info("AI 输出命中安全词，已拦截", MODULE)
            return OUTPUT_NOTICE, True
        for pattern in patterns:
            text = pattern.sub(lambda m: "*" * len(m.group(0)), text)
        return text, False


class RateLimit:
    """每用户每分钟的唤醒次数限制，超限后在本周期内只提示一次"""

    _times: ClassVar[dict[str, deque[float]]] = {}
    _notified: ClassVar[dict[str, float]] = {}

    @classmethod
    def take(cls, user_id: str) -> str | None:
        """记录一次唤醒，超出 USER_RATE_LIMIT 时返回提示

        参数:
            user_id: 用户id

        返回:
            str | None: 未超限为 None；超限时为提示语，本周期已提示过则为空字符串
        """
        limit = int(Config.get_config(MODULE, "USER_RATE_LIMIT") or 0)
        if limit <= 0:
            return None
        now = time.time()
        times = cls._times.setdefault(user_id, deque())
        while times and now - times[0] >= 60:
            times.popleft()
        if len(times) < limit:
            times.append(now)
            cls._notified.pop(user_id, None)
            return None
        if now - cls._notified.get(user_id, 0) < 60:
            return ""
        cls._notified[user_id] = now
        return RATE_NOTICE


class SessionLimit:
    """每个会话每分钟的对话次数限制，超出时按到达顺序排队等待，排队已满时丢弃"""

    _times: ClassVar[dict[str, deque[float]]] = {}
    _waiting: ClassVar[dict[str, int]] = {}
    _queues: ClassVar[dict[str, asyncio.Lock]] = {}
    QUEUE = 10
    """每个会话最多排队的消息数（含等待会话锁的运行）"""

    @classmethod
    async def acquire(cls, session_id: str) -> bool:
        """占用一次 SESSION_RATE_LIMIT 名额，名额用完时按到达顺序等到有空位

        参数:
            session_id: 会话id

        返回:
            bool: 是否可以继续，排队已满时为 False
        """
        from zhenxun.services.ai.run.session import session_manager

        waiting = cls._waiting.get(session_id, 0)
        running = len(session_manager.live_tasks.get(session_id, ()))
        if waiting + running > cls.QUEUE:
            return False
        limit = int(Config.get_config(MODULE, "SESSION_RATE_LIMIT") or 0)
        if limit <= 0:
            return True
        cls._waiting[session_id] = waiting + 1
        try:
            async with cls._queues.setdefault(session_id, asyncio.Lock()):
                times = cls._times.setdefault(session_id, deque())
                while True:
                    now = time.monotonic()
                    while times and now - times[0] >= 60:
                        times.popleft()
                    if len(times) < limit:
                        times.append(now)
                        return True
                    await asyncio.sleep(60 - (now - times[0]))
        finally:
            cls._waiting[session_id] -= 1
            if not cls._waiting[session_id]:
                cls._waiting.pop(session_id, None)
                cls._queues.pop(session_id, None)
