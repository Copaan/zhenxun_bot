"""主动对话：群聊中没有唤醒 Bot 的消息按概率插话"""

import asyncio
from dataclasses import dataclass, field
from datetime import date, datetime
import random
import time
from typing import Any, ClassVar

from nonebot.adapters import Bot
from pydantic import BaseModel, Field
from tortoise.functions import Sum
from tortoise.timezone import localtime

from zhenxun.configs.config import Config
from zhenxun.models.ai_agent_run import AIAgentRun
from zhenxun.models.ai_chat_group import AIChatGroup
from zhenxun.services.log import logger
from zhenxun.utils.pydantic_compat import model_validate

from ._buffer import ChatLine
from ._data_source import MODULE

PROACTIVE_KEY = "ai_chat_proactive"
"""匹配状态中标记本条消息为主动插话的键"""


class ProactiveConfig(BaseModel):
    """群的主动对话设置，为空的项使用全局值"""

    cooldown: int | None = Field(default=None, ge=0, le=86400)
    daily_tokens: int | None = Field(default=None, ge=0)


@dataclass
class Settings:
    """群生效的主动对话设置"""

    probability: float
    cooldown: int
    daily_tokens: int


@dataclass
class GroupState:
    """群的主动对话运行状态，只在内存中"""

    last_human_at: float = 0
    last_proactive_at: float = 0
    arrived: asyncio.Event = field(default_factory=asyncio.Event)


def _cfg(key: str, default: Any) -> Any:
    value = Config.get_config(MODULE, key)
    return default if value is None else value


class Proactive:
    _states: ClassVar[dict[tuple[str, str], GroupState]] = {}
    _spent_day: ClassVar[date | None] = None
    _spent: ClassVar[dict[str, int]] = {}

    @classmethod
    def state(cls, bot_id: str, group_id: str) -> GroupState:
        """获取群的主动对话状态

        参数:
            bot_id: Bot id
            group_id: 群号

        返回:
            GroupState: 状态
        """
        key = (bot_id, group_id)
        if key not in cls._states:
            cls._states[key] = GroupState()
        return cls._states[key]

    @classmethod
    async def settings(cls, group_id: str) -> Settings | None:
        """合并群设置与全局配置，全局关闭或群停用 AI 聊天时为空

        参数:
            group_id: 群号

        返回:
            Settings | None: 生效的设置
        """
        if not _cfg("PROACTIVE_ENABLED", True):
            return None
        setting = await AIChatGroup.get_setting(group_id)
        if setting and setting.disabled:
            return None
        group = ProactiveConfig()
        if setting and isinstance(setting.proactive, dict):
            try:
                group = model_validate(ProactiveConfig, setting.proactive)
            except ValueError as e:
                logger.warning(f"群 {group_id} 的主动对话设置无效: {e}", MODULE)
        probability = (
            setting.active_probability
            if setting and setting.active_probability is not None
            else _cfg("ACTIVE_PROBABILITY", 0.0)
        )
        return Settings(
            probability=float(probability or 0),
            cooldown=group.cooldown
            if group.cooldown is not None
            else int(_cfg("PROACTIVE_COOLDOWN", 0)),
            daily_tokens=group.daily_tokens or 0,
        )

    @classmethod
    async def used_tokens(cls, group_id: str) -> tuple[int, int]:
        """今日主动插话用掉的 token，首次读取时从运行记录汇总

        参数:
            group_id: 群号

        返回:
            tuple[int, int]: (该群用量, 全部用量)
        """
        today = date.today()
        if cls._spent_day != today:
            start = (
                datetime.now()
                .astimezone()
                .replace(hour=0, minute=0, second=0, microsecond=0)
            )
            rows = (
                await AIAgentRun.filter(
                    source="interject", created_at__gte=localtime(start)
                )
                .annotate(total=Sum("tokens"))
                .group_by("group_id")
                .values("group_id", "total")
            )
            cls._spent = {str(r["group_id"]): int(r["total"] or 0) for r in rows}
            cls._spent_day = today
        return cls._spent.get(group_id, 0), sum(cls._spent.values())

    @classmethod
    def spend(cls, group_id: str, tokens: int) -> None:
        """累加今日主动插话用量

        参数:
            group_id: 群号
            tokens: token 数
        """
        if cls._spent_day == date.today():
            cls._spent[group_id] = cls._spent.get(group_id, 0) + tokens

    @classmethod
    def observe(cls, bot_id: str, group_id: str, line: ChatLine) -> None:
        """记录一条群成员消息，唤醒等待新消息的工具

        参数:
            bot_id: Bot id
            group_id: 群号
            line: 消息
        """
        st = cls.state(bot_id, group_id)
        st.last_human_at = line.at
        st.arrived.set()
        st.arrived = asyncio.Event()

    @classmethod
    def sent(cls, bot_id: str, group_id: str) -> None:
        """记录一次主动插话，用于冷却

        参数:
            bot_id: Bot id
            group_id: 群号
        """
        cls.state(bot_id, group_id).last_proactive_at = time.time()

    @classmethod
    async def roll(cls, bot: Bot, group_id: str) -> bool:
        """按主动回复概率决定是否对这条未唤醒的群消息插话，命中后还需不在冷却中
        且未超出今日预算

        参数:
            bot: Bot
            group_id: 群号

        返回:
            bool: 是否插话
        """
        cfg = await cls.settings(group_id)
        if cfg is None or random.random() >= cfg.probability:
            return False
        st = cls.state(bot.self_id, group_id)
        reason = None
        if cfg.cooldown and time.time() - st.last_proactive_at < cfg.cooldown:
            reason = "冷却中"
        else:
            group_used, total_used = await cls.used_tokens(group_id)
            total = int(_cfg("PROACTIVE_DAILY_TOKENS", 0))
            if cfg.daily_tokens and group_used >= cfg.daily_tokens:
                reason = "已达本群今日预算"
            elif total and total_used >= total:
                reason = "已达全局今日预算"
        if reason:
            logger.debug(f"跳过主动插话: {reason}", MODULE, group_id=group_id)
            return False
        return True
