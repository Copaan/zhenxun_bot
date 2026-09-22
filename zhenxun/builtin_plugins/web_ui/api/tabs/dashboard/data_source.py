import asyncio
from datetime import datetime, timedelta
import time

import nonebot
from nonebot.adapters import Bot
from nonebot.drivers import Driver
from tortoise.expressions import RawSQL
from tortoise.functions import Count

from zhenxun.configs.config import BotConfig
from zhenxun.models.bot_connect_log import BotConnectLog
from zhenxun.models.chat_history import ChatHistory
from zhenxun.models.statistics import Statistics
from zhenxun.services.log import logger
from zhenxun.utils.manager.priority_manager import PriorityLifecycle
from zhenxun.utils.platform import PlatformUtils

from ....base_model import BaseResultModel, QueryModel
from ....utils import webui_db_call
from ..main.data_source import (
    bot_live,
    get_chat_history_counts,
    get_statistics_counts,
)
from .model import (
    AllChatAndCallCount,
    BotConnectLogInfo,
    BotInfo,
    ChatCallMonthCount,
    QueryChatCallCount,
)

driver: Driver = nonebot.get_driver()


CONNECT_TIME = 0


@PriorityLifecycle.on_startup(priority=5)
async def _():
    global CONNECT_TIME
    CONNECT_TIME = int(time.time())


class ApiDataSource:
    _REMOTE_TIMEOUT_SECONDS = 5.0

    @staticmethod
    async def __remote_call(coro, operation: str, default):
        try:
            return await asyncio.wait_for(coro, ApiDataSource._REMOTE_TIMEOUT_SECONDS)
        except asyncio.CancelledError:
            raise
        except Exception as error:
            logger.warning(
                f"Dashboard 远端查询失败，已使用降级值: {operation}",
                "WebUi",
                e=error,
            )
            return default

    @classmethod
    async def __build_bot_info(cls, bot: Bot) -> BotInfo:
        """构建Bot信息

        参数:
            bot: Bot

        返回:
            BotInfo: Bot信息
        """
        now = datetime.now()
        platform = PlatformUtils.get_platform(bot) or ""
        if platform == "qq":
            login_info = await cls.__remote_call(
                bot.get_login_info(), "bot.login_info", {}
            )
            nickname = login_info.get("nickname") or bot.self_id
            ava_url = (
                PlatformUtils.get_user_avatar_url(
                    bot.self_id, "qq", BotConfig.get_qbot_uid(bot.self_id)
                )
                or ""
            )
        else:
            nickname = bot.self_id
            ava_url = ""
        bot_info = BotInfo(
            self_id=bot.self_id, nickname=nickname, ava_url=ava_url, platform=platform
        )
        group, friend = await asyncio.gather(
            cls.__remote_call(
                PlatformUtils.get_group_list(bot, True), "bot.group_list", ([], None)
            ),
            cls.__remote_call(
                PlatformUtils.get_friend_list(bot), "bot.friend_list", ([], None)
            ),
        )
        bot_info.group_count = len(group[0]) if group and group[0] else 0
        bot_info.friend_count = len(friend[0]) if friend and friend[0] else 0
        day_start = now - timedelta(hours=now.hour, minutes=now.minute)
        bot_info.day_call, bot_info.received_messages = await asyncio.gather(
            webui_db_call(
                Statistics.filter(
                    create_time__gte=day_start,
                    bot_id=bot.self_id,
                ).count(),
                "Dashboard.bot_day_call",
            ),
            webui_db_call(
                ChatHistory.filter(
                    bot_id=bot_info.self_id,
                    create_time__gte=day_start,
                ).count(),
                "Dashboard.bot_received_messages",
            ),
        )
        bot_info.connect_time = bot_live.get(bot.self_id) or 0
        if bot_info.connect_time:
            connect_date = datetime.fromtimestamp(CONNECT_TIME)
            bot_info.connect_date = connect_date.strftime("%Y-%m-%d %H:%M:%S")
        return bot_info

    @classmethod
    async def get_bot_list(cls) -> list[BotInfo]:
        """获取bot列表

        返回:
            list[BotInfo]: Bot列表
        """
        return list(
            await asyncio.gather(
                *(cls.__build_bot_info(bot) for bot in nonebot.get_bots().values())
            )
        )

    @classmethod
    async def get_chat_and_call_count(cls, bot_id: str | None) -> QueryChatCallCount:
        """获取今日聊天和调用次数

        参数:
            bot_id: bot id

        返回:
            QueryChatCallCount: 数据内容
        """
        chat_counts, call_counts = await asyncio.gather(
            get_chat_history_counts(bot_id, "Dashboard.chat_counts"),
            get_statistics_counts(bot_id, "Dashboard.call_counts"),
        )
        return QueryChatCallCount(
            chat_num=chat_counts["total"],
            chat_day=chat_counts["day"],
            call_num=call_counts["total"],
            call_day=call_counts["day"],
        )

    @classmethod
    async def get_all_chat_and_call_count(
        cls, bot_id: str | None
    ) -> AllChatAndCallCount:
        """获取全部聊天和调用记录

        参数:
            bot_id: bot id

        返回:
            AllChatAndCallCount: 数据内容
        """
        chat_counts, call_counts = await asyncio.gather(
            get_chat_history_counts(bot_id, "Dashboard.chat_counts"),
            get_statistics_counts(bot_id, "Dashboard.call_counts"),
        )
        return AllChatAndCallCount(
            chat_week=chat_counts["week"],
            chat_month=chat_counts["month"],
            chat_year=chat_counts["year"],
            call_week=call_counts["week"],
            call_month=call_counts["month"],
            call_year=call_counts["year"],
        )

    @classmethod
    async def get_chat_and_call_month(cls, bot_id: str | None) -> ChatCallMonthCount:
        """获取一个月内的调用/消息记录次数，并根据日期对数据填充0

        参数:
            bot_id: bot id

        返回:
            ChatCallMonthCount: 数据内容
        """
        now = datetime.now()
        filter_date = now - timedelta(days=30, hours=now.hour, minutes=now.minute)
        chat_query = ChatHistory
        call_query = Statistics
        if bot_id:
            chat_query = chat_query.filter(bot_id=bot_id)
            call_query = call_query.filter(bot_id=bot_id)
        chat_date_list = await webui_db_call(
            chat_query.filter(create_time__gte=filter_date)
            .annotate(date=RawSQL("DATE(create_time)"), count=Count("id"))
            .group_by("date")
            .values("date", "count"),
            "Dashboard.chat_month_series",
        )
        call_date_list = await webui_db_call(
            call_query.filter(create_time__gte=filter_date)
            .annotate(date=RawSQL("DATE(create_time)"), count=Count("id"))
            .group_by("date")
            .values("date", "count"),
            "Dashboard.call_month_series",
        )
        date_list = []
        chat_count_list = []
        call_count_list = []
        chat_date2cnt = {str(date["date"]): date["count"] for date in chat_date_list}
        call_date2cnt = {str(date["date"]): date["count"] for date in call_date_list}
        date = now.date()
        for _ in range(30):
            if str(date) in chat_date2cnt:
                chat_count_list.append(chat_date2cnt[str(date)])
            else:
                chat_count_list.append(0)
            if str(date) in call_date2cnt:
                call_count_list.append(call_date2cnt[str(date)])
            else:
                call_count_list.append(0)
            date_list.append(str(date)[5:])
            date -= timedelta(days=1)
        chat_count_list.reverse()
        call_count_list.reverse()
        date_list.reverse()
        return ChatCallMonthCount(
            chat=chat_count_list, call=call_count_list, date=date_list
        )

    @classmethod
    async def get_connect_log(cls, query: QueryModel) -> BaseResultModel:
        """获取bot连接日志

        参数:
            query: 查询模型

        返回:
            BaseResultModel: 数据内容
        """
        total = await BotConnectLog.all().count()
        if total % query.size:
            total += 1
        data = (
            await BotConnectLog.all()
            .order_by("-id")
            .offset((query.index - 1) * query.size)
            .limit(query.size)
        )
        result_list = []
        for v in data:
            v.connect_time = v.connect_time.replace(tzinfo=None).replace(microsecond=0)
            result_list.append(
                BotConnectLogInfo(
                    bot_id=v.bot_id, connect_time=v.connect_time, type=v.type
                )
            )
        return BaseResultModel(total=total, data=result_list)
