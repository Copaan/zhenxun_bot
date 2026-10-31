from collections.abc import Iterator
from datetime import datetime, timedelta
from pathlib import Path

from tortoise.expressions import RawSQL
from tortoise.functions import Count

from zhenxun.configs.path_config import LOG_PATH
from zhenxun.models.bot_connect_log import BotConnectLog
from zhenxun.models.chat_history import ChatHistory
from zhenxun.models.statistics import Statistics
from zhenxun.utils.common_utils import SqlUtils

from ....base_model import BaseResultModel, QueryModel
from ....utils import webui_db_call
from ..main.data_source import window_starts
from .model import (
    BotConnectLogInfo,
    ChatCallMonthCount,
)


class ApiDataSource:
    @classmethod
    async def get_chat_and_call_month(cls, bot_id: str | None) -> ChatCallMonthCount:
        """获取一个月内的调用/消息记录次数，并根据日期对数据填充0

        参数:
            bot_id: bot id

        返回:
            ChatCallMonthCount: 数据内容
        """
        now = datetime.now()
        filter_date = window_starts(now)["month"]
        chat_query = ChatHistory.filter(is_bot=False)
        call_query = Statistics
        if bot_id:
            chat_query = chat_query.filter(bot_id=bot_id)
            call_query = call_query.filter(bot_id=bot_id)
        chat_date_list = await webui_db_call(
            chat_query.filter(create_time__gte=filter_date)
            .annotate(
                date=RawSQL(SqlUtils.local_date_sql("create_time")),
                count=Count("id"),
            )
            .group_by("date")
            .values("date", "count"),
            "Dashboard.chat_month_series",
        )
        call_date_list = await webui_db_call(
            call_query.filter(create_time__gte=filter_date)
            .annotate(
                date=RawSQL(SqlUtils.local_date_sql("create_time")),
                count=Count("id"),
            )
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

    @classmethod
    def today_log_file(cls) -> Path | None:
        """获取当天的运行日志文件

        返回:
            Path | None: 日志文件，当天尚未写入日志时为 None
        """
        path = LOG_PATH / f"{datetime.now():%Y-%m-%d}.log"
        return path if path.is_file() else None

    @classmethod
    def read_log_file(cls, path: Path, size: int) -> Iterator[bytes]:
        """分块读取日志文件的前 size 字节，读取期间新写入的内容不会被包含

        参数:
            path: 日志文件
            size: 读取的字节数

        返回:
            Iterator[bytes]: 文件内容分块
        """
        with path.open("rb") as file:
            while size > 0:
                chunk = file.read(min(size, 64 * 1024))
                if not chunk:
                    break
                size -= len(chunk)
                yield chunk
