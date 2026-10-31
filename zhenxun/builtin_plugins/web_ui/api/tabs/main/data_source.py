import asyncio
from datetime import datetime, timedelta
import time
from typing import Any

import nonebot
from nonebot.adapters import Bot
from nonebot.drivers import Driver
from tortoise.expressions import Q
from tortoise.functions import Count

from zhenxun.adapters.qq_official.diagnostics import public_identity, safe_avatar_url
from zhenxun.models.bot_connect_log import BotConnectLog
from zhenxun.models.bot_console import BotConsole
from zhenxun.models.chat_history import ChatHistory
from zhenxun.models.group_console import GroupConsole
from zhenxun.models.plugin_info import PluginInfo
from zhenxun.models.statistics import Statistics
from zhenxun.services.log import logger
from zhenxun.utils.platform import PlatformUtils

from ....config import AVA_URL, GROUP_AVA_URL, QueryDateType
from ....utils import webui_db_call, webui_platform
from .model import (
    ActiveGroup,
    BaseInfo,
    BotBlockModule,
    ChatCallCount,
    HotPlugin,
    QueryCount,
    TemplateBaseInfo,
)

driver: Driver = nonebot.get_driver()


def window_starts(now: datetime) -> dict[str, datetime]:
    """统计时间窗口起点

    参数:
        now: 当前时间

    返回:
        dict[str, datetime]: 今日、近7天、近30天、近一年的起始时间
    """
    today = now.replace(hour=0, minute=0, second=0, microsecond=0)
    return {
        "day": today,
        "week": today - timedelta(days=6),
        "month": today - timedelta(days=29),
        "year": today - timedelta(days=364),
    }


async def get_record_counts(
    model: Any, bot_id: str | None, operation: str
) -> QueryCount:
    """一次聚合查询获取总数与各时间窗口数量

    参数:
        model: ChatHistory、Statistics 或它们的查询
        bot_id: 存储 bot id，为空时统计全部
        operation: 数据库调用标识

    返回:
        QueryCount: 总数与今日、近7天、近30天、近一年数量
    """
    query = model.filter(bot_id=bot_id) if bot_id else model
    starts = window_starts(datetime.now())
    result = await webui_db_call(
        query.annotate(
            total_count=Count("id"),
            **{
                f"{key}_count": Count("id", _filter=Q(create_time__gte=start))
                for key, start in starts.items()
            },
        )
        .limit(1)
        .values("total_count", *(f"{key}_count" for key in starts)),
        operation,
    )
    row = result[0] if isinstance(result, list) and result else result
    row = row if isinstance(row, dict) else {}
    return QueryCount(
        num=int(row.get("total_count") or 0),
        **{key: int(row.get(f"{key}_count") or 0) for key in starts},
    )


async def top_plugins(query, operation: str) -> list[HotPlugin]:
    """获取调用次数最多的 5 个插件

    参数:
        query: Statistics 查询
        operation: 数据库调用标识

    返回:
        list[HotPlugin]: 插件模块、名称与调用次数
    """
    data_list = await webui_db_call(
        query.annotate(count=Count("id"))
        .group_by("plugin_name")
        .order_by("-count")
        .limit(5)
        .values_list("plugin_name", "count"),
        operation,
    )
    plugins = await webui_db_call(
        PluginInfo.get_plugins(
            load_status=None,
            filter_parent=False,
            module__in=[x[0] for x in data_list],
        ),
        f"{operation}_names",
    )
    module2name = {p.module: p.name for p in plugins}
    return [
        HotPlugin(module=module, name=module2name.get(module) or module, count=count)
        for module, count in data_list
    ]


class BotLive:
    def __init__(self):
        self._data = {}

    def add(self, bot_id: str):
        self._data[bot_id] = int(time.time())

    def get(self, bot_id: str) -> int | None:
        return self._data.get(bot_id)

    def remove(self, bot_id: str):
        if bot_id in self._data:
            del self._data[bot_id]


bot_live = BotLive()


def _bot_identity(bot: Bot) -> tuple[str, str, str, str, dict[str, bool]]:
    runtime_bot_id = str(bot.self_id)
    platform = webui_platform(bot)
    storage_bot_id = PlatformUtils.get_storage_bot_id(bot)
    bot_key = f"{platform}:{runtime_bot_id}"
    onebot = platform == "onebot_v11"
    capabilities = {
        "dashboard": True,
        "friend_list": onebot,
        "group_list": onebot,
        "chat": onebot,
        "bot_manage": onebot,
    }
    return bot_key, runtime_bot_id, storage_bot_id, platform, capabilities


@driver.on_bot_connect
async def _(bot: Bot):
    bot_live.add(str(bot.self_id))


@driver.on_bot_disconnect
async def _(bot: Bot):
    bot_live.remove(str(bot.self_id))


class ApiDataSource:
    @classmethod
    async def __build_bot_info(cls, bot: Bot) -> TemplateBaseInfo:
        """构建bot信息

        参数:
            bot: bot实例

        返回:
            TemplateBaseInfo: bot信息
        """
        bot_key, runtime_id, storage_id, platform, capabilities = _bot_identity(bot)
        login_info = None
        if platform == "onebot_v11":
            try:
                login_info = await bot.get_login_info()
            except Exception as e:
                logger.warning("调用接口get_login_info失败", "WebUi", e=e)
        try:
            self_info = getattr(bot, "self_info", None)
        except Exception:
            self_info = None
        nickname = (
            login_info.get("nickname")
            if isinstance(login_info, dict) and login_info.get("nickname")
            else getattr(self_info, "username", None) or runtime_id
        )
        cached_identity = (
            public_identity(runtime_id) if platform == "qq_official" else None
        )
        if platform == "qq_official" and nickname == runtime_id and cached_identity:
            nickname = cached_identity.username or runtime_id
        avatar_url = (
            AVA_URL.format(runtime_id)
            if platform == "onebot_v11"
            else safe_avatar_url(getattr(self_info, "avatar", None))
            or (cached_identity.avatar_url if cached_identity else None)
            or ""
        )
        return TemplateBaseInfo(
            bot=bot,
            self_id=runtime_id,
            nickname=str(nickname),
            ava_url=avatar_url,
            bot_key=bot_key,
            runtime_bot_id=runtime_id,
            storage_bot_id=storage_id,
            adapter=str(bot.adapter.get_name()),
            platform=platform,
            capabilities=capabilities,
        )

    @classmethod
    async def __init_bot_base_data(cls, select_bot: TemplateBaseInfo):
        """初始化bot的基础数据

        参数:
            select_bot: bot
        """
        # 群聊数量
        try:
            if not select_bot.capabilities.get("group_list"):
                raise NotImplementedError
            select_bot.group_count = len(
                (await PlatformUtils.get_group_list(select_bot.bot, True))[0]
            )
            # 好友数量
            select_bot.friend_count = len(
                (await PlatformUtils.get_friend_list(select_bot.bot))[0]
            )
        except NotImplementedError:
            select_bot.group_count = 0
            select_bot.friend_count = 0
        except Exception as e:
            logger.warning("获取bot好友/群组数量失败...", "WebUi", e=e)
            select_bot.group_count = 0
            select_bot.friend_count = 0
        select_bot.status = await BotConsole.get_bot_status(select_bot.storage_bot_id)
        # 连接时间
        select_bot.connect_time = bot_live.get(select_bot.runtime_bot_id) or 0
        if select_bot.connect_time:
            connect_date = datetime.fromtimestamp(select_bot.connect_time)
            select_bot.connect_date = connect_date.strftime("%Y-%m-%d %H:%M:%S")
        select_bot.connect_count = await webui_db_call(
            BotConnectLog.filter(bot_id=select_bot.storage_bot_id, type=1).count(),
            "Main.connect_count",
        )

    @classmethod
    async def get_base_info(cls, bot_id: str | None) -> list[BaseInfo] | None:
        """获取bot信息

        参数:
            bot_id: bot id

        返回:
            list[BaseInfo] | None: bot列表
        """
        bots = nonebot.get_bots()
        if not bots:
            return None
        select_bot: BaseInfo
        bot_list = [await cls.__build_bot_info(bot) for _, bot in bots.items()]
        # 获取指定qq号的bot信息，若无指定   则获取第一个
        if _bl := [b for b in bot_list if b.bot_key == bot_id]:
            select_bot = _bl[0]
        elif _bl := [b for b in bot_list if b.self_id == bot_id]:
            select_bot = _bl[0]
        else:
            select_bot = bot_list[0]
        await cls.__init_bot_base_data(select_bot)
        for bot in bot_list:
            bot.bot = None  # type: ignore
        select_bot.is_select = True
        return [BaseInfo(**e.to_dict()) for e in bot_list]

    @classmethod
    async def get_chat_and_call_count(cls, bot_id: str | None) -> ChatCallCount:
        """获取聊天与调用次数

        参数:
            bot_id: 存储 bot id

        返回:
            ChatCallCount: 聊天与调用的总数及各时间窗口数量
        """
        chat, call = await asyncio.gather(
            get_record_counts(
                ChatHistory.filter(is_bot=False), bot_id, "Main.chat_counts"
            ),
            get_record_counts(Statistics, bot_id, "Main.call_counts"),
        )
        return ChatCallCount(chat=chat, call=call)

    @classmethod
    def __get_query(
        cls,
        base_query: Any,
        date_type: QueryDateType | None = None,
        bot_id: str | None = None,
    ):
        """构建日期查询条件

        参数:
            date_type: 日期类型.
            bot_id: bot id.
        """
        query = base_query
        if bot_id:
            query = query.filter(bot_id=bot_id)
        if date_type:
            query = query.filter(
                create_time__gte=window_starts(datetime.now())[date_type.value]
            )
        return query

    @classmethod
    async def get_active_group(
        cls, date_type: QueryDateType | None = None, bot_id: str | None = None
    ) -> list[ActiveGroup]:
        """获取活跃群组

        参数:
            date_type: 日期类型.
            bot_id: bot id.

        返回:
            list[ActiveGroup]: 活跃群组列表
        """
        query = cls.__get_query(ChatHistory.filter(is_bot=False), date_type, bot_id)
        data_list = await webui_db_call(
            query.annotate(count=Count("id"))
            .filter(group_id__not_isnull=True)
            .group_by("group_id")
            .order_by("-count")
            .limit(5)
            .values_list("group_id", "count"),
            "Main.active_group",
        )
        id2name = {}
        if data_list:
            if info_list := await webui_db_call(
                GroupConsole.filter(group_id__in=[x[0] for x in data_list]).all(),
                "Main.active_group_names",
            ):
                for group_info in info_list:
                    id2name[group_info.group_id] = group_info.group_name
        return [
            ActiveGroup(
                group_id=data[0],
                name=id2name.get(data[0]) or data[0],
                chat_num=data[1],
                ava_img=GROUP_AVA_URL.format(data[0], data[0]),
            )
            for data in data_list
        ]

    @classmethod
    async def get_hot_plugin(
        cls, date_type: QueryDateType | None = None, bot_id: str | None = None
    ) -> list[HotPlugin]:
        """获取热门插件

        参数:
            date_type: 日期类型.
            bot_id: bot id.

        返回:
            list[HotPlugin]: 热门插件列表
        """
        return await top_plugins(
            cls.__get_query(Statistics, date_type, bot_id), "Main.hot_plugin"
        )

    @classmethod
    async def get_bot_block_module(cls, bot_id: str) -> BotBlockModule | None:
        """获取bot层面的禁用模块

        参数:
            bot_id: bot id

        返回:
            BotBlockModule | None: 数据内容
        """
        from zhenxun.services.plugin_policy import plugin_policy_service

        account = await plugin_policy_service.get_account(bot_id)
        catalog = await plugin_policy_service.catalog()
        all_plugins = [
            {"module": item["module"], "name": item["name"]}
            for item in catalog["plugins"]
        ]
        all_task = [
            {"module": item["module"], "name": item["name"]}
            for item in catalog["tasks"]
        ]
        return BotBlockModule(
            bot_id=account["bot_id"],
            block_tasks=account["block_tasks"],
            block_plugins=account["block_plugins"],
            all_plugins=all_plugins,
            all_tasks=all_task,
            policy_mode=account["mode"],
            policy_name=(account.get("policy") or {}).get("name"),
        )
