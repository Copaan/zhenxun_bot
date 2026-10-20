import nonebot
from nonebot.adapters import Bot
from nonebot.plugin import PluginMetadata
from tortoise.exceptions import IntegrityError, TransactionManagementError
from tortoise.transactions import in_transaction

from zhenxun.configs.utils import PluginExtraData
from zhenxun.models.bot_console import BotConsole
from zhenxun.services.db_context import database_ready
from zhenxun.services.log import logger
from zhenxun.utils.enum import PluginType
from zhenxun.utils.platform import PlatformUtils

driver = nonebot.get_driver()

__plugin_meta__ = PluginMetadata(
    name="Bot管理",
    description="指定bot对象的功能/被动开关和状态",
    usage="""
    指令:
        bot被动状态                 : bot的被动技能状态
        bot开启/关闭被动[被动名称]    : 被动技能开关
        bot开启/关闭所有被动          : 所有被动技能开关
        bot插件列表: bot插件列表状态  : bot插件列表
        bot开启/关闭所有插件          : 所有插件开关
        bot开启/关闭插件[插件名称]    : 插件开关
        bot休眠                    : bot休眠，屏蔽所有消息
        bot醒来                    : bot醒来
    """.strip(),
    extra=PluginExtraData(
        author="",
        version="0.1",
        plugin_type=PluginType.SUPERUSER,
    ).to_dict(),
)

from .bot_switch import *  # noqa: F403
from .plugin import *  # noqa: F403
from .task import *  # noqa: F403


@driver.on_bot_connect
async def init_bot_console(bot: Bot):
    """初始化Bot管理

    参数:
        bot: Bot
    """

    if not database_ready():
        logger.warning(
            "bot_console_database_not_ready: 跳过Bot管理初始化，数据库尚未就绪"
        )
        return

    platform = PlatformUtils.get_platform(bot)
    storage_bot_id = PlatformUtils.get_storage_bot_id(bot)
    try:
        bot_data, _ = await BotConsole.get_or_create(
            bot_id=storage_bot_id, platform=platform
        )
    except (IntegrityError, TransactionManagementError):
        async with in_transaction() as connection:
            bot_data = (
                await BotConsole.filter(bot_id=storage_bot_id)
                .using_db(connection)
                .get()
            )
    from zhenxun.services.cache.runtime_cache import BotMemoryCache

    await BotMemoryCache.upsert_from_model(bot_data)
    logger.info("初始化Bot管理完成...")
