from pathlib import Path

import nonebot
from nonebot.adapters import Bot

from zhenxun.services.log import logger
from zhenxun.utils.manager.priority_manager import PriorityLifecycle

from .__init_cache import register_cache_types

nonebot.load_plugins(str(Path(__file__).parent.resolve()))


driver = nonebot.get_driver()


@PriorityLifecycle.on_startup(
    priority=5,
    component_id="runtime:cache_types",
    depends_on=("management:cache_root", "runtime:reconcile_config"),
)
async def _():
    register_cache_types()
    logger.info("缓存类型注册完成")


@driver.on_bot_connect
async def _(bot: Bot):
    from zhenxun.services.bot_group_sync import bot_group_sync

    bot_group_sync.connect(bot)


@driver.on_bot_disconnect
async def _(bot: Bot):
    from zhenxun.services.bot_group_sync import bot_group_sync

    bot_group_sync.disconnect(bot)
