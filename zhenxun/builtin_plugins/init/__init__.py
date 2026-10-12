from pathlib import Path

import nonebot
from nonebot.adapters import Bot

nonebot.load_plugins(str(Path(__file__).parent.resolve()))


driver = nonebot.get_driver()


@driver.on_bot_connect
async def _(bot: Bot):
    from zhenxun.services.bot_group_sync import bot_group_sync

    bot_group_sync.connect(bot)


@driver.on_bot_disconnect
async def _(bot: Bot):
    from zhenxun.services.bot_group_sync import bot_group_sync

    bot_group_sync.disconnect(bot)
