import asyncio
from pathlib import Path

import nonebot
from nonebot.adapters import Bot
from nonebot.adapters.onebot.v11.exception import NetworkError

from zhenxun.models.group_console import GroupConsole
from zhenxun.services.log import logger
from zhenxun.utils.manager.priority_manager import PriorityLifecycle
from zhenxun.utils.platform import PlatformUtils

from .__init_cache import register_cache_types

nonebot.load_plugins(str(Path(__file__).parent.resolve()))


driver = nonebot.get_driver()


@PriorityLifecycle.on_startup(priority=5)
async def _():
    register_cache_types()
    logger.info("缓存类型注册完成")


@driver.on_bot_connect
async def _(bot: Bot):
    """同步 Bot 已存在的群组到 GroupConsole，并清理已退出的群

    参数:
        bot: Bot
    """
    if PlatformUtils.get_platform_scope(bot) != "qq_client":
        return

    logger.debug(f"更新Bot: {bot.self_id} 的群认证...", "群认证同步")

    current_group_list = []
    last_error = None
    for delay in (0, 1, 3):
        if delay:
            await asyncio.sleep(delay)
        try:
            current_group_list, _ = await asyncio.wait_for(
                PlatformUtils.get_group_list(bot), timeout=5.0
            )
            last_error = None
        except (NetworkError, asyncio.TimeoutError) as error:
            last_error = type(error).__name__
        if current_group_list:
            break

    if not current_group_list:
        if last_error:
            logger.warning(
                f"Bot: {bot.self_id} 群列表查询失败: {last_error}；"
                "已有限重试，保留已有群认证，等待重连或群消息自愈。",
                "群认证同步",
            )
        else:
            logger.info(
                f"Bot: {bot.self_id} 群列表仍为空；可能尚未就绪或账号没有群组，"
                "本次不写入数据库，保留已有群认证。",
                "群认证同步",
            )
        return

    db_group_list: list[str] = await GroupConsole.all().values_list(
        "group_id", flat=True
    )  # pyright: ignore[reportAssignmentType]
    db_group_ids = set(db_group_list)

    create_list = []
    for group in current_group_list:
        if group.group_id not in db_group_ids:
            group.group_flag = 1
            create_list.append(group)

    if create_list:
        await GroupConsole.bulk_create(create_list, 10)
        task_modules = await GroupConsole._get_task_modules(default_status=False)
        plugin_modules = await GroupConsole._get_plugin_modules(default_status=False)
        new_ids = [g.group_id for g in create_list]
        fresh = await GroupConsole.filter(group_id__in=new_ids).all()
        if task_modules or plugin_modules:
            for group in fresh:
                await GroupConsole._update_modules(group, task_modules, plugin_modules)
        from zhenxun.services.cache.runtime_cache import GroupMemoryCache

        for group in fresh:
            await GroupMemoryCache.upsert_from_model(group)

    logger.info(
        f"更新Bot: {bot.self_id} 的群认证完成，共创建 {len(create_list)} 条数据，",
        "群认证同步",
    )
