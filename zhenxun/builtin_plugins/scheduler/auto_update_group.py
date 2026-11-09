import nonebot
from nonebot_plugin_apscheduler import scheduler

from zhenxun.services.log import logger
from zhenxun.services.message_load import should_pause_db_tasks
from zhenxun.services.tags import tag_manager
from zhenxun.utils.platform import PlatformUtils


# 自动更新群组信息
@scheduler.scheduled_job(
    "cron",
    hour=3,
    minute=1,
)
async def _():
    await _refresh_accounts("group_directory_refresh", PlatformUtils.update_group)


# 自动更新好友信息
@scheduler.scheduled_job(
    "cron",
    hour=3,
    minute=1,
)
async def _():
    await _refresh_accounts("friend_directory_refresh", PlatformUtils.update_friend)


async def _refresh_accounts(task, refresh):
    """逐账号更新目录，区分无在线账号、负载暂停和失败。"""
    from zhenxun.services.scheduler.lifecycle import core_task_run

    async with core_task_run(task) as run:
        run.update(unit="accounts", failed=0, skipped=0)
        if should_pause_db_tasks():
            run.update(status="skipped", reason="db_pressure")
            return
        bots = list(nonebot.get_bots().values())
        if not bots:
            run.update(status="skipped", reason="no_connected_bot")
            return
        for bot in bots:
            if PlatformUtils.get_platform_scope(bot) != "qq_client":
                run["skipped"] += 1
                continue
            try:
                await refresh(bot)
                run["processed"] += 1
            except Exception as error:
                run["failed"] += 1
                logger.error(f"Bot: {bot.self_id} 目录更新失败", task, e=error)
        if run["skipped"] and not run["processed"] and not run["failed"]:
            run.update(status="skipped", reason="unsupported_platform")


# 自动清理静态标签中的无效群组
@scheduler.scheduled_job(
    "cron",
    hour=4,
    minute=50,
)
async def _prune_stale_tags():
    from zhenxun.services.scheduler.lifecycle import core_task_run

    async with core_task_run("static_group_tags_cleanup") as run:
        if should_pause_db_tasks():
            run.update(status="skipped", reason="db_pressure")
            return
        result = await tag_manager.prune_stale_group_links_report()
        run.update(result, processed=result["deleted"])
