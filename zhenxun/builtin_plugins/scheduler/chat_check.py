from datetime import timedelta
import time

import nonebot
from nonebot_plugin_apscheduler import scheduler

from zhenxun.configs.config import Config
from zhenxun.models.chat_history import ChatHistory
from zhenxun.models.task_info import TaskInfo
from zhenxun.services.bot_group_policy import bot_group_policy_service
from zhenxun.services.log import logger
from zhenxun.services.message_load import should_pause_db_tasks
from zhenxun.utils.platform import PlatformUtils
from zhenxun.utils.time_utils import TimeUtils

_INACTIVE_SECONDS = 48 * 60 * 60

Config.add_plugin_config(
    "chat_check",
    "STATUS",
    True,
    help="连续观察48小时无成员发言时暂停群被动，成员再次发言后恢复原状态",
    default_value=True,
    type=bool,
)


def _inactive_guard(bot, scope, group_id, last_message_at, channel_id=None):
    """以当前连接和群活动修订构造不活跃判定，证据不足时返回空。"""
    from zhenxun.builtin_plugins.hooks.auth_hook import connection_epochs

    epoch = connection_epochs.get(bot, scope)
    activity = connection_epochs.observe_group(bot, scope, group_id, channel_id)
    if epoch is None or activity is None:
        return None
    revision = activity.revision
    if last_message_at.tzinfo is None:
        last_message_at = last_message_at.replace(tzinfo=TimeUtils.zone())
    latest = max(last_message_at, activity.last_received_at or last_message_at)

    def still_inactive():
        current = connection_epochs.get(bot, scope)
        if (
            current is not epoch
            or epoch.groups.get((str(group_id), str(channel_id or ""))) is not activity
        ):
            return False
        if activity.revision != revision:
            return False
        observed = max(
            epoch.connected_monotonic,
            activity.observed_since,
            activity.last_received_monotonic
            if activity.last_received_monotonic is not None
            else activity.observed_since,
        )
        return (
            not should_pause_db_tasks()
            and bool(Config.get_config("chat_history", "FLAG"))
            and bool(Config.get_config("chat_check", "STATUS"))
            and time.monotonic() - observed > _INACTIVE_SECONDS
            and TimeUtils.now() - latest > timedelta(seconds=_INACTIVE_SECONDS)
        )

    return still_inactive if still_inactive() else None


@scheduler.scheduled_job("cron", hour=4, minute=40, max_instances=1, coalesce=True)
async def check_inactive_groups():
    """暂停已连续观察且超过48小时无人发言的群被动，保留恢复快照。"""
    from zhenxun.services.scheduler.lifecycle import core_task_run

    async with core_task_run("inactive_group_check") as run:
        run.update(
            failed=0, examined=0, no_history=0, not_eligible=0, empty_directories=0
        )
        if should_pause_db_tasks():
            run.update(status="skipped", reason="db_pressure")
            return
        if not Config.get_config("chat_history", "FLAG"):
            run.update(status="skipped", reason="history_disabled")
            logger.debug("未开启历史发言记录，过滤群组发言检测...")
            return
        if not Config.get_config("chat_check", "STATUS"):
            run.update(status="skipped", reason="config_disabled")
            logger.debug("未开启群组聊天时间检查，过滤群组发言检测...")
            return
        modules = await TaskInfo.get_modules(load_status=None)
        if not modules:
            run.update(status="skipped", reason="no_passive_tasks")
            return
        from zhenxun.builtin_plugins.hooks.auth_hook import connection_epochs

        bots = list(nonebot.get_bots().values())
        if not bots:
            run.update(status="skipped", reason="no_connected_bot")
            return
        for bot in bots:
            scope = PlatformUtils.get_platform_scope(bot)
            bot_id = PlatformUtils.get_storage_bot_id(bot)
            try:
                group_list, _ = await PlatformUtils.get_group_list(bot, True)
            except Exception as error:
                run["failed"] += 1
                logger.error(
                    "获取群列表失败，跳过该账号的不活跃检查",
                    "Chat检测",
                    target=bot_id,
                    e=error,
                )
                continue
            if not group_list:
                run["empty_directories"] += 1
            for group in group_list:
                run["examined"] += 1
                group_id = str(group.group_id)
                channel_id = getattr(group, "channel_id", None)
                try:
                    connection_epochs.observe_group(bot, scope, group_id, channel_id)
                    last_message = await (
                        ChatHistory.scoped_query(
                            scope, group_id=group_id, bot_id=bot_id, is_bot=False
                        )
                        .order_by("-create_time")
                        .first()
                    )
                    if last_message is None:
                        run["no_history"] += 1
                        continue
                    guard = _inactive_guard(
                        bot, scope, group_id, last_message.create_time, channel_id
                    )
                    if guard is None:
                        run["not_eligible"] += 1
                        continue
                    if await bot_group_policy_service.pause_inactive_tasks(
                        bot_id,
                        scope,
                        group_id,
                        modules,
                        channel_id=channel_id,
                        guard=guard,
                    ):
                        run["processed"] += 1
                        logger.info(
                            "群组48小时无成员发言，已暂停被动；再次发言后恢复原状态",
                            "Chat检测",
                            target=group_id,
                        )
                except Exception as error:
                    run["failed"] += 1
                    logger.error(
                        "检测群组发言时间失败", "Chat检测", target=group_id, e=error
                    )
