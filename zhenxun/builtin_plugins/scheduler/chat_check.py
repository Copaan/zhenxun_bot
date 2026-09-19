from datetime import datetime, timedelta

import nonebot
from nonebot_plugin_apscheduler import scheduler
import pytz

from zhenxun.configs.config import Config
from zhenxun.models.chat_history import ChatHistory
from zhenxun.models.task_info import TaskInfo
from zhenxun.services.bot_group_policy import bot_group_policy_service
from zhenxun.services.log import logger
from zhenxun.services.message_load import should_pause_tasks
from zhenxun.utils.platform import PlatformUtils

Config.add_plugin_config(
    "chat_check",
    "STATUS",
    True,
    help="是否开启群组两日内未发送任何消息，关闭该群全部被动",
    default_value=True,
    type=bool,
)


@scheduler.scheduled_job(
    "cron",
    hour=4,
    minute=40,
)
async def _():
    if should_pause_tasks():
        return
    if not Config.get_config("chat_history", "FLAG"):
        logger.debug("未开启历史发言记录，过滤群组发言检测...")
        return
    if not Config.get_config("chat_check", "STATUS"):
        logger.debug("未开启群组聊天时间检查，过滤群组发言检测...")
        return
    """检测群组发言时间并禁用全部被动"""
    if modules := await TaskInfo.get_modules(load_status=None):
        for bot in nonebot.get_bots().values():
            group_list, _ = await PlatformUtils.get_group_list(bot, True)
            for group in group_list:
                try:
                    last_message = (
                        await ChatHistory.scoped_query(
                            PlatformUtils.get_platform_scope(bot),
                            group_id=group.group_id,
                            bot_id=PlatformUtils.get_storage_bot_id(bot),
                        )
                        .annotate()
                        .order_by("-create_time")
                        .first()
                    )
                    if last_message:
                        now = datetime.now(pytz.timezone("Asia/Shanghai"))
                        if now - timedelta(days=2) > last_message.create_time:
                            await bot_group_policy_service.set_features(
                                PlatformUtils.get_storage_bot_id(bot),
                                PlatformUtils.get_platform_scope(bot),
                                group.group_id,
                                modules,
                                False,
                                task=True,
                            )
                            logger.info(
                                "群组两日内未发送任何消息，关闭该群全部被动",
                                "Chat检测",
                                target=group.group_id,
                            )
                except Exception:
                    logger.error(
                        "检测群组发言时间失败...", "Chat检测", target=group.group_id
                    )
