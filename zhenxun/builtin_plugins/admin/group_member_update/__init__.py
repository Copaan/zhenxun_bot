import asyncio
import random

import nonebot
from nonebot.adapters import Bot
from nonebot.permission import SUPERUSER
from nonebot.plugin import PluginMetadata
from nonebot_plugin_alconna import Alconna, Arparma, on_alconna
from nonebot_plugin_apscheduler import scheduler
from nonebot_plugin_session import EventSession

from zhenxun.configs.utils import PluginExtraData
from zhenxun.services.bot_group_sync import MemberUpdateManage
from zhenxun.services.log import logger
from zhenxun.services.message_load import should_pause_db_tasks
from zhenxun.services.tags import tag_manager
from zhenxun.utils.enum import PluginType
from zhenxun.utils.message import MessageUtils
from zhenxun.utils.platform import PlatformUtils
from zhenxun.utils.rules import admin_check, ensure_group
from zhenxun.utils.time_utils import TimeUtils

__plugin_meta__ = PluginMetadata(
    name="更新群组成员列表",
    description="更新群组成员列表",
    usage="""
    更新群组成员的基本信息
    指令：
        更新群组成员信息
    """.strip(),
    extra=PluginExtraData(
        author="HibiKier",
        version="0.1",
        plugin_type=PluginType.SUPER_AND_ADMIN,
        admin_level=1,
    ).to_dict(),
)

_matcher = on_alconna(
    Alconna("更新群组成员信息"),
    rule=admin_check(1) & ensure_group,
    priority=5,
    block=True,
)


_update_all_matcher = on_alconna(
    Alconna("更新所有群组信息"),
    permission=SUPERUSER,
    priority=1,
    block=True,
)


async def _update_all_groups_task(bot: Bot, session: EventSession):
    """
    在后台执行所有群组的更新任务，并向超级用户发送最终报告。
    """
    success_count = 0
    fail_count = 0
    total_count = 0
    bot_id = bot.self_id

    logger.info(f"Bot {bot_id}: 开始执行所有群组信息更新任务...", "更新所有群组")
    try:
        scene_map = await MemberUpdateManage.build_scene_map(bot)
        platform = PlatformUtils.get_platform(bot)
        group_ids = list(scene_map.keys())
        total_count = len(group_ids)
        for i, group_id in enumerate(group_ids):
            try:
                logger.debug(
                    f"Bot {bot_id}: 正在更新第 {i + 1}/{total_count} 个群组: "
                    f"{group_id}",
                    "更新所有群组",
                )
                result = await MemberUpdateManage.run_update(
                    bot,
                    group_id,
                    scene_map=scene_map,
                    platform=platform,
                )
                if result == MemberUpdateManage.SUCCESS_MESSAGE:
                    success_count += 1
                else:
                    fail_count += 1
            except Exception as e:
                fail_count += 1
                logger.error(
                    f"Bot {bot_id}: 更新群组 {group_id} 信息失败",
                    "更新所有群组",
                    e=e,
                )
            await asyncio.sleep(random.uniform(1.5, 3.0))
    except Exception as e:
        logger.error(f"Bot {bot_id}: 获取群组列表失败，任务中断", "更新所有群组", e=e)
        await PlatformUtils.send_superuser(
            bot,
            f"Bot {bot_id} 更新所有群组信息任务失败：无法获取群组列表。",
            session.id1,
        )
        return

    await tag_manager._invalidate_cache()
    summary_message = (
        f"🤖 Bot {bot_id} 所有群组信息更新任务完成！\n"
        f"总计群组: {total_count}\n"
        f"✅ 成功: {success_count}\n"
        f"❌ 失败: {fail_count}"
    )
    logger.info(summary_message.replace("\n", " | "), "更新所有群组")
    await PlatformUtils.send_superuser(bot, summary_message, session.id1)


@_update_all_matcher.handle()
async def _(bot: Bot, session: EventSession):
    await MessageUtils.build_message(
        "已开始在后台更新所有群组信息，过程可能需要几分钟到几十分钟，完成后将私聊通知您。"
    ).send(reply_to=True)
    asyncio.create_task(_update_all_groups_task(bot, session))  # noqa: RUF006


@_matcher.handle()
async def _(bot: Bot, session: EventSession, arparma: Arparma):
    if not (gid := session.id3 or session.id2):
        await MessageUtils.build_message("群组id为空...").send()
        return
    logger.info("更新群组成员信息", arparma.header_result, session=session)
    result = await MemberUpdateManage.run_update(bot, gid)
    await MessageUtils.build_message(result or "更新已完成").finish(reply_to=True)
    await tag_manager._invalidate_cache()


@scheduler.scheduled_job(
    "cron",
    hour=3,
    minute=0,
    timezone=TimeUtils.zone(),
    max_instances=1,
    coalesce=True,
)
async def _nightly_full_refresh():
    from zhenxun.services.scheduler.lifecycle import core_task_run

    async with core_task_run("nightly_group_members") as run:
        if should_pause_db_tasks():
            run.update(status="skipped", reason="db_pressure")
            return
        bots = list(nonebot.get_bots().values())
        if not bots:
            run.update(status="skipped", reason="no_connected_bot")
            return
        run.update(failed=0, skipped=0, reason_counts={})
        for bot in bots:
            if should_pause_db_tasks():
                run.update(
                    status="partial" if run["processed"] else "skipped",
                    reason="db_pressure",
                )
                break
            platform = PlatformUtils.get_platform(bot)
            if platform != "qq":
                run["reason_counts"]["unsupported_platform"] = (
                    run["reason_counts"].get("unsupported_platform", 0) + 1
                )
                continue
            try:
                scene_map = await MemberUpdateManage.build_scene_map(bot)
                if not scene_map:
                    run["reason_counts"]["empty_group_directory"] = (
                        run["reason_counts"].get("empty_group_directory", 0) + 1
                    )
                    continue
                for group_id in scene_map:
                    if should_pause_db_tasks():
                        run.update(
                            status="partial" if run["processed"] else "skipped",
                            reason="db_pressure",
                        )
                        break
                    try:
                        result = await MemberUpdateManage.run_update(
                            bot,
                            group_id,
                            scene_map=scene_map,
                            platform=platform,
                            only_if_due=True,
                        )
                        if result == MemberUpdateManage.SUCCESS_MESSAGE:
                            run["processed"] += 1
                        elif result is None:
                            run["skipped"] += 1
                            run["reason_counts"]["already_synced_today"] = (
                                run["reason_counts"].get("already_synced_today", 0) + 1
                            )
                        else:
                            run["failed"] += 1
                    except Exception as error:
                        run["failed"] += 1
                        logger.error(
                            f"Bot: {bot.self_id} 夜间更新群组成员信息失败",
                            target=group_id,
                            e=error,
                        )
            except Exception as error:
                run["failed"] += 1
                logger.error(f"Bot: {bot.self_id} 夜间更新群组信息", e=error)
        if run["processed"]:
            await tag_manager._invalidate_cache()
