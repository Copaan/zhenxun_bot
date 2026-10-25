from typing import cast

from nonebot.params import Depends
from nonebot_plugin_alconna import (
    AlconnaMatch,
    AlconnaMatches,
    Arparma,
    Match,
)
from nonebot_plugin_uninfo import Uninfo

from zhenxun.configs.config import Config
from zhenxun.models.scheduled_job import ScheduledJob
from zhenxun.services import scheduler_manager
from zhenxun.services.scheduler.registry import scheduler_registry
from zhenxun.services.scheduler.types import BaseTrigger
from zhenxun.utils.message import MessageUtils

from .commands import schedule_cmd
from .data_source import scheduler_admin_service
from .dependencies import (
    GetBotId,
    GetFinalPermission,
    GetTargeter,
    GetTriggerInfo,
    GetValidatedJobKwargs,
    RequireSchedule,
    ResolveTargets,
    _parse_trigger_from_arparma,
)


@schedule_cmd.assign("查看")
async def handle_view(
    page: Match[int] = AlconnaMatch("page"),
    targeter=Depends(GetTargeter),
):
    """处理 '查看' 子命令"""
    current_page = page.result if page.available else 1

    result = await scheduler_admin_service.get_schedules_view(
        filters=targeter._filters,
        page=current_page,
    )
    await MessageUtils.build_message(result).send(reply_to=True)


@schedule_cmd.assign("设置")
async def handle_set(
    session: Uninfo,
    target_groups: list[str] = Depends(ResolveTargets),
    plugin_name: Match[str] = AlconnaMatch("plugin_name"),
    tag_name: Match[str] = AlconnaMatch("tag_name"),
    jitter: Match[int] = AlconnaMatch("jitter_seconds"),
    spread: Match[int] = AlconnaMatch("spread_seconds"),
    interval: Match[int] = AlconnaMatch("interval_seconds"),
    job_name: Match[str] = AlconnaMatch("job_name"),
    bot_id_to_operate: str = Depends(GetBotId),
    trigger: BaseTrigger = Depends(GetTriggerInfo),
    job_kwargs: dict = Depends(GetValidatedJobKwargs),
    final_permission: int = Depends(GetFinalPermission),
):
    """处理 '设置' 子命令"""
    p_name = plugin_name.result
    jitter_val: int | None = jitter.result if jitter.available else None
    spread_val: int | None = spread.result if spread.available else None
    interval_val: int | None = interval.result if interval.available else None

    is_multi_target = (
        len(target_groups) > 1
        or (
            len(target_groups) == 1 and target_groups[0] == scheduler_manager.ALL_GROUPS
        )
        or tag_name.available
    )

    if is_multi_target:
        task_meta = scheduler_registry.tasks.get(p_name)
        if jitter_val is None:
            if task_meta and task_meta.get("default_jitter") is not None:
                jitter_val = cast(int | None, task_meta["default_jitter"])
            else:
                jitter_val = Config.get_config(
                    "SchedulerManager", "DEFAULT_JITTER_SECONDS"
                )
        if spread_val is None:
            if task_meta and task_meta.get("default_spread") is not None:
                spread_val = cast(int | None, task_meta["default_spread"])
            else:
                spread_val = Config.get_config(
                    "SchedulerManager", "DEFAULT_SPREAD_SECONDS"
                )

        if interval_val is None:
            if task_meta and task_meta.get("default_interval") is not None:
                interval_val = cast(int | None, task_meta["default_interval"])
            else:
                interval_val = Config.get_config(
                    "SchedulerManager", "DEFAULT_INTERVAL_SECONDS"
                )

    result_message = await scheduler_admin_service.set_schedule(
        targets=target_groups,
        plugin_name=p_name,
        trigger=trigger,
        job_kwargs=job_kwargs,
        permission=final_permission,
        bot_id=bot_id_to_operate,
        job_name=job_name.result if job_name.available else None,
        jitter=jitter_val,
        spread=spread_val,
        interval=interval_val,
        created_by=session.user.id,
    )
    await MessageUtils.build_message(result_message).send()


@schedule_cmd.assign("删除")
async def handle_delete(
    targeter=Depends(GetTargeter),
):
    """处理 '删除' 子命令"""
    result_message = await scheduler_admin_service.perform_bulk_operation(
        operation_name="删除", targeter=targeter
    )
    await schedule_cmd.finish(result_message)


@schedule_cmd.assign("暂停")
async def handle_pause(
    targeter=Depends(GetTargeter),
):
    """处理 '暂停' 子命令"""
    result_message = await scheduler_admin_service.perform_bulk_operation(
        operation_name="暂停", targeter=targeter
    )
    await schedule_cmd.finish(result_message)


@schedule_cmd.assign("恢复")
async def handle_resume(
    targeter=Depends(GetTargeter),
):
    """处理 '恢复' 子命令"""
    result_message = await scheduler_admin_service.perform_bulk_operation(
        operation_name="恢复", targeter=targeter
    )
    await schedule_cmd.finish(result_message)


@schedule_cmd.assign("执行")
async def handle_trigger(schedule: ScheduledJob = Depends(RequireSchedule)):
    """处理 '执行' 子命令"""
    result_message = await scheduler_admin_service.trigger_schedule_now(schedule)
    await schedule_cmd.finish(result_message)


@schedule_cmd.assign("更新")
async def handle_update(
    schedule: ScheduledJob = Depends(RequireSchedule),
    arp: Arparma = AlconnaMatches(),
    kwargs_str: Match[str] = AlconnaMatch("kwargs_str"),
):
    """处理 '更新' 子命令"""
    trigger = _parse_trigger_from_arparma(arp)
    if not trigger and not kwargs_str.available:
        await schedule_cmd.finish(
            "请提供需要更新的时间 (--cron/--interval/--date/--daily) 或参数 (--kwargs)"
        )

    result_message = await scheduler_admin_service.update_schedule(
        schedule, trigger, kwargs_str.result if kwargs_str.available else None
    )
    await schedule_cmd.finish(result_message)


@schedule_cmd.assign("插件列表")
async def handle_plugins_list():
    """处理 '插件列表' 子命令"""
    message = await scheduler_admin_service.get_plugins_list()
    await schedule_cmd.finish(message)


@schedule_cmd.assign("状态")
async def handle_status(
    schedule: ScheduledJob = Depends(RequireSchedule),
):
    """处理 '状态' 子命令"""
    message = await scheduler_admin_service.get_schedule_status(schedule.id)
    await schedule_cmd.finish(message)
