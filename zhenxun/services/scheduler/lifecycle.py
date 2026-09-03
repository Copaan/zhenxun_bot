"""
定时任务的生命周期管理

包含在机器人启动时加载和调度数据库中保存的任务的逻辑。
"""

import asyncio
import contextlib

from nonebot_plugin_apscheduler import scheduler

from zhenxun.services.lifecycle import ResourceReceipt, RuntimeHandle
from zhenxun.services.log import logger
from zhenxun.utils.manager.priority_manager import PriorityLifecycle
from zhenxun.utils.pydantic_compat import model_dump

from .engine import APSchedulerAdapter
from .manager import scheduler_manager
from .registry import scheduler_registry
from .repository import ScheduleRepository
from .types import JobConfig, ScheduleContext


def _managed_jobs():
    return [
        job
        for job in scheduler.get_jobs()
        if str(job.id).startswith("zhenxun_schedule_")
        or str(job.id).startswith("runtime::")
    ]


class SchedulerRuntimeHandle:
    async def quiesce(self) -> None:
        for job in _managed_jobs():
            with contextlib.suppress(Exception):
                job.pause()
        deadline = asyncio.get_running_loop().time() + 10
        while scheduler_registry.running_tasks:
            if asyncio.get_running_loop().time() >= deadline:
                raise TimeoutError("scheduler_drain_timeout")
            await asyncio.sleep(0.05)

    async def close(self) -> None:
        for job in _managed_jobs():
            with contextlib.suppress(Exception):
                scheduler.remove_job(job.id)

    def health(self) -> dict[str, object]:
        return {
            "healthy": bool(getattr(scheduler, "running", False)),
            "running_tasks": len(scheduler_registry.running_tasks),
        }

    def snapshot(self) -> dict[str, object]:
        return {
            "managed_jobs": len(_managed_jobs()),
            "running_tasks": len(scheduler_registry.running_tasks),
        }

    def resource_snapshot(self) -> list[ResourceReceipt]:
        return [
            ResourceReceipt(
                receipt_id=f"apscheduler:{job.id}",
                provider="apscheduler",
                resource_type="job",
                owner_id="runtime:scheduler",
                detail={"next_run_time": str(job.next_run_time or "")},
            )
            for job in _managed_jobs()
        ]


def _scheduler_healthy(_value=None) -> bool:
    return bool(getattr(scheduler, "running", False))


@PriorityLifecycle.on_startup(
    priority=90,
    task_id="runtime:restore_schedules",
    component_id="runtime:scheduler",
    depends_on=("runtime:runtime_cache",),
    restart_policy="component",
    pass_context=True,
    health=_scheduler_healthy,
)
async def _load_schedules_from_db(context):
    """在服务启动时从数据库加载并调度所有任务。"""
    logger.info("正在从数据库加载并调度所有定时任务...")
    all_schedules = await ScheduleRepository.get_all()
    schedules = [schedule for schedule in all_schedules if schedule.is_enabled]
    count = 0
    for schedule in schedules:
        if schedule.plugin_name in scheduler_registry.tasks:
            APSchedulerAdapter.add_or_reschedule_job(schedule)
            count += 1
        else:
            logger.warning(f"跳过加载定时任务：插件 '{schedule.plugin_name}' 未注册。")
    logger.info(f"数据库定时任务加载完成，共成功加载 {count} 个任务。")

    logger.info("正在检查并注册声明式默认任务...")
    declared_count = 0
    existing_declarations = {
        (
            schedule.plugin_name,
            schedule.target_identifier,
            schedule.bot_id,
        )
        for schedule in all_schedules
    }
    for task_info in scheduler_registry.persistent_declarations:
        plugin_name = task_info.plugin_name
        group_id = task_info.group_id
        bot_id = task_info.bot_id

        declaration_key = (plugin_name, group_id or "", bot_id)
        if declaration_key not in existing_declarations:
            logger.info(f"为插件 '{plugin_name}' 注册新的默认定时任务...")

            target_type = "GROUP" if group_id else "GLOBAL"
            target_identifier = group_id or ""

            config = JobConfig(
                trigger=task_info.trigger,
                job_kwargs=task_info.job_kwargs,
                bot_id=bot_id,
                source="PLUGIN_DEFAULT",
            )

            schedule = await scheduler_manager.add_schedule(
                plugin_name=plugin_name,
                target_type=target_type,
                target_identifier=target_identifier,
                config=config,
            )
            if schedule:
                declared_count += 1
                existing_declarations.add(declaration_key)
                logger.debug(f"默认任务 '{plugin_name}' 注册成功 (ID: {schedule.id})")
            else:
                logger.error(f"默认任务 '{plugin_name}' 注册失败")
        else:
            logger.debug(f"插件 '{plugin_name}' 的默认任务已存在于数据库中，跳过注册。")

    if declared_count > 0:
        logger.info(f"声明式任务检查完成，新注册了 {declared_count} 个默认任务。")

    logger.info("正在调度声明式临时任务...")
    ephemeral_count = 0
    for declaration in scheduler_registry.ephemeral_declarations:
        try:
            job_id = f"runtime::{declaration.plugin_name}::{declaration.func.__name__}"

            context = ScheduleContext(
                schedule_id=0,
                plugin_name=job_id,
                bot_id=None,
                platform_scope=None,
                group_id=None,
                job_kwargs={},
            )

            trigger_config_dict = model_dump(
                declaration.trigger, exclude={"trigger_type"}
            )

            APSchedulerAdapter.add_ephemeral_job(
                job_id=job_id,
                func=declaration.func,
                trigger_type=declaration.trigger.trigger_type,
                trigger_config=trigger_config_dict,
                context=context,
            )
            ephemeral_count += 1
        except Exception as e:
            logger.error(f"调度临时任务 '{declaration.plugin_name}' 失败", e=e)

    if ephemeral_count > 0:
        logger.info(f"临时任务调度完成，共成功加载 {ephemeral_count} 个任务。")
    return RuntimeHandle(
        controller=SchedulerRuntimeHandle(),
        metadata={"ownership": "composite"},
    )
