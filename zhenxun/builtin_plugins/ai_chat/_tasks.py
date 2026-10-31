"""AI 主动任务：按 cron、间隔或指定时间运行的 AI 任务，复用定时任务服务持久化。
所有用户都可以通过对话创建，用量计入创建者"""

import asyncio
from datetime import datetime, timedelta
import time
from types import SimpleNamespace
from typing import Any
import uuid

from apscheduler.triggers.cron import CronTrigger as APCronTrigger
from nonebot.adapters import Bot
from pydantic import BaseModel

from zhenxun.configs.config import Config
from zhenxun.models.ai_chat_group import AIChatGroup
from zhenxun.models.ai_persona import AIPersona
from zhenxun.models.ai_token_usage import AITokenUsage
from zhenxun.models.scheduled_job import ScheduledJob
from zhenxun.services.ai.context.memory.engine import chat_backend
from zhenxun.services.ai.core.exceptions import LLMException
from zhenxun.services.ai.core.messages import LLMMessage
from zhenxun.services.ai.run.context import RunContext, ScheduledDeps
from zhenxun.services.ai.tools.core.decorators import tool
from zhenxun.services.ai.tools.core.toolkit import BaseToolkit
from zhenxun.services.ai.tools.models import ToolResult
from zhenxun.services.ai.utils.runtime import PermissionUtils
from zhenxun.services.log import logger
from zhenxun.services.scheduler import ScheduleContext, Trigger, scheduler_manager
from zhenxun.services.scheduler.types import BaseTrigger, ExecutionOptions, JobConfig
from zhenxun.utils.platform import PlatformUtils
from zhenxun.utils.pydantic_compat import model_validate

from ._buffer import ChatBuffer
from ._data_source import (
    BACKGROUND_KEY,
    MODULE,
    PLUGIN_MODULE,
    SILENT,
    AIChatManager,
)
from ._reply import deliver, parse_silent

TASK_PLUGIN = "ai_chat"
"""主动任务在定时任务服务中的插件名"""
KINDS = {"cron": "cron 表达式", "interval": "间隔分钟", "date": "指定时间"}
MIN_INTERVAL = 600
"""两次执行的最小间隔秒数"""
RETRY_DELAY = 60
"""模型调用失败后重试一次前等待的秒数"""
WEEKDAYS = ("sun", "mon", "tue", "wed", "thu", "fri", "sat")
"""crontab 星期序号对应的 APScheduler 星期名称"""


class TaskParams(BaseModel):
    """主动任务参数，目标会话在创建时固定"""

    prompt: str
    creator: str
    group_id: str | None = None
    user_id: str | None = None


def build_trigger(kind: str, value: str) -> BaseTrigger:
    """解析主动任务的触发方式，限制两次执行至少间隔 10 分钟

    参数:
        kind: cron（“分 时 日 月 周”）、interval（间隔分钟）、date（YYYY-MM-DD HH:MM）
        value: 触发值

    返回:
        BaseTrigger: 触发器

    异常:
        ValueError: 触发方式无效
    """
    value = value.strip()
    if kind == "interval":
        if not value.isdigit() or int(value) * 60 < MIN_INTERVAL:
            raise ValueError(f"间隔至少 {MIN_INTERVAL // 60} 分钟。")
        return Trigger.interval(minutes=int(value))
    if kind == "date":
        try:
            run_at = datetime.strptime(value, "%Y-%m-%d %H:%M")
        except ValueError as e:
            raise ValueError("时间格式应为 YYYY-MM-DD HH:MM。") from e
        if run_at <= datetime.now():
            raise ValueError("执行时间必须晚于现在。")
        return Trigger.date(run_date=run_at)
    if kind != "cron":
        raise ValueError("触发方式应为 cron、interval 或 date。")
    fields = value.split()
    if len(fields) != 5:
        raise ValueError("cron 表达式应为“分 时 日 月 周”五段。")
    minute, hour, day, month, day_of_week = fields
    try:
        day_of_week = _crontab_weekday(day_of_week)
        trigger = APCronTrigger(
            minute=minute, hour=hour, day=day, month=month, day_of_week=day_of_week
        )
    except ValueError as e:
        raise ValueError(f"cron 表达式无效：{e}") from e
    now = datetime.now().astimezone()
    previous = trigger.get_next_fire_time(None, now)
    for _ in range(200):
        if previous is None or previous - now > timedelta(days=8):
            break
        current = trigger.get_next_fire_time(previous, previous + timedelta(seconds=1))
        if current and (current - previous).total_seconds() < MIN_INTERVAL:
            raise ValueError(f"两次执行至少间隔 {MIN_INTERVAL // 60} 分钟。")
        previous = current
    return Trigger.cron(
        minute=minute, hour=hour, day=day, month=month, day_of_week=day_of_week
    )


def _weekday(token: str) -> int:
    """标准 crontab 的星期值：数字 0–7（0 与 7 为周日）或英文缩写"""
    if token.isdigit() and int(token) <= 7:
        return int(token)
    if token in WEEKDAYS:
        return WEEKDAYS.index(token)
    raise ValueError(f"星期 {token} 无效")


def _crontab_weekday(field: str) -> str:
    """把标准 crontab 的星期字段转为 APScheduler 的星期名称（APScheduler 的数字
    0 表示周一，crontab 的 0 表示周日）

    参数:
        field: 星期字段，支持 *、列表、范围与步长

    返回:
        str: 逗号分隔的星期名称，* 原样返回

    异常:
        ValueError: 字段无效
    """
    if field == "*":
        return field
    days: set[int] = set()
    for part in field.lower().split(","):
        base, _, step = part.partition("/")
        if step and not (step.isdigit() and int(step) > 0):
            raise ValueError(f"步长 {step} 无效")
        if base == "*":
            start, end = 0, 6
        elif "-" in base:
            start, end = (_weekday(t) for t in base.split("-", 1))
        else:
            start = _weekday(base)
            end = 6 if step else start
        if start > end:
            raise ValueError(f"星期范围 {base} 无效")
        days.update(d % 7 for d in range(start, end + 1, int(step or 1)))
    return ",".join(WEEKDAYS[d] for d in sorted(days))


def describe_trigger(job: ScheduledJob) -> str:
    """把任务触发方式描述为一句话

    参数:
        job: 定时任务

    返回:
        str: 描述
    """
    config = job.trigger_config if isinstance(job.trigger_config, dict) else {}
    if job.trigger_type == "interval":
        return f"每 {config.get('minutes', 0)} 分钟"
    if job.trigger_type == "date":
        return f"{str(config.get('run_date', ''))[:16].replace('T', ' ')} 执行一次"
    keys = ("minute", "hour", "day", "month", "day_of_week")
    return "cron " + " ".join(str(config.get(key) or "*") for key in keys)


def _options() -> dict[str, Any]:
    """主动任务的执行选项：不并发运行，错过时在 TASK_CATCH_UP 分钟内补跑

    返回:
        dict[str, Any]: 执行选项
    """
    minutes = max(int(Config.get_config(MODULE, "TASK_CATCH_UP", 30) or 0), 1)
    return {"concurrency_policy": "SKIP", "misfire_grace_time": minutes * 60}


class AITasks:
    @classmethod
    def item(cls, job: ScheduledJob) -> dict[str, Any]:
        """任务的展示数据

        参数:
            job: 定时任务

        返回:
            dict[str, Any]: 展示数据
        """
        kwargs = job.job_kwargs if isinstance(job.job_kwargs, dict) else {}
        config = job.trigger_config if isinstance(job.trigger_config, dict) else {}
        if job.trigger_type == "interval":
            value = str(config.get("minutes", ""))
        elif job.trigger_type == "date":
            value = str(config.get("run_date", ""))[:16].replace("T", " ")
        else:
            keys = ("minute", "hour", "day", "month", "day_of_week")
            value = " ".join(str(config.get(key) or "*") for key in keys)
        return {
            "id": job.id,
            "name": job.name or "",
            "prompt": kwargs.get("prompt", ""),
            "creator": kwargs.get("creator", ""),
            "bot_id": job.bot_id,
            "group_id": kwargs.get("group_id"),
            "user_id": kwargs.get("user_id"),
            "kind": job.trigger_type,
            "value": value,
            "trigger": describe_trigger(job),
            "enabled": job.is_enabled,
            "last_run_at": job.last_run_at.isoformat() if job.last_run_at else None,
            "created_at": job.create_time.isoformat(),
        }

    @classmethod
    async def all(cls) -> list[ScheduledJob]:
        """全部主动任务

        返回:
            list[ScheduledJob]: 主动任务
        """
        return await ScheduledJob.filter(plugin_name=TASK_PLUGIN).order_by("id")

    @classmethod
    async def visible(cls, context: RunContext) -> list[ScheduledJob]:
        """对话中可以查看和取消的主动任务：超级用户为全部，群管理员为本群的任务
        与自己创建的任务，其他用户为自己创建的任务

        参数:
            context: 运行上下文

        返回:
            list[ScheduledJob]: 主动任务
        """
        jobs = await cls.all()
        if await PermissionUtils.check_superuser(context):
            return jobs
        user_id, group_id = context.get_user_id(), context.get_group_id()
        admin = bool(group_id) and await PermissionUtils.check_admin_level(context, 1)
        result = []
        for job in jobs:
            kwargs = job.job_kwargs if isinstance(job.job_kwargs, dict) else {}
            if kwargs.get("creator") == user_id or (
                admin and kwargs.get("group_id") == group_id
            ):
                result.append(job)
        return result

    @classmethod
    async def count_by(cls, creator: str) -> int:
        """用户创建的主动任务数

        参数:
            creator: 创建者

        返回:
            int: 任务数
        """
        return sum(
            1
            for job in await cls.all()
            if isinstance(job.job_kwargs, dict)
            and job.job_kwargs.get("creator") == creator
        )

    @classmethod
    async def get(cls, task_id: int) -> ScheduledJob | None:
        """获取主动任务

        参数:
            task_id: 任务id

        返回:
            ScheduledJob | None: 主动任务
        """
        return await ScheduledJob.get_or_none(id=task_id, plugin_name=TASK_PLUGIN)

    @classmethod
    async def create(
        cls,
        bot_id: str,
        params: TaskParams,
        name: str,
        kind: str,
        value: str,
    ) -> ScheduledJob:
        """创建主动任务

        参数:
            bot_id: 执行任务的 Bot
            params: 任务参数（prompt、创建者、目标群或私聊对象）
            name: 任务名称
            kind: 触发方式
            value: 触发值

        返回:
            ScheduledJob: 主动任务

        异常:
            ValueError: 参数无效或已达数量上限
        """
        if not params.prompt.strip():
            raise ValueError("任务说明不能为空。")
        if not (params.group_id or params.user_id):
            raise ValueError("需要指定目标群或私聊对象。")
        limit = int(Config.get_config(MODULE, "TASK_LIMIT") or 20)
        if await ScheduledJob.filter(plugin_name=TASK_PLUGIN).count() >= limit:
            raise ValueError(f"主动任务已达上限 {limit} 个。")
        trigger = build_trigger(kind, value)
        job = await scheduler_manager.add_schedule(
            TASK_PLUGIN,
            "GLOBAL",
            uuid.uuid4().hex[:16],
            JobConfig(
                trigger=trigger,
                job_kwargs=params.model_dump(),
                bot_id=bot_id,
                name=name.strip()[:64] or params.prompt.strip()[:20],
                created_by=params.creator,
                required_permission=9,
                source="AI",
                is_one_off=kind == "date",
                execution_options=model_validate(ExecutionOptions, _options()),
            ),
        )
        if job is None:
            raise ValueError("任务创建失败。")
        return job

    @classmethod
    async def update(
        cls, job: ScheduledJob, name: str, prompt: str, kind: str, value: str
    ) -> None:
        """修改主动任务的名称、说明与触发方式，目标会话不变

        参数:
            job: 主动任务
            name: 名称
            prompt: 任务说明
            kind: 触发方式
            value: 触发值

        异常:
            ValueError: 参数无效
        """
        if not prompt.strip():
            raise ValueError("任务说明不能为空。")
        trigger = build_trigger(kind, value)
        config = trigger.model_dump(mode="json", exclude_none=True)
        config.pop("trigger_type", None)
        job.name = name.strip()[:64] or prompt.strip()[:20]
        job.trigger_type = kind
        job.trigger_config = config
        kwargs = job.job_kwargs if isinstance(job.job_kwargs, dict) else {}
        job.job_kwargs = {**kwargs, "prompt": prompt.strip()}
        job.is_one_off = kind == "date"
        options = job.execution_options
        job.execution_options = {
            **(options if isinstance(options, dict) else {}),
            **_options(),
        }
        await job.save()
        if job.is_enabled:
            from zhenxun.services.scheduler.engine import APSchedulerAdapter

            APSchedulerAdapter.add_or_reschedule_job(job)

    @classmethod
    async def remove(cls, task_id: int) -> bool:
        """删除主动任务

        参数:
            task_id: 任务id

        返回:
            bool: 是否删除
        """
        if await cls.get(task_id) is None:
            return False
        count, _ = await scheduler_manager.target(id=task_id).remove()
        return count > 0

    @classmethod
    async def execute(cls, bot: Bot, task_id: int, name: str, params: TaskParams):
        """运行一次主动任务：以创建者身份运行人设 Agent，用量计入创建者，
        有结果时发送到目标会话；模型调用失败且尚未执行工具时等待后重试一次

        参数:
            bot: Bot
            task_id: 任务id
            name: 任务名称
            params: 任务参数
        """
        from zhenxun.services.scheduler.engine import _plugin_blocked

        from ._agent import USER_KEY, record_run, start_run

        group_id, user_id = params.group_id, params.user_id
        setting = await AIChatGroup.get_setting(group_id) if group_id else None
        target = SimpleNamespace(group_id=group_id, user_id=user_id)
        if (setting and setting.disabled) or await _plugin_blocked(
            PLUGIN_MODULE, bot, target
        ):
            logger.info(f"主动任务 #{task_id} 的目标会话不可用，跳过", MODULE)
            return
        persona = await AIPersona.get_for_scene(group_id, user_id)
        agent = AIChatManager.get_agent(persona)
        where = f"群 {group_id}" if group_id else f"与 {user_id} 的私聊"
        text = (
            f"【主动任务 #{task_id}「{name}」】现在是 "
            f"{datetime.now().strftime('%Y-%m-%d %H:%M')}，结果会发送到{where}。\n"
            f"任务说明：{params.prompt}\n"
            f"只输出要发送的内容；这次没有需要发送的就只输出 {SILENT}。"
        )
        for attempt in range(2):
            context = RunContext(
                deps=ScheduledDeps(
                    bot=bot,
                    group_id=group_id,
                    user_id=params.creator,
                    schedule_id=task_id,
                    platform=PlatformUtils.get_platform_scope(bot),
                ),
                session_id=f"{MODULE}:{bot.self_id}:task:{task_id}",
            )
            if setting and setting.model:
                context.run.current_model = setting.model
            if group_id:
                context.state[BACKGROUND_KEY] = ChatBuffer.background(
                    bot.self_id, group_id, None, 20
                )
            context.state[USER_KEY] = params.creator
            start_run(context)
            started = time.monotonic()
            label = f"[任务 #{task_id}] {params.prompt}"
            name_ = persona.name if persona else None
            try:
                result = await agent.run(text, context=context)
            except LLMException as e:
                await record_run(context, "task", name_, label, started, error=e)
                logger.warning(f"主动任务 #{task_id} 运行失败: {e}", MODULE)
                if any(m.role == "tool" for m in context.run.messages):
                    return
                if attempt == 0:
                    await asyncio.sleep(RETRY_DELAY)
                continue
            await record_run(context, "task", name_, label, started, result)
            await AITokenUsage.add_usage(params.creator, result.usage.total_tokens)
            reply = None if result.stopped else parse_silent(str(result.output))
            if not reply:
                return
            from ._stickers import send_marked, split_sticker

            reply, sticker_id = split_sticker(reply)
            voice = setting.voice_reply if setting else None
            target = None if group_id else user_id
            try:
                sent = bool(reply) and await deliver(
                    bot, group_id, target, reply, voice_mode=voice
                )
            except Exception as e:
                logger.warning(f"主动任务 #{task_id} 的结果发送失败: {e}", MODULE)
                return
            if sticker_id is not None:
                await send_marked(context, sticker_id)
            if sent and target:
                await cls.remember(bot, target, name, reply)
            return

    @classmethod
    async def remember(cls, bot: Bot, user_id: str, name: str, reply: str) -> None:
        """把私聊任务的结果写入该用户的私聊对话记忆，之后的对话可以接着聊

        参数:
            bot: Bot
            user_id: 私聊对象
            name: 任务名称
            reply: 已发送的结果
        """
        agent = AIChatManager.get_agent(None)
        if backend := chat_backend(agent.memory_config):
            await backend.add_messages(
                f"{MODULE}:{bot.self_id}:private:{user_id}",
                [
                    LLMMessage.user(
                        f"（系统提示：主动任务「{name}」按时运行，结果如下）"
                    ),
                    LLMMessage.assistant_text_response(reply),
                ],
            )


async def _run_task(bot: Bot, context: ScheduleContext, params: TaskParams):
    job = await AITasks.get(context.schedule_id)
    await AITasks.execute(bot, context.schedule_id, job.name if job else "", params)


def register_task_job() -> None:
    """在定时任务服务中注册主动任务的执行函数"""
    if TASK_PLUGIN not in scheduler_manager.get_registered_plugins():
        scheduler_manager.register(
            TASK_PLUGIN, params_model=TaskParams, default_permission=9
        )(_run_task)


class TaskToolkit(BaseToolkit):
    """主动任务工具：创建、查看、取消"""

    @tool(
        name="schedule_task",
        description=(
            "创建一个主动任务，按时运行 prompt 并把结果发到当前会话。prompt 要自包含"
            "（写清楚要做什么、发什么），运行时不会看到这次对话。kind：cron（value 为"
            "“分 时 日 月 周”，如 0 8 * * *）、interval（value 为间隔分钟，至少 10）、"
            "date（value 为 YYYY-MM-DD HH:MM，执行一次）。"
        ),
    )
    async def schedule_task(
        self, context: RunContext, name: str, prompt: str, kind: str, value: str
    ) -> ToolResult:
        bot, group_id = context.get_bot(), context.get_group_id()
        creator = context.get_user_id()
        if bot is None or not creator:
            return ToolResult(output="只能在 QQ 对话中创建主动任务。").as_error(False)
        limit = int(Config.get_config(MODULE, "TASK_USER_LIMIT") or 0)
        if (
            limit
            and not PlatformUtils.is_superuser(bot, creator)
            and await AITasks.count_by(creator) >= limit
        ):
            return ToolResult(
                output=f"每人最多创建 {limit} 个主动任务，请先取消不需要的。"
            ).as_error(False)
        params = TaskParams(
            prompt=prompt,
            creator=creator,
            group_id=group_id,
            user_id=None if group_id else creator,
        )
        try:
            job = await AITasks.create(bot.self_id, params, name, kind, value)
        except ValueError as e:
            return ToolResult(output=str(e)).as_error(False)
        return ToolResult(output=f"已创建主动任务 #{job.id}：{describe_trigger(job)}。")

    @tool(
        name="list_scheduled_tasks",
        description="列出当前用户可以管理的主动任务（群管理员可看到本群的任务）。",
    )
    async def list_scheduled_tasks(self, context: RunContext) -> ToolResult:
        jobs = await AITasks.visible(context)
        if not jobs:
            return ToolResult(output="当前没有主动任务。")
        lines = []
        for job in jobs:
            item = AITasks.item(job)
            where = f"群 {item['group_id']}" if item["group_id"] else "私聊"
            state = "" if job.is_enabled else "（已暂停）"
            lines.append(
                f"#{job.id} {item['name']}{state}：{item['trigger']}，发到{where}；"
                f"{item['prompt'][:60]}"
            )
        return ToolResult(output="\n".join(lines))

    @tool(name="cancel_scheduled_task", description="按编号删除一个主动任务。")
    async def cancel_scheduled_task(
        self, context: RunContext, task_id: int
    ) -> ToolResult:
        if all(job.id != task_id for job in await AITasks.visible(context)):
            return ToolResult(
                output=f"没有主动任务 #{task_id}，或你无权取消它。"
            ).as_error(False)
        if not await AITasks.remove(task_id):
            return ToolResult(output=f"没有主动任务 #{task_id}。").as_error(False)
        return ToolResult(output=f"已删除主动任务 #{task_id}。")
