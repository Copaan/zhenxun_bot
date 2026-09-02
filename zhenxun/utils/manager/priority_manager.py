from __future__ import annotations

import asyncio
from collections.abc import Callable
from dataclasses import dataclass
import time
from typing import ClassVar, Literal

import nonebot
from nonebot.utils import is_coroutine_callable

from zhenxun.services.log import logger
from zhenxun.services.startup import StartupStage, startup_coordinator
from zhenxun.utils.enum import PriorityLifecycleType
from zhenxun.utils.exception import HookPriorityException

driver = nonebot.get_driver()

FailurePolicy = Literal["fatal", "degrade"]


@dataclass(slots=True)
class HookSpec:
    stage: StartupStage = "runtime"
    timeout: float | None = None
    parallel_safe: bool = False
    failure_policy: FailurePolicy = "fatal"
    task_id: str | None = None
    depends_on: tuple[str, ...] = ()
    resource_group: str | None = None


class PriorityLifecycle:
    # Keep this registry shape compatible with the runtime reload layer.
    _data: ClassVar[dict[PriorityLifecycleType, dict[int, list[Callable]]]] = {}
    _metadata: ClassVar[dict[Callable, HookSpec]] = {}

    @classmethod
    def add(
        cls,
        hook_type: PriorityLifecycleType,
        func: Callable,
        priority: int,
        *,
        stage: StartupStage = "runtime",
        timeout: float | None = None,
        parallel_safe: bool = False,
        failure_policy: FailurePolicy | None = None,
        task_id: str | None = None,
        depends_on: tuple[str, ...] = (),
        resource_group: str | None = None,
    ):
        if hook_type not in cls._data:
            cls._data[hook_type] = {}
        if priority not in cls._data[hook_type]:
            cls._data[hook_type][priority] = []
        cls._data[hook_type][priority].append(func)
        cls._metadata[func] = HookSpec(
            stage=stage,
            timeout=timeout,
            parallel_safe=parallel_safe,
            failure_policy=failure_policy
            or ("degrade" if stage == "warmup" else "fatal"),
            task_id=task_id,
            depends_on=depends_on,
            resource_group=resource_group,
        )

    @classmethod
    def on_startup(
        cls,
        *,
        priority: int,
        stage: StartupStage = "runtime",
        timeout: float | None = None,
        parallel_safe: bool = False,
        failure_policy: FailurePolicy | None = None,
        task_id: str | None = None,
        depends_on: tuple[str, ...] = (),
        resource_group: str | None = None,
    ):
        def wrapper(func):
            cls.add(
                PriorityLifecycleType.STARTUP,
                func,
                priority,
                stage=stage,
                timeout=timeout,
                parallel_safe=parallel_safe,
                failure_policy=failure_policy,
                task_id=task_id,
                depends_on=depends_on,
                resource_group=resource_group,
            )
            return func

        return wrapper

    @classmethod
    def on_shutdown(cls, *, priority: int, timeout: float | None = None):
        def wrapper(func):
            cls.add(
                PriorityLifecycleType.SHUTDOWN,
                func,
                priority,
                timeout=timeout,
                failure_policy="degrade",
            )
            return func

        return wrapper


def _hook_name(func: Callable) -> str:
    return f"{getattr(func, '__module__', 'unknown')}:{getattr(func, '__name__', '?')}"


async def _run_hook(
    func: Callable,
    priority: int,
    hook_type: str = "startup",
    *,
    stage: str = "runtime",
    timeout: float | None = None,
) -> None:
    name = _hook_name(func)
    logger.debug(f"执行优先级 [{priority}] on_{hook_type} 方法: {func.__module__}")
    started = time.monotonic()
    state = "completed"
    error_code = None
    try:
        if is_coroutine_callable(func):
            awaitable = func()
            if timeout is not None:
                await asyncio.wait_for(awaitable, timeout=timeout)
            else:
                await awaitable
        else:
            func()
    except BaseException as error:
        state = "failed"
        error_code = (
            "hook_priority_interrupted"
            if isinstance(error, HookPriorityException)
            else "hook_timeout"
            if isinstance(error, TimeoutError)
            else f"hook_failed:{type(error).__name__}"
        )
        raise
    finally:
        startup_coordinator.record_operation(
            name,
            stage,
            state,
            (time.monotonic() - started) * 1000,
            priority=priority,
            error_code=error_code,
        )


async def _run_stage(stage: StartupStage) -> None:
    priority_data = PriorityLifecycle._data.get(PriorityLifecycleType.STARTUP, {})
    pending: dict[str, tuple[int, Callable, HookSpec]] = {}
    for priority in sorted(priority_data):
        for func in list(priority_data[priority]):
            spec = PriorityLifecycle._metadata.get(func, HookSpec())
            if spec.stage != stage:
                continue
            base_id = spec.task_id or _hook_name(func)
            task_id = base_id
            suffix = 2
            while task_id in pending:
                task_id = f"{base_id}#{suffix}"
                suffix += 1
            pending[task_id] = (priority, func, spec)

    completed: set[str] = set()

    async def run_one(task_id: str, item: tuple[int, Callable, HookSpec]) -> None:
        priority, func, spec = item
        try:
            await _run_hook(
                func,
                priority,
                stage=stage,
                timeout=spec.timeout,
            )
            if stage == "warmup":
                from zhenxun.services.startup_load import startup_load_planner

                startup_load_planner.finish_warmup_hook(
                    str(getattr(func, "__module__", ""))
                )
        except (Exception, HookPriorityException) as error:
            logger.error(
                f"执行启动钩子失败: {_hook_name(func)} ({type(error).__name__})",
                e=error if isinstance(error, Exception) else None,
            )
            from zhenxun.services.startup_load import startup_load_planner

            owner = startup_load_planner.owner_for_module(
                str(getattr(func, "__module__", ""))
            )
            if owner and not startup_load_planner.is_core_plugin(owner):
                startup_load_planner.mark_failed(
                    owner, f"plugin_lifecycle_failed:{type(error).__name__}"
                )
                if stage == "warmup":
                    startup_load_planner.finish_warmup_hook(
                        str(getattr(func, "__module__", ""))
                    )
                return
            if spec.failure_policy == "fatal":
                raise
            startup_coordinator.record_error(stage, f"{task_id}:{type(error).__name__}")

    running: dict[
        asyncio.Task[None], tuple[str, tuple[int, Callable, HookSpec], str]
    ] = {}
    while pending or running:
        ready = [
            (task_id, item)
            for task_id, item in pending.items()
            if set(item[2].depends_on) <= completed
        ]
        if not ready and not running:
            unresolved = ",".join(sorted(pending))
            raise RuntimeError(f"startup_dependency_cycle:{unresolved}")
        if ready:
            # Keep priorities as barriers even when a parallel hook finishes first.
            # Otherwise a higher-priority task could start while another task from
            # the previous priority is still mutating shared startup state.
            min_priority = (
                min(item[0] for _, item, _ in running.values())
                if running
                else min(item[0] for _, item in ready)
            )
            ready = sorted(
                (row for row in ready if row[1][0] == min_priority),
                key=lambda row: row[0],
            )
            active_groups = {row[2] for row in running.values()}
            running_is_parallel = all(
                row[1][2].parallel_safe for row in running.values()
            )
            for task_id, item in ready:
                spec = item[2]
                group = spec.resource_group or task_id
                if running and (not running_is_parallel or not spec.parallel_safe):
                    continue
                if not spec.parallel_safe and running:
                    continue
                if group in active_groups:
                    continue
                task = asyncio.create_task(
                    run_one(task_id, item), name=f"startup:{stage}:{task_id}"
                )
                running[task] = (task_id, item, group)
                pending.pop(task_id, None)
                active_groups.add(group)
                if not spec.parallel_safe:
                    break
        if not running:
            continue
        done, _ = await asyncio.wait(running, return_when=asyncio.FIRST_COMPLETED)
        for task in done:
            task_id, _, _ = running.pop(task)
            try:
                task.result()
            except BaseException:
                for active in running:
                    active.cancel()
                await asyncio.gather(*running, return_exceptions=True)
                raise
            completed.add(task_id)


_post_management_task: asyncio.Task[None] | None = None


async def _run_post_management() -> None:
    await startup_coordinator.wait_server_bound()
    startup_coordinator.begin_stage("runtime")
    try:
        from zhenxun.services.startup_load import startup_load_planner

        if startup_load_planner.prepared:
            await startup_load_planner.load_runtime()
        await _run_stage("runtime")
    except asyncio.CancelledError:
        raise
    except BaseException as error:
        startup_coordinator.fail_stage(
            "runtime", f"runtime_stage_failed:{type(error).__name__}"
        )
        return
    if startup_load_planner.prepared:
        startup_load_planner.prepare_warmup_gates()
    startup_coordinator.finish_stage("runtime")

    startup_coordinator.begin_stage("warmup")
    try:
        await _run_stage("warmup")
    except asyncio.CancelledError:
        raise
    except BaseException as error:
        startup_coordinator.fail_stage(
            "warmup", f"warmup_stage_failed:{type(error).__name__}", fatal=False
        )
        return
    startup_coordinator.finish_stage("warmup")


@driver.on_startup
async def _():
    global _post_management_task
    startup_coordinator.begin_stage("management")
    try:
        await _run_stage("management")
    except BaseException as error:
        startup_coordinator.fail_stage(
            "management", f"management_stage_failed:{type(error).__name__}"
        )
        raise
    startup_coordinator.finish_stage("management")
    _post_management_task = asyncio.create_task(
        _run_post_management(), name="zhenxun-startup-runtime"
    )


@driver.on_shutdown
async def _():
    global _post_management_task
    task = _post_management_task
    _post_management_task = None
    if task is not None and not task.done():
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    priority_data = PriorityLifecycle._data.get(PriorityLifecycleType.SHUTDOWN)
    if not priority_data:
        return
    for priority in sorted(priority_data):
        for func in list(priority_data[priority]):
            spec = PriorityLifecycle._metadata.get(func, HookSpec())
            try:
                await _run_hook(
                    func,
                    priority,
                    "shutdown",
                    stage="shutdown",
                    timeout=spec.timeout,
                )
            except Exception as error:
                logger.error(
                    f"执行优先级 [{priority}] on_shutdown 方法出错: {error}",
                    e=error,
                )
