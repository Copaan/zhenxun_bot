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
    for priority in sorted(priority_data):
        funcs = [
            func
            for func in list(priority_data[priority])
            if PriorityLifecycle._metadata.get(func, HookSpec()).stage == stage
        ]
        if not funcs:
            continue
        run_parallel = len(funcs) > 1 and all(
            PriorityLifecycle._metadata.get(func, HookSpec()).parallel_safe
            for func in funcs
        )

        async def run_one(func: Callable) -> None:
            spec = PriorityLifecycle._metadata.get(func, HookSpec(stage=stage))
            try:
                await _run_hook(
                    func,
                    priority,
                    stage=stage,
                    timeout=spec.timeout,
                )
            except (Exception, HookPriorityException) as error:
                logger.error(
                    f"执行启动钩子失败: {_hook_name(func)} ({type(error).__name__})",
                    e=error if isinstance(error, Exception) else None,
                )
                if spec.failure_policy == "fatal":
                    raise
                startup_coordinator.record_error(
                    stage, f"{_hook_name(func)}:{type(error).__name__}"
                )

        if run_parallel:
            results = await asyncio.gather(
                *(run_one(func) for func in funcs), return_exceptions=True
            )
            fatal = next(
                (item for item in results if isinstance(item, BaseException)), None
            )
            if fatal is not None:
                raise fatal
        else:
            for func in funcs:
                await run_one(func)


_post_management_task: asyncio.Task[None] | None = None


async def _run_post_management() -> None:
    await startup_coordinator.wait_server_bound()
    startup_coordinator.begin_stage("runtime")
    try:
        await _run_stage("runtime")
    except asyncio.CancelledError:
        raise
    except BaseException as error:
        startup_coordinator.fail_stage(
            "runtime", f"runtime_stage_failed:{type(error).__name__}"
        )
        return
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
