from __future__ import annotations

import asyncio
from collections.abc import Callable
from dataclasses import dataclass
import inspect
import time
from typing import ClassVar, Literal, cast

import nonebot

from zhenxun.services.lifecycle import ComponentSpec, LifecycleError, lifecycle_kernel
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
    depends_on: tuple[str, ...] = ()
    component_id: str | None = None
    config_keys: tuple[str, ...] = ()
    pass_context: bool = False


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
        depends_on: tuple[str, ...] = (),
        component_id: str | None = None,
        config_keys: tuple[str, ...] = (),
        pass_context: bool = False,
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
            depends_on=depends_on,
            component_id=component_id,
            config_keys=config_keys,
            pass_context=pass_context,
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
        depends_on: tuple[str, ...] = (),
        component_id: str | None = None,
        config_keys: tuple[str, ...] = (),
        pass_context: bool = False,
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
                depends_on=depends_on,
                component_id=component_id,
                config_keys=config_keys,
                pass_context=pass_context,
            )
            return func

        return wrapper

    @classmethod
    def on_shutdown(
        cls,
        *,
        priority: int,
        timeout: float | None = None,
        component_id: str | None = None,
    ):
        def wrapper(func):
            cls.add(
                PriorityLifecycleType.SHUTDOWN,
                func,
                priority,
                timeout=timeout,
                failure_policy="degrade",
                component_id=component_id,
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
    context: object | None = None,
) -> object | None:
    name = _hook_name(func)
    logger.debug(f"执行优先级 [{priority}] on_{hook_type} 方法: {func.__module__}")
    started = time.monotonic()
    state = "completed"
    error_code = None
    try:
        result = func(context) if context is not None else func()
        if inspect.isawaitable(result):
            if timeout is not None:
                result = await asyncio.wait_for(result, timeout=timeout)
            else:
                result = await result
        return result
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


def _hooks(hook_type: PriorityLifecycleType) -> list[tuple[int, Callable, HookSpec]]:
    priority_data = PriorityLifecycle._data.get(hook_type, {})
    return [
        (priority, func, PriorityLifecycle._metadata.get(func, HookSpec()))
        for priority in sorted(priority_data)
        for func in list(priority_data[priority])
    ]


def _keyed(
    hooks: list[tuple[int, Callable, HookSpec]], stage: str | None = None
) -> dict[str, tuple[int, Callable, HookSpec]]:
    """以组件 ID 为键整理钩子，未声明 ID 的钩子按阶段、模块与函数名生成

    参数:
        hooks: 优先级、函数与声明
        stage: 生成 ID 使用的阶段，缺省时取钩子声明的阶段

    返回:
        dict[str, tuple[int, Callable, HookSpec]]: 组件 ID 到钩子
    """
    result: dict[str, tuple[int, Callable, HookSpec]] = {}
    for item in hooks:
        base = (
            item[2].component_id
            or f"legacy:{stage or item[2].stage}:{_hook_name(item[1])}"
        )
        component_id, suffix = base, 2
        while component_id in result:
            component_id = f"{base}#{suffix}"
            suffix += 1
        result[component_id] = item
    return result


def _stage_hooks() -> dict[str, tuple[int, Callable, HookSpec]]:
    return _keyed(_hooks(PriorityLifecycleType.STARTUP))


def _shutdown_hooks() -> dict[str, tuple[int, Callable, HookSpec]]:
    """关闭钩子：声明了组件 ID 的与同 ID 的启动钩子配对，其余单独作为组件

    返回:
        dict[str, tuple[int, Callable, HookSpec]]: 组件 ID 到钩子
    """
    hooks = _hooks(PriorityLifecycleType.SHUTDOWN)
    paired: dict[str, tuple[int, Callable, HookSpec]] = {}
    for item in hooks:
        if component_id := item[2].component_id:
            if component_id in paired:
                raise LifecycleError(f"component_shutdown_duplicate:{component_id}")
            paired[component_id] = item
    alone = _keyed([item for item in hooks if not item[2].component_id], "shutdown")
    return {**paired, **alone}


def _shutdown_only() -> dict[str, tuple[int, Callable, HookSpec]]:
    return {
        component_id: item
        for component_id, item in _shutdown_hooks().items()
        if not item[2].component_id
    }


def _build_start_component(
    func: Callable,
    priority: int,
    spec: HookSpec,
    component_id: str,
) -> Callable:
    async def start_component(context=None):
        try:
            return await _run_hook(
                func,
                priority,
                stage=spec.stage,
                timeout=spec.timeout,
                context=context if spec.pass_context else None,
            )
        except (Exception, HookPriorityException) as error:
            logger.error(
                f"执行启动钩子失败: {_hook_name(func)} ({type(error).__name__})",
                e=error if isinstance(error, Exception) else None,
            )
            setattr(error, "_startup_trace_logged", True)
            if owner := _plugin_owner(func):
                from zhenxun.services.startup_load import startup_load_planner

                startup_load_planner.mark_failed(
                    owner, f"plugin_lifecycle_failed:{type(error).__name__}"
                )
            elif spec.failure_policy != "fatal":
                startup_coordinator.record_error(
                    spec.stage,
                    f"{component_id}:{type(error).__name__}",
                    source_type="component",
                    source_id=component_id,
                    display_name=component_id,
                )
            raise

    return start_component


def _build_stop_component(priority: int, func: Callable, spec: HookSpec) -> Callable:
    async def stop_component() -> None:
        await _run_hook(
            func,
            priority,
            "shutdown",
            stage="shutdown",
            timeout=spec.timeout,
        )

    return stop_component


def _plugin_owner(func: Callable) -> str | None:
    """钩子所属的非核心插件

    参数:
        func: 钩子函数

    返回:
        str | None: 插件 id，核心代码或核心插件时为 None
    """
    from zhenxun.services.startup_load import startup_load_planner

    owner = startup_load_planner.owner_for_module(str(getattr(func, "__module__", "")))
    return owner if owner and not startup_load_planner.is_core_plugin(owner) else None


def _sync_kernel_declarations() -> None:
    """把优先级钩子同步为生命周期组件，运行中的组件保持不变

    关闭钩子与同 ID 的启动钩子配对；单独注册的关闭钩子作为空启动的组件，
    随 runtime 阶段启动、在关闭时执行。
    """
    shutdowns = _shutdown_hooks()
    declarations = [
        (component_id, item, shutdowns.get(component_id))
        for component_id, item in _stage_hooks().items()
    ] + [(component_id, None, item) for component_id, item in _shutdown_only().items()]
    for component_id, start, stop in declarations:
        status = lifecycle_kernel.component_status(component_id)
        if status and status["state"] not in {"declared", "stopped", "failed"}:
            continue
        priority, func, spec = cast(tuple[int, Callable, HookSpec], start or stop)
        lifecycle_kernel.register(
            ComponentSpec(
                component_id=component_id,
                stage=spec.stage,
                depends_on=spec.depends_on,
                timeout=spec.timeout,
                failure_policy="degrade"
                if _plugin_owner(func)
                else spec.failure_policy,
                config_keys=spec.config_keys,
                priority=priority,
                stop_priority=stop[0] if stop else None,
                parallel_safe=spec.parallel_safe,
            ),
            _build_start_component(func, priority, spec, component_id)
            if start
            else lambda: None,
            stop=_build_stop_component(*stop) if stop else None,
            pass_context=spec.pass_context,
            replace=status is not None,
        )


def lifecycle_component_ids(module_names: set[str]) -> set[str]:
    """模块注册的生命周期组件

    参数:
        module_names: 模块名

    返回:
        set[str]: 组件 ID
    """
    hooks = {**_stage_hooks(), **_shutdown_only()}
    return {
        component_id
        for component_id, (_, func, _) in hooks.items()
        if str(getattr(func, "__module__", "")) in module_names
    }


async def _run_stage(
    stage: StartupStage, *, excluded_components: set[str] | None = None
) -> None:
    _sync_kernel_declarations()
    component_ids = {
        component_id
        for component_id, (_, _, spec) in _stage_hooks().items()
        if spec.stage == stage
    }
    if stage == "runtime":
        component_ids.update(_shutdown_only())
    component_ids.difference_update(excluded_components or set())
    await lifecycle_kernel.start_components(component_ids)


_post_management_task: asyncio.Task[None] | None = None


async def _run_post_management() -> None:
    await startup_coordinator.wait_server_bound()
    startup_coordinator.begin_stage("runtime")
    try:
        from zhenxun.services.startup_load import startup_load_planner

        if startup_load_planner.prepared:
            from zhenxun.services.plugin_host import plugin_host

            await plugin_host.park_disabled_entries()
            await startup_load_planner.load_runtime()
            plugin_host.write_index()
        await _run_stage("runtime")
    except asyncio.CancelledError:
        raise
    except BaseException as error:
        startup_coordinator.fail_stage(
            "runtime", f"runtime_stage_failed:{type(error).__name__}", error=error
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
            "warmup",
            f"warmup_stage_failed:{type(error).__name__}",
            fatal=False,
            error=error,
        )
    else:
        startup_coordinator.finish_stage("warmup")
    from zhenxun.services.plugin_host import plugin_host

    plugin_host.schedule_sync_disabled()


@driver.on_startup
async def _start_application_lifecycle() -> None:
    global _post_management_task
    from zhenxun.services.runtime_mutation import runtime_mutation_coordinator

    runtime_mutation_coordinator.reopen()
    from zhenxun.builtin_plugins.web_ui.api.configure.setup_access import setup_access

    setup_required = setup_access.state() in {"unconfigured", "partial"}
    from zhenxun.services.nonebot_store.runtime import recovery_required

    store_recovery = recovery_required()
    if setup_required:
        startup_coordinator.enter_setup_mode()
    startup_coordinator.begin_stage("management")
    try:
        await _run_stage(
            "management",
            excluded_components={"management:database", "management:restore-import"}
            if setup_required or store_recovery
            else None,
        )
    except BaseException as error:
        startup_coordinator.fail_stage(
            "management",
            f"management_stage_failed:{type(error).__name__}",
            error=error,
        )
        raise
    startup_coordinator.finish_stage("management")
    if store_recovery:
        startup_coordinator.fail_stage(
            "runtime", "dependency_transaction_recovery_required"
        )
        return
    if setup_required:
        logger.info(
            "首次配置尚未完成，worker 已进入 setup_only 管理模式；"
            "数据库、Bot 事件和运行时插件将在配置重启后启动。",
            "Startup",
        )
        return
    startup_context = lifecycle_kernel.component_context(
        "management:runtime_concurrency"
    )
    _post_management_task = startup_context.spawn_task(
        _run_post_management(), name="zhenxun-startup-runtime"
    )


@driver.on_shutdown
async def _shutdown_application_lifecycle():
    """在关闭预算内排空运行时变更、取消启动编排并停止全部组件"""
    global _post_management_task
    from zhenxun.services.lifecycle.deadline import (
        WORKER_SHUTDOWN_SECONDS,
        remaining_timeout,
        shutdown_budget,
    )
    from zhenxun.services.runtime_mutation import runtime_mutation_coordinator

    with shutdown_budget(lifecycle_kernel.shutdown_remaining(WORKER_SHUTDOWN_SECONDS)):
        if not await runtime_mutation_coordinator.quiesce(
            timeout=remaining_timeout(10)
        ):
            logger.error("运行时变更事务未能在关闭前排空。")
        task = _post_management_task
        _post_management_task = None
        if task is not None and not task.done():
            task.cancel()
            done, _ = await asyncio.wait({task}, timeout=remaining_timeout(2.0))
            for completed in done:
                if not completed.cancelled():
                    completed.exception()
        _sync_kernel_declarations()
        await lifecycle_kernel.stop_all()
    runtime_mutation_coordinator.close()
