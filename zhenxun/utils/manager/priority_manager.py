from __future__ import annotations

import asyncio
from collections.abc import Callable
from dataclasses import dataclass
import inspect
import time
from typing import ClassVar, Literal

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
    task_id: str | None = None
    depends_on: tuple[str, ...] = ()
    resource_group: str | None = None
    component_id: str | None = None
    scope: str = "infrastructure"
    restart_policy: str = "worker"
    config_keys: tuple[str, ...] = ()
    pass_context: bool = False
    health: Callable | None = None


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
        component_id: str | None = None,
        scope: str = "infrastructure",
        restart_policy: str = "worker",
        config_keys: tuple[str, ...] = (),
        pass_context: bool = False,
        health: Callable | None = None,
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
            component_id=component_id,
            scope=scope,
            restart_policy=restart_policy,
            config_keys=config_keys,
            pass_context=pass_context,
            health=health,
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
        component_id: str | None = None,
        scope: str = "infrastructure",
        restart_policy: str = "worker",
        config_keys: tuple[str, ...] = (),
        pass_context: bool = False,
        health: Callable | None = None,
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
                component_id=component_id,
                scope=scope,
                restart_policy=restart_policy,
                config_keys=config_keys,
                pass_context=pass_context,
                health=health,
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


def _stage_hooks() -> dict[str, tuple[int, Callable, HookSpec]]:
    priority_data = PriorityLifecycle._data.get(PriorityLifecycleType.STARTUP, {})
    pending: dict[str, tuple[int, Callable, HookSpec]] = {}
    for priority in sorted(priority_data):
        for func in list(priority_data[priority]):
            spec = PriorityLifecycle._metadata.get(func, HookSpec())
            base_id = (
                spec.component_id
                or spec.task_id
                or f"legacy:{spec.stage}:{_hook_name(func)}"
            )
            task_id = base_id
            suffix = 2
            while task_id in pending:
                task_id = f"{base_id}#{suffix}"
                suffix += 1
            pending[task_id] = (priority, func, spec)

    return pending


def _paired_shutdown_hooks() -> dict[str, tuple[int, Callable, HookSpec]]:
    result: dict[str, tuple[int, Callable, HookSpec]] = {}
    priority_data = PriorityLifecycle._data.get(PriorityLifecycleType.SHUTDOWN, {})
    for priority in sorted(priority_data):
        for func in list(priority_data[priority]):
            spec = PriorityLifecycle._metadata.get(func, HookSpec())
            if spec.component_id:
                if spec.component_id in result:
                    raise LifecycleError(
                        f"component_shutdown_duplicate:{spec.component_id}"
                    )
                result[spec.component_id] = (priority, func, spec)
    return result


def _shutdown_only_hooks() -> dict[str, tuple[int, Callable, HookSpec]]:
    result: dict[str, tuple[int, Callable, HookSpec]] = {}
    priority_data = PriorityLifecycle._data.get(PriorityLifecycleType.SHUTDOWN, {})
    for priority in sorted(priority_data):
        for func in list(priority_data[priority]):
            spec = PriorityLifecycle._metadata.get(func, HookSpec())
            if spec.component_id:
                continue
            base_id = f"legacy:shutdown:{_hook_name(func)}"
            component_id = base_id
            suffix = 2
            while component_id in result:
                component_id = f"{base_id}#{suffix}"
                suffix += 1
            result[component_id] = (priority, func, spec)
    return result


def _build_start_component(
    func: Callable,
    priority: int,
    spec: HookSpec,
    component_id: str,
) -> Callable:
    async def start_component(context=None):
        try:
            result = await _run_hook(
                func,
                priority,
                stage=spec.stage,
                timeout=spec.timeout,
                context=context if spec.pass_context else None,
            )
            if spec.stage == "warmup":
                from zhenxun.services.startup_load import startup_load_planner

                startup_load_planner.finish_warmup_hook(
                    str(getattr(func, "__module__", ""))
                )
            return result
        except (Exception, HookPriorityException) as error:
            logger.error(
                "执行启动钩子失败: " f"{_hook_name(func)} ({type(error).__name__})",
                e=error if isinstance(error, Exception) else None,
            )
            from zhenxun.services.lifecycle.provider_undo import active_undo
            from zhenxun.services.startup_load import startup_load_planner

            # A failed candidate belongs to the transaction, not the boot report.
            # The caller validates component state and rolls back before admission.
            if active_undo.get() is not None:
                raise

            owner = startup_load_planner.owner_for_module(
                str(getattr(func, "__module__", ""))
            )
            if owner and not startup_load_planner.is_core_plugin(owner):
                startup_load_planner.mark_failed(
                    owner, f"plugin_lifecycle_failed:{type(error).__name__}"
                )
                if spec.stage == "warmup":
                    startup_load_planner.finish_warmup_hook(
                        str(getattr(func, "__module__", ""))
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


def _build_stop_component(
    func: Callable,
    priority: int,
    spec: HookSpec,
) -> Callable:
    async def stop_component() -> None:
        await _run_hook(
            func,
            priority,
            "shutdown",
            stage="shutdown",
            timeout=spec.timeout,
        )

    return stop_component


def _sync_kernel_declarations() -> set[Callable]:
    hooks = _stage_hooks()
    shutdowns = _paired_shutdown_hooks()
    shutdown_only = _shutdown_only_hooks()
    paired: set[Callable] = {
        item[1] for item in [*shutdowns.values(), *shutdown_only.values()]
    }

    for component_id, (priority, func, spec) in hooks.items():
        status = lifecycle_kernel.component_status(component_id)
        if status and status["state"] not in {"declared", "stopped", "failed"}:
            continue

        start_component = _build_start_component(func, priority, spec, component_id)

        stop_item = shutdowns.get(component_id)
        stop_component = None
        if stop_item:
            stop_priority, stop_func, stop_spec = stop_item
            stop_component = _build_stop_component(stop_func, stop_priority, stop_spec)

        failure_policy = spec.failure_policy
        try:
            from zhenxun.services.startup_load import startup_load_planner

            owner = startup_load_planner.owner_for_module(
                str(getattr(func, "__module__", ""))
            )
            if owner and not startup_load_planner.is_core_plugin(owner):
                failure_policy = "degrade"
        except Exception:
            pass
        lifecycle_kernel.register(
            ComponentSpec(
                component_id=component_id,
                scope=spec.scope,  # type: ignore[arg-type]
                stage=spec.stage,
                depends_on=spec.depends_on,
                resource_group=spec.resource_group,
                timeout=spec.timeout,
                failure_policy=failure_policy,
                restart_policy=spec.restart_policy,  # type: ignore[arg-type]
                config_keys=spec.config_keys,
                priority=priority,
                stop_priority=stop_item[0] if stop_item else None,
                parallel_safe=spec.parallel_safe,
                source="priority_lifecycle",
            ),
            start_component,
            stop=stop_component,
            health=spec.health,
            pass_context=spec.pass_context,
            replace=status is not None,
        )
    for component_id, (priority, func, spec) in shutdown_only.items():
        status = lifecycle_kernel.component_status(component_id)
        if status and status["state"] not in {"declared", "stopped", "failed"}:
            continue
        lifecycle_kernel.register(
            ComponentSpec(
                component_id=component_id,
                scope="infrastructure",
                stage="runtime",
                timeout=spec.timeout,
                failure_policy="degrade",
                priority=priority,
                stop_priority=priority,
                source="priority_lifecycle_shutdown_only",
            ),
            lambda: None,
            stop=_build_stop_component(func, priority, spec),
            replace=status is not None,
        )
    return paired


def lifecycle_component_index() -> dict[str, set[str]]:
    result: dict[str, set[str]] = {}
    for hooks in (_stage_hooks(), _shutdown_only_hooks()):
        for component_id, (_, func, _) in hooks.items():
            module = str(getattr(func, "__module__", ""))
            result.setdefault(module, set()).add(component_id)
    return result


def lifecycle_component_ids(
    module_names: set[str], *, index: dict[str, set[str]] | None = None
) -> set[str]:
    if index is None:
        index = lifecycle_component_index()
    return {item for module in module_names for item in index.get(module, ())}


async def _run_stage(
    stage: StartupStage, *, excluded_components: set[str] | None = None
) -> None:
    _sync_kernel_declarations()
    component_ids = {
        component_id
        for component_id, (_, _, spec) in _stage_hooks().items()
        if spec.stage == stage
    }
    component_ids.update(lifecycle_kernel.native_component_ids_for_stage(stage))
    if stage == "runtime":
        component_ids.update(_shutdown_only_hooks())
    component_ids.difference_update(excluded_components or set())
    await lifecycle_kernel.start_components(component_ids)


_post_management_task: asyncio.Task[None] | None = None


async def _run_post_management() -> None:
    await startup_coordinator.wait_server_bound()
    startup_coordinator.begin_stage("runtime")
    try:
        from zhenxun.services.startup_load import startup_load_planner

        if startup_load_planner.prepared:
            await startup_load_planner.load_runtime()
            from zhenxun.services.runtime_reload import plugin_runtime_manager

            plugin_runtime_manager.activate_loaded_incarnations()
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
async def _start_application_lifecycle() -> None:
    global _post_management_task
    from zhenxun.services.runtime_mutation import runtime_mutation_coordinator

    runtime_mutation_coordinator.reopen()
    from zhenxun.builtin_plugins.web_ui.api.configure.setup_access import setup_access

    setup_required = setup_access.state() in {"unconfigured", "partial"}
    if setup_required:
        startup_coordinator.enter_setup_mode()
    startup_coordinator.begin_stage("management")
    try:
        await _run_stage(
            "management",
            excluded_components={"management:database"} if setup_required else None,
        )
    except BaseException as error:
        startup_coordinator.fail_stage(
            "management", f"management_stage_failed:{type(error).__name__}"
        )
        raise
    startup_coordinator.finish_stage("management")
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
    _post_management_task = startup_context.spawn_detached(
        _run_post_management(),
        scope_id="startup-runtime",
        scope="operation",
        name="zhenxun-startup-runtime",
    )


@driver.on_shutdown
async def _():
    from zhenxun.services.lifecycle.deadline import (
        received_shutdown_budget,
        shutdown_budget,
    )

    with shutdown_budget(received_shutdown_budget(15.0)):
        await _shutdown_with_budget()


async def _shutdown_with_budget():
    global _post_management_task
    from zhenxun.services.lifecycle.deadline import remaining_timeout
    from zhenxun.services.runtime_mutation import runtime_mutation_coordinator

    mutation_drained = await runtime_mutation_coordinator.quiesce(
        timeout=remaining_timeout(10)
    )
    if not mutation_drained:
        logger.error("运行时变更事务未能在关闭前排空，将要求 worker 恢复。")
    task = _post_management_task
    _post_management_task = None
    if task is not None and not task.done():
        task.cancel()
        done, pending = await asyncio.wait({task}, timeout=remaining_timeout(2.0))
        if pending:
            lifecycle_kernel._recovery_required.add("runtime_bootstrap")
        for completed in done:
            if not completed.cancelled():
                completed.exception()

    paired = _sync_kernel_declarations()
    await lifecycle_kernel.stop_all()
    runtime_mutation_coordinator.close()
    priority_data = PriorityLifecycle._data.get(PriorityLifecycleType.SHUTDOWN)
    if not priority_data:
        return
    for priority in sorted(priority_data):
        for func in list(priority_data[priority]):
            if func in paired:
                continue
            spec = PriorityLifecycle._metadata.get(func, HookSpec())
            try:
                await _run_hook(
                    func,
                    priority,
                    "shutdown",
                    stage="shutdown",
                    timeout=remaining_timeout(spec.timeout or 5.0),
                )
            except Exception as error:
                logger.error(
                    f"执行优先级 [{priority}] on_shutdown 方法出错: {error}",
                    e=error,
                )
