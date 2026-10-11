from __future__ import annotations

import asyncio
from collections.abc import Callable, Coroutine
import contextlib
from contextlib import AbstractAsyncContextManager, AsyncExitStack, asynccontextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
import inspect
import os
from pathlib import Path
import time
from typing import Any, Literal

from strenum import StrEnum

from zhenxun.utils.atomic_json import write_json_locked

from .deadline import WORKER_SHUTDOWN_SECONDS, remaining_timeout, shutdown_budget

LifecycleStage = Literal["management", "runtime", "warmup"]
FailurePolicy = Literal["fatal", "degrade"]
RestartPolicy = Literal["component", "worker"]
StartCallback = Callable[..., Any]
StopCallback = Callable[[], Any]

_DEFAULT_RECEIPT_PATH = Path("data/runtime/worker-shutdown-receipt.json")
_MAX_CONCURRENT_STARTS = 4


class LifecycleError(RuntimeError):
    pass


class ComponentState(StrEnum):
    DECLARED = "declared"
    STARTING = "starting"
    READY = "ready"
    DEGRADED = "degraded"
    STOPPING = "stopping"
    STOPPED = "stopped"
    FAILED = "failed"


@dataclass(frozen=True, slots=True)
class ComponentSpec:
    component_id: str
    stage: LifecycleStage = "runtime"
    depends_on: tuple[str, ...] = ()
    timeout: float | None = None
    failure_policy: FailurePolicy = "fatal"
    restart_policy: RestartPolicy = "worker"
    config_keys: tuple[str, ...] = ()
    priority: int = 0
    stop_priority: int | None = None
    parallel_safe: bool = False
    source: str = "native"

    def __post_init__(self) -> None:
        if not self.component_id or self.component_id.strip() != self.component_id:
            raise ValueError("component_id_invalid")
        if self.component_id in self.depends_on:
            raise ValueError("component_self_dependency")
        if self.timeout is not None and self.timeout <= 0:
            raise ValueError("component_timeout_invalid")

    def public_dict(self) -> dict[str, Any]:
        return {
            "component_id": self.component_id,
            "stage": self.stage,
            "depends_on": list(self.depends_on),
            "timeout": self.timeout,
            "failure_policy": self.failure_policy,
            "restart_policy": self.restart_policy,
            "config_keys": list(self.config_keys),
            "priority": self.priority,
            "stop_priority": self.stop_priority,
            "parallel_safe": self.parallel_safe,
            "source": self.source,
        }


@dataclass(slots=True)
class LifecycleOperationResult:
    apply_effect: str
    affected_components: list[str]
    reason_codes: list[str] = field(default_factory=list)
    rollback_state: str = "none"
    runtime_generation: int = 0

    def public_dict(self) -> dict[str, Any]:
        return {
            "apply_effect": self.apply_effect,
            "affected_components": list(self.affected_components),
            "reason_codes": list(self.reason_codes),
            "rollback_state": self.rollback_state,
            "runtime_generation": self.runtime_generation,
        }


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _error_code(error: BaseException) -> str:
    if isinstance(error, LifecycleError) and str(error):
        return str(error)
    return type(error).__name__


async def run_bounded(awaitable: Any, timeout: float, error_code: str) -> Any:
    """在关闭预算内等待清理协程，超时后取消并抛出错误

    参数:
        awaitable: 清理协程
        timeout: 最长等待秒数，受当前关闭预算约束
        error_code: 超时时抛出的错误码

    返回:
        Any: 清理协程的返回值
    """
    task = asyncio.ensure_future(awaitable)
    done, _ = await asyncio.wait({task}, timeout=remaining_timeout(timeout))
    if done:
        return task.result()
    task.cancel()
    task.add_done_callback(lambda item: item.cancelled() or item.exception())
    raise LifecycleError(error_code)


class LifecycleContext:
    def __init__(self, kernel: LifecycleKernel, spec: ComponentSpec) -> None:
        self.kernel = kernel
        self.spec = spec
        self.value: Any = None
        self.accepting = True
        self._stack = AsyncExitStack()
        self._tasks: dict[asyncio.Task[Any], Callable[[], None] | None] = {}

    def _check_accepting(self) -> None:
        if not self.accepting:
            raise LifecycleError("component_scope_revoked")

    async def enter_async_context(
        self, manager: AbstractAsyncContextManager[Any]
    ) -> Any:
        """进入异步上下文，组件停止时退出

        参数:
            manager: 异步上下文管理器

        返回:
            Any: 上下文管理器的进入结果
        """
        self._check_accepting()
        return await self._stack.enter_async_context(manager)

    def add_finalizer(
        self, callback: Callable[..., Any], *args: Any, **kwargs: Any
    ) -> None:
        """注册组件停止时执行的清理回调，按注册的逆序执行

        参数:
            callback: 同步或异步清理函数
            args: 位置参数
            kwargs: 关键字参数
        """
        self._check_accepting()

        async def finalize() -> None:
            result = callback(*args, **kwargs)
            if inspect.isawaitable(result):
                await result

        self._stack.push_async_callback(finalize)

    def spawn_task(
        self,
        coroutine: Coroutine[Any, Any, Any],
        *,
        name: str | None = None,
        cancel: Callable[[], None] | None = None,
        degrade_on_error: bool = True,
    ) -> asyncio.Task[Any]:
        """创建归属于组件的后台任务，组件停止时取消

        参数:
            coroutine: 任务协程
            name: 任务名称
            cancel: 自定义取消方式，缺省时直接取消任务
            degrade_on_error: 任务异常退出时是否将组件标记为降级

        返回:
            asyncio.Task[Any]: 已创建的任务
        """
        if not self.accepting:
            coroutine.close()
            raise LifecycleError("component_scope_revoked")
        task = asyncio.create_task(coroutine, name=name)
        self._tasks[task] = cancel
        if degrade_on_error:
            task.add_done_callback(self._task_done)
        task.add_done_callback(lambda item: self._tasks.pop(item, None))
        return task

    def _task_done(self, task: asyncio.Task[Any]) -> None:
        if task.cancelled():
            return
        error = task.exception()
        if error is not None and self.accepting:
            self.kernel._task_failed(self.spec.component_id, task, error)

    async def close(self) -> None:
        self.accepting = False
        current = asyncio.current_task()
        tasks = [
            task for task in self._tasks if not task.done() and task is not current
        ]
        for task in tasks:
            cancel = self._tasks.get(task)
            if cancel is not None:
                cancel()
            else:
                task.cancel()
        error: BaseException | None = None
        if tasks:
            _, pending = await asyncio.wait(tasks, timeout=remaining_timeout(5.0))
            if pending:
                error = LifecycleError("component_task_cancel_timeout")
        try:
            await run_bounded(self._stack.aclose(), 10.0, "component_finalizer_timeout")
        except BaseException as caught:
            error = error or caught
        if error is not None:
            raise error


class _Registration:
    def __init__(
        self,
        spec: ComponentSpec,
        start: StartCallback,
        stop: StopCallback | None,
        *,
        pass_context: bool,
    ) -> None:
        self.spec = spec
        self.start = start
        self.stop = stop
        self.pass_context = pass_context
        self.state = ComponentState.DECLARED
        self.context: LifecycleContext | None = None
        self.value: Any = None
        self.error_code: str | None = None
        self.generation = 0
        self.started_at: str | None = None
        self.stopped_at: str | None = None
        self.duration_ms: float | None = None

    def public_dict(self) -> dict[str, Any]:
        return {
            **self.spec.public_dict(),
            "state": self.state.value,
            "error_code": self.error_code,
            "runtime_generation": self.generation,
            "started_at": self.started_at,
            "stopped_at": self.stopped_at,
            "duration_ms": self.duration_ms,
        }


class LifecycleKernel:
    def __init__(self, receipt_path: Path | None = None) -> None:
        self._registrations: dict[str, _Registration] = {}
        self._start_order: list[str] = []
        self._operation_lock = asyncio.Lock()
        self._generation = 0
        self._recovery_required: set[str] = set()
        self._process_metadata: dict[str, Any] = {}
        self._receipt_path = receipt_path
        self._receipt_written = False
        self._shutdown_requested = False
        self._shutdown_deadline: float | None = None
        self._starting_tasks: set[asyncio.Task[Any]] = set()

    @property
    def runtime_generation(self) -> int:
        return self._generation

    @property
    def shutdown_requested(self) -> bool:
        return self._shutdown_requested

    @property
    def process_metadata(self) -> dict[str, Any]:
        return dict(self._process_metadata)

    def register(
        self,
        spec: ComponentSpec,
        start: StartCallback,
        *,
        stop: StopCallback | None = None,
        pass_context: bool = False,
        replace: bool = False,
    ) -> None:
        """声明组件

        参数:
            spec: 组件声明
            start: 启动回调
            stop: 停止回调
            pass_context: 启动回调是否接收 LifecycleContext
            replace: 是否替换已停止的同名组件
        """
        existing = self._registrations.get(spec.component_id)
        if existing and not replace:
            if existing.start is start and existing.stop is stop:
                return
            raise LifecycleError(f"component_duplicate:{spec.component_id}")
        if existing and spec.component_id in self._start_order:
            raise LifecycleError(f"component_active:{spec.component_id}")
        self._registrations[spec.component_id] = _Registration(
            spec, start, stop, pass_context=pass_context
        )

    def set_process_metadata(self, **metadata: Any) -> None:
        """更新进程级元数据，用于关闭收据的身份标识

        参数:
            metadata: 要合并的元数据
        """
        self._process_metadata.update(metadata)

    def require_recovery(self, owner: str) -> None:
        """标记某个资源未能确认释放，关闭收据将不被确认

        参数:
            owner: 资源所属的组件或模块
        """
        self._recovery_required.add(owner)

    def unregister_declared(self, component_id: str) -> None:
        if component_id in self._start_order:
            raise LifecycleError(f"component_active:{component_id}")
        self._registrations.pop(component_id, None)
        self._recovery_required.discard(component_id)

    def unregister_components(self, component_ids: set[str]) -> None:
        for component_id in sorted(component_ids, reverse=True):
            self.unregister_declared(component_id)

    def component_context(self, component_id: str) -> LifecycleContext:
        registration = self._registrations.get(component_id)
        if registration is None or registration.context is None:
            raise LifecycleError(f"component_context_unavailable:{component_id}")
        if not registration.context.accepting:
            raise LifecycleError(f"component_scope_revoked:{component_id}")
        return registration.context

    def validate(self) -> None:
        specs = {key: value.spec for key, value in self._registrations.items()}
        for component_id, spec in specs.items():
            if missing := set(spec.depends_on) - set(specs):
                raise LifecycleError(
                    "component_dependency_missing:"
                    f"{component_id}:{','.join(sorted(missing))}"
                )
        visiting: set[str] = set()
        completed: set[str] = set()

        def visit(component_id: str) -> None:
            if component_id in completed:
                return
            if component_id in visiting:
                raise LifecycleError(f"component_dependency_cycle:{component_id}")
            visiting.add(component_id)
            for dependency in specs[component_id].depends_on:
                visit(dependency)
            visiting.remove(component_id)
            completed.add(component_id)

        for component_id in sorted(specs):
            visit(component_id)

    async def start_components(self, component_ids: set[str]) -> None:
        """按依赖和优先级启动组件，致命失败时回滚本次已启动的组件

        参数:
            component_ids: 要启动的组件
        """
        async with self._operation_lock:
            self.validate()
            if missing := component_ids - set(self._registrations):
                raise LifecycleError(f"component_unknown:{','.join(sorted(missing))}")
            pending = {
                component_id
                for component_id in component_ids
                if component_id not in self._start_order
                and self._registrations[component_id].state
                is not ComponentState.STARTING
            }
            started: list[str] = []
            try:
                await self._start_subset(pending, started)
            except BaseException:
                for component_id in reversed(started):
                    await self._stop_one(component_id, suppress_errors=True)
                raise

    async def stop_components(self, component_ids: set[str]) -> None:
        async with self._shutdown_operation_lock():
            error = None
            for component_id in self._component_stop_order(component_ids):
                try:
                    await self._stop_one(component_id, suppress_errors=False)
                except BaseException as caught:
                    error = error or caught
            if error is not None:
                raise error

    async def _start_subset(self, pending: set[str], started: list[str]) -> None:
        running: dict[asyncio.Task[None], str] = {}
        try:
            while pending or running:
                if self._shutdown_requested:
                    raise asyncio.CancelledError
                self._block_failed_dependants(pending)
                ready = sorted(
                    component_id
                    for component_id in pending
                    if all(
                        self._registrations[dependency].state is ComponentState.READY
                        for dependency in self._registrations[
                            component_id
                        ].spec.depends_on
                    )
                )
                active = [self._registrations[item].spec for item in running.values()]
                priority = (
                    active[0].priority
                    if active
                    else min(
                        (self._registrations[item].spec.priority for item in ready),
                        default=None,
                    )
                )
                exclusive = any(not spec.parallel_safe for spec in active)
                for component_id in ready:
                    spec = self._registrations[component_id].spec
                    if exclusive or len(running) >= _MAX_CONCURRENT_STARTS:
                        break
                    if spec.priority != priority or (
                        running and not spec.parallel_safe
                    ):
                        continue
                    pending.remove(component_id)
                    task = asyncio.create_task(
                        self._start_one(component_id),
                        name=f"lifecycle-start:{component_id}",
                    )
                    running[task] = component_id
                    exclusive = not spec.parallel_safe
                if not running:
                    if pending:
                        raise LifecycleError(
                            "component_dependencies_not_ready:"
                            f"{','.join(sorted(pending))}"
                        )
                    return
                done, _ = await asyncio.wait(
                    running, return_when=asyncio.FIRST_COMPLETED
                )
                fatal = None
                for task in sorted(done, key=lambda item: running[item]):
                    component_id = running.pop(task)
                    try:
                        task.result()
                    except BaseException as error:
                        fatal = fatal or error
                    if component_id in self._start_order:
                        started.append(component_id)
                if fatal is not None:
                    raise fatal
        finally:
            if running:
                for task in running:
                    task.cancel()
                done, unresolved = await asyncio.wait(
                    running, timeout=remaining_timeout(5.0)
                )
                for task in done:
                    if not task.cancelled():
                        task.exception()
                    if running[task] in self._start_order:
                        started.append(running[task])
                for task in unresolved:
                    self.require_recovery(running[task])

    def _block_failed_dependants(self, pending: set[str]) -> None:
        for component_id in sorted(pending):
            registration = self._registrations[component_id]
            if not any(
                self._registrations[dependency].state
                in {ComponentState.DEGRADED, ComponentState.FAILED}
                for dependency in registration.spec.depends_on
            ):
                continue
            pending.remove(component_id)
            registration.error_code = "component_dependency_not_ready"
            if registration.spec.failure_policy == "fatal":
                registration.state = ComponentState.FAILED
                raise LifecycleError(f"component_dependency_not_ready:{component_id}")
            registration.state = ComponentState.DEGRADED

    async def _start_one(self, component_id: str) -> None:
        if self._shutdown_requested:
            raise asyncio.CancelledError
        registration = self._registrations[component_id]
        registration.state = ComponentState.STARTING
        registration.error_code = None
        context = LifecycleContext(self, registration.spec)
        registration.context = context
        started = time.monotonic()
        task = asyncio.current_task()
        if task is not None:
            self._starting_tasks.add(task)
        try:
            result = (
                registration.start(context)
                if registration.pass_context
                else registration.start()
            )
            if inspect.isawaitable(result):
                result = await asyncio.wait_for(result, registration.spec.timeout)
            if isinstance(result, AbstractAsyncContextManager):
                result = await context.enter_async_context(result)
            if self._shutdown_requested:
                raise asyncio.CancelledError
            context.value = registration.value = result
            self._generation += 1
            registration.generation = self._generation
            registration.state = ComponentState.READY
            registration.started_at = _now()
            registration.stopped_at = None
            self._start_order.append(component_id)
        except BaseException as error:
            cancelled = isinstance(error, asyncio.CancelledError)
            if cancelled:
                registration.state = ComponentState.STOPPED
                registration.error_code = "component_start_cancelled"
            else:
                registration.state = (
                    ComponentState.DEGRADED
                    if registration.spec.failure_policy == "degrade"
                    else ComponentState.FAILED
                )
                registration.error_code = f"component_start_failed:{_error_code(error)}"
            try:
                await context.close()
            except BaseException:
                registration.state = ComponentState.FAILED
                self.require_recovery(component_id)
            registration.context = None
            registration.value = None
            if cancelled or registration.spec.failure_policy == "fatal":
                raise
        finally:
            self._starting_tasks.discard(task)  # type: ignore[arg-type]
            registration.duration_ms = round((time.monotonic() - started) * 1000, 2)

    def _task_failed(
        self, component_id: str, task: asyncio.Task[Any], error: BaseException
    ) -> None:
        registration = self._registrations.get(component_id)
        if registration is None or registration.state is not ComponentState.READY:
            return
        registration.state = ComponentState.DEGRADED
        registration.error_code = f"task_failed:{type(error).__name__}"
        from zhenxun.services.log import logger

        logger.error(
            f"组件 {component_id} 的后台任务 {task.get_name()} 异常退出",
            "Lifecycle",
            e=error if isinstance(error, Exception) else None,
        )

    async def stop_all(self, timeout: float = 15.0) -> None:
        """在关闭预算内按依赖逆序停止全部已启动组件

        参数:
            timeout: 最长关闭秒数
        """
        self.request_shutdown()
        with shutdown_budget(self.shutdown_remaining(timeout)):
            async with self._shutdown_operation_lock():
                for component_id in self._start_order:
                    context = self._registrations[component_id].context
                    if context is not None:
                        context.accepting = False
                remaining = set(self._start_order)
                while remaining:
                    ordered = self._component_stop_order(remaining)
                    key = self._stop_key(ordered[0])
                    batch = [
                        item
                        for item in ordered
                        if self._stop_key(item) == key
                        and not self._has_dependants(item, remaining)
                    ] or ordered[:1]
                    await asyncio.gather(
                        *(self._stop_one(item, suppress_errors=True) for item in batch)
                    )
                    remaining.difference_update(batch)

    def request_shutdown(self) -> None:
        """进入关闭状态，开始关闭计时并取消正在启动的组件"""
        first_request = not self._shutdown_requested
        self._shutdown_requested = True
        if self._shutdown_deadline is None:
            self._shutdown_deadline = time.monotonic() + WORKER_SHUTDOWN_SECONDS
        if not first_request:
            return
        current = None
        with contextlib.suppress(RuntimeError):
            current = asyncio.current_task()
        for task in self._starting_tasks:
            if task is not current and not task.done():
                task.cancel()

    def shutdown_remaining(self, maximum: float) -> float:
        if self._shutdown_deadline is None:
            return maximum
        return min(maximum, max(0.0, self._shutdown_deadline - time.monotonic()))

    def finalize_terminal_receipt(
        self, *, exit_signals: list[int] | None = None
    ) -> None:
        """事件循环结束后写入关闭收据，供 launcher 和迁移确认资源已释放

        参数:
            exit_signals: 进程收到的退出信号
        """
        if self._receipt_path is None or self._receipt_written:
            return
        running = sorted(
            component_id
            for component_id, registration in self._registrations.items()
            if component_id in self._start_order
            or registration.state in {ComponentState.STARTING, ComponentState.STOPPING}
        )
        recovery_required = sorted(self._recovery_required | set(running))
        metadata = self._process_metadata
        receipt = {
            "identity": {
                "pid": os.getpid(),
                "boot_id": metadata.get("boot_id"),
                "launcher_boot_id": metadata.get("launcher_boot_id"),
                "startup_id": metadata.get("startup_id"),
            },
            "recorded_at": _now(),
            "exit_signals": list(exit_signals or []),
            "result": "confirmed"
            if self._shutdown_requested and not recovery_required
            else "unconfirmed",
            "budget_remaining_ms": round(self.shutdown_remaining(15.0) * 1000, 2),
            "budget_exhausted": self.shutdown_remaining(15.0) <= 0,
            "recovery_required": recovery_required,
            "failed_components": [
                {"component_id": component_id, "error_code": registration.error_code}
                for component_id, registration in sorted(self._registrations.items())
                if registration.state is ComponentState.FAILED
            ][:16],
        }
        try:
            write_json_locked(self._receipt_path, receipt, timeout=1)
        except (OSError, TimeoutError):
            from zhenxun.services.log import logger

            logger.error("关闭收据写入失败", "Lifecycle")
        else:
            self._receipt_written = True

    @asynccontextmanager
    async def _shutdown_operation_lock(self):
        try:
            await asyncio.wait_for(
                self._operation_lock.acquire(), remaining_timeout(15)
            )
        except asyncio.TimeoutError:
            self.require_recovery("lifecycle:startup_shutdown_timeout")
            raise LifecycleError("startup_shutdown_timeout") from None
        try:
            yield
        finally:
            self._operation_lock.release()

    def _stop_key(self, component_id: str) -> int:
        stop_priority = self._registrations[component_id].spec.stop_priority
        return -1 if stop_priority is None else stop_priority

    def _has_dependants(self, component_id: str, remaining: set[str]) -> bool:
        return any(
            component_id in self._registrations[other].spec.depends_on
            for other in remaining
        )

    def _component_stop_order(self, selected: set[str]) -> list[str]:
        remaining = {item for item in selected if item in self._start_order}
        positions = {item: index for index, item in enumerate(self._start_order)}
        ordered: list[str] = []
        while remaining:
            ready = [
                item for item in remaining if not self._has_dependants(item, remaining)
            ] or list(remaining)
            selected_item = min(
                ready, key=lambda item: (self._stop_key(item), -positions[item], item)
            )
            remaining.remove(selected_item)
            ordered.append(selected_item)
        return ordered

    async def _stop_one(self, component_id: str, *, suppress_errors: bool) -> None:
        registration = self._registrations.get(component_id)
        if registration is None or component_id not in self._start_order:
            return
        registration.state = ComponentState.STOPPING
        context = registration.context
        if context is not None:
            context.accepting = False
        error: BaseException | None = None
        try:
            if registration.stop is not None:
                try:
                    result = registration.stop()
                    if inspect.isawaitable(result):
                        await run_bounded(
                            result,
                            registration.spec.timeout or 10.0,
                            "component_stop_timeout",
                        )
                except BaseException as caught:
                    error = caught
            if context is not None:
                try:
                    await context.close()
                except BaseException as caught:
                    error = error or caught
        finally:
            self._start_order.remove(component_id)
            registration.context = None
            registration.value = None
            registration.stopped_at = _now()
            registration.state = (
                ComponentState.FAILED if error else ComponentState.STOPPED
            )
        if error is None:
            return
        registration.error_code = f"component_stop_failed:{_error_code(error)}"
        self.require_recovery(component_id)
        from zhenxun.services.log import logger

        logger.error(
            f"组件 {component_id} 停止失败: {registration.error_code}",
            "Lifecycle",
            e=error if isinstance(error, Exception) else None,
        )
        if not suppress_errors:
            raise error

    async def restart_for_config(
        self,
        keys: set[str],
        *,
        apply_change: Callable[[], Any],
        rollback_change: Callable[[], Any],
    ) -> LifecycleOperationResult:
        """重启读取指定配置的组件，失败时回滚配置并恢复原组件

        参数:
            keys: 变更的配置键
            apply_change: 应用配置变更
            rollback_change: 回滚配置变更

        返回:
            LifecycleOperationResult: 组件重启结果
        """
        normalized = {key.upper() for key in keys}
        direct = {
            component_id
            for component_id, registration in self._registrations.items()
            if normalized & {key.upper() for key in registration.spec.config_keys}
        }
        if not direct:
            return LifecycleOperationResult(
                "no_change", [], runtime_generation=self._generation
            )
        affected = self._dependent_closure(direct)
        if any(
            self._registrations[item].spec.restart_policy != "component"
            for item in affected
        ):
            return LifecycleOperationResult(
                "restart_pending",
                sorted(affected),
                ["component_not_restartable"],
                runtime_generation=self._generation,
            )
        from zhenxun.services.runtime_mutation import runtime_mutation_coordinator

        async with (
            runtime_mutation_coordinator.operation("component_config"),
            self._operation_lock,
        ):
            return await self._restart_affected(
                affected, apply_change=apply_change, rollback_change=rollback_change
            )

    async def _restart_affected(
        self,
        affected: set[str],
        *,
        apply_change: Callable[[], Any],
        rollback_change: Callable[[], Any],
    ) -> LifecycleOperationResult:
        active = [item for item in self._start_order if item in affected]
        change_applied = False
        started: list[str] = []
        try:
            for component_id in reversed(active):
                await self._stop_one(component_id, suppress_errors=False)
            value = apply_change()
            if inspect.isawaitable(value):
                await value
            change_applied = True
            await self._start_subset(set(active), started)
            return LifecycleOperationResult(
                "component_restarted",
                sorted(affected),
                runtime_generation=self._generation,
            )
        except BaseException as error:
            for component_id in reversed(started):
                await self._stop_one(component_id, suppress_errors=True)
            reasons = [f"component_restart_failed:{_error_code(error)}"]
            try:
                if change_applied:
                    value = rollback_change()
                    if inspect.isawaitable(value):
                        await value
                await self._start_subset(set(active), [])
            except BaseException as rollback_error:
                reasons.append(
                    f"component_rollback_failed:{type(rollback_error).__name__}"
                )
                return LifecycleOperationResult(
                    "restart_pending",
                    sorted(affected),
                    reasons,
                    "worker_recovery_required",
                    self._generation,
                )
            return LifecycleOperationResult(
                "rolled_back", sorted(affected), reasons, "semantic", self._generation
            )

    def _dependent_closure(self, direct: set[str]) -> set[str]:
        affected = set(direct)
        while True:
            dependents = {
                component_id
                for component_id, registration in self._registrations.items()
                if set(registration.spec.depends_on) & affected
            }
            if dependents <= affected:
                return affected
            affected.update(dependents)

    def status(self) -> dict[str, Any]:
        components = [
            registration.public_dict()
            for _, registration in sorted(self._registrations.items())
        ]
        states: dict[str, int] = {}
        for component in components:
            states[component["state"]] = states.get(component["state"], 0) + 1
        from zhenxun.services.runtime_mutation import runtime_mutation_coordinator

        return {
            "runtime_generation": self._generation,
            "shutdown_requested": self._shutdown_requested,
            "component_count": len(components),
            "state_counts": dict(sorted(states.items())),
            "recovery_required": sorted(self._recovery_required),
            "current_mutation": runtime_mutation_coordinator.status(),
            "components": components,
        }

    def component_status(self, component_id: str) -> dict[str, Any] | None:
        registration = self._registrations.get(component_id)
        if registration is None:
            return None
        return {
            **registration.public_dict(),
            "recovery_required": component_id in self._recovery_required,
        }

    def native_component_ids_for_stage(self, stage: LifecycleStage) -> set[str]:
        return {
            component_id
            for component_id, registration in self._registrations.items()
            if registration.spec.stage == stage
            and registration.spec.source != "priority_lifecycle"
        }


def _receipt_path_from_environment() -> Path:
    value = os.getenv("ZHENXUN_SHUTDOWN_RECEIPT_PATH", "").strip()
    return Path(value) if value else _DEFAULT_RECEIPT_PATH


lifecycle_kernel = LifecycleKernel(_receipt_path_from_environment())
