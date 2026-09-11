import asyncio
from concurrent.futures import ThreadPoolExecutor
import contextlib
from datetime import datetime, timezone
from functools import lru_cache
import os
from pathlib import Path
import signal
import threading
import time

import anyio.to_thread

from zhenxun.services.lifecycle.diagnostics import DiagnosticWorker
from zhenxun.services.log import logger
from zhenxun.services.memory_governor import (
    memory_governor_healthy,
    start_memory_governor,
    stop_memory_governor,
)
from zhenxun.services.send_queue import (
    send_queue_healthy,
    start_send_queue,
    stop_send_queue,
)
from zhenxun.services.startup import startup_coordinator
from zhenxun.services.uninfo_patch import apply_uninfo_onebot11_patch
from zhenxun.utils.manager.priority_manager import PriorityLifecycle

DEFAULT_EXECUTOR_MIN_WORKERS = 8
DEFAULT_EXECUTOR_MAX_WORKERS = 32
DEFAULT_ANYIO_MIN_TOKENS = 16
DEFAULT_ANYIO_MAX_TOKENS = 64

_thread_executor: ThreadPoolExecutor | None = None
_launcher_watchdog_task: asyncio.Task[None] | None = None
_launcher_force_exit_timer: threading.Timer | None = None
_runtime_hooks_registered = False
_alconna_patch_applied = False
_HEALTH_INTERVAL_SECONDS = 5.0
_UNOWNED_TASK_GRACE_SECONDS = 10.0
_unowned_task_seen: dict[int, float] = {}
_unowned_thread_seen: dict[int, float] = {}
_PACKAGE_ROOT = Path(__file__).resolve().parents[1]


def _clamp(value: int, minimum: int, maximum: int) -> int:
    return max(minimum, min(value, maximum))


def _get_executor_workers() -> int:
    cpu = os.cpu_count() or 4
    return _clamp(cpu * 2, DEFAULT_EXECUTOR_MIN_WORKERS, DEFAULT_EXECUTOR_MAX_WORKERS)


def _get_anyio_tokens(executor_workers: int) -> int:
    return _clamp(
        executor_workers * 2, DEFAULT_ANYIO_MIN_TOKENS, DEFAULT_ANYIO_MAX_TOKENS
    )


def _apply_alconna_conflict_patch() -> None:
    global _alconna_patch_applied
    if _alconna_patch_applied:
        return
    with contextlib.suppress(Exception):
        from arclet.alconna import formatter as alconna_formatter

        text_formatter = getattr(alconna_formatter, "TextFormatter", None)
        if text_formatter is None:
            return
        original_remove = getattr(text_formatter, "remove", None)
        if getattr(original_remove, "__zhenxun_safe_remove__", False):
            _alconna_patch_applied = True
            return

        def _safe_remove(self, base):
            # Tolerate duplicate command cleanup when formatter hash is absent.
            self.data.pop(base._hash, None)

        setattr(_safe_remove, "__zhenxun_safe_remove__", True)
        setattr(text_formatter, "remove", _safe_remove)
        _alconna_patch_applied = True


async def _launcher_watchdog_loop(launcher_pid: int) -> None:
    global _launcher_force_exit_timer
    try:
        import psutil
    except Exception:
        return
    current_pid = os.getpid()
    while True:
        await asyncio.sleep(2)
        if psutil.pid_exists(launcher_pid):
            continue
        logger.warning(
            f"检测到 launcher 进程 {launcher_pid} 已退出，worker 将主动结束...",
            "RuntimeBootstrap",
        )
        from zhenxun.services.lifecycle import lifecycle_kernel

        lifecycle_kernel.set_process_metadata(
            launcher_lost=True,
            shutdown_reason="launcher_process_missing",
            recovery_required=True,
        )
        timer = threading.Timer(15, os._exit, args=(1,))
        timer.daemon = True
        _launcher_force_exit_timer = timer
        timer.start()
        with contextlib.suppress(Exception):
            if os.name == "nt":
                signal.raise_signal(signal.SIGINT)
            else:
                os.kill(current_pid, signal.SIGTERM)
        await asyncio.Event().wait()


def _start_launcher_watchdog(context=None) -> None:
    global _launcher_watchdog_task
    if _launcher_watchdog_task is not None and not _launcher_watchdog_task.done():
        return
    launcher_pid_text = os.getenv("ZHENXUN_LAUNCHER_PID", "").strip()
    if not launcher_pid_text:
        return
    with contextlib.suppress(ValueError):
        launcher_pid = int(launcher_pid_text)
        if launcher_pid > 0:
            coroutine = _launcher_watchdog_loop(launcher_pid)
            _launcher_watchdog_task = (
                context.spawn_task(coroutine, name="launcher-watchdog")
                if context is not None
                else asyncio.create_task(coroutine, name="launcher-watchdog")
            )


def _launcher_watchdog_healthy(_value=None) -> bool:
    if not os.getenv("ZHENXUN_LAUNCHER_PID", "").strip():
        return True
    return _launcher_watchdog_task is not None and not _launcher_watchdog_task.done()


async def _stop_launcher_watchdog() -> None:
    global _launcher_force_exit_timer, _launcher_watchdog_task
    task = _launcher_watchdog_task
    _launcher_watchdog_task = None
    timer, _launcher_force_exit_timer = _launcher_force_exit_timer, None
    if timer is not None:
        timer.cancel()
    if task is None or task.done():
        return
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task


def finalize_runtime_executor() -> None:
    """Close the worker-owned default executor after all lifespan hooks."""
    global _thread_executor
    from zhenxun.services.lifecycle.deadline import (
        received_shutdown_budget,
        remaining_timeout,
    )

    executor = _thread_executor
    if executor is not None:
        from zhenxun.services.lifecycle import lifecycle_kernel

        deadline = time.monotonic() + min(
            remaining_timeout(received_shutdown_budget(2.0)),
            lifecycle_kernel.shutdown_remaining(2.0),
        )
        executor.shutdown(wait=False, cancel_futures=True)
        for thread in tuple(executor._threads):
            thread.join(max(0.0, deadline - time.monotonic()))
        if any(thread.is_alive() for thread in executor._threads):
            logger.error(
                "共享执行器仍有活动工作项 | code=shared_executor_shutdown_timeout",
                "Lifecycle",
            )
        else:
            _thread_executor = None


async def finish_runtime_workers() -> None:
    """Run after native lifespan teardown, before asyncio's runner joins its pool."""
    global _thread_executor
    from anyio._backends._asyncio import WorkerThread

    from zhenxun.services.lifecycle import lifecycle_kernel
    from zhenxun.services.lifecycle.deadline import (
        received_shutdown_budget,
        received_shutdown_request,
    )
    from zhenxun.services.shared_workers import shared_worker_threads

    lifecycle_kernel.request_shutdown()
    shutdown_request = received_shutdown_request()
    if shutdown_request.get("shutdown_id"):
        lifecycle_kernel.set_process_metadata(
            shutdown_id=shutdown_request["shutdown_id"]
        )
    remaining = min(
        lifecycle_kernel.shutdown_remaining(15), received_shutdown_budget(15)
    )
    deadline = time.monotonic() + remaining
    loop = asyncio.get_running_loop()
    executor = _thread_executor
    threads = set(shared_worker_threads())
    if executor is not None:
        executor.shutdown(wait=False, cancel_futures=True)
        threads.update(executor._threads)
        if getattr(loop, "_default_executor", None) is executor:
            # Keep ownership here; the runner's unbounded join is not our contract.
            loop._default_executor = None
            loop._executor_shutdown_called = True
    for thread in threads:
        if isinstance(thread, WorkerThread):
            thread.stop()
    # These external threads expose no awaitable completion notification.
    while any(thread.is_alive() for thread in threads) and time.monotonic() < deadline:  # noqa: ASYNC110
        await asyncio.sleep(min(0.05, max(0.0, deadline - time.monotonic())))
    remaining_threads = sum(thread.is_alive() for thread in threads)
    lifecycle_kernel.set_process_metadata(
        shared_worker_shutdown={
            "unreleased_threads": remaining_threads,
            "timed_out": bool(remaining_threads),
        }
    )
    if remaining_threads:
        lifecycle_kernel.require_recovery("infrastructure:shared_workers")
        logger.error(
            "共享工作线程尚未退出 | code=shared_worker_shutdown_timeout", "Lifecycle"
        )
    elif executor is not None:
        _thread_executor = None


def install_runtime_worker_teardown(app) -> None:
    original = app.router.lifespan_context
    if getattr(original, "__zhenxun_worker_teardown__", False):
        return
    from zhenxun.services.lifecycle import lifecycle_kernel

    lifecycle_kernel.set_process_metadata(
        role="worker",
        pid=os.getpid(),
        boot_id=startup_coordinator.boot_id,
        startup_id=os.getenv("ZHENXUN_WORKER_STARTUP_ID") or None,
        launcher_boot_id=os.getenv("ZHENXUN_LAUNCHER_BOOT_ID") or None,
    )
    lifecycle_kernel.defer_diagnostics_close()

    @contextlib.asynccontextmanager
    async def lifespan(application):
        try:
            async with original(application) as state:
                yield state
        finally:
            try:
                # A native shutdown hook can raise before NoneBot reaches the
                # kernel hook. Always release core-owned resources as well.
                try:
                    await lifecycle_kernel.stop_all()
                except Exception as error:
                    lifecycle_kernel.require_recovery("lifecycle:teardown_failed")
                    logger.error("Core lifecycle teardown failed", e=error)
                await finish_runtime_workers()
            finally:
                try:
                    if not await startup_coordinator.finish_persistence(
                        lifecycle_kernel.shutdown_remaining(15.0)
                    ):
                        lifecycle_kernel.require_recovery("startup:state_writer")
                    lifecycle_kernel.set_process_metadata(
                        startup_persistence=startup_coordinator.persistence_status()
                    )
                finally:
                    await lifecycle_kernel.finish_diagnostics()

    lifespan.__zhenxun_worker_teardown__ = True
    app.router.lifespan_context = lifespan


def _sample_process() -> dict[str, object]:
    import psutil

    from zhenxun.utils.process_tree import verified_descendants

    process = psutil.Process()
    return {
        "child_process_count": len(verified_descendants(process)),
        "rss_bytes": process.memory_info().rss,
        "process_sampled_at": datetime.now(timezone.utc).isoformat(),
    }


class _ProcessSampler:
    def __init__(self) -> None:
        self.worker = DiagnosticWorker("zhenxun-process-sampler")
        self.future: asyncio.Future | None = None
        self.latest: dict[str, object] = {
            "child_process_count": 0,
            "rss_bytes": 0,
            "process_sampled_at": None,
        }
        self.error_code: str | None = None
        self.stopped = False

    def poll(self) -> dict[str, object]:
        if self.future is not None and self.future.done():
            try:
                self.latest = self.future.result()
                self.error_code = None
            except Exception:
                self.error_code = "process_sample_failed"
            self.future = None
        if self.future is None and not self.worker.closed and not self.stopped:
            self.future = self.worker.submit(_sample_process)
        return {
            **self.latest,
            "process_sample_pending": self.future is not None,
            "process_sample_error_code": self.error_code,
            "process_sample_state": "stopped"
            if self.stopped and self.worker.released
            else "stopping"
            if self.stopped
            else "running",
            "process_sample_is_current": not self.stopped,
        }

    async def close(self) -> None:
        self.stopped = True
        released = await self.worker.close(2.0)
        from zhenxun.services.lifecycle import lifecycle_kernel

        lifecycle_kernel.set_process_metadata(**self.poll())
        if not released:
            from zhenxun.services.lifecycle.kernel import LifecycleError

            raise LifecycleError("process_sampler_shutdown_timeout")


@lru_cache(maxsize=4096)
def _is_core_task_source(filename: str, code_identity: object) -> bool:
    # Code objects change on reload; cache filesystem classification per code
    # generation rather than resolving the same path for every live task.
    try:
        return Path(filename).resolve().is_relative_to(_PACKAGE_ROOT)
    except (OSError, ValueError):
        return False


def _runtime_health_snapshot(
    loop_lag_ms: float, lifecycle_kernel=None, sampler: _ProcessSampler | None = None
) -> dict[str, object]:
    tasks = asyncio.all_tasks()
    threads = threading.enumerate()
    tracked_task_ids = (
        lifecycle_kernel.owned_task_ids() if lifecycle_kernel is not None else set()
    )
    tracked_thread_ids = (
        lifecycle_kernel.owned_thread_ids() if lifecycle_kernel is not None else set()
    )
    startup_writer = startup_coordinator._state_writer
    if startup_writer is not None:
        if startup_writer.task is not None:
            tracked_task_ids.add(id(startup_writer.task))
        tracked_thread_ids.update(
            id(thread) for thread in startup_writer.worker.threads
        )
    if sampler is not None:
        tracked_thread_ids.update(id(thread) for thread in sampler.worker.threads)
    from zhenxun.services.shared_workers import shared_worker_ids

    tracked_thread_ids.update(shared_worker_ids())
    if _thread_executor is not None:
        tracked_thread_ids.update(id(thread) for thread in _thread_executor._threads)
    now = asyncio.get_running_loop().time()
    active_ids = {id(task) for task in tasks if not task.done()}
    for identity in set(_unowned_task_seen) - (active_ids - tracked_task_ids):
        _unowned_task_seen.pop(identity, None)
    unowned: list[dict[str, object]] = []
    current = asyncio.current_task()
    for task in tasks:
        if task is current or task.done() or id(task) in tracked_task_ids:
            continue
        coroutine = task.get_coro()
        code = getattr(coroutine, "cr_code", None) or getattr(
            coroutine, "gi_code", None
        )
        filename = str(getattr(code, "co_filename", ""))
        is_zhenxun_source = _is_core_task_source(filename, code)
        if not is_zhenxun_source:
            continue
        first_seen = _unowned_task_seen.setdefault(id(task), now)
        age = now - first_seen
        if age < _UNOWNED_TASK_GRACE_SECONDS:
            continue
        if len(unowned) >= 32:
            continue
        unowned.append(
            {
                "name": task.get_name()[:100],
                "coroutine": str(
                    getattr(coroutine, "__qualname__", type(coroutine).__name__)
                )[:120],
                "age_seconds": round(age, 1),
            }
        )
    active_thread_ids = {id(thread) for thread in threads if thread.is_alive()}
    for identity in set(_unowned_thread_seen) - active_thread_ids:
        _unowned_thread_seen.pop(identity, None)
    unowned_threads: list[dict[str, object]] = []
    for thread in threads:
        if (
            thread is threading.current_thread()
            or not thread.is_alive()
            or id(thread) in tracked_thread_ids
        ):
            continue
        target = getattr(thread, "_target", None)
        target_module = str(getattr(target, "__module__", ""))
        if not target_module.startswith("zhenxun"):
            continue
        first_seen = _unowned_thread_seen.setdefault(id(thread), now)
        age = now - first_seen
        if age < _UNOWNED_TASK_GRACE_SECONDS:
            continue
        unowned_threads.append(
            {
                "name": thread.name[:100],
                "target": str(getattr(target, "__qualname__", target_module))[:120],
                "age_seconds": round(age, 1),
            }
        )
    return {
        "role": "worker",
        "boot_id": startup_coordinator.boot_id,
        "startup_id": os.getenv("ZHENXUN_WORKER_STARTUP_ID") or None,
        "runtime_sampled_at": datetime.now(timezone.utc).isoformat(),
        "pid": os.getpid(),
        "launcher_pid": os.getenv("ZHENXUN_LAUNCHER_PID") or None,
        "launcher_boot_id": os.getenv("ZHENXUN_LAUNCHER_BOOT_ID") or None,
        "event_loop_lag_ms": round(loop_lag_ms, 2),
        "asyncio_task_count": len(tasks),
        "thread_count": len(threads),
        "thread_names": sorted({thread.name for thread in threads})[:32],
        **(sampler.poll() if sampler is not None else {}),
        "owned_asyncio_task_count": len(tracked_task_ids),
        "unowned_zhenxun_tasks": unowned[:32],
        "owned_thread_count": len(tracked_thread_ids),
        "startup_persistence": startup_coordinator.persistence_status(),
        "unowned_zhenxun_threads": unowned_threads[:32],
    }


async def _lifecycle_health_loop(sampler: _ProcessSampler) -> None:
    from zhenxun.services.lifecycle import lifecycle_kernel

    loop = asyncio.get_running_loop()
    while True:
        expected = loop.time() + _HEALTH_INTERVAL_SECONDS
        await asyncio.sleep(_HEALTH_INTERVAL_SECONDS)
        started = loop.time()
        loop_lag_ms = max(0.0, (started - expected) * 1000)
        inspection_error = None
        try:
            from zhenxun.services.runtime_reload import plugin_runtime_manager

            await plugin_runtime_manager.refresh_lifecycle_scopes_async()
        except Exception:
            inspection_error = "plugin_resource_inspection_failed"
        snapshot = _runtime_health_snapshot(loop_lag_ms, lifecycle_kernel, sampler)
        inspection_ms = (loop.time() - started) * 1000
        health_started = loop.time()
        await lifecycle_kernel.check_health()
        lifecycle_kernel.set_process_metadata(
            **snapshot,
            inspection_duration_ms=round(inspection_ms, 2),
            inspection_error_code=inspection_error,
            health_check_duration_ms=round((loop.time() - health_started) * 1000, 2),
        )


def register_runtime_bootstrap(_driver) -> None:
    _apply_alconna_conflict_patch()
    apply_uninfo_onebot11_patch()
    global _runtime_hooks_registered
    if _runtime_hooks_registered:
        return
    _runtime_hooks_registered = True

    from nonebot.exception import IgnoredException
    from nonebot.message import event_preprocessor, run_postprocessor

    @run_postprocessor
    async def _record_message_failure(exception: Exception | None) -> None:
        from zhenxun.services.message_execution import current_execution

        if exception is not None and (execution := current_execution.get()):
            execution.errors.append(f"matcher:{type(exception).__name__}")

    @event_preprocessor
    async def _reject_events_until_runtime_ready() -> None:
        if not startup_coordinator.runtime_ready:
            raise IgnoredException("worker_runtime_starting")

    @PriorityLifecycle.on_startup(
        priority=-100,
        stage="management",
        timeout=10,
        component_id="management:runtime_concurrency",
        scope="worker",
    )
    async def _setup_runtime_concurrency() -> None:
        global _thread_executor
        workers = _get_executor_workers()
        loop = asyncio.get_running_loop()
        if _thread_executor is None:
            _thread_executor = ThreadPoolExecutor(
                max_workers=workers, thread_name_prefix="zhenxun-worker"
            )
        loop.set_default_executor(_thread_executor)
        with contextlib.suppress(Exception):
            limiter = anyio.to_thread.current_default_thread_limiter()
            limiter.total_tokens = _get_anyio_tokens(workers)

    @PriorityLifecycle.on_startup(
        priority=-98,
        stage="management",
        component_id="management:message_inbox",
        depends_on=("management:runtime_concurrency",),
        pass_context=True,
    )
    async def _setup_message_inbox(context):
        from zhenxun.services.message_inbox import message_inbox

        await message_inbox.start(context)

    @PriorityLifecycle.on_shutdown(
        priority=1000, component_id="management:message_inbox"
    )
    async def _stop_message_inbox():
        from zhenxun.services.message_inbox import message_inbox

        await message_inbox.close()

    @PriorityLifecycle.on_startup(
        priority=-99,
        stage="management",
        component_id="management:operations",
        scope="worker",
        depends_on=("management:runtime_concurrency",),
        pass_context=True,
        health=lambda _value: True,
    )
    async def _setup_operations(context) -> None:
        from zhenxun.services.lifecycle.operations import operation_registry

        operation_registry.bind(context)
        context.own_resource(
            receipt_id="operations:registry",
            provider="lifecycle",
            resource_type="operation_registry",
            release_check=lambda: not operation_registry.accepting
            and not operation_registry._tasks,
        )
        await operation_registry.recover_pending()

    @PriorityLifecycle.on_shutdown(priority=65, component_id="management:operations")
    async def _shutdown_operations() -> None:
        from zhenxun.services.lifecycle.operations import operation_registry

        await operation_registry.shutdown()

    @PriorityLifecycle.on_startup(
        priority=-95,
        stage="management",
        component_id="management:launcher_watchdog",
        scope="worker",
        depends_on=("management:runtime_concurrency",),
        pass_context=True,
        health=_launcher_watchdog_healthy,
    )
    async def _setup_launcher_watchdog(context) -> None:
        _start_launcher_watchdog(context)

    @PriorityLifecycle.on_shutdown(
        priority=60, component_id="management:launcher_watchdog"
    )
    async def _shutdown_launcher_watchdog() -> None:
        await _stop_launcher_watchdog()

    @PriorityLifecycle.on_startup(
        priority=-90,
        component_id="runtime:plugin_host",
        scope="worker",
        depends_on=("management:runtime_concurrency",),
        pass_context=True,
    )
    async def _setup_plugin_host(context) -> None:
        context.own_resource(
            receipt_id="plugin-host:scope-registry",
            provider="lifecycle",
            resource_type="plugin_scope_registry",
            release_check=lambda: not context._children,
        )

    @PriorityLifecycle.on_startup(
        priority=-80,
        component_id="runtime:send_queue",
        depends_on=("management:runtime_concurrency",),
        pass_context=True,
        health=send_queue_healthy,
    )
    async def _setup_send_queue(context) -> None:
        from zhenxun.services import send_queue

        context.own_resource(
            receipt_id="send-queue:adapter-patch",
            provider="nonebot",
            resource_type="adapter_patch",
            release_check=lambda: not send_queue._PATCHED,
        )
        await start_send_queue(context)

    @PriorityLifecycle.on_shutdown(priority=70, component_id="runtime:send_queue")
    async def _shutdown_send_queue() -> None:
        await stop_send_queue()

    @PriorityLifecycle.on_startup(
        priority=-79,
        component_id="runtime:memory_governor",
        depends_on=("management:runtime_concurrency",),
        pass_context=True,
        health=memory_governor_healthy,
    )
    async def _setup_memory_governor(context) -> None:
        await start_memory_governor(context)

    @PriorityLifecycle.on_shutdown(priority=69, component_id="runtime:memory_governor")
    async def _shutdown_memory_governor() -> None:
        await stop_memory_governor()

    @PriorityLifecycle.on_startup(
        priority=100,
        component_id="runtime:lifecycle_health",
        depends_on=("runtime:memory_governor",),
        failure_policy="degrade",
        pass_context=True,
    )
    async def _setup_lifecycle_health(context) -> None:
        from zhenxun.services.lifecycle import lifecycle_kernel

        sampler = _ProcessSampler()
        context.own_resource(
            receipt_id="lifecycle-health:process-sampler",
            provider="lifecycle",
            resource_type="diagnostic_worker",
            release_check=lambda: sampler.worker.released,
        )
        context.add_finalizer(sampler.close)
        lifecycle_kernel.set_process_metadata(
            **_runtime_health_snapshot(0.0, lifecycle_kernel, sampler)
        )
        context.spawn_task(_lifecycle_health_loop(sampler), name="lifecycle-health")

    @PriorityLifecycle.on_shutdown(
        priority=50, component_id="management:runtime_concurrency"
    )
    async def _shutdown_runtime_concurrency() -> None:
        # The executor remains available to native NoneBot shutdown hooks.
        # _run_worker closes it after nonebot.run() returns.
        return None
