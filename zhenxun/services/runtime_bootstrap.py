import asyncio
from concurrent.futures import ThreadPoolExecutor
import contextlib
import os
import signal
import threading
import time

import anyio.to_thread

from zhenxun.services.log import install_asyncio_exception_handler, logger
from zhenxun.services.memory_governor import start_memory_governor, stop_memory_governor
from zhenxun.services.send_queue import start_send_queue, stop_send_queue
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


def _apply_superuser_patch() -> None:
    """让 NoneBot 的 SUPERUSER 权限按平台判定超级用户"""
    from nonebot.permission import SuperUser

    from zhenxun.utils.platform import PlatformUtils

    async def _call(self, bot, event) -> bool:
        try:
            user_id = event.get_user_id()
        except Exception:
            return False
        return PlatformUtils.is_superuser(bot, user_id)

    SuperUser.__call__ = _call


async def _launcher_watchdog_loop(launcher_pid: int) -> None:
    global _launcher_force_exit_timer
    try:
        import psutil
    except Exception:
        return
    current_pid = os.getpid()
    loop = asyncio.get_running_loop()
    exited = loop.create_future()

    def wait_launcher() -> None:
        with contextlib.suppress(psutil.Error):
            psutil.Process(launcher_pid).wait()
        with contextlib.suppress(RuntimeError):
            loop.call_soon_threadsafe(lambda: exited.done() or exited.set_result(None))

    threading.Thread(
        target=wait_launcher, name="launcher-watchdog", daemon=True
    ).start()
    await exited
    logger.warning(
        f"检测到 launcher 进程 {launcher_pid} 已退出，worker 将主动结束...",
        "RuntimeBootstrap",
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
    """worker 退出前关闭 lifespan 未能回收的共享线程池，并取消排队中的任务"""
    global _thread_executor
    if _thread_executor is not None:
        _thread_executor.shutdown(wait=False, cancel_futures=True)
        _thread_executor = None


async def finish_runtime_workers() -> None:
    """Run after native lifespan teardown, before asyncio's runner joins its pool."""
    global _thread_executor
    from anyio._backends._asyncio import WorkerThread

    from zhenxun.services.lifecycle import lifecycle_kernel

    lifecycle_kernel.request_shutdown()
    remaining = lifecycle_kernel.shutdown_remaining(15)
    deadline = time.monotonic() + remaining
    loop = asyncio.get_running_loop()
    executor = _thread_executor
    threads: set[threading.Thread] = set()
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
    if any(thread.is_alive() for thread in threads):
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

    @contextlib.asynccontextmanager
    async def lifespan(application):
        try:
            async with original(application) as state:
                yield state
        finally:
            # A native shutdown hook can raise before NoneBot reaches the
            # kernel hook. Always release core-owned resources as well.
            try:
                await lifecycle_kernel.stop_all()
            except Exception as error:
                logger.error("Core lifecycle teardown failed", e=error)
            await finish_runtime_workers()

    lifespan.__zhenxun_worker_teardown__ = True
    app.router.lifespan_context = lifespan


def register_runtime_bootstrap(_driver) -> None:
    _apply_alconna_conflict_patch()
    _apply_superuser_patch()
    # Auth hooks can register before library plugins are loaded.  Importing
    # Uninfo here marks it as a normal module and prevents ``nonebot.require``
    # from loading it as a plugin later.  The CLI applies the patch again
    # after requiring the library plugins.
    import nonebot

    if nonebot.get_plugin("nonebot_plugin_uninfo") is not None:
        apply_uninfo_onebot11_patch()
    global _runtime_hooks_registered
    if _runtime_hooks_registered:
        return
    _runtime_hooks_registered = True

    from nonebot.exception import IgnoredException
    from nonebot.message import event_preprocessor

    @event_preprocessor
    async def _reject_events_until_runtime_ready() -> None:
        if not startup_coordinator.runtime_ready:
            raise IgnoredException("worker_runtime_starting")

    @PriorityLifecycle.on_startup(
        priority=-100,
        stage="management",
        timeout=10,
        component_id="management:runtime_concurrency",
    )
    async def _setup_runtime_concurrency() -> None:
        global _thread_executor
        workers = _get_executor_workers()
        loop = asyncio.get_running_loop()
        install_asyncio_exception_handler(loop)
        if _thread_executor is None:
            _thread_executor = ThreadPoolExecutor(
                max_workers=workers, thread_name_prefix="zhenxun-worker"
            )
        loop.set_default_executor(_thread_executor)
        with contextlib.suppress(Exception):
            limiter = anyio.to_thread.current_default_thread_limiter()
            limiter.total_tokens = _get_anyio_tokens(workers)

    @PriorityLifecycle.on_startup(
        priority=-95,
        stage="management",
        component_id="management:launcher_watchdog",
        depends_on=("management:runtime_concurrency",),
        pass_context=True,
    )
    async def _setup_launcher_watchdog(context) -> None:
        _start_launcher_watchdog(context)

    @PriorityLifecycle.on_shutdown(
        priority=60, component_id="management:launcher_watchdog"
    )
    async def _shutdown_launcher_watchdog() -> None:
        await _stop_launcher_watchdog()

    @PriorityLifecycle.on_startup(
        priority=-80,
        component_id="runtime:send_queue",
        depends_on=("management:runtime_concurrency",),
        pass_context=True,
    )
    async def _setup_send_queue(context) -> None:
        await start_send_queue(context)

    @PriorityLifecycle.on_shutdown(priority=70, component_id="runtime:send_queue")
    async def _shutdown_send_queue() -> None:
        await stop_send_queue()

    @PriorityLifecycle.on_startup(
        priority=-79,
        component_id="runtime:memory_governor",
        depends_on=("management:runtime_concurrency",),
        pass_context=True,
    )
    async def _setup_memory_governor(context) -> None:
        await start_memory_governor(context)

    @PriorityLifecycle.on_shutdown(priority=69, component_id="runtime:memory_governor")
    async def _shutdown_memory_governor() -> None:
        await stop_memory_governor()
