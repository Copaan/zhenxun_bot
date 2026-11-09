"""launcher 主循环：启动并监管 worker 与附属进程，处理重启、插件事务、更新和备份恢复"""

from __future__ import annotations

import asyncio
import contextlib
from dataclasses import dataclass
from functools import partial
import logging
import os
from pathlib import Path
import signal
import subprocess
import sys
import time
from typing import Any
import uuid

from zhenxun.configs.environment import sync_env_missing_items
from zhenxun.utils.restart_state import (
    clear_launcher_restart_signal,
    consume_launcher_action,
    restart_state_signature,
)

from . import sidecars
from .maintenance import run as run_maintenance
from .maintenance import sync_dependencies
from .sidecars import stop_process
from .supervisor import launcher_log, launcher_supervisor
from .transactions import PluginTransactions

WORKER_POLL_INTERVAL = 0.5
RESTART_POLL_INTERVAL = 0.5
# 重启状态文件变化后持续读取的秒数，覆盖重启请求的延迟生效时间
RESTART_SIGNAL_WINDOW = 2.0


@dataclass
class _Round:
    """一次 worker 运行期间的监管状态"""

    worker: subprocess.Popen
    return_code: int | None = None
    restart_requested: bool = False
    restart_action: tuple[str, list[str]] | None = None


class Launcher:
    """launcher 进程的主控制器"""

    def __init__(self, cwd: Path) -> None:
        self.cwd = cwd
        self.boot_id = ""
        self.stop_event = asyncio.Event()
        self.worker: subprocess.Popen | None = None
        self.ingress: subprocess.Popen | None = None
        self.ingress_signature: tuple | None = None
        self.transactions = PluginTransactions()
        self.pending_bot_verification = False
        self.qq_settings: Any = None
        self.webui_tls: Any = None
        self.builtin_ingress = False
        self.desired_ingress_signature: tuple | None = None
        self.next_restart_check = 0.0
        self.restart_signature: tuple[int, int] | None = None
        self.restart_window_until = 0.0

    @property
    def stopping(self) -> bool:
        return launcher_supervisor.shutdown_event.is_set()

    def stop_exit(self) -> SystemExit:
        return SystemExit(
            128 + int(launcher_supervisor.shutdown_signal or signal.SIGINT)
        )

    def on_signal(self, signum, _frame) -> None:
        if self.stopping:
            return
        launcher_supervisor.begin_shutdown(signal_number=signum)
        self.stop_event.set()
        launcher_log(f"received signal {signum}, scheduling worker shutdown")

    async def stop_sidecars(self) -> None:
        """停止 QQ ingress"""
        if self.ingress is not None:
            await stop_process(self.ingress, "QQ HTTPS ingress")
            self.ingress = None
            self.ingress_signature = None

    async def startup(self) -> None:
        """初始化 launcher：应用待处理的恢复与更新、补齐环境配置"""
        from zhenxun.services.migration.state import reconcile_terminal
        from zhenxun.services.update.update_service import (
            applied_update_pending,
            pending_job,
        )

        self.boot_id = launcher_supervisor.initialize()
        reconcile_terminal()
        from zhenxun.services.migration.restore import retry_cleanup

        await asyncio.to_thread(retry_cleanup)
        await self.apply_restore()
        self.pending_bot_verification = applied_update_pending()
        if pending_job():
            self.pending_bot_verification = (
                await run_maintenance("update-apply") or self.pending_bot_verification
            )
        sync_env_missing_items(self.cwd)
        clear_launcher_restart_signal()

    async def apply_restore(self) -> None:
        """执行待处理的备份恢复或切换数据库步骤"""
        from zhenxun.services.migration import state as restore_tasks
        from zhenxun.services.migration.restore import needs_apply

        while needs_apply():
            before = restore_tasks.load()
            launcher_log("applying backup restore before starting worker")
            await run_maintenance("restore-apply")
            after = restore_tasks.load()
            if after == before:
                raise RuntimeError("migration_maintenance_no_progress")

    async def wait_restore_import(self, worker: subprocess.Popen) -> None:
        """等待 worker 导入恢复数据，导入期间不计入就绪超时

        worker 进入 setup / 管理模式时不会导入，此时不再等待
        """
        from zhenxun.services.migration.restore import importing

        if importing():
            launcher_log("waiting for worker to import restored data")
        while True:
            if not importing() or worker.poll() is not None or self.stopping:
                break
            status = await asyncio.to_thread(
                sidecars.read_worker_status,
                self.qq_settings,
                scheme=self.webui_tls.scheme,
            )
            if (
                status
                and sidecars.bind_worker_status(worker, status)
                and status.get("operating_mode") in {"management_only", "setup_only"}
            ):
                launcher_log("worker skipped restore import, not waiting")
                break
            await asyncio.sleep(RESTART_POLL_INTERVAL)

    async def prepare_round(self) -> None:
        """应用插件事务并按最新配置调整附属进程"""
        await self.apply_restore()
        from contextlib import nullcontext

        from zhenxun.adapters.qq_official.config import (
            load_qq_launcher_settings,
            validate_builtin_ingress,
            validate_qq_config_data,
        )
        from zhenxun.configs.webui_tls import (
            certificate_sha256,
            load_webui_tls_settings,
            validate_webui_tls_settings,
        )
        from zhenxun.services.migration import restore
        from zhenxun.services.migration import state as restore_tasks

        pending = restore_tasks.load()
        scope = (
            restore_tasks.owned(pending)
            if pending
            and pending.get("record_version") == 2
            and pending["state"] in restore.RUNNING
            else nullcontext()
        )
        with scope:
            from zhenxun.services.nonebot_store.runtime import recovery_required

            if (
                pending
                and pending["state"] == "awaiting_orm"
                and restore_tasks.cancellation_requested(pending)
            ):
                restore_tasks.save(pending, state="failed_import", error="canceled")
                restore.rollback_after_stop(pending)
                pending = restore_tasks.load()
            if (
                not recovery_required()
                or (pending and pending["state"] == "awaiting_orm")
            ) and (
                not pending
                or pending["state"]
                not in {
                    "recovery_required",
                    "preparing",
                    "applying",
                }
            ):
                await self.transactions.apply()
            pending = restore_tasks.load()
            if pending and pending["state"] in {"importing", "awaiting_orm"}:
                try:
                    if not restore.defer_for_manual_orm(pending):
                        restore.dependency_ready(pending)
                        if pending["state"] == "awaiting_orm":
                            restore_tasks.save(
                                pending,
                                state="importing",
                                error=None,
                                error_code=None,
                                error_detail=None,
                            )
                except Exception as error:
                    restore_tasks.save(pending, state="failed_import", error=str(error))
                    restore.rollback_after_stop(pending)
        await self.apply_restore()
        qq = self.qq_settings = load_qq_launcher_settings(self.cwd)
        tls = self.webui_tls = load_webui_tls_settings(self.cwd)
        sidecars.certificate_pin = (
            certificate_sha256(tls.certfile) if tls.enabled else None
        )
        self.builtin_ingress = bool(
            qq.enabled
            and qq.config.has_webhook_bots
            and qq.config.qq_webhook_mode == "builtin_https"
        )
        validate_webui_tls_settings(
            tls,
            qq_https_port=(
                qq.config.qq_webhook_listen_port if self.builtin_ingress else None
            ),
        )
        self.desired_ingress_signature = (
            (
                qq.config.qq_webhook_listen_host,
                qq.config.qq_webhook_listen_port,
                qq.config.qq_webhook_tls_certfile,
                qq.config.qq_webhook_tls_keyfile,
                sidecars.worker_health_url(qq, scheme=tls.scheme),
            )
            if self.builtin_ingress
            else None
        )
        if (
            self.ingress is not None
            and self.ingress_signature != self.desired_ingress_signature
        ):
            await stop_process(self.ingress, "QQ HTTPS ingress")
            self.ingress = None
            self.ingress_signature = None
        if qq.enabled:
            validate_qq_config_data(qq.config)
        if self.builtin_ingress:
            validate_builtin_ingress(qq, check_port=self.ingress is None)

    async def spawn_worker(self) -> subprocess.Popen:
        """启动 worker

        返回:
            subprocess.Popen: worker 进程
        """
        env = os.environ.copy()
        startup_id = uuid.uuid4().hex
        env["ZHENXUN_LAUNCHER_PID"] = str(os.getpid())
        env["ZHENXUN_LAUNCHER_BOOT_ID"] = self.boot_id
        env["ZHENXUN_WORKER_STARTUP_ID"] = startup_id
        from zhenxun.services.migration.restore import bind_worker

        bind_worker(startup_id)
        from zhenxun.utils.restart_state import bind_restart_worker

        try:
            bind_restart_worker(startup_id)
        except (OSError, ValueError, RuntimeError) as error:
            launcher_log(f"restart evidence unavailable: {type(error).__name__}")
        if self.stopping:
            raise self.stop_exit()
        return await launcher_supervisor.start_process(
            "worker",
            partial(
                subprocess.Popen,
                sidecars.build_worker_command(),
                cwd=str(self.cwd),
                creationflags=sidecars.creationflags(),
                env=env,
            ),
            startup_id=startup_id,
        )

    async def verify_worker(self, worker: subprocess.Popen) -> bool:
        """验证插件事务与更新后的 worker，失败时停止 worker 并回滚

        返回:
            bool: 是否通过验证
        """
        from zhenxun.services.update.update_service import (
            finalize_applied_update,
            rollback_applied_update,
        )

        scheme = self.webui_tls.scheme
        from zhenxun.services.migration import restore
        from zhenxun.services.migration import state as restore_tasks

        pending = restore_tasks.load()
        if pending and pending["state"] in {
            "importing",
            "verifying",
            "rollback_import",
            "rollback_verifying",
            "failed_import",
        }:
            ready = pending["state"] in {
                "verifying",
                "rollback_verifying",
            } and await sidecars.wait_worker_ready(
                worker,
                self.qq_settings,
                scheme=scheme,
                require_warmup=True,
                require_healthy=True,
            )
            failed = (
                self.transactions.failures(ready)
                if self.transactions.needs_verification
                else (None if ready else [{"code": "worker_health_check_failed"}])
            )
            if failed is not None:
                await launcher_supervisor.stop_process(worker, allow_force=True)
                self.worker = None
                current = restore_tasks.load()
                if not current or any(
                    current.get(key) != pending.get(key)
                    for key in ("operation_id", "expected_worker_startup_id")
                ):
                    raise RuntimeError("migration_worker_task_changed")
                pending = current
                with restore_tasks.owned(pending):
                    restore_tasks.save(
                        pending,
                        error=pending.get("error")
                        or "migration_worker_verification_failed",
                        verification_failures=failed,
                    )
                    if pending["state"].startswith("rollback"):
                        restore_tasks.save(
                            pending,
                            state="recovery_required",
                            rollback_error="rollback_worker_verification_failed",
                        )
                    else:
                        restore.rollback_after_stop(pending)
                return False
            with restore_tasks.owned(pending):
                committed = await asyncio.to_thread(restore.commit_verified)
            if not committed:
                await launcher_supervisor.stop_process(worker, allow_force=True)
                self.worker = None
                current = restore_tasks.load()
                if not current or any(
                    current.get(key) != pending.get(key)
                    for key in ("operation_id", "expected_worker_startup_id")
                ):
                    raise RuntimeError("migration_worker_task_changed")
                pending = current
                with restore_tasks.owned(pending):
                    restore_tasks.save(pending, error="migration_canceled")
                    restore.rollback_after_stop(pending)
                return False
            self.transactions.verify_nonebot = False
            self.transactions.verify_source = False
        if self.transactions.needs_verification:
            launcher_log("waiting for managed plugin transaction verification")
            ready = await sidecars.wait_worker_ready(
                worker, self.qq_settings, scheme=scheme, require_warmup=True
            )
            failed = self.transactions.failures(ready)
            if failed is not None:
                await stop_process(worker)
                self.worker = None
                self.transactions.rollback(failed)
                return False
            self.transactions.finalize()
        if self.pending_bot_verification:
            launcher_log("waiting for updated worker health verification")
            self.pending_bot_verification = False
            if not await sidecars.wait_worker_ready(
                worker, self.qq_settings, scheme=scheme
            ):
                await stop_process(worker)
                self.worker = None
                launcher_log("updated worker failed health check, rolling back")
                rollback_applied_update()
                return False
            finalize_applied_update()
            launcher_log("updated worker passed health verification")
        return True

    async def start_ingress(self, worker: subprocess.Popen) -> None:
        """worker 业务就绪后启动 QQ HTTPS ingress"""
        qq, scheme = self.qq_settings, self.webui_tls.scheme
        launcher_log("waiting for QQ worker readiness before opening HTTPS ingress")
        if not await sidecars.wait_worker_ready(worker, qq, scheme=scheme):
            status = await asyncio.to_thread(
                sidecars.read_worker_status, qq, scheme=scheme
            )
            sidecars.bind_worker_status(worker, status)
            if status and status.get("operating_mode") in {
                "management_only",
                "setup_only",
            }:
                launcher_log(
                    "worker entered management/setup-only mode; "
                    "QQ ingress remains closed"
                )
                return
            await stop_process(worker)
            raise RuntimeError("QQ worker 未在规定时间内就绪，HTTPS Ingress 未启动")
        self.ingress = await sidecars.start_qq_ingress(qq, self.webui_tls, self.cwd)
        self.ingress_signature = self.desired_ingress_signature
        launcher_log(
            "QQ HTTPS ingress ready on "
            f"{qq.config.qq_webhook_listen_host}:{qq.config.qq_webhook_listen_port}"
        )

    def _restart_state_changed(self, now: float) -> bool:
        """重启状态文件是否在最近变化过，未变化时无需加锁读取

        参数:
            now: 当前 monotonic 时间

        返回:
            bool: 是否需要读取重启请求
        """
        signature = restart_state_signature()
        if signature != self.restart_signature:
            self.restart_signature = signature
            self.restart_window_until = now + RESTART_SIGNAL_WINDOW
        return now < self.restart_window_until

    async def _supervise_step(self, state: _Round) -> bool:
        """监管循环的一次检查

        返回:
            bool: 是否结束本轮监管
        """
        worker = state.worker
        state.return_code = worker.poll()
        if state.return_code is not None:
            return True
        if self.ingress is not None and self.ingress.poll() is not None:
            code = self.ingress.returncode
            launcher_supervisor.release(self.ingress, "unexpected_exit")
            self.ingress = None
            launcher_log("QQ HTTPS ingress exited unexpectedly")
            await stop_process(worker)
            raise SystemExit(code or 1)
        if self.stopping:
            clear_launcher_restart_signal()
            await self.stop_sidecars()
            await stop_process(worker)
            raise self.stop_exit()
        now = time.monotonic()
        if now >= self.next_restart_check:
            self.next_restart_check = now + RESTART_POLL_INTERVAL
            if self._restart_state_changed(now) and (
                action := consume_launcher_action()
            ):
                launcher_supervisor.begin_shutdown(restarting=True)
                state.restart_requested = True
                state.restart_action = action
                launcher_log("detected restart request, stopping current worker")
                await self.stop_sidecars()
                from zhenxun.services.migration.restore import business_blocked

                await launcher_supervisor.stop_process(
                    worker, allow_force=not business_blocked()
                )
                state.return_code = worker.poll()
                return True
        return False

    async def supervise(self, worker: subprocess.Popen) -> _Round:
        """监管运行中的 worker，直到退出或重启

        参数:
            worker: worker 进程

        返回:
            _Round: 本轮结束时的状态
        """
        state = _Round(worker)
        self.next_restart_check = 0.0
        self.restart_signature = None
        try:
            while True:
                if await self._supervise_step(state):
                    break
                with contextlib.suppress(asyncio.TimeoutError):
                    await asyncio.wait_for(self.stop_event.wait(), WORKER_POLL_INTERVAL)
        finally:
            await self.stop_sidecars()
            launcher_supervisor.release(
                worker, "restart" if state.restart_requested else "worker_exit"
            )
            if self.worker is worker:
                self.worker = None
        return state

    async def finish_round(self, state: _Round) -> None:
        """处理一轮监管的结果：重启前维护或退出

        参数:
            state: 本轮监管状态
        """
        from zhenxun.services.update.update_service import pending_job

        action = state.restart_action
        if not state.restart_requested:
            action = consume_launcher_action()
        if state.restart_requested or action:
            if self.stopping:
                raise self.stop_exit()
            launcher_supervisor.shutdown_deadline = None
            if action and action[0] == "sync_dependencies_restart":
                await sync_dependencies(action[1])
            await self.apply_restore()
            self.pending_bot_verification = (
                await run_maintenance("update-apply") if pending_job() else False
            )
            return
        if self.stopping:
            raise self.stop_exit()
        code = state.return_code if state.return_code is not None else 1
        launcher_log(f"worker exited unexpectedly (code={code}), launcher exiting")
        raise SystemExit(code)

    async def run(self) -> None:
        await self.startup()
        while True:
            if self.stopping or launcher_supervisor.shutdown_deadline is not None:
                raise self.stop_exit()
            await self.prepare_round()
            worker = self.worker = await self.spawn_worker()
            await self.wait_restore_import(worker)
            if not await self.verify_worker(worker):
                continue
            if self.builtin_ingress and self.ingress is None:
                await self.start_ingress(worker)
            await self.finish_round(await self.supervise(worker))


async def _launcher_entry(cwd: Path) -> None:
    launcher = Launcher(cwd)
    signals = [signal.SIGINT, signal.SIGTERM]
    if hasattr(signal, "SIGBREAK"):
        signals.append(signal.SIGBREAK)
    original = {sig: signal.getsignal(sig) for sig in signals}
    for sig in signals:
        signal.signal(sig, launcher.on_signal)
    try:
        await launcher.run()
    finally:
        original_failure = sys.exc_info()[1]
        try:
            try:
                await launcher_supervisor.shutdown()
            except Exception:
                if original_failure is None:
                    raise
                launcher_log(
                    "final cleanup failed; preserving original launcher failure"
                )
        finally:
            for sig, handler in original.items():
                signal.signal(sig, handler)


def run_launcher(cwd: Path) -> None:
    """运行 launcher 直到退出

    参数:
        cwd: 项目目录
    """
    from zhenxun.configs.webui_tls import WebUITLSConfigError

    try:
        asyncio.run(_launcher_entry(cwd))
    except WebUITLSConfigError as error:
        launcher_log(f"WebUI HTTPS 配置无效：{error}，请检查 .env.dev 的 WEBUI_* 配置")
        raise SystemExit(1) from None
    except (asyncio.CancelledError, OSError) as error:
        if launcher_supervisor.shutdown_signal is not None:
            launcher_log(
                "launcher_startup_outcome | phase=startup | "
                "code=launcher_startup_cancelled"
            )
            raise SystemExit(128 + launcher_supervisor.shutdown_signal) from None
        if isinstance(error, asyncio.CancelledError):
            raise
        code = (
            str(error)
            if str(error)
            in {"launcher_startup_interrupted", "launcher_process_start_failed"}
            else "launcher_startup_failed"
        )
        logging.getLogger(__name__).error(
            "launcher_startup_outcome | phase=startup | code=%s", code, exc_info=True
        )
        raise SystemExit(1) from None
