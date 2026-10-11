"""launcher 主循环：启动并监管 worker 与附属进程，处理重启、插件事务、更新和迁移"""

from __future__ import annotations

import asyncio
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
)

from . import migration as migration_steps
from . import sidecars
from .maintenance import run as run_maintenance
from .maintenance import sync_dependencies
from .sidecars import launcher_log, stop_process
from .supervisor import launcher_supervisor
from .transactions import PluginTransactions

WORKER_POLL_INTERVAL = 0.1
RESTART_POLL_INTERVAL = 0.5


@dataclass
class _Round:
    """一次 worker 运行期间的监管状态"""

    worker: subprocess.Popen
    return_code: int | None = None
    restart_requested: bool = False
    restart_action: tuple[str, list[str]] | None = None
    snapshot: bool = False
    shutdown_result: dict[str, Any] | None = None
    failed_export_id: str | None = None
    restore: Any = None
    preparation: asyncio.Task | None = None
    resume_original: bool = False


class Launcher:
    """launcher 进程的主控制器"""

    def __init__(self, cwd: Path, webui_dev_source: Path | None) -> None:
        self.cwd = cwd
        self.webui_dev_source = webui_dev_source
        self.webui_dev = None
        self.boot_id = ""
        self.stop_event = asyncio.Event()
        self.worker: subprocess.Popen | None = None
        self.ingress: subprocess.Popen | None = None
        self.ingress_signature: tuple | None = None
        self.http_sidecar: subprocess.Popen | None = None
        self.http_sidecar_signature: tuple | None = None
        self.http_retry_index = 0
        self.next_http_retry = 0.0
        self.transactions = PluginTransactions()
        self.pending_bot_verification = False
        self.migration: Any = None
        self.migration_export: Any = None
        self.session_resume_id: str | None = None
        self.adopted_worker: subprocess.Popen | None = None
        self.qq_settings: Any = None
        self.webui_tls: Any = None
        self.builtin_ingress = False
        self.desired_http_signature: tuple | None = None
        self.desired_ingress_signature: tuple | None = None
        self.next_restart_check = 0.0

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
        """停止 QQ ingress 与 WebUI HTTP 兼容入口"""
        if self.ingress is not None:
            await stop_process(self.ingress, "QQ HTTPS ingress")
            self.ingress = None
            self.ingress_signature = None
        if self.http_sidecar is not None:
            await stop_process(self.http_sidecar, "WebUI HTTP sidecar")
            self.http_sidecar = None
            self.http_sidecar_signature = None

    async def stop_for_handoff(self) -> None:
        """迁移接管前停止前端开发服务与附属进程"""
        if self.webui_dev is not None:
            await self.webui_dev.stop()
        await self.stop_sidecars()

    async def startup(self) -> None:
        """初始化 launcher：恢复迁移、应用待处理更新、补齐环境配置"""
        from zhenxun.services.migration.launcher import install_launcher_migration
        from zhenxun.services.migration.lease import (
            current_instance_lease,
            require_resolved_restore,
        )
        from zhenxun.services.update.update_service import (
            applied_update_pending,
            pending_job,
        )

        self.boot_id = launcher_supervisor.initialize()
        self.migration = await install_launcher_migration(
            current_instance_lease(), launcher_supervisor
        )
        if self.migration is not None:
            try:
                await self.migration.recover_with_management()
            except Exception as error:
                if getattr(error, "code", None) == "migration_launcher_stopping":
                    raise self.stop_exit() from None
                raise
            self.adopted_worker = self.migration.recovered_worker
        require_resolved_restore(self.cwd)
        self.pending_bot_verification = applied_update_pending()
        if pending_job():
            self.pending_bot_verification = (
                await run_maintenance("update-apply") or self.pending_bot_verification
            )
        sync_env_missing_items(self.cwd)
        clear_launcher_restart_signal()
        if self.webui_dev_source is not None:
            from zhenxun.services.webui_dev import WebUIDevServer

            self.webui_dev = WebUIDevServer(self.webui_dev_source, launcher_supervisor)

    async def prepare_round(self) -> None:
        """应用插件事务并按最新配置调整附属进程"""
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

        if self.worker is None and self.http_sidecar is not None:
            await stop_process(self.http_sidecar, "WebUI HTTP sidecar")
            self.http_sidecar = None
            self.http_sidecar_signature = None
        await self.transactions.apply()
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
            launcher_managed=True,
            check_redirect_port=self.http_sidecar is None,
        )
        self.desired_http_signature = sidecars.http_sidecar_signature(tls)
        if (
            self.http_sidecar is not None
            and self.http_sidecar_signature != self.desired_http_signature
        ):
            await stop_process(self.http_sidecar, "WebUI HTTP sidecar")
            self.http_sidecar = None
            self.http_sidecar_signature = None
        if not tls.http_sidecar_enabled:
            sidecars.publish_http_sidecar_state(
                mode="disabled",
                port=None,
                pid=None,
                state="disabled",
                active_connections=0,
                last_error=None,
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
        """启动 worker；迁移恢复出的 worker 直接接管

        返回:
            subprocess.Popen: worker 进程
        """
        env = os.environ.copy()
        env.pop("ZHENXUN_WEBUI_DEV_PORT", None)
        env.pop("ZHENXUN_WEBUI_DEV_TOKEN", None)
        if self.webui_dev is not None:
            env.update(self.webui_dev.worker_environment())
            if self.migration_export is None:
                await self.webui_dev.start()
        startup_id = uuid.uuid4().hex
        env["ZHENXUN_LAUNCHER_PID"] = str(os.getpid())
        env["ZHENXUN_LAUNCHER_BOOT_ID"] = self.boot_id
        env["ZHENXUN_WORKER_STARTUP_ID"] = startup_id
        if self.migration_export is not None:
            env.update(self.migration_export.resume_environment(startup_id))
        if self.session_resume_id is not None:
            from zhenxun.utils.atomic_json import write_json_locked

            write_json_locked(
                self.migration.store.path("jobs", self.session_resume_id).parent
                / "session-authorization.json",
                {
                    "job_id": self.session_resume_id,
                    "startup_id": startup_id,
                    "launcher_boot_id": self.boot_id,
                },
            )
            env["ZHENXUN_MIGRATION_SESSION_ID"] = self.session_resume_id
            self.session_resume_id = None
        if self.stopping:
            raise self.stop_exit()
        if self.adopted_worker is not None:
            worker, self.adopted_worker = self.adopted_worker, None
            if worker.poll() is not None:
                raise RuntimeError("migration promoted worker exited before adoption")
            return worker
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

    def _http_sidecar_failed(self, delay: float) -> None:
        self.next_http_retry = time.monotonic() + delay
        sidecars.publish_http_sidecar_state(
            state="degraded",
            active_connections=0,
            retry_in_seconds=delay,
            next_retry_at=time.time() + delay,
            retry_count=self.http_retry_index,
        )

    async def start_http_sidecar(self, worker: subprocess.Popen) -> None:
        """worker 的 WebUI 可访问后启动 HTTP 兼容入口"""
        tls = self.webui_tls
        if not await sidecars.wait_worker_webui_ready(
            worker, self.qq_settings, scheme=tls.scheme
        ):
            sidecars.publish_http_sidecar_state(
                mode=tls.effective_http_mode,
                port=tls.redirect_port,
                pid=None,
                last_error="https_worker_not_ready",
                error_code="https_worker_not_ready",
                stage="upstream_readiness",
                errno=None,
            )
            self._http_sidecar_failed(1.0)
            launcher_log("WebUI HTTP sidecar is degraded: HTTPS worker is not ready")
            return
        self.http_sidecar = await sidecars.start_http_sidecar(tls, self.cwd)
        if self.http_sidecar is None:
            self._http_sidecar_failed(1.0)
        else:
            self.http_sidecar_signature = self.desired_http_signature
            self.http_retry_index = 0

    async def retry_http_sidecar(self, worker: subprocess.Popen) -> None:
        if await sidecars.worker_webui_ready_once(
            worker, self.qq_settings, scheme=self.webui_tls.scheme
        ):
            self.http_sidecar = await sidecars.start_http_sidecar(
                self.webui_tls, self.cwd
            )
        if self.http_sidecar is not None:
            self.http_sidecar_signature = self.desired_http_signature
            self.http_retry_index = 0
            self.next_http_retry = 0.0
            return
        delay = sidecars.HTTP_SIDECAR_RETRY_DELAYS[
            min(self.http_retry_index, len(sidecars.HTTP_SIDECAR_RETRY_DELAYS) - 1)
        ]
        self.http_retry_index += 1
        self._http_sidecar_failed(delay)

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

    async def _supervise_step(self, state: _Round) -> bool:
        """监管循环的一次检查

        返回:
            bool: 是否结束本轮监管
        """
        worker = state.worker
        if self.migration_export is not None and self.migration_export.collect():
            self.migration_export = None
        state.return_code = worker.poll()
        if state.return_code is not None:
            if state.failed_export_id is not None:
                handle = launcher_supervisor._handles.get(worker.pid)
                if handle is not None and handle._live_processes(discover=True):
                    if self.stopping:
                        raise self.stop_exit()
                    return False
            if self.migration_export is not None:
                await self.migration_export.interrupt("migration_resumed_worker_exited")
            return True
        if self.ingress is not None and self.ingress.poll() is not None:
            code = self.ingress.returncode
            launcher_supervisor.release(self.ingress, "unexpected_exit")
            self.ingress = None
            launcher_log("QQ HTTPS ingress exited unexpectedly")
            await stop_process(worker)
            raise SystemExit(code or 1)
        if self.http_sidecar is not None and self.http_sidecar.poll() is not None:
            delay = sidecars.http_sidecar_exited(
                self.http_sidecar, self.http_retry_index
            )
            self.http_sidecar = None
            self.http_sidecar_signature = None
            self.http_retry_index += 1
            self.next_http_retry = time.monotonic() + delay
        if self.stopping:
            if state.preparation is not None:
                self.migration.store.request_cancel(state.restore.identity)
                state.preparation.cancel()
                await asyncio.gather(state.preparation, return_exceptions=True)
            if self.migration_export is not None:
                await self.migration_export.interrupt("migration_launcher_stopping")
            clear_launcher_restart_signal()
            await self.stop_sidecars()
            await stop_process(worker)
            raise self.stop_exit()
        now = time.monotonic()
        if self.migration is not None and self.migration_export is None:
            if await migration_steps.check_restore(self, state):
                return True
            result = await migration_steps.check_export(self, state)
            if result is not None:
                return result
        if (
            self.migration_export is None
            and state.restore is None
            and state.failed_export_id is None
            and now >= self.next_restart_check
        ):
            self.next_restart_check = now + RESTART_POLL_INTERVAL
            if action := consume_launcher_action():
                launcher_supervisor.begin_shutdown(restarting=True)
                state.restart_requested = True
                state.restart_action = action
                launcher_log("detected restart request, stopping current worker")
                await self.stop_sidecars()
                await stop_process(worker)
                state.return_code = worker.poll()
                return True
        if (
            self.webui_tls.http_sidecar_enabled
            and self.http_sidecar is None
            and state.failed_export_id is None
            and now >= self.next_http_retry
        ):
            await self.retry_http_sidecar(worker)
        return False

    async def supervise(self, worker: subprocess.Popen) -> _Round:
        """监管运行中的 worker，直到退出、重启或交给迁移

        参数:
            worker: worker 进程

        返回:
            _Round: 本轮结束时的状态
        """
        state = _Round(worker)
        self.next_restart_check = 0.0
        if self.migration is not None:
            self.migration.bind_export_worker(worker)
        try:
            while True:
                if await self._supervise_step(state):
                    break
                await asyncio.sleep(WORKER_POLL_INTERVAL)
        except Exception as error:
            export = self.migration_export
            if (
                export is None
                or self.migration.store.read("jobs", export.identity)["stage"]
                != "quiescing"
            ):
                raise
            export.fail(
                getattr(error, "code", "migration_shutdown_unconfirmed"), recovery=True
            )
            state.snapshot = True
            state.shutdown_result = {"result": "unconfirmed"}
        finally:
            if state.preparation is not None and not state.preparation.done():
                state.preparation.cancel()
                await asyncio.gather(state.preparation, return_exceptions=True)
            if self.migration is not None:
                self.migration.bind_export_worker(None)
            if self.migration_export is not None and not state.snapshot:
                if not self.migration_export.collect():
                    await self.migration_export.interrupt(
                        "migration_launcher_interrupted"
                    )
            await self.stop_sidecars()
            if self.webui_tls.http_sidecar_enabled:
                sidecars.publish_http_sidecar_state(
                    state="stopped",
                    pid=None,
                    active_connections=0,
                    retry_in_seconds=None,
                )
            launcher_supervisor.release(
                worker, "restart" if state.restart_requested else "worker_exit"
            )
            if self.worker is worker:
                self.worker = None
        return state

    async def finish_round(self, state: _Round) -> None:
        """处理一轮监管的结果：迁移快照、重启前维护或退出

        参数:
            state: 本轮监管状态
        """
        from zhenxun.services.migration.lease import require_resolved_restore
        from zhenxun.services.update.update_service import pending_job

        if state.failed_export_id is not None:
            if self.stopping:
                raise self.stop_exit()
            await self.migration.recover_with_management()
            require_resolved_restore(self.cwd)
            return
        if self.adopted_worker is not None or state.resume_original:
            return
        if state.snapshot:
            await migration_steps.finish_export(self, state)
            return
        action = state.restart_action
        if not state.restart_requested:
            action = consume_launcher_action()
        if state.restart_requested or action:
            if self.stopping:
                raise self.stop_exit()
            launcher_supervisor.shutdown_deadline = None
            if action and action[0] == "sync_dependencies_restart":
                await sync_dependencies(action[1])
            self.pending_bot_verification = (
                await run_maintenance("update-apply") if pending_job() else False
            )
            return
        raise SystemExit(state.return_code if state.return_code is not None else 1)

    async def run(self) -> None:
        await self.startup()
        while True:
            if self.stopping or launcher_supervisor.shutdown_deadline is not None:
                raise self.stop_exit()
            await self.prepare_round()
            worker = self.worker = await self.spawn_worker()
            if self.webui_tls.http_sidecar_enabled and self.http_sidecar is None:
                await self.start_http_sidecar(worker)
            if not await self.verify_worker(worker):
                continue
            if self.builtin_ingress and self.ingress is None:
                await self.start_ingress(worker)
            if self.migration_export is not None and not (
                await migration_steps.resume_export(self, worker)
            ):
                continue
            await self.finish_round(await self.supervise(worker))


async def _launcher_entry(cwd: Path, webui_dev_source: Path | None) -> None:
    launcher = Launcher(cwd, webui_dev_source)
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


def run_launcher(cwd: Path, webui_dev_source: Path | None = None) -> None:
    """运行 launcher 直到退出

    参数:
        cwd: 项目目录
        webui_dev_source: 前端源码目录，为 None 时不启用前端热更新
    """
    try:
        asyncio.run(_launcher_entry(cwd, webui_dev_source))
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
