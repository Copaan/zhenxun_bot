"""launcher 主循环中的迁移步骤：导出快照、恢复执行与原 worker 续跑"""

from __future__ import annotations

import asyncio
import subprocess
from typing import TYPE_CHECKING

from zhenxun.services.migration.errors import MigrationError

from . import sidecars
from .sidecars import launcher_log, stop_process
from .supervisor import launcher_supervisor

if TYPE_CHECKING:
    from .main import Launcher, _Round


async def resume_export(launcher: Launcher, worker: subprocess.Popen) -> bool:
    """迁移导出快照后等待原 worker 恢复，失败时进入迁移恢复

    参数:
        launcher: launcher 主控制器
        worker: 恢复运行的 worker

    返回:
        bool: worker 是否已恢复
    """
    export = launcher.migration_export
    try:
        ready = await launcher.migration._wait(
            lambda: sidecars.wait_worker_ready(
                worker,
                launcher.qq_settings,
                scheme=launcher.webui_tls.scheme,
                require_warmup=True,
            ),
            export.budget.phase(),
        )
        if not ready:
            raise MigrationError("migration_original_worker_resume_failed")
        export.resumed(worker)
    except Exception:
        export.fail("migration_original_worker_resume_failed", recovery=True)
        await launcher.stop_sidecars()
        await stop_process(worker)
        launcher.worker = None
        await launcher.migration.recover_with_management()
        launcher.migration_export = None
        return False
    if launcher.webui_dev is not None:
        try:
            await launcher.webui_dev.start()
        except Exception as error:
            launcher_log(f"Bot resumed; WebUI dev service unavailable: {error}")
    return True


async def _wait_restore_recovery(launcher: Launcher, state: _Round) -> bool:
    """迁移恢复中断后保持维护监听，等待重新授权并完成恢复

    返回:
        bool: 是否继续监管当前 worker
    """
    restore, migration = state.restore, launcher.migration
    stage = migration.store.read("jobs", restore.identity)["stage"]
    if stage not in {"failed", "rolled_back", "cancelled", "needs_preflight"}:
        while await migration.wait_for_reauthorization(launcher.stop_event):
            try:
                await migration.recover_before_startup()
            except MigrationError as error:
                migration.recovery_failed(restore.identity, error)
                continue
            if migration.store.active() is None:
                break
        if launcher.stopping:
            raise launcher.stop_exit()
    if restore.maintenance is not None:
        await restore.maintenance.close()
    state.resume_original = restore.stopped
    if state.resume_original:
        launcher.session_resume_id = restore.identity
        return False
    state.restore = None
    state.preparation = None
    return True


async def check_restore(launcher: Launcher, state: _Round) -> bool:
    """推进排队中的迁移恢复

    参数:
        launcher: launcher 主控制器
        state: 本轮监管状态

    返回:
        bool: 是否结束本轮监管
    """
    if state.restore is None:
        state.restore = launcher.migration.take_restore()
        if state.restore is not None:
            state.preparation = asyncio.create_task(
                state.restore.prepare(), name="migration-restore-prepare"
            )
    if state.preparation is None or not state.preparation.done():
        return False
    try:
        state.preparation.result()
    except BaseException as error:
        await state.restore.fail(
            getattr(error, "code", "migration_dependency_prepare_failed")
        )
        state.restore = None
        state.preparation = None
        return False
    try:
        launcher.adopted_worker = await state.restore.execute(
            worker=state.worker,
            network=launcher.webui_tls,
            prepared=True,
            stop_ingress=launcher.stop_for_handoff,
        )
    except MigrationError:
        if await _wait_restore_recovery(launcher, state):
            return False
    if launcher.adopted_worker is not None:
        launcher.migration._validation = None
        state.restore.validation.private.clear()
    state.return_code = state.worker.poll()
    return True


async def check_export(launcher: Launcher, state: _Round) -> bool | None:
    """开始排队中的迁移导出：停止 worker 以制作快照

    参数:
        launcher: launcher 主控制器
        state: 本轮监管状态

    返回:
        bool | None: True 结束本轮监管，False 继续监管，None 无导出任务
    """
    from zhenxun.services.lifecycle.deadline import shutdown_budget
    from zhenxun.services.migration.launcher import stop_worker_for_snapshot

    export = launcher.migration_export = launcher.migration.take_export()
    if export is None:
        return None
    if not export.begin(network=launcher.webui_tls):
        launcher.migration_export = None
        return None
    options = export.store.read("jobs", export.identity)["options"]
    with shutdown_budget(15):
        await launcher.stop_for_handoff()
        state.shutdown_result = await stop_worker_for_snapshot(
            state.worker,
            launcher_supervisor,
            allow_forced_shutdown=options.get("allow_forced_shutdown") is True,
        )
    if not state.shutdown_result.get("process_tree_released"):
        export.store.record_shutdown(export.identity, state.shutdown_result)
        export.fail("migration_shutdown_unconfirmed", recovery=True)
        state.failed_export_id = export.identity
        launcher.migration_export = None
        state.restart_requested = True
        return False
    state.snapshot = True
    state.return_code = state.worker.poll()
    return True


async def finish_export(launcher: Launcher, state: _Round) -> None:
    """worker 停止后制作迁移快照，失败时进入迁移恢复"""
    export = launcher.migration_export
    try:
        await export.snapshot_after_shutdown(
            state.shutdown_result, network=launcher.webui_tls
        )
    except Exception as error:
        export.fail(getattr(error, "code", "migration_snapshot_failed"), recovery=True)
        await launcher.migration.recover_with_management()
        launcher.migration_export = None
