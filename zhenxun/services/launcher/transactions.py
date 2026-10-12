"""launcher 侧的插件事务：重启前应用、worker 启动后验证或回滚"""

from __future__ import annotations

from typing import Any

from zhenxun.services.nonebot_store import runtime as nonebot_runtime
from zhenxun.services.nonebot_store import storage as nonebot_storage
from zhenxun.services.plugin_store import plugin_store_transaction as source_store
from zhenxun.utils.restart_state import clear_pending_restart_state

from .maintenance import run as run_maintenance
from .supervisor import launcher_log


class PluginTransactions:
    """NoneBot 依赖层与真寻插件源码事务"""

    def __init__(self) -> None:
        self.verify_nonebot = False
        self.verify_source = False
        self.source_pending: dict[str, Any] | None = None

    @property
    def needs_verification(self) -> bool:
        return self.verify_nonebot or self.verify_source

    async def apply(self) -> None:
        """在启动 worker 前应用待处理事务，并记录需要验证的部分"""
        source_before = source_store.pending_transaction()
        nonebot_before = nonebot_storage.pending_transaction()
        dependencies_ready = source_store.prepare_dependency_transaction()
        nonebot_applied = False
        if dependencies_ready and nonebot_storage.pending_transaction():
            nonebot_applied = await run_maintenance("nonebot-apply")
        nonebot_after = nonebot_storage.pending_transaction()
        nonebot_failed = bool(
            nonebot_before
            and nonebot_before.get("state") == "pending_restart"
            and nonebot_after
            and nonebot_after.get("state")
            in {"failed", "migration_blocked", "recovery_required"}
        )
        source_applied = (
            await run_maintenance("source-apply")
            if dependencies_ready
            and not nonebot_failed
            and not nonebot_runtime.recovery_required()
            and source_before
            else False
        )
        source_after = source_store.pending_transaction()
        source_failed = bool(
            source_before
            and source_before.get("state") == "pending_restart"
            and source_after
            and source_after.get("state") == "failed"
        )
        if source_applied:
            launcher_log("真寻插件源码事务已应用，等待 worker 启动验证")
        if source_failed and nonebot_applied:
            rolled_back = nonebot_runtime.rollback_for_launcher()
            nonebot_applied = False
            launcher_log(
                "真寻插件源码事务应用失败；依赖回滚"
                + ("已确认" if rolled_back else "待处理")
            )
        if nonebot_applied:
            launcher_log("NoneBot 插件依赖层已构建，等待 worker 启动验证")
        recovery = nonebot_runtime.recovery_required()
        self.source_pending = source_store.pending_transaction()
        self.verify_source = bool(
            self.source_pending
            and self.source_pending.get("state") == "verification_pending"
            and not recovery
        )
        self.verify_nonebot = bool(
            nonebot_storage.load_manifest().get("pending_verification") and not recovery
        )

    def failures(self, ready: bool) -> list[dict[str, Any]] | None:
        """检查 worker 启动后的插件加载结果

        参数:
            ready: worker 是否在限定时间内完成 warmup

        返回:
            list[dict[str, Any]] | None: 失败项，验证通过时为 None
        """
        if not ready:
            launcher_log("managed plugin worker failed, rolling back transaction")
            return [{"code": "worker_health_check_failed"}]
        nonebot_ok, nonebot_result = (
            nonebot_runtime.startup_verification()
            if self.verify_nonebot
            else (True, {})
        )
        source_ok, source_result = (
            source_store.startup_verification() if self.verify_source else (True, {})
        )
        if nonebot_ok and source_ok:
            return None
        failed = [
            *(nonebot_result.get("failed") or []),
            *(source_result.get("failed") or []),
        ]
        launcher_log(
            "managed plugin verification failed, rolling back transaction: "
            + ", ".join(str(item.get("store_key", "unknown")) for item in failed)
        )
        return failed

    def rollback(self, failed: list[dict[str, Any]]) -> None:
        """回滚未通过验证的事务（worker 必须已停止）

        参数:
            failed: 失败项
        """
        rolled_back = not self.verify_nonebot or nonebot_runtime.rollback_for_launcher()
        if self.verify_source and rolled_back:
            source_store.rollback_pending_transaction(failed)

    def finalize(self) -> None:
        """确认已通过验证的事务并清除对应的待重启项"""
        if self.verify_nonebot:
            nonebot_runtime.finalize_pending_transaction()
            clear_pending_restart_state("webui.nonebot-store")
        if self.verify_source:
            source_store.finalize_pending_transaction()
            for operation in (self.source_pending or {}).get("operations", []):
                clear_pending_restart_state(
                    f"webui.plugin:{operation.get('store_key', '')}"
                )
        launcher_log("managed plugin transaction passed startup verification")
