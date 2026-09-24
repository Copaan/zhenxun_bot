from __future__ import annotations

import sys

STAGES = {
    "queued": "等待执行",
    "preparing": "准备中",
    "quiescing": "停止业务",
    "snapshotting": "创建快照",
    "resuming": "恢复原实例",
    "compressing": "压缩校验",
    "applying": "应用迁移",
    "verifying": "启动与完整性验证",
    "committing": "提交中",
    "committed": "已提交，等待收尾",
    "completed": "完成",
    "failed": "失败",
    "partial": "部分完成",
    "rolling_back": "回滚中",
    "rolled_back": "已回滚",
    "recovery_required": "恢复受阻",
    "cancelled": "已取消",
    "awaiting_credentials": "等待授权",
    "needs_preflight": "需要重新预检",
}


def emit_progress(job: dict, *, stage_only: bool = False) -> None:
    """Write one safe task summary to stderr, preserving CLI JSON stdout."""
    value = job.get("progress", {}).get("runtime", {})
    stage = job.get("stage", "queued")
    parts = [f"[迁移 {job['id'][:8]}]", STAGES.get(stage, stage)]
    if not stage_only and value.get("stage") == stage:
        parts.append(value.get("step") or "处理中")
        if value.get("overall_percent") is not None:
            parts.append(f"流程估算 {value['overall_percent']:.1f}%")
        if value.get("current") is not None:
            total = value.get("total")
            parts.append(
                f"{value['current']}/{total if total is not None else '未知'} "
                f"{value.get('unit') or '项'}"
            )
        if value.get("bytes_done") is not None:
            total_bytes = value.get("bytes_total")
            parts.append(
                f"已处理 {value['bytes_done']}"
                + (f"/{total_bytes}" if total_bytes is not None else "")
                + " bytes"
            )
        parts.append(f"步骤耗时 {value.get('elapsed_seconds', 0):.1f}s")
        if value.get("total_elapsed_seconds") is not None:
            parts.append(f"任务总耗时 {value['total_elapsed_seconds']:.1f}s")
        if value.get("eta_seconds") is not None:
            parts.append(f"当前步骤预计剩余 {value['eta_seconds']:.1f}s")
    if job.get("first_error"):
        parts.append(job["first_error"])
    try:
        sys.stderr.write(" | ".join(parts) + "\n")
        sys.stderr.flush()
    except (OSError, UnicodeError):
        pass


class ConsoleProgress:
    """Deduplicate progress and tool diagnostics forwarded by a phase supervisor."""

    def __init__(self):
        self.sequence = None
        self.diagnostic = None
        self.shutdown = None

    def show(self, job: dict) -> None:
        value = job.get("progress", {}).get("runtime", {})
        sequence = (job["id"], job.get("stage"), value.get("sequence"))
        if sequence != self.sequence:
            emit_progress(job)
            self.sequence = sequence
        diagnostic = job.get("database_diagnostic") or {}
        identity = (
            job["id"],
            diagnostic.get("recorded_at"),
            diagnostic.get("cleanup_error"),
            job.get("first_error") if not diagnostic.get("error_code") else None,
        )
        if (
            diagnostic
            and (diagnostic.get("error_code") or job.get("first_error"))
            and identity != self.diagnostic
        ):
            self.diagnostic = identity
            try:
                sys.stderr.write(
                    f"[迁移 {job['id'][:8]}] 数据库工具 {diagnostic.get('tool')} "
                    f"退出码={diagnostic.get('return_code')} "
                    f"阶段={diagnostic.get('phase')} "
                    f"耗时={diagnostic.get('duration_seconds')}s\n"
                    + (
                        "工具已成功退出；后续迁移步骤失败。\n"
                        if not diagnostic.get("error_code")
                        else ""
                    )
                    + diagnostic.get("stderr", "")
                    + "\n"
                )
                sys.stderr.flush()
            except (OSError, UnicodeError):
                pass
        shutdown = job.get("shutdown_diagnostic") or {}
        identity = (job["id"], shutdown.get("recorded_at"))
        if (
            shutdown
            and shutdown.get("result") != "confirmed"
            and identity != self.shutdown
        ):
            self.shutdown = identity
            components = ", ".join(
                f"{item.get('component_id')}: {item.get('error_code')}"
                for item in shutdown.get("failed_components", [])
            )
            try:
                sys.stderr.write(
                    f"[迁移 {job['id'][:8]}] 关闭核验={shutdown.get('result')} "
                    f"强制停止={shutdown.get('forced', False)} "
                    f"剩余预算={shutdown.get('budget_remaining_ms')}ms "
                    f"{components}\n"
                )
                sys.stderr.flush()
            except (OSError, UnicodeError):
                pass
