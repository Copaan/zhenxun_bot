from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor
import contextvars
from dataclasses import dataclass
import os
from pathlib import Path
import subprocess
import sys
from types import SimpleNamespace


# eq=False 保持按身份哈希，与真实 Popen 一致（启动器用 WeakSet 跟踪已释放进程）
@dataclass(eq=False)
class FakeProcess:
    pid: int = 1234
    returncode: int | None = None
    terminated: int = 0
    killed: int = 0
    waited: int = 0

    def poll(self):
        return self.returncode

    def terminate(self) -> None:
        self.terminated += 1

    def kill(self) -> None:
        self.killed += 1
        self.returncode = -9

    def wait(self, timeout=None):
        self.waited += 1
        return self.returncode


def _isolated_supervisor(tmp_path: Path, monkeypatch):
    """构造隔离的启动器监督者，initialize 写的环境变量会被还原"""
    import zhenxun.services.launcher.sidecars as sidecars_module
    import zhenxun.services.launcher.supervisor as launcher_module
    from zhenxun.services.launcher.supervisor import LauncherSupervisor

    monkeypatch.setenv("ZHENXUN_LAUNCHER_BOOT_ID", "")
    supervisor = LauncherSupervisor()
    supervisor.initialize()
    monkeypatch.setattr(launcher_module, "launcher_supervisor", supervisor)
    monkeypatch.setattr(sidecars_module, "launcher_supervisor", supervisor)
    return supervisor


def test_terminate_worker_escalates_to_kill(tmp_path: Path, monkeypatch) -> None:
    from zhenxun.services.launcher.sidecars import stop_process
    from zhenxun.services.lifecycle.deadline import ShutdownBudget, current_budget

    supervisor = _isolated_supervisor(tmp_path, monkeypatch)
    # 子进程忽略协作式停止信号，迫使启动器在预算耗尽后升级为强制结束
    script = (
        "import signal, sys, time\n"
        "for name in ('SIGBREAK', 'SIGTERM'):\n"
        "    if hasattr(signal, name):\n"
        "        signal.signal(getattr(signal, name), signal.SIG_IGN)\n"
        "sys.stdout.write('ready\\n')\n"
        "sys.stdout.flush()\n"
        "time.sleep(60)\n"
    )
    process = subprocess.Popen(
        [sys.executable, "-c", script],
        stdout=subprocess.PIPE,
        creationflags=getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0),
    )
    try:
        assert process.stdout is not None
        assert process.stdout.readline().strip() == b"ready"
        handle = supervisor.attach("worker", process)
        # 用较短的外层预算限制协作阶段，避免等待默认的 15 秒。
        # 在独立线程的事件循环中执行，避免 asyncio.run 结束时清空主线程的事件循环。
        context = contextvars.copy_context()
        context.run(current_budget.set, ShutdownBudget.start(1.5))
        with ThreadPoolExecutor(max_workers=1) as pool:
            pool.submit(context.run, asyncio.run, stop_process(process)).result(
                timeout=30
            )

        assert process.wait(timeout=10) is not None
        force_stage = "terminate" if os.name == "nt" else "kill"
        assert handle.exit_reason == force_stage
        stages = [item["stage"] for item in handle.stop_stages]
        assert stages.index("cooperative") < stages.index(force_stage)
        cooperative = next(
            item for item in handle.stop_stages if item["stage"] == "cooperative"
        )
        assert cooperative["timed_out"] is True
        assert supervisor._handles == {}
    finally:
        if process.poll() is None:
            process.kill()
            process.wait(timeout=10)
        if process.stdout is not None:
            process.stdout.close()


def test_launcher_process_handle_distinguishes_spawn_and_runtime_pid(
    tmp_path: Path, monkeypatch
) -> None:
    from zhenxun.services.launcher.supervisor import ProcessHandle

    # 假进程没有真实 psutil 身份：模拟已核实的进程树（spawn 1234 → 运行时 5678）
    tree = {1234: 1.0, 5678: 2.0}

    def verified_tree(self, *, discover: bool = False):
        self.identities.update(tree)
        return [SimpleNamespace(pid=pid) for pid in tree]

    monkeypatch.setattr(ProcessHandle, "_live_processes", verified_tree)
    supervisor = _isolated_supervisor(tmp_path, monkeypatch)
    process = FakeProcess(pid=1234)
    handle = supervisor.attach("worker", process)  # type: ignore[arg-type]

    # 运行时状态必须携带本次启动器与本次 spawn 的身份，否则拒绝绑定
    assert not supervisor.bind_worker_runtime(
        process,  # type: ignore[arg-type]
        {"pid": 5678, "boot_id": "worker-boot"},
    )
    assert supervisor.bind_worker_runtime(
        process,  # type: ignore[arg-type]
        {
            "pid": 5678,
            "boot_id": "worker-boot",
            "operating_mode": "management_only",
            "launcher_boot_id": supervisor.boot_id,
            "startup_id": handle.startup_id,
        },
    )

    assert handle.spawn_pid == 1234
    assert handle.runtime_pid == 5678
    assert handle.worker_boot_id == "worker-boot"
    assert handle.operating_mode == "management_only"
