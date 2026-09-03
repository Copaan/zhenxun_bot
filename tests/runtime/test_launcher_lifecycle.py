from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pytest

from zhenxun.services.lifecycle.launcher import CommitSession


@dataclass
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


def test_wait_worker_exit_records_natural_exit(monkeypatch) -> None:
    import zhenxun.cli as cli

    process = FakeProcess(returncode=0)
    exits: list[tuple[int, str]] = []
    monkeypatch.setattr(
        cli,
        "_record_launcher_process_exit",
        lambda proc, reason: exits.append((proc.pid, reason)),
    )

    assert cli._wait_worker_exit(process, 0.1)
    assert exits == [(1234, "process_exit")]


def test_terminate_worker_escalates_to_kill(monkeypatch) -> None:
    import zhenxun.cli as cli

    process = FakeProcess()
    exits: list[tuple[int, str]] = []
    monkeypatch.setattr(cli.os, "name", "posix")
    monkeypatch.setattr(cli, "_wait_worker_exit", lambda _proc, _timeout: False)
    monkeypatch.setattr(
        cli,
        "_record_launcher_process_exit",
        lambda proc, reason: exits.append((proc.pid, reason)),
    )

    cli._terminate_worker(process)

    assert process.terminated == 1
    assert process.killed == 1
    assert process.waited == 1
    assert exits == [(1234, "killed")]


def test_launcher_commit_session_is_idempotent_and_recoverable(
    tmp_path: Path,
) -> None:
    session = CommitSession(tmp_path / "commit.json")
    prepared = session.begin(["source_plugins", "nonebot_generation"], "boot")
    assert session.begin(["ignored"], "other") == prepared

    session.transition("applied")
    session.transition("verifying")
    session.transition("rolling_back")
    rolled_back = session.transition("rolled_back")
    assert rolled_back["phase"] == "rolled_back"

    next_session = session.begin(["update"], "next-boot")
    assert next_session["session_id"] != prepared["session_id"]
    with pytest.raises(RuntimeError, match="launcher_commit_transition_invalid"):
        session.transition("committed")


def test_launcher_process_handle_distinguishes_spawn_and_runtime_pid(
    tmp_path: Path, monkeypatch
) -> None:
    from zhenxun.services.lifecycle import LifecycleKernel
    import zhenxun.services.lifecycle.launcher as launcher_module
    from zhenxun.services.lifecycle.launcher import LauncherSupervisor

    monkeypatch.setattr(launcher_module, "_COMMIT_PATH", tmp_path / "commit.json")
    supervisor = LauncherSupervisor(LifecycleKernel(tmp_path / "lifecycle.json"))
    supervisor.initialize()
    process = FakeProcess(pid=1234)
    supervisor.attach("worker", process)  # type: ignore[arg-type]

    supervisor.bind_worker_runtime(
        process,  # type: ignore[arg-type]
        {
            "pid": 5678,
            "boot_id": "worker-boot",
            "operating_mode": "management_only",
        },
    )

    worker = supervisor.snapshot()["process_graph"][0]
    assert worker["spawn_pid"] == 1234
    assert worker["runtime_pid"] == 5678
    assert worker["pid"] == 5678
    assert worker["worker_boot_id"] == "worker-boot"
    assert worker["operating_mode"] == "management_only"
