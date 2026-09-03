from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import subprocess
from threading import RLock
from typing import Any, ClassVar
import uuid

from zhenxun.utils.atomic_json import read_json_locked, write_json_locked

from .kernel import LifecycleKernel
from .models import ResourceReceipt

_STATE_PATH = Path(
    os.getenv(
        "ZHENXUN_LAUNCHER_LIFECYCLE_STATE_PATH",
        "data/runtime/launcher-lifecycle-state-v2.json",
    )
)
_COMMIT_PATH = Path(
    os.getenv(
        "ZHENXUN_LAUNCHER_COMMIT_STATE_PATH",
        "data/runtime/launcher-commit-session-v2.json",
    )
)
_LEGACY_STATE_PATH = Path("data/runtime/launcher-lifecycle-state-v1.json")
launcher_lifecycle_kernel = LifecycleKernel(_STATE_PATH)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass(slots=True)
class ProcessHandle:
    role: str
    process: subprocess.Popen[Any]
    launcher_boot_id: str
    accepting: bool = True
    exit_reason: str | None = None
    runtime_pid: int | None = None
    worker_boot_id: str | None = None
    operating_mode: str | None = None

    @property
    def spawn_pid(self) -> int:
        return self.process.pid

    def bind_runtime(
        self,
        *,
        runtime_pid: int | None,
        worker_boot_id: str | None,
        operating_mode: str | None,
    ) -> bool:
        values = (runtime_pid, worker_boot_id, operating_mode)
        previous = (self.runtime_pid, self.worker_boot_id, self.operating_mode)
        self.runtime_pid, self.worker_boot_id, self.operating_mode = values
        return values != previous

    async def quiesce(self) -> None:
        self.accepting = False

    async def close(self) -> None:
        if self.process.poll() is not None:
            return
        self.process.terminate()
        try:
            await asyncio.wait_for(asyncio.to_thread(self.process.wait), timeout=5)
        except TimeoutError:
            self.process.kill()
            await asyncio.to_thread(self.process.wait)
            self.exit_reason = "killed"

    def health(self) -> dict[str, object]:
        return {
            "healthy": self.process.poll() is None,
            "spawn_pid": self.spawn_pid,
            "runtime_pid": self.runtime_pid,
            "return_code": self.process.poll(),
        }

    def snapshot(self) -> dict[str, object]:
        return {
            "role": self.role,
            "pid": self.runtime_pid or self.spawn_pid,
            "spawn_pid": self.spawn_pid,
            "runtime_pid": self.runtime_pid,
            "worker_boot_id": self.worker_boot_id,
            "operating_mode": self.operating_mode,
            "running": self.process.poll() is None,
            "return_code": self.process.poll(),
            "accepting": self.accepting,
            "exit_reason": self.exit_reason,
            "launcher_boot_id": self.launcher_boot_id,
        }

    def resource_snapshot(self) -> list[ResourceReceipt]:
        return [
            ResourceReceipt(
                receipt_id=f"process:{self.process.pid}",
                provider="subprocess",
                resource_type="process",
                owner_id=f"launcher:{self.role}",
                reversible=False,
                state="active" if self.process.poll() is None else "released",
                detail={
                    "pid": self.runtime_pid or self.spawn_pid,
                    "spawn_pid": self.spawn_pid,
                    "runtime_pid": self.runtime_pid,
                    "role": self.role,
                },
            )
        ]


class CommitSession:
    _VALID_TRANSITIONS: ClassVar[dict[str, set[str]]] = {
        "prepared": {"applied", "rolling_back", "recovery_required"},
        "applied": {"verifying", "rolling_back", "recovery_required"},
        "verifying": {"committed", "rolling_back", "recovery_required"},
        "rolling_back": {"rolled_back", "recovery_required"},
        "committed": set(),
        "rolled_back": set(),
        "recovery_required": {"rolling_back", "verifying"},
    }

    def __init__(self, path: Path) -> None:
        self._path = path
        self._lock = RLock()

    def begin(self, targets: list[str], launcher_boot_id: str) -> dict[str, Any]:
        with self._lock:
            current = self.status()
            if current.get("phase") not in {None, "committed", "rolled_back"}:
                return current
            value = {
                "version": 2,
                "session_id": uuid.uuid4().hex,
                "launcher_boot_id": launcher_boot_id,
                "phase": "prepared",
                "targets": sorted(set(targets)),
                "created_at": _now(),
                "updated_at": _now(),
                "error_code": None,
            }
            write_json_locked(self._path, value)
            return value

    def transition(
        self, phase: str, *, error_code: str | None = None
    ) -> dict[str, Any]:
        with self._lock:
            value = self.status()
            current = value.get("phase")
            if current == phase:
                return value
            if phase not in self._VALID_TRANSITIONS.get(str(current), set()):
                raise RuntimeError(
                    f"launcher_commit_transition_invalid:{current}:{phase}"
                )
            value["phase"] = phase
            value["updated_at"] = _now()
            value["error_code"] = error_code
            write_json_locked(self._path, value)
            return value

    def status(self) -> dict[str, Any]:
        value = read_json_locked(self._path, {}, quarantine_corrupt=True)
        return value if isinstance(value, dict) else {}


class LauncherSupervisor:
    def __init__(self, kernel: LifecycleKernel) -> None:
        self.kernel = kernel
        self.boot_id = ""
        self._handles: dict[int, ProcessHandle] = {}
        self._role_pids: dict[str, int] = {}
        self.commit_session = CommitSession(_COMMIT_PATH)

    def initialize(self) -> str:
        self.boot_id = uuid.uuid4().hex
        self.kernel.set_process_metadata(
            role="launcher",
            pid=os.getpid(),
            boot_id=self.boot_id,
            commit_session=self.commit_session.status(),
        )
        return self.boot_id

    def attach(self, role: str, process: subprocess.Popen[Any]) -> ProcessHandle:
        previous_pid = self._role_pids.get(role)
        if previous_pid is not None and previous_pid in self._handles:
            previous = self._handles[previous_pid]
            if previous.process.poll() is None:
                raise RuntimeError(f"launcher_process_already_running:{role}")
        handle = ProcessHandle(role, process, self.boot_id)
        self._handles[process.pid] = handle
        self._role_pids[role] = process.pid
        self.kernel.observe_external_process(
            f"launcher:{role}",
            pid=process.pid,
            scope="worker" if role == "worker" else "infrastructure",
            metadata={
                "role": role,
                "launcher_boot_id": self.boot_id,
                "process_generation": uuid.uuid4().hex,
            },
            controller=handle,
        )
        return handle

    def release(self, process: subprocess.Popen[Any], reason: str) -> None:
        handle = self._handles.pop(process.pid, None)
        if handle is not None:
            handle.accepting = False
            handle.exit_reason = reason
            if self._role_pids.get(handle.role) == process.pid:
                self._role_pids.pop(handle.role, None)
        self.kernel.release_external_process(
            process.pid,
            return_code=process.poll(),
            reason=reason,
        )

    def bind_worker_runtime(
        self, process: subprocess.Popen[Any], status: dict[str, Any]
    ) -> None:
        handle = self._handles.get(process.pid)
        if handle is None or handle.role != "worker":
            return
        runtime_pid = status.get("pid")
        try:
            runtime_pid = int(runtime_pid) if runtime_pid is not None else None
        except (TypeError, ValueError):
            runtime_pid = None
        changed = handle.bind_runtime(
            runtime_pid=runtime_pid,
            worker_boot_id=str(status.get("boot_id") or "") or None,
            operating_mode=str(status.get("operating_mode") or "") or None,
        )
        if not changed:
            return
        self.kernel.observe_external_process(
            "launcher:worker",
            pid=handle.spawn_pid,
            scope="worker",
            metadata={
                "role": "worker",
                "launcher_boot_id": self.boot_id,
                "spawn_pid": handle.spawn_pid,
                "runtime_pid": handle.runtime_pid,
                "worker_boot_id": handle.worker_boot_id,
                "operating_mode": handle.operating_mode,
            },
            controller=handle,
        )

    def begin_commit(self, targets: list[str]) -> dict[str, Any]:
        value = self.commit_session.begin(targets, self.boot_id)
        self._sync_commit(value)
        return value

    def transition_commit(
        self, phase: str, *, error_code: str | None = None
    ) -> dict[str, Any]:
        value = self.commit_session.transition(phase, error_code=error_code)
        self._sync_commit(value)
        return value

    def _sync_commit(self, value: dict[str, Any]) -> None:
        self.kernel.set_process_metadata(commit_session=value)

    def snapshot(self) -> dict[str, Any]:
        result = self.kernel.status()
        result["process_graph"] = [
            handle.snapshot()
            for handle in sorted(self._handles.values(), key=lambda item: item.role)
        ]
        result["commit_session"] = self.commit_session.status()
        return result


launcher_supervisor = LauncherSupervisor(launcher_lifecycle_kernel)


def initialize_launcher_lifecycle() -> str:
    return launcher_supervisor.initialize()


def observe_launcher_process(role: str, process: subprocess.Popen[Any]) -> None:
    launcher_supervisor.attach(role, process)


def release_launcher_process(process: subprocess.Popen[Any], reason: str) -> None:
    launcher_supervisor.release(process, reason)


def bind_launcher_worker_runtime(
    process: subprocess.Popen[Any], status: dict[str, Any]
) -> None:
    launcher_supervisor.bind_worker_runtime(process, status)


def begin_launcher_commit(targets: list[str]) -> dict[str, Any]:
    return launcher_supervisor.begin_commit(targets)


def transition_launcher_commit(
    phase: str, *, error_code: str | None = None
) -> dict[str, Any]:
    return launcher_supervisor.transition_commit(phase, error_code=error_code)


def launcher_lifecycle_snapshot() -> dict[str, Any]:
    if launcher_supervisor.boot_id:
        return launcher_supervisor.snapshot()
    value: Any = None
    for path in (_STATE_PATH, _LEGACY_STATE_PATH):
        try:
            value = json.loads(path.read_text(encoding="utf-8"))
            break
        except (OSError, ValueError):
            continue
    if not isinstance(value, dict):
        return {}
    value["commit_session"] = launcher_supervisor.commit_session.status()
    return value


__all__ = [
    "CommitSession",
    "LauncherSupervisor",
    "ProcessHandle",
    "begin_launcher_commit",
    "bind_launcher_worker_runtime",
    "initialize_launcher_lifecycle",
    "launcher_lifecycle_kernel",
    "launcher_lifecycle_snapshot",
    "launcher_supervisor",
    "observe_launcher_process",
    "release_launcher_process",
    "transition_launcher_commit",
]
