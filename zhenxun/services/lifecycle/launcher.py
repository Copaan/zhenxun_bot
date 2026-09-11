from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import signal
import subprocess
from threading import RLock
import time
from typing import Any, ClassVar
import uuid

from zhenxun.utils.atomic_json import read_json_locked, write_json_locked
from zhenxun.utils.process_tree import verified_descendants

from .deadline import ShutdownBudget, current_budget, shutdown_budget
from .diagnostics import merge_terminal_receipt
from .kernel import LifecycleKernel
from .models import ComponentSpec, ResourceReceipt, RuntimeHandle

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
    startup_id: str = field(default_factory=lambda: uuid.uuid4().hex)
    shutdown_id: str | None = None
    identities: dict[int, float] = field(default_factory=dict)
    stop_stages: list[dict[str, Any]] = field(default_factory=list)
    readiness: str = "spawned"
    completion_expected: bool = False
    _next_tree_discovery: float = 0.0
    _tree_scan_count: int = 0

    def __post_init__(self) -> None:
        import psutil

        try:
            self.identities[self.spawn_pid] = psutil.Process(
                self.spawn_pid
            ).create_time()
        except psutil.Error:
            pass

    def _live_processes(self, *, discover: bool = False):
        import psutil

        if not self.identities and self.process.poll() is None:
            raise RuntimeError("process_identity_unverified")
        live = []
        for pid, created in list(self.identities.items()):
            try:
                process = psutil.Process(pid)
                if (
                    process.create_time() != created
                    or not process.is_running()
                    or process.status() == psutil.STATUS_ZOMBIE
                ):
                    self.identities.pop(pid, None)
                    continue
                live.append(process)
            except psutil.NoSuchProcess:
                self.identities.pop(pid, None)
                continue
        now = time.monotonic()
        if discover or not self.accepting or now >= self._next_tree_discovery:
            self._next_tree_discovery = now + 0.5
            known = {process.pid for process in live}
            covered: set[int] = set()
            # Visit each surviving root once; children of that root are already
            # covered by its recursive discovery. Identity checks stay uncached;
            # shutdown always discovers afresh to catch late child processes.
            for process in tuple(live):
                if process.pid in covered:
                    continue
                try:
                    self._tree_scan_count += 1
                    children = verified_descendants(process)
                    covered.update(child.pid for child in children)
                    for child in children:
                        created = child.create_time()
                        previous = self.identities.get(child.pid)
                        if previous is not None and previous != created:
                            continue
                        self.identities[child.pid] = created
                        if child.pid not in known and child.is_running():
                            live.append(child)
                            known.add(child.pid)
                except psutil.NoSuchProcess:
                    continue
        return live

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
        if runtime_pid is not None:
            live = self._live_processes(discover=runtime_pid not in self.identities)
            if runtime_pid not in {process.pid for process in live}:
                return False
        if self.worker_boot_id and worker_boot_id != self.worker_boot_id:
            return False
        self.runtime_pid, self.worker_boot_id, self.operating_mode = (
            runtime_pid,
            worker_boot_id,
            operating_mode,
        )
        return True

    async def quiesce(self) -> None:
        self.accepting = False

    async def close(self) -> None:
        self.accepting = False
        if not self._live_processes(discover=True):
            return
        with shutdown_budget(15.0 if self.role == "worker" else 5.0) as budget:
            started = asyncio.get_running_loop().time()
            state_path = os.getenv("ZHENXUN_SHUTDOWN_BUDGET_PATH")
            if state_path:
                try:
                    state = read_json_locked(
                        Path(state_path), {}, timeout=0, quarantine_corrupt=True
                    )
                    if (
                        not isinstance(state, dict)
                        or state.get("launcher_boot_id") != self.launcher_boot_id
                    ):
                        state = {
                            "launcher_boot_id": self.launcher_boot_id,
                            "requests": {},
                        }
                    if not isinstance(state.get("requests"), dict):
                        state["requests"] = {}
                    state["requests"][self.startup_id] = {
                        "shutdown_id": self.shutdown_id or self.startup_id,
                        "requested_at": __import__("time").time(),
                        "budget_ms": int(budget.remaining() * 1000),
                        "identities": {
                            str(pid): created
                            for pid, created in self.identities.items()
                        },
                    }
                    write_json_locked(Path(state_path), state, timeout=0)
                except (OSError, TimeoutError):
                    self.stop_stages.append(
                        {
                            "stage": "budget_handoff",
                            "error_code": "shutdown_budget_write_failed",
                        }
                    )
            try:
                self.process.send_signal(
                    signal.CTRL_BREAK_EVENT if os.name == "nt" else signal.SIGTERM
                )
                self.exit_reason = "cooperative_signal"
            except (OSError, ValueError):
                self.exit_reason = "cooperative_signal_failed"
            try:
                while self._live_processes() and budget.remaining():
                    self.process.poll()
                    await asyncio.sleep(min(0.05, budget.remaining()))
            finally:
                self.stop_stages.append(
                    {
                        "stage": "cooperative",
                        "elapsed_seconds": asyncio.get_running_loop().time() - started,
                        "timed_out": bool(self._live_processes()),
                    }
                )
            if self._live_processes():
                raise TimeoutError("process_cooperative_timeout")
            self.process.poll()

    async def force_close(self) -> None:
        import psutil

        for stage in ("terminate", "kill"):
            started = asyncio.get_running_loop().time()
            for process in reversed(self._live_processes(discover=True)):
                try:
                    getattr(process, stage)()
                except psutil.Error:
                    pass
            budget = ShutdownBudget.start(5.0)
            while self._live_processes() and budget.remaining():
                self.process.poll()
                await asyncio.sleep(min(0.05, budget.remaining()))
            pending = bool(self._live_processes())
            self.stop_stages.append(
                {
                    "stage": stage,
                    "elapsed_seconds": asyncio.get_running_loop().time() - started,
                    "timed_out": pending,
                }
            )
            self.exit_reason = stage
            if not pending:
                self.process.poll()
                return
        raise RuntimeError("process_tree_recovery_required")

    def health(self) -> dict[str, object]:
        live = bool(self._live_processes())
        # A finite child can exit during the process-tree scan.
        completed = self.completion_expected and self.process.poll() == 0
        return {
            "healthy": live or completed,
            "completed": completed,
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
            "running": bool(self._live_processes()),
            "readiness": self.readiness,
            "return_code": self.process.poll(),
            "spawn_return_code": self.process.poll(),
            "accepting": self.accepting,
            "exit_reason": self.exit_reason,
            "launcher_boot_id": self.launcher_boot_id,
            "startup_id": self.startup_id,
            "shutdown_id": self.shutdown_id,
            "stop_stages": list(self.stop_stages),
            "tree_scan_count": self._tree_scan_count,
        }

    def runtime_shutdown_receipt(self) -> dict[str, Any] | None:
        if self.role != "worker" or not self.worker_boot_id:
            return None
        path = Path(
            os.getenv(
                "ZHENXUN_LIFECYCLE_STATE_PATH", "data/runtime/lifecycle-state-v2.json"
            )
        )
        try:
            state = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            return None
        if not isinstance(state, dict):
            return None
        receipt = merge_terminal_receipt(state, path).get("terminal_shutdown")
        if not isinstance(receipt, dict):
            return None
        identity = receipt.get("identity", {})
        if (
            identity.get("boot_id") != self.worker_boot_id
            or identity.get("startup_id") != self.startup_id
            or identity.get("launcher_boot_id") != self.launcher_boot_id
            or identity.get("pid") != self.runtime_pid
            or identity.get("shutdown_correlation_id")
            != (self.shutdown_id or self.startup_id)
        ):
            return None
        return receipt

    def resource_snapshot(self) -> list[ResourceReceipt]:
        return [
            ResourceReceipt(
                receipt_id=f"process:{self.process.pid}",
                provider="subprocess",
                resource_type="process",
                owner_id=f"launcher:{self.role}",
                reversible=False,
                state="active" if self._live_processes() else "released",
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

    def presentation(self, boot_id: str) -> dict[str, Any]:
        value = self.status()
        historical = bool(
            value
            and value.get("launcher_boot_id") != boot_id
            and value.get("phase") in {"committed", "rolled_back"}
        )
        return {
            "commit_session": value,
            "current_commit_session": None if historical else value or None,
            "historical_commit_session": value if historical else None,
        }


class LauncherSupervisor:
    def __init__(self, kernel: LifecycleKernel) -> None:
        self.kernel = kernel
        self.boot_id = ""
        self._handles: dict[int, ProcessHandle] = {}
        self._role_pids: dict[str, int] = {}
        self.commit_session = CommitSession(_COMMIT_PATH)
        self.shutdown_deadline: ShutdownBudget | None = None
        self.shutdown_id: str | None = None
        self.shutdown_signal: int | None = None
        self._process_history: list[dict[str, Any]] = []

    async def run_recovery(self, callback):
        component_id = "launcher:recovery"
        if self.kernel.component_status(component_id) is None:
            self.kernel.register(
                ComponentSpec(component_id, stage="management"), callback
            )
            await self.kernel.start_components({component_id})
            return self.kernel._registrations[component_id].value
        context = self.kernel.component_context(component_id)
        async with context.activity():
            return callback()

    def _publish_processes(self) -> None:
        self.kernel.set_process_metadata(
            process_graph=[handle.snapshot() for handle in self._handles.values()],
            process_history=list(self._process_history),
        )

    def begin_shutdown(
        self, *, restarting: bool = False, signal_number: int | None = None
    ) -> None:
        if signal_number is not None and self.shutdown_signal is None:
            self.shutdown_signal = int(signal_number)
            self.kernel.set_process_metadata(exit_source=f"signal:{signal_number}")
        if not restarting:
            self.kernel.request_shutdown()
        if self.shutdown_deadline is None:
            self.shutdown_deadline = ShutdownBudget.start(15.0)
            self.shutdown_id = uuid.uuid4().hex
            self.kernel.set_process_metadata(shutdown_id=self.shutdown_id)
            for handle in self._handles.values():
                handle.shutdown_id = self.shutdown_id

    async def start_process(
        self,
        role: str,
        factory,
        *,
        ready=None,
        startup_id: str | None = None,
        completion_expected: bool = False,
    ) -> subprocess.Popen:
        if self.shutdown_deadline is not None:
            raise OSError("launcher_startup_interrupted")
        previous = self._role_pids.get(role)
        if previous in self._handles:
            await self.stop_process(self._handles[previous].process)
        if self.shutdown_deadline is not None:
            raise OSError("launcher_startup_interrupted")
        recovery_id = "launcher:recovery"
        if self.kernel.component_status(recovery_id) is None:
            self.kernel.register(
                ComponentSpec(recovery_id, stage="management"), lambda: None
            )
            await self.kernel.start_components({recovery_id})
        component_id = f"launcher:{role}"

        async def start():
            if self.shutdown_deadline is not None:
                raise OSError("launcher_startup_interrupted")
            process = factory()
            handle = ProcessHandle(role, process, self.boot_id)
            handle.completion_expected = completion_expected
            if startup_id:
                handle.startup_id = startup_id
            self._handles[process.pid] = handle
            self._role_pids[role] = process.pid
            self.kernel._registrations[component_id].runtime.metadata.update(
                pid=process.pid, role=role, startup_id=handle.startup_id
            )
            self._publish_processes()
            if ready is not None:
                await ready(process)
                handle.readiness = "listening"
                self._publish_processes()
            return RuntimeHandle(
                value=process,
                controller=handle,
                metadata={
                    "pid": process.pid,
                    "role": role,
                    "startup_id": handle.startup_id,
                },
            )

        self.kernel.register(
            ComponentSpec(
                component_id,
                stage="management",
                scope="infrastructure",
                depends_on=("launcher:worker",)
                if role in {"http_sidecar", "qq_ingress"}
                else (recovery_id,),
                failure_policy="degrade",
                finalizer_timeout=15.0,
            ),
            start,
            replace=self.kernel.component_status(component_id) is not None,
        )
        await self.kernel.start_components({component_id})
        pid = self._role_pids.get(role)
        if (
            pid is None
            or self.kernel.component_status(component_id)["state"] != "ready"
        ):
            if pid in self._handles:
                await self.stop_process(self._handles[pid].process)
            raise OSError("launcher_process_start_failed")
        self.kernel._recovery_required.discard(component_id)
        self._publish_processes()
        return self._handles[self._role_pids[role]].process

    async def stop_process(self, process: subprocess.Popen) -> None:
        handle = self._handles.get(process.pid)
        if handle is None:
            handle = self.attach("worker", process)
        parent = current_budget.get()
        budget = self.shutdown_deadline or ShutdownBudget.start(
            15.0 if handle.role == "worker" else 5.0
        )
        if parent is not None:
            budget = ShutdownBudget(
                min(budget.deadline, parent.deadline), parent=parent
            )
        token = current_budget.set(budget)
        try:
            try:
                await self.kernel.stop_components({f"launcher:{handle.role}"})
            except BaseException:
                pass
            if handle._live_processes():
                try:
                    await handle.close()
                except (TimeoutError, asyncio.CancelledError):
                    await handle.force_close()
            self.release(process, handle.exit_reason or "exited")
        finally:
            current_budget.reset(token)

    async def shutdown(self) -> None:
        self.begin_shutdown()
        extra_roles = set(self._role_pids) - {"qq_ingress", "http_sidecar", "worker"}
        for role in ("qq_ingress", "http_sidecar", *sorted(extra_roles), "worker"):
            pid = self._role_pids.get(role)
            if pid in self._handles:
                await self.stop_process(self._handles[pid].process)
        token = current_budget.set(self.shutdown_deadline)
        try:
            await self.kernel.stop_all(timeout=15.0)
        finally:
            current_budget.reset(token)

    def initialize(self) -> str:
        self.boot_id = uuid.uuid4().hex
        os.environ["ZHENXUN_LAUNCHER_BOOT_ID"] = self.boot_id
        os.environ["ZHENXUN_SHUTDOWN_BUDGET_PATH"] = str(
            _STATE_PATH.with_name("launcher-shutdown-budget-v2.json").resolve()
        )
        self.kernel.set_process_metadata(
            role="launcher",
            pid=os.getpid(),
            boot_id=self.boot_id,
            **self.commit_session.presentation(self.boot_id),
        )
        return self.boot_id

    def attach(self, role: str, process: subprocess.Popen[Any]) -> ProcessHandle:
        if process.pid in self._handles:
            return self._handles[process.pid]
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
        handle = self._handles.get(process.pid)
        if handle is not None and handle._live_processes():
            handle.exit_reason = "spawn_exited_tree_alive"
            self.kernel._recovery_required.add(f"launcher:{handle.role}")
            return
        self._handles.pop(process.pid, None)
        if handle is not None:
            handle.accepting = False
            handle.exit_reason = reason
            if self._role_pids.get(handle.role) == process.pid:
                self._role_pids.pop(handle.role, None)
            registration = self.kernel._registrations.get(f"launcher:{handle.role}")
            if registration is not None:
                registration.runtime.metadata["handle"] = handle.snapshot()
            self._process_history.append(
                {
                    **handle.snapshot(),
                    "runtime_shutdown": handle.runtime_shutdown_receipt(),
                }
            )
            self._process_history = self._process_history[-100:]
        self.kernel.release_external_process(
            process.pid,
            return_code=process.poll(),
            reason=reason,
        )
        self._publish_processes()

    def bind_worker_runtime(
        self, process: subprocess.Popen[Any], status: dict[str, Any]
    ) -> bool:
        handle = self._handles.get(process.pid)
        if handle is None or handle.role != "worker":
            return False
        if (
            status.get("launcher_boot_id") != self.boot_id
            or status.get("startup_id") != handle.startup_id
            or not status.get("boot_id")
        ):
            return False
        runtime_pid = status.get("pid")
        try:
            runtime_pid = int(runtime_pid) if runtime_pid is not None else None
        except (TypeError, ValueError):
            runtime_pid = None
        if runtime_pid is None:
            return False
        previous = (
            handle.runtime_pid,
            handle.worker_boot_id,
            handle.operating_mode,
            handle.readiness,
        )
        changed = handle.bind_runtime(
            runtime_pid=runtime_pid,
            worker_boot_id=str(status.get("boot_id") or "") or None,
            operating_mode=str(status.get("operating_mode") or "") or None,
        )
        if not changed:
            return False
        handle.readiness = str(status.get("state") or "starting")
        if previous == (
            handle.runtime_pid,
            handle.worker_boot_id,
            handle.operating_mode,
            handle.readiness,
        ):
            return True
        registration = self.kernel._registrations.get("launcher:worker")
        if registration is not None and registration.context is not None:
            registration.runtime.metadata.update(
                {
                    "spawn_pid": handle.spawn_pid,
                    "runtime_pid": handle.runtime_pid,
                    "worker_boot_id": handle.worker_boot_id,
                    "operating_mode": handle.operating_mode,
                    "handle": handle.snapshot(),
                }
            )
            self._publish_processes()
            return True
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
        return True

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
        self.kernel.set_process_metadata(
            **self.commit_session.presentation(self.boot_id)
        )

    def update_metadata(self, **values: Any) -> None:
        self.kernel.set_process_metadata(**values)

    def snapshot(self) -> dict[str, Any]:
        result = self.kernel.status()
        result["process_graph"] = [
            handle.snapshot()
            for handle in sorted(self._handles.values(), key=lambda item: item.role)
        ]
        result.update(self.commit_session.presentation(self.boot_id))
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
) -> bool:
    return launcher_supervisor.bind_worker_runtime(process, status)


def begin_launcher_commit(targets: list[str]) -> dict[str, Any]:
    return launcher_supervisor.begin_commit(targets)


def transition_launcher_commit(
    phase: str, *, error_code: str | None = None
) -> dict[str, Any]:
    return launcher_supervisor.transition_commit(phase, error_code=error_code)


def update_launcher_metadata(**values: Any) -> None:
    launcher_supervisor.update_metadata(**values)


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
    value["process_graph"] = value.get("process", {}).get("process_graph", [])
    value["process_history"] = value.get("process", {}).get("process_history", [])
    value.update(
        launcher_supervisor.commit_session.presentation(
            value.get("process", {}).get("boot_id", "")
        )
    )
    return merge_terminal_receipt(value, path)


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
    "update_launcher_metadata",
]
