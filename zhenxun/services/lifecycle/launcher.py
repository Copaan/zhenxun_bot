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
import weakref

from zhenxun.utils.atomic_json import read_json_locked, write_json_locked
from zhenxun.utils.process_tree import verified_descendants

from .deadline import ShutdownBudget, current_budget, shutdown_budget
from .diagnostics import merge_terminal_receipt, terminal_path
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
    _identity_unverified: bool = False
    _close_task: asyncio.Task | None = field(default=None, init=False, repr=False)
    _force_task: asyncio.Task | None = field(default=None, init=False, repr=False)
    _kernel_stop_task: asyncio.Task | None = field(default=None, init=False, repr=False)
    _runtime_identity: tuple[int, float] | None = field(
        default=None, init=False, repr=False
    )

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

        # Reap the direct child before inspecting psutil's stale zombie state.
        returncode = self.process.poll()
        if self._identity_unverified or (not self.identities and returncode is None):
            raise RuntimeError("process_identity_unverified")
        live = []
        for pid, created in list(self.identities.items()):
            try:
                process = psutil.Process(pid)
                if process.create_time() != created:
                    self._identity_unverified = True
                    raise RuntimeError("process_identity_unverified")
                if not process.is_running() or process.status() == psutil.STATUS_ZOMBIE:
                    if pid == self.spawn_pid and self.process.poll() is None:
                        live.append(process)
                        continue
                    self.identities.pop(pid, None)
                    continue
                live.append(process)
            except psutil.NoSuchProcess:
                if pid == self.spawn_pid and self.process.poll() is None:
                    raise RuntimeError("process_identity_unverified") from None
                self.identities.pop(pid, None)
                continue
            except psutil.AccessDenied as error:
                raise RuntimeError("process_identity_unverified") from error
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
                        try:
                            if child.status() == psutil.STATUS_ZOMBIE:
                                continue
                            created = child.create_time()
                            previous = self.identities.get(child.pid)
                            if previous is not None and previous != created:
                                continue
                            self.identities[child.pid] = created
                            if child.pid not in known and child.is_running():
                                live.append(child)
                                known.add(child.pid)
                        except (psutil.NoSuchProcess, psutil.ZombieProcess):
                            continue
                except (psutil.NoSuchProcess, psutil.ZombieProcess):
                    continue
                except psutil.AccessDenied as error:
                    raise RuntimeError("process_identity_unverified") from error
        return live

    def process_diagnostic(self) -> dict[str, Any]:
        """Capture bounded process identities and states without command arguments."""
        import psutil

        observations = []
        for pid, expected in sorted(self.identities.items())[:32]:
            item = {"pid": pid, "expected_created_at": expected}
            try:
                process = psutil.Process(pid)
                created = process.create_time()
                item.update(
                    created_at=created,
                    identity_verified=created == expected,
                    status=process.status(),
                    parent_pid=process.ppid(),
                    name=process.name()[:128],
                )
            except psutil.ZombieProcess:
                item["status"] = "zombie"
            except psutil.NoSuchProcess:
                item["status"] = "exited"
            except psutil.AccessDenied:
                item.update(status="access_denied", identity_verified=False)
            observations.append(item)
        return {
            "recorded_at": _now(),
            "role": self.role,
            "spawn_pid": self.spawn_pid,
            "runtime_pid": self.runtime_pid,
            "return_code": self.process.poll(),
            "processes": observations,
            "truncated": len(self.identities) > 32,
        }

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
        runtime_identity = None
        if runtime_pid is not None:
            live = self._live_processes(discover=runtime_pid not in self.identities)
            if runtime_pid not in {process.pid for process in live}:
                return False
            runtime_identity = (runtime_pid, self.identities[runtime_pid])
        # Discovery may adopt a replacement descendant for shutdown ownership,
        # but that process cannot inherit the accepted runtime's boot identity.
        if (
            self._runtime_identity is not None
            and runtime_identity != self._runtime_identity
        ):
            return False
        if self.worker_boot_id and worker_boot_id != self.worker_boot_id:
            return False
        self._runtime_identity = runtime_identity
        self.runtime_pid, self.worker_boot_id, self.operating_mode = (
            runtime_pid,
            worker_boot_id,
            operating_mode,
        )
        return True

    async def quiesce(self) -> None:
        self.accepting = False

    async def close(self) -> None:
        """Send one cooperative request and share its completion with all callers."""
        if self._close_task is None:
            self.shutdown_id = self.shutdown_id or self.startup_id
            self._close_task = asyncio.create_task(self._close_once())
            self._close_task.add_done_callback(self._consume_result)
        await asyncio.shield(self._close_task)

    @staticmethod
    def _consume_result(task: asyncio.Task) -> None:
        if not task.cancelled():
            task.exception()

    async def _close_once(self) -> None:
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
                        "requested_at": time.time(),
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
                pending, verified = self._observed_running()
                self.stop_stages.append(
                    {
                        "stage": "cooperative",
                        "elapsed_seconds": asyncio.get_running_loop().time() - started,
                        "timed_out": pending,
                        "identity_verified": verified,
                        **(
                            {"process_diagnostic": self.process_diagnostic()}
                            if pending
                            else {}
                        ),
                    }
                )
            if self._live_processes():
                raise TimeoutError("process_cooperative_timeout")
            self.process.poll()

    async def force_close(self, budget: ShutdownBudget) -> None:
        """Terminate a verified tree once within the supervisor's recovery window."""
        if self._force_task is None:
            self._force_task = asyncio.create_task(self._force_close_once(budget))
            self._force_task.add_done_callback(self._consume_result)
        await asyncio.shield(self._force_task)

    async def _force_close_once(self, budget: ShutdownBudget) -> None:
        import psutil

        stage = "terminate" if os.name == "nt" else "kill"
        started = time.monotonic()
        signalled: set[tuple[int, float]] = set()
        try:
            while live := self._live_processes(discover=True):
                if not budget.remaining():
                    raise RuntimeError("process_tree_recovery_required")
                for process in reversed(live):
                    identity = (process.pid, self.identities[process.pid])
                    if identity in signalled:
                        continue
                    try:
                        # A reused PID must never receive this tree's signal.
                        if psutil.Process(process.pid).create_time() != identity[1]:
                            self._identity_unverified = True
                            raise RuntimeError("process_identity_unverified")
                        getattr(process, stage)()
                    except psutil.NoSuchProcess:
                        continue
                    signalled.add(identity)
                self.exit_reason = stage
                await asyncio.sleep(min(0.05, budget.remaining()))
            self.process.poll()
        finally:
            pending, verified = self._observed_running()
            self.stop_stages.append(
                {
                    "stage": stage,
                    "elapsed_seconds": time.monotonic() - started,
                    "timed_out": pending,
                    "identity_verified": verified,
                    **(
                        {"process_diagnostic": self.process_diagnostic()}
                        if pending
                        else {}
                    ),
                }
            )

    def _observed_running(self) -> tuple[bool, bool]:
        """只读诊断用的存活判定，返回 (running, identity_verified)。

        _live_processes() 在身份无法核实时故意抛错：停机路径上把"查不到"
        当成"已退出"会漏杀整棵进程树。但 health()/snapshot() 是只读诊断，
        kernel._refresh_controller 启动/健康检查也会调它们，抛错会让一次
        psutil AccessDenied 变成组件启动失败。这里退回 Popen.poll() ——
        对直接子进程它仍是权威结论，且"核实不了就算活着"依旧是 fail-closed。
        """
        try:
            return bool(self._live_processes()), True
        except RuntimeError:
            return self.process.poll() is None, False

    def health(self) -> dict[str, object]:
        live, identity_verified = self._observed_running()
        # A finite child can exit during the process-tree scan.
        completed = self.completion_expected and self.process.poll() == 0
        return {
            "healthy": live or completed,
            "completed": completed,
            "identity_verified": identity_verified,
            "spawn_pid": self.spawn_pid,
            "runtime_pid": self.runtime_pid,
            "return_code": self.process.poll(),
        }

    def snapshot(self) -> dict[str, object]:
        running, identity_verified = self._observed_running()
        return {
            "role": self.role,
            "pid": self.runtime_pid or self.spawn_pid,
            "spawn_pid": self.spawn_pid,
            "runtime_pid": self.runtime_pid,
            "worker_boot_id": self.worker_boot_id,
            "operating_mode": self.operating_mode,
            "running": running,
            "identity_verified": identity_verified,
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

    def runtime_shutdown_receipt(
        self, *, include_failure: bool = False
    ) -> dict[str, Any] | None:
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
            state = {}
        if not isinstance(state, dict):
            state = {}
        candidates = [merge_terminal_receipt(state, path).get("terminal_shutdown")]
        if include_failure:
            try:
                candidates.append(
                    json.loads(terminal_path(path).read_text(encoding="utf-8"))
                )
            except (OSError, ValueError):
                pass
            failed = [
                {
                    "component_id": item["component_id"],
                    "error_code": item.get("error_code"),
                    "diagnostic": item.get("metadata", {}).get("stop_diagnostic", {}),
                }
                for item in state.get("components", [])
                if item.get("state") == "failed"
            ][:16]
            if failed:
                candidates.append(
                    {
                        "identity": state.get("snapshot_identity", {}),
                        "result": "unconfirmed",
                        "phase": "last_worker_snapshot",
                        "failed_components": failed,
                        "recovery_required": state.get("recovery_required", []),
                        "unresolved_resources": state.get("unresolved_resources", []),
                    }
                )
        for receipt in candidates:
            if not isinstance(receipt, dict):
                continue
            identity = receipt.get("identity") or {}
            if (
                identity.get("boot_id") != self.worker_boot_id
                or identity.get("startup_id") != self.startup_id
                or identity.get("launcher_boot_id") != self.launcher_boot_id
                or identity.get("pid") != self.runtime_pid
            ):
                continue
            if identity.get("shutdown_correlation_id") != (
                self.shutdown_id or self.startup_id
            ):
                if not include_failure or receipt.get("result") != "unconfirmed":
                    continue
                return {**receipt, "correlation_verified": False}
            return receipt
        return None

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
        self._released_processes: weakref.WeakSet = weakref.WeakSet()
        self._force_budget: ShutdownBudget | None = None

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
            self._force_budget = None
            self.shutdown_id = uuid.uuid4().hex
            self.kernel.set_process_metadata(shutdown_id=self.shutdown_id)
            for handle in self._handles.values():
                if handle._close_task is None:
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
            await self.stop_process(self._handles[previous].process, allow_force=True)
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
                await self.stop_process(self._handles[pid].process, allow_force=True)
            raise OSError("launcher_process_start_failed")
        self.kernel._recovery_required.discard(component_id)
        self._publish_processes()
        return self._handles[self._role_pids[role]].process

    async def stop_process(
        self, process: subprocess.Popen, *, allow_force: bool
    ) -> None:
        if process in self._released_processes:
            return
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
                if handle._kernel_stop_task is None:
                    handle._kernel_stop_task = asyncio.create_task(
                        self.kernel.stop_components({f"launcher:{handle.role}"})
                    )
                    handle._kernel_stop_task.add_done_callback(handle._consume_result)
                await asyncio.shield(handle._kernel_stop_task)
            except Exception as error:
                if not any(
                    item["stage"] == "component_stop" for item in handle.stop_stages
                ):
                    handle.stop_stages.append(
                        {"stage": "component_stop", "error_code": str(error)}
                    )
            if handle._live_processes():
                if handle._close_task is not None:
                    try:
                        await asyncio.shield(handle._close_task)
                    except TimeoutError:
                        pass
                if handle._live_processes():
                    if not allow_force:
                        raise TimeoutError("process_cooperative_timeout")
                    if self.shutdown_deadline is not None:
                        if self._force_budget is None:
                            self._force_budget = ShutdownBudget.start(5.0)
                        force_budget = self._force_budget
                    else:
                        force_budget = ShutdownBudget.start(5.0)
                    await handle.force_close(force_budget)
            self.release(process, handle.exit_reason or "exited")
        finally:
            current_budget.reset(token)

    async def shutdown(self) -> None:
        self.begin_shutdown()
        extra_roles = set(self._role_pids) - {"qq_ingress", "http_sidecar", "worker"}
        first_error = None
        for role in ("qq_ingress", "http_sidecar", *sorted(extra_roles), "worker"):
            pid = self._role_pids.get(role)
            if pid in self._handles:
                try:
                    await self.stop_process(
                        self._handles[pid].process, allow_force=True
                    )
                except Exception as error:
                    first_error = first_error or error
        token = current_budget.set(self.shutdown_deadline)
        try:
            try:
                await self.kernel.stop_all(timeout=15.0)
            except Exception as error:
                first_error = first_error or error
            if first_error is not None:
                raise first_error
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
        self._released_processes.add(process)
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
                    "runtime_shutdown": handle.runtime_shutdown_receipt(
                        include_failure=True
                    ),
                }
            )
            self._process_history = self._process_history[-100:]
        self.kernel.release_external_process(
            process.pid,
            return_code=process.poll(),
            reason=reason,
        )
        if handle is not None:
            self.kernel.clear_recovery(f"launcher:{handle.role}")
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
