from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
import os
import signal
import subprocess
import sys
from threading import Event
import time
from typing import Any
import uuid
import weakref

from zhenxun.services.lifecycle.deadline import (
    ShutdownBudget,
    current_budget,
    shutdown_budget,
)
from zhenxun.utils.process_tree import verified_descendants


def launcher_log(message: str) -> None:
    sys.stderr.write(f"[zx launcher] {message}\n")
    sys.stderr.flush()


@dataclass(slots=True)
class ProcessHandle:
    role: str
    process: subprocess.Popen[Any]
    accepting: bool = True
    exit_reason: str | None = None
    runtime_pid: int | None = None
    worker_boot_id: str | None = None
    operating_mode: str | None = None
    startup_id: str = field(default_factory=lambda: uuid.uuid4().hex)
    identities: dict[int, float] = field(default_factory=dict)
    stop_stages: list[dict[str, Any]] = field(default_factory=list)
    readiness: str = "spawned"
    completion_expected: bool = False
    _next_tree_discovery: float = 0.0
    _tree_scan_count: int = 0
    _identity_unverified: bool = False
    _close_task: asyncio.Task | None = field(default=None, init=False, repr=False)
    _force_task: asyncio.Task | None = field(default=None, init=False, repr=False)
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
                    if (
                        pid not in {self.spawn_pid, self.runtime_pid}
                        or returncode is not None
                    ):
                        # The tracked incarnation exited. Its reused PID belongs
                        # to another process and must never receive our signals.
                        self.identities.pop(pid, None)
                        continue
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
        if (
            self._runtime_identity is not None
            and self._runtime_identity[0] == runtime_pid
        ):
            # Re-binding the accepted runtime only needs its birth time; tree
            # discovery is reserved for adoption and shutdown.
            import psutil

            try:
                created = psutil.Process(runtime_pid).create_time()
            except psutil.Error:
                return False
            if created != self._runtime_identity[1]:
                return False
            runtime_identity = self._runtime_identity
        elif runtime_pid is not None:
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

    async def close(self) -> None:
        """Send one cooperative request and share its completion with all callers."""
        if self._close_task is None:
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
                }
            )

    def _observed_running(self) -> tuple[bool, bool]:
        """记录停止阶段用的存活判定，身份无法核实时退回 Popen.poll()

        返回:
            tuple[bool, bool]: (是否仍在运行, 身份是否已核实)
        """
        try:
            return bool(self._live_processes()), True
        except RuntimeError:
            return self.process.poll() is None, False


class LauncherSupervisor:
    def __init__(self) -> None:
        self.boot_id = ""
        self._handles: dict[int, ProcessHandle] = {}
        self._role_pids: dict[str, int] = {}
        self._closers: list[Callable[[], Awaitable[Any]]] = []
        self.shutdown_deadline: ShutdownBudget | None = None
        self.shutdown_signal: int | None = None
        self.shutdown_event = Event()
        self._released_processes: weakref.WeakSet = weakref.WeakSet()
        self._force_budget: ShutdownBudget | None = None

    def add_closer(self, closer: Callable[[], Awaitable[Any]]) -> None:
        """注册 launcher 关闭时在子进程停止后执行的清理函数

        参数:
            closer: 异步清理函数
        """
        self._closers.append(closer)

    def begin_shutdown(
        self, *, restarting: bool = False, signal_number: int | None = None
    ) -> None:
        if signal_number is not None and self.shutdown_signal is None:
            self.shutdown_signal = int(signal_number)
        if not restarting:
            self.shutdown_event.set()
        if self.shutdown_deadline is None:
            self.shutdown_deadline = ShutdownBudget.start(15.0)
            self._force_budget = None

    async def start_process(
        self,
        role: str,
        factory,
        *,
        ready=None,
        startup_id: str | None = None,
        completion_expected: bool = False,
    ) -> subprocess.Popen:
        """启动并登记一个受管子进程，就绪检查失败时停止该进程

        参数:
            role: 进程角色
            factory: 创建进程的函数
            ready: 就绪检查协程函数
            startup_id: 启动标识
            completion_expected: 是否为会自行结束的一次性进程

        返回:
            subprocess.Popen: 已启动的进程
        """
        if self.shutdown_deadline is not None:
            raise OSError("launcher_startup_interrupted")
        previous = self._role_pids.get(role)
        if previous in self._handles:
            await self.stop_process(self._handles[previous].process, allow_force=True)
        if self.shutdown_deadline is not None:
            raise OSError("launcher_startup_interrupted")
        process = factory()
        handle = ProcessHandle(role, process)
        handle.completion_expected = completion_expected
        if startup_id:
            handle.startup_id = startup_id
        self._handles[process.pid] = handle
        self._role_pids[role] = process.pid
        try:
            if ready is not None:
                await ready(process)
                handle.readiness = "listening"
        except Exception as error:
            await self.stop_process(process, allow_force=True)
            raise OSError("launcher_process_start_failed") from error
        return process

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
                await handle.close()
            except TimeoutError:
                pass
            except Exception as error:
                handle.stop_stages.append(
                    {"stage": "cooperative_stop", "error_code": str(error)}
                )
            if handle._live_processes():
                if not allow_force:
                    raise TimeoutError("process_cooperative_timeout")
                if self.shutdown_deadline is not None:
                    if self._force_budget is None:
                        self._force_budget = ShutdownBudget.start(5.0)
                    force_budget = self._force_budget
                else:
                    force_budget = ShutdownBudget.start(5.0)
                launcher_log(
                    f"{handle.role} (pid={process.pid}) did not exit in time, "
                    "terminating process tree"
                )
                await handle.force_close(force_budget)
            self.release(process, handle.exit_reason or "exited")
        finally:
            current_budget.reset(token)

    async def shutdown(self) -> None:
        self.begin_shutdown()
        extra_roles = set(self._role_pids) - {"qq_ingress", "worker"}
        first_error = None
        for role in ("qq_ingress", *sorted(extra_roles), "worker"):
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
            for closer in reversed(self._closers):
                try:
                    await closer()
                except Exception as error:
                    first_error = first_error or error
            self._closers.clear()
            if first_error is not None:
                raise first_error
        finally:
            current_budget.reset(token)

    def initialize(self) -> str:
        self.boot_id = uuid.uuid4().hex
        os.environ["ZHENXUN_LAUNCHER_BOOT_ID"] = self.boot_id
        return self.boot_id

    def attach(self, role: str, process: subprocess.Popen[Any]) -> ProcessHandle:
        if process.pid in self._handles:
            return self._handles[process.pid]
        previous_pid = self._role_pids.get(role)
        if previous_pid is not None and previous_pid in self._handles:
            previous = self._handles[previous_pid]
            if previous.process.poll() is None:
                raise RuntimeError(f"launcher_process_already_running:{role}")
        handle = ProcessHandle(role, process)
        self._handles[process.pid] = handle
        self._role_pids[role] = process.pid
        return handle

    def release(self, process: subprocess.Popen[Any], reason: str) -> None:
        handle = self._handles.get(process.pid)
        if handle is not None and handle._live_processes():
            handle.exit_reason = "spawn_exited_tree_alive"
            return
        self._handles.pop(process.pid, None)
        self._released_processes.add(process)
        if handle is not None:
            handle.accepting = False
            handle.exit_reason = reason
            if self._role_pids.get(handle.role) == process.pid:
                self._role_pids.pop(handle.role, None)

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
        if not handle.bind_runtime(
            runtime_pid=runtime_pid,
            worker_boot_id=str(status.get("boot_id") or "") or None,
            operating_mode=str(status.get("operating_mode") or "") or None,
        ):
            return False
        handle.readiness = str(status.get("state") or "starting")
        return True


launcher_supervisor = LauncherSupervisor()

__all__ = ["LauncherSupervisor", "ProcessHandle", "launcher_supervisor"]
