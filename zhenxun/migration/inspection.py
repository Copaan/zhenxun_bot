from __future__ import annotations

import asyncio
import os
from pathlib import Path
import time
import uuid

from filelock import FileLock, Timeout

from zhenxun.utils.atomic_json import read_json_locked, write_json_locked

from .errors import MigrationError
from .paths import contained_path
from .tasks import TaskStore

_TERMINAL = {"succeeded", "failed", "expired"}


def _process_created_at(pid: int) -> float | None:
    try:
        import psutil

        return float(psutil.Process(pid).create_time())
    except (ImportError, OSError, ValueError):
        return None


class InspectionCoordinator:
    """Coordinate one durable read-only inspection per inspection kind."""

    def __init__(self, project: Path, kind: str):
        if kind not in {"archive", "database"}:
            raise ValueError(kind)
        self.store = TaskStore(project)
        self.kind = kind
        self.boot_id = uuid.uuid4().hex
        self.pid = os.getpid()
        self.created_at = _process_created_at(self.pid)
        self._tasks: dict[str, asyncio.Task] = {}
        self._runner_tasks: dict[str, asyncio.Task] = {}
        self._reaper_tasks: dict[str, asyncio.Task] = {}

    @property
    def _active_path(self) -> Path:
        self.store._initialize()
        return contained_path(self.store.root, f"inspection-{self.kind}.json")

    @property
    def _lock_path(self) -> Path:
        self.store._initialize()
        return contained_path(self.store.root, f"inspection-{self.kind}.lock")

    def _busy(self, record: dict | None, *, status: int = 409) -> MigrationError:
        details = {}
        if record:
            details["inspection"] = self.store.public_inspection(record)
        return MigrationError(
            "migration_inspection_busy", status=status, details=details
        )

    def _owner_alive(self, record: dict) -> bool:
        pid = record.get("owner_pid")
        created_at = record.get("owner_created_at")
        if type(pid) is not int or pid <= 0:
            return False
        actual = _process_created_at(pid)
        return actual is not None and (
            created_at is None or abs(actual - float(created_at)) < 2
        )

    def _read_active(self) -> dict | None:
        value = read_json_locked(self._active_path, None)
        if not isinstance(value, dict) or not value.get("id"):
            return None
        try:
            record = self.store.read("inspections", value["id"])
        except MigrationError:
            return None
        if record.get("status") in _TERMINAL and record.get("completion") is not None:
            return None
        return record

    def _clear_active(self, identity: str) -> None:
        value = read_json_locked(self._active_path, None)
        if isinstance(value, dict) and value.get("id") == identity:
            self._active_path.unlink(missing_ok=True)

    def _recover_dead_owner(self, record: dict) -> None:
        if self._owner_alive(record):
            return
        self.store.update_inspection(
            record["id"],
            status="failed",
            phase="worker_lost",
            error={"code": "migration_inspection_worker_lost"},
        )
        self._clear_active(record["id"])

    def _find_completed(self, key: str) -> dict | None:
        directory = self.store.root / "inspections"
        if not directory.is_dir():
            return None
        matches = []
        for child in directory.iterdir():
            if not child.is_dir() or not (child / "record.json").is_file():
                continue
            try:
                record = self.store.read("inspections", child.name)
            except MigrationError:
                continue
            if (
                record.get("kind") == self.kind
                and record.get("key") == key
                and record.get("status") == "succeeded"
            ):
                matches.append(record)
        return max(matches, key=lambda value: value.get("updated_at", 0), default=None)

    async def submit(self, session: str, *, key: str, request: dict, runner):
        self.store._initialize()
        try:
            with FileLock(str(self._lock_path), timeout=0, thread_local=False):
                active = self._read_active()
                if active:
                    if active.get("key") == key:
                        return self.store.public_inspection(active)
                    expired = active.get("deadline", 0) <= time.time()
                    if expired and not self._owner_alive(active):
                        self._recover_dead_owner(active)
                        active = None
                    if active:
                        raise self._busy(active)
                completed = self._find_completed(key)
                if completed:
                    return self.store.public_inspection(completed)
                record = self.store.create_inspection(
                    session,
                    self.kind,
                    key,
                    request,
                    deadline=time.time() + 3600,
                    owner_pid=self.pid,
                    owner_created_at=self.created_at,
                    owner_boot_id=self.boot_id,
                )
                write_json_locked(self._active_path, {"id": record["id"]})
        except Timeout:
            raise self._busy(self._read_active()) from None

        task = asyncio.create_task(
            self._run(record["id"], runner),
            name=f"migration-inspection:{self.kind}:{record['id']}",
        )
        self._tasks[record["id"]] = task
        task.add_done_callback(lambda _: self._tasks.pop(record["id"], None))
        return self.store.public_inspection(record)

    async def _run(self, identity: str, runner) -> None:
        handed_off = False
        runner_task = asyncio.create_task(
            runner(), name=f"migration-inspection-worker:{self.kind}:{identity}"
        )
        self._runner_tasks[identity] = runner_task
        try:
            record = self.store.update_inspection(
                identity, status="running", phase="starting"
            )
            self.store.update_inspection(
                identity,
                phase="checking",
                progress={"step": "checking", "updated_at": time.time()},
            )
            remaining = max(0.1, record["deadline"] - time.time())
            done, _ = await asyncio.wait({runner_task}, timeout=remaining)
            if not done:
                raise _InspectionDeadline
            outcome = runner_task.result()
            diagnostic = None
            if isinstance(outcome, tuple) and len(outcome) == 2:
                outcome, diagnostic = outcome
            if not isinstance(outcome, dict):
                raise MigrationError("migration_inspection_result_invalid")
            self.store.update_inspection(
                identity,
                status="succeeded",
                phase="completed",
                progress={"percent": 100, "updated_at": time.time()},
                result=outcome,
                diagnostic=diagnostic,
                completion={
                    "status": "finished",
                    "finished_at": time.time(),
                    "outcome": "succeeded",
                },
            )
        except _InspectionDeadline:
            handed_off = True
            self.store.update_inspection(
                identity,
                status="expired",
                phase="timed_out",
                error={"code": "migration_inspection_timeout"},
            )
            self._start_reaper(identity, runner_task)
        except asyncio.CancelledError:
            if not runner_task.done():
                handed_off = True
                self.store.update_inspection(
                    identity,
                    status="failed",
                    phase="cancelled",
                    error={"code": "migration_inspection_cancelled"},
                )
                self._start_reaper(identity, runner_task)
            else:
                self.store.update_inspection(
                    identity,
                    status="failed",
                    phase="cancelled",
                    error={"code": "migration_inspection_cancelled"},
                )
        except BaseException as error:
            self.store.update_inspection(
                identity,
                status="expired"
                if getattr(error, "code", None) == "migration_budget_exhausted"
                else "failed",
                phase="failed",
                error={
                    "code": getattr(error, "code", type(error).__name__),
                    **(
                        {"path": error.path}
                        if isinstance(error, MigrationError) and error.path
                        else {}
                    ),
                },
                completion={
                    "status": "finished",
                    "finished_at": time.time(),
                    "outcome": "failed",
                },
            )
        finally:
            self._runner_tasks.pop(identity, None)
            if not handed_off:
                self._release_active(identity)

    async def _reap(self, identity: str, runner_task: asyncio.Task) -> None:
        try:
            try:
                await runner_task
            except BaseException as error:
                completion = {
                    "status": "finished",
                    "finished_at": time.time(),
                    "outcome": "failed",
                    "error": type(error).__name__,
                }
            else:
                completion = {
                    "status": "finished",
                    "finished_at": time.time(),
                    "outcome": "completed_after_timeout",
                }
            self.store.update_inspection(identity, completion=completion)
        except (MigrationError, OSError):
            pass
        finally:
            self._runner_tasks.pop(identity, None)
            self._release_active(identity)

    def _start_reaper(self, identity: str, runner_task: asyncio.Task) -> None:
        task = asyncio.create_task(
            self._reap(identity, runner_task),
            name=f"migration-inspection-reap:{self.kind}:{identity}",
        )
        self._reaper_tasks[identity] = task
        task.add_done_callback(lambda _: self._reaper_tasks.pop(identity, None))

    def _release_active(self, identity: str) -> None:
        try:
            with FileLock(str(self._lock_path), timeout=5, thread_local=False):
                self._clear_active(identity)
        except (Timeout, OSError):
            pass

    def get(self, identity: str, session: str) -> dict:
        return self.store.public_inspection(
            self.store.read("inspections", identity, session=session)
        )


class _InspectionDeadline(Exception):
    """Marks the coordinator deadline without rewriting runner exceptions."""
