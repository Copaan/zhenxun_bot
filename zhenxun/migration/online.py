from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
import time

from zhenxun.utils.atomic_json import write_json_locked

from .errors import MigrationError
from .tasks import TERMINAL, MigrationBudget


@dataclass
class OnlineExport:
    """A launcher-owned export spanning two real worker lifetimes."""

    service: object
    identity: str
    budget: MigrationBudget
    password: str | None = field(default=None, repr=False)
    management_snapshot: object | None = field(default=None, repr=False)
    first_error: str | None = None
    pack_task: asyncio.Task | None = None

    @classmethod
    def accepted(cls, service, identity: str, *, password: str | None = None):
        handoff = service.accept(identity)
        remaining = handoff["deadline_at"] - time.time()
        if remaining <= 0:
            raise MigrationError("migration_budget_exhausted")
        return cls(service, identity, MigrationBudget.start(remaining), password)

    @property
    def store(self):
        return self.service.store

    def _receipt(self, name: str, result: dict) -> None:
        write_json_locked(
            self.store.path("jobs", self.identity).parent / name,
            {
                "job_id": self.identity,
                "launcher_boot_id": self.service.supervisor.boot_id,
                "recorded_at": time.time(),
                **result,
            },
        )

    def begin(self) -> bool:
        try:
            self.budget.checkpoint()
        except MigrationError as error:
            self.fail(error.code)
            raise
        if self.store.read("jobs", self.identity)["cancel_requested"]:
            self.store.transition(self.identity, "cancelled")
            self.password = None
            return False
        self.store.transition(self.identity, "preparing")
        self.store.transition(self.identity, "quiescing")
        return True

    async def snapshot_after_shutdown(self, shutdown: dict, *, network=None) -> None:
        if (
            shutdown.get("result") != "confirmed"
            or shutdown.get("forced")
            or not shutdown.get("process_tree_released")
        ):
            self.fail("migration_shutdown_unconfirmed", recovery=True)
            raise MigrationError("migration_shutdown_unconfirmed")
        self._receipt("quiesced.json", shutdown)
        self.store.transition(self.identity, "snapshotting")
        management = None
        try:
            if network is not None:
                from .maintenance import MaintenanceProcess

                management = MaintenanceProcess(
                    self.service, self.identity, self.management_snapshot
                )
                await management.start(network, budget=self.budget.phase())
            await self.service.phases.run(
                self.identity, "export_snapshot", budget=self.budget.phase()
            )
        except Exception as error:
            # Even a failed/cancelled snapshot must resume the original instance.
            self.first_error = (
                error.code
                if isinstance(error, MigrationError)
                else "migration_snapshot_failed"
            )
        finally:
            if management is not None:
                try:
                    await management.close()
                except MigrationError as error:
                    self.fail(error.code, recovery=True)
                    raise
        self.store.transition(self.identity, "resuming", error_code=self.first_error)

    def resume_environment(self, startup_id: str) -> dict[str, str]:
        self.budget.checkpoint()
        if self.store.read("jobs", self.identity)["stage"] != "resuming":
            raise MigrationError("migration_export_phase_invalid")
        self._receipt("resume-authorization.json", {"startup_id": startup_id})
        return {"ZHENXUN_MIGRATION_RESUME_ID": self.identity}

    def resumed(self, worker) -> None:
        handle = self.service.supervisor._handles.get(worker.pid)
        if (
            handle is None
            or worker.poll() is not None
            or not handle.runtime_pid
            or not handle.worker_boot_id
            or handle.operating_mode != "normal"
            or handle.readiness not in {"warmup_ready", "degraded"}
        ):
            self.fail("migration_original_worker_resume_failed", recovery=True)
            raise MigrationError("migration_original_worker_resume_failed")
        self._receipt(
            "resumed.json",
            {
                "startup_id": handle.startup_id,
                "boot_id": handle.worker_boot_id,
                "runtime_pid": handle.runtime_pid,
                "spawn_pid": worker.pid,
            },
        )
        if (
            self.first_error
            or self.store.read("jobs", self.identity)["cancel_requested"]
        ):
            code = self.first_error or "migration_cancelled"
            self.fail(code)
            return
        self.store.transition(self.identity, "compressing")
        self.pack_task = asyncio.create_task(
            self._pack(), name=f"migration-export:{self.identity}"
        )
        self.service._waiters.add(self.pack_task)

    async def _pack(self) -> dict:
        try:
            return await self.service.phases.run(
                self.identity,
                "export_pack",
                budget=self.budget.phase(),
                private_input={"archive_password": self.password}
                if self.password
                else {},
            )
        except BaseException as error:
            self.fail(
                error.code
                if isinstance(error, MigrationError)
                else "migration_cancelled"
                if isinstance(error, asyncio.CancelledError)
                else "migration_export_failed"
            )
            raise
        finally:
            self.password = None

    def fail(self, code: str, *, recovery: bool = False) -> None:
        job = self.store.read("jobs", self.identity)
        if job["stage"] in TERMINAL or job["stage"] == "recovery_required":
            return
        self.first_error = self.first_error or code
        self.password = None
        publication = self.store.path("jobs", self.identity).parent / "publication.json"
        recovery = recovery or publication.exists()
        if job["stage"] in {"queued", "preparing"}:
            recovery = False
        self.store.transition(
            self.identity,
            "recovery_required"
            if recovery
            else "cancelled"
            if code == "migration_cancelled"
            else "failed",
            error_code=self.first_error,
            progress={"resume_error": code if recovery else None},
        )

    def collect(self) -> bool:
        if self.pack_task is None:
            return self.store.read("jobs", self.identity)["stage"] in TERMINAL
        if not self.pack_task.done():
            return False
        self.service._waiters.discard(self.pack_task)
        if not self.pack_task.cancelled():
            self.pack_task.exception()
        return True

    async def interrupt(self, code: str) -> None:
        self.fail(code, recovery=True)
        if self.pack_task is not None and not self.pack_task.done():
            self.pack_task.cancel()
            done, _ = await asyncio.wait(
                {self.pack_task},
                timeout=max(0, min(15, self.budget.deadline - time.monotonic())),
            )
            if not done:
                self.fail(code, recovery=True)
                raise MigrationError("migration_export_cleanup_unconfirmed")
        self.collect()
        self.fail(code, recovery=True)
