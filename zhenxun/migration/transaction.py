from __future__ import annotations

import asyncio
import time

from zhenxun.utils.atomic_json import read_json_locked, write_json_locked

from .commit import decide_restore, read_commit
from .errors import MigrationError
from .generation import abandon_generation
from .launcher import stop_worker_for_snapshot
from .maintenance import MaintenanceProcess
from .snapshot import assert_offline
from .tasks import MigrationBudget
from .validation_process import ValidationProcess


class RestoreTransaction:
    """One transaction owns phases and the real validation worker, including failure."""

    def __init__(self, service, identity, management, *, private=None, offline=False):
        self.service, self.identity = service, identity
        self.management = management
        self.private = private or {}
        self.offline = offline
        self.validation = None
        self.maintenance = None
        self.worker = None
        self.stopped = False
        self.applied = False
        receipt = service.accept(identity)
        remaining = receipt["deadline_at"] - time.time()
        if remaining <= 0:
            raise MigrationError("migration_budget_exhausted")
        self.budget = MigrationBudget.start(remaining)

    @property
    def store(self):
        return self.service.store

    @property
    def directory(self):
        return self.store.path("jobs", self.identity).parent

    async def prepare(self):
        self.store.transition(self.identity, "preparing")
        return await self.service.phases.run(
            self.identity,
            "restore_dependencies",
            budget=self.budget.phase(),
            private_input=self.private,
        )

    async def execute(
        self, *, worker=None, network=None, prepared=False, stop_ingress=None
    ):
        self.worker = worker
        try:
            if network is not None:
                from .recovery_context import save_management_context

                self.management, network = save_management_context(
                    self.store,
                    self.identity,
                    self.management,
                    network,
                    lease=self.service.lease,
                )
            if self.offline:
                if worker is not None:
                    raise MigrationError("migration_offline_worker_present")
                assert_offline(self.service.lease.project)
            if not prepared:
                await self.prepare()
            self.store.transition(self.identity, "quiescing")
            if worker is not None:
                from zhenxun.services.lifecycle.deadline import shutdown_budget

                with shutdown_budget(min(15, self.budget.deadline - time.monotonic())):
                    if stop_ingress is not None:
                        await stop_ingress()
                    shutdown = await self.service._wait(
                        lambda: stop_worker_for_snapshot(
                            worker, self.service.supervisor
                        ),
                        self.budget.phase(15),
                    )
                if (
                    shutdown.get("result") != "confirmed"
                    or shutdown.get("forced")
                    or not shutdown.get("process_tree_released")
                ):
                    raise MigrationError("migration_shutdown_unconfirmed")
                write_json_locked(self.directory / "quiesced.json", shutdown)
            self.stopped = True
            if network is not None:
                self.maintenance = MaintenanceProcess(
                    self.service, self.identity, self.management
                )
                await self.maintenance.start(network, budget=self.budget.phase())
            self.store.transition(self.identity, "snapshotting")
            prepared_plan = await self.service.phases.run(
                self.identity,
                "restore_prepare",
                budget=self.budget.phase(),
                private_input=self.private,
            )
            self.store.transition(self.identity, "applying")
            await self.service.phases.run(
                self.identity,
                "restore_apply",
                budget=self.budget.phase(),
                private_input={
                    **self.private,
                    "expected_revision": prepared_plan["revision"],
                },
            )
            self.applied = True
            # Release the old listener before validation binds final configuration.
            if self.maintenance is not None:
                await self.maintenance.close()
                self.maintenance = None
            self.store.transition(self.identity, "verifying")
            self.validation = ValidationProcess(
                self.service,
                self.identity,
                self.management,
                private=self.private,
            )
            validation = await self.validation.start(budget=self.budget.phase())
            self.store.transition(self.identity, "committing")
            release = await self.validation.close() if self.offline else None
            decide_restore(
                self.store,
                self.identity,
                lease=self.service.lease,
                validation=validation,
                mode="offline" if self.offline else "online",
                release=release,
            )
            await self.service.phases.run(
                self.identity, "restore_publish", budget=self.budget.phase()
            )
            if not self.offline:
                await self.validation.wait_for("promoted", budget=self.budget.phase())
                from .configuration import publish_process_environment

                publish_process_environment(
                    self.store,
                    self.identity,
                    self.private.get("configuration"),
                    lease=self.service.lease,
                )
            dependency = read_json_locked(self.directory / "dependency-result.json", {})
            dependency["plugin_initialization"] = "completed"
            dependency["failed_plugins"] = validation.get("failed_plugins", [])
            partial = bool(
                validation.get("failed_plugins")
                or dependency.get("missing")
                or dependency.get("consistency")
            )
            self.store.transition(
                self.identity,
                "partial" if partial else "completed",
                progress={
                    "validation": validation,
                    "offline": self.offline,
                    "dependencies": dependency,
                    "original_worker_stopped": self.stopped,
                    "business_opened": not self.offline,
                },
            )
            self.private.clear()
            return None if self.offline else self.validation.process
        except BaseException as error:
            code = (
                error.code
                if isinstance(error, MigrationError)
                else "migration_cancelled"
                if isinstance(error, asyncio.CancelledError)
                else "migration_restore_failed"
            )
            try:
                await self.fail(code, network=network)
            except BaseException as cleanup_error:
                # Keep the triggering error; cleanup diagnostics are a separate
                # durable receipt, including when the record itself is damaged.
                write_json_locked(
                    self.directory / "restore-cleanup-failure.json",
                    {
                        "first_error": code,
                        "cleanup_error": getattr(
                            cleanup_error, "code", "migration_cleanup_failed"
                        ),
                    },
                )
            raise

    async def fail(self, code, *, network=None):
        job = self.store.read("jobs", self.identity)
        self.store.transition(self.identity, job["stage"], error_code=code)
        decision = read_commit(self.store, self.identity)
        if self.validation is not None and self.validation.process is not None:
            try:
                await self.validation.close()
            except BaseException:
                self.store.transition(
                    self.identity,
                    "recovery_required",
                    error_code=code,
                    progress={"validation_release": "unconfirmed"},
                )
                return
        if decision is not None:
            self.store.transition(
                self.identity,
                "recovery_required",
                error_code=code,
                progress={"commit_decided": True},
            )
            return
        applied = (self.directory / "restore-application.json").exists()
        if applied or job["stage"] in {"applying", "verifying", "committing"}:
            self.store.transition(self.identity, "rolling_back", error_code=code)
            try:
                result = await self.service.phases.run(
                    self.identity,
                    "restore_rollback",
                    budget=self.budget.phase(),
                    private_input=self.private,
                )
                abandon_generation(
                    self.service.lease.project, self.identity, lease=self.service.lease
                )
                self.store.transition(self.identity, "rolled_back", progress=result)
            except BaseException as rollback_error:
                self.store.transition(
                    self.identity,
                    "recovery_required",
                    error_code=getattr(
                        rollback_error, "code", "migration_restore_rollback_failed"
                    ),
                )
        else:
            abandon_generation(
                self.service.lease.project, self.identity, lease=self.service.lease
            )
            self.store.transition(
                self.identity,
                "needs_preflight"
                if job["stage"] == "snapshotting"
                and code
                in {
                    "migration_target_changed",
                    "migration_database_target_changed",
                    "migration_database_candidate_changed",
                }
                else "failed",
                error_code=code,
                progress={"original_worker_resume_required": self.stopped},
            )
        if self.maintenance is None and self.stopped and network is not None:
            self.maintenance = MaintenanceProcess(
                self.service, self.identity, self.management
            )
            await self.maintenance.start(network, budget=self.budget.phase())
