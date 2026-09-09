from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
import hashlib
import json
from threading import RLock
import time

from filelock import FileLock

from zhenxun.utils.atomic_json import read_json_locked, write_json_locked

from .control import LocalControlServer
from .errors import MigrationError
from .lease import InstanceLease
from .phases import PhaseSupervisor
from .service import capabilities
from .tasks import MigrationBudget, TaskStore


class LauncherMigrationService:
    def __init__(self, lease: InstanceLease, supervisor):
        self.lease, self.supervisor = lease, supervisor
        self.store = TaskStore(lease.project)
        self.phases = PhaseSupervisor(lease, supervisor)
        self.control = LocalControlServer(lease, self.dispatch, with_peer=True)
        self._waiters: set[asyncio.Task] = set()
        self._dispatch_lock = RLock()
        self._export_worker = None
        self._queued_export = None
        self._accepted_export_id = None
        self._queued_restore = None
        self._accepted_restore_id = None
        self._maintenance = None
        self._validation = None
        self.recovered_worker = None
        self._recovery_credentials = {}
        self._recovery_event = None
        self._recovery_loop = None
        self._retention_running = False

    def management_peers(self):
        return tuple(
            {"pid": process.pid, "created_at": process.create_time()}
            for handle in self.supervisor._handles.values()
            if handle.role == "migration_management"
            and handle.operating_mode == "maintenance"
            and handle.runtime_pid is not None
            for process in handle._live_processes(discover=True)
        )

    def forget_recovery_credentials(self, identity):
        with self._dispatch_lock:
            self._recovery_credentials.pop(identity, None)

    def recovery_failed(self, identity, error):
        self.forget_recovery_credentials(identity)
        job = self.store.read("jobs", identity)
        if job["stage"] == "committed":
            self.store.transition(identity, "recovery_required", error_code=error.code)
            job = self.store.read("jobs", identity)
        self.store.transition(
            identity,
            job["stage"],
            error_code=error.code,
            progress={**job["progress"], "last_recovery_error": error.code},
        )

    async def wait_for_reauthorization(self, stop_event):
        with self._dispatch_lock:
            if stop_event.is_set():
                return False
            if self._recovery_credentials:
                return True
            if self._recovery_event is None:
                self._recovery_event = asyncio.Event()
                self._recovery_loop = asyncio.get_running_loop()
            self._recovery_event.clear()
        credential = asyncio.create_task(self._recovery_event.wait())
        stopped = asyncio.create_task(stop_event.wait())
        try:
            await asyncio.wait(
                {credential, stopped}, return_when=asyncio.FIRST_COMPLETED
            )
            return not stop_event.is_set()
        finally:
            for task in (credential, stopped):
                if not task.done():
                    task.cancel()
            await asyncio.gather(credential, stopped, return_exceptions=True)

    def bind_export_worker(self, worker) -> None:
        with self._dispatch_lock:
            self._export_worker = worker

    def take_export(self):
        with self._dispatch_lock:
            value, self._queued_export = self._queued_export, None
            return value

    def take_restore(self):
        with self._dispatch_lock:
            value, self._queued_restore = self._queued_restore, None
            return value

    def _enqueue_restore(self, payload: dict, *, peer_pid: int) -> dict:
        from .configuration import bound_environment
        from .maintenance_app import ManagementSnapshot
        from .transaction import RestoreTransaction

        with self._dispatch_lock:
            worker = self._export_worker
            handle = self.supervisor._handles.get(worker.pid) if worker else None
            identity = payload.get("task_id")
            job = self.store.read("jobs", identity)
            first = job["options"].get("first_deployment") is True
            if (
                handle is None
                or worker.poll() is not None
                or handle.runtime_pid != peer_pid
                or handle.operating_mode != ("setup_only" if first else "normal")
                or handle.readiness
                not in ({"management_ready"} if first else {"warmup_ready", "degraded"})
                or not handle.worker_boot_id
                or peer_pid not in {p.pid for p in handle._live_processes()}
                or self.supervisor.shutdown_deadline is not None
            ):
                raise MigrationError(
                    "migration_worker_identity_unconfirmed", status=409
                )
            options = job["options"]
            if job["action"] != "restore":
                raise MigrationError("migration_action_invalid")
            if options.get("requester_boot_id") != handle.worker_boot_id:
                raise MigrationError(
                    "migration_worker_identity_unconfirmed", status=409
                )
            if not options.get("source_trusted") or not options.get(
                "replacement_confirmed"
            ):
                raise MigrationError("migration_restore_confirmation_required")
            private = payload.get("private") or {}
            if not isinstance(private, dict):
                raise MigrationError("migration_private_input_invalid")
            bound_environment(options, private.get("configuration"))
            management = ManagementSnapshot.parse(payload.get("management"))
            receipt = self.accept(identity)
            if job["stage"] == "queued" and self._accepted_restore_id != identity:
                self._queued_restore = RestoreTransaction(
                    self, identity, management, private=private
                )
                self._accepted_restore_id = identity
            return receipt

    def _enqueue_export(self, payload: dict, *, peer_pid: int) -> dict:
        from .maintenance_app import ManagementSnapshot
        from .online import OnlineExport
        from .snapshot import ExportOptions

        with self._dispatch_lock:
            worker = self._export_worker
            handle = self.supervisor._handles.get(worker.pid) if worker else None
            if (
                handle is None
                or worker.poll() is not None
                or handle.runtime_pid != peer_pid
                or handle.operating_mode != "normal"
                or handle.readiness not in {"warmup_ready", "degraded"}
                or not handle.worker_boot_id
                or peer_pid not in {p.pid for p in handle._live_processes()}
                or self.supervisor.shutdown_deadline is not None
            ):
                raise MigrationError(
                    "migration_worker_identity_unconfirmed", status=409
                )
            identity = payload.get("task_id")
            job = self.store.read("jobs", identity)
            if job["action"] != "export":
                raise MigrationError("migration_action_invalid")
            options = job["options"]
            if options.get("requester_boot_id") != handle.worker_boot_id:
                raise MigrationError(
                    "migration_worker_identity_unconfirmed", status=409
                )
            ExportOptions(
                categories=frozenset(options.get("categories", [])),
                plaintext_confirmed=options.get("plaintext_confirmed") is True,
                dependencies=options.get("dependencies", True),
            )
            password = payload.get("archive_password")
            if password is not None and (
                not isinstance(password, str) or not 1 <= len(password.encode()) <= 4096
            ):
                raise MigrationError("migration_password_invalid")
            if not password and options.get("plaintext_confirmed") is not True:
                raise MigrationError("migration_plaintext_confirmation_required")
            snapshot = ManagementSnapshot.parse(payload.get("management"))
            receipt = self.accept(identity)
            if job["stage"] == "queued" and self._accepted_export_id != identity:
                self._queued_export = OnlineExport.accepted(
                    self, identity, password=password
                )
                self._accepted_export_id = identity
                self._queued_export.management_snapshot = snapshot
            elif self._queued_export and self._queued_export.identity != identity:
                raise MigrationError("migration_operation_in_progress", status=409)
            return receipt

    async def _wait(self, callback, budget: MigrationBudget):
        budget.checkpoint()
        task = asyncio.create_task(callback(), name="migration_worker_transition")
        self._waiters.add(task)

        def consume(completed):
            self._waiters.discard(completed)
            if not completed.cancelled():
                completed.exception()

        task.add_done_callback(consume)
        try:
            done, _ = await asyncio.wait(
                {task}, timeout=max(0, budget.deadline - time.monotonic())
            )
            if not done:
                raise MigrationError("migration_budget_exhausted")
            return task.result()
        finally:
            if not task.done():
                # Keep cancellation-resistant work owned until real completion.
                task.cancel()

    def dispatch(self, operation: str, payload: dict, *, peer_pid: int) -> dict:
        self.lease.require_held()
        if operation == "phase_input":
            return self.phases.input_for(payload, peer_pid=peer_pid)
        if operation in {"validation_input", "validation_state"}:
            if self._validation is None:
                raise MigrationError("migration_validation_identity_unconfirmed")
            return self._validation.dispatch(operation, payload, peer_pid=peer_pid)
        if operation == "maintenance_input":
            if self._maintenance is None:
                raise MigrationError("migration_management_identity_unconfirmed")
            return self._maintenance.input_for(payload, peer_pid=peer_pid)
        if operation == "capabilities":
            return {**capabilities(), "launcher_connected": True}
        if operation == "status":
            identity = payload.get("task_id")
            return (
                self.store.public(self.store.read("jobs", identity))
                if identity
                else self.store.list_jobs()
            )
        if operation == "cancel":
            return self.store.public(self.store.request_cancel(payload.get("task_id")))
        if operation == "credentials":
            return self.supply_recovery_credentials(payload)
        if operation == "export":
            return self._enqueue_export(payload, peer_pid=peer_pid)
        if operation == "restore":
            return self._enqueue_restore(payload, peer_pid=peer_pid)
        # A transport is not proof that worker quiescing/validation is available.
        raise MigrationError("migration_maintenance_validation_unavailable", status=409)

    def accept(self, job_id: str) -> dict:
        """Persist launcher ownership before the worker may relinquish its task."""
        self.lease.require_held()
        if self._retention_running:
            raise MigrationError("migration_asset_cleanup_busy", status=409)
        job = self.store.read("jobs", job_id)
        seconds = job["options"].get("budget_seconds", 21600)
        if type(seconds) not in {int, float} or not 0 < seconds <= 21600:
            raise MigrationError("migration_budget_invalid")
        directory = self.store.path("jobs", job_id).parent
        with FileLock(str(directory / "handoff.lock"), timeout=0):
            previous = read_json_locked(directory / "handoff.json", None)
            digest = hashlib.sha256(
                json.dumps(job["options"], sort_keys=True).encode()
            ).hexdigest()
            if previous:
                if (
                    previous.get("launcher_boot_id") != self.supervisor.boot_id
                    or previous.get("options_sha256") != digest
                ):
                    raise MigrationError(
                        "migration_handoff_recovery_required", status=409
                    )
                return previous
            if job["stage"] != "queued":
                raise MigrationError("migration_handoff_stage_invalid", status=409)
            self.store.reserve(job_id)
            result = {
                "job_id": job_id,
                "launcher_boot_id": self.supervisor.boot_id,
                "lease_id": self.lease.identity,
                "options_sha256": digest,
                "accepted_at": time.time(),
                "deadline_at": time.time() + seconds,
            }
            write_json_locked(directory / "handoff.json", result)
            return result

    def supply_recovery_credentials(self, payload: dict) -> dict:
        from .native_database import DatabaseEndpoint

        self.lease.require_held()
        identity = payload.get("task_id")
        active = self.store.active()
        if (
            active is None
            or active["id"] != identity
            or active["action"] != "restore"
            or active["stage"] not in {"awaiting_credentials", "recovery_required"}
        ):
            raise MigrationError("migration_recovery_stage_invalid", status=409)
        directory = self.store.path("jobs", identity).parent
        plan = read_json_locked(directory / "restore-plan.json", None)
        if not isinstance(plan, dict) or plan.get("job_id") != identity:
            raise MigrationError("migration_recovery_receipt_invalid", status=409)
        database = payload.get("database")
        selected = plan.get("database") or {}
        if selected.get("engine") in {"mysql", "postgres"}:
            if not isinstance(database, dict) or set(database) != {"target_url"}:
                raise MigrationError("migration_database_credentials_required")
            endpoint = DatabaseEndpoint.parse(database.get("target_url", ""))
            if endpoint.identity() != selected.get("target"):
                raise MigrationError("migration_database_target_changed", status=409)
        elif database != {}:
            raise MigrationError("migration_database_credentials_not_applicable")
        with self._dispatch_lock:
            self._recovery_credentials[identity] = {"database": dict(database)}
            if self._recovery_loop is not None and not self._recovery_loop.is_closed():
                self._recovery_loop.call_soon_threadsafe(self._recovery_event.set)
        return {"task_id": identity, "credentials_received": True}

    async def recover_before_startup(self) -> dict | None:
        """Recover interrupted restore writes before any normal worker is spawned."""
        from .snapshot import assert_offline

        self.lease.require_held()
        job = self.store.active()
        if job is None:
            return None
        if job["action"] != "restore":
            if job["stage"] == "recovery_required":
                raise MigrationError("migration_startup_recovery_required", status=409)
            assert_offline(self.lease.project, management_peers=self.management_peers())
            directory = self.store.path("jobs", job["id"]).parent
            if (directory / "publication.json").exists():
                raise MigrationError(
                    "migration_publication_recovery_required", status=409
                )
            if job["stage"] == "quiescing":
                self.store.transition(
                    job["id"],
                    "recovery_required",
                    error_code="migration_shutdown_unconfirmed",
                )
                raise MigrationError("migration_shutdown_unconfirmed", status=409)
            return self.store.transition(
                job["id"], "cancelled", error_code="migration_export_interrupted"
            )
        identity = job["id"]
        directory = self.store.path("jobs", identity).parent
        if (directory / "restore-commit.json").exists() or job.get("committed_at"):
            from .commit import read_commit, reconcile_decision

            decision = read_commit(self.store, identity)
            if decision is None:
                raise MigrationError("migration_commit_receipt_missing")
            reconcile_decision(self.store, identity, lease=self.lease)
            assert_offline(self.lease.project, management_peers=self.management_peers())
            handoff = read_json_locked(directory / "handoff.json", {})
            remaining = handoff.get("deadline_at", 0) - time.time()
            if remaining <= 0:
                raise MigrationError("migration_budget_exhausted")
            await self.phases.run(
                identity,
                "restore_publish",
                budget=MigrationBudget.start(remaining).phase(),
            )
            if decision["mode"] == "online":
                return await self._recover_committed_worker(
                    identity, decision, handoff["deadline_at"]
                )
            dependencies = read_json_locked(directory / "dependency-result.json", {})
            partial = bool(
                decision["validation"].get("failed_plugins")
                or dependencies.get("missing")
                or dependencies.get("consistency")
            )
            return self.store.transition(
                identity,
                "partial" if partial else "completed",
                progress={"commit_recovered": True, "offline": True},
            )
        assert_offline(self.lease.project, management_peers=self.management_peers())
        application = read_json_locked(directory / "restore-application.json", None)
        journals = (
            directory / "restore-stage/files-journal.json.events",
            directory / "database-stage/database-journal.json.events",
            directory / "database-stage/database-journal.json",
        )
        if application is None and not any(path.exists() for path in journals):
            if job["stage"] not in {
                "queued",
                "preparing",
                "quiescing",
                "snapshotting",
                "preflight",
                "awaiting_confirmation",
            }:
                raise MigrationError(
                    "migration_application_receipt_missing", status=409
                )
            from .generation import abandon_generation

            abandon_generation(self.lease.project, identity, lease=self.lease)
            return self.store.transition(
                identity, "cancelled", error_code="migration_interrupted_before_apply"
            )
        handoff = read_json_locked(directory / "handoff.json", None)
        if (
            not isinstance(handoff, dict)
            or handoff.get("job_id") != identity
            or handoff.get("options_sha256")
            != hashlib.sha256(
                json.dumps(job["options"], sort_keys=True).encode()
            ).hexdigest()
            or not isinstance(application, dict)
            or application.get("job_id") != identity
        ):
            raise MigrationError("migration_recovery_receipt_invalid", status=409)
        remaining = handoff.get("deadline_at", 0) - time.time()
        if remaining <= 0:
            raise MigrationError("migration_budget_exhausted")
        write_json_locked(
            directory / "recovery-handoff.json",
            {
                "job_id": identity,
                "launcher_boot_id": self.supervisor.boot_id,
                "previous_launcher_boot_id": handoff["launcher_boot_id"],
                "lease_id": self.lease.identity,
                "deadline_at": handoff["deadline_at"],
            },
        )
        if job["stage"] not in {
            "applying",
            "verifying",
            "committing",
            "recovery_required",
            "rolling_back",
            "awaiting_credentials",
        }:
            raise MigrationError("migration_recovery_stage_invalid", status=409)
        native_plan = read_json_locked(directory / "restore-plan.json", {})
        native = (native_plan.get("database") or {}).get("engine") in {
            "mysql",
            "postgres",
        }
        with self._dispatch_lock:
            private = self._recovery_credentials.pop(identity, {})
        if native and not private:
            if job["stage"] != "awaiting_credentials":
                if job["stage"] != "rolling_back":
                    self.store.transition(
                        identity,
                        "rolling_back",
                        error_code="migration_restore_interrupted",
                    )
                self.store.transition(
                    identity,
                    "awaiting_credentials",
                    error_code="migration_database_credentials_required",
                )
            return self.store.public(self.store.read("jobs", identity))
        self.store.transition(
            identity,
            "rolling_back",
            error_code="migration_restore_interrupted"
            if job["stage"] != "rolling_back"
            else None,
        )
        try:
            result = await self.phases.run(
                identity,
                "restore_rollback",
                budget=MigrationBudget.start(remaining).phase(),
                private_input=private,
            )
        except BaseException as error:
            code = (
                error.code
                if isinstance(error, MigrationError)
                else "migration_recovery_failed"
            )
            self.store.transition(identity, "recovery_required", error_code=code)
            raise
        return self.store.transition(identity, "rolled_back", progress=result)

    async def _recover_committed_worker(self, identity, decision, deadline_at):
        from .configuration import committed_environment, publish_process_environment
        from .recovery_context import load_management_context
        from .validation_process import ValidationProcess

        remaining = deadline_at - time.time()
        if remaining <= 0:
            raise MigrationError("migration_budget_exhausted")
        budget = MigrationBudget.start(remaining)
        management, _ = load_management_context(self.store, identity, lease=self.lease)
        configuration = committed_environment(self.store, identity)
        private = {"configuration": configuration}
        job = self.store.read("jobs", identity)
        if (job["options"].get("database") or {}).get("engine") in {
            "mysql",
            "postgres",
        }:
            private["database"] = {"target_url": configuration.get("DB_URL", "")}
        validator = ValidationProcess(
            self, identity, management, private=private, recovery=True
        )
        try:
            validation = await validator.start(budget=budget.phase())
            await validator.wait_for("promoted", budget=budget.phase())
            publish_process_environment(
                self.store, identity, configuration, lease=self.lease
            )
            directory = self.store.path("jobs", identity).parent
            dependency = read_json_locked(directory / "dependency-result.json", {})
            partial = bool(
                validation.get("failed_plugins")
                or dependency.get("missing")
                or dependency.get("consistency")
            )
            result = self.store.transition(
                identity,
                "partial" if partial else "completed",
                progress={
                    "commit_recovered": True,
                    "offline": False,
                    "business_opened": True,
                    "validation": validation,
                    "dependencies": dependency,
                },
            )
            self.recovered_worker = validator.process
            self._validation = None
            validator.private.clear()
            return result
        except BaseException:
            await validator.close()
            raise
        finally:
            private.clear()

    async def recover_with_management(self) -> None:
        """Reopen the original authenticated listener while startup is blocked."""
        from .maintenance import MaintenanceProcess
        from .recovery_context import load_management_context

        try:
            await self.recover_before_startup()
        except MigrationError as error:
            active = self.store.active()
            if active is None:
                raise
            self.store.transition(active["id"], active["stage"], error_code=error.code)
            if active["stage"] in {"applying", "verifying", "committing", "committed"}:
                self.store.transition(active["id"], "recovery_required")
        active = self.store.active()
        if active is None:
            return
        snapshot, network = load_management_context(
            self.store, active["id"], lease=self.lease
        )
        # The listener can outlive the expired operation budget; it cannot grant
        # a fresh budget or automatically retry any destructive phase.
        maintenance = MaintenanceProcess(self, active["id"], snapshot)
        await maintenance.start(
            network, budget=MigrationBudget.start(30), keep_available=True
        )
        stop = asyncio.Event()

        async def observe_shutdown():
            while self.supervisor.shutdown_deadline is None:  # noqa: ASYNC110
                await asyncio.sleep(0.1)
            stop.set()

        watcher = asyncio.create_task(observe_shutdown())
        try:
            while await self.wait_for_reauthorization(stop):
                try:
                    await maintenance.close()
                    await self.recover_before_startup()
                except MigrationError as error:
                    self.recovery_failed(active["id"], error)
                    await maintenance.start(
                        network, budget=MigrationBudget.start(30), keep_available=True
                    )
                    continue
                if self.store.active() is None:
                    return
            raise MigrationError("migration_launcher_stopping")
        finally:
            watcher.cancel()
            await asyncio.gather(watcher, return_exceptions=True)
            await maintenance.close()

    async def export(
        self,
        job_id: str,
        *,
        quiesce: Callable[[], Awaitable[dict]],
        resume: Callable[[], Awaitable[None]],
        archive_password: str | None = None,
    ) -> dict:
        if self.store.read("jobs", job_id)["action"] != "export":
            raise MigrationError("migration_action_invalid")
        handoff = self.accept(job_id)
        budget = MigrationBudget.start(max(0.001, handoff["deadline_at"] - time.time()))
        if time.time() >= handoff["deadline_at"]:
            raise MigrationError("migration_budget_exhausted")
        stopped = False
        quiesce_started = False
        resumed = False
        resume_failed = False
        resume_attempted = False
        identity = job_id
        try:
            if self.store.read("jobs", identity)["cancel_requested"]:
                raise MigrationError("migration_cancelled")
            self.store.transition(identity, "preparing")
            self.store.transition(identity, "quiescing")
            quiesce_started = True
            shutdown = await self._wait(quiesce, budget.phase(15))
            if (
                shutdown.get("result") != "confirmed"
                or shutdown.get("forced")
                or not shutdown.get("process_tree_released")
            ):
                raise MigrationError("migration_shutdown_unconfirmed")
            stopped = True
            write_json_locked(
                self.store.path("jobs", identity).parent / "quiesced.json", shutdown
            )
            self.store.transition(identity, "snapshotting")
            await self.phases.run(identity, "export_snapshot", budget=budget.phase())
            self.store.transition(identity, "resuming")
            resume_attempted = True
            await self._wait(resume, budget.phase())
            resumed = True
            self.store.transition(identity, "compressing")
            return await self.phases.run(
                identity,
                "export_pack",
                budget=budget.phase(),
                private_input={"archive_password": archive_password}
                if archive_password
                else {},
            )
        except BaseException as error:
            code = (
                error.code
                if isinstance(error, MigrationError)
                else "migration_cancelled"
                if isinstance(error, asyncio.CancelledError)
                else "migration_export_failed"
            )
            if stopped and not resumed and not resume_attempted:
                resume_attempted = True
                try:
                    if self.supervisor.shutdown_deadline is not None:
                        raise MigrationError("migration_launcher_stopping")
                    await self._wait(resume, budget.phase())
                    resumed = True
                except BaseException:
                    resume_failed = True
            resume_failed = resume_failed or (resume_attempted and not resumed)
            record = self.store.read("jobs", identity)
            if record["stage"] != "completed":
                publication = (
                    self.store.path("jobs", identity).parent / "publication.json"
                )
                target = (
                    "recovery_required"
                    if publication.exists()
                    or resume_failed
                    or (quiesce_started and not stopped)
                    else "cancelled"
                    if code == "migration_cancelled"
                    else "failed"
                )
                self.store.transition(
                    identity,
                    target,
                    error_code=code,
                    progress={
                        "original_worker_resumed": resumed,
                        "resume_error": "migration_original_worker_resume_failed"
                        if resume_failed
                        else None,
                    },
                )
            raise

    async def close(self) -> None:
        from zhenxun.services.lifecycle.deadline import remaining_timeout

        self.control.stopped.set()
        with self._dispatch_lock:
            self._recovery_credentials.clear()
        deadline = time.monotonic() + remaining_timeout(2)
        thread = self.control.thread
        # A native thread has no asyncio completion primitive; poll its real state.
        while thread is not None and thread.is_alive() and time.monotonic() < deadline:  # noqa: ASYNC110
            await asyncio.sleep(0.02)
        self.control.close(deadline=deadline)
        pending = set(self._waiters)
        for task in pending:
            task.cancel()
        if pending:
            done, pending = await asyncio.wait(
                pending, timeout=max(0, deadline - time.monotonic())
            )
            for task in done:
                self._waiters.discard(task)
                if not task.cancelled():
                    task.exception()
        if pending:
            raise MigrationError("migration_worker_transition_unreleased")

    async def maintain_assets(self):
        from .analysis_process import run_analysis

        while not self.control.stopped.is_set():
            try:
                if self.store.active() is None:
                    self._retention_running = True
                    await run_analysis(
                        self.lease.project,
                        "retention",
                        budget=MigrationBudget.start(3600),
                    )
            except (MigrationError, OSError):
                # Cleanup failure never changes migration completion or deletes
                # evidence through an alternative path. The next interval retries.
                pass
            finally:
                self._retention_running = False
            await asyncio.sleep(3600)


async def install_launcher_migration(
    lease: InstanceLease, supervisor
) -> LauncherMigrationService | None:
    from zhenxun.services.lifecycle.models import ComponentSpec, RuntimeHandle

    service = LauncherMigrationService(lease, supervisor)

    def start():
        service.control.start()
        return RuntimeHandle(value=service, controller=service)

    supervisor.kernel.register(
        ComponentSpec(
            "launcher:migration_control",
            stage="management",
            scope="infrastructure",
            depends_on=("launcher:recovery",),
            finalizer_timeout=2,
            failure_policy="degrade",
        ),
        start,
    )
    await supervisor.kernel.start_components({"launcher:migration_control"})
    state = supervisor.kernel.component_status("launcher:migration_control")
    if state is None or state["state"] != "ready":
        # Optional migration transport must not prevent an otherwise valid bot
        # from starting on a platform without the required local IPC support.
        await service.close()
        return None
    task = asyncio.create_task(
        service.maintain_assets(), name="migration-asset-retention"
    )
    service._waiters.add(task)
    task.add_done_callback(service._waiters.discard)
    return service


async def stop_worker_for_snapshot(worker, supervisor) -> dict:
    handle = supervisor._handles.get(worker.pid)
    if handle is None or not handle.worker_boot_id or not handle.runtime_pid:
        raise MigrationError("migration_worker_identity_unconfirmed")
    await supervisor.stop_process(worker)
    receipt = handle.runtime_shutdown_receipt()
    unresolved_roles = sorted(
        item.role
        for item in supervisor._handles.values()
        if item._live_processes(discover=True)
    )
    return {
        "result": receipt.get("result", "unconfirmed") if receipt else "unconfirmed",
        "identity": receipt.get("identity", {}) if receipt else {},
        "forced": any(
            stage["stage"] in {"terminate", "kill"} for stage in handle.stop_stages
        ),
        "process_tree_released": not handle._live_processes(discover=True)
        and not unresolved_roles,
        "unresolved_roles": unresolved_roles,
    }
