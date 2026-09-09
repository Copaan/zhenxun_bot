from __future__ import annotations

import asyncio
from copy import deepcopy
from dataclasses import asdict
import os
import subprocess
import sys
from threading import RLock
import time
import uuid

from zhenxun.utils.atomic_json import read_json_locked, write_json_locked

from .commit import read_commit
from .errors import MigrationError
from .tasks import _public_payload


class ValidationProcess:
    """Own the actual worker and validate all messages against its ProcessHandle."""

    def __init__(self, service, identity, management, *, private=None, recovery=False):
        self.service, self.identity = service, identity
        self.management = management
        self.private = deepcopy(private or {})
        self.startup_id = uuid.uuid4().hex
        self.process = None
        self.state = None
        self.sequence = -1
        self._deadline = 0.0
        self._lock = RLock()
        self.recovery = recovery
        self._recovered_decision = None

    def _peer(self, payload, peer_pid):
        self.service.lease.require_held()
        handle = (
            self.service.supervisor._handles.get(self.process.pid)
            if self.process
            else None
        )
        if (
            handle is None
            or self.process.poll() is not None
            or payload.get("task_id") != self.identity
            or payload.get("startup_id") != self.startup_id
            or peer_pid not in {p.pid for p in handle._live_processes(discover=True)}
            or (handle.runtime_pid is not None and peer_pid != handle.runtime_pid)
            or time.monotonic() >= self._deadline
        ):
            raise MigrationError("migration_validation_identity_unconfirmed")
        return handle

    def dispatch(self, operation, payload, *, peer_pid):
        with self._lock:
            handle = self._peer(payload, peer_pid)
            if operation == "validation_input":
                return {
                    "job_id": self.identity,
                    "startup_id": self.startup_id,
                    "launcher_boot_id": self.service.supervisor.boot_id,
                    "management": asdict(self.management),
                    "private": self.private,
                    "remaining_seconds": self._deadline - time.monotonic(),
                }
            state = payload.get("state")
            sequence = payload.get("sequence")
            if (
                not isinstance(state, dict)
                or type(sequence) is not int
                or sequence < self.sequence
                or state.get("job_id") != self.identity
                or state.get("startup_id") != self.startup_id
                or state.get("launcher_boot_id") != self.service.supervisor.boot_id
                or state.get("pid") != peer_pid
                or state.get("state")
                not in {"starting", "validated", "promoted", "failed"}
                or not state.get("worker_boot_id")
            ):
                raise MigrationError("migration_validation_state_invalid")
            _public_payload(state)
            if sequence == self.sequence and state != self.state:
                raise MigrationError("migration_validation_sequence_conflict")
            decision = read_commit(self.service.store, self.identity)
            if self.recovery:
                if state["state"] == "validated":
                    from .committed_recovery import recovered_decision

                    self._recovered_decision = recovered_decision(
                        self.service, self.identity, self.authorization, state
                    )
                decision = self._recovered_decision
            publication = read_json_locked(
                self.service.store.path("jobs", self.identity).parent
                / "restore-publication.json",
                None,
            )
            published = bool(
                decision
                and publication
                and publication.get("job_id") == self.identity
                and publication.get("decided_at") == decision["decided_at"]
                and publication.get("session_rotated") is True
                and publication.get("configuration_published") is True
                and publication.get("generation")
                == decision["validation"].get("generation")
            )
            previous = self.state.get("state") if self.state else None
            allowed = {
                None: {"starting", "validated", "failed"},
                "starting": {"starting", "validated", "failed"},
                "validated": {"validated", "promoted", "failed"},
                "promoted": {"promoted", "failed"},
                "failed": {"failed"},
            }
            if state["state"] not in allowed[previous]:
                raise MigrationError("migration_validation_transition_invalid")
            if state["state"] == "promoted" and (
                not published
                or decision["mode"] != "online"
                or decision["validation"].get("startup_id") != self.startup_id
                or decision["validation"].get("worker_boot_id")
                != state["worker_boot_id"]
                or state.get("business_opened") is not True
            ):
                raise MigrationError("migration_validation_promotion_unconfirmed")
            if (
                state["state"] in {"starting", "validated"}
                and state.get("business_opened") is not False
            ):
                raise MigrationError("migration_validation_business_gate_open")
            if self.state and self.state["worker_boot_id"] != state["worker_boot_id"]:
                raise MigrationError("migration_validation_identity_unconfirmed")
            if not handle.bind_runtime(
                runtime_pid=peer_pid,
                worker_boot_id=state["worker_boot_id"],
                operating_mode="normal"
                if state["state"] == "promoted"
                else "migration_validation",
            ):
                raise MigrationError("migration_validation_identity_unconfirmed")
            self.state, self.sequence = state, sequence
            write_json_locked(
                self.service.store.path("jobs", self.identity).parent
                / "validation-state.json",
                {**state, "sequence": sequence},
            )
            return {
                "decision": decision,
                "sequence": sequence,
                "publication_ready": published,
            }

    async def start(self, *, budget):
        self._deadline = budget.deadline
        job = self.service.store.read("jobs", self.identity)
        if (
            job["stage"] != ("committed" if self.recovery else "verifying")
            or job["action"] != "restore"
        ):
            raise MigrationError("migration_validation_stage_invalid")
        self.service._validation = self
        directory = self.service.store.path("jobs", self.identity).parent
        self.authorization = {
            "job_id": self.identity,
            "startup_id": self.startup_id,
            "launcher_boot_id": self.service.supervisor.boot_id,
            "lease_id": self.service.lease.identity,
        }
        if self.recovery:
            from .committed_recovery import authorize_revalidation

            authorize_revalidation(self.service, self.identity, self.startup_id)
        write_json_locked(
            directory / "validation-authorization.json",
            self.authorization,
        )
        environment = {
            **os.environ,
            "ZHENXUN_LAUNCHER_PID": str(os.getpid()),
            "ZHENXUN_LAUNCHER_BOOT_ID": self.service.supervisor.boot_id,
            "ZHENXUN_INSTANCE_LEASE_ID": self.service.lease.identity,
            "ZHENXUN_WORKER_STARTUP_ID": self.startup_id,
            "ZHENXUN_MIGRATION_VALIDATION_ID": self.identity,
        }
        environment.pop("ZHENXUN_MIGRATION_RESUME_ID", None)

        def spawn():
            return subprocess.Popen(
                [sys.executable, "-m", "zhenxun.cli", "run-worker"],
                cwd=self.service.lease.project,
                env=environment,
                stdin=subprocess.DEVNULL,
                creationflags=subprocess.CREATE_NEW_PROCESS_GROUP
                if os.name == "nt"
                else 0,
            )

        self.process = await self.service.supervisor.start_process(
            "worker",
            spawn,
            startup_id=self.startup_id,
        )
        return await self.wait_for("validated", budget=budget)

    async def wait_for(self, expected, *, budget):
        while True:
            budget.checkpoint()
            if self.service.supervisor.shutdown_deadline is not None:
                raise MigrationError("migration_launcher_stopping")
            if self.process is None or self.process.poll() is not None:
                raise MigrationError("migration_validation_worker_exited")
            with self._lock:
                state = self.state
            if state and state["state"] == "failed":
                raise MigrationError("migration_validation_failed")
            if state and state["state"] == expected:
                return state
            if (
                self.service.store.read("jobs", self.identity)["cancel_requested"]
                and read_commit(self.service.store, self.identity) is None
            ):
                raise MigrationError("migration_cancelled")
            await asyncio.sleep(0.05)

    async def close(self):
        if self.process is None:
            return None
        process = self.process
        handle = self.service.supervisor._handles.get(process.pid)
        await self.service.supervisor.stop_process(process)
        receipt = handle.runtime_shutdown_receipt() if handle else None
        if (
            handle is None
            or not receipt
            or handle._live_processes(discover=True)
            or any(s["stage"] in {"terminate", "kill"} for s in handle.stop_stages)
            or receipt.get("result") != "confirmed"
        ):
            raise MigrationError("migration_validation_release_unconfirmed")
        self.process = None
        self.service._validation = None
        self.private.clear()
        return {**receipt, "forced": False, "process_tree_released": True}
