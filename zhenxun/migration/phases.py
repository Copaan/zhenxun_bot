from __future__ import annotations

import asyncio
from dataclasses import asdict
import os
from pathlib import Path
import re
import subprocess
import sys
from threading import RLock
import time
import uuid

import psutil

from zhenxun.utils.atomic_json import read_json_locked, write_json_locked

from .access import private_directory
from .archive import MANIFEST_LIMIT, build_archive, file_hash
from .errors import MigrationError
from .lease import DelegatedLease, InstanceLease
from .paths import contained_path
from .snapshot import ExportOptions, capture_snapshot
from .tasks import MigrationBudget, TaskStore

PHASES = {
    "export_snapshot": ("export", "snapshotting"),
    "export_pack": ("export", "compressing"),
    "export_reconcile": ("export", ("compressing", "recovery_required")),
    "restore_prepare": ("restore", "snapshotting"),
    "restore_dependencies": ("restore", "preparing"),
    "restore_publish": ("restore", "committed"),
    "restore_apply": ("restore", "applying"),
    "restore_rollback": ("restore", "rolling_back"),
}
_ID = re.compile(r"^[a-f0-9]{32}$")


class PhaseSupervisor:
    """One managed child per phase; credentials remain in the private IPC response."""

    def __init__(self, lease: InstanceLease, supervisor):
        lease.require_held()
        self.lease, self.supervisor = lease, supervisor
        self.store = TaskStore(lease.project)
        self._inputs: dict[str, dict] = {}
        self._lock = RLock()
        self._running = False

    def input_for(self, payload: dict, *, peer_pid: int) -> dict:
        identity = payload.get("invocation_id")
        if not isinstance(identity, str) or not _ID.fullmatch(identity):
            raise MigrationError("migration_phase_identity_invalid")
        with self._lock:
            value = self._inputs.get(identity)
            if not value or not value.get("pid") or value.get("created_at") is None:
                raise MigrationError("migration_phase_identity_mismatch")
            try:
                peer = psutil.Process(peer_pid)
                ancestry = {
                    parent.pid: parent.create_time() for parent in peer.parents()
                }
                ancestry[peer.pid] = peer.create_time()
                if ancestry.get(value["pid"]) != value["created_at"]:
                    raise MigrationError("migration_phase_identity_mismatch")
                if value.get("runtime_pid", peer_pid) != peer_pid:
                    raise MigrationError("migration_phase_identity_mismatch")
                value.update(
                    runtime_pid=peer_pid, runtime_created_at=peer.create_time()
                )
            except psutil.Error:
                raise MigrationError("migration_phase_identity_mismatch") from None
            self.lease.require_held()
            if time.monotonic() >= value["deadline"]:
                raise MigrationError("migration_budget_exhausted")
            return {
                **value["request"],
                "remaining_seconds": value["deadline"] - time.monotonic(),
                "management_peers": [
                    {"pid": process.pid, "created_at": process.create_time()}
                    for handle in self.supervisor._handles.values()
                    if handle.role == "migration_management"
                    and handle.operating_mode == "maintenance"
                    and handle.runtime_pid is not None
                    for process in handle._live_processes(discover=True)
                ],
            }

    async def run(
        self,
        job_id: str,
        phase: str,
        *,
        budget: MigrationBudget,
        private_input: dict | None = None,
    ) -> dict:
        if self._running:
            raise MigrationError("migration_phase_busy", status=409)
        if phase not in PHASES:
            raise MigrationError("migration_phase_invalid")
        self.lease.require_held()
        budget.checkpoint()
        job = self.store.read("jobs", job_id)
        if self.store.active() is None or self.store.active()["id"] != job_id:
            raise MigrationError("migration_operation_not_reserved")
        action, expected = PHASES[phase]
        stages = (expected,) if isinstance(expected, str) else expected
        if job["action"] != action or job["stage"] not in stages:
            raise MigrationError("migration_phase_stage_mismatch")
        invocation = uuid.uuid4().hex
        directory = self.store.path("jobs", job_id).parent / "phases" / invocation
        private_directory(directory)
        receipt_path = directory / "result.json"
        request = {
            "job_id": job_id,
            "phase": phase,
            "invocation_id": invocation,
            "launcher_boot_id": self.supervisor.boot_id,
            "job_revision": job["revision"],
            "private_input": private_input or {},
        }
        process = None
        self._running = True
        with self._lock:
            self._inputs[invocation] = {"request": request, "deadline": budget.deadline}

        def spawn():
            child = subprocess.Popen(
                [sys.executable, "-m", "zhenxun.migration.phase_worker", invocation],
                cwd=self.lease.project,
                env={
                    **os.environ,
                    "PYTHONPATH": os.pathsep.join(
                        filter(
                            None,
                            (
                                str(Path(__file__).resolve().parents[2]),
                                os.environ.get("PYTHONPATH"),
                            ),
                        )
                    ),
                    "ZHENXUN_LAUNCHER_PID": str(os.getpid()),
                    "ZHENXUN_INSTANCE_LEASE_ID": self.lease.identity,
                    "ZHENXUN_LAUNCHER_BOOT_ID": self.supervisor.boot_id,
                },
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0,
            )
            with self._lock:
                self._inputs[invocation].update(
                    pid=child.pid, created_at=psutil.Process(child.pid).create_time()
                )
            return child

        try:
            process = await self.supervisor.start_process(
                "migration_phase",
                spawn,
                startup_id=invocation,
                completion_expected=True,
            )
            while process.poll() is None:
                budget.checkpoint()
                self.supervisor._publish_processes()
                if (
                    phase != "restore_rollback"
                    and self.store.read("jobs", job_id)["cancel_requested"]
                ):
                    raise MigrationError("migration_cancelled")
                if self.supervisor.shutdown_deadline is not None:
                    raise MigrationError("migration_launcher_stopping")
                await asyncio.sleep(0.05)
            result = read_json_locked(receipt_path, None)
            with self._lock:
                process_created = self._inputs[invocation].get("runtime_created_at")
                runtime_pid = self._inputs[invocation].get("runtime_pid")
            if not isinstance(result, dict) or any(
                result.get(key) != expected
                for key, expected in {
                    "job_id": job_id,
                    "phase": phase,
                    "invocation_id": invocation,
                    "launcher_boot_id": self.supervisor.boot_id,
                    "pid": runtime_pid,
                    "created_at": process_created,
                    "job_revision": job["revision"],
                }.items()
            ):
                raise MigrationError("migration_phase_result_unconfirmed")
            if process.returncode != 0 or result.get("state") != "completed":
                code = result.get("error_code", "migration_phase_failed")
                if not isinstance(code, str) or not re.fullmatch(
                    r"migration_[a-z0-9_]{1,100}", code
                ):
                    code = "migration_phase_failed"
                raise MigrationError(code)
            return result["result"]
        finally:
            try:
                if process is not None:
                    await self.supervisor.stop_process(process)
            finally:
                with self._lock:
                    self._inputs.pop(invocation, None)
                self._running = False


def execute_phase(project: Path, request: dict, *, lease: DelegatedLease) -> dict:
    phase, identity = request["phase"], request["job_id"]
    store = TaskStore(project)
    job = store.read("jobs", identity)
    if job["revision"] != request["job_revision"] or (
        job["cancel_requested"] and phase != "restore_rollback"
    ):
        raise MigrationError("migration_phase_job_changed", status=409)
    if phase.startswith("restore_"):
        from .restore_phases import execute_restore_phase

        return execute_restore_phase(project, request, lease=lease)
    budget = MigrationBudget.start(min(float(request["remaining_seconds"]), 3600))

    def check():
        budget.checkpoint()
        lease.require_held()
        if store.read("jobs", identity)["cancel_requested"]:
            raise MigrationError("migration_cancelled")

    options = ExportOptions(
        categories=frozenset(job["options"]["categories"]),
        plaintext_confirmed=job["options"]["plaintext_confirmed"],
        dependencies=job["options"].get("dependencies", True),
    )
    directory = store.path("jobs", identity).parent
    snapshot = directory / "snapshot"
    description = directory / "snapshot.json"
    if phase == "export_reconcile":
        return store.reconcile_export(identity, lease=lease, checkpoint=check)
    if phase == "export_snapshot":
        files, metadata = capture_snapshot(
            project,
            snapshot,
            options=options,
            lease=lease,
            budget=budget,
            checkpoint=check,
        )
        entries = [
            {
                **asdict(entry),
                "sha256": file_hash(
                    contained_path(snapshot, entry.path, regular=True), check
                ),
            }
            for entry in files
        ]
        write_json_locked(description, {"files": entries, "metadata": metadata})
        return {"files": len(files), "bytes": sum(entry.size for entry in files)}
    if phase == "export_pack":
        from .discovery import FileEntry

        if description.stat().st_size > MANIFEST_LIMIT:
            raise MigrationError("migration_snapshot_manifest_limit")
        data = read_json_locked(description, None)
        if not isinstance(data, dict):
            raise MigrationError("migration_snapshot_missing")
        files = []
        for entry in data["files"]:
            if (
                file_hash(contained_path(snapshot, entry["path"], regular=True), check)
                != entry["sha256"]
            ):
                raise MigrationError("migration_snapshot_changed")
            files.append(
                FileEntry(
                    **{key: value for key, value in entry.items() if key != "sha256"}
                )
            )
        password = request["private_input"].get("archive_password")
        if password is not None and (
            not isinstance(password, str) or len(password.encode()) > 4096
        ):
            raise MigrationError("migration_password_invalid")
        destination = directory / "export.zx"
        return build_archive(
            snapshot,
            files,
            destination,
            metadata=data["metadata"],
            password=password.encode() if password else None,
            plaintext_confirmed=options.plaintext_confirmed,
            checkpoint=check,
            publish=lambda src, dst, outcome: store.publish_export(
                identity, src, dst, outcome
            ),
        )
    raise MigrationError("migration_phase_invalid")
