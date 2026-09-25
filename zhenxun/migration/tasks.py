from __future__ import annotations

from collections.abc import Callable
from dataclasses import asdict, dataclass
import hashlib
import hmac
import os
from pathlib import Path
import re
import time
import uuid

from filelock import FileLock, Timeout

from zhenxun.utils.atomic_json import (
    mutate_json_locked,
    read_json_locked,
    write_json_locked,
)

from .access import private_directory
from .archive import Limits, file_hash, require_space
from .errors import MigrationError
from .paths import contained_path

UPLOAD_CHUNK = 8 * 1024 * 1024
PREFLIGHT_TTL = 30 * 60
_ID = re.compile(r"^[0-9a-f]{32}$")
_HASH = re.compile(r"^[0-9a-f]{64}$")
_CODE = re.compile(r"^[a-z][a-z0-9_]{0,127}$")
_SECRET_KEYS = {
    "password",
    "secret",
    "token",
    "credentials",
    "db_url",
    "url",
    "connection_url",
    "authorization",
    "access_token",
}
TERMINAL = frozenset(
    {"completed", "partial", "cancelled", "failed", "rolled_back", "needs_preflight"}
)

PROGRESS_WRITE_INTERVAL = 1.0
_TRANSITIONS = {
    "queued": {"preflight", "preparing", "awaiting_credentials", "cancelled", "failed"},
    "preflight": {"awaiting_confirmation", "failed", "cancelled"},
    "awaiting_confirmation": {"preparing", "cancelled", "failed"},
    "preparing": {"quiescing", "failed", "cancelled", "awaiting_credentials"},
    "quiescing": {"snapshotting", "failed", "cancelled", "recovery_required"},
    "snapshotting": {
        "needs_preflight",
        "resuming",
        "applying",
        "failed",
        "cancelled",
        "recovery_required",
    },
    "resuming": {"compressing", "failed", "cancelled", "recovery_required"},
    "compressing": {"completed", "failed", "cancelled", "recovery_required"},
    "applying": {"verifying", "rolling_back", "recovery_required"},
    "verifying": {"committing", "rolling_back", "recovery_required"},
    "committing": {"committed", "rolling_back", "recovery_required"},
    "committed": {"completed", "partial", "recovery_required"},
    "rolling_back": {
        "rolled_back",
        "cancelled",
        "recovery_required",
        "awaiting_credentials",
    },
    "recovery_required": {"rolling_back", "awaiting_credentials"},
    "awaiting_credentials": {"preparing", "rolling_back", "cancelled"},
}


def _public_payload(value, *, depth: int = 0, count: list[int] | None = None) -> None:
    count = [0] if count is None else count
    count[0] += 1
    if depth > 16 or count[0] > 10_000:
        raise MigrationError("migration_task_metadata_limit")
    if isinstance(value, dict):
        for key, item in value.items():
            if not isinstance(key, str) or key.casefold() in _SECRET_KEYS:
                raise MigrationError("migration_secret_in_task_forbidden")
            _public_payload(item, depth=depth + 1, count=count)
    elif isinstance(value, list):
        for item in value:
            _public_payload(item, depth=depth + 1, count=count)
    elif isinstance(value, str):
        if len(value) > 2048 or "://" in value or "\x00" in value:
            raise MigrationError("migration_task_metadata_invalid")
    elif value is not None and type(value) not in {bool, int, float}:
        raise MigrationError("migration_task_metadata_invalid")


@dataclass(frozen=True)
class MigrationBudget:
    deadline: float

    @classmethod
    def start(cls, seconds: float = 6 * 60 * 60) -> MigrationBudget:
        if not 0 < seconds <= 7 * 24 * 60 * 60:
            raise MigrationError("migration_budget_invalid")
        return cls(time.monotonic() + seconds)

    def phase(self, seconds: float = 3600) -> MigrationBudget:
        return MigrationBudget(min(self.deadline, time.monotonic() + max(0, seconds)))

    def checkpoint(self) -> None:
        if time.monotonic() >= self.deadline:
            raise MigrationError("migration_budget_exhausted")


class MigrationProgress:
    """Throttle durable phase progress without changing the task revision."""

    def __init__(
        self,
        store: TaskStore,
        identity: str,
        phase: str,
        *,
        start_percent: float = 0.0,
        end_percent: float = 100.0,
    ):
        self.store = store
        self.identity = identity
        self.phase = phase
        self.start_percent = start_percent
        self.end_percent = end_percent
        self.started_at = time.time()
        self.started_monotonic = time.monotonic()
        self.last_write = 0.0
        self.last_value = None
        job = store.read("jobs", identity)
        self.stage = job["stage"]
        self.revision = job["revision"]
        self.job_started_at = job["created_at"]
        self.step_started_at = self.started_monotonic
        self.console = False

    def update(
        self,
        *,
        current: int | None = None,
        total: int | None = None,
        unit: str | None = None,
        bytes_done: int | None = None,
        bytes_total: int | None = None,
        step: str | None = None,
        percent: float | None = None,
        force: bool = False,
    ) -> None:
        now = time.time()
        monotonic = time.monotonic()
        value = (
            current,
            total,
            unit,
            bytes_done,
            bytes_total,
            step,
            percent,
        )
        changed_step = self.last_value is None or step != self.last_value[5]
        if not force and not changed_step:
            interval = 5.0 if value == self.last_value else PROGRESS_WRITE_INTERVAL
            if monotonic - self.last_write < interval and not (
                percent == 100 and value != self.last_value
            ):
                return
        if changed_step:
            self.step_started_at = monotonic
        self.last_value = value
        self.last_write = monotonic
        elapsed = max(0.0, monotonic - self.step_started_at)
        phase_percent = percent
        eta = None
        if phase_percent is not None:
            phase_percent = max(0.0, min(100.0, float(phase_percent)))
            overall = (
                self.start_percent
                + (self.end_percent - self.start_percent) * phase_percent / 100
            )
            if phase_percent > 0:
                step_elapsed = monotonic - self.step_started_at
                if step_elapsed >= 2:
                    eta = step_elapsed * (100 / phase_percent - 1)
        else:
            overall = None
        record = self.store.update_progress(
            self.identity,
            {
                "phase": self.phase,
                "step": step,
                "percent": round(phase_percent, 2)
                if phase_percent is not None
                else None,
                "overall_percent": round(overall, 2) if overall is not None else None,
                "current": current,
                "total": total,
                "unit": unit,
                "bytes_done": bytes_done,
                "bytes_total": bytes_total,
                "elapsed_seconds": round(elapsed, 2),
                "total_elapsed_seconds": round(max(0.0, now - self.job_started_at), 2),
                "eta_seconds": round(max(0.0, eta), 2) if eta is not None else None,
                "updated_at": now,
            },
            expected_stage=self.stage,
            expected_revision=self.revision,
        )
        if self.console:
            from .progress import emit_progress

            emit_progress(record)

    def finish(self, *, step: str | None = None) -> None:
        values = self.last_value or (None,) * 7
        self.update(
            current=values[0],
            total=values[1],
            unit=values[2],
            bytes_done=values[3],
            bytes_total=values[4],
            percent=100,
            step=step,
            force=True,
        )


class TaskStore:
    """Durable, secret-free job records shared by CLI, worker and launcher."""

    def __init__(self, project: Path, *, clock: Callable[[], float] = time.time):
        self.project = project.absolute()
        self.root = contained_path(self.project, "migration/state/v1")
        self.clock = clock

    def _initialize(self) -> None:
        private_directory(contained_path(self.project, "migration"))
        private_directory(self.root)

    def path(self, kind: str, identity: str) -> Path:
        if (
            kind not in {"jobs", "uploads", "preflights", "inspections"}
            or not isinstance(identity, str)
            or not _ID.fullmatch(identity)
        ):
            raise MigrationError("migration_record_not_found", status=404)
        return contained_path(self.root, f"{kind}/{identity}/record.json")

    @staticmethod
    def owner(session: str) -> str:
        if not session:
            raise MigrationError("migration_session_required", status=401)
        return hashlib.sha256(session.encode()).hexdigest()

    def read(self, kind: str, identity: str, *, session: str | None = None) -> dict:
        path = self.path(kind, identity)
        if not path.exists():
            raise MigrationError("migration_record_not_found", status=404)
        value = read_json_locked(path, None)
        if not isinstance(value, dict) or value.get("id") != identity:
            raise MigrationError("migration_record_corrupt")
        if session is not None and not hmac.compare_digest(
            value["owner"], self.owner(session)
        ):
            raise MigrationError("migration_session_mismatch", status=403)
        return value

    def _create(
        self, kind: str, session: str, value: dict, *, identity: str | None = None
    ) -> dict:
        self._initialize()
        identity = identity or uuid.uuid4().hex
        path = self.path(kind, identity)
        if path.exists():
            raise MigrationError("migration_record_exists", status=409)
        private_directory(path.parent)
        result = {
            "schema": 1,
            "id": identity,
            "owner": self.owner(session),
            "created_at": self.clock(),
            "updated_at": self.clock(),
            **value,
        }
        write_json_locked(path, result)
        return result

    def create_job(
        self, session: str, action: str, options: dict, *, identity: str | None = None
    ) -> dict:
        if action not in {"export", "restore"}:
            raise MigrationError("migration_action_invalid")
        _public_payload(options)
        return self._create(
            "jobs",
            session,
            {
                "action": action,
                "options": options,
                "stage": "queued",
                "cancel_requested": False,
                "first_error": None,
                "rollback_error": None,
                "revision": 0,
                "progress": {},
            },
            identity=identity,
        )

    def create_inspection(
        self,
        session: str,
        kind: str,
        key: str,
        request: dict,
        *,
        deadline: float,
        owner_pid: int,
        owner_created_at: float | None,
        owner_boot_id: str,
    ) -> dict:
        if kind not in {"archive", "database"} or not _HASH.fullmatch(key):
            raise MigrationError("migration_inspection_request_invalid")
        _public_payload(request)
        now = self.clock()
        return self._create(
            "inspections",
            session,
            {
                "kind": kind,
                "key": key,
                "request": request,
                "status": "queued",
                "phase": "queued",
                "progress": {},
                "started_at": None,
                "deadline": deadline,
                "owner_pid": owner_pid,
                "owner_created_at": owner_created_at,
                "owner_boot_id": owner_boot_id,
                "result": None,
                "error": None,
                "diagnostic": None,
                "completion": None,
                "created_at": now,
                "updated_at": now,
            },
        )

    def update_inspection(
        self,
        identity: str,
        *,
        status: str | None = None,
        phase: str | None = None,
        progress: dict | None = None,
        result: dict | None = None,
        error: dict | None = None,
        diagnostic: dict | None = None,
        completion: dict | None = None,
    ) -> dict:
        if status is not None and status not in {
            "queued",
            "running",
            "succeeded",
            "failed",
            "expired",
        }:
            raise MigrationError("migration_inspection_status_invalid")
        for value in (progress, result, error, diagnostic, completion):
            if value is not None:
                if value is diagnostic:
                    if (
                        not isinstance(value, dict)
                        or len(str(value.get("stderr", ""))) > 1024 * 1024
                    ):
                        raise MigrationError("migration_task_metadata_invalid")
                else:
                    _public_payload(value)

        def update(record):
            if not isinstance(record, dict) or record.get("id") != identity:
                raise MigrationError("migration_record_not_found", status=404)
            if status is not None:
                record["status"] = status
                if status == "running" and record.get("started_at") is None:
                    record["started_at"] = self.clock()
            if phase is not None:
                record["phase"] = phase
            if progress is not None:
                record["progress"] = dict(progress)
            if result is not None:
                record["result"] = result
            if error is not None:
                record["error"] = error
            if diagnostic is not None:
                record["diagnostic"] = diagnostic
            if completion is not None:
                record["completion"] = completion
            record["updated_at"] = self.clock()
            return dict(record)

        return mutate_json_locked(self.path("inspections", identity), None, update)

    @staticmethod
    def public_inspection(value: dict) -> dict:
        result = {
            key: value[key]
            for key in (
                "id",
                "kind",
                "status",
                "phase",
                "progress",
                "created_at",
                "started_at",
                "updated_at",
                "deadline",
                "owner_pid",
                "owner_boot_id",
                "result",
                "error",
                "diagnostic",
                "completion",
            )
            if key in value
        }
        now = time.time()
        started = result.get("started_at") or result.get("created_at") or now
        result["elapsed_seconds"] = round(
            max(0.0, (result.get("updated_at") or now) - started), 2
        )
        result["remaining_seconds"] = round(
            max(0.0, (result.get("deadline") or now) - now),
            2,
        )
        return result

    def transition(
        self,
        identity: str,
        stage: str,
        *,
        progress: dict | None = None,
        error_code: str | None = None,
    ) -> dict:
        self.read("jobs", identity)
        if error_code and not _CODE.fullmatch(error_code):
            raise MigrationError("migration_error_code_invalid")
        _public_payload(progress or {})
        stage_changed = False

        def change(value):
            nonlocal stage_changed
            if not isinstance(value, dict):
                raise MigrationError("migration_record_not_found", status=404)
            current = value["stage"]
            stage_changed = current != stage
            decision = read_json_locked(
                self.path("jobs", identity).parent / "restore-commit.json", None
            )
            if decision is not None and stage in {
                "rolling_back",
                "rolled_back",
                "cancelled",
                "failed",
            }:
                raise MigrationError("migration_committed_rollback_forbidden")
            if stage != current and stage not in _TRANSITIONS.get(current, set()):
                raise MigrationError("migration_stage_transition_invalid", status=409)
            if (
                stage in {"committing", "committed", "completed", "partial"}
                and value["cancel_requested"]
                and not value.get("committed_at")
            ):
                raise MigrationError("migration_cancelled")
            if stage == "committed":
                value["committed_at"] = self.clock()
            value.update(
                stage=stage, updated_at=self.clock(), revision=value["revision"] + 1
            )
            if progress is not None:
                retained = {
                    key: value.get("progress", {})[key]
                    for key in (
                        "snapshot_mode",
                        "snapshot_generated",
                        "original_worker_resumed",
                        "runtime",
                    )
                    if (value["action"] == "export" or key == "runtime")
                    and key in value.get("progress", {})
                }
                value["progress"] = {**retained, **progress}
            if error_code:
                key = "rollback_error" if current == "rolling_back" else "first_error"
                value[key] = value.get(key) or error_code
            if stage in {"completed", "partial"}:
                value["expires_at"] = self.clock() + (
                    72 * 3600 if value["action"] == "export" else 7 * 86400
                )
            return dict(value)

        record = mutate_json_locked(self.path("jobs", identity), None, change)
        from .progress import emit_progress

        if stage_changed:
            emit_progress(record, stage_only=True)
        return record

    def update_progress(
        self,
        identity: str,
        progress: dict,
        *,
        expected_stage: str,
        expected_revision: int,
    ) -> dict:
        """Atomically update progress while preserving the phase revision."""
        self.read("jobs", identity)
        _public_payload(progress)
        import math

        for key in (
            "current",
            "total",
            "bytes_done",
            "bytes_total",
            "percent",
            "overall_percent",
            "elapsed_seconds",
            "total_elapsed_seconds",
            "eta_seconds",
        ):
            number = progress.get(key)
            if number is not None and (
                type(number) not in {int, float}
                or not math.isfinite(number)
                or number < 0
            ):
                raise MigrationError("migration_task_metadata_invalid")
        for key in ("percent", "overall_percent"):
            if progress.get(key) is not None and progress[key] > 100:
                raise MigrationError("migration_task_metadata_invalid")

        def change(value):
            if not isinstance(value, dict) or value.get("id") != identity:
                raise MigrationError("migration_record_not_found", status=404)
            if (
                value["stage"] != expected_stage
                or value["revision"] != expected_revision
                or value["stage"] in TERMINAL
            ):
                return dict(value)
            previous = value.get("progress", {}).get("runtime", {})
            progress_value = {
                **progress,
                "stage": value["stage"],
                "sequence": previous.get("sequence", 0) + 1,
            }
            value["progress"] = {
                **value.get("progress", {}),
                "runtime": progress_value,
            }
            value["updated_at"] = self.clock()
            return dict(value)

        return mutate_json_locked(self.path("jobs", identity), None, change)

    def request_cancel(self, identity: str) -> dict:
        self.read("jobs", identity)

        def cancel(value):
            if not isinstance(value, dict):
                raise MigrationError("migration_record_not_found", status=404)
            decision = read_json_locked(
                self.path("jobs", identity).parent / "restore-commit.json", None
            )
            if decision is not None:
                from .commit import options_digest

                if (
                    not isinstance(decision, dict)
                    or decision.get("decision") != "commit"
                    or decision.get("job_id") != identity
                    or decision.get("options_sha256")
                    != options_digest(value["options"])
                ):
                    raise MigrationError("migration_commit_receipt_invalid")
                value["cancel_result"] = "already_committed"
            elif value["stage"] not in TERMINAL and not value.get("committed_at"):
                value.update(
                    cancel_requested=True,
                    updated_at=self.clock(),
                    revision=value["revision"] + 1,
                )
            return dict(value)

        return mutate_json_locked(self.path("jobs", identity), None, cancel)

    def finish_interrupted_export(self, identity: str, *, lease) -> dict:
        """End a read-only export only after proving no instance writer remains."""
        from .snapshot import assert_offline

        lease.require_held()
        if lease.project.resolve() != self.project.resolve():
            raise MigrationError("migration_instance_lease_mismatch")
        assert_offline(self.project)
        directory = self.path("jobs", identity).parent
        if any(directory.glob("restore-*.json")) or any(
            (directory / name).exists() for name in ("restore-stage", "database-stage")
        ):
            raise MigrationError("migration_recovery_receipt_invalid")

        def finish(value):
            if not isinstance(value, dict) or value.get("action") != "export":
                raise MigrationError("migration_export_phase_invalid")
            if value["stage"] in TERMINAL:
                return dict(value)
            value.update(
                stage="failed",
                updated_at=self.clock(),
                revision=value["revision"] + 1,
                first_error=value.get("first_error") or "migration_export_interrupted",
                progress={
                    **value.get("progress", {}),
                    "export_recovered": True,
                    "original_instance_start_allowed": True,
                },
            )
            return dict(value)

        return mutate_json_locked(self.path("jobs", identity), None, finish)

    def publish_export(
        self, identity: str, source: Path, destination: Path, result: dict
    ) -> dict:
        _public_payload(result)
        receipt = self.path("jobs", identity).parent / "publication.json"

        def publish(value):
            if not isinstance(value, dict) or value.get("stage") != "compressing":
                raise MigrationError("migration_export_phase_invalid", status=409)
            if value["cancel_requested"]:
                raise MigrationError("migration_cancelled")
            outcome = {
                **{
                    key: item
                    for key, item in value.get("progress", {}).items()
                    if key
                    in {
                        "snapshot_mode",
                        "snapshot_generated",
                        "original_worker_resumed",
                        "runtime",
                    }
                },
                **result,
            }
            if receipt.exists():
                raise MigrationError("migration_publication_recovery_required")
            info = source.stat()
            write_json_locked(
                receipt,
                {
                    "schema": 1,
                    "job_id": identity,
                    "destination": str(destination.absolute()),
                    "device": info.st_dev,
                    "inode": info.st_ino,
                    "result": outcome,
                    "decided_at": self.clock(),
                },
            )
            os.link(source, destination)
            value.update(
                stage="completed",
                committed_at=self.clock(),
                expires_at=self.clock() + 72 * 3600,
                updated_at=self.clock(),
                revision=value["revision"] + 1,
                progress=outcome,
            )
            return dict(value)

        return mutate_json_locked(self.path("jobs", identity), None, publish)

    def reconcile_export(
        self, identity: str, *, lease, checkpoint=lambda: None
    ) -> dict:
        """Recover publication without replacing or removing an unknown file."""
        lease.require_held()
        if lease.project.resolve() != self.project.resolve():
            raise MigrationError("migration_instance_lease_mismatch")
        receipt_path = self.path("jobs", identity).parent / "publication.json"
        receipt = read_json_locked(receipt_path, None)
        if not isinstance(receipt, dict) or receipt.get("job_id") != identity:
            raise MigrationError("migration_publication_receipt_invalid")
        destination = Path(receipt["destination"])
        destination = contained_path(destination.parent, destination.name)
        if not destination.exists():
            raise MigrationError("migration_publication_unconfirmed")
        info = destination.stat()
        result = receipt["result"]
        if (
            (info.st_dev, info.st_ino) != (receipt["device"], receipt["inode"])
            or info.st_size != result["size"]
            or file_hash(destination, checkpoint) != result["sha256"]
        ):
            raise MigrationError("migration_publication_conflict")

        def reconcile(value):
            if not isinstance(value, dict) or value.get("action") != "export":
                raise MigrationError("migration_publication_receipt_invalid")
            if value["stage"] == "completed":
                return dict(value)
            if value["stage"] not in {"compressing", "recovery_required"}:
                raise MigrationError("migration_export_phase_invalid")
            value.update(
                stage="completed",
                committed_at=receipt["decided_at"],
                updated_at=self.clock(),
                expires_at=self.clock() + 72 * 3600,
                revision=value["revision"] + 1,
                progress=result,
                publication_recovered=True,
            )
            return dict(value)

        return mutate_json_locked(self.path("jobs", identity), None, reconcile)

    def list_jobs(self, *, offset: int = 0, limit: int = 20) -> dict:
        if offset < 0 or not 1 <= limit <= 100:
            raise MigrationError("migration_pagination_invalid")
        directory = self.root / "jobs"
        if not directory.is_dir():
            return {"items": [], "total": 0}
        records = []
        for path in directory.iterdir():
            # Creation makes the private directory before atomically publishing
            # record.json. Concurrent status reads must not treat that window
            # as a missing durable job.
            if _ID.fullmatch(path.name) and (path / "record.json").is_file():
                records.append(self.read("jobs", path.name))
            if len(records) > 10_000:
                raise MigrationError("migration_task_history_limit")
        records.sort(key=lambda item: (item["created_at"], item["id"]), reverse=True)
        return {
            "items": [self.public(item) for item in records[offset : offset + limit]],
            "total": len(records),
        }

    def reserve(self, identity: str) -> dict:
        self.read("jobs", identity)
        path = self.root / "active.json"

        def claim(value):
            if value and value.get("id") != identity:
                other = self.read("jobs", value["id"])
                if other["stage"] not in TERMINAL:
                    raise MigrationError("migration_operation_in_progress", status=409)
            value.clear()
            value.update(id=identity)
            return dict(value)

        return mutate_json_locked(path, {}, claim)

    def bind_requester(self, identity: str, session: str, boot_id: str) -> dict:
        """Bind a confirmed task once; never rewrite options after handoff."""
        self.read("jobs", identity, session=session)
        path = self.path("jobs", identity)

        def bind(value):
            if (path.parent / "handoff.json").exists():
                raise MigrationError("migration_handoff_already_recorded", status=409)
            if value.get("stage") != "queued":
                raise MigrationError("migration_handoff_stage_invalid", status=409)
            previous = value["options"].get("requester_boot_id")
            if previous is not None and previous != boot_id:
                raise MigrationError(
                    "migration_worker_identity_unconfirmed", status=409
                )
            value["options"]["requester_boot_id"] = boot_id
            _public_payload(value["options"])
            return dict(value)

        return mutate_json_locked(path, None, bind)

    def active(self) -> dict | None:
        path = self.root / "active.json"
        if not path.exists():
            return None
        value = read_json_locked(path, None)
        if not isinstance(value, dict) or not value.get("id"):
            raise MigrationError("migration_active_record_corrupt")
        job = self.read("jobs", value["id"])
        return None if job["stage"] in TERMINAL else job

    def create_upload(
        self, session: str, *, total: int, limits: Limits = Limits()
    ) -> dict:
        if type(total) is not int or not 0 < total <= limits.compressed:
            raise MigrationError("migration_compressed_limit", status=413)
        self._initialize()
        require_space(self.root, total)
        return self._create(
            "uploads",
            session,
            {
                "stage": "uploading",
                "total": total,
                "offset": 0,
                "limits": asdict(limits),
                "expires_at": self.clock() + 24 * 3600,
            },
        )

    def append_chunk(
        self, identity: str, session: str, *, offset: int, data: bytes, digest: str
    ) -> dict:
        if not data or len(data) > UPLOAD_CHUNK:
            raise MigrationError("migration_upload_chunk_limit", status=413)
        if not _HASH.fullmatch(digest) or hashlib.sha256(data).hexdigest() != digest:
            raise MigrationError("migration_upload_chunk_hash_mismatch")
        path = self.path("uploads", identity)
        try:
            with FileLock(str(path.parent / "upload.lock"), timeout=0):
                state = self.read("uploads", identity, session=session)
                if state["expires_at"] <= self.clock():
                    raise MigrationError("migration_upload_expired", status=410)
                if state["stage"] != "uploading":
                    raise MigrationError("migration_upload_sealed", status=409)
                if (
                    type(offset) is not int
                    or offset < 0
                    or offset + len(data) > state["total"]
                ):
                    raise MigrationError("migration_upload_range_invalid", status=409)
                artifact = contained_path(path.parent, "archive.zx.part")
                if offset < state["offset"] and offset + len(data) <= state["offset"]:
                    with artifact.open("rb") as stream:
                        stream.seek(offset)
                        if hashlib.sha256(stream.read(len(data))).hexdigest() != digest:
                            raise MigrationError(
                                "migration_upload_chunk_conflict", status=409
                            )
                    return state
                if offset != state["offset"]:
                    raise MigrationError("migration_upload_offset_conflict", status=409)
                require_space(path.parent, len(data))
                with artifact.open("r+b" if artifact.exists() else "xb") as stream:
                    stream.seek(0, os.SEEK_END)
                    if stream.tell() < offset:
                        raise MigrationError("migration_upload_truncated")
                    stream.seek(offset)
                    stream.truncate()
                    stream.write(data)
                    stream.flush()
                    os.fsync(stream.fileno())
                state.update(offset=offset + len(data), updated_at=self.clock())
                write_json_locked(path, state)
                return state
        except Timeout:
            raise MigrationError("migration_upload_busy", status=409) from None

    def seal_upload(
        self, identity: str, session: str, digest: str, *, checkpoint=lambda: None
    ) -> dict:
        if not _HASH.fullmatch(digest):
            raise MigrationError("migration_upload_hash_invalid")
        path = self.path("uploads", identity)
        try:
            with FileLock(str(path.parent / "upload.lock"), timeout=0):
                value = self.read("uploads", identity, session=session)
                if value["expires_at"] <= self.clock():
                    raise MigrationError("migration_upload_expired", status=410)
                if value["stage"] == "sealed":
                    if value["sha256"] != digest:
                        raise MigrationError(
                            "migration_upload_hash_conflict", status=409
                        )
                if value["offset"] != value["total"]:
                    raise MigrationError("migration_upload_incomplete", status=409)
                artifact = contained_path(path.parent, "archive.zx.part", regular=True)
                if (
                    artifact.stat().st_size != value["total"]
                    or file_hash(artifact, checkpoint) != digest
                ):
                    raise MigrationError("migration_upload_hash_mismatch")
                value.update(stage="sealed", sha256=digest, updated_at=self.clock())
                write_json_locked(path, value)
                return value
        except Timeout:
            raise MigrationError("migration_upload_busy", status=409) from None

    def create_preflight(
        self, session: str, *, upload_id: str, target_revision: str, options: dict
    ) -> dict:
        upload = self.read("uploads", upload_id, session=session)
        if upload["stage"] != "sealed" or upload["expires_at"] <= self.clock():
            raise MigrationError("migration_upload_not_ready", status=409)
        if not _HASH.fullmatch(target_revision):
            raise MigrationError("migration_target_revision_invalid")
        _public_payload(options)
        return self._create(
            "preflights",
            session,
            {
                "upload_id": upload_id,
                "sha256": upload["sha256"],
                "target_revision": target_revision,
                "options": options,
                "expires_at": self.clock() + PREFLIGHT_TTL,
            },
        )

    def confirm_preflight(
        self, identity: str, session: str, *, target_revision: str, reserve: bool = True
    ) -> dict:
        path = self.path("preflights", identity)
        try:
            with FileLock(str(path.parent / "confirm.lock"), timeout=0):
                value = self.read("preflights", identity, session=session)
                if value.get("job_id"):
                    return self.read("jobs", value["job_id"])
                if value["expires_at"] <= self.clock():
                    raise MigrationError("migration_preflight_expired", status=410)
                if value["target_revision"] != target_revision:
                    raise MigrationError("migration_target_changed", status=409)
                upload = self.read("uploads", value["upload_id"], session=session)
                self.seal_upload(upload["id"], session, value["sha256"])
                job_options = {
                    **value["options"],
                    "preflight_id": identity,
                    "target_revision": target_revision,
                }
                if self.path("jobs", identity).exists():
                    job = self.read("jobs", identity, session=session)
                    if job["options"] != job_options or job["action"] != "restore":
                        raise MigrationError(
                            "migration_operation_id_conflict", status=409
                        )
                else:
                    job = self.create_job(
                        session, "restore", job_options, identity=identity
                    )
                if reserve:
                    self.reserve(job["id"])
                value["job_id"] = job["id"]
                write_json_locked(path, value)
                return job
        except Timeout:
            raise MigrationError("migration_confirmation_busy", status=409) from None

    @staticmethod
    def public(value: dict) -> dict:
        result = {
            key: value[key]
            for key in (
                "id",
                "action",
                "stage",
                "created_at",
                "updated_at",
                "expires_at",
                "progress",
                "first_error",
                "rollback_error",
                "cancel_requested",
                "revision",
                "total",
                "offset",
                "sha256",
                "target_revision",
                "shutdown_diagnostic",
                "database_diagnostic",
            )
            if key in value
        }
        runtime = value.get("progress", {}).get("runtime")
        if runtime and value["stage"] == "completed":
            result["progress"] = {
                **value["progress"],
                "runtime": {
                    **runtime,
                    "stage": "completed",
                    "step": "迁移完成",
                    "percent": 100,
                    "overall_percent": 100,
                    "eta_seconds": 0,
                    "total_elapsed_seconds": max(
                        0, value["updated_at"] - value["created_at"]
                    ),
                    "updated_at": value["updated_at"],
                },
            }
        return result

    def record_shutdown(self, identity: str, shutdown: dict) -> None:
        """Keep the original worker's evidence even after recovery replaces it."""
        allowed = (
            "result",
            "identity",
            "forced",
            "process_tree_released",
            "process_identity_verified",
            "stop_stages",
            "unresolved_roles",
            "budget_remaining_ms",
            "budget_exhausted",
            "recovery_required",
            "unresolved_resources",
            "failed_components",
            "correlation_verified",
        )
        evidence = {key: shutdown[key] for key in allowed if key in shutdown}
        _public_payload(evidence)
        record = {"job_id": identity, "recorded_at": self.clock(), **evidence}
        write_json_locked(
            self.path("jobs", identity).parent / "shutdown-attempt.json", record
        )

        def update(job):
            if not isinstance(job, dict) or job.get("id") != identity:
                raise MigrationError("migration_record_not_found", status=404)
            job["shutdown_diagnostic"] = record
            job["revision"] += 1
            job["updated_at"] = self.clock()
            return job

        mutate_json_locked(self.path("jobs", identity), None, update)

    def record_database_diagnostic(self, identity: str, diagnostic: dict) -> None:
        """Persist bounded, credential-redacted database tool diagnostics."""
        allowed = {
            "tool",
            "engine",
            "phase",
            "operation",
            "return_code",
            "duration_seconds",
            "stdout_bytes",
            "stderr_bytes",
            "stderr",
            "tool_version",
            "environment",
            "recorded_at",
            "started_at",
            "error_code",
            "cleanup_error",
            "truncated",
            "timed_out",
            "process_returned",
            "diagnostic_id",
        }
        if set(diagnostic) - allowed or not isinstance(diagnostic.get("stderr"), str):
            raise MigrationError("migration_task_metadata_invalid")
        if len(diagnostic["stderr"]) > 1024 * 1024:
            raise MigrationError("migration_database_output_limit")
        for key, value in diagnostic.items():
            if key == "stderr":
                continue
            if not isinstance(value, str | int | float) and value is not None:
                raise MigrationError("migration_task_metadata_invalid")
            if isinstance(value, str) and len(value) > 2048:
                raise MigrationError("migration_task_metadata_invalid")

        def update(job):
            if not isinstance(job, dict) or job.get("id") != identity:
                raise MigrationError("migration_record_not_found", status=404)
            previous = job.get("database_diagnostic", {})
            if not previous.get("error_code"):
                job["database_diagnostic"] = dict(diagnostic)
            elif diagnostic.get("cleanup_error"):
                job["database_diagnostic"] = {
                    **previous,
                    "cleanup_error": diagnostic["cleanup_error"],
                }
            job["updated_at"] = self.clock()
            return job

        mutate_json_locked(self.path("jobs", identity), None, update)
