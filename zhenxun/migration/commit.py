from __future__ import annotations

import hashlib
import json

from zhenxun.utils.atomic_json import (
    mutate_json_locked,
    read_json_locked,
    write_json_locked,
)

from .errors import MigrationError
from .tasks import TaskStore, _public_payload


def options_digest(options: dict) -> str:
    return hashlib.sha256(json.dumps(options, sort_keys=True).encode()).hexdigest()


def read_commit(store: TaskStore, identity: str) -> dict | None:
    directory = store.path("jobs", identity).parent
    receipt = read_json_locked(directory / "restore-commit.json", None)
    if receipt is None:
        return None
    job = store.read("jobs", identity)
    validation = receipt.get("validation", {}) if isinstance(receipt, dict) else {}
    if (
        not isinstance(receipt, dict)
        or receipt.get("schema") != 1
        or receipt.get("job_id") != identity
        or receipt.get("decision") != "commit"
        or receipt.get("options_sha256") != options_digest(job["options"])
        or receipt.get("mode") not in {"online", "offline"}
        or not isinstance(validation, dict)
        or not validation.get("startup_id")
        or not validation.get("worker_boot_id")
        or type(receipt.get("decided_at")) not in {int, float}
    ):
        raise MigrationError("migration_commit_receipt_invalid")
    _public_payload(receipt)
    return receipt


def decide_restore(
    store: TaskStore,
    identity: str,
    *,
    lease,
    validation: dict,
    mode: str,
    release: dict | None = None,
) -> dict:
    """Serialize cancellation with a durable commit decision under the job lock."""
    lease.require_held()
    if lease.project.absolute() != store.project or mode not in {"online", "offline"}:
        raise MigrationError("migration_commit_authorization_invalid")
    if not isinstance(validation, dict):
        raise MigrationError("migration_validation_receipt_incomplete")
    _public_payload(validation)
    required = {
        "configuration",
        "database",
        "core",
        "plugins",
        "dependencies",
        "listener",
    }
    if (
        validation.get("job_id") != identity
        or validation.get("state") != "validated"
        or validation.get("business_opened") is not False
        or not validation.get("startup_id")
        or not validation.get("worker_boot_id")
        or not validation.get("launcher_boot_id")
        or not isinstance(validation.get("checks"), dict)
        or set(validation.get("checks", {})) != required
        or any(validation["checks"][key] is not True for key in required)
    ):
        raise MigrationError("migration_validation_receipt_incomplete")
    if mode == "offline" and (
        not release
        or release.get("result") != "confirmed"
        or release.get("forced") is not False
        or release.get("process_tree_released") is not True
        or release.get("identity", {}).get("startup_id") != validation["startup_id"]
        or release.get("identity", {}).get("boot_id") != validation["worker_boot_id"]
    ):
        raise MigrationError("migration_validation_release_unconfirmed")
    if release is not None:
        _public_payload(release)
    directory = store.path("jobs", identity).parent
    receipt_path = directory / "restore-commit.json"

    def commit(job):
        lease.require_held()
        if not isinstance(job, dict) or job.get("action") != "restore":
            raise MigrationError("migration_action_invalid")
        previous = read_json_locked(receipt_path, None)
        if previous is not None:
            if (
                previous.get("job_id") != identity
                or previous.get("options_sha256") != options_digest(job["options"])
                or previous.get("mode") != mode
                or previous.get("validation") != validation
            ):
                raise MigrationError("migration_commit_receipt_conflict")
            if job["stage"] in {"completed", "partial"}:
                return dict(job)
            receipt = previous
        else:
            if job["stage"] != "committing":
                raise MigrationError("migration_commit_stage_invalid")
            if job["cancel_requested"]:
                raise MigrationError("migration_cancelled")
            receipt = {
                "schema": 1,
                "job_id": identity,
                "decision": "commit",
                "options_sha256": options_digest(job["options"]),
                "decided_at": store.clock(),
                "mode": mode,
                "validation": validation,
                "release": release,
            }
            write_json_locked(receipt_path, receipt)
        job.update(
            stage="committed",
            committed_at=receipt["decided_at"],
            updated_at=store.clock(),
            revision=job["revision"] + 1,
        )
        return dict(job)

    mutate_json_locked(store.path("jobs", identity), None, commit)
    return read_commit(store, identity)


def reconcile_decision(store: TaskStore, identity: str, *, lease) -> dict:
    lease.require_held()
    if lease.project.absolute() != store.project:
        raise MigrationError("migration_commit_authorization_invalid")
    receipt = read_commit(store, identity)
    if receipt is None:
        raise MigrationError("migration_commit_receipt_missing")

    def reconcile(job):
        if job.get("action") != "restore" or job.get("stage") not in {
            "committing",
            "committed",
            "recovery_required",
            "completed",
            "partial",
        }:
            raise MigrationError("migration_commit_stage_invalid")
        if job["stage"] not in {"completed", "partial"}:
            job.update(
                stage="committed",
                committed_at=receipt["decided_at"],
                updated_at=store.clock(),
                revision=job["revision"] + 1,
            )
        return dict(job)

    return mutate_json_locked(store.path("jobs", identity), None, reconcile)
