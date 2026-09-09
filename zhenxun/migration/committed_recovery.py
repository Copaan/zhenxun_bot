from __future__ import annotations

from zhenxun.utils.atomic_json import read_json_locked, write_json_locked

from .commit import options_digest, read_commit
from .errors import MigrationError


def authorize_revalidation(service, identity, startup_id):
    service.lease.require_held()
    store = service.store
    decision = read_commit(store, identity)
    if decision is None or decision["mode"] != "online":
        raise MigrationError("migration_commit_authorization_invalid")
    value = {
        "job_id": identity,
        "startup_id": startup_id,
        "launcher_boot_id": service.supervisor.boot_id,
        "lease_id": service.lease.identity,
        "decided_at": decision["decided_at"],
        "options_sha256": decision["options_sha256"],
    }
    write_json_locked(
        store.path("jobs", identity).parent / "commit-revalidation.json", value
    )
    return value


def revalidation_authorized(store, identity, authorization):
    decision = read_commit(store, identity)
    grant = read_json_locked(
        store.path("jobs", identity).parent / "commit-revalidation.json", {}
    )
    if (
        decision is None
        or decision["mode"] != "online"
        or grant.get("job_id") != identity
        or grant.get("decided_at") != decision["decided_at"]
        or grant.get("options_sha256")
        != options_digest(store.read("jobs", identity)["options"])
        or any(
            not authorization.get(k) or grant.get(k) != authorization[k]
            for k in ("startup_id", "launcher_boot_id", "lease_id")
        )
    ):
        raise MigrationError("migration_commit_revalidation_unauthorized")
    return decision


def recovered_decision(service, identity, authorization, validation):
    """Bind a fresh validated worker to the original irreversible decision."""
    service.lease.require_held()
    decision = revalidation_authorized(service.store, identity, authorization)
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
        or validation.get("startup_id") != authorization["startup_id"]
        or validation.get("launcher_boot_id") != service.supervisor.boot_id
        or not validation.get("worker_boot_id")
        or validation.get("state") != "validated"
        or validation.get("business_opened") is not False
        or set(validation.get("checks", {})) != required
        or any(validation["checks"][k] is not True for k in required)
        or validation.get("generation") != decision["validation"].get("generation")
    ):
        raise MigrationError("migration_validation_receipt_incomplete")
    projected = {**decision, "validation": validation}
    write_json_locked(
        service.store.path("jobs", identity).parent / "recovered-validation.json",
        projected,
    )
    return projected
