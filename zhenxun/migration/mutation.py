from __future__ import annotations

import asyncio
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from functools import wraps
import os
from pathlib import Path

from zhenxun.utils.atomic_json import read_json_locked

from .errors import MigrationError
from .lease import validate_delegation
from .tasks import TaskStore


@dataclass
class _StartupGrant:
    task_id: str
    startup_id: str
    owner: object
    active: bool = True


_startup_grant: ContextVar[_StartupGrant | None] = ContextVar(
    "migration_startup_grant", default=None
)


def require_mutation_available(project: Path) -> None:
    job = TaskStore(project).active()
    if job is None:
        return
    grant = _startup_grant.get()
    if (
        grant is not None
        and grant.active
        and grant.task_id == job["id"]
        and grant.startup_id == os.getenv("ZHENXUN_WORKER_STARTUP_ID")
        and (job["action"], job["stage"])
        in {
            ("export", "resuming"),
            ("restore", "verifying"),
            ("restore", "committing"),
            ("restore", "committed"),
        }
    ):
        return
    raise MigrationError("migration_operation_in_progress", status=409)


@contextmanager
def resuming_startup(project: Path):
    """Authorize only descendants of a designated, live startup invocation."""
    validation_id = os.getenv("ZHENXUN_MIGRATION_VALIDATION_ID")
    identity = validation_id or os.getenv("ZHENXUN_MIGRATION_RESUME_ID")
    if not identity:
        yield
        return
    store = TaskStore(project)
    job = store.read("jobs", identity)
    grant = _startup_grant.get()
    if (
        grant is not None
        and grant.active
        and grant.task_id == identity
        and grant.owner is asyncio.current_task()
    ):
        yield
        return
    owner = validate_delegation(
        project,
        int(os.getenv("ZHENXUN_LAUNCHER_PID", "0")),
        identity=os.getenv("ZHENXUN_INSTANCE_LEASE_ID"),
    )
    receipt = read_json_locked(
        store.path("jobs", identity).parent
        / (
            "validation-authorization.json"
            if validation_id
            else "resume-authorization.json"
        ),
        None,
    )
    startup_id = os.getenv("ZHENXUN_WORKER_STARTUP_ID")
    recovery = bool(validation_id and job["stage"] == "committed")
    if recovery:
        from .commit import read_commit
        from .committed_recovery import revalidation_authorized

        decision = read_commit(store, identity)
        original_worker = (
            decision
            and decision["validation"].get("startup_id") == startup_id
            and decision["validation"].get("launcher_boot_id")
            == os.getenv("ZHENXUN_LAUNCHER_BOOT_ID")
        )
        if not original_worker:
            revalidation_authorized(store, identity, receipt or {})
    if (
        (job["action"], job["stage"])
        != (
            ("restore", "committed" if recovery else "verifying")
            if validation_id
            else ("export", "resuming")
        )
        or not isinstance(receipt, dict)
        or receipt.get("job_id") != identity
        or receipt.get("startup_id") != startup_id
        or receipt.get("launcher_boot_id") != os.getenv("ZHENXUN_LAUNCHER_BOOT_ID")
        or (validation_id and receipt.get("lease_id") != owner.get("identity"))
        or not owner.get("identity")
        or not startup_id
    ):
        raise MigrationError("migration_startup_authorization_invalid")
    grant = _StartupGrant(identity, startup_id, asyncio.current_task())
    token = _startup_grant.set(grant)
    try:
        yield
    finally:
        # Copied ContextVars cannot authorize background work after startup ends.
        grant.active = False
        _startup_grant.reset(token)


def migration_startup_scope(function):
    @wraps(function)
    async def wrapped(*args, **kwargs):
        with resuming_startup(Path.cwd()):
            return await function(*args, **kwargs)

    return wrapped
