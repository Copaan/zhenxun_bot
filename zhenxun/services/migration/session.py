from __future__ import annotations

from dataclasses import replace
import os
import re

from jose import JWTError

from zhenxun.utils.atomic_json import read_json_locked
from zhenxun.utils.webui_session import (
    AuthenticatedSession,
    decode_token,
    matches_context,
)

from .errors import MigrationError
from .paths import contained_path


def bind_session(snapshot, session, task_id):
    """Carry accepted authorization without rechecking browser lifetime in a task."""
    if not isinstance(session, AuthenticatedSession):
        return snapshot
    return replace(
        snapshot, secret=session.secret, session_context=session.context(task_id)
    )


def require_task_access(task_id, method, path):
    """Restrict an inherited session to its original operation and related resources."""
    prefix = "/zhenxun/api/migration"
    reads = {f"{prefix}/{name}" for name in ("tasks", "capabilities", "discover")}
    reads.update(
        {
            "/zhenxun/api/system/startup/status",
            "/zhenxun/api/configure/status",
            f"{prefix}/tasks/{task_id}",
            f"{prefix}/tasks/{task_id}/recovery",
            f"{prefix}/tasks/{task_id}/download",
            f"{prefix}/preflights/{task_id}",
        }
    )
    writes = {f"{prefix}/tasks/{task_id}/{name}" for name in ("cancel", "credentials")}
    writes.add(f"{prefix}/preflights/{task_id}/confirm")
    writes.add("/zhenxun/api/auth/activity")
    if (method in {"GET", "HEAD"} and path in reads) or (
        method == "POST" and path in writes
    ):
        return
    raise MigrationError("migration_session_mismatch", status=403)


def task_listing(store, task_id, *, offset=0, limit=20):
    job = store.read("jobs", task_id)
    listing = store.list_jobs(offset=0, limit=1)
    return {
        "items": [store.public(job)] if offset == 0 and limit > 0 else [],
        "total": 1,
        "candidate_resources": [
            item
            for item in listing.get("candidate_resources", [])
            if item.get("preflight_id") == task_id
        ],
    }


def continuation_session(project, token):
    """Load only a launcher-delegated worker's original migration session."""
    from .commit import options_digest
    from .lease import validate_delegation
    from .maintenance_app import ManagementSnapshot
    from .tasks import TaskStore

    validation = os.getenv("ZHENXUN_MIGRATION_VALIDATION_ID")
    resumed = os.getenv("ZHENXUN_MIGRATION_RESUME_ID")
    task_id = validation or resumed or os.getenv("ZHENXUN_MIGRATION_SESSION_ID")
    if not task_id or not re.fullmatch(r"[a-f0-9]{32}", task_id):
        raise MigrationError("migration_login_required", status=401)
    try:
        validate_delegation(
            project,
            int(os.getenv("ZHENXUN_LAUNCHER_PID", "0")),
            identity=os.getenv("ZHENXUN_INSTANCE_LEASE_ID"),
        )
        store = TaskStore(project)
        directory = store.path("jobs", task_id).parent
        receipt = read_json_locked(
            directory
            / (
                "validation-authorization.json"
                if validation
                else "resume-authorization.json"
                if resumed
                else "session-authorization.json"
            ),
            {},
        )
        if (
            receipt.get("job_id") != task_id
            or not receipt.get("startup_id")
            or receipt["startup_id"] != os.getenv("ZHENXUN_WORKER_STARTUP_ID")
            or receipt.get("launcher_boot_id") != os.getenv("ZHENXUN_LAUNCHER_BOOT_ID")
        ):
            raise ValueError
        path = contained_path(directory, "management/context.json", regular=True)
        if path.stat().st_size > 64 * 1024:
            raise ValueError
        context = read_json_locked(path, {})
        if (
            not isinstance(context, dict)
            or context.get("schema") != 1
            or context.get("job_id") != task_id
            or context.get("options_sha256")
            != options_digest(store.read("jobs", task_id)["options"])
        ):
            raise ValueError
        snapshot = ManagementSnapshot.parse(context.get("management"))
        claims = decode_token(token, snapshot.secret)
        if (
            not (
                matches_context(claims, snapshot.session_context)
                or (
                    claims.get("maintenance_task_id") == task_id
                    and claims.get("auth_source") == "password"
                    and claims.get("sub") == snapshot.username
                )
            )
            or not snapshot.session_context
            or snapshot.session_context["task_id"] != task_id
        ):
            raise ValueError
        session = AuthenticatedSession(claims["sid"], claims, snapshot.secret)
        session.task_id = task_id
        return session
    except (OSError, ValueError, TypeError, JWTError, MigrationError):
        raise MigrationError("migration_login_required", status=401) from None
