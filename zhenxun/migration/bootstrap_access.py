from __future__ import annotations

import re

_RESTORE_WRITES = {
    "POST": re.compile(
        r"/zhenxun/api/migration/(?:bootstrap/session|uploads|"
        r"uploads/[a-f0-9]{32}/seal|inspect|packages/register|preflight|"
        r"preflights/[a-f0-9]{32}/confirm|tasks/[a-f0-9]{32}/(?:cancel|credentials))"
    ),
    "PUT": re.compile(r"/zhenxun/api/migration/uploads/[a-f0-9]{32}/chunks"),
}


def bootstrap_status(snapshot: dict) -> dict:
    """Describe setup migration readiness independently of the business runtime."""
    available = snapshot.get("operating_mode") == "setup_only"
    ready = available and snapshot.get("state") == "management_ready"
    return {
        "available": available,
        "ready": ready,
        "reason": None
        if ready
        else "migration_management_not_ready"
        if available
        else "migration_first_deployment_unavailable",
        "state": snapshot.get("state"),
        "operating_mode": snapshot.get("operating_mode"),
    }


def allows_bootstrap_mutation(method: str, path: str, snapshot: dict) -> bool:
    """Admit setup restore routes to their own authentication and transaction checks."""
    pattern = _RESTORE_WRITES.get(method)
    return bool(
        pattern and pattern.fullmatch(path) and bootstrap_status(snapshot)["ready"]
    )
