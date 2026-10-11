from __future__ import annotations

from fastapi import APIRouter, HTTPException, Request

from .bootstrap_access import bootstrap_status
from .service import discover_packages


def create_bootstrap_router(
    project, authority, *, result, startup_snapshot=None, setup_authorization=None
):
    if startup_snapshot is None:
        from zhenxun.services.startup import startup_coordinator

        startup_snapshot = startup_coordinator.snapshot
    router = APIRouter(prefix="/migration/bootstrap")

    def readiness():
        value = bootstrap_status(startup_snapshot())
        if not authority.first_deployment():
            value.update(
                available=False,
                ready=False,
                reason="migration_first_deployment_unavailable",
            )
        return value

    @router.get("/status")
    def status():
        value = readiness()
        return result(
            {
                **value,
                "session_auth_version": 2,
                "discovered": bool(discover_packages(project))
                if value["available"]
                else False,
            }
        )

    @router.post("/session")
    async def session(request: Request):
        raise HTTPException(410, "migration_session_protocol_obsolete")

    return router
