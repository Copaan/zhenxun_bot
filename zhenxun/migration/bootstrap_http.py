from __future__ import annotations

import json

from fastapi import APIRouter, HTTPException, Request

from zhenxun.utils.network import is_private_client

from .bootstrap_access import bootstrap_status
from .errors import MigrationError
from .maintenance_app import _origin
from .service import discover_packages


def create_bootstrap_router(project, authority, *, result, startup_snapshot=None):
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
                "discovered": bool(discover_packages(project))
                if value["available"]
                else False,
            }
        )

    @router.post("/session")
    async def session(request: Request):
        _origin(request)
        if not request.client or not is_private_client(request.client.host):
            raise HTTPException(403, "migration_private_access_required")
        data = bytearray()
        async for chunk in request.stream():
            data.extend(chunk)
            if len(data) > 1024:
                raise HTTPException(413, "migration_private_input_limit")
        try:
            payload = json.loads(data)
            if not isinstance(payload, dict) or set(payload) != {"code"}:
                raise ValueError
            value = readiness()
            if not value["ready"]:
                raise MigrationError(value["reason"], status=409)
            return result(authority.exchange(payload["code"]))
        except MigrationError as error:
            raise HTTPException(error.status, error.code) from None
        except (ValueError, TypeError):
            raise HTTPException(
                400, "migration_console_authorization_invalid"
            ) from None

    return router
