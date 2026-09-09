from __future__ import annotations

import json

from fastapi import APIRouter, HTTPException, Request

from zhenxun.utils.network import is_private_client

from .errors import MigrationError
from .maintenance_app import _origin
from .service import discover_packages


def create_bootstrap_router(project, authority, *, result):
    router = APIRouter(prefix="/migration/bootstrap")

    @router.get("/status")
    def status():
        available = authority.first_deployment()
        return result(
            {
                "available": available,
                "discovered": bool(discover_packages(project)) if available else False,
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
            return result(authority.exchange(payload["code"]))
        except MigrationError as error:
            raise HTTPException(error.status, error.code) from None
        except (ValueError, TypeError):
            raise HTTPException(
                400, "migration_console_authorization_invalid"
            ) from None

    return router
