from pathlib import Path

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.security import OAuth2PasswordBearer

from zhenxun.services.migration.bootstrap import bootstrap_authority
from zhenxun.services.migration.bootstrap_http import create_bootstrap_router
from zhenxun.services.migration.errors import MigrationError
from zhenxun.services.migration.http import create_router, session_request
from zhenxun.services.migration.maintenance_app import _origin
from zhenxun.utils.network import is_private_client

from ...base_model import Result

authority = bootstrap_authority(Path.cwd())
optional_bearer = OAuth2PasswordBearer(tokenUrl="/zhenxun/api/login", auto_error=False)


async def migration_session(
    request: Request, token: str | None = Depends(optional_bearer)
) -> str:
    _origin(session_request(request))
    if request.headers.get("x-migration-session") or request.headers.get(
        "x-setup-token"
    ):
        raise HTTPException(410, "migration_session_protocol_obsolete")
    if not token:
        raise HTTPException(401, "migration_login_required")
    from zhenxun.configs.config import Config
    from zhenxun.services.migration.session import (
        continuation_session,
        require_task_access,
    )
    from zhenxun.utils.webui_session import AuthenticatedSession

    from ...security import decode_access_token_status
    from ..configure.setup_access import client_ip, setup_access

    claims, _ = decode_access_token_status(token)
    if claims is not None:
        return AuthenticatedSession(
            claims["sid"], claims, Config.get_config("web-ui", "secret")
        )
    if authority.first_deployment():
        if not request.client or not is_private_client(request.client.host):
            raise HTTPException(403, "migration_private_access_required")
        try:
            return setup_access.identity(
                await setup_access.authorize(token, client_ip(request))
            )
        except HTTPException:
            pass
    try:
        session = continuation_session(Path.cwd(), token)
        require_task_access(session.task_id, request.method, request.url.path)
        request.state.migration_session = session
        return session
    except MigrationError as error:
        raise HTTPException(error.status, error.code) from None


router = APIRouter()


router.include_router(create_bootstrap_router(Path.cwd(), authority, result=Result.ok))

router.include_router(
    create_router(
        Path.cwd(),
        authentication=Depends(migration_session),
        session_dependency=migration_session,
        result=Result.ok,
        first_authorization=authority.validate,
    )
)
