from pathlib import Path

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.security import OAuth2PasswordBearer

from zhenxun.migration.bootstrap import bootstrap_authority
from zhenxun.migration.bootstrap_http import create_bootstrap_router
from zhenxun.migration.errors import MigrationError
from zhenxun.migration.http import create_router, session_request
from zhenxun.migration.maintenance_app import _origin
from zhenxun.utils.network import is_private_client

from ...base_model import Result
from .plugin_manage.archive import archive_session

authority = bootstrap_authority(Path.cwd())
optional_bearer = OAuth2PasswordBearer(tokenUrl="/zhenxun/api/login", auto_error=False)


async def migration_session(
    request: Request, token: str | None = Depends(optional_bearer)
) -> str:
    if bootstrap := request.headers.get("x-migration-session"):
        _origin(session_request(request))
        if not request.client or not is_private_client(request.client.host):
            raise HTTPException(403, "migration_private_access_required")
        try:
            return authority.authenticate(bootstrap)
        except MigrationError as error:
            raise HTTPException(error.status, error.code) from None
    if not token:
        raise HTTPException(401, "migration_login_required")
    return await archive_session(session_request(request), token)


router = APIRouter()


router.include_router(create_bootstrap_router(Path.cwd(), authority, result=Result.ok))

router.include_router(
    create_router(
        Path.cwd(),
        authentication=Depends(migration_session),
        session_dependency=migration_session,
        result=Result.ok,
        first_authorization=authority.management,
    )
)
