from fastapi import APIRouter
from fastapi.responses import JSONResponse

from ....base_model import Result
from ....restart_service import request_webui_restart, restart_status_data
from ....utils import authentication

router = APIRouter(prefix="/restart")


@router.get(
    "/status",
    dependencies=[authentication()],
    response_model=Result,
    response_class=JSONResponse,
)
async def restart_status() -> Result:
    return Result.ok(restart_status_data())


@router.post(
    "",
    dependencies=[authentication()],
    response_model=Result,
    response_class=JSONResponse,
)
async def restart_worker() -> Result:
    ok, message, data = await request_webui_restart("webui.manual")
    return Result.ok(data, info=message) if ok else Result.fail(message, code=409)


@router.post(
    "/apply-pending",
    dependencies=[authentication()],
    response_model=Result,
    response_class=JSONResponse,
)
async def apply_pending_restart() -> Result:
    data = restart_status_data()
    if not data["pending_restart"]:
        return Result.fail("当前没有等待重启应用的修改。", code=409)
    ok, message, data = await request_webui_restart("webui.pending-apply")
    return Result.ok(data, info=message) if ok else Result.fail(message, code=409)


__all__ = ["router"]
