from typing import Any

from fastapi import APIRouter
from fastapi.responses import JSONResponse

from zhenxun.services.runtime_reload import plugin_runtime_manager
from zhenxun.services.startup import startup_coordinator

from ....base_model import Result
from ....restart_service import transaction_verification_status
from ....utils import authentication

router = APIRouter(prefix="/runtime")


@router.get(
    "/startup/status",
    response_model=Result[dict[str, Any]],
    response_class=JSONResponse,
    description="获取worker分级启动状态",
)
async def get_startup_status() -> Result[dict[str, Any]]:
    return Result.ok(
        {**startup_coordinator.snapshot(), **transaction_verification_status()}
    )


@router.get(
    "/status",
    dependencies=[authentication()],
    response_model=Result[dict[str, Any]],
    response_class=JSONResponse,
    description="获取自动生命周期与热加载状态",
)
async def get_runtime_status() -> Result[dict[str, Any]]:
    return Result.ok(plugin_runtime_manager.status())
