from typing import Any

from fastapi import APIRouter
from fastapi.responses import JSONResponse

from zhenxun.services.plugin_host import plugin_host
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
    from zhenxun.services.bot_group_policy import bot_group_policy_service

    return Result.ok(
        {
            **plugin_host.status(),
            "group_membership_writes": dict(bot_group_policy_service.write_counts),
        }
    )
