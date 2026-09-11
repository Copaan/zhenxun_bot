from typing import Any

from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

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
    from zhenxun.services.bot_group_policy import bot_group_policy_service
    from zhenxun.services.message_inbox import message_inbox

    return Result.ok(
        {
            **plugin_runtime_manager.status(),
            "message_inbox": message_inbox.snapshot(),
            "group_membership_writes": dict(bot_group_policy_service.write_counts),
        }
    )


class InboxResolution(BaseModel):
    expected_revision: int = Field(ge=1)
    action: str = "dismiss"


@router.get("/messages/{identity}/operations", dependencies=[authentication()])
async def get_message_operations(identity: str, after: str = "", limit: int = 50):
    from zhenxun.models.asset_operation import AssetOperation
    from zhenxun.services.db_context import with_db_timeout

    if len(identity) != 64 or not 1 <= limit <= 100:
        raise HTTPException(422, "invalid_message_pagination")
    rows = await with_db_timeout(
        AssetOperation.filter(event_id=identity, id__gt=after)
        .exclude(kind="delivery")
        .order_by("id")
        .limit(limit)
        .values("id", "kind", "state", "create_time"),
        source="message_audit",
    )
    return Result.ok(
        {
            "items": rows,
            "next": rows[-1]["id"] if rows else None,
            "third_party_effects_verified": False,
        }
    )


@router.get("/messages", dependencies=[authentication()])
async def get_message_queue(after: int = 0, limit: int = 50, state: str | None = None):
    from zhenxun.services.message_inbox import message_inbox

    if message_inbox.worker is None or message_inbox.worker.closed:
        raise HTTPException(503, "message_inbox_unavailable")
    if after < 0 or not 1 <= limit <= 100:
        raise HTTPException(422, "invalid_message_pagination")
    rows = await message_inbox.io(message_inbox.store.page, after, limit, state)
    return Result.ok(
        {
            "items": rows,
            "next": rows[-1]["sequence"] if rows else None,
            "status": message_inbox.snapshot(),
        }
    )


@router.post("/messages/{identity}/resolve", dependencies=[authentication()])
async def resolve_message(identity: str, request: InboxResolution):
    from zhenxun.services.message_inbox import message_inbox
    from zhenxun.services.message_store import InboxConflict

    if message_inbox.worker is None or message_inbox.worker.closed:
        raise HTTPException(503, "message_inbox_unavailable")
    try:
        result = await message_inbox.io(
            message_inbox.store.resolve,
            identity,
            request.expected_revision,
            request.action,
        )
    except InboxConflict as error:
        raise HTTPException(409, str(error)) from None
    except KeyError:
        raise HTTPException(404, "message_not_found") from None
    except ValueError as error:
        raise HTTPException(422, str(error)) from None
    return Result.ok(result)
