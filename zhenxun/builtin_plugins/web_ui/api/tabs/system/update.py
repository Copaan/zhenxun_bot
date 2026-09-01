from __future__ import annotations

from typing import Literal

from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from zhenxun.update_service import (
    UpdateServiceError,
    check_updates,
    create_update_job,
    read_job,
    request_update_apply,
)

from ....base_model import Result
from ....restart_service import restart_status_data
from ....utils import authentication

router = APIRouter(prefix="/update")


class UpdateCheckRequest(BaseModel):
    channel: Literal["main", "release"] = "main"


class UpdateJobRequest(BaseModel):
    component: Literal["bot", "resource", "webui"]
    channel: Literal["main", "release"] = "main"
    method: Literal["download", "git"] = "git"
    source: Literal["github", "aliyun"] = "aliyun"
    force: bool = False


def _service_error(error: UpdateServiceError) -> HTTPException:
    code = str(error)
    if code == "job_not_found":
        return HTTPException(status_code=404, detail="更新任务不存在。")
    if code in {"update_in_progress", "pending_update_exists"}:
        return HTTPException(status_code=409, detail="已有更新任务正在进行。")
    if code == "release_blocked":
        return HTTPException(
            status_code=409,
            detail={
                "code": "release_blocked",
                "message": "该版本存在已知兼容性问题，已禁止更新。",
            },
        )
    if code == "update_not_pending":
        return HTTPException(
            status_code=409,
            detail="该更新任务不处于等待应用状态，请重新检查任务状态。",
        )
    return HTTPException(status_code=422, detail=f"更新请求无效（{code}）。")


@router.get(
    "/status",
    dependencies=[authentication()],
    response_model=Result,
    response_class=JSONResponse,
)
async def update_status(channel: Literal["main", "release"] = "main") -> Result:
    return Result.ok(await check_updates(channel=channel))


@router.post(
    "/check",
    dependencies=[authentication()],
    response_model=Result,
    response_class=JSONResponse,
)
async def update_check(payload: UpdateCheckRequest) -> Result:
    return Result.ok(await check_updates(channel=payload.channel, refresh=True))


@router.post(
    "/jobs",
    dependencies=[authentication()],
    response_model=Result,
    response_class=JSONResponse,
)
async def start_update(payload: UpdateJobRequest) -> Result:
    try:
        job = await create_update_job(
            component=payload.component,
            channel=payload.channel,
            method=payload.method,
            source=payload.source,
            force=payload.force,
        )
    except UpdateServiceError as error:
        raise _service_error(error) from error
    return Result.ok(job, info="更新任务已创建。")


@router.get(
    "/jobs/{job_id}",
    dependencies=[authentication()],
    response_model=Result,
    response_class=JSONResponse,
)
async def update_job(job_id: str) -> Result:
    try:
        return Result.ok(read_job(job_id))
    except UpdateServiceError as error:
        raise _service_error(error) from error


@router.post(
    "/jobs/{job_id}/apply",
    dependencies=[authentication()],
    response_model=Result,
    response_class=JSONResponse,
)
async def apply_update_job(job_id: str) -> Result:
    try:
        ok, message, job = await request_update_apply(job_id)
    except UpdateServiceError as error:
        raise _service_error(error) from error
    restart = restart_status_data()
    data = {
        **job,
        **restart,
        "apply_mode": "restart_requested" if ok else "restart_pending",
        "restart_required": True,
        "restart_available": ok,
        "reason_codes": [f"update:{job.get('component', 'unknown')}:{job_id}"],
    }
    return Result.ok(data, info=message) if ok else Result.fail(message, code=409)


__all__ = ["router"]
