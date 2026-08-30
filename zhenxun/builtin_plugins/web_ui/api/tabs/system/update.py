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
)

from ....base_model import Result
from ....utils import authentication

router = APIRouter(prefix="/update")


class UpdateCheckRequest(BaseModel):
    channel: Literal["main", "release"] = "main"


class UpdateJobRequest(BaseModel):
    component: Literal["bot", "resource", "webui"]
    channel: Literal["main", "release"] = "main"
    method: Literal["download", "git"] = "download"
    source: Literal["github", "aliyun"] = "github"
    force: bool = False


def _service_error(error: UpdateServiceError) -> HTTPException:
    code = str(error)
    if code == "job_not_found":
        return HTTPException(status_code=404, detail="更新任务不存在。")
    if code in {"update_in_progress", "pending_update_exists"}:
        return HTTPException(status_code=409, detail="已有更新任务正在进行。")
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


__all__ = ["router"]
