from fastapi import APIRouter
from fastapi.responses import JSONResponse, StreamingResponse
from nonebot import require

from zhenxun.services.log import logger

from ....base_model import BaseResultModel, QueryModel, Result
from ....utils import DB_BUSY_MESSAGE, authentication
from .data_source import ApiDataSource
from .model import (
    ChatCallMonthCount,
    RuntimeOverview,
)
from .overview import build_runtime_overview

require("plugin_store")

router = APIRouter(prefix="/dashboard")


@router.get(
    "/overview",
    dependencies=[authentication()],
    response_model=Result[RuntimeOverview],
    response_class=JSONResponse,
    description="获取WebUI运行概览",
)
async def _(force: bool = False) -> Result[RuntimeOverview]:
    return Result.ok(await build_runtime_overview(force))


@router.get(
    "/get_chat_and_call_month",
    dependencies=[authentication()],
    response_model=Result[ChatCallMonthCount],
    response_class=JSONResponse,
    description="获取聊天/调用记录的一个月数量",  # type: ignore
)
async def _(bot_id: str | None = None) -> Result[ChatCallMonthCount]:
    try:
        return Result.ok(await ApiDataSource.get_chat_and_call_month(bot_id))
    except TimeoutError:
        return Result.fail(DB_BUSY_MESSAGE)
    except Exception as e:
        logger.error(f"{router.prefix}/get_chat_and_call_month 调用错误", "WebUi", e=e)
        return Result.fail("趋势统计读取失败，请稍后重试。")


@router.post(
    "/get_connect_log",
    dependencies=[authentication()],
    response_model=Result[BaseResultModel],
    response_class=JSONResponse,
    description="获取Bot连接记录",  # type: ignore
)
async def _(query: QueryModel) -> Result[BaseResultModel]:
    try:
        return Result.ok(await ApiDataSource.get_connect_log(query))
    except Exception as e:
        logger.error(f"{router.prefix}/get_connect_log 调用错误", "WebUi", e=e)
        return Result.fail("连接记录读取失败，请稍后重试。")


@router.get(
    "/log_file",
    dependencies=[authentication()],
    description="下载当天的运行日志",
)
async def _():
    path = ApiDataSource.today_log_file()
    if path is None:
        return JSONResponse({"detail": "今天还没有写入日志文件"}, 404)
    return StreamingResponse(
        ApiDataSource.read_log_file(path, path.stat().st_size),
        media_type="text/plain; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="zhenxun-{path.name}"'},
    )
