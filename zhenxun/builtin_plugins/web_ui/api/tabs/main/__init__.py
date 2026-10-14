import asyncio
import contextlib

from fastapi import APIRouter
from fastapi.responses import JSONResponse
import nonebot
from starlette.websockets import WebSocket, WebSocketDisconnect, WebSocketState
from websockets.exceptions import ConnectionClosedError, ConnectionClosedOK

from zhenxun.models.bot_console import BotConsole
from zhenxun.services.log import logger

from ....base_model import Result
from ....config import QueryDateType
from ....security import (
    authenticate_websocket,
    close_authenticated_websocket,
    is_websocket_disconnect_error,
    send_authenticated_text,
    spawn_websocket_task,
    unregister_authenticated_websocket,
)
from ....utils import DB_BUSY_MESSAGE, authentication, get_system_status
from .data_source import ApiDataSource
from .model import (
    ActiveGroup,
    BaseInfo,
    BotBlockModule,
    BotStatusParam,
    ChatCallCount,
    HotPlugin,
)

driver = nonebot.get_driver()

ws_router = APIRouter()
router = APIRouter(prefix="/main")
_SYSTEM_STATUS_CONNECTIONS: set[WebSocket] = set()
_MAX_SYSTEM_STATUS_CONNECTIONS = 16
_SYSTEM_STATUS_STOPPING = False


async def _close_system_status_websocket(websocket: WebSocket) -> None:
    with contextlib.suppress(Exception):
        await asyncio.wait_for(
            close_authenticated_websocket(
                websocket, code=1001, reason="server shutdown"
            ),
            timeout=2,
        )


@driver.on_shutdown
async def _close_system_status_websockets() -> None:
    global _SYSTEM_STATUS_STOPPING
    _SYSTEM_STATUS_STOPPING = True
    websockets = list(_SYSTEM_STATUS_CONNECTIONS)
    if not websockets:
        return
    await asyncio.gather(
        *(_close_system_status_websocket(websocket) for websocket in websockets),
        return_exceptions=True,
    )
    _SYSTEM_STATUS_CONNECTIONS.clear()


@router.get(
    "/get_base_info",
    dependencies=[authentication()],
    response_model=Result[list[BaseInfo]],
    response_class=JSONResponse,
    description="基础信息",
)
async def _(bot_id: str | None = None) -> Result[list[BaseInfo]]:
    """获取Bot基础信息

    参数:
        bot_id (Optional[str], optional): bot_id. Defaults to None.

    返回:
        Result: 获取指定bot信息与bot列表
    """
    try:
        result = await ApiDataSource.get_base_info(bot_id)
        return Result.ok(result, "拿到信息啦!")
    except TimeoutError:
        return Result.fail(DB_BUSY_MESSAGE)
    except Exception as e:
        logger.error(f"{router.prefix}/get_base_info 调用错误", "WebUi", e=e)
        return Result.fail(f"发生了一点错误捏 {type(e)}: {e}")


@router.get(
    "/get_chat_and_call_count",
    dependencies=[authentication()],
    response_model=Result[ChatCallCount],
    response_class=JSONResponse,
    description="获取聊天与调用次数",
)
async def _(bot_id: str | None = None) -> Result[ChatCallCount]:
    try:
        return Result.ok(await ApiDataSource.get_chat_and_call_count(bot_id))
    except TimeoutError:
        return Result.fail(DB_BUSY_MESSAGE)
    except Exception as e:
        logger.error(f"{router.prefix}/get_chat_and_call_count 调用错误", "WebUi", e=e)
        return Result.fail("消息统计读取失败，请稍后重试。")


@router.get(
    "/get_active_group",
    dependencies=[authentication()],
    response_model=Result[list[ActiveGroup]],
    response_class=JSONResponse,
    description="获取活跃群聊",
)
async def _(
    date_type: QueryDateType | None = None, bot_id: str | None = None
) -> Result[list[ActiveGroup]]:
    try:
        return Result.ok(
            await ApiDataSource.get_active_group(date_type, bot_id), "拿到信息啦!"
        )
    except TimeoutError:
        return Result.fail(DB_BUSY_MESSAGE)
    except Exception as e:
        logger.error(f"{router.prefix}/get_active_group 调用错误", "WebUi", e=e)
        return Result.fail(f"发生了一点错误捏 {type(e)}: {e}")


@router.get(
    "/get_hot_plugin",
    dependencies=[authentication()],
    response_model=Result[list[HotPlugin]],
    response_class=JSONResponse,
    description="获取热门插件",
)
async def _(
    date_type: QueryDateType | None = None, bot_id: str | None = None
) -> Result[list[HotPlugin]]:
    try:
        return Result.ok(
            await ApiDataSource.get_hot_plugin(date_type, bot_id), "拿到信息啦!"
        )
    except TimeoutError:
        return Result.fail(DB_BUSY_MESSAGE)
    except Exception as e:
        logger.error(f"{router.prefix}/get_hot_plugin 调用错误", "WebUi", e=e)
        return Result.fail(f"发生了一点错误捏 {type(e)}: {e}")


@router.post(
    "/change_bot_status",
    dependencies=[authentication()],
    response_model=Result,
    response_class=JSONResponse,
    description="修改bot全局开关",
)
async def _(param: BotStatusParam):
    try:
        await BotConsole.set_bot_status(param.status, param.bot_id)
        return Result.ok(info="修改bot全局开关成功！")
    except (ValueError, KeyError):
        return Result.fail("Bot未初始化...")


@router.get(
    "/get_bot_block_module",
    dependencies=[authentication()],
    response_model=Result[BotBlockModule],
    response_class=JSONResponse,
    description="获取bot层面的禁用模块",
)
async def _(bot_id: str) -> Result[BotBlockModule]:
    try:
        return Result.ok(
            await ApiDataSource.get_bot_block_module(bot_id), "拿到信息啦!"
        )
    except Exception as e:
        logger.error(f"{router.prefix}/get_bot_block_module 调用错误", "WebUi", e=e)
        return Result.fail(f"发生了一点错误捏 {type(e)}: {e}")


@ws_router.websocket("/system_status")
async def system_logs_realtime(websocket: WebSocket, sleep: int = 5):
    if not await authenticate_websocket(websocket):
        return
    if len(_SYSTEM_STATUS_CONNECTIONS) >= _MAX_SYSTEM_STATUS_CONNECTIONS:
        await close_authenticated_websocket(
            websocket, code=1013, reason="connection limit reached"
        )
        unregister_authenticated_websocket(websocket)
        return
    _SYSTEM_STATUS_CONNECTIONS.add(websocket)
    logger.debug("ws system_status is connect")

    disconnect_event = asyncio.Event()

    async def _watch_disconnect() -> None:
        try:
            while websocket.client_state == WebSocketState.CONNECTED:
                await websocket.receive()
        except (WebSocketDisconnect, ConnectionClosedError, ConnectionClosedOK):
            pass
        except OSError as error:
            if not is_websocket_disconnect_error(error):
                logger.warning(
                    f"ws system_status receive failed: {type(error).__name__}"
                )
        except Exception as e:
            logger.warning(f"ws system_status receive failed: {type(e).__name__}")
        finally:
            disconnect_event.set()

    receive_task = spawn_websocket_task(
        _watch_disconnect(),
        name="webui-system-status-receive",
    )
    try:
        while (
            websocket.client_state == WebSocketState.CONNECTED
            and not _SYSTEM_STATUS_STOPPING
        ):
            system_status = await get_system_status()
            sent = await asyncio.wait_for(
                send_authenticated_text(websocket, system_status.model_dump_json()),
                timeout=5,
            )
            if not sent:
                break
            try:
                await asyncio.wait_for(disconnect_event.wait(), timeout=max(sleep, 1))
            except asyncio.TimeoutError:
                pass
    except (
        asyncio.CancelledError,
        WebSocketDisconnect,
        ConnectionClosedError,
        ConnectionClosedOK,
    ):
        pass
    finally:
        _SYSTEM_STATUS_CONNECTIONS.discard(websocket)
        unregister_authenticated_websocket(websocket)
        receive_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await receive_task
        await _close_system_status_websocket(websocket)
