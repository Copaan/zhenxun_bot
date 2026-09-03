import asyncio

from fastapi import APIRouter
from loguru import logger
from nonebot.utils import escape_tag
from starlette.websockets import WebSocket, WebSocketDisconnect, WebSocketState

from ...security import (
    authenticate_websocket,
    close_authenticated_websocket,
    send_authenticated_text,
    unregister_authenticated_websocket,
)
from .log_manager import LOG_STORAGE, ensure_log_sink_started, stop_log_sink_if_idle

router = APIRouter()


@router.websocket("/logs")
async def system_logs_realtime(websocket: WebSocket):
    if not await authenticate_websocket(websocket):
        return
    await ensure_log_sink_started()

    async def log_listener(log: str):
        if not await asyncio.wait_for(
            send_authenticated_text(websocket, log), timeout=5
        ):
            raise WebSocketDisconnect()

    if not LOG_STORAGE.add_listener(log_listener):
        await send_authenticated_text(websocket, "日志连接数已达上限，请稍后再试。")
        await close_authenticated_websocket(
            websocket, code=1013, reason="connection limit"
        )
        unregister_authenticated_websocket(websocket)
        stop_log_sink_if_idle()
        return
    try:
        while websocket.client_state == WebSocketState.CONNECTED:
            recv = await websocket.receive()
            logger.trace(
                f"{system_logs_realtime.__name__!r} received "
                f"<e>{escape_tag(repr(recv))}</e>"
            )
    except WebSocketDisconnect:
        pass
    finally:
        unregister_authenticated_websocket(websocket)
        LOG_STORAGE.remove_listener(log_listener)
        stop_log_sink_if_idle()
