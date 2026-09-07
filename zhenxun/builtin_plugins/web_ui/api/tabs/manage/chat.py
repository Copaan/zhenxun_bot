import asyncio
from datetime import datetime

from fastapi import APIRouter
import nonebot
from nonebot import on_message
from nonebot.adapters.onebot.v11 import MessageEvent
from nonebot_plugin_alconna import At, Hyper, Image, Text, UniMsg
from nonebot_plugin_uninfo import Uninfo
from starlette.websockets import WebSocket, WebSocketDisconnect, WebSocketState

from zhenxun.models.group_member_info import GroupInfoUser
from zhenxun.utils.depends import UserName

from ....config import AVA_URL
from ....security import (
    authenticate_websocket,
    close_authenticated_websocket,
    record_websocket_disconnect,
    send_authenticated_json,
    unregister_authenticated_websocket,
)
from .model import Message, MessageItem

driver = nonebot.get_driver()

_CHAT_CONNECTIONS: set[WebSocket] = set()
_MAX_CHAT_CONNECTIONS = 16

ID2NAME = {}

ID_LIST = []

ws_router = APIRouter()


matcher = on_message(block=False, priority=1, rule=lambda: bool(_CHAT_CONNECTIONS))


@driver.on_shutdown
async def _():
    from zhenxun.services.lifecycle import lifecycle_kernel

    timeout = lifecycle_kernel.shutdown_remaining(1.0)
    await asyncio.gather(
        *(
            asyncio.wait_for(
                close_authenticated_websocket(ws, code=1001, reason="server shutdown"),
                timeout=timeout,
            )
            for ws in tuple(_CHAT_CONNECTIONS)
        ),
        return_exceptions=True,
    )


@ws_router.websocket("/chat")
async def _(websocket: WebSocket):
    if not await authenticate_websocket(websocket):
        return
    if len(_CHAT_CONNECTIONS) >= _MAX_CHAT_CONNECTIONS:
        await close_authenticated_websocket(
            websocket, code=1013, reason="connection limit reached"
        )
        unregister_authenticated_websocket(websocket)
        return
    _CHAT_CONNECTIONS.add(websocket)
    try:
        while websocket.client_state == WebSocketState.CONNECTED:
            await websocket.receive()
    except (WebSocketDisconnect, OSError) as error:
        if not record_websocket_disconnect(error):
            raise
    finally:
        unregister_authenticated_websocket(websocket)
        _CHAT_CONNECTIONS.discard(websocket)


async def message_handle(
    message: UniMsg,
    group_id: str | None,
):
    time = str(datetime.now().replace(microsecond=0))
    messages = []
    for m in message:
        if isinstance(m, Text | str):
            messages.append(MessageItem(type="text", msg=str(m), time=time))
        elif isinstance(m, Image):
            if m.url:
                messages.append(MessageItem(type="img", msg=m.url, time=time))
        elif isinstance(m, At):
            if group_id:
                if m.target == "0":
                    uname = "全体成员"
                else:
                    uname = m.target
                    if group_id not in ID2NAME:
                        ID2NAME[group_id] = {}
                    if m.target in ID2NAME[group_id]:
                        uname = ID2NAME[group_id][m.target]
                    elif group_user := await GroupInfoUser.get_or_none(
                        user_id=m.target, group_id=group_id
                    ):
                        uname = group_user.user_name
                        if m.target not in ID2NAME[group_id]:
                            ID2NAME[group_id][m.target] = uname
                messages.append(MessageItem(type="at", msg=f"@{uname}", time=time))
        elif isinstance(m, Hyper):
            messages.append(MessageItem(type="text", msg="[分享消息]", time=time))
    return messages


@matcher.handle()
async def _(
    message: UniMsg, event: MessageEvent, session: Uninfo, uname: str = UserName()
):
    global ID2NAME, ID_LIST
    if _CHAT_CONNECTIONS:
        msg_id = event.message_id
        if msg_id in ID_LIST:
            return
        ID_LIST.append(msg_id)
        if len(ID_LIST) > 50:
            ID_LIST = ID_LIST[40:]
        gid = session.group.id if session.group else None
        messages = await message_handle(message, gid)
        data = Message(
            object_id=gid or session.user.id,
            user_id=session.user.id,
            group_id=gid,
            message=messages,
            name=uname,
            ava_url=AVA_URL.format(session.user.id),
        )

        async def deliver(websocket):
            try:
                if await asyncio.wait_for(
                    send_authenticated_json(websocket, data.to_dict()), timeout=2
                ):
                    return
            except (TimeoutError, WebSocketDisconnect, OSError):
                pass
            unregister_authenticated_websocket(websocket)
            _CHAT_CONNECTIONS.discard(websocket)
            await close_authenticated_websocket(
                websocket, code=1013, reason="slow consumer"
            )

        await asyncio.gather(*(deliver(ws) for ws in tuple(_CHAT_CONNECTIONS)))
