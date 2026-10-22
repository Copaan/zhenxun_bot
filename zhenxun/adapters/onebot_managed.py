"""Managed OneBot ingress; protocol logic follows the locked upstream adapter."""

import asyncio
from collections.abc import Awaitable, Callable
import contextlib
import hashlib
import hmac
import inspect
import json
import time
from typing import Any, ClassVar, cast

from nonebot.adapters import Bot as BaseBot
from nonebot.adapters.onebot.store import ResultStore
from nonebot.adapters.onebot.v11 import Adapter, Bot
from nonebot.adapters.onebot.v11.adapter import RECONNECT_INTERVAL
from nonebot.adapters.onebot.v11.event import LifecycleMetaEvent
from nonebot.adapters.onebot.v11.exception import NetworkError
from nonebot.adapters.onebot.v11.utils import handle_api_result, log
from nonebot.drivers import URL, Request, Response, WebSocket
from nonebot.exception import WebSocketClosed
from nonebot.utils import DataclassEncoder, escape_tag
from starlette.datastructures import Headers

from zhenxun.services.startup import startup_coordinator
from zhenxun.utils.exception import DependencyUnavailable
from zhenxun.utils.network import public_client_address

_ADMISSION_TIMEOUT = 30.0
_INGRESS_PREFIX = "/onebot/"
_REJECTED_LIMIT = 1024
_QUEUED_APIS = frozenset(
    {
        "send_msg",
        "send_group_msg",
        "send_private_msg",
        "send_forward_msg",
        "send_group_forward_msg",
        "send_private_forward_msg",
        "send_like",
    }
)
_SEND_INTERVAL = 0.05
_QUEUE_LIMIT = 500
_DRAIN_TIMEOUT = 3.0


class SendRejected(DependencyUnavailable):
    """发送请求未交给协议端，调用方不能视为发送成功"""


class _BotQueue:
    def __init__(self) -> None:
        self.lock = asyncio.Lock()
        self.size = 0
        self.last = 0.0


class SendQueue:
    """OneBot 发送队列：每个 Bot 按到达顺序逐条发送，相邻两条保持最小间隔"""

    _bots: ClassVar[dict[str, _BotQueue]] = {}
    _pending: ClassVar[int] = 0
    _stopping: ClassVar[bool] = False
    _closed: ClassVar[bool] = False
    _drained: ClassVar[asyncio.Event | None] = None

    @classmethod
    async def call(cls, self_id: str, send: Callable[[], Awaitable[Any]]) -> Any:
        """排队发送

        参数:
            self_id: Bot id
            send: 实际发送的调用

        返回:
            Any: 发送结果

        异常:
            SendRejected: 队列已满或正在关闭，消息未发送
        """
        if cls._stopping:
            raise SendRejected("send_queue_stopping")
        queue = cls._bots.setdefault(self_id, _BotQueue())
        if queue.size >= _QUEUE_LIMIT:
            log("WARNING", f"Bot {escape_tag(self_id)} 发送队列已满，拒绝发送")
            raise SendRejected("send_queue_full")
        queue.size += 1
        cls._pending += 1
        try:
            async with queue.lock:
                if cls._closed:
                    raise SendRejected("send_queue_stopping")
                wait = queue.last + _SEND_INTERVAL - time.monotonic()
                if wait > 0:
                    await asyncio.sleep(wait)
                try:
                    return await send()
                finally:
                    queue.last = time.monotonic()
        finally:
            queue.size -= 1
            cls._pending -= 1
            if cls._pending == 0 and cls._drained is not None:
                cls._drained.set()

    @classmethod
    def forget(cls, self_id: str) -> None:
        """Bot 断开后移除其队列，已排队的消息仍按原顺序发送"""
        cls._bots.pop(self_id, None)

    @classmethod
    def start(cls) -> None:
        """恢复接收发送请求"""
        cls._stopping = cls._closed = False

    @classmethod
    async def stop(cls) -> None:
        """停止接收发送请求，等待已排队的消息发出，超时后丢弃剩余消息"""
        cls._stopping = True
        if cls._pending:
            cls._drained = asyncio.Event()
            try:
                await asyncio.wait_for(cls._drained.wait(), _DRAIN_TIMEOUT)
            except asyncio.TimeoutError:
                cls._closed = True
                log("WARNING", f"发送队列关闭超时，丢弃未发送的消息 {cls._pending} 条")
            finally:
                cls._drained = None
        cls._bots.clear()


class _PublicIngressGuard:
    """拒绝缺少认证的公网 OneBot 接入，本机与内网来源不受影响"""

    def __init__(self, app, adapter: "ManagedOneBotAdapter"):
        self.app = app
        self.adapter = adapter

    async def __call__(self, scope, receive, send):
        if scope["type"] in {"http", "websocket"} and str(
            scope.get("path", "")
        ).startswith(_INGRESS_PREFIX):
            if self.adapter._public_rejected(scope):
                if scope["type"] == "websocket":
                    await receive()
                    await send({"type": "websocket.close", "code": 1008})
                else:
                    await send(
                        {
                            "type": "http.response.start",
                            "status": 403,
                            "headers": [(b"content-type", b"text/plain")],
                        }
                    )
                    await send(
                        {
                            "type": "http.response.body",
                            "body": b"Authentication required for public source",
                        }
                    )
                return
        await self.app(scope, receive, send)


class ManagedResultStore(ResultStore):
    def add_result(self, result: dict[str, Any]):
        echo = result.get("echo")
        if isinstance(echo, str) and echo.isdecimal():
            future = self._futures.get(int(echo))
            if future is not None and not future.done():
                future.set_result(result)


class ManagedOneBotAdapter(Adapter):
    _result_store = ManagedResultStore()

    async def _call_api(self, bot: Bot, api: str, **data: Any) -> Any:
        if api in _QUEUED_APIS:
            return await SendQueue.call(
                bot.self_id, lambda: self._call_api_unqueued(bot, api, **data)
            )
        return await self._call_api_unqueued(bot, api, **data)

    def bot_disconnect(self, bot: BaseBot) -> None:
        SendQueue.forget(bot.self_id)
        super().bot_disconnect(bot)

    async def _call_api_unqueued(self, bot: Bot, api: str, **data: Any) -> Any:
        websocket = self.connections.get(bot.self_id)
        if websocket is None:
            return await super()._call_api(bot, api, **data)
        timeout = data.get("_timeout", self.config.api_timeout)
        store = self._result_store
        sequence = store.get_seq()
        future = asyncio.get_running_loop().create_future()
        # A peer can reply while send() is still waiting for transport drain.
        store._futures[sequence] = future

        async def exchange():
            await websocket.send(
                json.dumps(
                    {"action": api, "params": data, "echo": str(sequence)},
                    cls=DataclassEncoder,
                )
            )
            return handle_api_result(await future)

        try:
            return await asyncio.wait_for(exchange(), timeout)
        except asyncio.TimeoutError:
            raise NetworkError(f"WebSocket call api {api} timeout") from None
        finally:
            if store._futures.get(sequence) is future:
                store._futures.pop(sequence)
            if not future.done():
                future.cancel()

    async def _admit(self, self_id: str, timeout: float | None = _ADMISSION_TIMEOUT):
        """等待运行时插件加载完成后再接入 Bot

        参数:
            self_id: Bot id
            timeout: 最长等待秒数，None 为一直等待

        返回:
            bool: 是否可以接入
        """
        if await startup_coordinator.wait_bot_admission(0):
            return True
        log("INFO", f"<y>Bot {escape_tag(self_id)}</y> 等待运行时插件加载完成后接入")
        if await startup_coordinator.wait_bot_admission(timeout):
            return True
        log("WARNING", f"Bot {escape_tag(self_id)} 接入超时，等待协议端重连")
        return False

    def _public_rejected(self, scope: dict) -> bool:
        """公网来源的接入请求是否因缺少认证被拒绝

        反向 WebSocket 需设置 ONEBOT_ACCESS_TOKEN，由适配器校验；HTTP 上报需设置
        ONEBOT_SECRET 由适配器校验签名，或携带 ONEBOT_ACCESS_TOKEN。本机与内网
        来源不受限制。每个来源地址只记录一次拒绝原因。

        参数:
            scope: ASGI 连接信息

        返回:
            bool: 是否拒绝
        """
        headers = Headers(scope=scope)
        client = scope.get("client")
        address = public_client_address(client[0] if client else None, headers)
        if address is None:
            return False
        token = self.onebot_config.onebot_access_token
        if scope["type"] == "websocket":
            if token:
                return False
            reason = "未设置 ONEBOT_ACCESS_TOKEN"
        else:
            if self.onebot_config.onebot_secret or (
                token
                and hmac.compare_digest(
                    headers.get("authorization", "").encode(),
                    f"Bearer {token}".encode(),
                )
            ):
                return False
            reason = (
                "未携带有效的 ONEBOT_ACCESS_TOKEN"
                if token
                else "未设置 ONEBOT_SECRET 或 ONEBOT_ACCESS_TOKEN"
            )
        if address not in self._rejected:
            if len(self._rejected) >= _REJECTED_LIMIT:
                self._rejected.clear()
            self._rejected.add(address)
            log(
                "WARNING",
                f"已拒绝公网来源 {escape_tag(address)} 的 OneBot 接入：{reason}，"
                "公网接入必须认证，本机与局域网连接不受影响",
            )
        return True

    def __init__(self, *args, **kwargs):
        for name, expected in _UPSTREAM_HASHES.items():
            source = inspect.getsource(getattr(Adapter, name)).rstrip()
            if hashlib.sha256(source.encode()).hexdigest() != expected:
                raise RuntimeError(f"onebot_ingress_version_unsupported:{name}")
        for name, expected in _UPSTREAM_STORE_HASHES.items():
            source = inspect.getsource(getattr(ResultStore, name)).rstrip()
            if hashlib.sha256(source.encode()).hexdigest() != expected:
                raise RuntimeError(f"onebot_receipt_version_unsupported:{name}")
        super().__init__(*args, **kwargs)
        self._rejected: set[str] = set()
        app = getattr(self.driver, "server_app", None)
        if app is not None and not any(
            item.cls is _PublicIngressGuard for item in app.user_middleware
        ):
            if app.middleware_stack is not None:
                raise RuntimeError("onebot_ingress_guard_unavailable")
            app.add_middleware(_PublicIngressGuard, adapter=self)
        if not self.onebot_config.onebot_access_token:
            log(
                "INFO",
                "未设置 ONEBOT_ACCESS_TOKEN：OneBot 只接受本机与局域网的无认证接入，"
                "公网来源将被拒绝；使用内网穿透（frp 等）时也请设置该项",
            )

    def _offer_event(self, bot: Bot, event):
        task = asyncio.create_task(bot.handle_event(event))
        task.add_done_callback(self.tasks.discard)
        self.tasks.add(task)

    async def _handle_http(self, request: Request) -> Response:
        self_id = request.headers.get("x-self-id")

        # check self_id
        if not self_id:
            log("WARNING", "Missing X-Self-ID Header")
            return Response(400, content="Missing X-Self-ID Header")

        # check signature
        response = self._check_signature(request)
        if response is not None:
            return response

        if data := request.content:
            json_data = json.loads(data)
            if event := self.json_to_event(json_data):
                if not (bot := self.bots.get(self_id, None)):
                    if not await self._admit(self_id):
                        return Response(503, content="Runtime starting")
                    bot = self.bots.get(self_id) or Bot(self, self_id)
                if self_id not in self.bots:
                    self.bot_connect(bot)
                    log("INFO", f"<y>Bot {escape_tag(self_id)}</y> connected")
                self._offer_event(cast(Bot, bot), event)
        else:
            return Response(400, content="Invalid request body")
        return Response(204)

    async def _handle_ws(self, websocket: WebSocket) -> None:
        self_id = websocket.request.headers.get("x-self-id")

        # check self_id
        if not self_id:
            log("WARNING", "Missing X-Self-ID Header")
            await websocket.close(1008, "Missing X-Self-ID Header")
            return

        # check access_token
        response = self._check_access_token(websocket.request)
        if response is not None:
            content = cast(str, response.content)
            await websocket.close(1008, content)
            return

        if not await self._admit(self_id):
            await websocket.close(1013, "Runtime starting")
            return
        if self_id in self.bots:
            log("WARNING", f"There's already a bot {self_id}, ignored")
            await websocket.close(1008, "Duplicate X-Self-ID")
            return

        await websocket.accept()
        bot = Bot(self, self_id)
        self.bot_connect(bot)
        self.connections[self_id] = websocket

        log("INFO", f"<y>Bot {escape_tag(self_id)}</y> connected")

        try:
            while True:
                data = await websocket.receive()
                json_data = json.loads(data)
                if event := self.json_to_event(json_data):
                    self._offer_event(bot, event)
        except WebSocketClosed:
            log("WARNING", f"WebSocket for Bot {escape_tag(self_id)} closed by peer")
        except Exception as e:
            log(
                "ERROR",
                "<r><bg #f8bbd0>Error while process data from websocket "
                f"for bot {escape_tag(self_id)}.</bg #f8bbd0></r>",
                e,
            )
        finally:
            with contextlib.suppress(Exception):
                await websocket.close()
            self.connections.pop(self_id, None)
            self.bot_disconnect(bot)

    async def _forward_ws(self, url: URL) -> None:
        headers = {}
        if self.onebot_config.onebot_access_token:
            headers["Authorization"] = (
                f"Bearer {self.onebot_config.onebot_access_token}"
            )
        request = Request("GET", url, headers=headers, timeout=30.0)

        bot: Bot | None = None

        while True:
            try:
                async with self.websocket(request) as ws:
                    log(
                        "DEBUG",
                        f"WebSocket Connection to {escape_tag(str(url))} established",
                    )
                    try:
                        while True:
                            data = await ws.receive()
                            json_data = json.loads(data)
                            event = self.json_to_event(json_data)
                            if not event:
                                continue
                            if not bot:
                                if (
                                    not isinstance(event, LifecycleMetaEvent)
                                    or event.sub_type != "connect"
                                ):
                                    continue
                                self_id = event.self_id
                                await self._admit(str(self_id), None)
                                bot = Bot(self, str(self_id))
                                self.bot_connect(bot)
                                self.connections[str(self_id)] = ws
                                log(
                                    "INFO",
                                    f"<y>Bot {escape_tag(str(self_id))}</y> connected",
                                )
                            self._offer_event(bot, event)
                    except WebSocketClosed as e:
                        log(
                            "ERROR",
                            "<r><bg #f8bbd0>WebSocket Closed</bg #f8bbd0></r>",
                            e,
                        )
                    except Exception as e:
                        log(
                            "ERROR",
                            (
                                "<r><bg #f8bbd0>"
                                "Error while process data from websocket"
                                f"{escape_tag(str(url))}. Trying to reconnect..."
                                "</bg #f8bbd0></r>"
                            ),
                            e,
                        )
                    finally:
                        if bot:
                            self.connections.pop(bot.self_id, None)
                            self.bot_disconnect(bot)
                            bot = None

            except Exception as e:
                log(
                    "ERROR",
                    "<r><bg #f8bbd0>Error while setup websocket to "
                    f"{escape_tag(str(url))}. Trying to reconnect...</bg #f8bbd0></r>",
                    e,
                )

            await asyncio.sleep(RECONNECT_INTERVAL)


_UPSTREAM_HASHES = {
    "_handle_http": "95b83bb41f844321aba2dc6473e1467c136818481e613a7cc6e49977d9c474be",
    "_handle_ws": "d0ad76582f435865b6e5ac12fa44c6054ac44134f49bc56236f029ad1329eb4e",
    "_forward_ws": "4bf97ddb2907ed45ec44fb0470f511da58b9f494473d6dc7b2eebfdfe32dea53",
}

_UPSTREAM_STORE_HASHES = {
    "get_seq": "aa721bdfaa0f07e0f8fe23993156bd71482d1bfe9a4b4287d69477028281cd4b",
    "fetch": "08746c68ec50d99018331e6f639592f991d36cbd156122dbb4384c6596388370",
    "add_result": "a9264ae42ee4fa291555e50faaf44b441bf444b39afc09ab2e5003c74a1ec35f",
}
