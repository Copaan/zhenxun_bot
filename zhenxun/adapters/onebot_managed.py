"""Managed OneBot ingress; protocol logic follows the locked upstream adapter."""

import asyncio
import contextlib
import hashlib
import inspect
import json
import time
from typing import Any, cast

from nonebot.adapters.onebot.store import ResultStore
from nonebot.adapters.onebot.v11 import Adapter, Bot
from nonebot.adapters.onebot.v11.adapter import RECONNECT_INTERVAL
from nonebot.adapters.onebot.v11.event import LifecycleMetaEvent
from nonebot.adapters.onebot.v11.exception import NetworkError
from nonebot.adapters.onebot.v11.utils import handle_api_result, log
from nonebot.drivers import URL, Request, Response, WebSocket
from nonebot.exception import WebSocketClosed
from nonebot.utils import DataclassEncoder, escape_tag

from zhenxun.services.message_inbox import message_inbox


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
        from zhenxun.services import send_queue

        if send_queue._PATCHED:
            return await send_queue._queued_call_api(self, bot, api, **data)
        return await self._call_api_unqueued(bot, api, **data)

    async def _call_api_unqueued(self, bot: Bot, api: str, **data: Any) -> Any:
        websocket = self.connections.get(bot.self_id)
        if websocket is None:
            from zhenxun.services.send_queue import _ORIG_CALL_API

            return await _ORIG_CALL_API(self, bot, api, **data)
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

    def bot_connect(self, bot):
        super().bot_connect(bot)
        message_inbox.register_bot(bot, self.json_to_event, bot.handle_event)

    def __init__(self, *args, **kwargs):
        self._control_tasks = set()
        self._last_rejection_warning = float("-inf")
        for name, expected in _UPSTREAM_HASHES.items():
            source = inspect.getsource(getattr(Adapter, name)).rstrip()
            if hashlib.sha256(source.encode()).hexdigest() != expected:
                raise RuntimeError(f"onebot_ingress_version_unsupported:{name}")
        for name, expected in _UPSTREAM_STORE_HASHES.items():
            source = inspect.getsource(getattr(ResultStore, name)).rstrip()
            if hashlib.sha256(source.encode()).hexdigest() != expected:
                raise RuntimeError(f"onebot_receipt_version_unsupported:{name}")
        super().__init__(*args, **kwargs)

    def _offer_event(self, bot, event):
        if event.get_type() == "message":
            future = message_inbox.observe(bot, event)
            future.add_done_callback(self._observe_receipt)
            return future
        future = asyncio.get_running_loop().create_future()
        if len(self._control_tasks) >= 64:
            future.set_result({"accepted": False, "reason": "control_capacity"})
            return future
        task = asyncio.create_task(bot.handle_event(event))
        self._control_tasks.add(task)
        self.tasks.add(task)
        task.add_done_callback(self._control_tasks.discard)
        task.add_done_callback(self.tasks.discard)
        future.set_result({"accepted": True})
        return future

    def _observe_receipt(self, future):
        if future.cancelled():
            return
        try:
            receipt = future.result()
        except Exception:
            receipt = {"accepted": False, "reason": "persistence_unconfirmed"}
        if (
            not receipt["accepted"]
            and time.monotonic() - self._last_rejection_warning >= 10
        ):
            self._last_rejection_warning = time.monotonic()
            log("WARNING", "Managed message ingress rejected: " + receipt["reason"])

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
                    bot = Bot(self, self_id)
                    self.bot_connect(bot)
                    log("INFO", f"<y>Bot {escape_tag(self_id)}</y> connected")
                bot = cast(Bot, bot)
                receipt = await self._offer_event(bot, event)
                if not receipt["accepted"]:
                    return Response(503, content="Message persistence unavailable")
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
        elif self_id in self.bots:
            log("WARNING", f"There's already a bot {self_id}, ignored")
            await websocket.close(1008, "Duplicate X-Self-ID")
            return

        # check access_token
        response = self._check_access_token(websocket.request)
        if response is not None:
            content = cast(str, response.content)
            await websocket.close(1008, content)
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
