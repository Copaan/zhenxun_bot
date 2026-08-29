from __future__ import annotations

import asyncio
import hashlib
from typing import Any, cast
from typing_extensions import override

from nonebot.adapters.qq.adapter import Adapter as QQAdapter
from nonebot.adapters.qq.models import Dispatch, WebhookVerify
from nonebot.drivers import URL, ASGIMixin, HTTPServerSetup, Request, Response

from zhenxun.services.db_context import DB_TIMEOUT_SECONDS
from zhenxun.services.log import logger

from . import models as _models  # noqa: F401
from . import runtime as _runtime
from .bot import ZhenxunQQBot
from .context import (
    digest_identifier,
    prepare_event_context,
    receipt_seen,
    reserve_webhook_receipt,
)

ACK_BODY = '{"op":12}'


class ZhenxunQQAdapter(QQAdapter):
    """QQ Webhook adapter with durable replay protection and bounded replies."""

    def __init__(self, driver, **kwargs: Any):
        self._webhook_bots: dict[str, ZhenxunQQBot] = {}
        self._startup_lock = asyncio.Lock()
        self._prepared_bot_connect_lock = asyncio.Lock()
        self._webhook_route_registered = False
        super().__init__(driver, **kwargs)
        _runtime.register_adapter_runtime(self)

    @override
    async def startup(self) -> None:
        async with self._startup_lock:
            if not isinstance(self.driver, ASGIMixin):
                raise RuntimeError("QQ Webhook requires an ASGI driver")

            if not self._webhook_bots:
                prepared_bots: dict[str, ZhenxunQQBot] = {}
                for bot_info in self.qq_config.qq_bots:
                    bot = ZhenxunQQBot(self, bot_info.id, bot_info)
                    bot.self_info = await bot.me()
                    prepared_bots[bot_info.id] = bot
                self._webhook_bots = prepared_bots

            if not self._webhook_route_registered:
                self.setup_http_server(
                    HTTPServerSetup(
                        URL("/qq/webhook"),
                        "POST",
                        f"{self.get_name()} Webhook",
                        self._handle_http,
                    )
                )
                self._webhook_route_registered = True

        await _runtime.connect_prepared_adapter(self)

    @override
    async def shutdown(self) -> None:
        await super().shutdown()
        _runtime.unregister_adapter_runtime(self)
        self._webhook_bots.clear()
        self._webhook_route_registered = False

    async def connect_prepared_bots(self) -> None:
        from zhenxun.models.bot_console import BotConsole
        from zhenxun.services.cache.runtime_cache import BotMemoryCache

        async with self._prepared_bot_connect_lock:
            for bot in self._webhook_bots.values():
                if bot.self_id in self.bots:
                    continue
                storage_bot_id = f"qq_api:{bot.self_id}"
                bot_data, _ = await BotConsole.get_or_create(
                    bot_id=storage_bot_id,
                    defaults={"platform": "qq"},
                )
                await BotMemoryCache.upsert_from_model(bot_data)
                if bot.self_id not in self.bots:
                    self.bot_connect(bot)

    def _resolve_webhook_bot(self, app_id: str) -> ZhenxunQQBot | None:
        if app_id in self.bots:
            return cast(ZhenxunQQBot, self.bots[app_id])
        return self._webhook_bots.get(app_id)

    @staticmethod
    def _ack() -> Response:
        return Response(
            200,
            headers={"Content-Type": "application/json"},
            content=ACK_BODY,
        )

    @override
    async def _handle_http(self, request: Request) -> Response:
        app_id = request.headers.get("X-Bot-Appid")
        if not app_id:
            return Response(403, content="Missing X-Bot-Appid header")
        bot = self._resolve_webhook_bot(app_id)
        if bot is None:
            return Response(403, content="Bot not found")
        if request.content is None:
            return Response(400, content="Missing request content")

        try:
            payload = self.data_to_payload(bot, request.content)
        except Exception:
            return Response(400, content="Invalid request content")

        if isinstance(payload, WebhookVerify):
            response = self._webhook_verify(bot, payload)
            response.headers["Content-Type"] = "application/json"
            return response

        try:
            signature_error = self._check_signature(bot, request)
        except Exception:
            return Response(403, content="Invalid signature")
        if signature_error is not None:
            return signature_error
        if not isinstance(payload, Dispatch):
            return self._ack()

        raw_content = (
            request.content.encode()
            if isinstance(request.content, str)
            else request.content
        )
        event_identity = str(payload.id or hashlib.sha256(raw_content).hexdigest())
        event_digest = digest_identifier(event_identity)

        async def prepare_dispatch():
            event = self.payload_to_event(payload)
            context = await prepare_event_context(app_id, event)
            inserted = await reserve_webhook_receipt(
                app_id=app_id,
                event_digest=event_digest,
                event_type=str(payload.type or ""),
            )
            return event, context, inserted

        try:
            if await receipt_seen(app_id, event_digest):
                return self._ack()
            event, context, inserted = await asyncio.wait_for(
                prepare_dispatch(), timeout=DB_TIMEOUT_SECONDS
            )
        except Exception as exc:
            logger.error(
                "QQ Webhook 数据库预留失败，平台可安全重试",
                "QQOfficialWebhook",
                target=app_id,
                e=exc,
            )
            return Response(503, content="Webhook reservation unavailable")

        if not inserted:
            return self._ack()
        if context is None:
            logger.debug(
                "QQ Webhook 事件不在首期支持范围，已确认但不分发: "
                f"{payload.type or ''}",
                "QQOfficialWebhook",
            )
            return self._ack()

        if bot.self_id not in self.bots:
            self.bot_connect(bot)
        task = asyncio.create_task(bot.handle_event(event))
        task.add_done_callback(self.tasks.discard)
        self.tasks.add(task)
        return self._ack()


__all__ = ["ZhenxunQQAdapter"]
