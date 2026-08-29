from __future__ import annotations

import asyncio
from collections import Counter
import hashlib
import time
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
    release_webhook_receipt,
    reserve_webhook_receipt,
)
from .dispatcher import QQWebhookDispatcher, dispatch_routing_key

ACK_BODY = '{"op":12}'


class ZhenxunQQAdapter(QQAdapter):
    """QQ Webhook adapter with durable replay protection and bounded replies."""

    def __init__(self, driver, **kwargs: Any):
        self._webhook_bots: dict[str, ZhenxunQQBot] = {}
        self._startup_lock = asyncio.Lock()
        self._prepared_bot_connect_lock = asyncio.Lock()
        self._webhook_route_registered = False
        self._health_route_registered = False
        self._dispatcher = QQWebhookDispatcher()
        self._metrics: Counter[str] = Counter()
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
            if not self._health_route_registered:
                self.setup_http_server(
                    HTTPServerSetup(
                        URL("/qq/healthz"),
                        "GET",
                        f"{self.get_name()} Health",
                        self._handle_health,
                    )
                )
                self._health_route_registered = True
            await self._dispatcher.start()

        await _runtime.connect_prepared_adapter(self)

    @override
    async def shutdown(self) -> None:
        await self._dispatcher.stop()
        await super().shutdown()
        _runtime.unregister_adapter_runtime(self)
        self._webhook_bots.clear()
        self._webhook_route_registered = False
        self._health_route_registered = False

    def is_ready(self) -> bool:
        return bool(
            _runtime.database_ready()
            and self._dispatcher.accepting
            and self._webhook_bots
            and all(bot_id in self.bots for bot_id in self._webhook_bots)
        )

    async def _handle_health(self, _request: Request) -> Response:
        status = 200 if self.is_ready() else 503
        value = "ready" if status == 200 else "degraded"
        return Response(
            status,
            headers={"Content-Type": "application/json"},
            content=f'{{"status":"{value}"}}',
        )

    def _record(self, name: str, value: int = 1) -> None:
        metrics = getattr(self, "_metrics", None)
        if metrics is None:
            metrics = Counter()
            self._metrics = metrics
        metrics[name] += value

    async def diagnostics(self) -> dict[str, object]:
        return {
            "ready": self.is_ready(),
            "configured_bots": len(self._webhook_bots),
            "connected_bots": len(self.bots),
            "metrics": dict(getattr(self, "_metrics", {})),
            "dispatcher": await self._dispatcher.snapshot(),
        }

    async def _rollback_receipt(self, app_id: str, event_digest: str) -> None:
        try:
            await release_webhook_receipt(app_id, event_digest)
        except Exception as exc:
            self._record("receipt_rollback_failed")
            logger.error(
                "QQ Webhook 回执预留撤销失败，平台重试仍返回服务不可用",
                "QQOfficialWebhook",
                target=app_id,
                e=exc,
            )

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
            self._record("signature_failed")
            return Response(403, content="Invalid signature")
        if signature_error is not None:
            self._record("signature_failed")
            return signature_error
        if not isinstance(payload, Dispatch):
            return self._ack()

        if not self.is_ready():
            return Response(503, content="Webhook worker not ready")

        raw_content = (
            request.content.encode()
            if isinstance(request.content, str)
            else request.content
        )
        event_identity = str(payload.id or hashlib.sha256(raw_content).hexdigest())
        event_digest = digest_identifier(event_identity)
        routing_key = dispatch_routing_key(
            app_id, str(payload.type or ""), payload.data
        )

        if await receipt_seen(app_id, event_digest):
            self._record("duplicate_receipt")
            return self._ack()
        if not await self._dispatcher.reserve():
            self._record("queue_overloaded")
            return Response(503, content="Webhook dispatch queue full")
        async with self._dispatcher.ordering(routing_key):
            try:
                if await receipt_seen(app_id, event_digest):
                    self._record("duplicate_receipt")
                    await self._dispatcher.release()
                    return self._ack()
                started_at = time.perf_counter()
                inserted = await asyncio.wait_for(
                    reserve_webhook_receipt(
                        app_id=app_id,
                        event_digest=event_digest,
                        event_type=str(payload.type or ""),
                    ),
                    timeout=DB_TIMEOUT_SECONDS,
                )
                self._record(
                    "receipt_db_microseconds",
                    max(0, int((time.perf_counter() - started_at) * 1_000_000)),
                )
            except Exception as exc:
                self._record("receipt_db_failed")
                await self._dispatcher.release()
                await self._rollback_receipt(app_id, event_digest)
                logger.error(
                    "QQ Webhook 数据库预留失败，平台可安全重试",
                    "QQOfficialWebhook",
                    target=app_id,
                    e=exc,
                )
                return Response(503, content="Webhook reservation unavailable")

            if not inserted:
                self._record("duplicate_receipt")
                await self._dispatcher.release()
                return self._ack()

            try:
                event = self.payload_to_event(payload)
                context = await asyncio.wait_for(
                    prepare_event_context(app_id, event), timeout=DB_TIMEOUT_SECONDS
                )
            except Exception as exc:
                self._record("identity_failed")
                await self._dispatcher.release()
                await self._rollback_receipt(app_id, event_digest)
                logger.error(
                    "QQ Webhook 身份准备失败，已撤销预留供平台重试",
                    "QQOfficialWebhook",
                    target=app_id,
                    e=exc,
                )
                return Response(503, content="Webhook identity unavailable")
            if context is None:
                self._record("unsupported_event")
                await self._dispatcher.release()
                logger.debug(
                    "QQ Webhook 事件不在首期支持范围，已确认但不分发: "
                    f"{payload.type or ''}",
                    "QQOfficialWebhook",
                )
                return self._ack()

            if not await self._dispatcher.enqueue(routing_key, bot.handle_event, event):
                self._record("enqueue_failed")
                await self._rollback_receipt(app_id, event_digest)
                return Response(503, content="Webhook dispatcher unavailable")
            self._record("accepted")
            return self._ack()


__all__ = ["ZhenxunQQAdapter"]
