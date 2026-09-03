from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
import json
from types import SimpleNamespace
from uuid import uuid4

import nonebot
import pytest


def _valid_bot(**overrides):
    from zhenxun.adapters.qq_official.config import (
        QQOfficialBotConfig,
        QQOfficialIntent,
    )

    data = {
        "id": "app",
        "token": "token",
        "secret": "secret",
        "use_websocket": False,
        "intent": QQOfficialIntent(c2c_group_at_messages=True),
    }
    data.update(overrides)
    return QQOfficialBotConfig(**data)


def test_config_validation_accepts_minimal_webhook(app, monkeypatch) -> None:
    del app
    from zhenxun.adapters.qq_official.config import (
        QQOfficialConfig,
        validate_qq_official_config,
    )

    config = QQOfficialConfig(qq_bots=[_valid_bot()])
    monkeypatch.setattr(nonebot, "get_plugin_config", lambda _: config)
    assert validate_qq_official_config() == config


def test_config_validation_accepts_websocket_and_mixed_modes(app, monkeypatch) -> None:
    del app
    from zhenxun.adapters.qq_official.config import (
        QQOfficialConfig,
        validate_qq_official_config,
    )

    for config in (
        QQOfficialConfig(qq_bots=[_valid_bot(use_websocket=True)]),
        QQOfficialConfig(
            qq_bots=[
                _valid_bot(id="webhook"),
                _valid_bot(id="websocket", use_websocket=True),
            ]
        ),
    ):
        monkeypatch.setattr(nonebot, "get_plugin_config", lambda _, c=config: c)
        assert validate_qq_official_config() == config


def test_config_validation_rejects_unsafe_enablement(app, monkeypatch) -> None:
    del app
    from zhenxun.adapters.qq_official.config import (
        QQOfficialConfig,
        QQOfficialConfigError,
        QQOfficialIntent,
        validate_qq_official_config,
    )

    configs = [
        QQOfficialConfig(qq_bots=[]),
        QQOfficialConfig(qq_bots=[_valid_bot(), _valid_bot()]),
        QQOfficialConfig(
            qq_bots=[_valid_bot(intent=QQOfficialIntent(c2c_group_at_messages=False))]
        ),
        QQOfficialConfig(qq_bots=[_valid_bot()], qq_verify_webhook=False),
        QQOfficialConfig(
            qq_bots=[
                _valid_bot(
                    intent=QQOfficialIntent(
                        c2c_group_at_messages=True,
                        open_forum_event=True,
                    )
                )
            ]
        ),
    ]
    for config in configs:
        monkeypatch.setattr(nonebot, "get_plugin_config", lambda _, c=config: c)
        with pytest.raises(QQOfficialConfigError):
            validate_qq_official_config()


def test_models_use_cross_database_unique_keys_and_no_raw_openid(app) -> None:
    del app
    from zhenxun.adapters.qq_official.models import (
        QQOfficialIdentity,
        QQWebhookReceipt,
    )

    identity_unique = QQOfficialIdentity._meta.unique_together
    receipt_unique = QQWebhookReceipt._meta.unique_together
    assert identity_unique == (("app_id", "scene", "group_scope", "openid_digest"),)
    assert receipt_unique == (("app_id", "event_id_digest"),)
    assert "openid" not in QQOfficialIdentity._meta.fields_map
    assert "payload" not in QQWebhookReceipt._meta.fields_map


def _reply_context(*, scene: str = "group"):
    from zhenxun.adapters.qq_official.context import OfficialQQEventContext

    now = datetime.now(timezone.utc)
    principal = uuid4()
    return OfficialQQEventContext(
        app_id="app",
        scene=scene,  # type: ignore[arg-type]
        actor_openid="actor",
        group_openid="group" if scene == "group" else "",
        union_openid="",
        source_kind="msg_id",
        source_id="message",
        principal_id=principal,
        storage_bot_id="qq_api:app",
        storage_user_id=f"principal:{principal}",
        storage_group_id="qq_api:app:group:group" if scene == "group" else None,
        received_at=now,
        reply_deadline=now + timedelta(minutes=5),
        max_passive_replies=5,
    )


@pytest.mark.asyncio
async def test_reply_sequence_is_atomic_and_bounded(app) -> None:
    del app
    from zhenxun.adapters.qq_official.cache import REPLY_STATE_CACHE
    from zhenxun.adapters.qq_official.context import (
        OfficialReplyUnavailable,
        allocate_reply_sequence,
        finish_reply,
    )

    await REPLY_STATE_CACHE.clear()
    context = _reply_context()

    allocations = await asyncio.gather(
        *(allocate_reply_sequence(context) for _ in range(5))
    )
    assert sorted(sequence for _, sequence in allocations) == [1, 2, 3, 4, 5]
    with pytest.raises(OfficialReplyUnavailable):
        await allocate_reply_sequence(context)

    await asyncio.gather(
        *(finish_reply(state, successful=True) for state, _ in allocations)
    )
    with pytest.raises(OfficialReplyUnavailable):
        await allocate_reply_sequence(context)


@pytest.mark.asyncio
async def test_failed_reply_consumes_sequence_but_not_success_limit(app) -> None:
    del app
    from zhenxun.adapters.qq_official.cache import REPLY_STATE_CACHE
    from zhenxun.adapters.qq_official.context import (
        allocate_reply_sequence,
        finish_reply,
    )

    await REPLY_STATE_CACHE.clear()
    context = _reply_context()
    state, first = await allocate_reply_sequence(context)
    await finish_reply(state, successful=False)
    state, second = await allocate_reply_sequence(context)
    await finish_reply(state, successful=True)
    assert (first, second) == (1, 2)
    assert state.successful == 1


def test_storage_bot_id_namespaces_official_adapter(app) -> None:
    del app
    from zhenxun.utils.platform import PlatformUtils

    adapter = SimpleNamespace(get_name=lambda: "QQ")
    bot = SimpleNamespace(self_id="123", adapter=adapter)
    assert PlatformUtils.get_storage_bot_id(bot) == "qq_api:123"  # type: ignore[arg-type]


@pytest.mark.asyncio
async def test_official_scope_blocks_legacy_identity_writes(app) -> None:
    del app
    from zhenxun.models.user_console import UserConsole
    from zhenxun.services.platform_identity import (
        CURRENT_PLATFORM_SCOPE,
        UnsafeLegacyIdentityWrite,
    )

    token = CURRENT_PLATFORM_SCOPE.set("qq_api")
    try:
        with pytest.raises(UnsafeLegacyIdentityWrite):
            await UserConsole.create(user_id="raw-openid", platform="qq")
        with pytest.raises(UnsafeLegacyIdentityWrite):
            await UserConsole.filter(user_id="raw-openid").update(platform="qq")
        with pytest.raises(UnsafeLegacyIdentityWrite):
            await UserConsole.filter(user_id="raw-openid").delete()
    finally:
        CURRENT_PLATFORM_SCOPE.reset(token)
    assert await UserConsole.get_or_none(user_id="raw-openid") is None


@pytest.mark.asyncio
async def test_webhook_dispatch_ack_deduplicates_and_fails_closed(
    app, monkeypatch
) -> None:
    del app
    from nonebot.adapters.qq.models import Dispatch
    from nonebot.drivers import Request

    import zhenxun.adapters.qq_official.adapter as adapter_module
    from zhenxun.adapters.qq_official.adapter import ZhenxunQQAdapter
    from zhenxun.adapters.qq_official.dispatcher import QQWebhookDispatcher

    handled = []
    handled_signal = asyncio.Event()

    class FakeBot:
        self_id = "app"

        async def handle_event(self, event) -> None:
            handled.append(event)
            handled_signal.set()

    adapter = object.__new__(ZhenxunQQAdapter)
    adapter._webhook_bots = {"app": FakeBot()}
    adapter.bots = {"app": adapter._webhook_bots["app"]}
    adapter.tasks = set()
    adapter._dispatcher = QQWebhookDispatcher(shard_count=2, capacity=4)
    await adapter._dispatcher.start()
    adapter.is_ready = lambda: True
    adapter.data_to_payload = lambda bot, content: Dispatch(
        id="event-1", t="GROUP_AT_MESSAGE_CREATE", d={}
    )
    event = SimpleNamespace()
    adapter.payload_to_event = lambda payload: event
    adapter._check_signature = lambda bot, request: None
    adapter.bot_connect = lambda bot: adapter.bots.__setitem__(bot.self_id, bot)

    monkeypatch.setattr(adapter_module, "receipt_seen", lambda *args: _false())
    monkeypatch.setattr(adapter_module, "prepare_event_context", _context_marker)
    monkeypatch.setattr(adapter_module, "reserve_webhook_receipt", _true_kwargs)
    monkeypatch.setattr(adapter_module, "release_webhook_receipt", _no_op_args)

    request = Request(
        "POST",
        "https://example.test/qq/webhook",
        headers={"X-Bot-Appid": "app"},
        content=b"{}",
    )
    response = await adapter._handle_http(request)
    await asyncio.wait_for(handled_signal.wait(), timeout=1)
    assert response.status_code == 200
    assert response.content == '{"op":12}'
    assert handled == [event]

    handled.clear()
    monkeypatch.setattr(adapter_module, "receipt_seen", lambda *args: _true())
    response = await adapter._handle_http(request)
    assert response.status_code == 200
    assert handled == []

    async def fail_context(*args):
        raise RuntimeError("database unavailable")

    monkeypatch.setattr(adapter_module, "receipt_seen", lambda *args: _false())
    monkeypatch.setattr(adapter_module, "prepare_event_context", fail_context)
    response = await adapter._handle_http(request)
    assert response.status_code == 503
    assert handled == []
    await adapter._dispatcher.stop()


@pytest.mark.asyncio
async def test_webhook_verification_response_is_json(app) -> None:
    del app
    from nonebot.adapters.qq.models import WebhookVerify
    from nonebot.drivers import Request

    from zhenxun.adapters.qq_official.adapter import ZhenxunQQAdapter

    bot = SimpleNamespace(
        self_id="app",
        bot_info=SimpleNamespace(secret="test-secret"),
    )
    adapter = object.__new__(ZhenxunQQAdapter)
    adapter._webhook_bots = {"app": bot}
    adapter.bots = {}
    adapter.data_to_payload = lambda bot, content: WebhookVerify(
        op=13,
        d={"plain_token": "challenge", "event_ts": "1234567890"},
    )
    request = Request(
        "POST",
        "https://example.test/qq/webhook",
        headers={"X-Bot-Appid": "app"},
        content=b"{}",
    )

    response = await adapter._handle_http(request)

    assert response.status_code == 200
    assert response.headers["Content-Type"] == "application/json"
    assert json.loads(response.content)["plain_token"] == "challenge"


async def _false() -> bool:
    return False


async def _true() -> bool:
    return True


async def _true_kwargs(**kwargs) -> bool:
    return True


async def _context_marker(*args):
    return object()


async def _no_op_args(*args, **kwargs) -> None:
    return None


@pytest.mark.asyncio
async def test_runtime_connects_when_database_is_ready_before_adapter(
    app, monkeypatch
) -> None:
    del app
    import zhenxun.adapters.qq_official.runtime as runtime

    connected = 0

    class FakeAdapter:
        async def connect_prepared_bots(self) -> None:
            nonlocal connected
            connected += 1

    async def no_op() -> None:
        return None

    runtime._adapters.clear()
    runtime._database_ready = False
    monkeypatch.setattr(runtime, "_drain_expired_receipts", no_op)
    try:
        await runtime.start_qq_official_runtime()
        adapter = FakeAdapter()
        runtime.register_adapter_runtime(adapter)
        await runtime.connect_prepared_adapter(adapter)
        assert connected == 1
    finally:
        await runtime.stop_qq_official_runtime()


@pytest.mark.asyncio
async def test_runtime_connects_when_adapter_is_prepared_before_database(
    app, monkeypatch
) -> None:
    del app
    import zhenxun.adapters.qq_official.runtime as runtime

    connected = 0

    class FakeAdapter:
        async def connect_prepared_bots(self) -> None:
            nonlocal connected
            connected += 1

    async def no_op() -> None:
        return None

    runtime._adapters.clear()
    runtime._database_ready = False
    monkeypatch.setattr(runtime, "_drain_expired_receipts", no_op)
    adapter = FakeAdapter()
    runtime.register_adapter_runtime(adapter)
    try:
        await runtime.connect_prepared_adapter(adapter)
        assert connected == 0
        await runtime.start_qq_official_runtime()
        assert connected == 1
    finally:
        await runtime.stop_qq_official_runtime()


@pytest.mark.asyncio
async def test_prepared_bot_connection_is_concurrent_and_idempotent(
    app, monkeypatch
) -> None:
    del app
    from zhenxun.adapters.qq_official.adapter import ZhenxunQQAdapter
    from zhenxun.models.bot_console import BotConsole
    from zhenxun.services.cache.runtime_cache import BotMemoryCache

    self_id = f"lifecycle-{uuid4().hex}"
    storage_bot_id = f"qq_api:{self_id}"
    await BotConsole.filter(bot_id=storage_bot_id).delete()
    adapter = object.__new__(ZhenxunQQAdapter)
    adapter._prepared_bot_connect_lock = asyncio.Lock()
    adapter._webhook_bots = {self_id: SimpleNamespace(self_id=self_id)}
    adapter.bots = {}
    connect_count = 0

    async def cache_no_op(bot_data) -> None:
        del bot_data

    def connect(bot) -> None:
        nonlocal connect_count
        connect_count += 1
        adapter.bots[bot.self_id] = bot

    monkeypatch.setattr(BotMemoryCache, "upsert_from_model", cache_no_op)
    adapter.bot_connect = connect
    try:
        await asyncio.gather(*(adapter.connect_prepared_bots() for _ in range(20)))
        assert connect_count == 1
        assert await BotConsole.filter(bot_id=storage_bot_id).count() == 1
    finally:
        await BotConsole.filter(bot_id=storage_bot_id).delete()


@pytest.mark.asyncio
async def test_adapter_startup_is_idempotent_and_prewarm_is_atomic(
    app, monkeypatch
) -> None:
    import zhenxun.adapters.qq_official.adapter as adapter_module
    from zhenxun.adapters.qq_official.adapter import ZhenxunQQAdapter
    from zhenxun.models.bot_console import BotConsole

    adapter = object.__new__(ZhenxunQQAdapter)
    adapter._startup_lock = asyncio.Lock()
    adapter._prepared_bot_connect_lock = asyncio.Lock()
    adapter._webhook_bots = {}
    adapter._webhook_route_registered = False
    adapter._health_route_registered = False
    from zhenxun.adapters.qq_official.dispatcher import QQWebhookDispatcher

    adapter._dispatcher = QQWebhookDispatcher(shard_count=1, capacity=4)
    adapter.driver = nonebot.get_driver()
    adapter.qq_config = SimpleNamespace(qq_bots=[SimpleNamespace(id="app")])
    routes = []
    notifications = 0
    prewarms = 0
    info_logs = []
    error_logs = []

    class FakeBot:
        def __init__(self, owner, self_id, info) -> None:
            del owner, info
            self.self_id = self_id

        async def me(self):
            nonlocal prewarms
            prewarms += 1
            return object()

    async def notify(owner) -> None:
        nonlocal notifications
        assert owner is adapter
        notifications += 1

    def capture_info(*args, **kwargs) -> None:
        info_logs.append((args, kwargs))

    def capture_error(*args, **kwargs) -> None:
        error_logs.append((args, kwargs))

    adapter.setup_http_server = routes.append
    adapter.get_name = lambda: "QQ"
    monkeypatch.setattr(adapter_module, "ZhenxunQQBot", FakeBot)
    monkeypatch.setattr(adapter_module._runtime, "connect_prepared_adapter", notify)
    monkeypatch.setattr(adapter_module.logger, "info", capture_info)
    monkeypatch.setattr(adapter_module.logger, "error", capture_error)

    await asyncio.gather(adapter.startup(), adapter.startup())
    assert prewarms == 1
    assert len(routes) == 2
    assert notifications == 2
    assert info_logs == [
        (
            ("QQ 官方 Bot 信息预热完成（API me）", "QQOfficial"),
            {"target": "app"},
        )
    ]
    assert error_logs == []

    failing = object.__new__(ZhenxunQQAdapter)
    failing._startup_lock = asyncio.Lock()
    failing._prepared_bot_connect_lock = asyncio.Lock()
    failing._webhook_bots = {}
    failing._webhook_route_registered = False
    failing._health_route_registered = False
    failing._dispatcher = QQWebhookDispatcher(shard_count=1, capacity=4)
    failing.driver = nonebot.get_driver()
    failing.qq_config = SimpleNamespace(
        qq_bots=[
            SimpleNamespace(id="first", token="private-token", secret="private-secret"),
            SimpleNamespace(
                id="second", token="private-token", secret="private-secret"
            ),
        ]
    )
    failed_routes = []

    class FailingBot(FakeBot):
        async def me(self):
            if self.self_id == "second":
                raise RuntimeError("prewarm failed with private-provider-response")
            return object()

    failing.setup_http_server = failed_routes.append
    failing.get_name = lambda: "QQ"
    monkeypatch.setattr(adapter_module, "ZhenxunQQBot", FailingBot)
    with pytest.raises(RuntimeError, match="prewarm failed"):
        await failing.startup()
    assert failing._webhook_bots == {}
    assert not failing.is_ready()
    assert failed_routes == []
    assert notifications == 2
    assert info_logs[-1] == (
        ("QQ 官方 Bot 信息预热完成（API me）", "QQOfficial"),
        {"target": "first"},
    )
    assert len(error_logs) == 1
    assert error_logs[0][0][0].startswith("QQ 官方 Bot 信息预热失败（API me）")
    assert "code=qq_credential_failed" in error_logs[0][0][0]
    assert error_logs[0][0][1] == "QQOfficial"
    assert error_logs[0][1] == {"target": "second"}
    captured_logs = repr((info_logs, error_logs))
    assert "private-token" not in captured_logs
    assert "private-secret" not in captured_logs
    assert "private-provider-response" not in captured_logs
    failed_rows = await BotConsole.filter(
        bot_id__in=["qq_api:first", "qq_api:second"]
    ).count()
    assert failed_rows == 0
    await adapter._dispatcher.stop()
