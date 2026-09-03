from __future__ import annotations

from io import StringIO
import json
from types import SimpleNamespace

from dotenv import dotenv_values
import httpx
import nonebot
import pytest


@pytest.mark.asyncio
async def test_database_probe_keeps_application_connection_alive(app) -> None:
    del app
    from tortoise import Tortoise

    from zhenxun.builtin_plugins.web_ui.api.configure.data_source import (
        test_db_connection,
    )

    connection = Tortoise.get_connection("default")
    await connection.execute_query("SELECT 1")
    assert await test_db_connection("sqlite://:memory:") is True
    await connection.execute_query("SELECT 1")


@pytest.mark.asyncio
async def test_connection_probe_errors_do_not_expose_credentials(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from zhenxun.builtin_plugins.web_ui.api.configure import data_source

    secret = "not-for-output"
    result = await data_source.test_db_connection(
        f"unknown://user:{secret}@localhost/database"
    )
    assert isinstance(result, str)
    assert secret not in result

    closed = False

    class FakeRedis:
        def __init__(self, **kwargs) -> None:
            assert kwargs["password"] == secret

        async def ping(self) -> None:
            raise RuntimeError(secret)

        async def aclose(self) -> None:
            nonlocal closed
            closed = True

    monkeypatch.setattr(data_source, "Redis", FakeRedis)
    result = await data_source.test_redis_connection("localhost", 6379, secret)
    assert isinstance(result, str)
    assert secret not in result
    assert closed


def test_dotenv_updates_remain_parseable() -> None:
    from zhenxun.builtin_plugins.web_ui.api.configure import (
        _quote_env,
        _set_env_value,
    )

    content = "HOST = 127.0.0.1\n# CACHE_MODE=NONE\n"
    content = _set_env_value(content, "HOST", "0.0.0.0")
    content = _set_env_value(content, "CACHE_MODE", "REDIS")
    content = _set_env_value(
        content,
        "REDIS_PASSWORD",
        _quote_env('value with "quotes"'),
    )
    parsed = dotenv_values(stream=StringIO(content))
    assert parsed["HOST"] == "0.0.0.0"
    assert parsed["CACHE_MODE"] == "REDIS"
    assert parsed["REDIS_PASSWORD"] == 'value with "quotes"'


def test_protocol_status_projects_only_non_sensitive_runtime_fields(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from zhenxun.adapters.qq_official.config import (
        QQOfficialBotConfig,
        QQOfficialConfig,
        QQOfficialIntent,
    )
    from zhenxun.adapters.qq_official.diagnostics import (
        clear_connection_diagnostics,
    )
    from zhenxun.builtin_plugins.web_ui.api import protocol

    def make_bot(self_id: str, adapter_name: str, self_info=None):
        adapter = SimpleNamespace(get_name=lambda: adapter_name)
        return SimpleNamespace(self_id=self_id, adapter=adapter, self_info=self_info)

    bots = {
        "client": make_bot("client", "OneBot V11"),
        "official": make_bot(
            "official",
            "QQ",
            SimpleNamespace(
                id="bot-id",
                username="Official Bot",
                avatar="https://example.com/official.png",
            ),
        ),
    }
    monkeypatch.setattr(nonebot, "get_bots", lambda: bots)
    monkeypatch.setattr(
        nonebot,
        "get_plugin_config",
        lambda _: QQOfficialConfig(
            qq_webhook_mode="builtin_https",
            qq_bots=[
                QQOfficialBotConfig(
                    id="official",
                    secret="private-secret",
                    use_websocket=True,
                    intent=QQOfficialIntent(c2c_group_at_messages=True),
                )
            ],
        ),
    )
    monkeypatch.setattr(protocol.BotConfig, "qq_adapter_load", True)
    clear_connection_diagnostics()

    status = protocol.build_protocol_status()
    payload = status.model_dump()
    assert status.onebot_v11_connected
    assert status.qq_official_enabled
    assert status.qq_official_connected
    assert status.qq_webhook_mode == "builtin_https"
    assert {item.platform for item in status.connections} == {
        "onebot_v11",
        "qq_official",
    }
    assert status.qq_bots[0].avatar_url == "https://example.com/official.png"
    assert status.qq_bots[0].username == "Official Bot"
    assert status.qq_bots[0].connected
    serialized = json.dumps(payload)
    assert "token" not in serialized.lower()
    assert "secret" not in serialized.lower()
    status_route = next(
        route for route in protocol.router.routes if route.path.endswith("/status")
    )
    assert status_route.dependant.dependencies


def test_existing_menu_is_merged_by_module_and_gains_protocol_entry(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from zhenxun.builtin_plugins.web_ui.api.menu import data_source

    menu_dir = tmp_path / "web_ui"
    menu_dir.mkdir()
    (menu_dir / "menu.json").write_text(
        json.dumps(
            [
                {
                    "name": "自定义仪表盘",
                    "module": "dashboard",
                    "router": "/dashboard",
                    "icon": "dashboard",
                    "default": True,
                }
            ],
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(data_source, "DATA_PATH", tmp_path)

    manager = data_source.MenuManager()
    by_module = {item.module: item for item in manager.menu}
    assert by_module["dashboard"].name == "自定义仪表盘"
    assert by_module["protocol"].router == "/protocol"


def test_qq_public_error_and_avatar_are_safely_projected() -> None:
    from zhenxun.adapters.qq_official.diagnostics import (
        clear_connection_diagnostics,
        public_error_from_exception,
        public_identity,
        safe_avatar_url,
        update_public_identity,
    )

    request = httpx.Request("GET", "https://api.sgroup.qq.com/gateway/bot")
    response = httpx.Response(
        401,
        request=request,
        json={"code": 11243, "message": "credential rejected"},
        headers={"X-Tps-trace-ID": "trace-123"},
    )
    error = httpx.HTTPStatusError("rejected", request=request, response=response)
    public_error = public_error_from_exception(error, stage="gateway")

    assert public_error.code == "qq_gateway_failed"
    assert public_error.provider_code == "11243"
    assert public_error.http_status == 401
    assert public_error.trace_id == "trace-123"
    assert public_error.provider_explanation
    assert public_error.suggestion
    assert safe_avatar_url("https://example.com/avatar.png")
    assert safe_avatar_url("javascript:alert(1)") is None
    assert safe_avatar_url("https://user:secret@example.com/avatar.png") is None
    clear_connection_diagnostics()
    update_public_identity(
        "app-id",
        bot_id="bot-id",
        username="Official Bot",
        avatar_url="https://example.com/avatar.png",
    )
    identity = public_identity("app-id")
    assert identity
    assert identity.username == "Official Bot"
    assert identity.avatar_url == "https://example.com/avatar.png"


@pytest.mark.asyncio
async def test_delete_last_qq_bot_clears_credentials_and_disables_adapter(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import zhenxun.builtin_plugins.web_ui.api.protocol.configuration as configuration

    env_file = tmp_path / ".env.dev"
    env_file.write_text(
        "QQ_ADAPTER_LOAD=True\n"
        "QQ_BOTS="
        + json.dumps(
            [
                {
                    "id": "10001",
                    "token": "private-token",
                    "secret": "private-secret",
                    "use_websocket": True,
                    "intent": {"c2c_group_at_messages": True},
                }
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    current = env_file.read_text(encoding="utf-8")
    monkeypatch.setattr(configuration, "_ENV_FILE", env_file)
    monkeypatch.setattr(configuration, "_source_path", lambda: env_file)
    monkeypatch.setattr(
        configuration, "issue_restart_ticket", lambda *args, **kwargs: None
    )
    monkeypatch.setattr(
        configuration,
        "restart_status_data",
        lambda: {
            "access_urls": ["http://localhost:8080"],
            "access_targets": [{"kind": "local", "url": "http://localhost:8080"}],
        },
    )
    monkeypatch.setenv("ZHENXUN_LAUNCHER_PID", "1")

    result = await configuration.delete_qq_bot(
        "10001",
        configuration._revision(current),
    )

    values = dotenv_values(stream=StringIO(env_file.read_text(encoding="utf-8")))
    assert result.data["remaining"] == 0
    assert result.data["qq_enabled"] is False
    assert values["QQ_ADAPTER_LOAD"] == "False"
    assert json.loads(values["QQ_BOTS"]) == []
    serialized = json.dumps(result.model_dump())
    assert "private-token" not in serialized
    assert "private-secret" not in serialized
