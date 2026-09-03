from __future__ import annotations

import base64
from io import StringIO
import json

from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from dotenv import dotenv_values
from fastapi import HTTPException
import pytest
from starlette.requests import Request


def _request(token: str = "admin-token", host: str = "192.168.1.10") -> Request:
    return Request(
        {
            "type": "http",
            "method": "POST",
            "path": "/",
            "headers": [(b"authorization", f"Bearer {token}".encode())],
            "client": (host, 12345),
        }
    )


def test_qr_secret_decryption_uses_aes_gcm(app) -> None:
    del app
    from zhenxun.builtin_plugins.web_ui.api.protocol.qq_registration import (
        _decrypt_secret,
    )

    key = bytes(range(32))
    nonce = bytes(range(12))
    encrypted = AESGCM(key).encrypt(nonce, b"app-secret", None)
    payload = base64.b64encode(nonce + encrypted).decode()

    assert _decrypt_secret(payload, key) == "app-secret"


async def test_registration_sessions_are_owner_bound_and_bounded(app) -> None:
    del app
    import zhenxun.builtin_plugins.web_ui.api.protocol.qq_registration as registration

    store = registration._RegistrationStore()
    owner = registration._owner(_request())
    registration_id, _ = await store.create(owner, "task", b"k" * 32)
    assert (await store.get(registration_id, owner)).task_id == "task"

    with pytest.raises(HTTPException) as exc_info:
        await store.get(
            registration_id,
            registration._owner(_request(token="other")),
        )
    assert getattr(exc_info.value, "status_code", None) == 404

    await store.create(owner, "task-2", b"k" * 32)
    with pytest.raises(HTTPException) as exc_info:
        await store.create(owner, "task-3", b"k" * 32)
    assert getattr(exc_info.value, "status_code", None) == 429


async def test_completed_registration_saves_websocket_bot_without_leaking_secret(
    app, monkeypatch
) -> None:
    del app
    import zhenxun.builtin_plugins.web_ui.api.protocol.qq_registration as registration

    store = registration._RegistrationStore()
    monkeypatch.setattr(registration, "_sessions", store)
    request = _request()
    owner = registration._owner(request)
    key = bytes(range(32))
    registration_id, session = await store.create(owner, "provider-task", key)
    nonce = bytes(range(12))
    encrypted = base64.b64encode(
        nonce + AESGCM(key).encrypt(nonce, b"private-secret", None)
    ).decode()
    saved = []

    async def poll(_task_id):
        return {
            "status": 2,
            "bot_appid": "10001",
            "bot_encrypt_secret": encrypted,
        }

    async def probe(app_id, secret):
        assert (app_id, secret) == ("10001", "private-secret")
        return {
            "app_id": app_id,
            "bot_id": "bot-id",
            "username": "Test Bot",
            "avatar_url": "https://example.com/avatar.png",
        }

    async def save(app_id, secret):
        saved.append((app_id, secret))
        return {"revision": "a" * 64, "updated_existing": False}

    monkeypatch.setattr(registration, "_poll_bind_task", poll)
    monkeypatch.setattr(registration, "_probe_credential", probe)
    monkeypatch.setattr(registration, "_save_websocket_bot", save)
    monkeypatch.setattr(
        registration,
        "restart_status_data",
        lambda: {
            "launcher_managed": True,
            "access_urls": ["http://host:8080"],
            "access_targets": [{"kind": "network", "url": "http://host:8080"}],
        },
    )

    result = await registration.poll_qq_registration(registration_id, request)

    assert result.data["status"] == "completed"
    assert result.data["bot"]["app_id"] == "10001"
    assert result.data["bot"]["avatar_url"] == "https://example.com/avatar.png"
    assert saved == [("10001", "private-secret")]
    assert session.key == b""
    assert session.task_id == ""
    assert "private-secret" not in json.dumps(result.dict())


def test_websocket_bot_config_allows_empty_legacy_token(app) -> None:
    del app
    from zhenxun.adapters.qq_official.config import (
        QQOfficialBotConfig,
        QQOfficialConfig,
        QQOfficialIntent,
        validate_qq_config_data,
    )

    bot = QQOfficialBotConfig(
        id="10001",
        secret="secret",
        use_websocket=True,
        intent=QQOfficialIntent(c2c_group_at_messages=True),
    )
    validate_qq_config_data(QQOfficialConfig(qq_bots=[bot]))
    assert bot.token == ""


async def test_registration_merges_bot_and_preserves_existing_token(
    app, monkeypatch, tmp_path
) -> None:
    del app
    import zhenxun.builtin_plugins.web_ui.api.protocol.qq_registration as registration

    env_file = tmp_path / ".env.dev"
    env_file.write_text(
        "HOST=0.0.0.0\n"
        "QQ_ADAPTER_LOAD=False\n"
        "QQ_BOTS="
        + json.dumps(
            [
                {
                    "id": "10001",
                    "token": "legacy-token",
                    "secret": "old-secret",
                    "use_websocket": False,
                    "intent": {"c2c_group_at_messages": True},
                }
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(registration, "_ENV_FILE", env_file)
    monkeypatch.setattr(registration, "_source_path", lambda: env_file)
    monkeypatch.setattr(
        registration,
        "issue_restart_ticket",
        lambda *args, **kwargs: None,
    )

    result = await registration._save_websocket_bot("10001", "new-secret")

    values = dotenv_values(stream=StringIO(env_file.read_text(encoding="utf-8")))
    bots = json.loads(values["QQ_BOTS"])
    assert result["updated_existing"] is True
    assert values["QQ_ADAPTER_LOAD"] == "True"
    assert bots[0]["token"] == "legacy-token"
    assert bots[0]["secret"] == "new-secret"
    assert bots[0]["use_websocket"] is True


async def test_registration_save_waits_for_explicit_restart(
    app, monkeypatch, tmp_path
) -> None:
    del app
    import zhenxun.builtin_plugins.web_ui.api.protocol.qq_registration as registration
    from zhenxun.services import runtime_reload
    from zhenxun.services.runtime_reload import coordinator as coordinator_module
    from zhenxun.services.runtime_reload.coordinator import RuntimeChangeCoordinator
    from zhenxun.services.runtime_reload.manager import PluginRuntimeManager
    from zhenxun.services.runtime_reload.models import ApplyMode, RuntimeOperation
    from zhenxun.utils import _restart_utils, restart_state

    env_file = tmp_path / ".env.dev"
    env_file.write_text(
        "HOST=0.0.0.0\nQQ_ADAPTER_LOAD=False\nQQ_BOTS=[]\n",
        encoding="utf-8",
    )
    state_file = tmp_path / "restart.json"
    manager = PluginRuntimeManager()
    coordinator = RuntimeChangeCoordinator(manager)
    restart_reasons: list[str] = []

    async def record_restart(affected: set[str], reason: str) -> RuntimeOperation:
        restart_reasons.append(reason)
        return RuntimeOperation(
            ApplyMode.RESTART_PENDING,
            "pending_restart",
            sorted(affected),
            reason,
        )

    monkeypatch.setattr(registration, "_ENV_FILE", env_file)
    monkeypatch.setattr(registration, "_source_path", lambda: env_file)
    monkeypatch.setattr(runtime_reload, "plugin_runtime_manager", manager)
    monkeypatch.setattr(coordinator_module, "_ENV_FILES", {env_file.resolve()})
    monkeypatch.setattr(manager, "request_restart", record_restart)
    monkeypatch.setattr(_restart_utils, "_RESTART_STATE_FILE", state_file)
    monkeypatch.setattr(restart_state, "_RESTART_STATE_FILE", state_file)
    monkeypatch.setattr(_restart_utils, "_restart_pending", False)
    monkeypatch.setenv("ZHENXUN_LAUNCHER_PID", "123")

    await registration._save_websocket_bot("10001", "new-secret")

    saved_state = restart_state.read_restart_state()
    assert saved_state["restart_tickets"]["webui.settings"]["source"] == (
        "webui.settings"
    )
    assert "launcher_action" not in saved_state
    assert "pending_request" not in saved_state
    assert restart_state.consume_launcher_action() is None

    # A delayed watchfiles notification for the WebUI transaction is absorbed.
    assert await coordinator.process({env_file}) is None
    assert restart_reasons == []

    ok, _ = await _restart_utils.request_restart(
        "webui.settings",
        require_ticket="webui.settings",
    )

    assert ok is True
    requested_state = restart_state.read_restart_state()
    assert requested_state["launcher_action"] == "restart"
    assert requested_state["pending_request"]["source"] == "webui.settings"
    assert "restart_ticket" not in requested_state
