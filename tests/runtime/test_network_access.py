from __future__ import annotations

import asyncio
import importlib.util
from io import StringIO
import json
from pathlib import Path
from types import SimpleNamespace
from typing import ClassVar

from jose import jwt
import pytest


def _load_webui_security():
    path = Path("zhenxun/builtin_plugins/web_ui/security.py").resolve()
    spec = importlib.util.spec_from_file_location(
        "zhenxun.builtin_plugins.web_ui._security_test", path
    )
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_internal_connect_host_separates_wildcard_from_dial_address() -> None:
    from zhenxun.utils.network import internal_connect_host

    assert internal_connect_host("0.0.0.0") == "127.0.0.1"
    assert internal_connect_host("::") == "::1"
    assert internal_connect_host("192.168.3.29") == "192.168.3.29"
    assert internal_connect_host("127.0.0.1") == "127.0.0.1"


def test_private_address_discovery_filters_inactive_and_non_rfc1918(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import socket

    import zhenxun.utils.network as network

    monkeypatch.setattr(
        network.psutil,
        "net_if_stats",
        lambda: {
            "lan": SimpleNamespace(isup=True),
            "down": SimpleNamespace(isup=False),
        },
    )
    monkeypatch.setattr(
        network.psutil,
        "net_if_addrs",
        lambda: {
            "lan": [
                SimpleNamespace(family=socket.AF_INET, address="192.168.3.29"),
                SimpleNamespace(family=socket.AF_INET, address="10.0.0.8"),
                SimpleNamespace(family=socket.AF_INET, address="127.0.0.1"),
                SimpleNamespace(family=socket.AF_INET, address="169.254.2.3"),
                SimpleNamespace(family=socket.AF_INET, address="8.8.8.8"),
            ],
            "down": [SimpleNamespace(family=socket.AF_INET, address="172.16.0.5")],
        },
    )

    assert network.private_ipv4_addresses() == ["10.0.0.8", "192.168.3.29"]
    assert [item.url for item in network.local_access_urls("0.0.0.0", 8080)] == [
        "http://localhost:8080",
        "http://10.0.0.8:8080",
        "http://192.168.3.29:8080",
    ]


def test_access_urls_do_not_present_wildcard_as_destination() -> None:
    from zhenxun.utils.network import local_access_urls

    urls = local_access_urls("127.0.0.1", 8080)
    assert [item.url for item in urls] == ["http://localhost:8080"]
    assert all("0.0.0.0" not in item.url for item in urls)


def test_ipv6_access_urls_are_bracketed_and_private_only(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import socket

    import zhenxun.utils.network as network

    monkeypatch.setattr(
        network.psutil,
        "net_if_stats",
        lambda: {"lan": SimpleNamespace(isup=True)},
    )
    monkeypatch.setattr(
        network.psutil,
        "net_if_addrs",
        lambda: {
            "lan": [
                SimpleNamespace(family=socket.AF_INET6, address="fd00::29"),
                SimpleNamespace(family=socket.AF_INET6, address="fe80::1%lan"),
                SimpleNamespace(family=socket.AF_INET6, address="2001:db8::1"),
            ]
        },
    )

    assert network.private_ipv6_addresses() == ["fd00::29"]
    assert [item.url for item in network.local_access_urls("::", 8080)] == [
        "http://localhost:8080",
        "http://[fd00::29]:8080",
    ]
    assert [item.url for item in network.local_access_urls("fd00::29", 8080)] == [
        "http://[fd00::29]:8080"
    ]


def test_console_banner_writes_connection_code_only_to_stderr(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import zhenxun.utils.network as network

    output = StringIO()
    monkeypatch.setattr(network.sys, "stderr", output)
    monkeypatch.setattr(network, "private_ipv4_addresses", lambda: ["192.168.3.29"])

    network.emit_webui_console_banner(
        "0.0.0.0",
        8080,
        connection_code="secret-code",
        state="configured",
        username="admin\nforged",
    )

    banner = output.getvalue()
    assert "http://localhost:8080/#/connect?code=secret-code" in banner
    assert "http://192.168.3.29:8080/#/connect?code=secret-code" in banner
    assert "普通登录: http://192.168.3.29:8080" in banner
    assert "管理员账号: admin forged" in banner
    assert "带 code 的连接链接" in banner
    assert "请勿分享" in banner


def test_unconfigured_console_banner_explains_loopback_only_access(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import zhenxun.utils.network as network

    output = StringIO()
    monkeypatch.setattr(network.sys, "stderr", output)
    network.emit_webui_console_banner(
        "127.0.0.1",
        8080,
        connection_code="setup-code",
        state="unconfigured",
    )

    banner = output.getvalue()
    assert "首次配置：必须打开下面带 code 的连接链接" in banner
    assert "普通登录（配置完成后）" in banner
    assert "当前仅监听本机" in banner


def test_private_client_classification() -> None:
    is_private_client = _load_webui_security().is_private_client

    assert is_private_client("127.0.0.1")
    assert is_private_client("192.168.3.29")
    assert is_private_client("10.0.0.2")
    assert is_private_client("fd00::1")
    assert not is_private_client("8.8.8.8")
    assert not is_private_client("203.0.113.5")
    assert not is_private_client(None)


def test_missing_webui_credentials_reject_token_without_error(monkeypatch) -> None:
    security = _load_webui_security()
    monkeypatch.setattr(security.Config, "get_config", lambda *args: None)

    assert not security.validate_access_token("Bearer invalid")


@pytest.mark.asyncio
async def test_login_attempt_reservations_are_bounded_under_concurrency() -> None:
    limiter = _load_webui_security().LoginAttemptLimiter()

    results = await asyncio.gather(*(limiter.reserve("192.168.3.5") for _ in range(12)))
    assert results.count(True) == 5
    assert results.count(False) == 7


@pytest.mark.asyncio
async def test_websocket_requires_private_client_and_valid_token(monkeypatch) -> None:
    security = _load_webui_security()

    class FakeWebSocket:
        def __init__(self, host: str, message: str) -> None:
            self.scope = {"client": (host, 12345)}
            self.message = message
            self.accepted = False
            self.closed: tuple[int, str] | None = None

        async def accept(self) -> None:
            self.accepted = True

        async def receive_text(self) -> str:
            return self.message

        async def close(self, code: int, reason: str) -> None:
            self.closed = (code, reason)

    monkeypatch.setattr(
        security, "validate_access_token", lambda token: token == "Bearer valid"
    )
    valid = FakeWebSocket("192.168.3.5", '{"type":"auth","token":"Bearer valid"}')
    invalid = FakeWebSocket("192.168.3.5", '{"type":"auth","token":"bad"}')
    public = FakeWebSocket("8.8.8.8", '{"token":"Bearer valid"}')

    assert await security.authenticate_websocket(valid)  # type: ignore[arg-type]
    assert valid.accepted
    assert valid.closed is None
    assert not await security.authenticate_websocket(invalid)  # type: ignore[arg-type]
    assert invalid.closed is not None
    assert invalid.closed[0] == 1008
    assert not await security.authenticate_websocket(public)  # type: ignore[arg-type]
    assert not public.accepted
    assert public.closed is not None
    assert public.closed[0] == 1008


@pytest.mark.asyncio
async def test_websocket_initial_expired_token_uses_auth_expired_code(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    security = _load_webui_security()
    values = {"secret": "secret", "username": "admin", "password": "configured"}
    monkeypatch.setattr(
        security.Config,
        "get_config",
        lambda _module, key, *args: values.get(key),
    )
    token = jwt.encode({"sub": "admin", "exp": 1}, values["secret"], algorithm="HS256")

    class FakeWebSocket:
        scope: ClassVar = {"client": ("127.0.0.1", 12345)}
        closed: tuple[int, str] | None = None

        async def accept(self) -> None:
            pass

        async def receive_text(self) -> str:
            return json.dumps({"type": "auth", "token": token})

        async def close(self, code: int, reason: str) -> None:
            self.closed = (code, reason)

    websocket = FakeWebSocket()
    assert not await security.authenticate_websocket(websocket)  # type: ignore[arg-type]
    assert websocket.closed == (
        security.WEBSOCKET_AUTH_EXPIRED,
        "authentication expired",
    )


@pytest.mark.asyncio
async def test_console_jwt_and_websockets_are_scoped_to_current_boot(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    security = _load_webui_security()
    values = {
        "secret": "current-secret",
        "username": "admin",
        "password": "configured",
    }
    monkeypatch.setattr(
        security.Config,
        "get_config",
        lambda _module, key, *args: values.get(key),
    )
    manager = security.console_access
    await manager.reset_for_tests()
    code = await manager.prepare()
    assert code
    boot_id = await manager.claim(code, "127.0.0.1")
    console_token = jwt.encode(
        {"sub": "admin", "auth_source": "console", "boot_id": boot_id},
        values["secret"],
        algorithm="HS256",
    )
    regular_token = jwt.encode({"sub": "admin"}, values["secret"], algorithm="HS256")

    assert security.validate_access_token(console_token)
    assert security.validate_access_token(regular_token)
    await manager.reset_for_tests()
    await manager.prepare()
    assert not security.validate_access_token(console_token)
    assert security.validate_access_token(regular_token)

    class FakeWebSocket:
        closed = False

        async def close(self, code: int, reason: str) -> None:
            self.closed = code == 1008 and reason == "credentials changed"

    websocket = FakeWebSocket()
    security._AUTHENTICATED_WEBSOCKETS.add(websocket)
    await security.revoke_authenticated_websockets()
    assert websocket.closed
    assert not security._AUTHENTICATED_WEBSOCKETS


@pytest.mark.asyncio
async def test_hashed_static_assets_receive_immutable_cache_header(tmp_path) -> None:
    PrivateNetworkStaticFiles = _load_webui_security().PrivateNetworkStaticFiles

    (tmp_path / "app.1234abcd.js").write_text("console.log('ok')", encoding="utf-8")
    (tmp_path / "plain.js").write_text("console.log('ok')", encoding="utf-8")
    static = PrivateNetworkStaticFiles(directory=tmp_path)
    scope = {
        "type": "http",
        "method": "GET",
        "path": "/app.1234abcd.js",
        "headers": [],
        "client": ("192.168.3.5", 12345),
    }

    hashed = await static.get_response("app.1234abcd.js", scope)
    plain = await static.get_response("plain.js", scope)

    assert hashed.headers["cache-control"] == ("public, max-age=31536000, immutable")
    assert "cache-control" not in plain.headers
