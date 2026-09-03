from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
import ipaddress
import json
from pathlib import Path
import socket
import ssl

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID
import httpx
import pytest
import uvicorn


def _write_certificate(directory: Path, *, expired: bool = False) -> tuple[Path, Path]:
    directory.mkdir(parents=True, exist_ok=True)
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    now = datetime.now(timezone.utc)
    subject = issuer = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "localhost")])
    certificate = (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(issuer)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - timedelta(days=2))
        .not_valid_after(
            now - timedelta(days=1) if expired else now + timedelta(days=2)
        )
        .add_extension(
            x509.SubjectAlternativeName(
                [
                    x509.DNSName("localhost"),
                    x509.IPAddress(ipaddress.ip_address("127.0.0.1")),
                ]
            ),
            critical=False,
        )
        .sign(key, hashes.SHA256())
    )
    cert_path = directory / "cert.pem"
    key_path = directory / "key.pem"
    cert_path.write_bytes(certificate.public_bytes(serialization.Encoding.PEM))
    key_path.write_bytes(
        key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        )
    )
    return cert_path, key_path


def _bot_config_json() -> str:
    return json.dumps(
        [
            {
                "id": "app",
                "token": "token",
                "secret": "secret",
                "use_websocket": False,
                "intent": {"c2c_group_at_messages": True},
            }
        ]
    )


def test_launcher_settings_ignore_qq_values_when_disabled(
    tmp_path, monkeypatch
) -> None:
    from zhenxun.adapters.qq_official.config import load_qq_launcher_settings

    (tmp_path / ".env.dev").write_text(
        "QQ_ADAPTER_LOAD=False\n"
        "QQ_BOTS=not-json\n"
        "QQ_WEBHOOK_MODE=invalid\n"
        "QQ_WEBHOOK_LISTEN_PORT=invalid\n",
        encoding="utf-8",
    )
    for key in (
        "QQ_ADAPTER_LOAD",
        "QQ_BOTS",
        "QQ_WEBHOOK_MODE",
        "QQ_WEBHOOK_LISTEN_PORT",
    ):
        monkeypatch.delenv(key, raising=False)

    settings = load_qq_launcher_settings(tmp_path)

    assert settings.enabled is False
    assert settings.config.qq_webhook_mode == "external"


def test_builtin_config_validates_certificate_and_port(tmp_path, monkeypatch) -> None:
    from zhenxun.adapters.qq_official.config import (
        QQOfficialConfigError,
        load_qq_launcher_settings,
        validate_builtin_ingress,
        validate_qq_config_data,
    )

    cert_path, key_path = _write_certificate(tmp_path)
    env = (
        "QQ_ADAPTER_LOAD=True\n"
        f"QQ_BOTS='{_bot_config_json()}'\n"
        "QQ_WEBHOOK_MODE=builtin_https\n"
        "QQ_WEBHOOK_LISTEN_HOST=127.0.0.1\n"
        "QQ_WEBHOOK_LISTEN_PORT=18443\n"
        f"QQ_WEBHOOK_TLS_CERTFILE={cert_path}\n"
        f"QQ_WEBHOOK_TLS_KEYFILE={key_path}\n"
        "HOST=127.0.0.1\nPORT=18080\n"
    )
    (tmp_path / ".env.dev").write_text(env, encoding="utf-8")
    for key in (
        "QQ_ADAPTER_LOAD",
        "QQ_BOTS",
        "QQ_WEBHOOK_MODE",
        "QQ_WEBHOOK_LISTEN_HOST",
        "QQ_WEBHOOK_LISTEN_PORT",
        "QQ_WEBHOOK_TLS_CERTFILE",
        "QQ_WEBHOOK_TLS_KEYFILE",
        "HOST",
        "PORT",
    ):
        monkeypatch.delenv(key, raising=False)

    settings = load_qq_launcher_settings(tmp_path)
    validate_qq_config_data(settings.config)
    validate_builtin_ingress(settings)

    _, expired_key = _write_certificate(tmp_path / "expired", expired=True)
    expired_cert = tmp_path / "expired" / "cert.pem"
    broken = settings.__class__(
        enabled=True,
        config=settings.config.copy(
            update={
                "qq_webhook_tls_certfile": str(expired_cert),
                "qq_webhook_tls_keyfile": str(expired_key),
            }
        ),
        worker_host=settings.worker_host,
        worker_port=settings.worker_port,
    )
    with pytest.raises(QQOfficialConfigError, match="有效期"):
        validate_builtin_ingress(broken, check_port=False)


@pytest.mark.parametrize(
    ("bind_host", "connect_host"),
    [
        ("0.0.0.0", "127.0.0.1"),
        ("::", "::1"),
        ("192.168.3.29", "192.168.3.29"),
        ("127.0.0.1", "127.0.0.1"),
    ],
)
def test_launcher_uses_dialable_worker_address(bind_host, connect_host) -> None:
    from zhenxun.adapters.qq_official.config import (
        QQLauncherSettings,
        QQOfficialConfig,
    )
    from zhenxun.cli import _build_ingress_command, _worker_health_url

    settings = QQLauncherSettings(
        enabled=True,
        config=QQOfficialConfig(
            qq_webhook_mode="builtin_https",
            qq_webhook_tls_certfile="cert.pem",
            qq_webhook_tls_keyfile="key.pem",
        ),
        worker_host=bind_host,
        worker_port=8080,
    )

    command = _build_ingress_command(settings)
    expected_host = f"[{connect_host}]" if ":" in connect_host else connect_host
    assert command[-1] == f"http://{expected_host}:8080"
    assert _worker_health_url(settings) == f"http://{expected_host}:8080/qq/healthz"


@pytest.mark.asyncio
async def test_ingress_preserves_body_signature_and_response() -> None:
    from zhenxun.adapters.qq_official.ingress import QQWebhookIngress

    captured = {}

    async def upstream(request: httpx.Request) -> httpx.Response:
        captured["body"] = await request.aread()
        captured["headers"] = request.headers
        return httpx.Response(
            200,
            content=b'{"op":12}',
            headers={"Content-Type": "application/json"},
        )

    proxy = QQWebhookIngress("http://worker.test")
    proxy._client = httpx.AsyncClient(transport=httpx.MockTransport(upstream))
    transport = httpx.ASGITransport(app=proxy.create_app())
    body = b'{"op":0, "d":{"content":"raw bytes"}}'
    async with httpx.AsyncClient(
        transport=transport, base_url="http://ingress"
    ) as client:
        response = await client.post(
            "/qq/webhook",
            content=body,
            headers={
                "Content-Type": "application/json",
                "X-Bot-Appid": "app",
                "X-Signature-Ed25519": "signature",
                "X-Signature-Timestamp": "timestamp",
            },
        )
        assert response.status_code == 200
        assert response.content == b'{"op":12}'
        assert response.headers["content-type"] == "application/json"
    await proxy._client.aclose()
    assert captured["body"] == body
    assert captured["headers"]["x-signature-ed25519"] == "signature"
    assert captured["headers"]["x-signature-timestamp"] == "timestamp"


@pytest.mark.asyncio
async def test_ingress_rejects_unsafe_requests_and_reports_degraded() -> None:
    from zhenxun.adapters.qq_official.ingress import (
        MAX_REQUEST_BODY,
        QQWebhookIngress,
    )

    proxy = QQWebhookIngress("http://worker.test")
    proxy._client = httpx.AsyncClient(
        transport=httpx.MockTransport(lambda _request: httpx.Response(503))
    )
    transport = httpx.ASGITransport(app=proxy.create_app())
    async with httpx.AsyncClient(
        transport=transport, base_url="http://ingress"
    ) as client:
        health = await client.get("/qq/healthz")
        assert health.status_code == 503
        assert health.json() == {"status": "degraded"}
        assert (await client.get("/qq/webhook")).status_code == 405
        assert (await client.get("/other")).status_code == 404
        compressed = await client.post(
            "/qq/webhook",
            content=b"{}",
            headers={"Content-Type": "application/json", "Content-Encoding": "gzip"},
        )
        assert compressed.status_code == 415
        non_json = await client.post("/qq/webhook", content=b"text")
        assert non_json.status_code == 415
        oversized = await client.post(
            "/qq/webhook",
            content=b"x" * (MAX_REQUEST_BODY + 1),
            headers={"Content-Type": "application/json"},
        )
        assert oversized.status_code == 413
    await proxy._client.aclose()


@pytest.mark.asyncio
async def test_dispatcher_orders_shards_and_enforces_global_capacity() -> None:
    from zhenxun.adapters.qq_official.dispatcher import QQWebhookDispatcher

    dispatcher = QQWebhookDispatcher(shard_count=2, capacity=3)
    await dispatcher.start()
    gate = asyncio.Event()
    started: list[str] = []
    completed: list[str] = []

    async def handler(value: str) -> None:
        started.append(value)
        await gate.wait()
        completed.append(value)

    key_a = "a"
    key_b = next(
        value
        for value in (f"b-{index}" for index in range(100))
        if dispatcher._shard_index(value) != dispatcher._shard_index(key_a)
    )
    assert await dispatcher.reserve()
    assert await dispatcher.enqueue(key_a, handler, "a1")
    assert await dispatcher.reserve()
    assert await dispatcher.enqueue(key_a, handler, "a2")
    assert await dispatcher.reserve()
    assert await dispatcher.enqueue(key_b, handler, "b1")
    assert not await dispatcher.reserve()
    for _ in range(100):
        if set(started) == {"a1", "b1"}:
            break
        await asyncio.sleep(0)
    assert set(started) == {"a1", "b1"}
    assert "a2" not in started
    gate.set()
    for _ in range(100):
        if len(completed) == 3:
            break
        await asyncio.sleep(0)
    assert completed.index("a1") < completed.index("a2")
    await dispatcher.stop()
    assert (await dispatcher.snapshot())["admitted"] == 0


def test_ingress_child_environment_excludes_credentials(monkeypatch) -> None:
    from zhenxun.cli import _ingress_environment

    monkeypatch.setenv("QQ_BOTS", "secret-json")
    monkeypatch.setenv("QQ_TOKEN", "token")
    monkeypatch.setenv("SOME_API_KEY", "key")
    monkeypatch.setenv("SAFE_VALUE", "visible")

    environment = _ingress_environment()

    assert "QQ_BOTS" not in environment
    assert "QQ_TOKEN" not in environment
    assert "SOME_API_KEY" not in environment
    assert environment["SAFE_VALUE"] == "visible"


@pytest.mark.asyncio
async def test_ingress_serves_real_tls_12_or_newer(tmp_path) -> None:
    from zhenxun.adapters.qq_official.ingress import (
        IngressSettings,
        QQWebhookIngress,
        build_ingress_config,
    )

    cert_path, key_path = _write_certificate(tmp_path)
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        port = int(probe.getsockname()[1])
    settings = IngressSettings(
        listen_host="127.0.0.1",
        listen_port=port,
        certfile=str(cert_path),
        keyfile=str(key_path),
        upstream_url="http://127.0.0.1:1",
    )
    proxy = QQWebhookIngress(settings.upstream_url)
    server = uvicorn.Server(build_ingress_config(settings, proxy.create_app()))
    task = asyncio.create_task(server.serve())
    try:
        for _ in range(200):
            if server.started:
                break
            await asyncio.sleep(0.01)
        assert server.started
        tls = ssl.create_default_context()
        tls.check_hostname = False
        tls.verify_mode = ssl.CERT_NONE
        tls.minimum_version = ssl.TLSVersion.TLSv1_2
        async with httpx.AsyncClient(verify=tls) as client:
            response = await client.get(f"https://127.0.0.1:{port}/qq/healthz")
        assert response.status_code == 503
        assert response.json() == {"status": "degraded"}
    finally:
        server.should_exit = True
        await task
