from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import ipaddress
import json
from pathlib import Path
import socket
from typing import Literal

from cryptography import x509
from cryptography.hazmat.primitives import serialization
from dotenv import dotenv_values
from pydantic import BaseModel, Field, ValidationError, validator

from zhenxun.utils.network import internal_connect_host

QQWebhookMode = Literal["external", "builtin_https"]


class QQOfficialIntent(BaseModel):
    guilds: bool = False
    guild_members: bool = False
    guild_messages: bool = False
    guild_message_reactions: bool = False
    direct_message: bool = False
    open_forum_event: bool = False
    audio_live_member: bool = False
    c2c_group_at_messages: bool = False
    interaction: bool = False
    message_audit: bool = False
    forum_event: bool = False
    audio_action: bool = False
    at_messages: bool = False

    class Config:
        extra = "forbid"


class QQOfficialBotConfig(BaseModel):
    id: str
    token: str
    secret: str
    use_websocket: bool = False
    intent: QQOfficialIntent = Field(default_factory=QQOfficialIntent)

    @validator("id", "token", "secret")
    def _not_blank(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("must not be blank")
        return value

    class Config:
        extra = "forbid"


class QQOfficialConfig(BaseModel):
    qq_bots: list[QQOfficialBotConfig] = Field(default_factory=list)
    qq_verify_webhook: bool = True
    qq_webhook_mode: QQWebhookMode = "external"
    qq_webhook_listen_host: str = "0.0.0.0"
    qq_webhook_listen_port: int = 443
    qq_webhook_tls_certfile: str = ""
    qq_webhook_tls_keyfile: str = ""
    qq_webhook_public_base_url: str = ""


@dataclass(frozen=True, slots=True)
class QQLauncherSettings:
    enabled: bool
    config: QQOfficialConfig
    worker_host: str
    worker_port: int

    @property
    def worker_connect_host(self) -> str:
        return internal_connect_host(self.worker_host)


class QQOfficialConfigError(RuntimeError):
    """Raised when explicitly enabled QQ official support is unsafe to start."""


def _parse_bool(value: object, *, field: str) -> bool:
    if isinstance(value, bool):
        return value
    normalized = str(value or "").strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"", "0", "false", "no", "off"}:
        return False
    raise QQOfficialConfigError(f"{field} 必须为布尔值")


def _parse_port(value: object, *, field: str) -> int:
    try:
        port = int(str(value).strip())
    except (TypeError, ValueError):
        raise QQOfficialConfigError(f"{field} 必须为端口号") from None
    if not 1 <= port <= 65535:
        raise QQOfficialConfigError(f"{field} 必须在 1..65535 范围内")
    return port


def load_qq_launcher_settings(project_root: Path) -> QQLauncherSettings:
    """Read launcher-facing settings without importing NoneBot or adapter secrets."""
    values = {
        str(key).upper(): value
        for key, value in dotenv_values(project_root / ".env.dev").items()
        if key
    }
    import os

    for key in (
        "QQ_ADAPTER_LOAD",
        "QQ_BOTS",
        "QQ_VERIFY_WEBHOOK",
        "QQ_WEBHOOK_MODE",
        "QQ_WEBHOOK_LISTEN_HOST",
        "QQ_WEBHOOK_LISTEN_PORT",
        "QQ_WEBHOOK_TLS_CERTFILE",
        "QQ_WEBHOOK_TLS_KEYFILE",
        "QQ_WEBHOOK_PUBLIC_BASE_URL",
        "HOST",
        "PORT",
    ):
        if key in os.environ:
            values[key] = os.environ[key]

    enabled = _parse_bool(values.get("QQ_ADAPTER_LOAD"), field="QQ_ADAPTER_LOAD")
    if not enabled:
        return QQLauncherSettings(
            enabled=False,
            config=QQOfficialConfig(),
            worker_host=str(values.get("HOST") or "127.0.0.1").strip(),
            worker_port=_parse_port(values.get("PORT", 8080), field="PORT"),
        )

    bots: object
    if enabled:
        raw_bots = values.get("QQ_BOTS")
        try:
            bots = json.loads(str(raw_bots or ""))
        except json.JSONDecodeError as exc:
            raise QQOfficialConfigError("QQ_BOTS 必须是合法 JSON 数组") from exc

    try:
        config = QQOfficialConfig(
            qq_bots=bots,
            qq_verify_webhook=_parse_bool(
                values.get("QQ_VERIFY_WEBHOOK", True), field="QQ_VERIFY_WEBHOOK"
            ),
            qq_webhook_mode=str(values.get("QQ_WEBHOOK_MODE") or "external"),
            qq_webhook_listen_host=str(
                values.get("QQ_WEBHOOK_LISTEN_HOST") or "0.0.0.0"
            ),
            qq_webhook_listen_port=_parse_port(
                values.get("QQ_WEBHOOK_LISTEN_PORT", 443),
                field="QQ_WEBHOOK_LISTEN_PORT",
            ),
            qq_webhook_tls_certfile=str(values.get("QQ_WEBHOOK_TLS_CERTFILE") or ""),
            qq_webhook_tls_keyfile=str(values.get("QQ_WEBHOOK_TLS_KEYFILE") or ""),
            qq_webhook_public_base_url=str(
                values.get("QQ_WEBHOOK_PUBLIC_BASE_URL") or ""
            ),
        )
    except ValidationError as exc:
        raise QQOfficialConfigError(
            f"QQ 官方适配器配置无效，错误字段: {_validation_paths(exc)}"
        ) from None

    return QQLauncherSettings(
        enabled=enabled,
        config=config,
        worker_host=str(values.get("HOST") or "127.0.0.1").strip(),
        worker_port=_parse_port(values.get("PORT", 8080), field="PORT"),
    )


def _validate_bind_host(host: str) -> None:
    try:
        ipaddress.ip_address(host)
    except ValueError:
        raise QQOfficialConfigError(
            "QQ_WEBHOOK_LISTEN_HOST 必须是本机 IPv4 或 IPv6 地址"
        ) from None


def _public_key_bytes(key: object) -> bytes:
    public_key = key.public_key()  # type: ignore[union-attr]
    return public_key.public_bytes(
        serialization.Encoding.DER,
        serialization.PublicFormat.SubjectPublicKeyInfo,
    )


def validate_builtin_ingress(
    settings: QQLauncherSettings, *, check_port: bool = True
) -> None:
    """Fail before process startup when built-in HTTPS cannot be started safely."""
    if not settings.enabled or settings.config.qq_webhook_mode != "builtin_https":
        return
    config = settings.config
    _validate_bind_host(config.qq_webhook_listen_host)
    if config.qq_webhook_listen_port == settings.worker_port:
        raise QQOfficialConfigError("公网 HTTPS 端口不能与 Bot PORT 相同")

    cert_path = Path(config.qq_webhook_tls_certfile).expanduser()
    key_path = Path(config.qq_webhook_tls_keyfile).expanduser()
    for field, path in (
        ("QQ_WEBHOOK_TLS_CERTFILE", cert_path),
        ("QQ_WEBHOOK_TLS_KEYFILE", key_path),
    ):
        if not path.is_absolute() or not path.is_file():
            raise QQOfficialConfigError(f"{field} 必须是可读的绝对文件路径")
    try:
        certificate = x509.load_pem_x509_certificate(cert_path.read_bytes())
        private_key = serialization.load_pem_private_key(
            key_path.read_bytes(), password=None
        )
    except (OSError, ValueError, TypeError) as exc:
        raise QQOfficialConfigError("QQ Webhook TLS 证书或私钥不是有效 PEM") from exc
    not_after = certificate.not_valid_after_utc
    not_before = certificate.not_valid_before_utc
    now = datetime.now(timezone.utc)
    if now < not_before or now >= not_after:
        raise QQOfficialConfigError("QQ Webhook TLS 证书不在有效期内")
    if _public_key_bytes(certificate) != _public_key_bytes(private_key):
        raise QQOfficialConfigError("QQ Webhook TLS 证书与私钥不匹配")

    if check_port:
        family = (
            socket.AF_INET6
            if ipaddress.ip_address(config.qq_webhook_listen_host).version == 6
            else socket.AF_INET
        )
        probe = socket.socket(family, socket.SOCK_STREAM)
        try:
            probe.bind((config.qq_webhook_listen_host, config.qq_webhook_listen_port))
        except OSError as exc:
            raise QQOfficialConfigError("QQ Webhook HTTPS 监听端口不可用") from exc
        finally:
            probe.close()


def _validation_paths(error: ValidationError) -> str:
    paths = []
    for item in error.errors():
        location = ".".join(str(part) for part in item.get("loc", ()))
        if location:
            paths.append(location)
    return ", ".join(dict.fromkeys(paths)) or "QQ_BOTS"


def validate_qq_official_config() -> QQOfficialConfig:
    """Validate all QQ settings before adapter imports can create side effects."""
    import nonebot

    try:
        config = nonebot.get_plugin_config(QQOfficialConfig)
    except ValidationError as exc:
        raise QQOfficialConfigError(
            f"QQ 官方适配器配置无效，错误字段: {_validation_paths(exc)}"
        ) from None

    validate_qq_config_data(config)
    return config


def validate_qq_config_data(config: QQOfficialConfig) -> None:
    """Validate credentials and intents without importing adapter runtime code."""
    if not config.qq_bots:
        raise QQOfficialConfigError("QQ_ADAPTER_LOAD=True 时 QQ_BOTS 不能为空")
    if not config.qq_verify_webhook:
        raise QQOfficialConfigError("QQ_VERIFY_WEBHOOK 必须保持开启")

    app_ids = [item.id for item in config.qq_bots]
    duplicates = sorted({app_id for app_id in app_ids if app_ids.count(app_id) > 1})
    if duplicates:
        raise QQOfficialConfigError("QQ_BOTS 存在重复 AppID")

    for index, item in enumerate(config.qq_bots):
        if item.use_websocket:
            raise QQOfficialConfigError(
                f"QQ_BOTS.{index}.use_websocket 首期 Webhook 模式必须为 false"
            )
        if not item.intent.c2c_group_at_messages:
            raise QQOfficialConfigError(
                f"QQ_BOTS.{index}.intent.c2c_group_at_messages 必须为 true"
            )
        unsupported_intents = [
            name
            for name, enabled in item.intent.dict().items()
            if name != "c2c_group_at_messages" and enabled
        ]
        if unsupported_intents:
            raise QQOfficialConfigError(
                f"QQ_BOTS.{index}.intent 包含首期不支持的字段: "
                + ", ".join(unsupported_intents)
            )


__all__ = [
    "QQLauncherSettings",
    "QQOfficialBotConfig",
    "QQOfficialConfig",
    "QQOfficialConfigError",
    "QQOfficialIntent",
    "QQWebhookMode",
    "load_qq_launcher_settings",
    "validate_builtin_ingress",
    "validate_qq_config_data",
    "validate_qq_official_config",
]
