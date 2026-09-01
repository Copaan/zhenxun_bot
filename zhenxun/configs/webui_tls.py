from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
import socket
from typing import Any

from cryptography import x509
from cryptography.hazmat.primitives import serialization
from dotenv import dotenv_values


class WebUITLSConfigError(ValueError):
    pass


@dataclass(frozen=True, slots=True)
class WebUITLSSettings:
    host: str = "0.0.0.0"
    port: int = 8080
    enabled: bool = False
    certfile: str = ""
    keyfile: str = ""
    redirect_enabled: bool = False
    redirect_port: int = 80

    @property
    def scheme(self) -> str:
        return "https" if self.enabled else "http"


def _bool(value: Any) -> bool:
    return str(value or "").strip().casefold() in {"1", "true", "yes", "on"}


def _port(value: Any, field: str, default: int) -> int:
    try:
        port = int(value if value not in {None, ""} else default)
    except (TypeError, ValueError) as exc:
        raise WebUITLSConfigError(f"{field} 必须是有效端口") from exc
    if not 1 <= port <= 65535:
        raise WebUITLSConfigError(f"{field} 必须在 1 到 65535 之间")
    return port


def settings_from_values(values: dict[str, Any]) -> WebUITLSSettings:
    return WebUITLSSettings(
        host=str(values.get("HOST") or "0.0.0.0").strip(),
        port=_port(values.get("PORT"), "PORT", 8080),
        enabled=_bool(values.get("WEBUI_HTTPS_ENABLED")),
        certfile=str(values.get("WEBUI_TLS_CERTFILE") or "").strip(),
        keyfile=str(values.get("WEBUI_TLS_KEYFILE") or "").strip(),
        redirect_enabled=_bool(values.get("WEBUI_HTTP_REDIRECT_ENABLED")),
        redirect_port=_port(
            values.get("WEBUI_HTTP_REDIRECT_PORT"),
            "WEBUI_HTTP_REDIRECT_PORT",
            80,
        ),
    )


def load_webui_tls_settings(root: Path | None = None) -> WebUITLSSettings:
    env_file = (root or Path.cwd()) / ".env.dev"
    values = dict(dotenv_values(env_file))
    return settings_from_values(values)


def _public_key_bytes(key: object) -> bytes:
    public_key = key.public_key()  # type: ignore[union-attr]
    return public_key.public_bytes(
        serialization.Encoding.DER,
        serialization.PublicFormat.SubjectPublicKeyInfo,
    )


def _validate_certificate(certfile: str, keyfile: str) -> None:
    cert_path = Path(certfile).expanduser()
    key_path = Path(keyfile).expanduser()
    for field, path in (
        ("WEBUI_TLS_CERTFILE", cert_path),
        ("WEBUI_TLS_KEYFILE", key_path),
    ):
        if not path.is_absolute() or not path.is_file():
            raise WebUITLSConfigError(f"{field} 必须是可读的绝对文件路径")
    try:
        certificate = x509.load_pem_x509_certificate(cert_path.read_bytes())
        private_key = serialization.load_pem_private_key(
            key_path.read_bytes(), password=None
        )
    except (OSError, ValueError, TypeError) as exc:
        raise WebUITLSConfigError("WebUI TLS 证书或私钥不是有效 PEM") from exc
    now = datetime.now(timezone.utc)
    if now < certificate.not_valid_before_utc or now >= certificate.not_valid_after_utc:
        raise WebUITLSConfigError("WebUI TLS 证书不在有效期内")
    if _public_key_bytes(certificate) != _public_key_bytes(private_key):
        raise WebUITLSConfigError("WebUI TLS 证书与私钥不匹配")


def validate_webui_tls_settings(
    settings: WebUITLSSettings,
    *,
    qq_https_port: int | None = None,
    launcher_managed: bool = True,
    check_redirect_port: bool = False,
) -> None:
    if settings.redirect_enabled and not settings.enabled:
        raise WebUITLSConfigError("启用 HTTP 重定向前必须先启用 WebUI HTTPS")
    if settings.redirect_enabled and not launcher_managed:
        raise WebUITLSConfigError("HTTP 重定向仅在 launcher 托管模式下可用")
    if settings.enabled:
        _validate_certificate(settings.certfile, settings.keyfile)
    if settings.redirect_enabled and settings.redirect_port == settings.port:
        raise WebUITLSConfigError("HTTP 重定向端口不能与 WebUI HTTPS 端口相同")
    occupied = {port for port in (qq_https_port,) if port is not None}
    if settings.enabled and settings.port in occupied:
        raise WebUITLSConfigError("WebUI HTTPS 端口与 QQ Webhook HTTPS 端口冲突")
    if settings.redirect_enabled and settings.redirect_port in occupied:
        raise WebUITLSConfigError("HTTP 重定向端口与 QQ Webhook HTTPS 端口冲突")
    if check_redirect_port and settings.redirect_enabled:
        family = socket.AF_INET6 if ":" in settings.host else socket.AF_INET
        probe = socket.socket(family, socket.SOCK_STREAM)
        try:
            probe.bind((settings.host, settings.redirect_port))
        except OSError as exc:
            raise WebUITLSConfigError("HTTP 重定向监听端口不可用") from exc
        finally:
            probe.close()


def current_webui_scheme() -> str:
    try:
        return load_webui_tls_settings().scheme
    except (OSError, WebUITLSConfigError):
        return "http"


__all__ = [
    "WebUITLSConfigError",
    "WebUITLSSettings",
    "current_webui_scheme",
    "load_webui_tls_settings",
    "settings_from_values",
    "validate_webui_tls_settings",
]
