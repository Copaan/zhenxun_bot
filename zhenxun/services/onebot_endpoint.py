from datetime import datetime, timezone
import ipaddress
from pathlib import Path
import re

from cryptography import x509
from urllib3.util.ssl_match_hostname import CertificateError, match_hostname


def normalize_reverse_ws_host(value: str) -> str:
    host = value.strip().rstrip(".")
    if not host:
        return ""
    try:
        return str(ipaddress.ip_address(host))
    except ValueError:
        pass
    try:
        host = host.encode("idna").decode("ascii").lower()
    except UnicodeError as error:
        raise ValueError("连接主机必须是主机名或 IP，不接受 URL") from error
    if len(host) > 253 or not all(
        re.fullmatch(r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?", label)
        for label in host.split(".")
    ):
        raise ValueError("连接主机必须是主机名或 IP，不接受端口、路径或凭据")
    return host


def reverse_ws_address(settings, configured_host: str, page_host: str) -> dict:
    try:
        host = normalize_reverse_ws_host(configured_host or page_host)
    except ValueError:
        host = ""
    if host in {"0.0.0.0", "::"}:
        host = ""
    authority = f"[{host}]" if ":" in host else host
    scheme = "wss" if settings.enabled else "ws"
    return {
        "connection_host": host,
        "configured_host": configured_host,
        "host_source": "configured" if configured_host else "page_host",
        "scheme": scheme,
        "port": settings.port,
        "url": f"{scheme}://{authority}:{settings.port}/onebot/v11/ws"
        if host
        else None,
        "reachability": "unverified",
    }


def reverse_ws_diagnostic(
    settings,
    configured_host: str,
    page_host: str,
    *,
    certificate_pem: bytes | None = None,
) -> dict:
    address = reverse_ws_address(settings, configured_host, page_host)
    host = address["connection_host"]
    result = {
        **address,
        "certificate_name_match": None,
        "certificate_valid_now": None,
        "certificate_expires_at": None,
        "certificate_domains": [],
        "certificate_candidates": [],
        "certificate_wildcards": [],
        "trust": "client_verification_required",
        "code": "connection_host_invalid"
        if not host
        else "tls_not_enabled"
        if not settings.enabled
        else "tls_certificate_unavailable",
    }
    if not settings.enabled:
        return result
    try:
        certificate = x509.load_pem_x509_certificate(
            certificate_pem
            if certificate_pem is not None
            else Path(settings.certfile).read_bytes()
        )
        san = certificate.extensions.get_extension_for_class(
            x509.SubjectAlternativeName
        ).value
        domains = san.get_values_for_type(x509.DNSName)
        ips = san.get_values_for_type(x509.IPAddress)
        normalized_domains = []
        for domain in domains:
            wildcard = domain.startswith("*.")
            try:
                normalized = normalize_reverse_ws_host(
                    domain[2:] if wildcard else domain
                )
            except ValueError:
                continue
            if not normalized:
                continue
            normalized = f"*.{normalized}" if wildcard else normalized
            if normalized not in normalized_domains:
                normalized_domains.append(normalized)
        result["certificate_domains"] = normalized_domains[:20]
        result["certificate_wildcards"] = [
            name for name in normalized_domains[:20] if name.startswith("*.")
        ]
        before = certificate.not_valid_before_utc
        after = certificate.not_valid_after_utc
        result["certificate_expires_at"] = after.isoformat()
        result["certificate_valid_now"] = before <= datetime.now(timezone.utc) <= after

        def name_matches(name: str) -> bool:
            try:
                match_hostname(
                    {
                        "subjectAltName": [("DNS", domain) for domain in domains]
                        + [("IP Address", str(ip)) for ip in ips]
                    },
                    name,
                )
                return True
            except CertificateError:
                return False

        result["certificate_candidates"] = [
            {
                **reverse_ws_address(settings, name, ""),
                "host_source": "certificate_san",
                "certificate_name_match": name_matches(name),
                "certificate_valid_now": result["certificate_valid_now"],
                "trust": "client_verification_required",
            }
            for name in normalized_domains[:20]
            if not name.startswith("*.")
        ]
        result["certificate_name_match"] = name_matches(host)
        result["code"] = (
            "connection_host_invalid"
            if not host
            else "tls_certificate_time_invalid"
            if not result["certificate_valid_now"]
            else "tls_certificate_name_mismatch"
            if not result["certificate_name_match"]
            else "tls_certificate_identity_ok"
        )
    except (OSError, ValueError, x509.ExtensionNotFound):
        pass
    return result


def configured_reverse_ws_endpoint(values: dict, page_host: str) -> dict:
    import nonebot

    from zhenxun.configs.webui_tls import runtime_webui_settings, settings_from_values

    desired = settings_from_values(values)
    current = runtime_webui_settings()
    host = str(values.get("ONEBOT_REVERSE_WS_HOST") or "")
    current_host = str(
        getattr(nonebot.get_driver().config, "onebot_reverse_ws_host", "") or ""
    )
    return {
        **reverse_ws_address(desired, host, page_host),
        "pending_restart": host != current_host
        or any(
            getattr(desired, key) != getattr(current, key)
            for key in ("enabled", "host", "port", "certfile", "keyfile")
        ),
        "scope": "saved_configuration",
    }


def current_reverse_ws_diagnostic(page_host: str) -> dict:
    import nonebot

    from zhenxun.configs.webui_tls import (
        runtime_webui_certificate,
        runtime_webui_settings,
    )

    config = nonebot.get_driver().config
    settings = runtime_webui_settings()
    return reverse_ws_diagnostic(
        settings,
        str(getattr(config, "onebot_reverse_ws_host", "") or ""),
        page_host,
        certificate_pem=runtime_webui_certificate(),
    )


class OneBotHandshakeDiagnostics:
    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope.get("path") != "/onebot/v11/ws" or scope["type"] not in {
            "http",
            "websocket",
        }:
            return await self.app(scope, receive, send)
        from zhenxun.services.webui_transport import transport_runtime

        reported = False

        async def observed_send(message):
            nonlocal reported
            if not reported:
                kind = message["type"]
                code = None
                if kind == "websocket.accept":
                    code = "onebot_handshake_accepted"
                elif kind == "websocket.close":
                    code = "onebot_handshake_rejected"
                elif kind in {"http.response.start", "websocket.http.response.start"}:
                    status = message.get("status", 500)
                    if status in {401, 403}:
                        code = "onebot_auth_rejected"
                    elif status >= 400:
                        code = "onebot_http_handshake_failed"
                if code:
                    reported = True
                    transport_runtime.record_diagnostic(code)
            await send(message)

        await self.app(scope, receive, observed_send)
