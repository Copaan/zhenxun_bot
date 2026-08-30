from __future__ import annotations

from dataclasses import dataclass
import ipaddress
import socket
import sys

import psutil

_IPV4_PRIVATE_NETWORKS = (
    ipaddress.ip_network("10.0.0.0/8"),
    ipaddress.ip_network("172.16.0.0/12"),
    ipaddress.ip_network("192.168.0.0/16"),
)
_IPV6_PRIVATE_NETWORK = ipaddress.ip_network("fc00::/7")


@dataclass(frozen=True, slots=True)
class AccessUrl:
    label: str
    url: str


def internal_connect_host(bind_host: str) -> str:
    """Return an address local components can dial for a server bind host."""
    normalized = bind_host.strip().strip("[]")
    if normalized == "0.0.0.0":
        return "127.0.0.1"
    if normalized == "::":
        return "::1"
    return normalized or "127.0.0.1"


def _is_private_ipv4(address: ipaddress.IPv4Address) -> bool:
    return any(address in network for network in _IPV4_PRIVATE_NETWORKS)


def private_ipv4_addresses() -> list[str]:
    """List active RFC1918 addresses without exposing wildcard or link-local IPs."""
    stats = psutil.net_if_stats()
    result: set[str] = set()
    for interface, addresses in psutil.net_if_addrs().items():
        interface_stats = stats.get(interface)
        if interface_stats is not None and not interface_stats.isup:
            continue
        for address in addresses:
            if address.family != socket.AF_INET:
                continue
            try:
                ip = ipaddress.IPv4Address(address.address)
            except ipaddress.AddressValueError:
                continue
            if _is_private_ipv4(ip):
                result.add(str(ip))
    return sorted(result, key=lambda value: int(ipaddress.IPv4Address(value)))


def private_ipv6_addresses() -> list[str]:
    """List active unique-local IPv6 addresses suitable for browser URLs."""
    stats = psutil.net_if_stats()
    result: set[str] = set()
    for interface, addresses in psutil.net_if_addrs().items():
        interface_stats = stats.get(interface)
        if interface_stats is not None and not interface_stats.isup:
            continue
        for address in addresses:
            if address.family != socket.AF_INET6:
                continue
            raw_address = address.address.split("%", 1)[0]
            try:
                ip = ipaddress.IPv6Address(raw_address)
            except ipaddress.AddressValueError:
                continue
            if ip in _IPV6_PRIVATE_NETWORK:
                result.add(str(ip))
    return sorted(result, key=lambda value: int(ipaddress.IPv6Address(value)))


def local_access_urls(
    bind_host: str, port: int, scheme: str = "http"
) -> list[AccessUrl]:
    """Build user-facing URLs for the addresses covered by one listening socket."""
    normalized = bind_host.strip().strip("[]")
    urls: list[AccessUrl] = []
    if normalized in {"0.0.0.0", "::", "127.0.0.1", "::1", "localhost"}:
        urls.append(AccessUrl("Local", f"{scheme}://localhost:{port}"))
    if normalized == "0.0.0.0":
        urls.extend(
            AccessUrl("Network", f"{scheme}://{address}:{port}")
            for address in private_ipv4_addresses()
        )
    elif normalized == "::":
        urls.extend(
            AccessUrl("Network", f"{scheme}://[{address}]:{port}")
            for address in private_ipv6_addresses()
        )
    else:
        try:
            ip = ipaddress.ip_address(normalized)
        except ValueError:
            ip = None
        if isinstance(ip, ipaddress.IPv4Address) and _is_private_ipv4(ip):
            urls.append(AccessUrl("Network", f"{scheme}://{ip}:{port}"))
        elif isinstance(ip, ipaddress.IPv6Address) and ip in _IPV6_PRIVATE_NETWORK:
            urls.append(AccessUrl("Network", f"{scheme}://[{ip}]:{port}"))
    return list(dict.fromkeys(urls))


def format_access_url_banner(bind_host: str, port: int) -> str:
    urls = local_access_urls(bind_host, port)
    if not urls:
        return f"WebUI listening on http://{bind_host}:{port}"
    lines = ["WebUI is ready"]
    lines.extend(f"  -> {item.label}: {item.url}" for item in urls)
    return "\n".join(lines)


def emit_webui_console_banner(
    bind_host: str,
    port: int,
    *,
    connection_code: str,
    state: str,
    username: str | None = None,
) -> None:
    """Write the startup-only WebUI recovery links outside application logs."""
    urls = local_access_urls(bind_host, port)
    if not urls:
        urls = [AccessUrl("Local", f"http://{bind_host}:{port}")]
    display_urls = list(urls)
    if bind_host.strip().strip("[]") in {"0.0.0.0", "::"}:
        display_urls.sort(key=lambda item: item.label != "Network")
    safe_username = " ".join(str(username or "").split())
    lines = ["", "WebUI is ready"]
    for item in display_urls:
        lines.append(f"  -> {item.label}: {item.url}/#/connect?code={connection_code}")
    preferred_login = next(
        (item.url for item in display_urls if item.label == "Network"),
        display_urls[0].url,
    )
    lines.append(f"  -> Normal login: {preferred_login}")
    if state == "configured":
        lines.append(f"  -> Account: {safe_username or 'configured administrator'}")
        lines.append("  -> Access: open a connection link for temporary admin login")
    else:
        lines.append("  -> Setup: open a connection link to finish initial setup")
    lines.append("  -> Security: links are valid for this startup; do not share them")
    sys.stderr.write("\n".join(lines) + "\n")
    sys.stderr.flush()


__all__ = [
    "AccessUrl",
    "emit_webui_console_banner",
    "format_access_url_banner",
    "internal_connect_host",
    "local_access_urls",
    "private_ipv4_addresses",
    "private_ipv6_addresses",
]
