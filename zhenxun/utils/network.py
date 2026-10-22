from __future__ import annotations

from collections.abc import Mapping
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


def _parse_address(value: str) -> ipaddress.IPv4Address | ipaddress.IPv6Address | None:
    """解析地址文本，去掉引号、方括号与端口

    参数:
        value: 地址文本

    返回:
        IPv4Address | IPv6Address | None: 地址，IPv4 映射的 IPv6 地址转为 IPv4，
            无法解析时为 None
    """
    text = value.strip().strip('"')
    if text.startswith("["):
        text = text[1:].split("]", 1)[0]
    elif text.count(":") == 1:
        text = text.split(":", 1)[0]
    try:
        address = ipaddress.ip_address(text)
    except ValueError:
        return None
    if isinstance(address, ipaddress.IPv6Address) and address.ipv4_mapped:
        return address.ipv4_mapped
    return address


def public_client_address(host: str | None, headers: Mapping[str, str]) -> str | None:
    """请求来源中的公网地址

    依次检查直接连接地址与反向代理转发头（X-Forwarded-For、X-Real-IP、
    Forwarded）中的客户端地址；本机、局域网与 CGNAT 等非公网地址以及无法解析的
    地址不计入。

    参数:
        host: 直接连接的客户端地址
        headers: 请求头

    返回:
        str | None: 第一个公网地址，来源都不是公网地址时为 None
    """
    candidates = [host or "", headers.get("x-real-ip") or ""]
    candidates += (headers.get("x-forwarded-for") or "").split(",")
    for item in (headers.get("forwarded") or "").replace(",", ";").split(";"):
        key, _, value = item.partition("=")
        if key.strip().lower() == "for":
            candidates.append(value)
    for value in candidates:
        address = _parse_address(value)
        if address is not None and address.is_global:
            return str(address)
    return None


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


def format_access_url_banner(bind_host: str, port: int, *, scheme: str = "http") -> str:
    urls = local_access_urls(bind_host, port, scheme)
    if not urls:
        return f"WebUI listening on {scheme}://{bind_host}:{port}"
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
    scheme: str = "http",
    http_compatibility_port: int | None = None,
) -> None:
    """Write the startup-only WebUI recovery links outside application logs."""
    urls = local_access_urls(bind_host, port, scheme)
    if scheme == "https" and http_compatibility_port is not None:
        urls.extend(local_access_urls(bind_host, http_compatibility_port, "http"))
    if not urls:
        urls = [AccessUrl("Local", f"{scheme}://{bind_host}:{port}")]
    display_urls = list(urls)
    if bind_host.strip().strip("[]") in {"0.0.0.0", "::"}:
        display_urls.sort(key=lambda item: item.label != "Network")
    safe_username = " ".join(str(username or "").split())
    lines = ["", "WebUI 已启动"]
    if state == "configured":
        lines.append("  临时管理：打开下面带连接码的链接即可直接登录")
    else:
        lines.append("  首次配置：必须打开下面带连接码的链接")
    for item in display_urls:
        label = "本机" if item.label == "Local" else "局域网"
        lines.append(
            f"  -> {label}连接链接: {item.url}/#/connect?code={connection_code}"
        )
    preferred_login = next(
        (item.url for item in display_urls if item.label == "Network"),
        display_urls[0].url,
    )
    login_label = "普通登录" if state == "configured" else "普通登录（配置完成后）"
    lines.append(f"  -> {login_label}: {preferred_login}")
    if state == "configured":
        lines.append(f"  -> 管理员账号: {safe_username or '已配置'}")
        lines.append("  -> 提示: 连接链接无需输入账号密码，忘记密码时也可用它登录")
    else:
        lines.append("  -> 提示: 普通地址不带连接码，无法进行首次配置")
    normalized_host = bind_host.strip().strip("[]")
    has_network = any(item.label == "Network" for item in display_urls)
    if normalized_host in {"0.0.0.0", "::"}:
        if not has_network:
            lines.append("  -> 网络: 未检测到私网网卡地址，仅显示本机链接")
        lines.append("  -> 公网: 使用服务器公网 IP 或域名访问，需放通端口")
    elif normalized_host in {"127.0.0.1", "::1", "localhost"}:
        lines.append("  -> 网络: 当前仅监听本机，局域网设备无法访问")
    lines.append("  -> 安全提示: 连接链接仅对本次启动有效，请勿分享")
    if http_compatibility_port is not None:
        lines.append("  -> HTTP 兼容入口为明文传输，请优先使用 HTTPS")
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
    "public_client_address",
]
