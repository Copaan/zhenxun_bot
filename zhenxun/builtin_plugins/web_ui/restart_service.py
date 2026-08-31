from __future__ import annotations

import os
from typing import Any
from urllib.parse import urlsplit

import nonebot

from zhenxun.utils._restart_utils import request_restart
from zhenxun.utils.network import AccessUrl, local_access_urls

from .console_access import console_access


def preferred_access_targets(host: str, port: int) -> list[AccessUrl]:
    urls = local_access_urls(host, port)
    if host.strip().strip("[]") in {"0.0.0.0", "::"}:
        urls.sort(key=lambda item: item.label != "Network")
    return urls


def preferred_access_urls(host: str, port: int) -> list[str]:
    return [item.url for item in preferred_access_targets(host, port)]


def _target_kind(url: str) -> str:
    try:
        hostname = (urlsplit(url).hostname or "").lower()
    except ValueError:
        return "custom"
    if hostname in {"localhost", "127.0.0.1", "::1"}:
        return "local"
    return "network"


def _current_access_urls() -> list[str]:
    driver = nonebot.get_driver()
    host = str(getattr(driver.config, "host", "127.0.0.1"))
    port = int(getattr(driver.config, "port", 8080))
    return preferred_access_urls(host, port)


def restart_status_data(*, access_urls: list[str] | None = None) -> dict[str, Any]:
    urls = list(dict.fromkeys([*(access_urls or []), *_current_access_urls()]))
    access_targets = [{"kind": _target_kind(url), "url": url} for url in urls]
    return {
        "launcher_managed": bool(os.getenv("ZHENXUN_LAUNCHER_PID")),
        "boot_id": console_access.current_boot_id(),
        "access_urls": urls,
        "access_targets": access_targets,
        "preferred_url": urls[0] if urls else None,
    }


async def request_webui_restart(
    source: str,
    *,
    require_ticket: str | None = None,
    access_urls: list[str] | None = None,
) -> tuple[bool, str, dict[str, Any]]:
    status = restart_status_data(access_urls=access_urls)
    if not status["launcher_managed"]:
        return False, "当前不是 launcher 托管模式，请手动重启真寻。", status
    ok, message = await request_restart(source, require_ticket=require_ticket)
    return ok, message, status


__all__ = [
    "preferred_access_targets",
    "preferred_access_urls",
    "request_webui_restart",
    "restart_status_data",
]
