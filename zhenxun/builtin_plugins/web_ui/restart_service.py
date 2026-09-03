from __future__ import annotations

import os
from typing import Any
from urllib.parse import urlsplit

import nonebot

from zhenxun.configs.webui_tls import current_webui_scheme, load_webui_tls_settings
from zhenxun.utils._restart_utils import (
    get_pending_restart_items,
    get_pending_restart_reasons,
    request_restart,
)
from zhenxun.utils.network import AccessUrl, local_access_urls

from .console_access import console_access


def preferred_access_targets(host: str, port: int) -> list[AccessUrl]:
    urls = local_access_urls(host, port, current_webui_scheme())
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


def transaction_verification_status() -> dict[str, Any]:
    sources: list[str] = []
    try:
        from zhenxun.nonebot_store.storage import load_manifest

        if load_manifest().get("pending_verification"):
            sources.append("nonebot_store")
    except Exception:
        pass
    try:
        from zhenxun.plugin_store_transaction import pending_transaction

        transaction = pending_transaction() or {}
        if transaction.get("state") == "verification_pending":
            sources.append("zhenxun_store")
    except Exception:
        pass
    return {
        "transaction_verification_pending": bool(sources),
        "transaction_verification_sources": sources,
    }


def restart_status_data(*, access_urls: list[str] | None = None) -> dict[str, Any]:
    urls = list(dict.fromkeys([*(access_urls or []), *_current_access_urls()]))
    access_targets = [{"kind": _target_kind(url), "url": url} for url in urls]
    tls = load_webui_tls_settings()
    pending_reasons = set(get_pending_restart_reasons())
    pending_items = get_pending_restart_items()
    try:
        from zhenxun.nonebot_store.storage import pending_transaction as nonebot_pending

        transaction = nonebot_pending() or {}
        operations = transaction.get("operations", [])
        if operations and transaction.get("state") in {
            "pending_restart",
            "building",
            "migration_blocked",
            "failed",
        }:
            aggregate = next(
                (
                    item
                    for item in pending_items
                    if item.get("source") == "webui.nonebot-store"
                ),
                None,
            )
            pending_items = [
                item
                for item in pending_items
                if item.get("source") != "webui.nonebot-store"
            ]
            reasons = list((aggregate or {}).get("reasons", []))
            for operation in operations:
                if not isinstance(operation, dict):
                    continue
                pending_items.append(
                    {
                        "source": "webui.nonebot-store",
                        "operation_id": operation.get("operation_id"),
                        "store_key": f"nonebot:{operation.get('project_link', '')}",
                        "action": operation.get("action"),
                        "reasons": reasons,
                        "updated_at": (aggregate or {}).get("updated_at", 0),
                    }
                )
    except Exception:
        pass
    try:
        from zhenxun.plugin_store_transaction import public_transaction

        transaction = public_transaction() or {}
        existing_sources = {str(item.get("source")) for item in pending_items}
        for operation in transaction.get("operations", []):
            source = f"webui.plugin:{operation.get('store_key', '')}"
            if source in existing_sources:
                continue
            reason = str(operation.get("reason") or "plugin_change_requires_restart")
            pending_reasons.add(reason)
            pending_items.append(
                {
                    "source": source,
                    "operation_id": operation.get("operation_id"),
                    "store_key": operation.get("store_key"),
                    "action": operation.get("action"),
                    "reasons": [reason],
                    "updated_at": 0,
                }
            )
    except Exception:
        pass
    try:
        from zhenxun.services.runtime_reload import plugin_runtime_manager

        runtime_reasons = set(plugin_runtime_manager.pending_restart)
        pending_reasons.update(runtime_reasons)
        persisted_reasons = {
            str(reason) for item in pending_items for reason in item.get("reasons", [])
        }
        uncovered_runtime_reasons = runtime_reasons - persisted_reasons
        if uncovered_runtime_reasons:
            pending_items.append(
                {
                    "source": "runtime.plugins",
                    "reasons": sorted(uncovered_runtime_reasons),
                    "updated_at": 0,
                }
            )
    except Exception:
        pass
    return {
        "launcher_managed": bool(os.getenv("ZHENXUN_LAUNCHER_PID")),
        "boot_id": console_access.current_boot_id(),
        "access_urls": urls,
        "access_targets": access_targets,
        "preferred_url": urls[0] if urls else None,
        "scheme": tls.scheme,
        "https_enabled": tls.enabled,
        "http_redirect_enabled": tls.redirect_enabled,
        "http_redirect_port": tls.redirect_port if tls.redirect_enabled else None,
        "pending_restart": bool(pending_reasons),
        "pending_reasons": sorted(pending_reasons),
        "pending_count": len(pending_items),
        "pending_items": pending_items,
        **transaction_verification_status(),
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
    "transaction_verification_status",
]
