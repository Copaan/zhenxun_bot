from __future__ import annotations

import os
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

from zhenxun.configs.webui_tls import (
    WebUITLSSettings,
    load_webui_tls_settings,
    runtime_webui_settings,
)
from zhenxun.services.startup import startup_coordinator
from zhenxun.services.webui_http import http_compat_state
from zhenxun.utils._restart_utils import (
    request_restart,
)
from zhenxun.utils.network import AccessUrl, local_access_urls

from .console_access import console_access
from .restart_evidence import CONFIGURATION_SOURCES, reconcile_configuration_restarts


def preferred_access_targets(
    host: str,
    port: int,
    *,
    settings: WebUITLSSettings | None = None,
    http_available: bool | None = None,
) -> list[AccessUrl]:
    tls = settings or runtime_webui_settings()
    urls = local_access_urls(host, port, tls.scheme)
    if http_available is None:
        state = http_compat_state()
        http_available = (
            state.get("state") == "ready"
            and state.get("port") == tls.redirect_port
            and state.get("mode") == tls.effective_http_mode
        )
    if tls.http_compat_enabled and http_available:
        urls.extend(local_access_urls(host, tls.redirect_port, "http"))
    if host.strip().strip("[]") in {"0.0.0.0", "::"}:
        urls.sort(key=lambda item: item.label != "Network")
    return list(dict.fromkeys(urls))


def preferred_access_urls(
    host: str,
    port: int,
    *,
    settings: WebUITLSSettings | None = None,
    http_available: bool | None = None,
) -> list[str]:
    return [
        item.url
        for item in preferred_access_targets(
            host,
            port,
            settings=settings,
            http_available=http_available,
        )
    ]


def _target_kind(url: str) -> str:
    try:
        hostname = (urlsplit(url).hostname or "").lower()
    except ValueError:
        return "custom"
    if hostname in {"localhost", "127.0.0.1", "::1"}:
        return "local"
    return "network"


def _current_access_urls() -> list[str]:
    tls = runtime_webui_settings()
    return preferred_access_urls(tls.host, tls.port)


def transaction_verification_status() -> dict[str, Any]:
    sources: list[str] = []
    known = True
    try:
        from zhenxun.services.nonebot_store.storage import (
            load_manifest,
            pending_transaction,
        )

        transaction = pending_transaction() or {}
        if load_manifest().get("pending_verification") or transaction.get("state") in {
            "verification_pending",
            "settling",
            "committing",
        }:
            sources.append("nonebot_store")
    except Exception:
        known = False
    try:
        from zhenxun.services.plugin_store.plugin_store_transaction import (
            pending_transaction,
        )

        transaction = pending_transaction() or {}
        if transaction.get("state") in {"verification_pending", "committing"}:
            sources.append("zhenxun_store")
    except Exception:
        known = False
    return {
        "transaction_verification_pending": bool(sources) or not known,
        "transaction_verification_known": known,
        "transaction_verification_sources": sources,
    }


def network_configuration_status() -> dict[str, Any]:
    tls = runtime_webui_settings()
    desired = load_webui_tls_settings()
    http = http_compat_state() if tls.http_compat_enabled else {}
    return {
        "network_runtime": {
            "scheme": tls.scheme,
            "port": tls.port,
            "http_mode": tls.effective_http_mode,
            "http_port": tls.redirect_port if tls.http_compat_enabled else None,
            "http_state": http.get("state", "disabled"),
            **{
                f"http_{key}": http.get(key)
                for key in ("error_code", "errno", "retry_count", "retry_in_seconds")
            },
        },
        "network_configured": {
            "scheme": desired.scheme,
            "port": getattr(desired, "port", 8080),
            "http_mode": getattr(desired, "effective_http_mode", "disabled"),
            "http_port": getattr(desired, "redirect_port", 0)
            if getattr(desired, "http_compat_enabled", False)
            else None,
            "pending_restart": desired != tls,
        },
        "access_urls": _current_access_urls(),
    }


def access_targets(urls: list[str]) -> list[dict[str, str]]:
    """整理前端使用的访问目标

    参数:
        urls: 访问地址列表

    返回:
        list[dict[str, str]]: 访问目标
    """
    return [
        {"kind": _target_kind(url), "url": url, "scheme": urlsplit(url).scheme}
        for url in urls
    ]


def _transaction_state(transaction: dict[str, Any]) -> str:
    if transaction.get("state") in {"settling", "committing", "rolling_back"}:
        return "verifying"
    if transaction.get("rollback_confirmed"):
        return "rolled_back"
    if transaction.get("verification_confirmed") or transaction.get("state") in {
        "committed",
        "cleanup_pending",
    }:
        return (
            "cleanup_pending"
            if transaction.get("cleanup_error")
            or transaction.get("state") == "cleanup_pending"
            else "applied"
        )
    if transaction.get("preparation_only"):
        return "preparing"
    return {
        "preparing": "preparing",
        "pending_restart": "restart_pending",
        "building": "preparing",
        "applying": "applying",
        "rolling_back": "verifying",
        "migration_blocked": "attention_required",
        "verification_pending": "verifying",
        "settling": "verifying",
        "committing": "verifying",
        "failed": "recovery_required",
        "recovery_required": "recovery_required",
    }.get(transaction.get("state"), "unknown")


def restart_changes() -> tuple[list[dict[str, Any]], bool]:
    """汇总变更真实状态，读取失败保留待核验提示。"""
    from zhenxun.utils.restart_state import _PENDING_RESTARTS_KEY, read_restart_state

    changes = []
    known = True
    try:
        records = read_restart_state().get(_PENDING_RESTARTS_KEY, {})
    except Exception:
        records = {}
        known = False
        changes.append(
            {
                "source": "restart.state",
                "state": "unknown",
                "reason": "restart_record_unreadable",
                "reasons": [],
            }
        )
    try:
        configuration = reconcile_configuration_restarts() if records else {}
    except Exception:
        configuration = {}
        known = False
    for source, record in records.items():
        if source in CONFIGURATION_SOURCES:
            item = configuration.get(
                source,
                {
                    "source": source,
                    "state": "unknown",
                    "reason": "configuration_verification_unavailable",
                    "reasons": record.get("reasons", []),
                },
            )
            changes.append(item)
            if item.get("reason") == "configuration_verification_unavailable":
                known = False
        elif source.startswith("webui.update:"):
            try:
                from zhenxun.services.update.update_service import read_job

                job = read_job(source.split(":", 1)[1])
                state = {
                    "queued": "preparing",
                    "preparing": "preparing",
                    "staged": "preparing",
                    "pending_restart": "restart_pending",
                    "restart_requested": "applying",
                    "applying": "applying",
                    "verifying": "verifying",
                    "completed": "applied",
                    "cancelled": "rolled_back",
                    "failed": "recovery_required",
                    "recovery_required": "recovery_required",
                }.get(job.get("state"), "unknown")
                if job.get("rollback_confirmed"):
                    state = "rolled_back"
                elif state == "applied" and job.get("cleanup_error"):
                    state = "cleanup_pending"
                changes.append(
                    {
                        "source": source,
                        "state": state,
                        "reasons": record.get("reasons", []),
                        "reason": "update_" + str(job.get("state", "unknown")),
                    }
                )
            except Exception:
                known = False
                changes.append(
                    {
                        "source": source,
                        "state": "unknown",
                        "reason": "update_record_unreadable",
                        "reasons": [],
                    }
                )
        elif source != "webui.nonebot-store" and not source.startswith("webui.plugin:"):
            changes.append(
                {
                    "source": source,
                    "state": "unknown",
                    "reason": "restart_evidence_missing",
                    "reasons": record.get("reasons", []),
                }
            )

    def append_transaction(transaction, source_for):
        state = _transaction_state(transaction)
        operations = transaction.get("operations", []) or [{}]
        for operation in operations:
            key = source_for(operation)
            marker = records.get(key, {})
            changes.append(
                {
                    "source": key,
                    "state": state,
                    "transaction_state": transaction.get("state"),
                    "operation_id": operation.get("operation_id"),
                    "store_key": operation.get("store_key")
                    or operation.get("project_link"),
                    "action": operation.get("action"),
                    "reasons": marker.get("reasons", []),
                    "reason": "external_database_manual_migration_required"
                    if transaction.get("state") == "migration_blocked"
                    else state,
                    "cleanup_pending": state == "cleanup_pending",
                    "revision": transaction.get("revision"),
                }
            )

    try:
        from zhenxun.services.nonebot_store.storage import (
            dependency_sync_status,
            pending_transaction,
        )

        transaction = pending_transaction()
        sync = dependency_sync_status()
        if transaction:
            append_transaction(transaction, lambda _: "webui.nonebot-store")
        elif sync.get("operation_id") and (
            sync.get("status") not in {"succeeded", "canceled"}
            or "webui.nonebot-store" in records
        ):
            state = {
                "prepared": "restart_pending",
                "pending": "applying",
                "running": "applying",
                "succeeded": "applied",
                "canceled": "rolled_back",
                "failed": "attention_required",
            }.get(sync.get("status"), "unknown")
            changes.append(
                {
                    "source": "webui.nonebot-store",
                    "state": state,
                    "operation_id": sync["operation_id"],
                    "reason": "dependency_sync_" + str(sync.get("status")),
                    "reasons": records.get("webui.nonebot-store", {}).get(
                        "reasons", []
                    ),
                }
            )
        elif "webui.nonebot-store" in records:
            changes.append(
                {
                    "source": "webui.nonebot-store",
                    "state": "unknown",
                    "reason": "transaction_receipt_unavailable",
                    "reasons": [],
                }
            )
    except Exception:
        known = False
        changes.append(
            {
                "source": "webui.nonebot-store",
                "state": "unknown",
                "reason": "transaction_record_unreadable",
                "reasons": [],
            }
        )
    try:
        from zhenxun.services.plugin_store.plugin_store_transaction import (
            public_transaction,
        )

        transaction = public_transaction()
        if transaction:
            append_transaction(
                transaction, lambda op: "webui.plugin:" + str(op.get("store_key", ""))
            )
        covered_sources = {item["source"] for item in changes}
        for source in records:
            if source.startswith("webui.plugin:") and source not in covered_sources:
                changes.append(
                    {
                        "source": source,
                        "state": "unknown",
                        "reason": "transaction_receipt_unavailable",
                        "reasons": records[source].get("reasons", []),
                    }
                )
    except Exception:
        known = False
        changes.append(
            {
                "source": "webui.plugin",
                "state": "unknown",
                "reason": "transaction_record_unreadable",
                "reasons": [],
            }
        )
    try:
        from zhenxun.services.plugin_host import plugin_host

        covered = {reason for item in changes for reason in item.get("reasons", [])}
        reasons = set(plugin_host.pending_restart) - covered
        if reasons:
            changes.append(
                {
                    "source": "runtime.plugins",
                    "state": "restart_pending",
                    "reason": "plugin_change_requires_restart",
                    "reasons": sorted(reasons),
                }
            )
    except Exception:
        known = False
    unique = {}
    for item in changes:
        key = (item["source"], item.get("operation_id"), item.get("revision"))
        unique[key] = item
    return list(unique.values()), known


def restart_status_data(*, access_urls: list[str] | None = None) -> dict[str, Any]:
    tls = runtime_webui_settings()
    desired = load_webui_tls_settings()
    urls = _current_access_urls()
    target_urls = list(
        dict.fromkeys(
            [
                *(access_urls or []),
                *preferred_access_urls(
                    getattr(desired, "host", "127.0.0.1"),
                    getattr(desired, "port", 8080),
                    settings=desired,
                    http_available=True,
                ),
            ]
        )
    )
    targets = access_targets(urls)
    changes, status_known = restart_changes()
    pending_items = [item for item in changes if item["state"] == "restart_pending"]
    pending_reasons = {
        str(reason) for item in pending_items for reason in item.get("reasons", [])
    }
    return {
        "launcher_managed": bool(os.getenv("ZHENXUN_LAUNCHER_PID")),
        "boot_id": console_access.current_boot_id(),
        "worker_boot_id": startup_coordinator.boot_id,
        "access_urls": urls,
        "access_targets": targets,
        "preferred_url": urls[0] if urls else None,
        "scheme": tls.scheme,
        "https_enabled": tls.enabled,
        "http_mode": tls.effective_http_mode,
        "http_port": tls.redirect_port if tls.http_compat_enabled else None,
        **network_configuration_status(),
        "target_access_urls": target_urls,
        "target_access_targets": access_targets(target_urls),
        "pending_restart": bool(pending_items),
        "pending_reasons": sorted(pending_reasons),
        "pending_count": len(pending_items),
        "pending_items": pending_items,
        "change_items": changes,
        "status_known": status_known,
        "reconciliation_pending": any(
            item["state"] in {"preparing", "applying", "verifying"} for item in changes
        ),
        **transaction_verification_status(),
    }


async def request_webui_restart(
    source: str,
    *,
    require_ticket: str | None = None,
    access_urls: list[str] | None = None,
) -> tuple[bool, str, dict[str, Any]]:
    status = restart_status_data(access_urls=access_urls)
    # A restart reply describes where the next worker will listen. Polling status
    # continues to expose only the current worker's effective addresses.
    status["access_urls"] = status["target_access_urls"]
    status["access_targets"] = status["target_access_targets"]
    status["preferred_url"] = next(iter(status["access_urls"]), None)
    if not status["launcher_managed"]:
        return False, "当前不是 launcher 托管模式，请手动重启真寻。", status
    from zhenxun.services.nonebot_store.storage import (
        dependency_sync_status,
        save_dependency_sync_status,
    )

    sync = dependency_sync_status()
    if sync.get("status") == "prepared":
        from zhenxun.services.nonebot_store.dependencies import (
            environment_report_async,
            guard_orm_sync,
        )
        from zhenxun.services.runtime_mutation import (
            StoreOperationBusyError,
            store_operation,
        )
        from zhenxun.utils._restart_utils import request_dependency_restart

        try:
            async with store_operation(
                operation_id=sync.get("operation_id"), owner="webui.dependency-restart"
            ):
                report = await environment_report_async()
                if dependency_sync_status() != sync or report[
                    "fingerprint"
                ] != sync.get("fingerprint"):
                    return False, "environment_analysis_stale", status
                guard_orm_sync()
                save_dependency_sync_status({**sync, "status": "pending"})
                ok, message = await request_dependency_restart(
                    source, {Path("uv.lock"), Path("pyproject.toml")}
                )
                if not ok:
                    save_dependency_sync_status(sync)
        except StoreOperationBusyError:
            return False, "plugin_operation_in_progress", status
        except Exception as error:
            return False, str(error), status
    else:
        ok, message = await request_restart(source, require_ticket=require_ticket)
    if ok:
        from .security import quiesce_authenticated_websockets

        await quiesce_authenticated_websockets(timeout=0.75)
    return ok, message, status


__all__ = [
    "access_targets",
    "preferred_access_targets",
    "preferred_access_urls",
    "request_webui_restart",
    "restart_status_data",
    "transaction_verification_status",
]
