"""Persistent store operation results shared by workers and the launcher."""

from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
import uuid

from zhenxun.utils.atomic_json import mutate_json_locked, read_json_locked

_JOURNAL_FILE = Path("data/runtime/plugin-store-operations-v1.json")


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _boot_id() -> str:
    """Identify this worker process for journal reconciliation.

    Imported lazily so the journal stays usable in contexts that never start a
    worker, such as the launcher applying a pending transaction.
    """
    try:
        from zhenxun.services.startup import startup_coordinator

        return str(startup_coordinator.boot_id or "")
    except Exception:
        return ""


def _reconciled(value: dict[str, Any]) -> dict[str, Any]:
    """Reconcile durable generation state before detecting an abandoned worker."""
    if value.get("store_key") == "dependencies:manual":
        from zhenxun.services.nonebot_store.storage import load_manifest

        receipt = (
            load_manifest()
            .get("declaration_receipts", {})
            .get(value.get("operation_id"))
        )
        if isinstance(receipt, dict):
            value.update(
                status="completed", apply_mode="applied", result=deepcopy(receipt)
            )
            return value
    if value.get("store_key") == "dependencies:repair":
        from zhenxun.services.nonebot_store.storage import dependency_sync_status

        sync = dependency_sync_status()
        if sync.get("operation_id") == value.get("operation_id"):
            state = sync.get("status")
            mode, status = "verifying", "running"
            if state == "prepared":
                mode, status = "restart_pending", "pending"
            elif state == "canceled":
                mode, status = "rolled_back", "canceled"
            elif state == "failed":
                mode, status = "failed", "failed"
            elif state == "succeeded":
                from zhenxun.services.startup import startup_coordinator

                if startup_coordinator.runtime_ready:
                    mode, status = "applied", "completed"
            value["status"], value["apply_mode"] = status, mode
            value.setdefault("result", {}).update(status=status, apply_mode=mode)
            if sync.get("code"):
                value["result"]["reason"] = sync["code"]
            return value
    if value.get("store_key") in {"dependencies:manual", "dependencies:repair"} or str(
        value.get("store_key") or ""
    ).startswith("nonebot:"):
        from zhenxun.services.nonebot_store.storage import pending_transaction

        transaction = pending_transaction() or {}
        if not any(
            item.get("operation_id") == value.get("operation_id")
            for item in transaction.get("operations", [])
        ):
            from zhenxun.services.nonebot_store.cleanup import transaction_for_operation

            transaction = (
                transaction_for_operation(str(value.get("operation_id"))) or {}
            )
        if any(
            item.get("operation_id") == value.get("operation_id")
            for item in transaction.get("operations", [])
        ):
            state = transaction.get("state")
            mode, status = {
                "pending_restart": ("restart_pending", "pending"),
                "migration_blocked": ("restart_pending", "pending"),
                "building": ("preparing", "running"),
                "verification_pending": ("verifying", "running"),
                "recovery_required": ("recovery_required", "recovery_required"),
                "failed": (
                    "rolled_back"
                    if transaction.get("rollback_confirmed")
                    else "failed",
                    "failed",
                ),
                "committed": ("applied", "completed"),
                "cleanup_pending": ("applied", "completed"),
            }.get(state, (value.get("apply_mode"), value.get("status")))
            value["status"], value["apply_mode"] = status, mode
            value.setdefault("result", {}).update(status=status, apply_mode=mode)
            value["result"]["transaction_state"] = state
            if state == "migration_blocked":
                value["result"].update(
                    reason="external_database_manual_migration_required",
                    restart_available=False,
                )
            if transaction.get("cleanup_error"):
                value["result"]["cleanup_error"] = transaction["cleanup_error"]
            elif transaction.get("cleanup_state") == "completed":
                value["result"].pop("cleanup_error", None)
            if state != "building":
                return value
    if value.get("status") != "running":
        return value
    boot_id = _boot_id()
    if not boot_id or value.get("boot_id") in {None, "", boot_id}:
        return value
    value["status"] = "interrupted"
    result = value.get("result")
    if isinstance(result, dict):
        result["status"] = "interrupted"
        result.setdefault("reason", "plugin_operation_interrupted")
    return value


def begin_operation(
    store_key: str,
    action: str,
    operation_id: str | None = None,
    *,
    download_source: str = "auto",
) -> str:
    operation_id = operation_id or uuid.uuid4().hex
    record_operation(
        store_key,
        {
            "operation_id": operation_id,
            "status": "running",
            "action": action,
            "download_source": download_source,
            "apply_mode": None,
        },
    )
    return operation_id


def record_operation(store_key: str, result: dict[str, Any]) -> str:
    operation_id = str(result.get("operation_id") or uuid.uuid4().hex)
    result["operation_id"] = operation_id

    def update(state: dict[str, Any]) -> None:
        operations = state.setdefault("operations", {})
        prior = operations.get(operation_id, {}).get("result", {})
        for key in ("download_source", "request_binding"):
            if key in prior and key not in result:
                result[key] = prior[key]
        operations[operation_id] = {
            "operation_id": operation_id,
            "store_key": store_key,
            "status": result.get("status") or "completed",
            "apply_mode": result.get("apply_mode"),
            "result": deepcopy(result),
            "updated_at": _now(),
            # Recorded so a ``running`` entry left behind by a crash can be told
            # apart from one that is genuinely in flight in this process.
            "boot_id": _boot_id(),
        }
        order = [item for item in state.get("order", []) if item != operation_id]
        order.append(operation_id)
        state["order"] = order[-100:]
        for stale in set(operations) - set(state["order"]):
            operations.pop(stale, None)

    mutate_json_locked(_JOURNAL_FILE, {}, update)
    return operation_id


def operation_status(operation_id: str) -> dict[str, Any] | None:
    state = read_json_locked(_JOURNAL_FILE, {})
    value = state.get("operations", {}).get(operation_id)
    return _reconciled(deepcopy(value)) if isinstance(value, dict) else None


def current_operation() -> dict[str, Any] | None:
    state = read_json_locked(_JOURNAL_FILE, {})
    order = state.get("order", [])
    if not isinstance(order, list) or not order:
        return None
    value = state.get("operations", {}).get(str(order[-1]))
    return _reconciled(deepcopy(value)) if isinstance(value, dict) else None


__all__ = [
    "begin_operation",
    "current_operation",
    "operation_status",
    "record_operation",
]
