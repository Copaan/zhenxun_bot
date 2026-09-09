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
        for key in ("download_source",):
            if key in prior and key not in result:
                result[key] = prior[key]
        operations[operation_id] = {
            "operation_id": operation_id,
            "store_key": store_key,
            "status": result.get("status") or "completed",
            "apply_mode": result.get("apply_mode"),
            "result": deepcopy(result),
            "updated_at": _now(),
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
    return deepcopy(value) if isinstance(value, dict) else None


def current_operation() -> dict[str, Any] | None:
    state = read_json_locked(_JOURNAL_FILE, {})
    order = state.get("order", [])
    if not isinstance(order, list) or not order:
        return None
    value = state.get("operations", {}).get(str(order[-1]))
    return deepcopy(value) if isinstance(value, dict) else None


__all__ = [
    "begin_operation",
    "current_operation",
    "operation_status",
    "record_operation",
]
