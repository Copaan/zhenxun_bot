"""Manual package requests submitted through managed store transactions."""

from __future__ import annotations

import asyncio
import contextlib
from copy import deepcopy
from hashlib import sha256
import json
import os

from zhenxun.services.nonebot_store.dependencies import (
    DependencyAnalysisError,
    environment_report_async,
    safe_process_error,
)
from zhenxun.services.nonebot_store.manual import (
    preview_manual_dependencies,
    request_digest,
)
from zhenxun.services.nonebot_store.runtime import (
    prepare_generation,
    stage_generation,
)
from zhenxun.services.nonebot_store.storage import (
    active_transaction as pending_transaction,
)
from zhenxun.services.nonebot_store.storage import (
    defer_transaction,
    draft_transaction,
    load_manifest,
    remove_generation,
    save_manifest,
    utc_now,
)
from zhenxun.services.plugin_store.operation_journal import record_operation

from ....apply_result import update_pending_restart
from ....base_model import Result
from .model import InstallDependenciesPayload
from .store import (
    PreviewStore,
    StoreOperationBusyError,
    _store_operation,
    replayed_bound_operation,
    run_bound_operation,
)

_PREVIEWS = PreviewStore()
_OWNER = "dependencies:manual"


def _revision(transaction: dict | None) -> str:
    return str((transaction or {}).get("revision") or "")


def _failure(error: Exception) -> Result:
    return Result.fail(
        getattr(error, "code", type(error).__name__),
        code=409,
        details={
            "stage": "dependency_transaction",
            "diagnostic": safe_process_error(str(error)),
            "issues": getattr(error, "details", None),
        },
    )


async def preview(payload: InstallDependenciesPayload) -> Result:
    """Bind a read-only package plan to the active and pending generations."""
    try:
        from zhenxun.services.nonebot_store.storage import dependency_sync_status

        if dependency_sync_status().get("status") in {"prepared", "pending", "running"}:
            return Result.fail("dependency_transaction_pending", code=409)
        pending = pending_transaction()
        if pending and pending.get("state") != "pending_restart":
            return Result.fail("dependency_transaction_pending", code=409)
        manifest = (pending or {}).get("target_manifest") or load_manifest()
        plan = await preview_manual_dependencies(
            payload.dependencies,
            payload.handle_type,
            manifest=manifest,
        )
        if pending and plan.get("declaration_only"):
            plan.update(declaration_only=False, restart_required=True)
            from zhenxun.services.nonebot_store.policy import summary_revision

            plan["summary_revision"] = summary_revision(plan)
        if _revision(pending_transaction()) != _revision(pending):
            return Result.fail("environment_analysis_stale", code=409)
        if (await environment_report_async())["fingerprint"] != plan["fingerprint"]:
            return Result.fail("environment_analysis_stale", code=409)
        identity = _PREVIEWS.put({**plan, "pending_revision": _revision(pending)})
        return Result.ok(
            {
                **{
                    key: value
                    for key, value in plan.items()
                    if key != "target_manifest"
                },
                "preview_id": identity,
            }
        )
    except Exception as error:
        return _failure(error)


def _binding(payload: InstallDependenciesPayload) -> str:
    return sha256(
        json.dumps(
            [
                request_digest(payload.handle_type, payload.dependencies),
                payload.preview_id,
                payload.expected_fingerprint,
                payload.confirmed,
                payload.confirm_source_build,
                payload.summary_revision,
            ],
            sort_keys=True,
        ).encode()
    ).hexdigest()


async def apply(payload: InstallDependenciesPayload) -> Result:
    """Keep accepted work alive across request cancellation, keyed by operation ID."""
    if not payload.preview_id or not payload.expected_fingerprint:
        return Result.fail("dependency_preview_required", code=409)
    if not payload.operation_id or not payload.confirmed:
        return Result.fail("dependency_confirmation_required", code=400)
    try:
        binding = _binding(payload)
    except DependencyAnalysisError as error:
        return _failure(error)
    return await run_bound_operation(
        payload.operation_id, binding, lambda: _apply(payload, binding)
    )


async def _apply(payload: InstallDependenciesPayload, binding: str) -> Result:
    from .nonebot_store import _operation_result

    identity = str(payload.operation_id)
    built = None
    recorded = False
    staged = False

    def record(result: dict) -> None:
        record_operation(
            _OWNER, {**result, "operation_id": identity, "request_binding": binding}
        )

    try:
        async with _store_operation(
            operation_id=identity, owner="webui.manual-dependencies"
        ):
            if replayed := replayed_bound_operation(identity, binding):
                return replayed
            plan = _PREVIEWS.get(str(payload.preview_id))
            pending = pending_transaction()
            if not plan:
                return Result.fail("environment_analysis_stale", code=409)
            if (
                plan["request_digest"]
                != request_digest(payload.handle_type, payload.dependencies)
                or plan["fingerprint"] != payload.expected_fingerprint
                or _revision(pending) != plan["pending_revision"]
                or (await environment_report_async())["fingerprint"]
                != plan["fingerprint"]
            ):
                return Result.fail("environment_analysis_stale", code=409)
            if (
                payload.summary_revision is not None
                and payload.summary_revision != plan.get("summary_revision")
            ):
                return Result.fail("environment_analysis_stale", code=409)
            if pending and pending.get("state") != "pending_restart":
                return Result.fail("dependency_transaction_pending", code=409)
            if plan["source_build_required"] and not payload.confirm_source_build:
                return Result.fail("source_build_confirmation_required", code=400)
            if plan["no_change"]:
                result = _operation_result(
                    "no_change",
                    [],
                    operation_id=identity,
                    status="completed",
                    changes=[],
                    retained=plan["retained"],
                )
                record(result)
                return Result.ok(result, info="依赖及托管声明均无需变更")
            if plan.get("declaration_only") and not pending:
                record(
                    {
                        "status": "running",
                        "apply_mode": "preparing",
                        "action": payload.handle_type,
                    }
                )
                target = deepcopy(plan["target_manifest"])
                result = _operation_result(
                    "applied",
                    ["declaration_updated"],
                    operation_id=identity,
                    status="completed",
                    changes=[],
                    retained=plan["retained"],
                    restart_required=False,
                )
                target.setdefault("declaration_receipts", {})[identity] = {
                    **result,
                    "request_binding": binding,
                }
                save_manifest(target)
                try:
                    record(result)
                except OSError as error:
                    result["cleanup_error"] = {
                        "code": "operation_receipt_pending",
                        "diagnostic": safe_process_error(str(error)),
                    }
                _PREVIEWS.pop(str(payload.preview_id))
                return Result.ok(result, info="托管声明已保存，依赖未改变，无需重启")
            if not os.environ.get("ZHENXUN_LAUNCHER_PID"):
                return Result.fail("dependency_repair_requires_launcher", code=409)
            record(
                {
                    "status": "running",
                    "apply_mode": "preparing",
                    "action": payload.handle_type,
                }
            )
            recorded = True
            transaction = draft_transaction(
                pending,
                plan["target_manifest"],
                action="manual_dependencies",
                remaining_conflicts=deepcopy(plan.get("remaining_conflicts", [])),
                state="pending_restart",
                source_build_confirmed=bool(
                    payload.confirm_source_build
                    or (pending or {}).get("source_build_confirmed")
                ),
            )
            operation = {
                "operation_id": identity,
                "kind": "manual_dependencies",
                "store_key": _OWNER,
                "action": payload.handle_type,
                "name": ", ".join(payload.dependencies),
                "created_at": utc_now(),
                "target_manifest": deepcopy(plan["target_manifest"]),
                "request_binding": binding,
                "database_migration_possible": False,
                "source_build_confirmed": payload.confirm_source_build,
            }
            transaction["operations"].append(operation)
            if plan["source_build_required"] or (
                pending and not isinstance(pending.get("generation"), int)
            ):
                defer_transaction(transaction)
                staged = True
            else:
                built = await prepare_generation(transaction)
                if (await environment_report_async())["fingerprint"] != plan[
                    "fingerprint"
                ]:
                    raise DependencyAnalysisError("environment_analysis_stale")
                stage_generation(transaction, built)
                staged = True
            update_pending_restart(
                "webui.nonebot-store",
                ["manual_dependencies_changed"],
            )
            result = _operation_result(
                "restart_pending",
                ["manual_dependencies_changed"],
                operation_id=identity,
                status="pending",
                transaction_revision=transaction["revision"],
                changes=plan["changes"],
                retained=plan["retained"],
            )
            record(result)
            _PREVIEWS.pop(str(payload.preview_id))
            return Result.ok(result, info="依赖事务已准备，重启验证后生效")
    except StoreOperationBusyError:
        return Result.fail("plugin_operation_in_progress", code=409)
    except asyncio.CancelledError:
        if built is not None and not staged:
            remove_generation(built["generation"])
        if recorded:
            with contextlib.suppress(Exception):
                record(
                    {
                        "status": "interrupted",
                        "apply_mode": "restart_pending" if staged else "failed",
                        "reason": "dependency_operation_interrupted",
                    }
                )
        raise
    except Exception as error:
        if built is not None and not staged:
            remove_generation(built["generation"])
        if recorded:
            with contextlib.suppress(Exception):
                record(
                    {
                        "status": "pending"
                        if staged
                        else "interrupted"
                        if getattr(error, "code", "")
                        == "dependency_operation_interrupted"
                        else "failed",
                        "apply_mode": "restart_pending" if staged else "failed",
                        "reason": getattr(error, "code", type(error).__name__),
                        "diagnostic": safe_process_error(str(error)),
                    }
                )
        if staged:
            return Result.ok(
                {
                    "operation_id": identity,
                    "status": "pending",
                    "apply_mode": "restart_pending",
                    "diagnostic": safe_process_error(str(error)),
                    "reason": "dependency_followup_required",
                },
                info="依赖事务已保存；收尾失败，请查询原操作，不要重复提交",
            )
        return _failure(error)
