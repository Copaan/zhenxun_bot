from __future__ import annotations

import asyncio
from hashlib import sha256
from urllib.parse import urlsplit

from fastapi import APIRouter, Depends, HTTPException, Query, Request

from zhenxun import plugin_archive as service
from zhenxun.plugin_archive_dependencies import ArchiveDependencyConflict
from zhenxun.plugin_store_coordinator import (
    StoreOperationBusyError,
    plugin_store_operation_coordinator,
)
from zhenxun.plugin_store_receipts import source_digest
from zhenxun.plugin_store_transaction import ArchiveSourceBuildConflict, stage_operation

from ....apply_result import update_pending_restart
from ....base_model import Result
from ....security import decode_access_token_status
from ....utils import authentication, oauth2_scheme
from .model import ArchiveActionPayload, ArchiveConfirmPayload
from .operation_journal import operation_status, record_operation


async def archive_session(request: Request, token: str = Depends(oauth2_scheme)) -> str:
    origin = request.headers.get("origin", "")
    try:
        parsed = urlsplit(origin)
        actual = urlsplit(str(request.base_url))

        def port(value):
            return value.port or (443 if value.scheme == "https" else 80)

        valid_origin = (
            parsed.scheme in {"http", "https"}
            and not parsed.username
            and not parsed.password
            and not parsed.path
            and not parsed.query
            and not parsed.fragment
            and parsed.hostname == actual.hostname
            and parsed.scheme == actual.scheme
            and port(parsed) == port(actual)
        )
    except ValueError:
        valid_origin = False
    if not valid_origin:
        raise HTTPException(403, "archive_origin_forbidden")
    claims, _ = decode_access_token_status(token)
    if not claims or not claims.get("sid"):
        raise HTTPException(401, "archive_session_required")
    return str(claims["sid"])


router = APIRouter(prefix="/archive", dependencies=[authentication()])


def _http_error(error: Exception) -> HTTPException:
    if isinstance(error, ArchiveDependencyConflict | ArchiveSourceBuildConflict):
        return HTTPException(409, error.code)
    if isinstance(error, service.ArchiveError):
        if error.candidates:
            return HTTPException(
                error.status, {"code": error.code, "candidates": error.candidates}
            )
        return HTTPException(error.status, error.code)
    if isinstance(error, StoreOperationBusyError):
        return HTTPException(409, "plugin_operation_in_progress")
    return HTTPException(400, "archive_operation_failed")


@router.post("/preflight", response_model=Result[dict])
async def preflight(
    request: Request,
    filename: str = Query(min_length=1, max_length=255),
    session: str = Depends(archive_session),
) -> Result[dict]:
    try:
        length = request.headers.get("content-length")
        if length and int(length) > service.MAX_COMPRESSED:
            raise service.ArchiveError("archive_upload_limit", 413)
        result = await asyncio.wait_for(
            service.preflight(request.stream(), filename, session),
            timeout=300,
        )
        return Result.ok(result)
    except Exception as error:
        raise _http_error(error) from error


@router.post("/preflight/{preflight_id}/confirm", response_model=Result[dict])
async def confirm(
    preflight_id: str,
    payload: ArchiveConfirmPayload,
    session: str = Depends(archive_session),
) -> Result[dict]:
    try:
        async with plugin_store_operation_coordinator.operation(
            owner="webui.plugin_archive"
        ):
            result = service.confirm(
                preflight_id,
                session,
                payload.archive_digest,
                replace=payload.replace,
                trusted=payload.confirm_third_party_code,
            )
            if result.get("restart_required"):
                update_pending_restart(
                    f"webui.plugin:{result['store_key']}",
                    ["local_archive_install"],
                    issue_ticket=False,
                )
        return Result.ok(result, "归档安装已暂存，重启验证后生效。")
    except Exception as error:
        raise _http_error(error) from error


@router.delete("/preflight/{preflight_id}", response_model=Result)
async def delete(preflight_id: str, session: str = Depends(archive_session)) -> Result:
    try:
        async with plugin_store_operation_coordinator.operation(
            owner="webui.plugin_archive"
        ):
            service.delete_preflight(preflight_id, session)
        return Result.ok()
    except Exception as error:
        raise _http_error(error) from error


@router.get("/installed", response_model=Result[list[dict]])
async def installed() -> Result[list[dict]]:
    return Result.ok(service.archive_receipts())


async def _action(payload: ArchiveActionPayload, action: str, session: str) -> dict:
    if not payload.confirmed:
        raise service.ArchiveError("archive_confirmation_required")
    async with plugin_store_operation_coordinator.operation(
        operation_id=payload.operation_id,
        owner="webui.plugin_archive",
    ):
        previous = operation_status(payload.operation_id)
        if previous:
            result = previous.get("result", {})
            if (
                previous.get("store_key") != payload.store_key
                or result.get("archive_action") != action
                or result.get("archive_owner") != sha256(session.encode()).hexdigest()
                or result.get("expected_digest") != payload.expected_digest
            ):
                raise service.ArchiveError("plugin_operation_id_conflict", 409)
            if previous.get("status") == "completed":
                return service.operation_view(
                    {k: v for k, v in result.items() if k != "archive_owner"}
                )
            pending = service._pending_source()
            if any(
                op.get("operation_id") == payload.operation_id
                for op in pending.get("operations", [])
            ):
                if pending.get("state") != "pending_restart":
                    raise service.ArchiveError("plugin_transaction_not_mutable", 409)
                result.update(
                    status="completed",
                    apply_mode="restart_pending",
                    restart_required=True,
                )
                record_operation(payload.store_key, result)
                return service.operation_view(
                    {k: v for k, v in result.items() if k != "archive_owner"}
                )
        target, receipt = service.managed_archive(payload.store_key)
        if source_digest(target) != payload.expected_digest:
            raise service.ArchiveError("archive_target_changed", 409)
        if action == "load" and payload.expected_digest != receipt.get("source_digest"):
            raise service.ArchiveError("archive_target_changed", 409)
        binding = {
            "operation_id": payload.operation_id,
            "action": action,
            "archive_action": action,
            "archive_owner": sha256(session.encode()).hexdigest(),
            "expected_digest": payload.expected_digest,
        }
        record_operation(payload.store_key, {**binding, "status": "running"})
        # Both load and uninstall are launcher transactions, not live imports.
        try:
            result = stage_operation(
                action="uninstall" if action == "uninstall" else "update",
                store_key=payload.store_key,
                module=receipt["module"],
                runtime_module=receipt["runtime_module"],
                live_path=target,
                candidate_path=None if action == "uninstall" else target,
                receipt=None if action == "uninstall" else receipt,
                base_digest=payload.expected_digest,
                reason=f"local_archive_{action}",
                operation_id=payload.operation_id,
            )
        except Exception:
            record_operation(payload.store_key, {**binding, "status": "failed"})
            raise
        result.update(archive_action=action, status="completed", restart_required=True)
        result["expected_digest"] = payload.expected_digest
        record_operation(
            payload.store_key,
            {**result, "archive_owner": sha256(session.encode()).hexdigest()},
        )
        update_pending_restart(
            f"webui.plugin:{payload.store_key}",
            [f"local_archive_{action}"],
            issue_ticket=False,
        )
        return result


@router.post("/load", response_model=Result[dict])
async def load(
    payload: ArchiveActionPayload, session: str = Depends(archive_session)
) -> Result[dict]:
    try:
        return Result.ok(await _action(payload, "load", session))
    except Exception as error:
        raise _http_error(error) from error


@router.post("/uninstall", response_model=Result[dict])
async def uninstall(
    payload: ArchiveActionPayload, session: str = Depends(archive_session)
) -> Result[dict]:
    try:
        return Result.ok(await _action(payload, "uninstall", session))
    except Exception as error:
        raise _http_error(error) from error


def decorate_archive_plugin(plugin):
    key = plugin.store_key or ""
    if key.startswith("local_archive:"):
        receipt = service.StoreReceiptStore.load().get(key, {})
        plugin.management_source = "local_archive"
        plugin.management_route = "/plugin?archives=1"
        plugin.archive_digest = receipt.get("archive_digest")
        # The legacy list's uninstall button is wired to the catalog-only endpoint.
        plugin.uninstall_supported = False
        plugin.uninstall_reason = "请在外部归档管理中卸载"
    return plugin
