from __future__ import annotations

import asyncio
from copy import deepcopy
from datetime import datetime, timedelta, timezone
import importlib.metadata
import os
from pathlib import Path
from typing import Any, Literal
import uuid

from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse
from packaging.utils import canonicalize_name
from packaging.version import InvalidVersion, Version
from pydantic import BaseModel, Field

from zhenxun.nonebot_store.dependencies import (
    DependencyAnalysisError,
    environment_fingerprint,
    environment_report,
    fetch_pypi_metadata,
    installed_inventory,
    metadata_compatibility,
    preflight_environment_repair,
    protected_core,
    safe_process_error,
    solve_install,
    uninstall_plan,
)
from zhenxun.nonebot_store.registry import get_registry, get_registry_plugin
from zhenxun.nonebot_store.runtime import (
    LayerBuildError,
    activate_current_generation,
    build_generation,
    commit_generation,
    finalize_orm_migration,
    finalize_pending_transaction,
    generation_native_extensions,
    module_source_path,
    rollback_pending_transaction,
    stage_generation,
)
from zhenxun.nonebot_store.storage import (
    clear_pending_transaction,
    dependency_sync_status,
    load_manifest,
    pending_transaction,
    remove_generation,
    save_dependency_sync_status,
    save_pending_transaction,
    utc_now,
)
from zhenxun.plugin_archive_dependencies import (
    ArchiveDependencyConflict,
    preserve_archive_dependencies,
)
from zhenxun.plugin_store_transaction import (
    ArchiveSourceBuildConflict,
    archive_dependency_policy,
)
from zhenxun.services.lifecycle.operations import operation_registry
from zhenxun.services.log import logger
from zhenxun.services.runtime_reload import plugin_runtime_manager

from ....apply_result import update_pending_restart
from ....base_model import Result
from ....restart_service import restart_status_data
from ....utils import authentication
from .operation_journal import begin_operation, record_operation
from .store import StoreOperationBusyError, _store_operation

router = APIRouter(prefix="/store/nonebot")
_ANALYSIS_TTL = timedelta(minutes=20)
_ANALYSES: dict[str, dict[str, Any]] = {}
_PENDING_TRANSACTION_STATES = {
    "building",
    "pending_restart",
    "verification_pending",
    "migration_blocked",
}


class AnalyzePayload(BaseModel):
    project_link: str = Field(min_length=1, max_length=200)
    action: Literal["install", "update", "uninstall"]


class ApplyPayload(BaseModel):
    analysis_id: str = Field(min_length=32, max_length=64)
    operation_id: str | None = Field(default=None, pattern=r"^[0-9a-f]{32}$")
    confirm_non_core_changes: bool = False
    confirm_source_build: bool = False
    confirm_third_party_code: bool = False
    confirm_compatibility_overrides: bool = False
    confirm_database_migration: bool = False


class EnvironmentRepairPayload(BaseModel):
    expected_fingerprint: str = Field(min_length=64, max_length=64)
    confirmed: bool = False


def _environment_view() -> dict[str, Any]:
    try:
        result = environment_report(check_lock=True)
    except DependencyAnalysisError as error:
        result = {
            "status": (
                "project_lock_stale"
                if error.code in {"project_lock_missing", "project_metadata_missing"}
                else "failed"
            ),
            "fingerprint": "",
            "repairable": False,
            "immutable_drift": [],
            "compatible_shared_drift": [],
            "incompatible_shared_drift": [],
            "extra_packages": [],
            "extra_count": 0,
            "error_code": error.code,
        }
    except Exception as error:
        result = {
            "status": "failed",
            "fingerprint": "",
            "repairable": False,
            "immutable_drift": [],
            "compatible_shared_drift": [],
            "incompatible_shared_drift": [],
            "extra_packages": [],
            "extra_count": 0,
            "error_code": type(error).__name__,
        }
    result["sync"] = dependency_sync_status()
    result["launcher_managed"] = bool(os.environ.get("ZHENXUN_LAUNCHER_PID"))
    return result


def _enabled_adapters() -> set[str]:
    from zhenxun.configs.config import BotConfig

    enabled = {"nonebot.adapters.onebot.v11"}
    if BotConfig.qq_adapter_load:
        enabled.add("nonebot.adapters.qq")
    return enabled


def _basic_block_reasons(plugin: dict[str, Any]) -> list[dict[str, str]]:
    reasons: list[dict[str, str]] = []
    for code in plugin.get("registry_validation_errors") or []:
        reasons.append(
            {
                "code": str(code),
                "message": "Registry 模块名不是有效的 Python 导入路径",
            }
        )
    if not plugin.get("valid"):
        reasons.append(
            {"code": "registry_plugin_invalid", "message": "Registry 标记为无效"}
        )
    if plugin.get("skip_test"):
        reasons.append(
            {"code": "registry_plugin_untested", "message": "Registry 未测试此版本"}
        )
    supported = plugin.get("supported_adapters")
    if isinstance(supported, list) and supported:
        declared = {str(item) for item in supported}
        if not declared & _enabled_adapters():
            reasons.append(
                {
                    "code": "adapter_incompatible",
                    "message": "插件声明的适配器与当前启用适配器不匹配",
                }
            )
    return reasons


def _managed_plugin(
    project_link: str, manifest: dict[str, Any] | None = None
) -> dict[str, Any] | None:
    value = (
        (manifest or load_manifest()).get("plugins", {}).get(f"nonebot:{project_link}")
    )
    return value if isinstance(value, dict) else None


def _pending_revision(transaction: dict[str, Any] | None = None) -> str:
    transaction = transaction if transaction is not None else pending_transaction()
    if not isinstance(transaction, dict):
        return ""
    return str(transaction.get("revision") or "")


def _effective_manifest(
    transaction: dict[str, Any] | None = None,
) -> dict[str, Any]:
    transaction = transaction if transaction is not None else pending_transaction()
    if isinstance(transaction, dict) and transaction.get("state") in {
        "building",
        "pending_restart",
    }:
        target = transaction.get("target_manifest")
        if isinstance(target, dict):
            return deepcopy(target)
    return load_manifest()


def _external_version(
    project_link: str, inventory: dict[str, str] | None = None
) -> str | None:
    if inventory is not None:
        return inventory.get(canonicalize_name(project_link))
    try:
        return importlib.metadata.version(project_link)
    except importlib.metadata.PackageNotFoundError:
        return None


def _version_is_newer(current: str, target: str) -> bool:
    try:
        return Version(target) > Version(current)
    except InvalidVersion:
        return current != target


def _catalog_item(
    plugin: dict[str, Any],
    *,
    manifest: dict[str, Any] | None = None,
    inventory: dict[str, str] | None = None,
    pending: dict[str, Any] | None = None,
) -> dict[str, Any]:
    project_link = str(plugin["project_link"])
    managed = _managed_plugin(project_link, manifest)
    external = None if managed else _external_version(project_link, inventory)
    pending_operations = pending.get("operations", []) if pending else []
    matching_operation = next(
        (
            item
            for item in pending_operations
            if isinstance(item, dict) and item.get("project_link") == project_link
        ),
        None,
    )
    matching_transaction = bool(
        pending and (matching_operation or pending.get("project_link") == project_link)
    )
    transaction_state = (
        str(pending.get("state") or "") if matching_transaction and pending else ""
    )
    pending_action = (
        str((matching_operation or pending).get("action"))
        if matching_transaction and transaction_state in _PENDING_TRANSACTION_STATES
        else None
    )
    pending_reasons = []
    if matching_transaction and pending:
        error_code = str(pending.get("error_code") or "")
        if error_code:
            pending_reasons.append({"code": error_code})
    reasons = _basic_block_reasons(plugin)
    failure_reasons: list[dict[str, Any]] = []
    if matching_transaction and transaction_state == "failed" and pending:
        for failure in pending.get("failure_reasons") or []:
            if not isinstance(failure, dict):
                continue
            failure_store_key = str(failure.get("store_key") or "")
            if failure_store_key and failure_store_key != f"nonebot:{project_link}":
                continue
            failure_reasons.append(
                {
                    "code": str(failure.get("code") or "plugin_import_failed"),
                    "store_key": failure_store_key or None,
                    "module_name": str(failure.get("module_name") or "") or None,
                    "paths": [
                        str(path)
                        for path in failure.get("paths") or []
                        if isinstance(path, str)
                    ][:20],
                }
            )
        if not failure_reasons:
            failure_reasons.append(
                {
                    "code": str(
                        pending.get("error_code") or "nonebot_plugin_apply_failed"
                    ),
                    "paths": [],
                }
            )
    update_available = bool(
        managed
        and _version_is_newer(str(managed.get("version", "0")), str(plugin["version"]))
    )
    compatibility_overrides = (
        deepcopy(managed.get("compatibility_overrides") or []) if managed else []
    )
    effective_inventory = inventory or installed_inventory()
    compatibility_override_stale = any(
        effective_inventory.get(canonicalize_name(str(item.get("name") or "")))
        != str(item.get("effective_version") or "")
        for item in compatibility_overrides
        if isinstance(item, dict)
    )
    if failure_reasons:
        install_state = "failed"
    elif reasons:
        install_state = "blocked"
    elif managed:
        install_state = "update_available" if update_available else "managed"
    elif external:
        install_state = "external"
    else:
        install_state = "not_installed"
    module_name = str(plugin.get("module_name") or "")
    runtime = (
        plugin_runtime_manager.classification_for(module_name)
        if (managed or external) and module_name
        else {"reload_support": "hot_reloadable", "reload_reasons": []}
    )

    def capability(available: bool, reason: str) -> dict[str, Any]:
        if not available:
            return {"mode": "blocked", "reason_codes": [reason]}
        return {"mode": "analysis_required", "reason_codes": []}

    return {
        "store_key": f"nonebot:{project_link}",
        "project_link": project_link,
        "module_name": plugin.get("module_name"),
        "name": plugin.get("name") or project_link,
        "description": plugin.get("desc") or "",
        "author": plugin.get("author") or "",
        "homepage": plugin.get("homepage") or "",
        "tags": plugin.get("tags") or [],
        "is_official": bool(plugin.get("is_official")),
        "plugin_type": plugin.get("type") or "application",
        "supported_adapters": plugin.get("supported_adapters"),
        "valid": bool(plugin.get("valid")),
        "skip_test": bool(plugin.get("skip_test")),
        "updated_at": plugin.get("time"),
        "version": str(plugin.get("version") or ""),
        "installed_version": (str(managed.get("version")) if managed else external),
        "install_state": install_state,
        "update_available": update_available,
        "compatibility": "blocked" if reasons else "compatible",
        "blocked_reasons": reasons,
        "failure_reasons": failure_reasons,
        "pending_reasons": pending_reasons,
        "managed": bool(managed),
        "external": bool(external),
        "compatibility_overrides": compatibility_overrides,
        "compatibility_unverified": bool(compatibility_overrides),
        "compatibility_override_stale": compatibility_override_stale,
        "install_capability": capability(
            not managed and not external and not reasons,
            "plugin_incompatible" if reasons else "plugin_already_installed",
        ),
        "update_capability": capability(
            bool(managed and update_available),
            "plugin_update_not_available" if managed else "plugin_not_managed",
        ),
        "uninstall_capability": capability(
            bool(managed),
            "external_install_not_managed" if external else "plugin_not_managed",
        ),
        **runtime,
        "apply_mode": (
            "restart_pending"
            if pending_action
            else "failed"
            if failure_reasons
            else None
        ),
        "pending_action": pending_action,
        "pending_operation_id": (
            matching_operation.get("operation_id") if matching_operation else None
        ),
        "transaction_state": transaction_state or None,
    }


def _cleanup_analyses() -> None:
    threshold = datetime.now(timezone.utc) - _ANALYSIS_TTL
    for analysis_id, analysis in list(_ANALYSES.items()):
        try:
            created = datetime.fromisoformat(str(analysis["created_at"]))
        except (KeyError, TypeError, ValueError):
            created = threshold - timedelta(seconds=1)
        task = analysis.get("task")
        if created < threshold and not (
            isinstance(task, asyncio.Task) and not task.done()
        ):
            _ANALYSES.pop(analysis_id, None)


def _public_analysis(analysis: dict[str, Any]) -> dict[str, Any]:
    return {
        key: deepcopy(value)
        for key, value in analysis.items()
        if key not in {"metadata", "registry_plugin", "task", "base_manifest"}
    }


async def _analyze(analysis_id: str) -> None:
    analysis = _ANALYSES[analysis_id]
    analysis["status"] = "analyzing"
    try:
        plugin, registry_meta = await get_registry_plugin(analysis["project_link"])
        reasons = _basic_block_reasons(plugin)
        pending = pending_transaction()
        manifest = _effective_manifest(pending)
        pending_revision = _pending_revision(pending)
        inventory = installed_inventory()
        managed = _managed_plugin(analysis["project_link"], manifest)
        external = (
            None if managed else _external_version(analysis["project_link"], inventory)
        )
        action = analysis["action"]
        if action == "install" and managed:
            reasons.append({"code": "plugin_already_managed", "message": "插件已安装"})
        if action == "install" and external:
            reasons.append(
                {
                    "code": "external_install_not_managed",
                    "message": "同名分发包由 WebUI 外部安装，不能自动接管",
                }
            )
        if action in {"update", "uninstall"} and not managed:
            reasons.append(
                {"code": "plugin_not_managed", "message": "该插件不由 WebUI 管理"}
            )

        metadata: dict[str, Any] | None = None
        if action != "uninstall":
            metadata = await fetch_pypi_metadata(
                str(plugin["project_link"]), str(plugin["version"])
            )
            reasons.extend(metadata_compatibility(metadata))
        if reasons:
            analysis.update(
                {
                    "status": "blocked",
                    "compatible": False,
                    "blocked_reasons": reasons,
                    "registry_plugin": plugin,
                    "registry_cache": registry_meta,
                }
            )
            return

        plan = (
            uninstall_plan(managed or plugin, manifest=manifest)
            if action == "uninstall"
            else await solve_install(plugin, metadata or {}, manifest=manifest)
        )
        analysis.update(
            {
                "status": "ready",
                "compatible": True,
                "blocked_reasons": [],
                "registry_plugin": plugin,
                "metadata": metadata,
                "registry_cache": registry_meta,
                "fingerprint": environment_fingerprint(
                    plugin, pending_revision=pending_revision
                ),
                "pending_revision": pending_revision,
                "base_manifest": manifest,
                "plan": plan,
                "environment": _environment_view(),
                "plugin": _catalog_item(plugin, manifest=manifest, inventory=inventory),
            }
        )
    except (DependencyAnalysisError, ArchiveDependencyConflict) as error:
        reason: dict[str, Any] = {
            "code": error.code,
            "message": safe_process_error(str(error)),
        }
        if getattr(error, "details", None) is not None:
            reason["details"] = error.details
        analysis.update(
            {
                "status": "blocked",
                "compatible": False,
                "blocked_reasons": [reason],
                "environment": _environment_view(),
            }
        )
    except KeyError:
        analysis.update(
            {
                "status": "failed",
                "compatible": False,
                "blocked_reasons": [
                    {"code": "nonebot_plugin_not_found", "message": "插件不存在"}
                ],
            }
        )
    except Exception as error:
        logger.error("NoneBot 插件依赖分析失败", "WebUi", e=error)
        analysis.update(
            {
                "status": "failed",
                "compatible": False,
                "blocked_reasons": [
                    {
                        "code": "dependency_analysis_failed",
                        "message": type(error).__name__,
                    }
                ],
            }
        )
    finally:
        analysis["completed_at"] = utc_now()
        if operation_registry.get(analysis_id) is not None:
            operation_registry.update(analysis_id, progress=100, phase="analyzed")


def _recover_analysis(record: dict[str, Any]):
    public_input = record.get("public_input") or {}
    analysis_id = str(record.get("operation_id") or "")
    project_link = str(public_input.get("project_link") or "")
    action = str(public_input.get("action") or "")
    if (
        not analysis_id
        or not project_link
        or action
        not in {
            "install",
            "update",
            "uninstall",
        }
    ):
        return None
    _ANALYSES[analysis_id] = {
        "analysis_id": analysis_id,
        "project_link": project_link,
        "action": action,
        "status": "queued",
        "created_at": utc_now(),
        "recovered": True,
    }
    return _analyze(analysis_id)


operation_registry.register_recovery_handler(
    "nonebot_store_analysis", _recover_analysis
)


def _target_manifest(analysis: dict[str, Any]) -> dict[str, Any]:
    current = analysis.get("base_manifest") or load_manifest()
    target = deepcopy(current)
    target.pop("generation_digest", None)
    target["pending_verification"] = False
    plugin = analysis["registry_plugin"]
    store_key = f"nonebot:{plugin['project_link']}"
    if analysis["action"] == "uninstall":
        target["plugins"].pop(store_key, None)
    else:
        target["plugins"][store_key] = {
            "store_key": store_key,
            "project_link": plugin["project_link"],
            "module_name": plugin["module_name"],
            "name": plugin.get("name") or plugin["project_link"],
            "version": str(plugin["version"]),
            "state": "managed",
            "installed_at": utc_now(),
            "registry_time": plugin.get("time"),
            "compatibility_overrides": deepcopy(
                analysis["plan"].get("compatibility_overrides", [])
            ),
            "environment_fingerprint": analysis.get("fingerprint"),
            "resolution_inputs": deepcopy(
                analysis["plan"].get("candidate_inputs") or []
            ),
        }
    core = protected_core()
    target["packages"] = {}
    for name, version in analysis["plan"]["resolved_packages"].items():
        if canonicalize_name(name) in core:
            continue
        # The manifest keeps the resolver contract deliberately small. Runtime
        # metadata is derived from the installed generation after activation;
        # embedding it here made package entries differ across machines and
        # confused distribution records with plugin records.
        target["packages"][name] = {"version": str(version)}
    preserve_archive_dependencies(target, core=core)
    return target


def _operation_result(
    mode: str, reason_codes: list[str], **extra: Any
) -> dict[str, Any]:
    source = "webui.nonebot-store"
    restart_required = mode == "restart_pending"
    launcher_managed = update_pending_restart(
        source, reason_codes if restart_required else [], issue_ticket=False
    )
    if restart_required and launcher_managed:
        from zhenxun.utils._restart_utils import issue_restart_ticket

        issue_restart_ticket("webui.nonebot-store", ttl_seconds=10 * 60)
    status = restart_status_data()
    return {
        "apply_mode": mode,
        "restart_required": restart_required,
        "restart_available": restart_required and status["launcher_managed"],
        "reason_codes": reason_codes,
        "access_urls": status["access_urls"],
        "access_targets": status["access_targets"],
        **extra,
    }


def _hot_apply_info(action: str) -> str:
    return {
        "install": "插件已安装并热加载",
        "update": "插件已更新并热加载",
        "uninstall": "插件已卸载并热加载",
    }.get(action, "插件变更已热加载")


async def _apply_hot(
    analysis: dict[str, Any], transaction: dict[str, Any], build: dict[str, Any]
) -> dict[str, Any]:
    plugin = analysis["registry_plugin"]
    module_name = str(plugin["module_name"])
    action = analysis["action"]
    previous_manifest = load_manifest()
    commit_generation(transaction, build, verify_on_start=False)
    activate_current_generation()
    try:
        if action == "install":
            root = module_source_path(module_name)
            if root is None:
                raise RuntimeError("plugin_module_missing_after_install")
            operation = await plugin_runtime_manager.load_new_plugin(
                module_name, root, submit_restart=False
            )
        elif action == "update":
            operation = await plugin_runtime_manager.reload_plugin(module_name)
        else:
            operation = await plugin_runtime_manager.unload_plugin(module_name)
        if operation.mode.value != "hot_reloaded":
            raise RuntimeError(operation.reason or "plugin_runtime_apply_failed")
    except Exception:
        rollback_pending_transaction()
        activate_current_generation()
        if action in {"update", "uninstall"}:
            await plugin_runtime_manager.recover_plugin(
                module_name, submit_restart=False
            )
        raise
    finalize_pending_transaction()
    old_generation = previous_manifest.get("active_generation")
    return _operation_result(
        "hot_reloaded",
        [],
        generation=build["generation"],
        previous_generation=old_generation,
    )


@router.get(
    "/environment",
    dependencies=[authentication()],
    response_model=Result[dict],
    response_class=JSONResponse,
)
async def dependency_environment() -> Result[dict]:
    return Result.ok(_environment_view())


@router.post(
    "/environment/repair",
    dependencies=[authentication()],
    response_model=Result[dict],
    response_class=JSONResponse,
)
async def repair_dependency_environment(
    payload: EnvironmentRepairPayload,
) -> Result[dict]:
    view = _environment_view()
    if not payload.confirmed:
        return Result.fail("dependency_repair_confirmation_required", code=400)
    if not view.get("launcher_managed"):
        return Result.fail("dependency_repair_requires_launcher", code=409)
    if payload.expected_fingerprint != view.get("fingerprint"):
        return Result.fail("environment_analysis_stale", code=409)
    if not view.get("repairable"):
        return Result.fail("dependency_environment_not_repairable", code=409)
    ok, detail = await preflight_environment_repair()
    if not ok:
        return Result.fail(f"dependency_repair_preflight_failed: {detail}", code=409)
    from zhenxun.utils._restart_utils import request_dependency_restart

    save_dependency_sync_status(
        {
            "status": "pending",
            "fingerprint": payload.expected_fingerprint,
            "mode": "locked_inexact",
        }
    )
    accepted, message = await request_dependency_restart(
        "webui.nonebot-store.environment-repair",
        {Path("pyproject.toml"), Path("uv.lock")},
    )
    if not accepted:
        save_dependency_sync_status(
            {"status": "failed", "code": "dependency_restart_not_accepted"}
        )
        return Result.fail(message, code=409)
    status = restart_status_data()
    return Result.ok(
        {
            "apply_mode": "restart_requested",
            "restart_required": True,
            "restart_available": True,
            "reason_codes": ["dependency_environment_repair"],
            "boot_id": status.get("boot_id"),
            "access_urls": status["access_urls"],
            "access_targets": status["access_targets"],
        },
        info="依赖同步已提交，正在重启真寻",
    )


@router.get(
    "/plugins",
    dependencies=[authentication()],
    response_model=Result[dict],
    response_class=JSONResponse,
)
async def list_plugins(
    search: str = "",
    page: int = Query(1, ge=1),
    page_size: int = Query(30, ge=1, le=100),
    status: str = "all",
    plugin_type: str = "all",
    adapter: str = "all",
    include_incompatible: bool = False,
    refresh: bool = False,
) -> Result[dict]:
    try:
        entries, meta = await get_registry(refresh=refresh)
        pending = pending_transaction()
        manifest = _effective_manifest(pending)
        inventory = installed_inventory()
        keyword = search.strip().casefold()
        items = []
        for entry in entries:
            item = _catalog_item(
                entry,
                manifest=manifest,
                inventory=inventory,
                pending=pending,
            )
            if item["compatibility"] == "blocked" and not include_incompatible:
                continue
            if (
                keyword
                and keyword
                not in (
                    f"{item['name']} {item['project_link']} {item['module_name']} "
                    f"{item['author']}"
                ).casefold()
            ):
                continue
            if status == "installed" and item["install_state"] not in {
                "managed",
                "external",
                "update_available",
            }:
                continue
            if status not in {"all", "installed"} and item["install_state"] != status:
                continue
            if plugin_type != "all" and item["plugin_type"] != plugin_type:
                continue
            supported = item["supported_adapters"] or []
            if adapter != "all" and supported and adapter not in supported:
                continue
            items.append(item)
        start = (page - 1) * page_size
        return Result.ok(
            {
                "items": items[start : start + page_size],
                "total": len(items),
                "page": page,
                "page_size": page_size,
                "registry": meta,
                "enabled_adapters": sorted(_enabled_adapters()),
            }
        )
    except Exception as error:
        logger.error("读取 NoneBot 插件商店失败", "WebUi", e=error)
        return Result.fail("nonebot_registry_unavailable")


@router.get(
    "/plugins/{project_link}",
    dependencies=[authentication()],
    response_model=Result[dict],
    response_class=JSONResponse,
)
async def plugin_detail(project_link: str) -> Result[dict]:
    try:
        plugin, meta = await get_registry_plugin(project_link)
        metadata = await fetch_pypi_metadata(project_link, str(plugin["version"]))
        info = metadata["info"]
        return Result.ok(
            {
                **_catalog_item(
                    plugin,
                    manifest=_effective_manifest(),
                    inventory=installed_inventory(),
                    pending=pending_transaction(),
                ),
                "registry": meta,
                "pypi": {
                    "summary": info.get("summary") or "",
                    "requires_python": info.get("requires_python") or "",
                    "license": info.get("license_expression")
                    or info.get("license")
                    or "",
                    "project_urls": info.get("project_urls") or {},
                    "requires_dist": info.get("requires_dist") or [],
                    "wheel_available": any(
                        str(item.get("filename", "")).endswith(".whl")
                        for item in metadata.get("urls") or []
                        if isinstance(item, dict)
                    ),
                },
            }
        )
    except KeyError:
        return Result.fail("nonebot_plugin_not_found", code=404)
    except DependencyAnalysisError as error:
        return Result.fail(error.code, code=400)


@router.post(
    "/analyze",
    dependencies=[authentication()],
    response_model=Result[dict],
    response_class=JSONResponse,
)
async def analyze_plugin(payload: AnalyzePayload) -> Result[dict]:
    _cleanup_analyses()
    pending = pending_transaction()
    if pending is not None and pending.get("state") not in {"pending_restart"}:
        return Result.fail("nonebot_transaction_pending", code=409)
    analysis_id = uuid.uuid4().hex
    analysis = {
        "analysis_id": analysis_id,
        "project_link": payload.project_link,
        "action": payload.action,
        "status": "queued",
        "created_at": utc_now(),
    }
    _ANALYSES[analysis_id] = analysis
    _, task = operation_registry.start(
        "nonebot_store_analysis",
        _analyze(analysis_id),
        operation_id=analysis_id,
        public_input={
            "project_link": payload.project_link,
            "action": payload.action,
        },
        recovery_policy="restart",
        name=f"nonebot-analysis:{analysis_id[:8]}",
    )
    analysis["task"] = task
    return Result.ok(_public_analysis(analysis), info="依赖分析已开始")


@router.get(
    "/analyses/{analysis_id}",
    dependencies=[authentication()],
    response_model=Result[dict],
    response_class=JSONResponse,
)
async def analysis_status(analysis_id: str) -> Result[dict]:
    _cleanup_analyses()
    analysis = _ANALYSES.get(analysis_id)
    if analysis is None:
        return Result.fail("analysis_not_found", code=404)
    return Result.ok(_public_analysis(analysis))


@router.post(
    "/apply",
    dependencies=[authentication()],
    response_model=Result[dict],
    response_class=JSONResponse,
)
async def apply_analysis(payload: ApplyPayload) -> Result[dict]:
    analysis = _ANALYSES.get(payload.analysis_id)
    if analysis is None:
        return Result.fail("analysis_not_found", code=404)
    applied_result = analysis.get("applied_result")
    if isinstance(applied_result, dict):
        if payload.operation_id == analysis.get("applied_operation_id"):
            return Result.ok(deepcopy(applied_result), info="重复请求已复用原操作结果")
        return Result.fail("analysis_already_applied", code=409)
    if analysis.get("status") != "ready":
        return Result.fail("analysis_not_ready", code=409)
    operation_id = payload.operation_id or uuid.uuid4().hex

    def completed(result: dict[str, Any], info: str) -> Result[dict]:
        result.setdefault("operation_id", operation_id)
        analysis["applied_operation_id"] = operation_id
        analysis["applied_result"] = deepcopy(result)
        record_operation(
            f"nonebot:{analysis['registry_plugin']['project_link']}", result
        )
        return Result.ok(result, info=info)

    def record_failure(code: str) -> None:
        record_operation(
            f"nonebot:{analysis['registry_plugin']['project_link']}",
            {
                "operation_id": operation_id,
                "status": "failed",
                "apply_mode": "failed",
                "reason": code,
                "rolled_back": True,
            },
        )

    plugin = analysis["registry_plugin"]
    current_pending = pending_transaction()
    current_revision = _pending_revision(current_pending)
    if analysis.get("pending_revision", "") != current_revision:
        return Result.fail("analysis_stale", code=409)
    if environment_fingerprint(
        plugin, pending_revision=current_revision
    ) != analysis.get("fingerprint"):
        return Result.fail("analysis_stale", code=409)
    plan = analysis["plan"]
    if not payload.confirm_third_party_code:
        return Result.fail("third_party_code_confirmation_required", code=400)
    if plan["non_core_changes"] and not payload.confirm_non_core_changes:
        return Result.fail("non_core_changes_confirmation_required", code=400)
    if plan["source_build_required"] and not payload.confirm_source_build:
        return Result.fail("source_build_confirmation_required", code=400)
    if (
        plan.get("compatibility_overrides")
        and not payload.confirm_compatibility_overrides
    ):
        return Result.fail("compatibility_override_confirmation_required", code=400)
    if (
        plan.get("database_migration_possible")
        and not payload.confirm_database_migration
    ):
        return Result.fail("database_migration_confirmation_required", code=400)

    journal_store_key = f"nonebot:{plugin['project_link']}"
    begin_operation(journal_store_key, analysis["action"], operation_id)
    previous_pending = deepcopy(current_pending)
    new_generation: int | None = None
    try:
        async with _store_operation(
            operation_id=operation_id, owner="webui.nonebot_store"
        ):
            latest_pending = pending_transaction()
            if _pending_revision(latest_pending) != current_revision:
                return Result.fail("analysis_stale", code=409)
            target = _target_manifest(analysis)
            operations = deepcopy(
                latest_pending.get("operations", []) if latest_pending else []
            )
            operation = {
                "operation_id": operation_id,
                "analysis_id": payload.analysis_id,
                "action": analysis["action"],
                "project_link": plugin["project_link"],
                "module_name": plugin["module_name"],
                "name": plugin.get("name") or plugin["project_link"],
                "database_migration_possible": bool(
                    plan.get("database_migration_possible")
                ),
                "created_at": utc_now(),
                "target_manifest": deepcopy(target),
            }
            existing_index = next(
                (
                    index
                    for index, item in enumerate(operations)
                    if item.get("project_link") == plugin["project_link"]
                ),
                None,
            )
            if existing_index is None:
                operations.append(operation)
            else:
                previous_operation = operations[existing_index]
                base_plugins = (
                    latest_pending.get("base_manifest", {}).get("plugins", {})
                    if latest_pending
                    else load_manifest().get("plugins", {})
                )
                store_key = f"nonebot:{plugin['project_link']}"
                if (
                    previous_operation.get("action") == "install"
                    and analysis["action"] == "uninstall"
                    and store_key not in base_plugins
                ):
                    operations.pop(existing_index)
                else:
                    operations[existing_index] = operation
            transaction = {
                "version": 2,
                "revision": uuid.uuid4().hex,
                "base_manifest": deepcopy(
                    latest_pending.get("base_manifest")
                    if latest_pending
                    else load_manifest()
                ),
                "operations": operations,
                "target_manifest": target,
                "source_build_confirmed": bool(
                    payload.confirm_source_build
                    or (latest_pending or {}).get("source_build_confirmed")
                ),
                "database_migration_confirmed": bool(
                    payload.confirm_database_migration
                    or (latest_pending or {}).get("database_migration_confirmed")
                ),
                "database_migration_possible": bool(
                    plan.get("database_migration_possible")
                    or (latest_pending or {}).get("database_migration_possible")
                ),
                "database_type": (
                    plan.get("database_type")
                    if plan.get("database_migration_possible")
                    else (latest_pending or {}).get("database_type", "none")
                ),
                "state": "building",
                "created_at": (latest_pending or {}).get("created_at", utc_now()),
                "updated_at": utc_now(),
                "generation": (latest_pending or {}).get("generation"),
                "generation_digest": (latest_pending or {}).get("generation_digest"),
                "native_extensions": (latest_pending or {}).get(
                    "native_extensions", []
                ),
            }
            if not operations:
                remove_generation(transaction.get("generation"))
                finalize_orm_migration()
                clear_pending_transaction()
                update_pending_restart("webui.nonebot-store", [], issue_ticket=False)
                return completed(
                    _operation_result("rolled_back", ["transaction_canceled"]),
                    "待应用插件变更已相互抵消",
                )
            with archive_dependency_policy(transaction):
                pass
            module_name = str(plugin["module_name"])
            runtime = plugin_runtime_manager.classification_for(module_name)
            root_change = next(
                (
                    item
                    for item in plan["package_changes"]["changed"]
                    if canonicalize_name(item["name"])
                    == canonicalize_name(str(plugin["project_link"]))
                ),
                None,
            )
            dependency_changes = [
                item
                for item in plan["package_changes"]["changed"]
                if item is not root_change
            ]
            hot_candidate = (
                latest_pending is None
                and not plan["source_build_required"]
                and not dependency_changes
                and not plan.get("shared_changes")
                and not plan.get("compatibility_overrides")
                and plan["pure_python_candidate"]
                and (
                    analysis["action"] == "install"
                    or runtime["reload_support"] == "hot_reloadable"
                )
            )
            deferred_source_build = bool(
                plan["source_build_required"]
                or (
                    latest_pending
                    and latest_pending.get("source_build_confirmed")
                    and not isinstance(latest_pending.get("generation"), int)
                )
            )
            if deferred_source_build:
                remove_generation(transaction.get("generation"))
                transaction.pop("generation", None)
                transaction.pop("generation_digest", None)
                transaction["native_extensions"] = []
                transaction["state"] = "pending_restart"
                save_pending_transaction(transaction)
                return completed(
                    _operation_result(
                        "restart_pending",
                        ["source_build_required"],
                        transaction_revision=transaction["revision"],
                        pending_operations=operations,
                    ),
                    "依赖事务已保存，重启后构建并生效",
                )

            previous_native = generation_native_extensions()
            build = build_generation(transaction)
            new_generation = int(build["generation"])
            next_native = generation_native_extensions(build["path"])
            native_changed = previous_native != next_native
            source_runtime = {
                "reload_support": "hot_reloadable",
                "reload_reasons": [],
            }
            if analysis["action"] != "uninstall":
                source_root = module_source_path(module_name, build["path"])
                if source_root is None:
                    raise LayerBuildError("plugin_module_missing_after_install")
                source_runtime = plugin_runtime_manager.classification_for_source(
                    module_name, source_root
                )
                hot_candidate = hot_candidate and (
                    source_runtime["reload_support"] == "hot_reloadable"
                )
            if hot_candidate and not native_changed:
                result = await _apply_hot(analysis, transaction, build)
                return completed(result, _hot_apply_info(analysis["action"]))

            transaction["native_extensions_changed"] = native_changed
            stage_generation(transaction, build)
            reasons = []
            if dependency_changes:
                reasons.append("non_core_dependencies_changed")
            if plan.get("shared_changes"):
                reasons.append("shared_dependencies_changed")
            if plan.get("compatibility_overrides"):
                reasons.append("compatibility_override_unverified")
            if native_changed:
                reasons.append("native_extensions_changed")
            if source_runtime["reload_support"] != "hot_reloadable":
                reasons.extend(
                    source_runtime.get("reload_reasons")
                    or ["plugin_not_hot_reloadable"]
                )
            if (
                runtime["reload_support"] != "hot_reloadable"
                and analysis["action"] != "install"
            ):
                reasons.extend(
                    runtime.get("reload_reasons") or ["plugin_not_hot_reloadable"]
                )
            if plan.get("database_migration_possible"):
                reasons.append("database_migration_possible")
            return completed(
                _operation_result(
                    "restart_pending",
                    reasons or ["plugin_restart_required"],
                    transaction_revision=transaction["revision"],
                    pending_operations=operations,
                ),
                "插件事务已准备，重启后生效",
            )
    except StoreOperationBusyError:
        record_failure("plugin_operation_in_progress")
        return Result.fail("plugin_operation_in_progress", code=409)
    except (ArchiveSourceBuildConflict, ArchiveDependencyConflict) as error:
        record_failure(error.code)
        return Result.fail(error.code, code=409)
    except LayerBuildError as error:
        if new_generation is not None:
            remove_generation(new_generation)
        if previous_pending is not None:
            save_pending_transaction(previous_pending)
        logger.error(f"NoneBot 依赖层构建失败: {error.code}", "WebUi")
        record_failure(error.code)
        return Result.fail(error.code, code=400)
    except Exception as error:
        if new_generation is not None:
            remove_generation(new_generation)
        if previous_pending is not None:
            save_pending_transaction(previous_pending)
        logger.error("NoneBot 插件应用失败，已保留原事务", "WebUi", e=error)
        record_failure("nonebot_plugin_apply_failed")
        return Result.fail("nonebot_plugin_apply_failed", code=500)


def _public_transaction(transaction: dict[str, Any] | None) -> dict[str, Any] | None:
    if not transaction:
        return None
    return {
        "revision": transaction.get("revision"),
        "state": transaction.get("state"),
        "created_at": transaction.get("created_at"),
        "updated_at": transaction.get("updated_at"),
        "database_migration_possible": bool(
            transaction.get("database_migration_possible")
        ),
        "database_type": transaction.get("database_type", "none"),
        "operations": [
            {
                key: item.get(key)
                for key in (
                    "operation_id",
                    "action",
                    "project_link",
                    "module_name",
                    "name",
                    "created_at",
                )
            }
            for item in transaction.get("operations", [])
            if isinstance(item, dict)
        ],
    }


@router.get(
    "/transactions/pending",
    dependencies=[authentication()],
    response_model=Result[dict],
    response_class=JSONResponse,
)
async def pending_transaction_status() -> Result[dict]:
    return Result.ok({"transaction": _public_transaction(pending_transaction())})


@router.delete(
    "/transactions/pending/{operation_id}",
    dependencies=[authentication()],
    response_model=Result[dict],
    response_class=JSONResponse,
)
async def cancel_pending_operation(operation_id: str) -> Result[dict]:
    try:
        async with _store_operation(
            operation_id=operation_id, owner="webui.nonebot_store"
        ):
            transaction = pending_transaction()
            if transaction is None or transaction.get("state") not in {
                "pending_restart",
                "migration_blocked",
            }:
                return Result.fail("nonebot_transaction_not_cancelable", code=409)
            operations = transaction.get("operations", [])
            index = next(
                (
                    offset
                    for offset, item in enumerate(operations)
                    if item.get("operation_id") == operation_id
                ),
                None,
            )
            if index is None:
                return Result.fail("nonebot_transaction_operation_not_found", code=404)
            remaining = deepcopy(operations[:index])
            removed = operations[index:]
            old_generation = transaction.get("generation")
            if not remaining:
                remove_generation(old_generation)
                finalize_orm_migration()
                clear_pending_transaction()
                update_pending_restart("webui.nonebot-store", [], issue_ticket=False)
                return Result.ok(
                    {
                        **_operation_result("rolled_back", ["transaction_canceled"]),
                        "removed_operation_ids": [
                            item.get("operation_id") for item in removed
                        ],
                    },
                    info="待重启插件事务已取消",
                )
            transaction["operations"] = remaining
            transaction["target_manifest"] = deepcopy(remaining[-1]["target_manifest"])
            transaction["revision"] = uuid.uuid4().hex
            transaction["updated_at"] = utc_now()
            if isinstance(old_generation, int):
                build = build_generation(transaction)
                stage_generation(transaction, build)
            else:
                save_pending_transaction(transaction)
            return Result.ok(
                {
                    **_operation_result(
                        "restart_pending",
                        ["plugin_restart_required"],
                        transaction_revision=transaction["revision"],
                        pending_operations=_public_transaction(transaction)[
                            "operations"
                        ],
                    ),
                    "removed_operation_ids": [
                        item.get("operation_id") for item in removed
                    ],
                },
                info="已撤销所选操作及其后的依赖操作",
            )
    except StoreOperationBusyError:
        return Result.fail("plugin_operation_in_progress", code=409)
    except LayerBuildError as error:
        return Result.fail(error.code, code=400)


@router.post(
    "/transactions/cancel",
    dependencies=[authentication()],
    response_model=Result[dict],
    response_class=JSONResponse,
)
async def cancel_transaction() -> Result[dict]:
    try:
        async with _store_operation(owner="webui.nonebot_store"):
            transaction = pending_transaction()
            if transaction is None:
                return Result.fail("nonebot_transaction_not_found", code=404)
            state = str(transaction.get("state") or "")
            if state not in {
                "pending_restart",
                "verification_pending",
                "migration_blocked",
                "failed",
            }:
                return Result.fail("nonebot_transaction_not_cancelable", code=409)
            if state == "verification_pending":
                rollback_pending_transaction()
                activate_current_generation()
            elif state in {"pending_restart", "migration_blocked", "failed"}:
                remove_generation(transaction.get("generation"))
            finalize_orm_migration()
            clear_pending_transaction()
            update_pending_restart("webui.nonebot-store", [], issue_ticket=False)
            failed = state == "failed"
            return Result.ok(
                _operation_result(
                    "rolled_back",
                    [
                        "failed_transaction_cleared"
                        if failed
                        else "transaction_canceled"
                    ],
                    project_link=transaction.get("project_link"),
                    action=transaction.get("action"),
                ),
                info=("失败记录已清除" if failed else "待重启插件事务已取消"),
            )
    except StoreOperationBusyError:
        return Result.fail("plugin_operation_in_progress", code=409)
    except Exception as error:
        logger.error("取消 NoneBot 插件事务失败", "WebUi", e=error)
        return Result.fail("nonebot_transaction_cancel_failed", code=500)
