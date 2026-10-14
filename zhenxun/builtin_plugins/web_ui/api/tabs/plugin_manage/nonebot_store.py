from __future__ import annotations

import asyncio
import contextlib
from copy import deepcopy
from hashlib import sha256
import importlib.metadata
import os
from typing import Any, Literal, TypeVar
import uuid

from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse
from packaging.utils import canonicalize_name
from pydantic import BaseModel, Field

from zhenxun.services.lifecycle import lifecycle_kernel
from zhenxun.services.log import logger
from zhenxun.services.nonebot_store.dependencies import (
    DependencyAnalysisError,
    environment_fingerprint,
    environment_report_async,
    fetch_pypi_metadata,
    installed_inventory,
    metadata_compatibility,
    preflight_environment_repair,
    protected_core,
    safe_process_error,
    solve_install,
    uninstall_plan,
)
from zhenxun.services.nonebot_store.registry import get_registry, get_registry_plugin
from zhenxun.services.nonebot_store.runtime import (
    LayerBuildError,
    activate_current_generation,
    commit_generation,
    finalize_orm_migration,
    finalize_pending_transaction,
    generation_native_extensions,
    module_source_path,
    prepare_generation,
    record_manual_operations,
    rollback_pending_transaction,
    stage_generation,
)
from zhenxun.services.nonebot_store.storage import (
    PENDING_FILE,
    STARTUP_STATUS_FILE,
    clear_pending_transaction,
    defer_transaction,
    dependency_sync_status,
    draft_transaction,
    load_manifest,
    read_json,
    remove_generation,
    save_dependency_sync_status,
    save_pending_transaction,
    utc_now,
    write_json,
)
from zhenxun.services.nonebot_store.storage import (
    active_transaction as pending_transaction,
)
from zhenxun.services.plugin_host import RuntimeOperation, plugin_host
from zhenxun.services.plugin_store.operation_journal import record_operation

from ....apply_result import update_pending_restart
from ....base_model import Result
from ....restart_service import restart_status_data
from ....utils import authentication
from .store import (
    PreviewStore,
    StoreOperationBusyError,
    _store_operation,
    _version_compare,
    replayed_bound_operation,
    run_bound_operation,
)

router = APIRouter(prefix="/store/nonebot")
_ANALYSES = PreviewStore()
_REPAIR_PREVIEWS = PreviewStore()
_PENDING_TRANSACTION_STATES = {
    "building",
    "pending_restart",
    "verification_pending",
    "migration_blocked",
    "recovery_required",
    "cleanup_pending",
    "committed",
}


class AnalyzePayload(BaseModel):
    project_link: str = Field(min_length=1, max_length=200)
    action: Literal["install", "update", "uninstall"]
    prefer_existing: bool = True


class ApplyPayload(BaseModel):
    analysis_id: str = Field(min_length=32, max_length=64)
    operation_id: str | None = Field(default=None, pattern=r"^[0-9a-f]{32}$")
    confirmed: bool = False
    summary_revision: str | None = None


class EnvironmentRepairPayload(BaseModel):
    operation_id: str | None = Field(default=None, pattern=r"^[0-9a-f]{32}$")
    expected_fingerprint: str = Field(min_length=64, max_length=64)
    confirmed: bool = False
    preview_id: str | None = None
    group_ids: list[str] | None = None


_BoundPayload = TypeVar("_BoundPayload", ApplyPayload, EnvironmentRepairPayload)


async def _environment_view() -> dict[str, Any]:
    try:
        result = await environment_report_async(check_lock=True)
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
            "incompatible_shared_drift": [],
            "error_code": error.code,
        }
    except Exception as error:
        result = {
            "status": "failed",
            "fingerprint": "",
            "repairable": False,
            "immutable_drift": [],
            "incompatible_shared_drift": [],
            "error_code": type(error).__name__,
        }
    result.setdefault(
        "core_summary",
        {
            "status": "unconfirmed",
            "checked_count": 0,
            "issues": [
                {
                    "kind": "environment_check_failed",
                    "error_code": result.get("error_code"),
                }
            ],
        },
    )
    result["sync"] = dependency_sync_status()
    startup = read_json(STARTUP_STATUS_FILE, {})
    startup.pop("environment_versions", None)
    result["startup_verification"] = startup
    pending = pending_transaction() or {}
    from zhenxun.services.nonebot_store.policy import orm_console_instructions

    result["orm_manual"] = orm_console_instructions(pending)
    result["candidate_built"] = isinstance(pending.get("generation"), int)
    result["launcher_managed"] = bool(os.environ.get("ZHENXUN_LAUNCHER_PID"))
    return result


def _enabled_adapters() -> set[str]:
    from zhenxun.configs.config import BotConfig

    enabled = {"nonebot.adapters.onebot.v11"}
    if BotConfig.qq_adapter_load:
        enabled.add("nonebot.adapters.qq")
    if BotConfig.telegram_adapter_load:
        enabled.add("nonebot.adapters.telegram")
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
    return reasons


def _registry_warnings(plugin: dict[str, Any]) -> list[dict[str, str]]:
    reasons = []
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


def _local_registry_item(plugin: dict) -> dict:
    return {
        **plugin,
        "valid": True,
        "skip_test": False,
        "supported_adapters": [],
        "catalog_source": "local",
    }


async def _catalog_sources(
    manifest: dict, *, refresh: bool = False
) -> tuple[list, dict]:
    """Keep managed packages discoverable when remote catalog data is unavailable."""
    local = {
        p["project_link"]: _local_registry_item(p)
        for p in manifest.get("plugins", {}).values()
        if p.get("state") == "managed"
    }
    try:
        entries, meta = await get_registry(refresh=refresh)
    except Exception as error:
        return list(local.values()), {
            "unavailable": True,
            "diagnostic": type(error).__name__,
        }
    for item in entries:
        local[item["project_link"]] = item
    return list(local.values()), meta


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


def _plugin_transaction(project_link: str) -> dict[str, Any] | None:
    """查找最近一次包含该插件且已确认回滚的事务

    参数:
        project_link: 插件包名

    返回:
        dict[str, Any] | None: 需要在商店中展示失败信息的事务
    """
    from zhenxun.services.nonebot_store.storage import (
        pending_transaction as recorded_transaction,
    )
    from zhenxun.services.nonebot_store.storage import settled_transactions

    for record in (recorded_transaction(), *settled_transactions()):
        if record and any(
            item.get("project_link") == project_link
            for item in record.get("operations", [])
        ):
            return record if record.get("rollback_confirmed") else None
    return None


def _failure_reasons(transaction: dict[str, Any], project_link: str) -> list[dict]:
    """提取失败事务中属于该插件的失败原因

    参数:
        transaction: 失败的事务
        project_link: 插件包名

    返回:
        list[dict]: 失败原因，没有逐插件记录时返回事务错误码
    """
    reasons = []
    for failure in transaction.get("failure_reasons") or []:
        if not isinstance(failure, dict):
            continue
        store_key = str(failure.get("store_key") or "")
        if store_key and store_key != f"nonebot:{project_link}":
            continue
        reasons.append(
            {
                "code": str(failure.get("code") or "plugin_import_failed"),
                "store_key": store_key or None,
                "module_name": str(failure.get("module_name") or "") or None,
                "paths": [
                    str(path)
                    for path in failure.get("paths") or []
                    if isinstance(path, str)
                ][:20],
            }
        )
    return reasons or [
        {
            "code": str(transaction.get("error_code") or "nonebot_plugin_apply_failed"),
            "paths": [],
        }
    ]


def _catalog_item(
    plugin: dict[str, Any],
    *,
    manifest: dict[str, Any] | None = None,
    inventory: dict[str, str] | None = None,
    pending: dict[str, Any] | None = None,
) -> dict[str, Any]:
    project_link = str(plugin["project_link"])
    if pending is None:
        pending = _plugin_transaction(project_link)
    managed = _managed_plugin(project_link, manifest)
    if (
        pending
        and pending.get("rollback_confirmed")
        and managed
        and (
            str(managed.get("installed_at") or "")
            > str(pending.get("created_at") or "")
        )
    ):
        pending = None
    external = None if managed else _external_version(project_link, inventory)
    operation = next(
        (
            item
            for item in (pending or {}).get("operations", [])
            if isinstance(item, dict) and item.get("project_link") == project_link
        ),
        None,
    )
    if not (pending and (operation or pending.get("project_link") == project_link)):
        pending = None
    transaction_state = str(pending.get("state") or "") if pending else ""
    pending_action = (
        str((operation or pending).get("action"))
        if pending and transaction_state in _PENDING_TRANSACTION_STATES
        else None
    )
    pending_reasons = (
        [{"code": str(pending["error_code"])}]
        if pending and pending.get("error_code")
        else []
    )
    reasons = _basic_block_reasons(plugin)
    failure_reasons = (
        _failure_reasons(pending, project_link)
        if pending and transaction_state == "failed"
        else []
    )
    update_available = bool(
        managed
        and _version_compare(str(managed.get("version", "0")), str(plugin["version"]))
        == -1
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
    elif managed:
        install_state = "update_available" if update_available else "managed"
    elif external:
        install_state = "external"
    elif reasons:
        install_state = "blocked"
    else:
        install_state = "not_installed"
    rollback_confirmed = bool(pending and pending.get("rollback_confirmed"))
    return {
        "store_key": f"nonebot:{project_link}",
        "project_link": project_link,
        "module_name": plugin.get("module_name"),
        "name": plugin.get("name") or project_link,
        "description": plugin.get("desc") or "",
        "author": plugin.get("author") or "",
        "homepage": plugin.get("homepage") or "",
        "is_official": bool(plugin.get("is_official")),
        "plugin_type": plugin.get("type") or "application",
        "supported_adapters": plugin.get("supported_adapters"),
        "version": str(plugin.get("version") or ""),
        "install_state": install_state,
        "update_available": update_available,
        "blocked_reasons": reasons,
        "warnings": _registry_warnings(plugin),
        "catalog_source": plugin.get("catalog_source", "registry"),
        "failure_reasons": failure_reasons,
        "dependency_retry_available": rollback_confirmed
        and any(
            item.get("code") == "dependency_import_failed" for item in failure_reasons
        ),
        "pending_reasons": pending_reasons,
        "managed": bool(managed),
        "compatibility_unverified": bool(compatibility_overrides),
        "compatibility_override_stale": compatibility_override_stale,
        "apply_mode": (
            "recovery_required"
            if transaction_state == "recovery_required"
            else "cleanup_pending"
            if transaction_state in {"committed", "cleanup_pending"}
            else "restart_pending"
            if pending_action
            else "failed"
            if failure_reasons
            else None
        ),
        "pending_action": pending_action,
        "pending_operation_id": operation.get("operation_id") if operation else None,
        "transaction_state": transaction_state or None,
        "rollback_confirmed": rollback_confirmed,
    }


def _public_analysis(analysis: dict[str, Any]) -> dict[str, Any]:
    return {
        key: deepcopy(value)
        for key, value in analysis.items()
        if key not in {"metadata", "registry_plugin", "task", "base_manifest"}
    }


async def _analyze(analysis: dict[str, Any]) -> None:
    analysis["status"] = "analyzing"
    try:
        pending = pending_transaction()
        manifest = _effective_manifest(pending)
        pending_revision = _pending_revision(pending)
        inventory = installed_inventory()
        managed = _managed_plugin(analysis["project_link"], manifest)
        if analysis["action"] == "uninstall" and managed:
            plugin, registry_meta = _local_registry_item(managed), {"source": "local"}
            reasons = []
        else:
            plugin, registry_meta = await get_registry_plugin(analysis["project_link"])
            reasons = _basic_block_reasons(plugin)
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
            reasons.extend(
                item
                for item in metadata_compatibility(metadata)
                if not item["code"].endswith("_version_incompatible")
            )
        if reasons:
            analysis.update(
                {
                    "status": "blocked",
                    "blocked_reasons": reasons,
                    "registry_plugin": plugin,
                }
            )
            return

        plan = (
            uninstall_plan(managed or plugin, manifest=manifest)
            if action == "uninstall"
            else await solve_install(
                plugin,
                metadata or {},
                manifest=manifest,
                prefer_existing=analysis.get("prefer_existing", True),
            )
        )
        from zhenxun.services.nonebot_store.policy import summary_revision

        plan["warnings"] = _registry_warnings(plugin)
        plan["artifact_hashes"] = sorted(
            {
                str(item.get("digests", {}).get("sha256"))
                for item in (metadata or {}).get("urls", [])
                if item.get("digests", {}).get("sha256")
            }
        )
        plan["hot_apply_expected"] = _hot_candidate(action, plugin, plan, pending)
        plan["restart_required"] = not plan["hot_apply_expected"]
        plan["summary_revision"] = summary_revision(
            {
                **plan,
                "plugin": {
                    key: plugin.get(key)
                    for key in ("project_link", "version", "module_name")
                },
            }
        )
        analysis.update(
            {
                "status": "ready",
                "blocked_reasons": [],
                "registry_plugin": plugin,
                "metadata": metadata,
                "fingerprint": environment_fingerprint(
                    plugin, pending_revision=pending_revision
                ),
                "pending_revision": pending_revision,
                "base_manifest": manifest,
                "plan": plan,
                "environment": await _environment_view(),
                "plugin": _catalog_item(plugin, manifest=manifest, inventory=inventory),
            }
        )
    except DependencyAnalysisError as error:
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
                "environment": await _environment_view(),
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


def _dependency_evidence(plan: dict[str, Any]) -> dict[str, Any]:
    return {
        "dependency_inputs": list(plan.get("candidate_inputs") or []),
        "candidate_packages": list(plan.get("candidate_packages") or []),
        "dependency_packages": {
            str(item["name"]): str(item.get("to") or item.get("version"))
            for item in [
                *(plan.get("package_changes", {}).get("added") or []),
                *(plan.get("package_changes", {}).get("changed") or []),
                *(plan.get("shared_changes") or []),
            ]
        },
    }


def _target_manifest(analysis: dict[str, Any]) -> dict[str, Any]:
    current = analysis.get("base_manifest") or load_manifest()
    target = deepcopy(current)
    target.pop("generation_digest", None)
    target["pending_verification"] = False
    target["dependency_warnings"] = deepcopy(
        analysis["plan"].get("remaining_conflicts", [])
    )
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
            "compatibility_overrides": deepcopy(
                analysis["plan"].get("compatibility_overrides", [])
            ),
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
    return target


def _operation_result(
    mode: str, reason_codes: list[str], **extra: Any
) -> dict[str, Any]:
    source = "webui.nonebot-store"
    restart_required = mode == "restart_pending"
    update_pending_restart(source, reason_codes if restart_required else [])
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


def _release_only(transaction: dict[str, Any] | None) -> bool:
    """待处理事务是否只登记了运行期释放的依赖，可随热处理一并生效"""
    if not transaction:
        return False
    return bool(
        transaction.get("state") == "pending_restart"
        and not transaction.get("operations")
        and not isinstance(transaction.get("generation"), int)
        and not transaction.get("source_transaction_revision")
        and not transaction.get("source_build_confirmed")
        and not transaction.get("database_migration_possible")
    )


def _hot_candidate(
    action: str,
    plugin: dict[str, Any],
    plan: dict[str, Any],
    pending: dict[str, Any] | None,
) -> bool:
    """插件变更能否不重启直接生效，依赖层构建后还需检查插件源码与原生扩展

    参数:
        action: install / update / uninstall
        plugin: 商店插件信息
        plan: 依赖分析结果
        pending: 当前待处理事务

    返回:
        bool: 是否可热处理
    """
    project = canonicalize_name(str(plugin["project_link"]))
    dependency_changed = any(
        canonicalize_name(item["name"]) != project
        for item in plan["package_changes"]["changed"]
    )
    return bool(
        (pending is None or _release_only(pending))
        and not plan["source_build_required"]
        and not dependency_changed
        and not plan.get("shared_changes")
        and not plan.get("compatibility_overrides")
        and not plan.get("database_migration_possible")
        and plan.get("pure_python_candidate")
        and (
            action == "install"
            or plugin_host.classification_for(str(plugin["module_name"]))[
                "reload_support"
            ]
            == "hot_reloadable"
        )
    )


def _hot_apply_info(action: str) -> str:
    return {
        "install": "插件已安装并热加载",
        "update": "插件已更新并热加载",
        "uninstall": "插件已卸载并热加载",
    }.get(action, "插件变更已热加载")


def _require_hot(operation: RuntimeOperation) -> None:
    if operation.mode.value != "hot_reloaded":
        raise RuntimeError(operation.reason or "plugin_runtime_apply_failed")


async def _apply_hot(
    analysis: dict[str, Any], transaction: dict[str, Any], build: dict[str, Any]
) -> dict[str, Any]:
    """在运行中的进程内切换依赖层并热加载、重载或卸载插件

    卸载在切换依赖层之前完成：切换后清单不再登记该插件，无法再判定为可热处理。
    事务先以待验证状态落盘，进程中途退出时由 launcher 按启动验证确认或回滚；
    热处理失败时回滚依赖层并从原依赖层恢复插件。

    参数:
        analysis: 分析结果
        transaction: 待提交的依赖事务
        build: 已构建的依赖层

    返回:
        dict[str, Any]: 操作结果

    异常:
        RuntimeError: 热处理失败，依赖层已回滚
    """
    plugin = analysis["registry_plugin"]
    module_name = str(plugin["module_name"])
    action = analysis["action"]
    previous_manifest = load_manifest()
    transaction.update(
        generation=int(build["generation"]),
        generation_digest=str(build["digest"]),
        native_extensions=list(build.get("native_extensions") or []),
        state="verification_pending",
    )
    if action == "uninstall":
        _require_hot(await plugin_host.unload_plugin(module_name))
    try:
        save_pending_transaction(transaction)
        commit_generation(transaction, build, verify_on_start=True)
    except Exception:
        if action == "uninstall" and (root := module_source_path(module_name)):
            await plugin_host.load_new_plugin(
                module_name, root, submit_restart=False, dependencies_verified=True
            )
        raise
    activate_current_generation()
    try:
        if action == "install":
            root = module_source_path(module_name)
            if root is None:
                raise RuntimeError("plugin_module_missing_after_install")
            _require_hot(
                await plugin_host.load_new_plugin(
                    module_name, root, submit_restart=False, dependencies_verified=True
                )
            )
        elif action == "update":
            _require_hot(await plugin_host.reload_plugin(module_name))
    except Exception as error:
        rollback_pending_transaction()
        activate_current_generation()
        failed = read_json(PENDING_FILE, None)
        if isinstance(failed, dict) and failed.get("revision") == transaction.get(
            "revision"
        ):
            failed["error_code"] = "plugin_hot_apply_failed"
            failed["failure_reasons"] = [
                {
                    "code": str(error) or type(error).__name__,
                    "store_key": f"nonebot:{plugin['project_link']}",
                    "module_name": module_name,
                    "paths": [],
                }
            ]
            write_json(PENDING_FILE, failed)
        if action != "install" and (root := module_source_path(module_name)):
            await plugin_host.load_new_plugin(
                module_name, root, submit_restart=False, dependencies_verified=True
            )
        raise
    finalize_pending_transaction()
    old_generation = previous_manifest.get("active_generation")
    try:
        return _operation_result(
            "hot_reloaded",
            [],
            generation=build["generation"],
            previous_generation=old_generation,
        )
    except OSError as error:
        return {
            "apply_mode": "hot_reloaded",
            "generation": build["generation"],
            "previous_generation": old_generation,
            "cleanup_error": {
                "code": "operation_followup_pending",
                "diagnostic": safe_process_error(str(error)),
            },
        }


@router.get(
    "/environment",
    dependencies=[authentication()],
    response_model=Result[dict],
    response_class=JSONResponse,
)
async def dependency_environment() -> Result[dict]:
    return Result.ok(await _environment_view())


@router.post(
    "/environment/repair/preview",
    dependencies=[authentication()],
    response_model=Result[dict],
)
async def preview_dependency_repair() -> Result[dict]:
    from zhenxun.services.nonebot_store.repair import preview_layer_repair

    view = await _environment_view()
    if not view.get("repairable"):
        return Result.fail("dependency_environment_not_repairable", code=409)
    if pending_transaction() or dependency_sync_status().get("status") in {
        "prepared",
        "pending",
        "running",
    }:
        return Result.fail("dependency_transaction_pending", code=409)
    try:
        if view.get("repair_mode") == "layer":
            preview = await preview_layer_repair(view)
        else:
            ok, detail = await preflight_environment_repair()
            if not ok:
                return Result.fail(
                    f"dependency_repair_preflight_failed: {detail}", code=409
                )
            preview = {
                "mode": "base",
                "fingerprint": view["fingerprint"],
                "changes": view["immutable_drift"] + view["incompatible_shared_drift"],
                "diagnostic": detail,
            }
    except DependencyAnalysisError as error:
        return Result.fail(str(error), code=409)
    identity = _REPAIR_PREVIEWS.put(preview)
    return Result.ok(
        {
            key: value
            for key, value in {**preview, "preview_id": identity}.items()
            if key not in {"target_manifest", "base_manifest"}
        }
    )


@router.post(
    "/environment/repair",
    dependencies=[authentication()],
    response_model=Result[dict],
    response_class=JSONResponse,
)
async def repair_dependency_environment(
    payload: EnvironmentRepairPayload,
) -> Result[dict]:
    payload, binding = _bind(payload)
    return await run_bound_operation(
        str(payload.operation_id),
        binding,
        lambda: _repair_dependency_environment(payload, binding),
    )


def _bind(payload: _BoundPayload) -> tuple[_BoundPayload, str]:
    """补全操作 ID 并计算请求内容摘要

    参数:
        payload: 请求内容

    返回:
        tuple[_BoundPayload, str]: 补全后的请求与摘要
    """
    payload = payload.model_copy(
        update={"operation_id": payload.operation_id or uuid.uuid4().hex}
    )
    return payload, sha256(payload.model_dump_json().encode()).hexdigest()


async def _repair_dependency_environment(
    payload: EnvironmentRepairPayload, binding: str
) -> Result[dict]:
    identity = str(payload.operation_id)

    def record(result: dict) -> None:
        result.update(operation_id=identity, request_binding=binding)
        record_operation("dependencies:repair", result)

    recorded = False
    response = None
    try:
        async with _store_operation(
            operation_id=identity, owner="webui.nonebot-store.environment-repair"
        ):
            if replayed := replayed_bound_operation(identity, binding):
                return replayed
            record({"status": "running", "apply_mode": "preparing"})
            recorded = True
            response = await _perform_environment_repair(payload, binding)
            result = (
                (response.data or {})
                if response.suc
                else {
                    "status": "failed",
                    "apply_mode": "failed",
                    "reason": response.info,
                }
            )
            record(result)
            return response
    except StoreOperationBusyError:
        return Result.fail("plugin_operation_in_progress", code=409)
    except (Exception, asyncio.CancelledError) as error:
        if response is not None and response.suc and response.data is not None:
            response.data["cleanup_error"] = {
                "code": "operation_receipt_pending",
                "diagnostic": safe_process_error(str(error)),
            }
            return response
        transaction = pending_transaction() or {}
        staged = any(
            item.get("operation_id") == identity
            for item in transaction.get("operations", [])
        )
        result = {
            "operation_id": identity,
            "request_binding": binding,
            "status": "pending"
            if staged
            else "interrupted"
            if isinstance(error, asyncio.CancelledError)
            else "failed",
            "apply_mode": "restart_pending" if staged else "failed",
            "reason": "dependency_followup_required"
            if staged
            else getattr(error, "code", type(error).__name__),
            "diagnostic": safe_process_error(str(error)),
        }
        if recorded:
            with contextlib.suppress(Exception):
                record(result)
        if isinstance(error, asyncio.CancelledError):
            raise
        if staged:
            return Result.ok(result, info="修复事务已保存；请查询原操作，不要重复提交")
        return Result.fail(result["reason"], code=409, details=result)


async def _perform_environment_repair(
    payload: EnvironmentRepairPayload, binding: str
) -> Result[dict]:
    view = await _environment_view()
    if not payload.confirmed:
        return Result.fail("dependency_repair_confirmation_required", code=400)
    if not view.get("launcher_managed"):
        return Result.fail("dependency_repair_requires_launcher", code=409)
    if payload.expected_fingerprint != view.get("fingerprint"):
        return Result.fail("environment_analysis_stale", code=409)
    if not view.get("repairable"):
        return Result.fail("dependency_environment_not_repairable", code=409)
    preview = _REPAIR_PREVIEWS.get(payload.preview_id)
    if not preview or preview["fingerprint"] != view["fingerprint"]:
        return Result.fail("environment_analysis_stale", code=409)
    if preview["mode"] == "layer":
        build = None
        try:
            from zhenxun.services.nonebot_store.repair import select_repair_groups

            selected = select_repair_groups(preview, payload.group_ids)
            if (
                pending_transaction()
                or (await environment_report_async())["fingerprint"]
                != preview["fingerprint"]
            ):
                return Result.fail("environment_analysis_stale", code=409)
            transaction = draft_transaction(
                None,
                selected["target_manifest"],
                action="environment_repair",
                operations=[
                    {
                        "operation_id": payload.operation_id,
                        "kind": "environment_repair",
                        "request_binding": binding,
                        "name": "依赖环境修复",
                        "target_manifest": deepcopy(selected["target_manifest"]),
                        "created_at": utc_now(),
                    }
                ],
                remaining_conflicts=selected["remaining_conflicts"],
                source_build_confirmed=bool(selected.get("source_build_required")),
                state="building",
            )
            build = await prepare_generation(transaction)
            if (await environment_report_async())["fingerprint"] != preview[
                "fingerprint"
            ]:
                raise DependencyAnalysisError("environment_analysis_stale")
            stage_generation(transaction, build)
            _REPAIR_PREVIEWS.pop(payload.preview_id)
            return Result.ok(
                _operation_result(
                    "restart_pending",
                    ["dependency_environment_repair"],
                    status="pending",
                    changes=selected["changes"],
                    remaining_conflicts=selected["remaining_conflicts"],
                ),
                info="定向修复已准备完成，重启 Bot 后验证并生效",
            )
        finally:
            if (
                build is not None
                and (pending_transaction() or {}).get("generation")
                != build["generation"]
            ):
                remove_generation(build["generation"])
    ok, detail = await preflight_environment_repair()
    if not ok:
        return Result.fail(f"dependency_repair_preflight_failed: {detail}", code=409)
    if (
        pending_transaction()
        or (await environment_report_async())["fingerprint"]
        != payload.expected_fingerprint
    ):
        return Result.fail("environment_analysis_stale", code=409)
    save_dependency_sync_status(
        {
            "status": "prepared",
            "fingerprint": payload.expected_fingerprint,
            "mode": "locked_inexact",
            "operation_id": payload.operation_id,
            "request_binding": binding,
        }
    )
    return Result.ok(
        _operation_result(
            "restart_pending",
            ["dependency_environment_repair"],
            status="pending",
            operation_id=payload.operation_id,
        ),
        info="基础环境修复已准备，请点击重启执行",
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
        pending = pending_transaction()
        manifest = _effective_manifest(pending)
        entries, meta = await _catalog_sources(manifest, refresh=refresh)
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
            if (
                item["blocked_reasons"]
                and not item["managed"]
                and not include_incompatible
            ):
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
        local = _managed_plugin(project_link, _effective_manifest())
        try:
            plugin, _ = await get_registry_plugin(project_link)
        except Exception:
            if not local:
                raise
            plugin = _local_registry_item(local)
        try:
            metadata = await fetch_pypi_metadata(project_link, str(plugin["version"]))
        except Exception:
            if not local:
                raise
            metadata = {"info": {}, "urls": []}
        info = metadata["info"]
        return Result.ok(
            {
                **_catalog_item(
                    plugin,
                    manifest=_effective_manifest(),
                    inventory=installed_inventory(),
                    pending=pending_transaction(),
                ),
                "pypi": {
                    "summary": info.get("summary") or "",
                    "requires_python": info.get("requires_python") or "",
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
    pending = pending_transaction()
    if pending is not None and pending.get("state") not in {"pending_restart"}:
        return Result.fail("nonebot_transaction_pending", code=409)
    analysis = {
        "project_link": payload.project_link,
        "action": payload.action,
        "prefer_existing": payload.prefer_existing,
        "status": "queued",
        "created_at": utc_now(),
    }
    analysis_id = analysis["analysis_id"] = _ANALYSES.put(analysis)
    analysis["task"] = lifecycle_kernel.spawn_task(
        "management:webui",
        _analyze(analysis),
        name=f"nonebot-analysis:{analysis_id[:8]}",
    )
    return Result.ok(_public_analysis(analysis), info="依赖分析已开始")


@router.get(
    "/analyses/{analysis_id}",
    dependencies=[authentication()],
    response_model=Result[dict],
    response_class=JSONResponse,
)
async def analysis_status(analysis_id: str) -> Result[dict]:
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
    payload, binding = _bind(payload)
    return await run_bound_operation(
        str(payload.operation_id),
        binding,
        lambda: _apply_analysis(payload, binding),
    )


def _journal_key(analysis: dict[str, Any]) -> str:
    return f"nonebot:{analysis['registry_plugin']['project_link']}"


def _analysis_stale(analysis: dict[str, Any], revision: str) -> bool:
    """分析结果是否已与待处理事务或依赖环境不一致

    参数:
        analysis: 插件分析结果
        revision: 当前待处理事务修订

    返回:
        bool: 是否需要重新分析
    """
    return analysis.get("pending_revision", "") != revision or environment_fingerprint(
        analysis["registry_plugin"], pending_revision=revision
    ) != analysis.get("fingerprint")


def _completed(
    analysis: dict[str, Any],
    operation_id: str,
    binding: str,
    result: dict[str, Any],
    info: str,
) -> Result[dict]:
    """记录插件操作结果，并缓存供同一操作 ID 的重复请求复用

    参数:
        analysis: 插件分析结果
        operation_id: 操作 ID
        binding: 请求绑定摘要
        result: 操作结果
        info: 提示信息

    返回:
        Result[dict]: 操作结果
    """
    result.setdefault("operation_id", operation_id)
    result["request_binding"] = binding
    analysis["applied_operation_id"] = operation_id
    analysis["applied_result"] = deepcopy(result)
    try:
        record_operation(_journal_key(analysis), result)
    except OSError as error:
        result["cleanup_error"] = {
            "code": "operation_receipt_pending",
            "diagnostic": safe_process_error(str(error)),
        }
    return Result.ok(result, info=info)


def _record_failure(
    analysis: dict[str, Any],
    operation_id: str,
    binding: str,
    code: str,
    diagnostic: str = "",
) -> None:
    record_operation(
        _journal_key(analysis),
        {
            "operation_id": operation_id,
            "status": "interrupted"
            if code == "dependency_operation_interrupted"
            else "failed",
            "apply_mode": "failed",
            "reason": code,
            "request_binding": binding,
            "diagnostic": diagnostic,
        },
    )


def _merge_operation(
    pending: dict[str, Any] | None, operation: dict[str, Any]
) -> list[dict[str, Any]]:
    """把插件操作并入待处理事务，同一插件只保留最后一次操作

    尚未生效的安装随后又被卸载时，两次操作相互抵消。

    参数:
        pending: 当前待处理事务
        operation: 本次插件操作

    返回:
        list[dict[str, Any]]: 合并后的操作列表
    """
    operations = deepcopy((pending or {}).get("operations", []))
    project_link = operation["project_link"]
    index = next(
        (
            offset
            for offset, item in enumerate(operations)
            if item.get("project_link") == project_link
        ),
        None,
    )
    if index is None:
        operations.append(operation)
        return operations
    base_plugins = ((pending or {}).get("base_manifest") or {}).get("plugins", {})
    if (
        operations[index].get("action") == "install"
        and operation["action"] == "uninstall"
        and f"nonebot:{project_link}" not in base_plugins
    ):
        operations.pop(index)
    else:
        operations[index] = operation
    return operations


def _restart_reasons(
    analysis: dict[str, Any], source_runtime: dict[str, Any], native_changed: bool
) -> list[str]:
    """汇总插件变更需要重启才能生效的原因

    参数:
        analysis: 插件分析结果
        source_runtime: 新版本源码的热重载判定
        native_changed: 原生扩展是否变化

    返回:
        list[str]: 重启原因代码
    """
    plan = analysis["plan"]
    plugin = analysis["registry_plugin"]
    root = canonicalize_name(str(plugin["project_link"]))
    reasons = []
    if any(
        canonicalize_name(item["name"]) != root
        for item in plan["package_changes"]["changed"]
    ):
        reasons.append("non_core_dependencies_changed")
    if plan.get("shared_changes"):
        reasons.append("shared_dependencies_changed")
    if plan.get("compatibility_overrides"):
        reasons.append("compatibility_override_unverified")
    if native_changed:
        reasons.append("native_extensions_changed")
    if source_runtime["reload_support"] != "hot_reloadable":
        reasons.extend(
            source_runtime.get("reload_reasons") or ["plugin_not_hot_reloadable"]
        )
    runtime = plugin_host.classification_for(str(plugin["module_name"]))
    if (
        runtime["reload_support"] != "hot_reloadable"
        and analysis["action"] != "install"
    ):
        reasons.extend(runtime.get("reload_reasons") or ["plugin_not_hot_reloadable"])
    if plan.get("database_migration_possible"):
        reasons.append("database_migration_possible")
    return reasons or ["plugin_restart_required"]


async def _apply_analysis(payload: ApplyPayload, binding: str) -> Result[dict]:
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
    previous_pending = pending_transaction()
    if _analysis_stale(analysis, _pending_revision(previous_pending)):
        return Result.fail("analysis_stale", code=409)
    plan = analysis["plan"]
    if not payload.confirmed:
        return Result.fail("third_party_code_confirmation_required", code=400)
    if payload.summary_revision != plan.get("summary_revision"):
        return Result.fail("analysis_stale", code=409)
    operation_id = payload.operation_id or uuid.uuid4().hex
    plugin = analysis["registry_plugin"]
    action = analysis["action"]
    new_generation: int | None = None
    staged = False
    try:
        async with _store_operation(
            operation_id=operation_id, owner="webui.nonebot_store"
        ):
            if replayed := replayed_bound_operation(operation_id, binding):
                return replayed
            latest_pending = pending_transaction()
            if _analysis_stale(analysis, _pending_revision(latest_pending)):
                return Result.fail("analysis_stale", code=409)
            record_operation(
                _journal_key(analysis),
                {
                    "operation_id": operation_id,
                    "action": action,
                    "status": "running",
                    "request_binding": binding,
                },
            )
            target = _target_manifest(analysis)
            migration = bool(plan.get("database_migration_possible"))
            operations = _merge_operation(
                latest_pending,
                {
                    "operation_id": operation_id,
                    "analysis_id": payload.analysis_id,
                    "action": action,
                    "project_link": plugin["project_link"],
                    "module_name": plugin["module_name"],
                    "name": plugin.get("name") or plugin["project_link"],
                    "database_migration_possible": migration,
                    **_dependency_evidence(plan),
                    "source_build_confirmed": bool(plan["source_build_required"]),
                    "created_at": utc_now(),
                    "target_manifest": deepcopy(target),
                },
            )
            pending = latest_pending or {}
            if not operations:
                remove_generation(pending.get("generation"))
                finalize_orm_migration()
                clear_pending_transaction()
                return _completed(
                    analysis,
                    operation_id,
                    binding,
                    _operation_result("rolled_back", ["transaction_canceled"]),
                    "待应用插件变更已相互抵消",
                )
            transaction = draft_transaction(
                latest_pending,
                target,
                operations=operations,
                remaining_conflicts=plan.get("remaining_conflicts", []),
                source_build_confirmed=bool(
                    plan["source_build_required"]
                    or pending.get("source_build_confirmed")
                ),
                database_migration_confirmed=True,
                database_migration_possible=migration
                or bool(pending.get("database_migration_possible")),
                database_type=plan.get("database_type")
                if migration
                else pending.get("database_type", "none"),
                state="building",
            )
            if plan["source_build_required"] or (
                pending.get("source_build_confirmed")
                and not isinstance(pending.get("generation"), int)
            ):
                defer_transaction(transaction)
                staged = True
                return _completed(
                    analysis,
                    operation_id,
                    binding,
                    _operation_result("restart_pending", ["source_build_required"]),
                    "依赖事务已保存，重启后构建并生效",
                )
            hot_candidate = _hot_candidate(action, plugin, plan, latest_pending)
            previous_native = generation_native_extensions()
            build = await prepare_generation(transaction)
            new_generation = int(build["generation"])
            native_changed = previous_native != generation_native_extensions(
                build["path"]
            )
            module_name = str(plugin["module_name"])
            source_runtime = {"reload_support": "hot_reloadable", "reload_reasons": []}
            if action != "uninstall":
                source_root = module_source_path(module_name, build["path"])
                if source_root is None:
                    raise LayerBuildError("plugin_module_missing_after_install")
                # The generation was built from a resolved dependency plan, so
                # any requirements file inside the package is already satisfied
                # by the layer. Tell the classifier, or it reports a dependency
                # restart that _apply_hot can only turn into a hard failure.
                source_runtime = plugin_host.classification_for_source(
                    module_name, source_root, dependencies_verified=True
                )
            if (
                hot_candidate
                and not native_changed
                and source_runtime["reload_support"] == "hot_reloadable"
            ):
                result = await _apply_hot(analysis, transaction, build)
                return _completed(
                    analysis, operation_id, binding, result, _hot_apply_info(action)
                )
            stage_generation(transaction, build)
            staged = True
            return _completed(
                analysis,
                operation_id,
                binding,
                _operation_result(
                    "restart_pending",
                    _restart_reasons(analysis, source_runtime, native_changed),
                ),
                "插件事务已准备，重启后生效",
            )
    except asyncio.CancelledError:
        with contextlib.suppress(Exception):
            record_operation(
                _journal_key(analysis),
                {
                    "operation_id": operation_id,
                    "request_binding": binding,
                    "status": "pending" if staged else "interrupted",
                    "apply_mode": "restart_pending" if staged else "failed",
                    "reason": "dependency_operation_interrupted",
                },
            )
        raise
    except StoreOperationBusyError:
        _record_failure(analysis, operation_id, binding, "plugin_operation_in_progress")
        return Result.fail("plugin_operation_in_progress", code=409)
    except Exception as error:
        diagnostic = safe_process_error(str(error))
        if staged:
            return _completed(
                analysis,
                operation_id,
                binding,
                {
                    "apply_mode": "restart_pending",
                    "status": "pending",
                    "diagnostic": diagnostic,
                },
                "依赖事务已保存，请查询原操作状态",
            )
        if new_generation is not None:
            remove_generation(new_generation)
        if previous_pending is not None:
            save_pending_transaction(previous_pending)
        build_error = isinstance(error, LayerBuildError | DependencyAnalysisError)
        code = (getattr(error, "code", "") if build_error else "") or (
            "nonebot_plugin_apply_failed"
        )
        logger.error(f"NoneBot 插件应用失败: {code}", "WebUi", e=error)
        _record_failure(analysis, operation_id, binding, code, diagnostic)
        return Result.fail(
            code,
            code=400 if build_error else 500,
            details={
                "stage": "dependency_build"
                if build_error
                else "dependency_transaction",
                "diagnostic": diagnostic,
                "issues": getattr(error, "details", None),
            },
        )


def _cancelable(transaction: dict[str, Any]) -> bool:
    """事务是否处于可撤销状态且未被当前运行层使用

    参数:
        transaction: 待处理事务

    返回:
        bool: 是否可撤销
    """
    generation = transaction.get("generation")
    return transaction.get("state") in {
        "pending_restart",
        "migration_blocked",
        "failed",
    } and not (
        isinstance(generation, int)
        and load_manifest().get("active_generation") == generation
    )


def _clear_transaction(transaction: dict[str, Any]) -> Result[dict]:
    """撤销整个待处理事务

    参数:
        transaction: 待处理事务

    返回:
        Result[dict]: 撤销结果
    """
    remove_generation(transaction.get("generation"))
    finalize_orm_migration()
    clear_pending_transaction()
    record_manual_operations(transaction, "rolled_back", "rolled_back")
    failed = transaction.get("state") == "failed"
    return Result.ok(
        _operation_result(
            "rolled_back",
            ["failed_transaction_cleared" if failed else "transaction_canceled"],
        ),
        info="失败记录已清除" if failed else "待重启插件事务已取消",
    )


def _cancel_prepared_repair(sync: dict) -> Result[dict]:
    save_dependency_sync_status({**sync, "status": "canceled"})
    return Result.ok(
        _operation_result(
            "rolled_back",
            ["transaction_canceled"],
            operation_id=sync.get("operation_id"),
            status="rolled_back",
        ),
        info="已撤销待执行的基础环境修复，运行环境未改变",
    )


async def cancel_pending_operation(operation_id: str) -> Result[dict]:
    """撤销 NoneBot 商店待重启事务中的指定操作及其之后的操作

    参数:
        operation_id: 操作 id

    返回:
        Result[dict]: 撤销结果
    """
    try:
        async with _store_operation(
            operation_id=operation_id, owner="webui.nonebot_store"
        ):
            sync = dependency_sync_status()
            if (
                sync.get("operation_id") == operation_id
                and sync.get("status") == "prepared"
            ):
                return _cancel_prepared_repair(sync)
            transaction = pending_transaction()
            if transaction is None or not _cancelable(transaction):
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
                return _clear_transaction(transaction)
            transaction = draft_transaction(
                transaction,
                remaining[-1]["target_manifest"],
                operations=remaining,
                state="pending_restart",
            )
            if isinstance(old_generation, int):
                build = await prepare_generation(transaction)
                stage_generation(transaction, build)
            else:
                save_pending_transaction(transaction)
            record_manual_operations(
                {"operations": removed}, "rolled_back", "rolled_back"
            )
            return Result.ok(
                _operation_result("restart_pending", ["plugin_restart_required"]),
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
        async with _store_operation(
            owner="webui.nonebot_store", allow_prepared_repair=True
        ):
            sync = dependency_sync_status()
            if sync.get("status") == "prepared":
                return _cancel_prepared_repair(sync)
            transaction = pending_transaction()
            if transaction is None:
                return Result.fail("nonebot_transaction_not_found", code=404)
            if not _cancelable(transaction):
                return Result.fail("nonebot_transaction_not_cancelable", code=409)
            return _clear_transaction(transaction)
    except StoreOperationBusyError:
        return Result.fail("plugin_operation_in_progress", code=409)
    except Exception as error:
        logger.error("取消 NoneBot 插件事务失败", "WebUi", e=error)
        return Result.fail("nonebot_transaction_cancel_failed", code=500)
