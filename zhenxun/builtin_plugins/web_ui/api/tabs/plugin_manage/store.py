from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from hashlib import sha256
from pathlib import Path
import shutil
import tempfile
from typing import Any

from fastapi import APIRouter
from fastapi.responses import JSONResponse
from nonebot import require
from nonebot.compat import model_dump
from nonebot.utils import path_to_module_name
from packaging.version import InvalidVersion, Version

from zhenxun.plugin_store_coordinator import (
    StoreOperationBusyError,
    plugin_store_operation_coordinator,
)
from zhenxun.plugin_store_transaction import stage_operation
from zhenxun.services.log import logger
from zhenxun.services.runtime_reload import plugin_runtime_manager
from zhenxun.utils.repo_utils.utils import redact_git_output

from ....apply_result import update_pending_restart
from ....base_model import Result
from ....restart_service import restart_status_data
from ....utils import authentication
from .model import PluginIr, PluginReloadPayload
from .operation_journal import (
    begin_operation,
    current_operation,
    operation_status,
    record_operation,
)
from .store_receipts import StoreReceiptStore, source_digest

router = APIRouter(prefix="/store")
_AI_CHAT_PLUGIN_MODULES = frozenset(
    {
        "ai",
        "bym_ai",
        "chat_toolkit",
        "leekchat",
        "multimodal_ai",
        "zhenxun_plugin_chatinter",
        "zhipu_toolkit",
    }
)


def _plugin_capabilities(plugin) -> list[str]:
    capabilities = {
        str(item).strip()
        for item in getattr(plugin, "capabilities", [])
        if str(item).strip()
    }
    if plugin.module in _AI_CHAT_PLUGIN_MODULES:
        capabilities.add("ai_chat")
    return sorted(capabilities)


class PluginRuntimeModuleError(ValueError):
    pass


def _safe_store_error(error: Exception) -> str:
    return redact_git_output(error).replace("\r", " ").replace("\n", " ")[:500]


def _store_key(source: str, module: str) -> str:
    return f"{source}:{module}"


def _request_value(param: PluginIr) -> str:
    if param.store_key:
        source, separator, module = param.store_key.partition(":")
        if separator and source in {"official", "community"} and module:
            return module
        raise ValueError("plugin_store_key_invalid")
    if param.id is None:
        raise ValueError("plugin_store_key_required")
    return str(param.id)


def _validate_requested_source(param: PluginIr, *, is_external: bool) -> str:
    source = "community" if is_external else "official"
    if param.store_key and param.store_key != _store_key(source, _request_value(param)):
        raise ValueError("plugin_store_key_source_mismatch")
    return source


def _backup_plugin(path: Path, root: Path) -> Path | None:
    if not path.exists():
        return None
    backup = root / path.name
    if path.is_dir():
        shutil.copytree(path, backup)
    else:
        shutil.copy2(path, backup)
    return backup


def _remove_path(path: Path) -> None:
    if path.is_dir():
        shutil.rmtree(path)
    else:
        path.unlink(missing_ok=True)


def _restore_plugin(path: Path, backup: Path | None) -> None:
    _remove_path(path)
    if backup is None:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    if backup.is_dir():
        shutil.copytree(backup, path)
    else:
        shutil.copy2(backup, path)


def _version_compare(installed: str | None, catalog: str) -> int | None:
    if not installed:
        return None
    try:
        current = Version(str(installed).strip().removeprefix("v"))
        target = Version(str(catalog).strip().removeprefix("v"))
    except InvalidVersion:
        return 0 if str(installed).strip() == str(catalog).strip() else None
    return (current > target) - (current < target)


def _install_state(
    *,
    installed: bool,
    installed_version: str | None,
    catalog_version: str,
    digest: str | None,
    receipt: dict[str, Any] | None,
) -> tuple[str, bool, str | None]:
    if not installed:
        return "not_installed", False, None
    comparison_version = (
        str(receipt.get("catalog_version")) if receipt else installed_version
    )
    comparison = _version_compare(comparison_version, catalog_version)
    update_available = comparison == -1
    if receipt and digest and digest != receipt.get("source_digest"):
        return "locally_modified", update_available, "local_source_changed"
    if comparison == -1:
        return "update_available", True, None
    if comparison == 1:
        return "local_ahead", False, None
    if comparison == 0:
        return "installed", False, None
    return "version_unknown", False, "installed_version_uncomparable"


def _runtime_capability(
    *,
    available: bool,
    runtime: dict[str, Any],
    unavailable_reason: str,
) -> dict[str, Any]:
    if not available:
        return {"mode": "blocked", "reason_codes": [unavailable_reason]}
    hot = runtime.get("reload_support") == "hot_reloadable"
    return {
        "mode": "hot_reloadable" if hot else "restart_required",
        "reason_codes": list(runtime.get("reload_reasons") or []),
    }


def _receipt_data(
    *,
    source: str,
    plugin_info: Any,
    runtime_module: str,
    path: Path,
) -> dict[str, Any]:
    return {
        "source": source,
        "module": plugin_info.module,
        "module_path": plugin_info.module_path,
        "runtime_module": runtime_module,
        "catalog_version": str(plugin_info.version),
        "source_digest": source_digest(path),
        "installed_at": datetime.now(timezone.utc).isoformat(),
    }


def _write_receipt(
    *,
    store_key: str,
    source: str,
    plugin_info: Any,
    runtime_module: str,
    path: Path,
) -> None:
    StoreReceiptStore.set(
        store_key,
        _receipt_data(
            source=source,
            plugin_info=plugin_info,
            runtime_module=runtime_module,
            path=path,
        ),
    )


def _dependency_inputs(install_result: Any) -> list[str]:
    plan = install_result.dependency_plan
    values = plan.get("candidate_inputs") or []
    return [str(value) for value in values if isinstance(value, str)]


def _dependency_packages(install_result: Any) -> dict[str, str]:
    plan = install_result.dependency_plan
    changes = plan.get("package_changes") or {}
    result = {
        str(item["name"]): str(item["version"])
        for item in changes.get("added", [])
        if isinstance(item, dict) and item.get("name") and item.get("version")
    }
    result.update(
        {
            str(item["name"]): str(item["to"])
            for item in changes.get("changed", [])
            if isinstance(item, dict) and item.get("name") and item.get("to")
        }
    )
    return result


@asynccontextmanager
async def _store_operation(
    *, operation_id: str | None = None, owner: str | None = None
) -> AsyncIterator[None]:
    async with plugin_store_operation_coordinator.operation(
        operation_id=operation_id, owner=owner
    ):
        yield


def _snapshot_plugin_files(path: Path) -> dict[Path, str]:
    if path.is_file():
        candidates = [path]
    elif path.is_dir():
        candidates = [item for item in path.rglob("*") if item.is_file()]
    else:
        candidates = []
    result: dict[Path, str] = {}
    for item in candidates:
        try:
            result[item.resolve()] = sha256(item.read_bytes()).hexdigest()
        except OSError:
            continue
    return result


def _changed_plugin_files(
    before: dict[Path, str], after: dict[Path, str], *, include_requirements: bool
) -> set[Path]:
    changed = {
        path
        for path in before.keys() | after.keys()
        if before.get(path) != after.get(path)
    }
    if not include_requirements:
        changed = {
            path
            for path in changed
            if path.name not in {"requirement.txt", "requirements.txt"}
        }
    return changed


def _mark_store_content_processed(
    before: dict[Path, str], after: dict[Path, str]
) -> None:
    """Absorb watchfiles events produced by the current store transaction."""
    for path in before.keys() | after.keys():
        plugin_runtime_manager.mark_content_processed(path)


def _resolve_runtime_module_name(path: Path) -> str:
    """Resolve an installed store path using the same naming as NoneBot scanning."""
    project_root = Path.cwd().resolve()
    plugin_root = (project_root / "zhenxun" / "plugins").resolve()
    resolved_path = path.resolve()
    try:
        resolved_path.relative_to(plugin_root)
    except ValueError as e:
        raise PluginRuntimeModuleError("plugin_runtime_module_invalid") from e

    if resolved_path.is_file() and resolved_path.suffix == ".py":
        entrypoint = resolved_path
    elif resolved_path.is_dir() and (resolved_path / "__init__.py").is_file():
        entrypoint = resolved_path / "__init__.py"
    else:
        raise PluginRuntimeModuleError("plugin_runtime_module_invalid")

    try:
        module_name = path_to_module_name(entrypoint)
    except ValueError as e:
        raise PluginRuntimeModuleError("plugin_runtime_module_invalid") from e
    if not module_name or any(
        not part.isidentifier() for part in module_name.split(".")
    ):
        raise PluginRuntimeModuleError("plugin_runtime_module_invalid")
    return module_name


def _resolve_uninstall_runtime_module_name(path: Path) -> str:
    """Resolve a plugin module even when only runtime-created files remain."""
    try:
        return _resolve_runtime_module_name(path)
    except PluginRuntimeModuleError:
        project_root = Path.cwd().resolve()
        plugin_root = (project_root / "zhenxun" / "plugins").resolve()
        resolved_path = path.resolve()
        try:
            relative_path = resolved_path.relative_to(plugin_root)
        except ValueError as e:
            raise PluginRuntimeModuleError("plugin_runtime_module_invalid") from e
        if (
            not resolved_path.is_dir()
            or not relative_path.parts
            or any(not part.isidentifier() for part in relative_path.parts)
        ):
            raise PluginRuntimeModuleError("plugin_runtime_module_invalid")
        module_name = ".".join(("zhenxun", "plugins", *relative_path.parts))
        if any(not part.isidentifier() for part in module_name.split(".")):
            raise PluginRuntimeModuleError("plugin_runtime_module_invalid")
        return module_name


def _store_operation_info(action: str, plugin_name: str, operation: dict) -> str:
    mode = operation.get("apply_mode")
    if mode == "hot_reloaded":
        if action == "热重载":
            return f"插件 {plugin_name} 已热重载"
        return f"插件 {plugin_name} 已{action}并热加载"
    if mode == "restart_requested":
        return f"插件 {plugin_name} 已{action}，已请求受控重启"
    if mode == "restart_pending":
        return f"插件 {plugin_name} 已{action}，等待重启后生效"
    return f"插件 {plugin_name} 文件已{action}，但运行时应用失败"


def _log_store_operation(action: str, plugin_name: str, operation: dict) -> str:
    info = _store_operation_info(action, plugin_name, operation)
    if operation.get("apply_mode") == "failed":
        reason = operation.get("reason") or "plugin_runtime_apply_failed"
        logger.error(f"{info} reason={reason}", "插件商店")
    else:
        logger.info(info, "插件商店")
    return info


async def _apply_store_change(
    store_manager,
    plugin_info,
    is_external: bool,
    before: dict[Path, str],
    *,
    after: dict[Path, str] | None = None,
    include_requirements: bool = True,
    newly_installed: bool = False,
) -> dict:
    path = store_manager._resolve_local_plugin_path(
        plugin_info, is_external=is_external
    )
    after = after if after is not None else _snapshot_plugin_files(path)
    changed = _changed_plugin_files(
        before,
        after,
        include_requirements=include_requirements,
    )
    try:
        if newly_installed:
            module_name = _resolve_runtime_module_name(path)
            try:
                operation = await plugin_runtime_manager.load_new_plugin(
                    module_name,
                    path,
                    changed,
                    submit_restart=False,
                )
            except TypeError as error:
                if "submit_restart" not in str(error):
                    raise
                operation = await plugin_runtime_manager.load_new_plugin(
                    module_name, path, changed
                )
        else:
            operation = (
                await plugin_runtime_manager.apply_plugin_changes(
                    changed, submit_restart=False
                )
                if changed
                else None
            )
    finally:
        _mark_store_content_processed(before, after)
    if operation is None and changed:
        operation = plugin_runtime_manager.last_operation
    return (
        operation.public_dict()
        if operation
        else {
            "apply_mode": "hot_reloaded",
            "status": "completed",
            "changed": [],
            "reason": None,
            "generation": plugin_runtime_manager.generation,
        }
    )


def _decorate_operation(operation: dict, store_key: str) -> dict:
    mode = str(operation.get("apply_mode") or "failed")
    reason = str(operation.get("reason") or "").strip()
    source = f"webui.plugin:{store_key}"
    restart_required = mode in {"restart_pending", "restart_requested"}
    if restart_required:
        launcher_managed = update_pending_restart(
            source,
            [reason or "plugin_change_requires_restart"],
            issue_ticket=False,
        )
        if launcher_managed:
            from zhenxun.utils._restart_utils import issue_restart_ticket

            issue_restart_ticket("webui.settings", ttl_seconds=10 * 60)
    elif mode != "failed" or operation.get("rolled_back"):
        update_pending_restart(source, [], issue_ticket=False)
    status = restart_status_data()
    operation.update(
        {
            "restart_required": restart_required,
            "restart_available": restart_required and status["launcher_managed"],
            "reason_codes": [reason] if reason else [],
            "access_urls": status["access_urls"],
            "access_targets": status["access_targets"],
        }
    )
    record_operation(store_key, operation)
    return operation


def _record_failed_operation(
    operation_id: str | None, store_key: str | None, reason: str
) -> None:
    if not operation_id or not store_key:
        return
    record_operation(
        store_key,
        {
            "operation_id": operation_id,
            "status": "failed",
            "apply_mode": "failed",
            "reason": reason,
            "reason_codes": [reason],
            "rolled_back": True,
        },
    )


def _replayed_operation(
    operation_id: str | None, store_key: str
) -> dict[str, Any] | None:
    if not operation_id:
        return None
    entry = operation_status(operation_id)
    if not entry:
        return None
    if entry.get("store_key") != store_key:
        raise ValueError("plugin_operation_id_conflict")
    if entry.get("status") == "running":
        raise StoreOperationBusyError("plugin_operation_in_progress")
    result = entry.get("result")
    return result if isinstance(result, dict) else None


@router.get(
    "/get_plugin_store",
    dependencies=[authentication()],
    response_model=Result[dict],
    response_class=JSONResponse,
    description="获取插件商店插件信息",  # type: ignore
)
async def _(refresh: bool = False) -> Result[dict]:
    try:
        require("plugin_store")
        from zhenxun.builtin_plugins.plugin_store import StoreManager

        if refresh:
            await StoreManager.invalidate_cache()

        official_plugins, community_plugins = await StoreManager.get_data()
        catalog_health = await StoreManager.catalog_health(
            official_plugins, refresh=refresh
        )
        installed_plugins = await StoreManager.get_installed_plugins()
        receipts = StoreReceiptStore.load()
        from zhenxun.plugin_store_transaction import public_transaction

        pending_transaction = public_transaction()
        pending_by_key = {
            str(item.get("store_key")): item
            for item in (pending_transaction or {}).get("operations", [])
        }
        plugin_list = []
        for idx, (plugin, source) in enumerate(
            [(item, "official") for item in official_plugins]
            + [(item, "community") for item in community_plugins]
        ):
            store_key = _store_key(source, plugin.module)
            receipt = receipts.get(store_key)
            path = StoreManager._resolve_local_plugin_path(
                plugin, is_external=source == "community"
            )
            installed_version = installed_plugins.get(plugin.module)
            installed = path.exists() or installed_version is not None
            digest = source_digest(path) if installed else None
            runtime_module = str((receipt or {}).get("runtime_module") or "")
            if installed and not runtime_module:
                try:
                    runtime_module = _resolve_runtime_module_name(path)
                except PluginRuntimeModuleError:
                    runtime_module = plugin.module
            state, update_available, state_reason = _install_state(
                installed=installed,
                installed_version=installed_version,
                catalog_version=str(plugin.version),
                digest=digest,
                receipt=receipt,
            )
            health = catalog_health.get(
                plugin.module, {"status": "unknown", "reason": None}
            )
            blocked_reasons = (
                [{"code": str(health["reason"])}]
                if health.get("reason") == "catalog_source_missing"
                else []
            )
            pending_operation = pending_by_key.get(store_key)
            runtime = plugin_runtime_manager.classification_for(
                runtime_module or plugin.module
            )
            plugin_list.append(
                {
                    **model_dump(plugin),
                    "name": plugin.name,
                    "id": idx,
                    "source": source,
                    "store_key": store_key,
                    "capabilities": _plugin_capabilities(plugin),
                    "installed": installed,
                    "installed_version": installed_version,
                    "runtime_module": runtime_module or None,
                    "install_state": state,
                    "source_digest": digest,
                    "apply_mode": ("restart_pending" if pending_operation else None),
                    "pending_action": (
                        pending_operation.get("action") if pending_operation else None
                    ),
                    "pending_operation_id": (
                        pending_operation.get("operation_id")
                        if pending_operation
                        else None
                    ),
                    "transaction_state": (
                        pending_transaction.get("state")
                        if pending_operation and pending_transaction
                        else None
                    ),
                    "state_reason": state_reason,
                    "catalog_status": health.get("status", "unknown"),
                    "blocked_reasons": blocked_reasons,
                    "update_available": update_available,
                    "install_capability": _runtime_capability(
                        available=not installed and not blocked_reasons,
                        runtime=runtime,
                        unavailable_reason=(
                            "catalog_source_missing"
                            if blocked_reasons
                            else "plugin_already_installed"
                        ),
                    ),
                    "update_capability": _runtime_capability(
                        available=installed and update_available,
                        runtime=runtime,
                        unavailable_reason=(
                            "plugin_update_not_available"
                            if installed
                            else "plugin_not_installed"
                        ),
                    ),
                    "uninstall_capability": _runtime_capability(
                        available=installed,
                        runtime=runtime,
                        unavailable_reason="plugin_not_installed",
                    ),
                    **runtime,
                }
            )
        return Result.ok(
            {
                "install_module": list(installed_plugins),
                "plugin_list": plugin_list,
                "catalog_warnings": StoreManager._catalog_warnings,
            }
        )
    except Exception as e:
        safe_error = _safe_store_error(e)
        logger.error(f"获取插件商店插件信息失败: {safe_error}", "WebUi")
        return Result.fail(f"获取插件商店插件信息失败: {safe_error}")


@router.post(
    "/install_plugin",
    dependencies=[authentication()],
    response_model=Result,
    response_class=JSONResponse,
    description="安装插件",  # type: ignore
)
async def _(param: PluginIr) -> Result:
    path: Path | None = None
    path_existed = False
    journal_operation_id: str | None = None
    journal_store_key: str | None = None
    try:
        async with _store_operation(
            operation_id=param.operation_id, owner="webui.plugin_store"
        ):
            require("plugin_store")
            from zhenxun.builtin_plugins.plugin_store import StoreManager

            request_value = _request_value(param)
            plugin_info, is_external = await StoreManager.get_plugin_by_value(
                request_value
            )
            if not is_external:
                health = await StoreManager.catalog_health([plugin_info])
                if health.get(plugin_info.module, {}).get("status") == "missing":
                    return Result.fail("catalog_source_missing", code=409)
            source = _validate_requested_source(param, is_external=is_external)
            journal_store_key = _store_key(source, plugin_info.module)
            if replayed := _replayed_operation(param.operation_id, journal_store_key):
                return Result.ok(replayed, info="重复请求已复用原操作结果")
            journal_operation_id = begin_operation(
                journal_store_key, "install", param.operation_id
            )
            path = StoreManager._resolve_local_plugin_path(
                plugin_info, is_external=is_external
            )
            path_existed = path.exists()
            async with plugin_runtime_manager.hold_content_changes({path}):
                before = _snapshot_plugin_files(path)
                install_result = await StoreManager.add_plugin(
                    request_value,
                    install_dependencies=False,
                    confirm_source_build=param.confirm_source_build,
                    return_result=True,
                )
                after = _snapshot_plugin_files(path)
                runtime_module = _resolve_runtime_module_name(path)
                _mark_store_content_processed(before, after)
            if install_result.dependency_changes:
                runtime_operation = await plugin_runtime_manager.request_restart(
                    {runtime_module},
                    "plugin_dependencies_changed",
                    submit_launcher=False,
                )
                operation = runtime_operation.public_dict()
            else:
                operation = await _apply_store_change(
                    StoreManager,
                    plugin_info,
                    is_external,
                    before,
                    after=after,
                    include_requirements=False,
                    newly_installed=True,
                )
            if operation.get("apply_mode") == "failed":
                if not path_existed:
                    _remove_path(path)
                operation["rolled_back"] = True
            elif operation.get("apply_mode") == "restart_pending":
                receipt = _receipt_data(
                    source=source,
                    plugin_info=plugin_info,
                    runtime_module=runtime_module,
                    path=path,
                )
                staged = stage_operation(
                    action="install",
                    store_key=_store_key(source, plugin_info.module),
                    module=plugin_info.module,
                    runtime_module=runtime_module,
                    live_path=path,
                    candidate_path=path,
                    base_source_path=path.parent / f".{path.name}.missing",
                    base_digest="missing",
                    receipt=receipt,
                    reason=str(operation.get("reason") or "plugin_restart_required"),
                    dependency_inputs=_dependency_inputs(install_result),
                    dependency_packages=_dependency_packages(install_result),
                    source_build_confirmed=param.confirm_source_build,
                    operation_id=journal_operation_id,
                )
                async with plugin_runtime_manager.hold_content_changes({path}):
                    before_staging = _snapshot_plugin_files(path)
                    _remove_path(path)
                    _mark_store_content_processed(before_staging, {})
                operation.update(staged)
            else:
                _write_receipt(
                    store_key=_store_key(source, plugin_info.module),
                    source=source,
                    plugin_info=plugin_info,
                    runtime_module=runtime_module,
                    path=path,
                )
        operation.setdefault("operation_id", journal_operation_id)
        operation = _decorate_operation(operation, journal_store_key)
        info = _log_store_operation("安装", plugin_info.name, operation)
        return Result.ok(operation, info=info)
    except StoreOperationBusyError:
        return Result.fail("plugin_operation_in_progress", code=409)
    except PluginRuntimeModuleError:
        if path is not None and not path_existed:
            _remove_path(path)
        logger.error(
            "插件安装路径无法转换为有效运行时模块名",
            "插件商店",
        )
        _record_failed_operation(
            journal_operation_id,
            journal_store_key,
            "plugin_runtime_module_invalid",
        )
        return Result.fail("plugin_runtime_module_invalid", code=400)
    except Exception as e:
        if path is not None and not path_existed:
            _remove_path(path)
        reason = _safe_store_error(e)
        _record_failed_operation(journal_operation_id, journal_store_key, reason)
        if reason == "source_build_confirmation_required":
            return Result.fail(reason, code=409)
        return Result.fail(f"安装插件失败: {reason}")


@router.post(
    "/update_plugin",
    dependencies=[authentication()],
    response_model=Result,
    response_class=JSONResponse,
    description="更新插件",  # type: ignore
)
async def _(param: PluginIr) -> Result:
    journal_operation_id: str | None = None
    journal_store_key: str | None = None
    try:
        async with _store_operation(
            operation_id=param.operation_id, owner="webui.plugin_store"
        ):
            require("plugin_store")
            from zhenxun.builtin_plugins.plugin_store import StoreManager

            request_value = _request_value(param)
            plugin_info, is_external = await StoreManager.get_plugin_by_value(
                request_value, is_update=True
            )
            source = _validate_requested_source(param, is_external=is_external)
            journal_store_key = _store_key(source, plugin_info.module)
            if replayed := _replayed_operation(param.operation_id, journal_store_key):
                return Result.ok(replayed, info="重复请求已复用原操作结果")
            journal_operation_id = begin_operation(
                journal_store_key, "update", param.operation_id
            )
            path = StoreManager._resolve_local_plugin_path(
                plugin_info, is_external=is_external
            )
            runtime_module = _resolve_runtime_module_name(path)
            with tempfile.TemporaryDirectory(prefix="zhenxun_plugin_update_") as root:
                backup = _backup_plugin(path, Path(root))
                try:
                    async with plugin_runtime_manager.hold_content_changes({path}):
                        before = _snapshot_plugin_files(path)
                        install_result = await StoreManager.update_plugin(
                            request_value,
                            install_dependencies=False,
                            confirm_source_build=param.confirm_source_build,
                            return_result=True,
                        )
                        after = _snapshot_plugin_files(path)
                        _mark_store_content_processed(before, after)
                    if install_result.dependency_changes:
                        runtime_operation = (
                            await plugin_runtime_manager.request_restart(
                                {runtime_module},
                                "plugin_dependencies_changed",
                                submit_launcher=False,
                            )
                        )
                        operation = runtime_operation.public_dict()
                    else:
                        operation = await _apply_store_change(
                            StoreManager,
                            plugin_info,
                            is_external,
                            before,
                            after=after,
                            include_requirements=False,
                        )
                    if operation.get("apply_mode") == "failed":
                        async with plugin_runtime_manager.hold_content_changes({path}):
                            failed = _snapshot_plugin_files(path)
                            _restore_plugin(path, backup)
                            restored = _snapshot_plugin_files(path)
                            _mark_store_content_processed(failed, restored)
                        recovery = await plugin_runtime_manager.recover_plugin(
                            runtime_module, submit_restart=False
                        )
                        operation["rolled_back"] = True
                        operation["rollback_runtime"] = recovery.mode.value
                    elif operation.get("apply_mode") == "restart_pending":
                        receipt = _receipt_data(
                            source=source,
                            plugin_info=plugin_info,
                            runtime_module=runtime_module,
                            path=path,
                        )
                        staged = stage_operation(
                            action="update",
                            store_key=_store_key(source, plugin_info.module),
                            module=plugin_info.module,
                            runtime_module=runtime_module,
                            live_path=path,
                            candidate_path=path,
                            base_source_path=backup,
                            receipt=receipt,
                            reason=str(
                                operation.get("reason") or "plugin_restart_required"
                            ),
                            dependency_inputs=_dependency_inputs(install_result),
                            dependency_packages=_dependency_packages(install_result),
                            source_build_confirmed=param.confirm_source_build,
                            operation_id=journal_operation_id,
                        )
                        async with plugin_runtime_manager.hold_content_changes({path}):
                            before_restore = _snapshot_plugin_files(path)
                            _restore_plugin(path, backup)
                            restored = _snapshot_plugin_files(path)
                            _mark_store_content_processed(before_restore, restored)
                        operation.update(staged)
                    else:
                        _write_receipt(
                            store_key=_store_key(source, plugin_info.module),
                            source=source,
                            plugin_info=plugin_info,
                            runtime_module=runtime_module,
                            path=path,
                        )
                except Exception:
                    async with plugin_runtime_manager.hold_content_changes({path}):
                        failed = _snapshot_plugin_files(path)
                        _restore_plugin(path, backup)
                        restored = _snapshot_plugin_files(path)
                        _mark_store_content_processed(failed, restored)
                    await plugin_runtime_manager.recover_plugin(
                        runtime_module, submit_restart=False
                    )
                    raise
        operation.setdefault("operation_id", journal_operation_id)
        operation = _decorate_operation(operation, journal_store_key)
        info = _log_store_operation("更新", plugin_info.name, operation)
        return Result.ok(operation, info=info)
    except StoreOperationBusyError:
        return Result.fail("plugin_operation_in_progress", code=409)
    except Exception as e:
        reason = _safe_store_error(e)
        _record_failed_operation(journal_operation_id, journal_store_key, reason)
        if reason == "source_build_confirmation_required":
            return Result.fail(reason, code=409)
        return Result.fail(f"更新插件失败: {reason}")


@router.post(
    "/remove_plugin",
    dependencies=[authentication()],
    response_model=Result,
    response_class=JSONResponse,
    description="移除插件",  # type: ignore
)
async def _(param: PluginIr) -> Result:
    journal_operation_id: str | None = None
    journal_store_key: str | None = None
    try:
        async with _store_operation(
            operation_id=param.operation_id, owner="webui.plugin_store"
        ):
            require("plugin_store")
            from zhenxun.builtin_plugins.plugin_store import StoreManager

            request_value = _request_value(param)
            if param.store_key:
                from zhenxun.plugin_store_transaction import (
                    cancel_operation,
                    pending_transaction,
                )

                pending = pending_transaction() or {}
                pending_install = next(
                    (
                        item
                        for item in pending.get("operations", [])
                        if item.get("store_key") == param.store_key
                        and item.get("action") == "install"
                    ),
                    None,
                )
                if pending_install:
                    operation = cancel_operation(str(pending_install["operation_id"]))
                    operation["status"] = "completed"
                    operation["reason"] = "transaction_canceled"
                    operation = _decorate_operation(operation, param.store_key)
                    return Result.ok(operation, info="待安装插件操作已撤销")
            plugin_info, is_external = await StoreManager.get_plugin_by_value(
                request_value, is_remove=True
            )
            source = _validate_requested_source(param, is_external=is_external)
            journal_store_key = _store_key(source, plugin_info.module)
            if replayed := _replayed_operation(param.operation_id, journal_store_key):
                return Result.ok(replayed, info="重复请求已复用原操作结果")
            journal_operation_id = begin_operation(
                journal_store_key, "uninstall", param.operation_id
            )
            path = StoreManager._resolve_local_plugin_path(
                plugin_info, is_external=is_external
            )
            runtime_module = _resolve_uninstall_runtime_module_name(path)
            runtime_classification = plugin_runtime_manager.classification_for(
                runtime_module
            )
            runtime_was_loaded = "not_loaded" not in runtime_classification.get(
                "reload_reasons", []
            )
            if (
                runtime_was_loaded
                and runtime_classification.get("reload_support") != "hot_reloadable"
            ):
                from zhenxun.services.plugin_init import PluginInitManager

                legacy_remove = any(
                    registered == runtime_module
                    or registered.startswith(f"{runtime_module}.")
                    for registered, model in PluginInitManager.plugins.items()
                    if model.remove
                )
                if legacy_remove:
                    _record_failed_operation(
                        journal_operation_id,
                        journal_store_key,
                        "legacy_lifecycle_not_transactional",
                    )
                    return Result.fail("legacy_lifecycle_not_transactional", code=409)
                reason = str(
                    (
                        runtime_classification.get("reload_reasons")
                        or ["plugin_not_hot_reloadable"]
                    )[0]
                )
                operation = stage_operation(
                    action="uninstall",
                    store_key=_store_key(source, plugin_info.module),
                    module=plugin_info.module,
                    runtime_module=runtime_module,
                    live_path=path,
                    candidate_path=None,
                    receipt=None,
                    reason=reason,
                    operation_id=journal_operation_id,
                )
                operation.update(
                    {
                        "status": "pending_restart",
                        "changed": [plugin_info.module],
                        "reason": reason,
                        "generation": plugin_runtime_manager.generation,
                    }
                )
                operation = _decorate_operation(
                    operation, _store_key(source, plugin_info.module)
                )
                info = _log_store_operation("卸载", plugin_info.name, operation)
                return Result.ok(operation, info=info)
            pending_runtime_reason = (
                plugin_runtime_manager.last_operation.reason
                if not runtime_was_loaded
                and plugin_runtime_manager.last_operation is not None
                else None
            )
            if runtime_was_loaded:
                runtime_operation = await plugin_runtime_manager.unload_plugin(
                    runtime_module
                )
                operation = runtime_operation.public_dict()
                if operation.get("apply_mode") == "failed":
                    operation.setdefault("operation_id", journal_operation_id)
                    operation["rolled_back"] = True
                    operation = _decorate_operation(
                        operation, _store_key(source, plugin_info.module)
                    )
                    info = _log_store_operation("卸载", plugin_info.name, operation)
                    return Result.ok(operation, info=info)
            else:
                operation = {
                    "apply_mode": "hot_reloaded",
                    "status": "completed",
                    "changed": [],
                    "reason": None,
                    "generation": plugin_runtime_manager.generation,
                }
            with tempfile.TemporaryDirectory(prefix="zhenxun_plugin_remove_") as root:
                backup = _backup_plugin(path, Path(root))
                try:
                    async with plugin_runtime_manager.hold_content_changes({path}):
                        before = _snapshot_plugin_files(path)
                        await StoreManager.remove_plugin(request_value)
                        after = _snapshot_plugin_files(path)
                        _mark_store_content_processed(before, after)
                    if pending_runtime_reason:
                        plugin_runtime_manager.clear_pending_restart(
                            pending_runtime_reason
                        )
                    StoreReceiptStore.delete(_store_key(source, plugin_info.module))
                except Exception:
                    async with plugin_runtime_manager.hold_content_changes({path}):
                        failed = _snapshot_plugin_files(path)
                        _restore_plugin(path, backup)
                        restored = _snapshot_plugin_files(path)
                        _mark_store_content_processed(failed, restored)
                    if runtime_was_loaded:
                        await plugin_runtime_manager.recover_plugin(
                            runtime_module, submit_restart=False
                        )
                    raise
        operation.setdefault("operation_id", journal_operation_id)
        operation = _decorate_operation(operation, journal_store_key)
        info = _log_store_operation("卸载", plugin_info.name, operation)
        return Result.ok(operation, info=info)
    except StoreOperationBusyError:
        return Result.fail("plugin_operation_in_progress", code=409)
    except Exception as e:
        reason = _safe_store_error(e)
        _record_failed_operation(journal_operation_id, journal_store_key, reason)
        return Result.fail(f"移除插件失败: {reason}")


@router.post(
    "/reload_plugin",
    dependencies=[authentication()],
    response_model=Result,
    response_class=JSONResponse,
    description="热重载插件",  # type: ignore
)
async def _(param: PluginReloadPayload) -> Result:
    journal_operation_id: str | None = None
    journal_store_key: str | None = None
    try:
        async with _store_operation(
            operation_id=param.operation_id, owner="webui.plugin_store"
        ):
            module = param.module
            if param.store_key:
                receipt = StoreReceiptStore.load().get(param.store_key)
                if receipt:
                    module = str(receipt.get("runtime_module") or "")
                if not module:
                    source, separator, catalog_module = param.store_key.partition(":")
                    if not separator or source not in {"official", "community"}:
                        raise ValueError("plugin_store_key_invalid")
                    require("plugin_store")
                    from zhenxun.builtin_plugins.plugin_store import StoreManager

                    plugin_info, is_external = await StoreManager.get_plugin_by_value(
                        catalog_module, is_update=True
                    )
                    path = StoreManager._resolve_local_plugin_path(
                        plugin_info, is_external=is_external
                    )
                    module = _resolve_runtime_module_name(path)
            if not module:
                raise ValueError("plugin_runtime_module_required")
            journal_store_key = param.store_key or module
            if replayed := _replayed_operation(param.operation_id, journal_store_key):
                return Result.ok(replayed, info="重复请求已复用原操作结果")
            journal_operation_id = begin_operation(
                journal_store_key, "reload", param.operation_id
            )
            operation = await plugin_runtime_manager.reload_plugin(module)
            operation_data = operation.public_dict()
            operation_data.setdefault("operation_id", journal_operation_id)
            operation_data = _decorate_operation(operation_data, journal_store_key)
        return Result.ok(
            operation_data,
            info=_store_operation_info("热重载", module, operation_data),
        )
    except StoreOperationBusyError:
        return Result.fail("plugin_operation_in_progress", code=409)
    except Exception as e:
        logger.error(f"插件手动热重载失败: {_safe_store_error(e)}", "WebUi")
        _record_failed_operation(
            journal_operation_id,
            journal_store_key,
            "plugin_reload_failed",
        )
        return Result.fail("plugin_reload_failed")


@router.get(
    "/operations/current",
    dependencies=[authentication()],
    response_model=Result[dict],
    response_class=JSONResponse,
)
async def get_current_store_operation() -> Result[dict]:
    return Result.ok({"operation": current_operation()})


@router.get(
    "/operations/{operation_id}",
    dependencies=[authentication()],
    response_model=Result[dict],
    response_class=JSONResponse,
)
async def get_store_operation(operation_id: str) -> Result[dict]:
    operation = operation_status(operation_id)
    if operation is None:
        return Result.fail("plugin_operation_not_found", code=404)
    return Result.ok(operation)


@router.get(
    "/transactions/pending",
    dependencies=[authentication()],
    response_model=Result[dict],
    response_class=JSONResponse,
)
async def get_unified_pending_transaction() -> Result[dict]:
    from zhenxun.nonebot_store.storage import pending_transaction as nonebot_pending
    from zhenxun.plugin_store_transaction import public_transaction

    return Result.ok(
        {
            "zhenxun": public_transaction(),
            "nonebot": nonebot_pending(),
        }
    )


@router.delete(
    "/transactions/pending/{operation_id}",
    dependencies=[authentication()],
    response_model=Result[dict],
    response_class=JSONResponse,
)
async def cancel_unified_pending_operation(operation_id: str) -> Result[dict]:
    from zhenxun.plugin_store_transaction import (
        cancel_operation,
        pending_transaction,
    )

    transaction = pending_transaction()
    source_operation = next(
        (
            item
            for item in (transaction or {}).get("operations", [])
            if item.get("operation_id") == operation_id
        ),
        None,
    )
    if source_operation:
        result = cancel_operation(operation_id)
        update_pending_restart(
            f"webui.plugin:{source_operation.get('store_key', '')}",
            [],
            issue_ticket=False,
        )
        return Result.ok(result, info="待重启插件操作已撤销")
    from .nonebot_store import cancel_pending_operation

    return await cancel_pending_operation(operation_id)


@router.post(
    "/transactions/cancel",
    dependencies=[authentication()],
    response_model=Result[dict],
    response_class=JSONResponse,
)
async def cancel_unified_transaction() -> Result[dict]:
    from zhenxun.nonebot_store.storage import pending_transaction as nonebot_pending
    from zhenxun.plugin_store_transaction import (
        cancel_operation,
        pending_transaction,
    )

    removed: list[str] = []
    source = pending_transaction()
    if source:
        for operation in source.get("operations", []):
            update_pending_restart(
                f"webui.plugin:{operation.get('store_key', '')}",
                [],
                issue_ticket=False,
            )
        result = cancel_operation()
        removed.extend(result.get("removed_operation_ids", []))
    had_nonebot = bool(nonebot_pending())
    if had_nonebot:
        from .nonebot_store import cancel_transaction

        response = await cancel_transaction()
        if not response.suc:
            return response
        response_data = response.data if isinstance(response.data, dict) else {}
        removed.extend(response_data.get("removed_operation_ids", []))
    if not removed and not had_nonebot and not source:
        return Result.fail("plugin_transaction_not_found", code=404)
    return Result.ok(
        {"apply_mode": "rolled_back", "removed_operation_ids": removed},
        info="待重启插件事务已取消",
    )
