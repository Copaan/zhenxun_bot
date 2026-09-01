import asyncio
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

from zhenxun.services.log import logger
from zhenxun.services.runtime_reload import plugin_runtime_manager
from zhenxun.utils.repo_utils.utils import redact_git_output

from ....apply_result import update_pending_restart
from ....base_model import Result
from ....restart_service import restart_status_data
from ....utils import authentication
from .model import PluginIr, PluginReloadPayload
from .store_receipts import StoreReceiptStore, source_digest

router = APIRouter(prefix="/store")
_STORE_OPERATION_LOCK = asyncio.Lock()
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


class StoreOperationBusyError(RuntimeError):
    pass


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
        {
            "source": source,
            "module": plugin_info.module,
            "module_path": plugin_info.module_path,
            "runtime_module": runtime_module,
            "catalog_version": str(plugin_info.version),
            "source_digest": source_digest(path),
            "installed_at": datetime.now(timezone.utc).isoformat(),
        },
    )


@asynccontextmanager
async def _store_operation() -> AsyncIterator[None]:
    if _STORE_OPERATION_LOCK.locked():
        raise StoreOperationBusyError("plugin_operation_in_progress")
    async with _STORE_OPERATION_LOCK:
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
                await plugin_runtime_manager.process_changes(
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
    return operation


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
        installed_plugins = await StoreManager.get_installed_plugins()
        receipts = StoreReceiptStore.load()
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
                    "apply_mode": None,
                    "state_reason": state_reason,
                    "update_available": update_available,
                    **plugin_runtime_manager.classification_for(
                        runtime_module or plugin.module
                    ),
                }
            )
        return Result.ok(
            {
                "install_module": list(installed_plugins),
                "plugin_list": plugin_list,
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
    try:
        async with _store_operation():
            require("plugin_store")
            from zhenxun.builtin_plugins.plugin_store import StoreManager

            request_value = _request_value(param)
            plugin_info, is_external = await StoreManager.get_plugin_by_value(
                request_value
            )
            source = _validate_requested_source(param, is_external=is_external)
            path = StoreManager._resolve_local_plugin_path(
                plugin_info, is_external=is_external
            )
            path_existed = path.exists()
            async with plugin_runtime_manager.hold_content_changes({path}):
                before = _snapshot_plugin_files(path)
                await StoreManager.add_plugin(request_value)
                after = _snapshot_plugin_files(path)
                runtime_module = _resolve_runtime_module_name(path)
                if StoreManager._last_install_had_requirements:
                    runtime_operation = await plugin_runtime_manager.request_restart(
                        {runtime_module},
                        "plugin_dependencies_installed",
                        submit_launcher=False,
                    )
                    operation = runtime_operation.public_dict()
                    _mark_store_content_processed(before, after)
                else:
                    operation = await _apply_store_change(
                        StoreManager,
                        plugin_info,
                        is_external,
                        before,
                        after=after,
                        newly_installed=True,
                    )
            if operation.get("apply_mode") == "failed":
                if not path_existed:
                    _remove_path(path)
                operation["rolled_back"] = True
            else:
                _write_receipt(
                    store_key=_store_key(source, plugin_info.module),
                    source=source,
                    plugin_info=plugin_info,
                    runtime_module=runtime_module,
                    path=path,
                )
        operation = _decorate_operation(
            operation, _store_key(source, plugin_info.module)
        )
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
        return Result.fail("plugin_runtime_module_invalid", code=400)
    except Exception as e:
        if path is not None and not path_existed:
            _remove_path(path)
        return Result.fail(f"安装插件失败: {_safe_store_error(e)}")


@router.post(
    "/update_plugin",
    dependencies=[authentication()],
    response_model=Result,
    response_class=JSONResponse,
    description="更新插件",  # type: ignore
)
async def _(param: PluginIr) -> Result:
    try:
        async with _store_operation():
            require("plugin_store")
            from zhenxun.builtin_plugins.plugin_store import StoreManager

            request_value = _request_value(param)
            plugin_info, is_external = await StoreManager.get_plugin_by_value(
                request_value, is_update=True
            )
            source = _validate_requested_source(param, is_external=is_external)
            path = StoreManager._resolve_local_plugin_path(
                plugin_info, is_external=is_external
            )
            runtime_module = _resolve_runtime_module_name(path)
            with tempfile.TemporaryDirectory(prefix="zhenxun_plugin_update_") as root:
                backup = _backup_plugin(path, Path(root))
                async with plugin_runtime_manager.hold_content_changes({path}):
                    before = _snapshot_plugin_files(path)
                    try:
                        await StoreManager.update_plugin(request_value)
                        after = _snapshot_plugin_files(path)
                        if StoreManager._last_install_had_requirements:
                            runtime_operation = (
                                await plugin_runtime_manager.request_restart(
                                    {runtime_module},
                                    "plugin_dependencies_installed",
                                    submit_launcher=False,
                                )
                            )
                            operation = runtime_operation.public_dict()
                            _mark_store_content_processed(before, after)
                        else:
                            operation = await _apply_store_change(
                                StoreManager,
                                plugin_info,
                                is_external,
                                before,
                                after=after,
                            )
                        if operation.get("apply_mode") == "failed":
                            _restore_plugin(path, backup)
                            recovery = await plugin_runtime_manager.recover_plugin(
                                runtime_module, submit_restart=False
                            )
                            operation["rolled_back"] = True
                            operation["rollback_runtime"] = recovery.mode.value
                        else:
                            _write_receipt(
                                store_key=_store_key(source, plugin_info.module),
                                source=source,
                                plugin_info=plugin_info,
                                runtime_module=runtime_module,
                                path=path,
                            )
                    except Exception:
                        _restore_plugin(path, backup)
                        await plugin_runtime_manager.recover_plugin(
                            runtime_module, submit_restart=False
                        )
                        raise
        operation = _decorate_operation(
            operation, _store_key(source, plugin_info.module)
        )
        info = _log_store_operation("更新", plugin_info.name, operation)
        return Result.ok(operation, info=info)
    except StoreOperationBusyError:
        return Result.fail("plugin_operation_in_progress", code=409)
    except Exception as e:
        return Result.fail(f"更新插件失败: {_safe_store_error(e)}")


@router.post(
    "/remove_plugin",
    dependencies=[authentication()],
    response_model=Result,
    response_class=JSONResponse,
    description="移除插件",  # type: ignore
)
async def _(param: PluginIr) -> Result:
    try:
        async with _store_operation():
            require("plugin_store")
            from zhenxun.builtin_plugins.plugin_store import StoreManager

            request_value = _request_value(param)
            plugin_info, is_external = await StoreManager.get_plugin_by_value(
                request_value, is_remove=True
            )
            source = _validate_requested_source(param, is_external=is_external)
            path = StoreManager._resolve_local_plugin_path(
                plugin_info, is_external=is_external
            )
            runtime_module = _resolve_runtime_module_name(path)
            runtime_was_loaded = "not_loaded" not in (
                plugin_runtime_manager.classification_for(runtime_module).get(
                    "reload_reasons", []
                )
            )
            pending_runtime_reason = (
                plugin_runtime_manager.last_operation.reason
                if not runtime_was_loaded
                and plugin_runtime_manager.last_operation is not None
                else None
            )
            with tempfile.TemporaryDirectory(prefix="zhenxun_plugin_remove_") as root:
                backup = _backup_plugin(path, Path(root))
                async with plugin_runtime_manager.hold_content_changes({path}):
                    before = _snapshot_plugin_files(path)
                    try:
                        await StoreManager.remove_plugin(request_value)
                        after = _snapshot_plugin_files(path)
                        if runtime_was_loaded:
                            operation = await _apply_store_change(
                                StoreManager,
                                plugin_info,
                                is_external,
                                before,
                                after=after,
                                include_requirements=False,
                            )
                        else:
                            _mark_store_content_processed(before, after)
                            if pending_runtime_reason:
                                plugin_runtime_manager.clear_pending_restart(
                                    pending_runtime_reason
                                )
                            operation = {
                                "apply_mode": "hot_reloaded",
                                "status": "completed",
                                "changed": [],
                                "reason": None,
                                "generation": plugin_runtime_manager.generation,
                            }
                        if operation.get("apply_mode") == "failed":
                            _restore_plugin(path, backup)
                            recovery = await plugin_runtime_manager.recover_plugin(
                                runtime_module, submit_restart=False
                            )
                            operation["rolled_back"] = True
                            operation["rollback_runtime"] = recovery.mode.value
                        else:
                            StoreReceiptStore.delete(
                                _store_key(source, plugin_info.module)
                            )
                    except Exception:
                        _restore_plugin(path, backup)
                        await plugin_runtime_manager.recover_plugin(
                            runtime_module, submit_restart=False
                        )
                        raise
        operation = _decorate_operation(
            operation, _store_key(source, plugin_info.module)
        )
        info = _log_store_operation("卸载", plugin_info.name, operation)
        return Result.ok(operation, info=info)
    except StoreOperationBusyError:
        return Result.fail("plugin_operation_in_progress", code=409)
    except Exception as e:
        return Result.fail(f"移除插件失败: {_safe_store_error(e)}")


@router.post(
    "/reload_plugin",
    dependencies=[authentication()],
    response_model=Result,
    response_class=JSONResponse,
    description="热重载插件",  # type: ignore
)
async def _(param: PluginReloadPayload) -> Result:
    try:
        async with _store_operation():
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
            operation = await plugin_runtime_manager.reload_plugin(module)
            operation_data = _decorate_operation(
                operation.public_dict(), param.store_key or module
            )
        return Result.ok(
            operation_data,
            info=_store_operation_info("热重载", module, operation_data),
        )
    except StoreOperationBusyError:
        return Result.fail("plugin_operation_in_progress", code=409)
    except Exception as e:
        logger.error(f"插件手动热重载失败: {_safe_store_error(e)}", "WebUi")
        return Result.fail("plugin_reload_failed")
