import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from hashlib import sha256
from pathlib import Path

from fastapi import APIRouter
from fastapi.responses import JSONResponse
from nonebot import require
from nonebot.compat import model_dump
from nonebot.utils import path_to_module_name

from zhenxun.services.log import logger
from zhenxun.services.runtime_reload import plugin_runtime_manager

from ....base_model import Result
from ....utils import authentication
from .model import PluginIr, PluginReloadPayload

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
    include_requirements: bool = True,
    newly_installed: bool = False,
) -> dict:
    path = store_manager._resolve_local_plugin_path(
        plugin_info, is_external=is_external
    )
    changed = _changed_plugin_files(
        before,
        _snapshot_plugin_files(path),
        include_requirements=include_requirements,
    )
    if newly_installed:
        module_name = _resolve_runtime_module_name(path)
        operation = await plugin_runtime_manager.load_new_plugin(
            module_name,
            path,
            changed,
        )
    else:
        operation = (
            await plugin_runtime_manager.process_changes(changed) if changed else None
        )
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


@router.get(
    "/get_plugin_store",
    dependencies=[authentication()],
    response_model=Result[dict],
    response_class=JSONResponse,
    description="获取插件商店插件信息",  # type: ignore
)
async def _() -> Result[dict]:
    try:
        require("plugin_store")
        from zhenxun.builtin_plugins.plugin_store import StoreManager

        official_plugins, community_plugins = await StoreManager.get_data()
        installed_plugins = await StoreManager.get_installed_plugins()
        plugin_list = []
        for idx, (plugin, source) in enumerate(
            [(item, "official") for item in official_plugins]
            + [(item, "community") for item in community_plugins]
        ):
            installed_version = installed_plugins.get(plugin.module)
            plugin_list.append(
                {
                    **model_dump(plugin),
                    "name": plugin.name,
                    "id": idx,
                    "source": source,
                    "capabilities": _plugin_capabilities(plugin),
                    "installed": installed_version is not None,
                    "installed_version": installed_version,
                    "update_available": bool(
                        installed_version is not None
                        and str(installed_version) != str(plugin.version)
                    ),
                    **plugin_runtime_manager.classification_for(plugin.module),
                }
            )
        return Result.ok(
            {
                "install_module": list(installed_plugins),
                "plugin_list": plugin_list,
            }
        )
    except Exception as e:
        logger.error("获取插件商店插件信息失败", "WebUi", e=e)
        return Result.fail(f"获取插件商店插件信息失败: {type(e)}: {e}")


@router.post(
    "/install_plugin",
    dependencies=[authentication()],
    response_model=Result,
    response_class=JSONResponse,
    description="安装插件",  # type: ignore
)
async def _(param: PluginIr) -> Result:
    try:
        async with _store_operation():
            require("plugin_store")
            from zhenxun.builtin_plugins.plugin_store import StoreManager

            plugin_info, is_external = await StoreManager.get_plugin_by_value(
                str(param.id)
            )
            path = StoreManager._resolve_local_plugin_path(
                plugin_info, is_external=is_external
            )
            before = _snapshot_plugin_files(path)
            await StoreManager.add_plugin(str(param.id))  # type: ignore
            operation = await _apply_store_change(
                StoreManager,
                plugin_info,
                is_external,
                before,
                newly_installed=True,
            )
        info = _log_store_operation("安装", plugin_info.name, operation)
        return Result.ok(operation, info=info)
    except StoreOperationBusyError:
        return Result.fail("plugin_operation_in_progress", code=409)
    except PluginRuntimeModuleError:
        logger.error(
            "插件安装路径无法转换为有效运行时模块名",
            "插件商店",
        )
        return Result.fail("plugin_runtime_module_invalid", code=400)
    except Exception as e:
        return Result.fail(f"安装插件失败: {type(e)}: {e}")


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

            plugin_info, is_external = await StoreManager.get_plugin_by_value(
                str(param.id), is_update=True
            )
            path = StoreManager._resolve_local_plugin_path(
                plugin_info, is_external=is_external
            )
            before = _snapshot_plugin_files(path)
            await StoreManager.update_plugin(str(param.id))  # type: ignore
            operation = await _apply_store_change(
                StoreManager, plugin_info, is_external, before
            )
        info = _log_store_operation("更新", plugin_info.name, operation)
        return Result.ok(operation, info=info)
    except StoreOperationBusyError:
        return Result.fail("plugin_operation_in_progress", code=409)
    except Exception as e:
        return Result.fail(f"更新插件失败: {type(e)}: {e}")


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

            plugin_info, is_external = await StoreManager.get_plugin_by_value(
                str(param.id), is_remove=True
            )
            path = StoreManager._resolve_local_plugin_path(
                plugin_info, is_external=is_external
            )
            before = _snapshot_plugin_files(path)
            await StoreManager.remove_plugin(str(param.id))  # type: ignore
            operation = await _apply_store_change(
                StoreManager,
                plugin_info,
                is_external,
                before,
                include_requirements=False,
            )
        info = _log_store_operation("卸载", plugin_info.name, operation)
        return Result.ok(operation, info=info)
    except StoreOperationBusyError:
        return Result.fail("plugin_operation_in_progress", code=409)
    except Exception as e:
        return Result.fail(f"移除插件失败: {type(e)}: {e}")


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
            operation = await plugin_runtime_manager.reload_plugin(param.module)
        return Result.ok(operation.public_dict(), info="插件运行时重载已处理")
    except StoreOperationBusyError:
        return Result.fail("plugin_operation_in_progress", code=409)
    except Exception as e:
        logger.error("插件手动热重载失败", "WebUi", e=e)
        return Result.fail("plugin_reload_failed")
