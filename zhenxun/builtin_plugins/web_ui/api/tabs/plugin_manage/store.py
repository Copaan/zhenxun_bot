import asyncio
from collections.abc import AsyncIterator, Callable, Coroutine
from contextlib import asynccontextmanager
from pathlib import Path
import time
from typing import Any, Literal
import uuid

from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse
from nonebot import get_loaded_plugins, require
from nonebot.compat import model_dump
from packaging.version import InvalidVersion, Version

from zhenxun.services.log import logger
from zhenxun.services.plugin_host import plugin_host
from zhenxun.services.plugin_store.operation_journal import (
    begin_operation,
    operation_status,
    record_operation,
)
from zhenxun.services.plugin_store.plugin_store_receipts import (
    StoreReceiptStore,
    artifact_version,
    source_digest,
)
from zhenxun.services.runtime_mutation import (
    StoreOperationBusyError,
    store_operation,
)
from zhenxun.utils.repo_utils.utils import redact_git_output

from ....apply_result import update_pending_restart
from ....base_model import Result
from ....restart_service import restart_status_data
from ....utils import authentication
from .model import PluginIr, PluginReloadPayload

router = APIRouter(prefix="/store")
_ACTIVE_OPERATIONS: dict[str, tuple[str, asyncio.Task]] = {}
_AI_CHAT_PLUGIN_MODULES = frozenset(
    {
        "ai",
        "chatinter",
        "bym_ai",
        "chat_toolkit",
        "leekchat",
        "multimodal_ai",
        "zhenxun_plugin_chatinter",
        "zhipu_toolkit",
    }
)


class PreviewStore:
    """限时保存依赖分析与预览，过期或超出容量时淘汰，仍在运行任务的项保留"""

    def __init__(self, ttl: float = 1200, limit: int = 64) -> None:
        self.ttl = ttl
        self.limit = limit
        self._items: dict[str, tuple[float, dict[str, Any]]] = {}

    @staticmethod
    def _running(value: dict[str, Any]) -> bool:
        task = value.get("task")
        return isinstance(task, asyncio.Task) and not task.done()

    def _prune(self) -> None:
        now = time.monotonic()
        for key, (created, value) in list(self._items.items()):
            if now - created > self.ttl and not self._running(value):
                del self._items[key]

    def put(self, value: dict[str, Any]) -> str:
        """保存一项并返回新标识

        参数:
            value: 分析或预览内容

        返回:
            str: 标识
        """
        self._prune()
        if len(self._items) >= self.limit:
            idle = [
                key for key, (_, item) in self._items.items() if not self._running(item)
            ]
            if idle:
                del self._items[idle[0]]
        key = uuid.uuid4().hex
        self._items[key] = (time.monotonic(), value)
        return key

    def get(self, key: str | None) -> dict[str, Any] | None:
        """读取未过期的项

        参数:
            key: 标识

        返回:
            dict[str, Any] | None: 内容，不存在或已过期时为 None
        """
        self._prune()
        item = self._items.get(key or "")
        return item[1] if item else None

    def pop(self, key: str | None) -> None:
        self._items.pop(key or "", None)


def _plugin_capabilities(plugin) -> list[str]:
    capabilities = {
        str(item).strip()
        for item in getattr(plugin, "capabilities", [])
        if str(item).strip()
    }
    if plugin.module in _AI_CHAT_PLUGIN_MODULES:
        capabilities.add("ai_chat")
    return sorted(capabilities)


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


def _version_compare(installed: str | None, catalog: str) -> int | None:
    """比较已安装版本与目标版本

    参数:
        installed: 已安装版本
        catalog: 目标版本

    返回:
        int | None: -1 较旧、0 相同、1 较新，无法比较时为 None
    """
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
    installed_version: str | None,
    catalog_version: str,
    digest: str | None,
    receipt: dict[str, Any] | None,
) -> tuple[str, bool]:
    """判断已安装插件相对商店目录的状态

    参数:
        installed_version: 插件元数据或数据库记录中的版本
        catalog_version: 商店目录中的版本
        digest: 本地源码摘要
        receipt: 安装回执

    返回:
        tuple[str, bool]: 安装状态与是否可更新
    """
    if not installed_version and receipt and digest == receipt.get("source_digest"):
        installed_version = receipt.get("installed_version")
    comparison = _version_compare(installed_version, catalog_version)
    if receipt and digest != receipt.get("source_digest"):
        return "locally_modified", comparison == -1
    if comparison is None:
        return "version_unknown", False
    return {
        -1: ("update_available", True),
        0: ("installed", False),
        1: ("local_ahead", False),
    }[comparison]


def _read_local_artifacts(
    targets: list[tuple[Path, bool]],
) -> list[tuple[bool, str | None, str | None]]:
    """读取商店插件对应的本地文件状态

    参数:
        targets: 插件本地路径与是否需要计算源码摘要

    返回:
        list[tuple[bool, str | None, str | None]]: 是否存在、元数据版本、源码摘要
    """
    artifacts = []
    for path, need_digest in targets:
        exists = path.exists()
        artifacts.append(
            (
                exists,
                artifact_version(path) if exists else None,
                source_digest(path) if exists and need_digest else None,
            )
        )
    return artifacts


def _warm_reload_classification() -> None:
    """预先计算已加载扩展插件的热处理分类，填充源码分析缓存"""
    try:
        for plugin in list(get_loaded_plugins()):
            if plugin.parent_plugin is None and plugin.module_name.startswith(
                "zhenxun.plugins."
            ):
                plugin_host.classification_for(plugin.module_name)
    except Exception as error:
        logger.debug(f"预热插件热处理分类失败: {type(error).__name__}", "WebUi")


@asynccontextmanager
async def _store_operation(
    *,
    operation_id: str | None = None,
    owner: str | None = None,
    allow_prepared_repair: bool = False,
) -> AsyncIterator[None]:
    async with store_operation(operation_id=operation_id, owner=owner):
        from zhenxun.services.nonebot_store.storage import (
            archive_settled,
            dependency_sync_status,
        )

        archive_settled()
        sync = dependency_sync_status()
        if (
            sync.get("status") in {"prepared", "pending", "running"}
            and not (allow_prepared_repair and sync.get("status") == "prepared")
            and sync.get("operation_id") != operation_id
        ):
            raise StoreOperationBusyError("dependency_transaction_pending")
        yield


def replayed_bound_operation(identity: str, binding: str) -> Result | None:
    """按操作 ID 复用已记录的结果

    参数:
        identity: 操作 ID
        binding: 请求内容摘要

    返回:
        Result | None: 已有记录时返回原结果，请求内容不同时返回冲突
    """
    previous = operation_status(identity)
    if not previous:
        return None
    result = previous.get("result") or {}
    if result.get("request_binding") != binding:
        return Result.fail("plugin_operation_input_changed", code=409)
    return Result.ok(result, info="重复请求已复用原操作结果")


async def run_bound_operation(
    identity: str, binding: str, run: Callable[[], Coroutine[Any, Any, Result]]
) -> Result:
    """按操作 ID 运行请求，连接断开后任务继续执行，重复请求复用同一任务或结果

    参数:
        identity: 操作 ID
        binding: 请求内容摘要
        run: 创建实际任务的函数

    返回:
        Result: 操作结果
    """
    active = _ACTIVE_OPERATIONS.get(identity)
    if active:
        if active[0] != binding:
            return Result.fail("plugin_operation_input_changed", code=409)
        return await asyncio.shield(active[1])
    if replayed := replayed_bound_operation(identity, binding):
        return replayed
    task = asyncio.create_task(run())
    _ACTIVE_OPERATIONS[identity] = (binding, task)
    task.add_done_callback(lambda _: _ACTIVE_OPERATIONS.pop(identity, None))
    return await asyncio.shield(task)


def _log_store_operation(action: str, plugin_name: str, operation: dict) -> str:
    require("plugin_store")
    from zhenxun.builtin_plugins.plugin_store import StoreManager

    info = StoreManager.operation_info(action, plugin_name, operation)
    if operation.get("apply_mode") == "failed":
        reason = operation.get("reason") or "plugin_runtime_apply_failed"
        logger.error(f"{info} reason={reason}", "插件商店")
    else:
        logger.info(info, "插件商店")
    return info


def _decorate_operation(operation: dict, store_key: str) -> dict:
    mode = str(operation.get("apply_mode") or "failed")
    reason = str(operation.get("reason") or "").strip()
    source = f"webui.plugin:{store_key}"
    restart_required = mode in {"restart_pending", "restart_requested"}
    if restart_required:
        launcher_managed = update_pending_restart(
            source,
            [reason or "plugin_change_requires_restart"],
        )
        if launcher_managed:
            from zhenxun.utils._restart_utils import issue_restart_ticket

            issue_restart_ticket("webui.settings", ttl_seconds=10 * 60)
    elif mode != "failed" or operation.get("rolled_back"):
        update_pending_restart(source, [])
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
    operation_id: str | None, store_key: str, download_source: str | None = None
) -> dict[str, Any] | None:
    if not operation_id:
        return None
    entry = operation_status(operation_id)
    if not entry:
        return None
    if entry.get("store_key") != store_key:
        raise ValueError("plugin_operation_id_conflict")
    if (
        download_source is not None
        and entry.get("result", {}).get("download_source", "auto") != download_source
    ):
        raise HTTPException(409, detail="plugin_download_source_conflict")
    if entry.get("status") == "running":
        raise StoreOperationBusyError("plugin_operation_in_progress")
    if entry.get("status") == "interrupted":
        # The process that owned this operation died before it reached a terminal
        # state, so there is no outcome to replay -- returning the partial record
        # would report a half-finished install as a completed one. Retrying under
        # the same operation id is safe: ``begin_operation`` overwrites the entry,
        # and on-disk state is reconciled by the pending transaction and receipts.
        return None
    result = entry.get("result")
    return result if isinstance(result, dict) else None


def _finish_operation(
    operation: dict, store_key: str, action: str, plugin_name: str
) -> Result:
    """登记操作结果并生成响应

    参数:
        operation: 操作结果
        store_key: 商店键
        action: 操作名称
        plugin_name: 插件名称

    返回:
        Result: 响应
    """
    operation = _decorate_operation(operation, store_key)
    return Result.ok(
        operation, info=_log_store_operation(action, plugin_name, operation)
    )


@router.get(
    "/get_plugin_store",
    dependencies=[authentication()],
    response_model=Result[dict],
    response_class=JSONResponse,
    description="获取插件商店插件信息",  # type: ignore
)
async def _(
    refresh: bool = False, download_source: Literal["auto", "ali", "git"] = "auto"
) -> Result[dict]:
    try:
        require("plugin_store")
        from zhenxun.builtin_plugins.plugin_store import StoreManager
        from zhenxun.services.plugin_store.plugin_store_transaction import (
            public_transaction,
        )

        source = None if download_source == "auto" else download_source
        (official_plugins, community_plugins), _, _ = await asyncio.gather(
            StoreManager.get_data(refresh=refresh, source=source),
            StoreManager.catalog_files(refresh=refresh, source=source),
            asyncio.to_thread(_warm_reload_classification),
        )
        catalog_health = await StoreManager.catalog_health(
            official_plugins, source=source
        )
        installed_plugins = await StoreManager.get_installed_plugins()
        receipts = StoreReceiptStore.load()
        transaction = public_transaction() or {}
        transaction_failed = transaction.get("state") == "failed"
        pending_by_key = {
            str(item.get("store_key")): item
            for item in transaction.get("operations", [])
        }
        entries = [(item, "official") for item in official_plugins] + [
            (item, "community") for item in community_plugins
        ]
        artifacts = await asyncio.to_thread(
            _read_local_artifacts,
            [
                (
                    StoreManager._resolve_local_plugin_path(item),
                    _store_key(item_source, item.module) in receipts,
                )
                for item, item_source in entries
            ],
        )
        plugin_list = []
        for idx, ((plugin, source), artifact) in enumerate(zip(entries, artifacts)):
            exists, local_version, digest = artifact
            store_key = _store_key(source, plugin.module)
            installed_version = installed_plugins.get(plugin.module)
            pending = pending_by_key.get(store_key) or {}
            item = {
                **model_dump(plugin),
                "name": plugin.name,
                "id": idx,
                "source": source,
                "store_key": store_key,
                "capabilities": _plugin_capabilities(plugin),
                "installed": exists or installed_version is not None,
                "installed_version": installed_version,
                "runtime_module": None,
                "install_state": "not_installed",
                "update_available": False,
                "catalog_status": catalog_health.get(plugin.module, {}).get(
                    "status", "unknown"
                ),
                "apply_mode": (
                    ("failed" if transaction_failed else "restart_pending")
                    if pending
                    else None
                ),
                "pending_action": pending.get("action"),
                "pending_operation_id": pending.get("operation_id"),
                "failure_reasons": (
                    transaction.get("failure_reasons", [])
                    if pending and transaction_failed
                    else []
                ),
            }
            if item["installed"]:
                receipt = receipts.get(store_key)
                runtime_module = str(
                    (receipt or {}).get("runtime_module")
                    or StoreManager.runtime_module(plugin)
                )
                state, update_available = _install_state(
                    installed_version=local_version or installed_version,
                    catalog_version=str(plugin.version),
                    digest=digest if receipt else None,
                    receipt=receipt,
                )
                item.update(
                    runtime_module=runtime_module,
                    install_state=state,
                    update_available=update_available,
                    parked=plugin_host.is_parked(runtime_module),
                    **plugin_host.classification_for(runtime_module),
                )
            plugin_list.append(item)
        return Result.ok({"plugin_list": plugin_list})
    except Exception as e:
        safe_error = _safe_store_error(e)
        logger.error(f"获取插件商店插件信息失败: {safe_error}", "WebUi")
        return Result.fail(f"获取插件商店插件信息失败: {safe_error}")


async def _install_or_update(param: PluginIr, action: str) -> Result:
    """安装或更新商店插件

    参数:
        param: 插件请求
        action: install 或 update

    返回:
        Result: 操作结果
    """
    label = "安装" if action == "install" else "更新"
    journal_operation_id: str | None = None
    journal_store_key: str | None = None
    try:
        async with _store_operation(
            operation_id=param.operation_id, owner="webui.plugin_store"
        ):
            if param.store_key and (
                replayed := _replayed_operation(
                    param.operation_id, param.store_key, param.download_source
                )
            ):
                return Result.ok(replayed, info="重复请求已复用原操作结果")
            require("plugin_store")
            from zhenxun.builtin_plugins.plugin_store import StoreManager

            requested_source = (
                None if param.download_source == "auto" else param.download_source
            )
            plugin_info, is_external = await StoreManager.get_plugin_by_value(
                _request_value(param),
                is_update=action == "update",
                source=requested_source,
            )
            if action == "install" and not is_external:
                health = await StoreManager.catalog_health(
                    [plugin_info], source=requested_source
                )
                if health.get(plugin_info.module, {}).get("status") == "missing":
                    return Result.fail("catalog_source_missing", code=409)
            source = _validate_requested_source(param, is_external=is_external)
            journal_store_key = _store_key(source, plugin_info.module)
            if replayed := _replayed_operation(
                param.operation_id, journal_store_key, param.download_source
            ):
                return Result.ok(replayed, info="重复请求已复用原操作结果")
            journal_operation_id = begin_operation(
                journal_store_key,
                action,
                param.operation_id,
                download_source=param.download_source,
            )
            if action == "update" and await StoreManager.is_latest(plugin_info):
                operation = {
                    "apply_mode": "hot_reloaded",
                    "status": "completed",
                    "changed": [],
                    "reason": "already_latest",
                    "unchanged": True,
                    "generation": plugin_host.generation,
                }
            else:
                operation = await StoreManager.apply_plugin(
                    plugin_info,
                    is_external,
                    requested_source,
                    confirm_source_build=param.confirm_source_build,
                    operation_id=journal_operation_id,
                )
        operation.setdefault("operation_id", journal_operation_id)
        operation["download_source"] = param.download_source
        return _finish_operation(operation, journal_store_key, label, plugin_info.name)
    except HTTPException:
        raise
    except StoreOperationBusyError:
        return Result.fail("plugin_operation_in_progress", code=409)
    except Exception as e:
        reason = _safe_store_error(e)
        _record_failed_operation(journal_operation_id, journal_store_key, reason)
        if reason == "source_build_confirmation_required":
            return Result.fail(reason, code=409)
        return Result.fail(f"{label}插件失败: {reason}")


@router.post(
    "/install_plugin",
    dependencies=[authentication()],
    response_model=Result,
    response_class=JSONResponse,
    description="安装插件",  # type: ignore
)
async def _(param: PluginIr) -> Result:
    return await _install_or_update(param, "install")


@router.post(
    "/update_plugin",
    dependencies=[authentication()],
    response_model=Result,
    response_class=JSONResponse,
    description="更新插件",  # type: ignore
)
async def _(param: PluginIr) -> Result:
    return await _install_or_update(param, "update")


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
            from zhenxun.services.plugin_store.plugin_store_transaction import (
                cancel_operation,
                pending_transaction,
            )

            request_value = _request_value(param)
            if param.store_key and (
                replayed := _replayed_operation(param.operation_id, param.store_key)
            ):
                return Result.ok(replayed, info="重复请求已复用原操作结果")
            pending_install = next(
                (
                    item
                    for item in (pending_transaction() or {}).get("operations", [])
                    if param.store_key
                    and item.get("store_key") == param.store_key
                    and item.get("action") == "install"
                ),
                None,
            )
            if pending_install and param.store_key:
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
            operation = await StoreManager.uninstall_plugin(
                plugin_info, is_external, operation_id=journal_operation_id
            )
        operation.setdefault("operation_id", journal_operation_id)
        return _finish_operation(operation, journal_store_key, "卸载", plugin_info.name)
    except HTTPException:
        raise
    except StoreOperationBusyError:
        return Result.fail("plugin_operation_in_progress", code=409)
    except Exception as e:
        reason = _safe_store_error(e)
        _record_failed_operation(journal_operation_id, journal_store_key, reason)
        if reason == "legacy_lifecycle_not_transactional":
            return Result.fail(reason, code=409)
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

                    plugin_info, _ = await StoreManager.get_plugin_by_value(
                        catalog_module, is_update=True
                    )
                    module = StoreManager.runtime_module(plugin_info)
            if not module:
                raise ValueError("plugin_runtime_module_required")
            journal_store_key = param.store_key or module
            if replayed := _replayed_operation(param.operation_id, journal_store_key):
                return Result.ok(replayed, info="重复请求已复用原操作结果")
            journal_operation_id = begin_operation(
                journal_store_key, "reload", param.operation_id
            )
            operation = await plugin_host.reload_plugin(module)
            operation_data = operation.public_dict()
            operation_data.setdefault("operation_id", journal_operation_id)
            operation_data = _decorate_operation(operation_data, journal_store_key)
        return Result.ok(
            operation_data,
            info=_log_store_operation("热重载", module, operation_data),
        )
    except HTTPException:
        raise
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
    "/operations/{operation_id}",
    dependencies=[authentication()],
    response_model=Result[dict],
    response_class=JSONResponse,
)
async def get_store_operation(operation_id: str) -> Result[dict]:
    operation = operation_status(operation_id)
    active = _ACTIVE_OPERATIONS.get(operation_id)
    if operation is None and active and not active[1].done():
        operation = {
            "operation_id": operation_id,
            "status": "running",
            "result": {"status": "running", "apply_mode": "preparing"},
        }
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
    from zhenxun.services.nonebot_store.storage import (
        pending_transaction as nonebot_pending,
    )
    from zhenxun.services.plugin_store.plugin_store_transaction import (
        public_transaction,
    )

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
    from zhenxun.services.plugin_store.plugin_store_transaction import (
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
        try:
            async with _store_operation(
                operation_id=operation_id, owner="webui.plugin_store"
            ):
                result = cancel_operation(operation_id)
        except StoreOperationBusyError:
            return Result.fail("plugin_operation_in_progress", code=409)
        except RuntimeError as exc:
            return Result.fail(str(exc), code=409)
        update_pending_restart(
            f"webui.plugin:{source_operation.get('store_key', '')}",
            [],
        )
        return Result.ok(result, info="待重启插件操作已撤销")
    from .nonebot_store import cancel_pending_operation

    return await cancel_pending_operation(operation_id)
