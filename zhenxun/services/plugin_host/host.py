"""插件热加载、卸载、重载与重启请求

插件身份与登记全部以 NoneBot 自身的插件表为准：卸载时按 ``registry`` 中的
注册表逐一移除插件登记，加载时直接使用 NoneBot 的导入机制。
无法热处理的插件（见 ``classifier``）改为请求重启。
"""

from __future__ import annotations

import asyncio
import importlib
import os
from pathlib import Path
import sys
from typing import Any

import nonebot
from nonebot.matcher import current_matcher
from nonebot.plugin import (
    Plugin,
    _current_plugin,
    _managers,
    _new_plugin,
    _plugins,
    _revert_plugin,
)
from nonebot.plugin.manager import PluginManager

from zhenxun.services.log import logger
from zhenxun.services.runtime_mutation import runtime_mutation_coordinator
from zhenxun.utils.atomic_json import write_json_locked
from zhenxun.utils.enum import PriorityLifecycleType

from . import registry
from .classifier import (
    DEPENDENCY_FILES,
    PluginClassifier,
    find_plugin,
    plugin_root,
    source_files,
)
from .models import ApplyMode, RuntimeOperation

INDEX_FILE = Path("data/runtime/loaded-plugins-v1.json")
CONFIG_RESTART_REASON = "import_time_config_consumer_requires_restart"
_DRAIN_TIMEOUT = 10.0
_CANCEL_TIMEOUT = 5.0
_HOOK_TIMEOUT = 30.0
_LOG_COMMAND = "插件热重载"


def current_owner() -> str | None:
    """当前代码所属的插件 id

    返回:
        str | None: 导入期为正在加载的插件，事件处理期为当前 matcher 的插件
    """
    if (plugin := _current_plugin.get()) is not None:
        return plugin.id_
    matcher = current_matcher.get(None)
    return getattr(matcher, "plugin_id", None) if matcher is not None else None


def _restore_kept_plugin(name: str) -> None:
    """保留的模型模块原本是子插件时，在新的父插件下重新登记"""
    module = sys.modules.get(name)
    if module is None or isinstance(getattr(module, "__plugin__", None), Plugin):
        return
    manager = next(
        (m for m in reversed(_managers) if name in m.controlled_modules.values()),
        None,
    )
    if manager is None:
        return
    parent = None
    candidate = name.rpartition(".")[0]
    while candidate and parent is None:
        parent = next(
            (p for p in list(_plugins.values()) if p.module_name == candidate), None
        )
        candidate = candidate.rpartition(".")[0]
    token = _current_plugin.set(parent)
    try:
        plugin = _new_plugin(name, module, manager)
    finally:
        _current_plugin.reset(token)
    module.__plugin__ = plugin  # type: ignore[attr-defined]
    plugin.metadata = getattr(module, "__plugin_meta__", None)


def same_path(left: Path, right: str) -> bool:
    return os.path.normcase(str(left.resolve())) == os.path.normcase(
        str(Path(right).resolve())
    )


def _ensure_manager(module_name: str, root: Path | None) -> None:
    """让 NoneBot 的插件查找器能找到该模块"""
    if any(module_name in m.controlled_modules.values() for m in _managers):
        return
    if root is not None:
        for manager in _managers:
            if any(same_path(root.parent, path) for path in manager.search_path):
                leaf = module_name.rsplit(".", 1)[-1]
                manager._searched_plugin_ids[leaf] = module_name
                return
    _managers.append(PluginManager([module_name]))


async def _call(func: Any, notes: list[str], *args: Any) -> None:
    try:
        result = func(*args)
        if asyncio.iscoroutine(result):
            await asyncio.wait_for(result, timeout=_HOOK_TIMEOUT)
    except Exception as error:
        name = getattr(registry._unwrap(args[0] if args else func), "__name__", "?")
        notes.append(f"shutdown_hook_failed:{name}:{type(error).__name__}")
        logger.warning(f"插件关闭钩子执行失败: {name}", _LOG_COMMAND, e=error)


async def _sync_shop_goods() -> None:
    """把 shop_register 中尚未注册使用方法的商品补登记到商店"""
    module = sys.modules.get("zhenxun.builtin_plugins.shop._data_source")
    if module is None:
        return
    from zhenxun.models.goods_info import GoodsInfo
    from zhenxun.utils.decorator.shop import shop_register

    shop_manage = module.ShopManage
    for name, goods in list(shop_register._data.items()):
        uuid = await GoodsInfo.add_goods(
            name,
            goods.price,
            goods.des,
            goods.discount,
            goods.limit_time,
            goods.daily_limit,
            goods.is_passive,
            goods.partition,
            goods.icon,
        )
        if uuid and uuid not in shop_manage.uuid2goods:
            await shop_manage.register_use(
                name,
                uuid,
                goods.func,
                goods.send_success_msg,
                goods.max_num_limit,
                goods.before_handle,
                goods.after_handle,
                **goods.kwargs,
            )


def _registered_configs(plugin: Plugin) -> set[tuple[str, str]]:
    """插件元数据中声明的配置项 (模块, 键)"""
    result = set()
    for item in [plugin, *registry.iter_sub_plugins(plugin)]:
        extra = getattr(item.metadata, "extra", None) or {}
        for config in extra.get("configs") or []:
            data = config if isinstance(config, dict) else vars(config)
            if key := data.get("key"):
                result.add((data.get("module") or item.name, str(key).upper()))
    return result


def _forget_configs(configs: set[tuple[str, str]]) -> None:
    from zhenxun.configs.config import Config

    for module, key in configs:
        entry = f"{module}:{key}".lower()
        while entry in Config.add_module:
            Config.add_module.remove(entry)
        if (group := Config._data.get(module)) is not None:
            group.configs.pop(key, None)
            if not group.configs:
                Config._data.pop(module, None)


class PluginHost:
    """插件热加载、卸载、重载，以及无法热处理时的重启请求"""

    def __init__(self) -> None:
        self.generation = 0
        """插件代际，每次热操作后递增"""
        self.last_operation: RuntimeOperation | None = None
        self.pending_restart: set[str] = set()
        """需要重启才能生效的变更原因"""
        self.classifier = PluginClassifier()

    # ------------------------------------------------------------ 查询
    def classification_for(self, module: str) -> dict[str, Any]:
        """已加载插件的热处理分类，见 PluginClassifier.classification_for"""
        return self.classifier.classification_for(module, self.generation)

    def classification_for_source(
        self, module: str, root: Path, *, dependencies_verified: bool = False
    ) -> dict[str, Any]:
        """未加载插件源码的热加载分类，见 PluginClassifier.classification_for_source"""
        return self.classifier.classification_for_source(
            module, root, dependencies_verified=dependencies_verified
        )

    def status(self) -> dict[str, Any]:
        """插件运行时状态"""
        return {
            "generation": self.generation,
            "pending_restart": bool(self.pending_restart),
            "pending_restart_reasons": sorted(self.pending_restart),
            "last_operation": (
                self.last_operation.public_dict() if self.last_operation else None
            ),
        }

    @staticmethod
    def write_index() -> None:
        """记录当前已加载的插件模块，供 launcher 启动验证读取"""
        modules = sorted({plugin.module_name for plugin in list(_plugins.values())})
        try:
            write_json_locked(INDEX_FILE, {"modules": modules})
        except OSError as error:
            logger.warning(f"已加载插件索引写入失败: {type(error).__name__}")

    # ------------------------------------------------------------ 卸载与加载
    @staticmethod
    async def _stop_hooks(
        collected: dict[str, list[Any]], modules: set[str], notes: list[str]
    ) -> None:
        from zhenxun.services.lifecycle import lifecycle_kernel
        from zhenxun.utils.manager.priority_manager import (
            _paired_shutdown_hooks,
            _run_hook,
            _shutdown_only_hooks,
            lifecycle_component_ids,
        )

        component_ids = {
            component_id
            for component_id in lifecycle_component_ids(modules)
            if lifecycle_kernel.component_status(component_id)
        }
        if component_ids:
            try:
                await lifecycle_kernel.stop_components(component_ids)
            except Exception as error:
                notes.append(f"component_stop_failed:{type(error).__name__}")
        try:
            hooks = {**_paired_shutdown_hooks(), **_shutdown_only_hooks()}
        except Exception:
            hooks = {}
        covered = {
            item[1]
            for component_id, item in hooks.items()
            if component_id in component_ids
        }
        for hook_type, priority, func in collected.get("priority_lifecycle", []):
            if hook_type == PriorityLifecycleType.SHUTDOWN and func not in covered:
                await _call(_run_hook, notes, func, priority, "shutdown")
        for func in reversed(collected.get("lifespan_shutdown", [])):
            await _call(func, notes)

    async def _teardown(
        self, own: registry.Ownership, plugins: list[Plugin], notes: list[str]
    ) -> list[str]:
        """移除插件全部登记、结束其任务并卸载模块

        参数:
            own: 归属
            plugins: 插件及其子插件
            notes: 过程记录

        返回:
            list[str]: 因模型保留、需在重新加载后恢复登记的子插件模块
        """
        from zhenxun.configs.config import Config

        collected = registry.collect(own)
        # 先移除 matcher 阻止新事件进入，再等待正在处理的事件结束
        registry.remove({"matcher": collected.pop("matcher", [])})
        _, handling = registry.owned_tasks(own)
        if handling:
            await asyncio.wait(handling, timeout=_DRAIN_TIMEOUT)
        await self._stop_hooks(collected, set(own.modules()), notes)
        registry.remove(collected)
        background, handling = registry.owned_tasks(own)
        tasks = [*background, *handling]
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.wait(tasks, timeout=_CANCEL_TIMEOUT)
            if alive := [task for task in tasks if not task.done()]:
                notes.append(f"tasks_not_finished:{len(alive)}")
        kept = registry.model_modules(own)
        kept_plugins = []
        for name in sorted(kept):
            module = sys.modules[name]
            if isinstance(getattr(module, "__plugin__", None), Plugin):
                kept_plugins.append(name)
                delattr(module, "__plugin__")
        for plugin in sorted(plugins, key=lambda p: p.id_.count(":"), reverse=True):
            if _plugins.get(plugin.id_) is plugin:
                _revert_plugin(plugin)
        registry.drop_modules(own, kept)
        registry.clear_runtime_caches()
        importlib.invalidate_caches()
        Config.forget_import_readers(own.plugin_ids)
        return kept_plugins

    async def _purge(self, module_name: str, root: Path | None, notes: list[str]):
        """清理加载失败时已产生的部分登记"""
        prefix = registry.Ownership.build([module_name], [], [])
        plugins = [
            p for p in list(_plugins.values()) if prefix.module_owned(p.module_name)
        ]
        own = registry.Ownership.build(
            [module_name, *(p.module_name for p in plugins)],
            [p.id_ for p in plugins],
            [root] if root else [],
        )
        try:
            await self._teardown(own, plugins, notes)
        except Exception as error:
            notes.append(f"purge_failed:{type(error).__name__}")
            logger.error(
                f"插件 {module_name} 加载失败后的清理未完成", _LOG_COMMAND, e=error
            )

    async def _load(
        self, module_name: str, root: Path | None, kept: list[str], notes: list[str]
    ) -> Plugin:
        """导入插件并执行其初始化与启动钩子，失败时清理已产生的登记

        参数:
            module_name: 模块名
            root: 插件包目录或模块文件
            kept: 需要恢复登记的子插件模块
            notes: 过程记录

        返回:
            Plugin: 加载后的插件
        """
        from zhenxun.services.lifecycle import lifecycle_kernel
        from zhenxun.services.plugin_init import PluginInitManager
        from zhenxun.utils.manager.priority_manager import (
            _sync_kernel_declarations,
            lifecycle_component_ids,
        )

        if module_name in sys.modules:
            raise RuntimeError("plugin_module_still_loaded")
        before = {
            kind: list(registry.lifespan_funcs(kind)) for kind in ("startup", "ready")
        }
        _ensure_manager(module_name, root)
        try:
            module = importlib.import_module(module_name)
            plugin = getattr(module, "__plugin__", None)
            if not isinstance(plugin, Plugin):
                raise RuntimeError("module_not_loaded_as_plugin")
            for name in kept:
                _restore_kept_plugin(name)
            own = registry.Ownership.of(plugin)
            for key in list(PluginInitManager.plugins):
                if own.module_owned(key):
                    await PluginInitManager.install(key, raise_on_error=True)
            _sync_kernel_declarations()
            if component_ids := lifecycle_component_ids(set(own.modules())):
                await lifecycle_kernel.start_components(component_ids)
                for component_id in sorted(component_ids):
                    status = lifecycle_kernel.component_status(component_id) or {}
                    if status.get("state") != "ready":
                        raise RuntimeError(
                            f"plugin_initialization_failed:{component_id}"
                        )
            lifespan = nonebot.get_driver()._lifespan
            for kind in ("startup", "ready"):
                if funcs := [
                    func
                    for func in registry.lifespan_funcs(kind)
                    if func not in before[kind] and own.owned(func)
                ]:
                    await lifespan._run_lifespan_func(funcs)
        except BaseException:
            await self._purge(module_name, root, notes)
            raise
        return plugin

    # ------------------------------------------------------------ 提交
    @staticmethod
    async def _reconcile(notes: list[str]) -> None:
        from tortoise import Tortoise

        if not Tortoise._inited:
            return
        try:
            from zhenxun.builtin_plugins.init.init_config import (
                reconcile_config_runtime,
            )
            from zhenxun.builtin_plugins.init.init_plugin import (
                reconcile_plugin_runtime,
            )
            from zhenxun.builtin_plugins.init.init_task import reconcile_task_runtime
            from zhenxun.services.plugin_policy import plugin_policy_service

            reconcile_config_runtime()
            await reconcile_plugin_runtime()
            await reconcile_task_runtime()
            await plugin_policy_service.reconcile()
            await _sync_shop_goods()
        except Exception as error:
            notes.append(f"metadata_reconcile_failed:{type(error).__name__}")
            logger.error("插件元数据协调失败", _LOG_COMMAND, e=error)
        cache = sys.modules.get("zhenxun.services.cache.runtime_cache")
        if cache is not None:
            try:
                await cache.PluginInfoMemoryCache.refresh()
                await cache.TaskInfoMemoryCache.refresh()
            except Exception as error:
                notes.append(f"cache_refresh_failed:{type(error).__name__}")

    async def _commit(
        self, loaded: list[Plugin], unloaded: list[str], notes: list[str]
    ) -> None:
        from zhenxun.services.startup_load import startup_load_planner

        self.generation += 1
        for plugin_id in unloaded:
            if plugin_id not in _plugins and (
                entry := startup_load_planner.entries.get(plugin_id)
            ):
                entry.status = "unloaded"
        if loaded:
            startup_load_planner.commit_runtime_recovery(
                {
                    module
                    for plugin in loaded
                    for module in registry.Ownership.of(plugin).modules()
                },
                plugin_ids={plugin.id_ for plugin in loaded},
            )
        await self._reconcile(notes)
        self.write_index()

    def _finish(
        self,
        mode: ApplyMode,
        changed: list[str],
        reason: str | None = None,
        notes: list[str] | None = None,
    ) -> RuntimeOperation:
        operation = RuntimeOperation(
            mode,
            "failed" if mode is ApplyMode.FAILED else "completed",
            changed,
            reason,
            self.generation,
            notes=notes or [],
        )
        self.last_operation = operation
        return operation

    async def _reload(self, targets: list[Plugin]) -> RuntimeOperation:
        closure = self.classifier.closure(targets, self.generation)
        ids = [plugin.id_ for plugin in closure]
        if any(self.classifier.reasons(plugin) for plugin in closure):
            return self._finish(ApplyMode.FAILED, ids, "plugin_not_hot_reloadable")
        specs = [(plugin.module_name, plugin_root(plugin)) for plugin in closure]
        for _, root in specs:
            for path in source_files(root) if root else []:
                try:
                    compile(path.read_bytes(), str(path), "exec")
                except (OSError, SyntaxError, ValueError) as error:
                    return self._finish(
                        ApplyMode.FAILED,
                        ids,
                        f"plugin_source_invalid:{type(error).__name__}",
                    )
        notes: list[str] = []
        kept: dict[str, list[str]] = {}
        for plugin in reversed(closure):
            kept[plugin.module_name] = await self._teardown(
                registry.Ownership.of(plugin),
                [plugin, *registry.iter_sub_plugins(plugin)],
                notes,
            )
        loaded: list[Plugin] = []
        reason = None
        for module_name, root in specs:
            try:
                loaded.append(
                    await self._load(module_name, root, kept[module_name], notes)
                )
            except Exception as error:
                reason = reason or f"plugin_load_failed:{type(error).__name__}"
                logger.error(f"插件 {module_name} 重新加载失败", _LOG_COMMAND, e=error)
        await self._commit(loaded, ids, notes)
        if reason:
            return self._finish(ApplyMode.FAILED, ids, reason, notes)
        logger.info(f"插件已热重载: {', '.join(ids)}", _LOG_COMMAND)
        return self._finish(ApplyMode.HOT_RELOADED, ids, notes=notes)

    # ------------------------------------------------------------ 公开操作
    async def reload_plugin(self, ref: str) -> RuntimeOperation:
        """重载插件及所有依赖它的插件

        参数:
            ref: 插件 id 或模块名

        返回:
            RuntimeOperation: 操作结果
        """
        async with runtime_mutation_coordinator.operation("plugin_reload"):
            plugin = find_plugin(ref)
            if plugin is None:
                return self._finish(ApplyMode.FAILED, [ref], "plugin_not_loaded")
            return await self._reload([plugin])

    async def unload_plugin(self, ref: str) -> RuntimeOperation:
        """卸载插件，依赖它的插件一并卸载，并移除该插件声明的配置项

        参数:
            ref: 插件 id 或模块名

        返回:
            RuntimeOperation: 操作结果
        """
        async with runtime_mutation_coordinator.operation("plugin_unload"):
            plugin = find_plugin(ref)
            if plugin is None:
                return self._finish(ApplyMode.FAILED, [ref], "plugin_not_loaded")
            closure = self.classifier.closure([plugin], self.generation)
            ids = [item.id_ for item in closure]
            if any(self.classifier.reasons(item) for item in closure):
                return self._finish(ApplyMode.FAILED, ids, "plugin_not_hot_reloadable")
            configs = _registered_configs(plugin)
            notes: list[str] = []
            for item in reversed(closure):
                await self._teardown(
                    registry.Ownership.of(item),
                    [item, *registry.iter_sub_plugins(item)],
                    notes,
                )
                self.classifier.hosted.discard(item.module_name)
            _forget_configs(configs)
            await self._commit([], ids, notes)
            logger.info(f"插件已卸载: {', '.join(ids)}", _LOG_COMMAND)
            return self._finish(ApplyMode.HOT_RELOADED, ids, notes=notes)

    async def load_new_plugin(
        self,
        module_name: str,
        root: Path,
        *,
        submit_restart: bool = True,
        dependencies_verified: bool = False,
    ) -> RuntimeOperation:
        """加载新安装的插件，已加载时改为重载，无法热加载时请求重启

        参数:
            module_name: 模块名
            root: 插件包目录或模块文件
            submit_restart: 需要重启时是否直接提交给 launcher
            dependencies_verified: 调用方已确认依赖无需变更

        返回:
            RuntimeOperation: 操作结果
        """
        if find_plugin(module_name) is not None:
            return await self.reload_plugin(module_name)
        root = root.resolve()
        if not source_files(root):
            return self._finish(
                ApplyMode.FAILED, [module_name], "plugin_source_missing"
            )
        reasons = self.classifier.source_reasons(
            module_name, root, new=True, dependencies_verified=dependencies_verified
        )
        if "dependencies_changed" in reasons:
            return await self.request_dependency_restart(
                {root / name for name in DEPENDENCY_FILES if (root / name).is_file()},
                submit_launcher=submit_restart,
            )
        if reasons:
            return await self.request_restart(
                {module_name}, sorted(reasons)[0], submit_launcher=submit_restart
            )
        async with runtime_mutation_coordinator.operation("plugin_load"):
            notes: list[str] = []
            try:
                plugin = await self._load(module_name, root, [], notes)
            except Exception as error:
                logger.error(f"插件 {module_name} 热加载失败", _LOG_COMMAND, e=error)
                await self._commit([], [], notes)
                return self._finish(
                    ApplyMode.FAILED,
                    [module_name],
                    f"plugin_import_failed:{type(error).__name__}",
                    notes,
                )
            self.classifier.hosted.add(module_name)
            await self._commit([plugin], [], notes)
            logger.info(f"插件已热加载: {plugin.id_}", _LOG_COMMAND)
            return self._finish(ApplyMode.HOT_RELOADED, [plugin.id_], notes=notes)

    async def recover_plugin(
        self, module: str, *, submit_restart: bool = True
    ) -> RuntimeOperation:
        """插件文件被恢复后重新加载，无法热处理时请求重启

        参数:
            module: 模块名
            submit_restart: 需要重启时是否直接提交给 launcher

        返回:
            RuntimeOperation: 操作结果
        """
        if (plugin := find_plugin(module)) is not None:
            if reasons := self.classifier.restart_reasons(plugin, self.generation):
                return await self.request_restart(
                    {plugin.id_}, sorted(reasons)[0], submit_launcher=submit_restart
                )
            return await self.reload_plugin(module)
        root = Path.cwd() / Path(*module.split("."))
        if not root.is_dir() and root.with_suffix(".py").is_file():
            root = root.with_suffix(".py")
        return await self.load_new_plugin(module, root, submit_restart=submit_restart)

    async def reload_config_consumers(
        self,
        changed: set[tuple[str, str]],
        *,
        restart_dependencies: set[tuple[str, str]] | None = None,
        submit_restart: bool = True,
    ) -> RuntimeOperation | None:
        """重载在导入期读取了已变更配置的插件，无法热处理时请求重启

        参数:
            changed: 本次变更的 (模块, 键)，键为 * 表示整个模块
            restart_dependencies: 与启动时相比变更的 (模块, 键)，默认同 changed
            submit_restart: 需要重启时是否直接提交给 launcher

        返回:
            RuntimeOperation | None: 操作结果，没有受影响插件时为 None
        """
        from zhenxun.configs.config import Config

        def readers(dependencies: set[tuple[str, str]]) -> dict[str, Plugin]:
            result = {}
            for (module, key), plugin_ids in list(Config.import_readers.items()):
                if (module, key) in dependencies or (module, "*") in dependencies:
                    for plugin_id in plugin_ids:
                        if plugin := find_plugin(plugin_id):
                            result[plugin.id_] = plugin
            return result

        affected = readers(changed)
        if not affected:
            return None
        unsafe = {
            plugin_id
            for plugin_id, plugin in readers(
                changed if restart_dependencies is None else restart_dependencies
            ).items()
            if self.classifier.restart_reasons(plugin, self.generation)
        }
        if unsafe & affected.keys():
            return await self.request_restart(
                set(affected), CONFIG_RESTART_REASON, submit_launcher=submit_restart
            )
        if not unsafe:
            self.clear_pending_restart(CONFIG_RESTART_REASON)
        hot = [
            plugin
            for plugin in affected.values()
            if not self.classifier.restart_reasons(plugin, self.generation)
        ]
        if not hot:
            return None
        async with runtime_mutation_coordinator.operation("plugin_reload"):
            return await self._reload(hot)

    def clear_pending_restart(self, reason: str | None = None) -> None:
        """清除待重启原因

        参数:
            reason: 原因，为空时全部清除
        """
        if reason is None:
            self.pending_restart.clear()
        else:
            self.pending_restart.discard(reason)

    async def request_restart(
        self, affected: set[str], reason: str, *, submit_launcher: bool = True
    ) -> RuntimeOperation:
        """登记需要重启才能生效的变更，由 launcher 托管时可直接提交重启

        参数:
            affected: 受影响的插件
            reason: 原因
            submit_launcher: 是否提交给 launcher

        返回:
            RuntimeOperation: 操作结果
        """
        self.pending_restart.add(reason)
        mode = ApplyMode.RESTART_PENDING
        if submit_launcher and os.environ.get("ZHENXUN_LAUNCHER_PID"):
            try:
                from zhenxun.utils._restart_utils import request_restart

                accepted, _ = await request_restart(f"plugin_host:{reason}")
                if accepted:
                    mode = ApplyMode.RESTART_REQUESTED
            except Exception as error:
                logger.warning(f"自动重启请求失败: {type(error).__name__}")
        operation = RuntimeOperation(
            mode, "pending_restart", sorted(affected), reason, self.generation
        )
        self.last_operation = operation
        return operation

    async def request_dependency_restart(
        self, changed: set[Path], *, submit_launcher: bool = True
    ) -> RuntimeOperation:
        """依赖文件变更，请求同步依赖后重启

        参数:
            changed: 变更的依赖文件
            submit_launcher: 是否提交给 launcher

        返回:
            RuntimeOperation: 操作结果
        """
        reason = "dependencies_changed"
        self.pending_restart.add(reason)
        mode = ApplyMode.RESTART_PENDING
        if submit_launcher and os.environ.get("ZHENXUN_LAUNCHER_PID"):
            try:
                from zhenxun.utils._restart_utils import request_dependency_restart

                accepted, _ = await request_dependency_restart(
                    f"plugin_host:{reason}", changed
                )
                if accepted:
                    mode = ApplyMode.RESTART_REQUESTED
            except Exception as error:
                logger.warning(f"依赖同步重启请求失败: {type(error).__name__}")
        operation = RuntimeOperation(
            mode,
            "pending_restart",
            sorted(path.name for path in changed),
            reason,
            self.generation,
        )
        self.last_operation = operation
        return operation


plugin_host = PluginHost()
