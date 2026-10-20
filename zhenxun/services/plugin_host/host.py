"""插件热加载、卸载、重载与重启请求

插件身份与登记全部以 NoneBot 自身的插件表为准：卸载时按 ``registry`` 中的
注册表逐一移除插件登记，加载时直接使用 NoneBot 的导入机制。
无法热处理的插件（见 ``classifier``）改为请求重启。
"""

from __future__ import annotations

import asyncio
from collections.abc import Collection
from contextlib import AsyncExitStack
from dataclasses import dataclass
import importlib
from importlib.util import find_spec
import os
from pathlib import Path
import sys
from typing import Any

import nonebot
from nonebot.exception import SkippedException
from nonebot.matcher import current_matcher
from nonebot.plugin import (
    Plugin,
    _current_plugin,
    _managers,
    _module_name_to_plugin_id,
    _new_plugin,
    _plugins,
    _revert_plugin,
)
from nonebot.plugin.manager import PluginManager
from nonebot.utils import run_coro_with_catch

from zhenxun.services.log import logger
from zhenxun.services.memory_governor import freeze_heap
from zhenxun.services.plugin_store.plugin_store_transaction import (
    LOADED_PLUGINS_INDEX,
)
from zhenxun.services.runtime_mutation import (
    RuntimeMutationBusyError,
    runtime_mutation_coordinator,
)
from zhenxun.utils.atomic_json import read_json_locked, write_json_locked
from zhenxun.utils.enum import PriorityLifecycleType

from . import registry
from .classifier import (
    DEPENDENCY_FILES,
    PluginClassifier,
    _classification,
    find_plugin,
    plugin_root,
    source_files,
)
from .models import ApplyMode, RuntimeOperation

CONFIG_RESTART_REASON = "import_time_config_consumer_requires_restart"
_DRAIN_TIMEOUT = 10.0
_CANCEL_TIMEOUT = 5.0
_HOOK_TIMEOUT = 30.0
_LOG_COMMAND = "插件热重载"
_PARK_DELAY = 1.0


@dataclass
class _Stopped:
    """热重载失败后停用的插件"""

    root: Path | None
    """插件包目录或模块文件"""
    cause: str
    """导致停用的插件模块，自身加载失败时为自身"""
    kept: list[str]
    """需要恢复登记的子插件模块"""
    missing: str | None = None
    """加载失败时缺少的第三方依赖"""


_connect_tasks: set[asyncio.Task] = set()


def _missing_dependency(error: BaseException, module_name: str) -> str | None:
    """导入失败原因是缺少插件外部的模块时，返回该模块的顶层包名

    参数:
        error: 导入时的异常
        module_name: 插件模块名

    返回:
        str | None: 缺少的包名
    """
    if isinstance(error, ModuleNotFoundError) and error.name:
        top = error.name.split(".")[0]
        if top not in {"zhenxun", module_name.split(".")[0]}:
            return top
    return None


def purge_failed_import(module_name: str) -> None:
    """清理启动期导入失败的插件已产生的登记与模块

    NoneBot 导入失败时只撤销插件登记，导入过程中已注册的响应器、钩子、
    定时任务等仍会生效，这里按模块前缀一并移除；定义 Tortoise 模型的模块保留。

    参数:
        module_name: 插件模块名
    """
    prefix = registry.Ownership.build([module_name], [], [])
    plugins = [p for p in list(_plugins.values()) if prefix.module_owned(p.module_name)]
    files = [Path(f) for p in plugins if (f := getattr(p.module, "__file__", None))]
    own = registry.Ownership.build(
        [module_name, *(p.module_name for p in plugins)],
        [*(p.id_ for p in plugins), _module_name_to_plugin_id(module_name)],
        files,
    )
    notes: list[str] = []
    try:
        registry.remove(registry.collect(own), notes)
        for plugin in sorted(plugins, key=lambda p: p.id_.count(":"), reverse=True):
            if _plugins.get(plugin.id_) is plugin:
                _revert_plugin(plugin)
        registry.drop_modules(own, registry.model_modules(own))
    except Exception as error:
        logger.error(
            f"插件 {module_name} 加载失败后的清理未完成", _LOG_COMMAND, e=error
        )


def _dependency_files(root: Path) -> set[Path]:
    return {root / name for name in DEPENDENCY_FILES if (root / name).is_file()}


async def _run_connect_hook(hook: Any, bot: Any) -> None:
    try:
        async with AsyncExitStack() as stack:
            await run_coro_with_catch(
                hook(bot=bot, stack=stack, dependency_cache={}), (SkippedException,)
            )
    except Exception as error:
        name = getattr(registry._unwrap(hook), "__name__", "?")
        logger.error(f"插件 Bot 连接钩子执行失败: {name}", _LOG_COMMAND, e=error)


def _connect_bots(hooks: list[Any]) -> None:
    """对已连接的 Bot 在后台执行插件新注册的连接钩子

    参数:
        hooks: 连接钩子
    """
    for bot in list(nonebot.get_bots().values()):
        for hook in hooks:
            task = asyncio.create_task(_run_connect_hook(hook, bot))
            _connect_tasks.add(task)
            task.add_done_callback(_connect_tasks.discard)


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


def _duties(plugin: Plugin) -> dict[str, list[str]]:
    """插件全局禁用后仍会运行的被动技能与可调度任务

    参数:
        plugin: 顶层插件

    返回:
        dict[str, list[str]]: tasks 为被动技能模块，schedules 为可调度任务名
    """
    from zhenxun.configs.utils import PluginExtraData

    tasks = {
        task.module
        for item in [plugin, *registry.iter_sub_plugins(plugin)]
        if item.metadata
        for task in PluginExtraData.from_extra(item.metadata.extra, item.name).tasks
        or []
    }
    schedules = {
        key
        for container, key in registry._scheduler_tasks(registry.Ownership.of(plugin))
        if isinstance(container, dict)
    }
    return {"tasks": sorted(tasks), "schedules": sorted(schedules)}


class PluginHost:
    """插件热加载、卸载、重载，以及无法热处理时的重启请求"""

    def __init__(self) -> None:
        self.generation = 0
        """插件代际，每次热操作后递增"""
        self.last_operation: RuntimeOperation | None = None
        self.pending_restart: set[str] = set()
        """需要重启才能生效的变更原因"""
        self.classifier = PluginClassifier()
        self.stopped: dict[str, _Stopped] = {}
        """热重载失败而停用的插件模块"""
        self.parked: dict[str, tuple[Path | None, list[str]]] = {}
        """手动卸载的全局禁用插件模块，值为插件目录与需恢复登记的子插件模块"""
        self.duties: dict[str, dict[str, list[str]]] = {}
        """插件模块的被动技能与可调度任务，见 _duties"""
        self._sync_task: asyncio.Task | None = None
        self._sync_again = False

    # ------------------------------------------------------------ 查询
    def classification_for(self, module: str) -> dict[str, Any]:
        """插件的热处理分类

        已加载插件见 PluginClassifier.classification_for；热重载失败而停用的插件
        按当前源码判定，并标记 stopped。

        参数:
            module: 插件 id 或模块名

        返回:
            dict[str, Any]: reload_support 与 reload_reasons
        """
        if find_plugin(module) is None and (name := self._stopped_module(module)):
            root = self.stopped[name].root
            reasons = (
                self.classifier.source_reasons(name, root, new=False)
                if root is not None and source_files(root)
                else {"plugin_source_missing"}
            )
            if self._missing(name):
                reasons.add("dependency_missing")
            return {**_classification(reasons), "stopped": True}
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
            "stopped": sorted(_module_name_to_plugin_id(m) for m in self.stopped),
            "parked": sorted(_module_name_to_plugin_id(m) for m in self.parked),
            "last_operation": (
                self.last_operation.public_dict() if self.last_operation else None
            ),
        }

    def is_parked(self, module_path: str) -> bool:
        """模块是否属于手动卸载的全局禁用插件"""
        return any(
            module_path == name or module_path.startswith(f"{name}.")
            for name in self.parked
        )

    def write_index(self) -> None:
        """记录已加载与手动卸载的插件模块，供 launcher 启动验证读取，
        并记录手动卸载的插件及各插件的被动技能与可调度任务，供下次启动判断是否跳过加载
        """
        for plugin in list(_plugins.values()):
            if plugin.parent_plugin is None:
                try:
                    self.duties[plugin.module_name] = _duties(plugin)
                except Exception:
                    self.duties.pop(plugin.module_name, None)
        modules = sorted(
            {plugin.module_name for plugin in list(_plugins.values())}
            | set(self.parked)
        )
        duties = {name: self.duties[name] for name in modules if name in self.duties}
        try:
            write_json_locked(
                LOADED_PLUGINS_INDEX,
                {"modules": modules, "duties": duties, "parked": sorted(self.parked)},
            )
        except OSError as error:
            logger.warning(f"已加载插件索引写入失败: {type(error).__name__}")

    # ------------------------------------------------------------ 卸载与加载
    @staticmethod
    async def _stop_hooks(
        collected: dict[str, list[Any]], modules: set[str], notes: list[str]
    ) -> None:
        from zhenxun.services.lifecycle import lifecycle_kernel
        from zhenxun.utils.manager.priority_manager import (
            _run_hook,
            _shutdown_hooks,
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
            hooks = _shutdown_hooks()
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
        registry.remove({"matcher": collected.pop("matcher", [])}, notes)
        _, handling = registry.owned_tasks(own)
        if handling:
            await asyncio.wait(handling, timeout=_DRAIN_TIMEOUT)
        await self._stop_hooks(collected, set(own.modules()), notes)
        registry.remove(collected, notes)
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

    async def _purge(
        self,
        module_name: str,
        root: Path | None,
        related: list[str],
        notes: list[str],
    ):
        """清理加载失败时已产生的部分登记

        插件导入期间被一并导入的其它未加载插件，其登记会挂在当前插件名下，
        因此按模块前缀和这些插件应有的 id 一起清理。

        参数:
            module_name: 模块名
            root: 插件包目录或模块文件
            related: 导入前尚未加载、可能被一并导入的其它插件模块
            notes: 过程记录
        """
        names = [module_name, *related]
        prefix = registry.Ownership.build(names, [], [])
        plugins = [
            p for p in list(_plugins.values()) if prefix.module_owned(p.module_name)
        ]
        files = [Path(f) for p in plugins if (f := getattr(p.module, "__file__", None))]
        own = registry.Ownership.build(
            [*names, *(p.module_name for p in plugins)],
            [
                *(p.id_ for p in plugins),
                *(_module_name_to_plugin_id(name) for name in names),
            ],
            [root, *files] if root else files,
        )
        try:
            await self._teardown(own, plugins, notes)
        except Exception as error:
            notes.append(f"purge_failed:{type(error).__name__}")
            logger.error(
                f"插件 {module_name} 加载失败后的清理未完成", _LOG_COMMAND, e=error
            )

    async def _load(
        self,
        module_name: str,
        root: Path | None,
        kept: list[str],
        notes: list[str],
        related: list[str] | None = None,
    ) -> list[Plugin]:
        """导入插件并执行其初始化、启动钩子与已连接 Bot 的连接钩子，
        失败时清理已产生的登记

        导入期间通过 require 等一并加载的其它插件与本插件一同初始化，
        失败时一同清理。

        参数:
            module_name: 模块名
            root: 插件包目录或模块文件
            kept: 需要恢复登记的子插件模块
            notes: 过程记录
            related: 同批加载的其它插件模块，失败时其中导入前未加载的一并清理

        返回:
            list[Plugin]: 加载后的插件，一并加载的其它插件在后
        """
        from zhenxun.services.lifecycle import lifecycle_kernel
        from zhenxun.services.plugin_init import PluginInitManager
        from zhenxun.utils.manager.priority_manager import (
            _sync_kernel_declarations,
            lifecycle_component_ids,
        )

        if module_name in sys.modules:
            raise RuntimeError("plugin_module_still_loaded")
        driver = nonebot.get_driver()
        before = {
            kind: list(registry.lifespan_funcs(kind)) for kind in ("startup", "ready")
        }
        connect = set(driver._bot_connection_hook)
        existing = set(_plugins)

        def new_plugins() -> list[Plugin]:
            return [
                p
                for p in list(_plugins.values())
                if p.id_ not in existing and p.parent_plugin is None
            ]

        unloaded = [
            name
            for name in related or []
            if name != module_name and name not in sys.modules
        ]
        _ensure_manager(module_name, root)
        try:
            module = importlib.import_module(module_name)
            plugin = getattr(module, "__plugin__", None)
            if not isinstance(plugin, Plugin):
                raise RuntimeError("module_not_loaded_as_plugin")
            for name in kept:
                _restore_kept_plugin(name)
            plugins = [plugin, *(p for p in new_plugins() if p is not plugin)]
            own = registry.Ownership.of(*plugins)
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
            for kind in ("startup", "ready"):
                if funcs := [
                    func
                    for func in registry.lifespan_funcs(kind)
                    if func not in before[kind] and own.owned(func)
                ]:
                    await driver._lifespan._run_lifespan_func(funcs)
        except BaseException:
            unloaded.extend(
                p.module_name for p in new_plugins() if p.module_name != module_name
            )
            await self._purge(module_name, root, unloaded, notes)
            raise
        if hooks := [
            hook
            for hook in driver._bot_connection_hook
            if hook not in connect and own.owned(hook)
        ]:
            _connect_bots(hooks)
        self.classifier.hosted.update(p.module_name for p in plugins)
        self.parked.pop(module_name, None)
        return plugins

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
        freeze_heap()

    def _finish(
        self,
        mode: ApplyMode,
        changed: list[str],
        reason: str | None = None,
        notes: list[str] | None = None,
        modules: list[str] | None = None,
    ) -> RuntimeOperation:
        operation = RuntimeOperation(
            mode,
            "failed" if mode is ApplyMode.FAILED else "completed",
            changed,
            reason,
            self.generation,
            notes=notes or [],
            stopped=[
                _module_name_to_plugin_id(name)
                for name in modules or []
                if name in self.stopped
            ],
        )
        self.last_operation = operation
        return operation

    @staticmethod
    def _source_error(roots: list[Path | None]) -> str | None:
        for root in roots:
            for path in source_files(root) if root else []:
                try:
                    compile(path.read_bytes(), str(path), "exec")
                except (OSError, SyntaxError, ValueError) as error:
                    return f"plugin_source_invalid:{type(error).__name__}"
        return None

    async def _load_batch(
        self,
        specs: list[tuple[str, Path | None, list[str]]],
        dependents: dict[str, set[str]],
        notes: list[str],
    ) -> tuple[list[Plugin], str | None]:
        """按顺序加载一批插件，加载失败的插件及依赖它的插件记为停用

        参数:
            specs: (模块名, 插件包目录或模块文件, 需恢复登记的子插件模块)，
                被依赖者在前
            dependents: 模块名到依赖它的模块名
            notes: 过程记录

        返回:
            tuple[list[Plugin], str | None]: 加载成功的插件，首个失败原因
        """
        modules = [name for name, _, _ in specs]
        loaded: list[Plugin] = []
        reason = None
        causes: dict[str, str] = {}
        for name, root, kept in specs:
            if name in causes:
                self.stopped[name] = _Stopped(root, causes[name], kept)
                notes.append(f"dependency_failed:{_module_name_to_plugin_id(name)}")
                logger.warning(
                    f"插件 {name} 依赖的插件加载失败，跳过加载", _LOG_COMMAND
                )
                continue
            if find_plugin(name) is not None:
                # 已在前面的插件导入时一并加载
                self.stopped.pop(name, None)
                continue
            try:
                loaded.extend(await self._load(name, root, kept, notes, modules))
            except Exception as error:
                missing = _missing_dependency(error, name)
                self.stopped[name] = _Stopped(root, name, kept, missing)
                for other in dependents.get(name, ()):
                    causes.setdefault(other, name)
                reason = reason or (
                    f"plugin_dependency_missing:{missing}"
                    if missing
                    else f"plugin_load_failed:{type(error).__name__}"
                )
                logger.error(f"插件 {name} 加载失败，已停用", _LOG_COMMAND, e=error)
            else:
                self.stopped.pop(name, None)
        return loaded, reason

    async def _reload(self, targets: list[Plugin]) -> RuntimeOperation:
        closure = self.classifier.closure(targets, self.generation)
        ids = [plugin.id_ for plugin in closure]
        if any(self.classifier.reasons(plugin) for plugin in closure):
            return self._finish(ApplyMode.FAILED, ids, "plugin_not_hot_reloadable")
        specs = [(plugin.module_name, plugin_root(plugin)) for plugin in closure]
        if error := self._source_error([root for _, root in specs]):
            return self._finish(ApplyMode.FAILED, ids, error)
        dependents = {}
        for plugin in closure:
            dependent_ids = self.classifier.dependents(plugin, self.generation)
            dependents[plugin.module_name] = {
                item.module_name for item in closure if item.id_ in dependent_ids
            }
        notes: list[str] = []
        kept: dict[str, list[str]] = {}
        for plugin in reversed(closure):
            kept[plugin.module_name] = await self._teardown(
                registry.Ownership.of(plugin),
                [plugin, *registry.iter_sub_plugins(plugin)],
                notes,
            )
        loaded, reason = await self._load_batch(
            [(name, root, kept[name]) for name, root in specs], dependents, notes
        )
        await self._commit(loaded, ids, notes)
        if reason:
            modules = [name for name, _ in specs]
            return self._finish(ApplyMode.FAILED, ids, reason, notes, modules)
        logger.info(f"插件已热重载: {', '.join(ids)}", _LOG_COMMAND)
        return self._finish(ApplyMode.HOT_RELOADED, ids, notes=notes)

    def _stopped_module(self, ref: str) -> str | None:
        if ref in self.stopped:
            return ref
        return next(
            (name for name in self.stopped if _module_name_to_plugin_id(name) == ref),
            None,
        )

    def _parked_module(self, ref: str) -> str | None:
        if ref in self.parked:
            return ref
        return next(
            (name for name in self.parked if _module_name_to_plugin_id(name) == ref),
            None,
        )

    def _missing(self, module_name: str) -> str | None:
        """停用插件仍未安装的第三方依赖"""
        missing = self.stopped[module_name].missing
        if not missing:
            return None
        importlib.invalidate_caches()
        return missing if find_spec(missing) is None else None

    async def _restore(
        self, module_name: str, *, submit_restart: bool
    ) -> RuntimeOperation:
        """重新加载热重载失败后停用的插件，以及因它停用的插件

        因它停用的插件之间的依赖关系已无法得知，按原顺序加载，
        其中一个失败时其后的插件继续保持停用。缺少第三方依赖时，插件声明了
        依赖文件则请求同步依赖后重启。

        参数:
            module_name: 模块名
            submit_restart: 需要重启时是否直接提交给 launcher

        返回:
            RuntimeOperation: 操作结果
        """
        files: set[Path] = set()
        reasons: set[str] = set()
        async with runtime_mutation_coordinator.operation("plugin_reload"):
            if module_name not in self.stopped:
                if (plugin := find_plugin(module_name)) is None:
                    return self._finish(
                        ApplyMode.FAILED, [module_name], "plugin_not_loaded"
                    )
                return self._finish(ApplyMode.HOT_RELOADED, [plugin.id_])
            batch = [module_name]
            for name, item in list(self.stopped.items()):
                if item.cause == module_name and name != module_name:
                    if item.root is not None and source_files(item.root):
                        batch.append(name)
                    else:
                        self.stopped.pop(name)
            ids = [_module_name_to_plugin_id(name) for name in batch]
            root = self.stopped[module_name].root
            if root is None or not source_files(root):
                self.stopped.pop(module_name)
                return self._finish(ApplyMode.FAILED, ids, "plugin_source_missing")
            roots = [self.stopped[name].root for name in batch]
            if error := self._source_error(roots):
                return self._finish(ApplyMode.FAILED, ids, error, modules=batch)
            if missing := self._missing(module_name):
                files = _dependency_files(root)
                if not files:
                    return self._finish(
                        ApplyMode.FAILED,
                        ids,
                        f"plugin_dependency_missing:{missing}",
                        modules=batch,
                    )
            for name, path in zip(batch, roots, strict=True):
                if path is not None:
                    reasons |= self.classifier.source_reasons(name, path, new=False)
            if not files and not reasons:
                return await self._restore_batch(batch, ids)
        if files:
            return await self.request_restart(
                set(ids),
                "dependencies_changed",
                submit_launcher=submit_restart,
                dependency_files=files,
            )
        return await self.request_restart(
            set(ids), sorted(reasons)[0], submit_launcher=submit_restart
        )

    async def _restore_batch(
        self, batch: list[str], ids: list[str]
    ) -> RuntimeOperation:
        """加载停用插件批次并提交，调用方需持有运行时变更锁

        参数:
            batch: 模块名，被依赖者在前
            ids: 插件 id

        返回:
            RuntimeOperation: 操作结果
        """
        notes: list[str] = []
        loaded, reason = await self._load_batch(
            [
                (name, self.stopped[name].root, self.stopped[name].kept)
                for name in batch
            ],
            {name: set(batch[index + 1 :]) for index, name in enumerate(batch)},
            notes,
        )
        await self._commit(loaded, [], notes)
        if reason:
            return self._finish(ApplyMode.FAILED, ids, reason, notes, batch)
        logger.info(f"停用的插件已重新加载: {', '.join(ids)}", _LOG_COMMAND)
        return self._finish(ApplyMode.HOT_RELOADED, ids, notes=notes)

    # ------------------------------------------------------------ 公开操作
    async def reload_plugin(self, ref: str) -> RuntimeOperation:
        """重载插件及所有依赖它的插件

        参数:
            ref: 插件 id 或模块名

        返回:
            RuntimeOperation: 操作结果
        """
        if find_plugin(ref) is None and (stopped := self._stopped_module(ref)):
            return await self._restore(stopped, submit_restart=False)
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
            if plugin is None and (stopped := self._stopped_module(ref)):
                self.stopped.pop(stopped)
                return self._finish(
                    ApplyMode.HOT_RELOADED, [_module_name_to_plugin_id(stopped)]
                )
            if plugin is None and (parked := self._parked_module(ref)):
                self.parked.pop(parked)
                return self._finish(
                    ApplyMode.HOT_RELOADED, [_module_name_to_plugin_id(parked)]
                )
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
        if module_name in self.stopped:
            return await self._restore(module_name, submit_restart=submit_restart)
        root = root.resolve()
        if not source_files(root):
            return self._finish(
                ApplyMode.FAILED, [module_name], "plugin_source_missing"
            )
        reasons = self.classifier.source_reasons(
            module_name, root, new=True, dependencies_verified=dependencies_verified
        )
        if "dependencies_changed" in reasons:
            return await self.request_restart(
                {module_name},
                "dependencies_changed",
                submit_launcher=submit_restart,
                dependency_files=_dependency_files(root),
            )
        if reasons:
            return await self.request_restart(
                {module_name}, sorted(reasons)[0], submit_launcher=submit_restart
            )
        async with runtime_mutation_coordinator.operation("plugin_load"):
            notes: list[str] = []
            try:
                plugins = await self._load(module_name, root, [], notes)
            except Exception as error:
                logger.error(f"插件 {module_name} 热加载失败", _LOG_COMMAND, e=error)
                await self._commit([], [], notes)
                from zhenxun.services.nonebot_store.runtime import (
                    missing_config_key,
                )

                missing = _missing_dependency(error, module_name)
                config_key = missing_config_key(error)
                return self._finish(
                    ApplyMode.FAILED,
                    [module_name],
                    f"plugin_dependency_missing:{missing}"
                    if missing
                    else f"plugin_configuration_required:{config_key}"
                    if config_key
                    else f"plugin_import_failed:{type(error).__name__}",
                    notes,
                )
            await self._commit(plugins, [], notes)
            ids = [plugin.id_ for plugin in plugins]
            logger.info(f"插件已热加载: {', '.join(ids)}", _LOG_COMMAND)
            return self._finish(ApplyMode.HOT_RELOADED, ids, notes=notes)

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
        if stopped := self._stopped_module(module):
            return await self._restore(stopped, submit_restart=submit_restart)
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

    # ------------------------------------------------------------ 全局禁用
    async def _disabled(self, modules: set[str]) -> set[str]:
        """全局禁用、且被动技能与定时任务均已停用的插件模块

        插件需至少有一条受管理的插件记录，且其下全部记录均为全局禁用；
        被动技能与可调度任务未知的插件不算在内。

        参数:
            modules: 待判断的顶层插件模块

        返回:
            set[str]: 满足条件的模块
        """
        from zhenxun.models.plugin_info import PluginInfo
        from zhenxun.models.scheduled_job import ScheduledJob
        from zhenxun.models.task_info import TaskInfo
        from zhenxun.services.plugin_policy import _MANAGED_PLUGIN_TYPES

        rows = await PluginInfo.filter(
            plugin_type__in=list(_MANAGED_PLUGIN_TYPES)
        ).values_list("module_path", "status", "block_type")
        tasks = set(await TaskInfo.filter(status=True).values_list("module", flat=True))
        schedules = set(
            await ScheduledJob.filter(is_enabled=True).values_list(
                "plugin_name", flat=True
            )
        )
        state: dict[str, bool] = {}
        for path, status, block_type in rows:
            block = getattr(block_type, "value", block_type)
            for name in modules:
                if path == name or str(path).startswith(f"{name}."):
                    off = not status and block in {None, "ALL"}
                    state[name] = state.get(name, True) and off
        return {
            name
            for name, off in state.items()
            if off
            and (duty := self.duties.get(name)) is not None
            and not tasks.intersection(duty["tasks"])
            and not schedules.intersection(duty["schedules"])
        }

    async def park_disabled_entries(self) -> None:
        """启动时跳过加载上次手动卸载且仍全局禁用的插件，解除禁用后再热加载

        仅处理可热加载、且没有未跳过的插件依赖它的第三方插件。
        """
        from zhenxun.services.startup_load import startup_load_planner

        index = read_json_locked(LOADED_PLUGINS_INDEX, {})
        self.duties.update(index.get("duties") or {})
        parked = set(index.get("parked") or ())
        entries = {
            entry.plugin_id: entry
            for entry in startup_load_planner.entries.values()
            if entry.phase == "runtime_load"
            and entry.status == "pending"
            and entry.source != "builtin"
            and entry.module_name in parked
        }
        if not entries:
            return
        try:
            disabled = await self._disabled({e.module_name for e in entries.values()})
        except Exception as error:
            logger.warning(f"读取插件全局禁用状态失败: {type(error).__name__}")
            return
        candidates = {
            plugin_id
            for plugin_id, entry in entries.items()
            if entry.module_name in disabled
            and not self.classifier.source_reasons(
                entry.module_name, entry.root, new=False
            )
        }
        changed = True
        while changed:
            changed = False
            for plugin_id, entry in startup_load_planner.entries.items():
                if plugin_id not in candidates and (
                    needed := entry.dependencies & candidates
                ):
                    candidates -= needed
                    changed = True
        for plugin_id in startup_load_planner.order:
            if plugin_id in candidates:
                entry = entries[plugin_id]
                entry.status = "unloaded"
                self.parked[entry.module_name] = (entry.root, [])
        if candidates:
            logger.info(f"跳过加载已卸载的插件: {', '.join(sorted(candidates))}")

    async def sync_disabled(
        self, unload: Collection[str] = (), reload: Collection[str] = ()
    ) -> RuntimeOperation | None:
        """卸载指定的全局禁用插件以释放资源，重新加载已解除禁用或指定的已卸载插件

        依赖它的插件同样禁用、且均可热处理时才卸载。

        参数:
            unload: 要卸载的顶层插件模块
            reload: 仍全局禁用但要重新加载的插件模块

        返回:
            RuntimeOperation | None: 操作结果，没有变化时为 None
        """
        async with runtime_mutation_coordinator.operation("plugin_park"):
            tops = {
                plugin.module_name: plugin
                for plugin in list(_plugins.values())
                if plugin.parent_plugin is None
            }
            for plugin in tops.values():
                self.duties[plugin.module_name] = _duties(plugin)
            disabled = await self._disabled(set(tops) | set(self.parked))
            eligible = []
            for name, plugin in tops.items():
                if name not in disabled or name not in unload:
                    continue
                closure = self.classifier.closure([plugin], self.generation)
                if all(
                    item.module_name in disabled and not self.classifier.reasons(item)
                    for item in closure
                ):
                    eligible.append(plugin)
            wake = [
                name for name in self.parked if name not in disabled or name in reload
            ]
            park = self.classifier.closure(eligible, self.generation)
            if not park and not wake:
                self.write_index()
                return None
            notes: list[str] = []
            for item in reversed(park):
                kept = await self._teardown(
                    registry.Ownership.of(item),
                    [item, *registry.iter_sub_plugins(item)],
                    notes,
                )
                self.classifier.hosted.discard(item.module_name)
                self.parked[item.module_name] = (plugin_root(item), kept)
            loaded, reason = await self._load_batch(
                [(name, *self.parked[name]) for name in wake], {}, notes
            )
            for name in wake:
                self.parked.pop(name, None)
            await self._commit(loaded, [item.id_ for item in park], notes)
            ids = [
                *(item.id_ for item in park),
                *(_module_name_to_plugin_id(name) for name in wake),
            ]
            if park:
                logger.info(
                    f"已卸载全局禁用的插件: {', '.join(i.id_ for i in park)}",
                    _LOG_COMMAND,
                )
            if loaded:
                logger.info(
                    f"已重新加载插件: {', '.join(p.id_ for p in loaded)}",
                    _LOG_COMMAND,
                )
            if reason:
                return self._finish(ApplyMode.FAILED, ids, reason, notes, wake)
            return self._finish(ApplyMode.HOT_RELOADED, ids, notes=notes)

    async def set_unloaded(
        self, module_path: str, unloaded: bool
    ) -> RuntimeOperation | None:
        """手动卸载全局禁用的插件以释放资源，或重新加载已卸载的插件

        参数:
            module_path: 插件模块
            unloaded: True 为卸载，False 为重新加载

        返回:
            RuntimeOperation | None: 操作结果，插件不满足条件时为 None
        """
        names = {
            plugin.module_name
            for plugin in list(_plugins.values())
            if plugin.parent_plugin is None
        } | set(self.parked)
        name = next(
            (
                name
                for name in names
                if module_path == name or module_path.startswith(f"{name}.")
            ),
            None,
        )
        if name is None:
            return None
        if unloaded:
            return await self.sync_disabled(unload={name})
        return await self.sync_disabled(reload={name})

    def schedule_sync_disabled(self) -> None:
        """在后台重新加载已解除禁用的已卸载插件，短时间内的多次请求合并执行"""
        if self._sync_task is not None and not self._sync_task.done():
            self._sync_again = True
            return
        self._sync_task = asyncio.create_task(self._sync_later())

    async def _sync_later(self) -> None:
        self._sync_again = True
        while self._sync_again:
            self._sync_again = False
            await asyncio.sleep(_PARK_DELAY)
            try:
                await self.sync_disabled()
            except Exception as error:
                if isinstance(error, RuntimeMutationBusyError) and str(error) in {
                    "runtime_mutation_quiescing",
                    "runtime_mutation_closed",
                }:
                    return
                logger.error("同步插件全局禁用状态失败", _LOG_COMMAND, e=error)

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
        self,
        affected: set[str],
        reason: str,
        *,
        submit_launcher: bool = True,
        dependency_files: set[Path] | None = None,
    ) -> RuntimeOperation:
        """登记需要重启才能生效的变更，由 launcher 托管时可直接提交重启

        参数:
            affected: 受影响的插件
            reason: 原因
            submit_launcher: 是否提交给 launcher
            dependency_files: 变更的依赖文件，给出时请求同步依赖后重启

        返回:
            RuntimeOperation: 操作结果
        """
        self.pending_restart.add(reason)
        mode = ApplyMode.RESTART_PENDING
        if submit_launcher and os.environ.get("ZHENXUN_LAUNCHER_PID"):
            from zhenxun.utils import _restart_utils

            source = f"plugin_host:{reason}"
            try:
                if dependency_files:
                    accepted, _ = await _restart_utils.request_dependency_restart(
                        source, dependency_files
                    )
                else:
                    accepted, _ = await _restart_utils.request_restart(source)
                if accepted:
                    mode = ApplyMode.RESTART_REQUESTED
            except Exception as error:
                logger.warning(f"自动重启请求失败: {type(error).__name__}")
        operation = RuntimeOperation(
            mode, "pending_restart", sorted(affected), reason, self.generation
        )
        self.last_operation = operation
        return operation


plugin_host = PluginHost()
