from __future__ import annotations

import asyncio
from collections import defaultdict
from collections.abc import Callable, Coroutine
import contextlib
from datetime import datetime, timezone
from functools import wraps
import inspect
import json
from pathlib import Path
import subprocess
import sys
import threading
from types import MethodType, ModuleType
from typing import Any

import nonebot
from nonebot.plugin import get_loaded_plugins

from zhenxun.services.log import logger

from .classifier import changed_model_file, classify_unit
from .compat import (
    NoneBotCompatibilityError,
    clean_matchers,
    remove_driver_hooks,
    remove_nested_managers,
    remove_plugin_init,
    remove_plugins,
    remove_priority_hooks,
    remove_processors,
    verify_nonebot_compatibility,
)
from .models import ApplyMode, PluginUnit, ReloadClassification, RuntimeOperation
from .ownership import current_owner, import_owner, owner_context

_INDEX_FILE = Path("data/runtime/lifecycle-index-v1.json")


def _module_file(module: Any) -> Path | None:
    value = getattr(module, "__file__", None)
    if not value:
        return None
    path = Path(value).resolve()
    if path.suffix in {".pyc", ".pyo"}:
        path = path.with_suffix(".py")
    return path if path.exists() else None


def _callable_module(value: Any) -> str:
    call = getattr(value, "func", value)
    call = getattr(call, "call", call)
    return str(getattr(call, "__module__", ""))


class PluginRuntimeManager:
    def __init__(self) -> None:
        self.units: dict[str, PluginUnit] = {}
        self.module_to_unit: dict[str, str] = {}
        self.generation = 0
        self.enabled = False
        self.compatibility_error: str | None = None
        self.pending_restart: set[str] = set()
        self.last_operation: RuntimeOperation | None = None
        self.webui_revision = ""
        self._reload_lock = asyncio.Lock()
        self._owned_tasks: dict[str, set[asyncio.Task[Any]]] = defaultdict(set)
        self._owned_threads: dict[str, set[threading.Thread]] = defaultdict(set)
        self._owned_processes: dict[str, set[subprocess.Popen[Any]]] = defaultdict(set)
        self._drained_events: dict[str, asyncio.Event] = {}
        self._original_task_factory: Callable[..., asyncio.Future[Any]] | None = None
        self._task_factory_installed = False
        self._watcher_task: asyncio.Task[Any] | None = None
        self._change_coordinator: Any | None = None
        self._pending_config_dependencies: dict[str, set[tuple[str, str]]] = (
            defaultdict(set)
        )
        self._original_matcher_run: Callable[..., Any] | None = None
        self._original_get_config: Callable[..., Any] | None = None
        self._original_add_plugin_config: Callable[..., Any] | None = None
        self._original_priority_add: Callable[..., Any] | None = None
        self._original_plugin_init_install: Callable[..., Any] | None = None
        self._original_plugin_init_remove: Callable[..., Any] | None = None
        self._original_plugin_init_install_all: Callable[..., Any] | None = None
        self._config_registrations: dict[str, set[tuple[str, str]]] = defaultdict(set)
        self._trie_entries: dict[str, list[tuple[str | None, Any]]] = defaultdict(list)
        self._original_trie_add_prefix: Callable[..., Any] | None = None
        self._job_owners: dict[str, str] = {}
        self._original_scheduler_add_job: Callable[..., Any] | None = None
        self._pending_dependencies: dict[str, set[str]] = defaultdict(set)
        self._content_digests: dict[Path, str] = {}
        self._classification_cache = self._load_index_cache()
        self._original_thread_start: Callable[..., Any] | None = None
        self._original_popen_init: Callable[..., Any] | None = None
        self._installed = False

    def install(self) -> None:
        if self._installed:
            return
        self._installed = True
        try:
            verify_nonebot_compatibility()
            self.enabled = True
        except NoneBotCompatibilityError as e:
            self.enabled = False
            self.compatibility_error = str(e)
            logger.warning(
                "插件热加载兼容层不可用，将自动使用重启模式: "
                f"{self.compatibility_error}"
            )

        driver = nonebot.get_driver()
        self._install_matcher_execution_wrapper()
        self._install_priority_lifecycle_tracking()
        self._install_plugin_init_tracking()
        self._install_config_access_tracking()
        self._install_trie_tracking()
        self._install_scheduler_tracking()
        self._install_processor_tracking()
        self._install_driver_hook_tracking(driver)
        self._install_require_tracking()
        self._install_thread_process_tracking()

        @driver.on_startup
        async def _start_runtime_manager() -> None:
            self.discover_loaded_plugins()
            self._install_task_factory()
            from .watcher import watch_runtime_changes

            self._watcher_task = asyncio.create_task(
                watch_runtime_changes(self), name="zhenxun-runtime-watcher"
            )

        @driver.on_shutdown
        async def _stop_runtime_manager() -> None:
            if self._watcher_task:
                self._watcher_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await self._watcher_task
                self._watcher_task = None
            await self._cancel_all_owned_tasks()
            self._restore_task_factory()

    def discover_loaded_plugins(self) -> None:
        plugins = list(get_loaded_plugins())
        roots: dict[str, list[Any]] = defaultdict(list)
        for plugin in plugins:
            root = plugin
            while root.parent_plugin:
                root = root.parent_plugin
            roots[root.id_].append(plugin)

        units: dict[str, PluginUnit] = {}
        module_to_unit: dict[str, str] = {}
        for root_id, members in roots.items():
            root_plugin = next(plugin for plugin in members if plugin.id_ == root_id)
            module_names = {plugin.module_name for plugin in members}
            for name in list(sys.modules):
                if any(
                    name == module or name.startswith(f"{module}.")
                    for module in module_names
                ):
                    module_names.add(name)
            files = {
                path
                for name in module_names
                if (module := sys.modules.get(name))
                and (path := _module_file(module)) is not None
            }
            root_file = _module_file(root_plugin.module)
            root_path = None
            if root_file:
                root_path = (
                    root_file.parent if root_file.name == "__init__.py" else root_file
                )
                if root_path.is_dir():
                    files.update(root_path.rglob("*.py"))
                    files.update(root_path.glob("requirement*.txt"))
            unit = PluginUnit(
                plugin_id=root_id,
                module_name=root_plugin.module_name,
                manager=root_plugin.manager,
                root=root_path,
                nested_managers=list(
                    {
                        id(plugin.manager): plugin.manager
                        for plugin in members
                        if plugin.manager is not root_plugin.manager
                    }.values()
                ),
                module_names=module_names,
                files=files,
            )
            classify_unit(unit, self._classification_cache.get(root_id))
            for owner, dependencies in self._pending_config_dependencies.items():
                if owner == root_id or owner.startswith(f"{root_id}:"):
                    unit.config_dependencies.update(dependencies)
            for owner, dependencies in self._pending_dependencies.items():
                if owner == root_id or owner.startswith(f"{root_id}:"):
                    unit.dependencies.update(dependencies)
            units[root_id] = unit
            for name in module_names:
                module_to_unit[name] = root_id

        self.units = units
        self.module_to_unit = module_to_unit
        self._collect_dependencies()
        self._collect_runtime_boundaries()
        self.webui_revision = self._read_webui_revision()
        self._persist_index()

    def _collect_dependencies(self) -> None:
        for unit in self.units.values():
            unit.dependencies = set(
                self._pending_dependencies.get(unit.plugin_id, set())
            )
            unit.dependencies = {
                self._root_owner(dependency) or dependency
                for dependency in unit.dependencies
            }
            for module_name in unit.module_names:
                module = sys.modules.get(module_name)
                if not module:
                    continue
                for value in vars(module).values():
                    imported_name = (
                        getattr(value, "__name__", "")
                        if isinstance(value, ModuleType)
                        else getattr(value, "__module__", "")
                    )
                    dependency = self.module_to_unit.get(imported_name)
                    if not dependency and imported_name:
                        dependency = self.owner_for_module(imported_name)
                    if dependency and dependency != unit.plugin_id:
                        unit.dependencies.add(dependency)

    def owner_for_module(self, module_name: str) -> str | None:
        if module_name in self.module_to_unit:
            return self.module_to_unit[module_name]
        candidates = (
            (name, owner)
            for name, owner in self.module_to_unit.items()
            if module_name.startswith(f"{name}.")
        )
        return next((owner for _, owner in sorted(candidates, reverse=True)), None)

    def track_config_access(self, module: str, key: str) -> None:
        owner = import_owner()
        if not owner:
            return
        dependency = (module, key.upper())
        self._pending_config_dependencies[owner].add(dependency)
        if owner in self.units:
            self.units[owner].config_dependencies.add(dependency)

    def _install_matcher_execution_wrapper(self) -> None:
        from nonebot.matcher import Matcher

        if self._original_matcher_run:
            return
        original = Matcher.run
        self._original_matcher_run = original
        manager = self

        async def run_with_owner(matcher, *args, **kwargs):
            source = getattr(matcher.__class__, "_source", None)
            plugin_id = getattr(source, "plugin_id", None)
            owner = plugin_id and manager._root_owner(plugin_id)
            unit = manager.units.get(owner or "")
            if unit and unit.draining:
                return None
            if unit:
                unit.in_flight += 1
                manager._drained_event(unit.plugin_id).clear()
            try:
                with owner_context(owner):
                    return await original(matcher, *args, **kwargs)
            finally:
                if unit:
                    unit.in_flight = max(0, unit.in_flight - 1)
                    if not unit.in_flight:
                        manager._drained_event(unit.plugin_id).set()

        Matcher.run = run_with_owner

    def _install_config_access_tracking(self) -> None:
        from zhenxun.configs.config import Config

        if self._original_get_config:
            return
        original = Config.get_config
        self._original_get_config = original
        manager = self

        def tracked_get_config(_self, module, key, *args, **kwargs):
            manager.track_config_access(str(module), str(key))
            return original(module, key, *args, **kwargs)

        Config.get_config = MethodType(tracked_get_config, Config)

        original_add = Config.add_plugin_config
        self._original_add_plugin_config = original_add

        def tracked_add_plugin_config(_self, module, key, value, *args, **kwargs):
            owner = import_owner()
            if owner:
                manager._config_registrations[owner].add(
                    (str(module), str(key).upper())
                )
            return original_add(module, key, value, *args, **kwargs)

        Config.add_plugin_config = MethodType(tracked_add_plugin_config, Config)

    def _install_priority_lifecycle_tracking(self) -> None:
        from zhenxun.utils.manager.priority_manager import PriorityLifecycle

        if self._original_priority_add:
            return
        original = PriorityLifecycle.add.__func__
        self._original_priority_add = original
        manager = self

        def tracked_add(cls, hook_type, func, priority):
            owner = current_owner() or manager.owner_for_module(
                getattr(func, "__module__", "")
            )
            wrapped = func
            if owner and inspect.iscoroutinefunction(func):

                @wraps(func)
                async def async_hook(*args, **kwargs):
                    manager._install_task_factory()
                    runtime_owner = manager._root_owner(owner) or owner
                    unit = manager.units.get(runtime_owner)
                    if unit:
                        unit.in_flight += 1
                        manager._drained_event(unit.plugin_id).clear()
                    try:
                        with owner_context(runtime_owner):
                            return await func(*args, **kwargs)
                    finally:
                        if unit:
                            unit.in_flight = max(0, unit.in_flight - 1)
                            if not unit.in_flight:
                                manager._drained_event(unit.plugin_id).set()

                wrapped = async_hook
            elif owner:

                @wraps(func)
                def sync_hook(*args, **kwargs):
                    manager._install_task_factory()
                    with owner_context(manager._root_owner(owner) or owner):
                        return func(*args, **kwargs)

                wrapped = sync_hook
            return original(cls, hook_type, wrapped, priority)

        PriorityLifecycle.add = classmethod(tracked_add)

    def _install_plugin_init_tracking(self) -> None:
        from zhenxun.services.plugin_init import PluginInitManager

        if self._original_plugin_init_install:
            return
        original_install = PluginInitManager.install.__func__
        original_remove = PluginInitManager.remove.__func__
        original_install_all = PluginInitManager.install_all.__func__
        self._original_plugin_init_install = original_install
        self._original_plugin_init_remove = original_remove
        self._original_plugin_init_install_all = original_install_all
        manager = self

        async def tracked_install(cls, module_path: str):
            manager._install_task_factory()
            owner = manager.owner_for_module(module_path) or module_path
            with owner_context(manager._root_owner(owner) or owner):
                return await original_install(cls, module_path)

        async def tracked_remove(cls, module_path: str):
            manager._install_task_factory()
            owner = manager.owner_for_module(module_path) or module_path
            with owner_context(manager._root_owner(owner) or owner):
                return await original_remove(cls, module_path)

        async def tracked_install_all(cls):
            for module_path in list(cls.plugins):
                await cls.install(module_path)

        PluginInitManager.install = classmethod(tracked_install)
        PluginInitManager.remove = classmethod(tracked_remove)
        PluginInitManager.install_all = classmethod(tracked_install_all)

    def _install_trie_tracking(self) -> None:
        from nonebot.rule import TrieRule

        if self._original_trie_add_prefix:
            return
        original = TrieRule.add_prefix.__func__
        self._original_trie_add_prefix = original
        manager = self

        def tracked_add_prefix(cls, prefix, value):
            manager._trie_entries[prefix].append((current_owner(), value))
            return original(cls, prefix, value)

        TrieRule.add_prefix = classmethod(tracked_add_prefix)

    def _install_scheduler_tracking(self) -> None:
        try:
            from nonebot_plugin_apscheduler import scheduler
        except (ImportError, RuntimeError):
            return
        if self._original_scheduler_add_job:
            return
        original = scheduler.add_job
        self._original_scheduler_add_job = original
        manager = self

        def tracked_add_job(_scheduler, func, *args, **kwargs):
            owner = current_owner() or manager.owner_for_module(
                getattr(func, "__module__", "")
            )
            wrapped = func
            if owner:
                if inspect.iscoroutinefunction(func):

                    @wraps(func)
                    async def async_job(*job_args, **job_kwargs):
                        root_owner = manager._root_owner(owner) or owner
                        unit = manager.units.get(root_owner)
                        if unit and unit.draining:
                            return None
                        if unit:
                            unit.in_flight += 1
                            manager._drained_event(root_owner).clear()
                        try:
                            with owner_context(root_owner):
                                return await func(*job_args, **job_kwargs)
                        finally:
                            if unit:
                                unit.in_flight = max(0, unit.in_flight - 1)
                                if not unit.in_flight:
                                    manager._drained_event(root_owner).set()

                    wrapped = async_job
                else:

                    @wraps(func)
                    def sync_job(*job_args, **job_kwargs):
                        with owner_context(manager._root_owner(owner) or owner):
                            return func(*job_args, **job_kwargs)

                    wrapped = sync_job
            job = original(wrapped, *args, **kwargs)
            if owner and getattr(job, "id", None):
                manager._job_owners[job.id] = owner
            return job

        scheduler.add_job = MethodType(tracked_add_job, scheduler)

    def _install_processor_tracking(self) -> None:
        import nonebot.message as message

        manager = self
        for name in (
            "event_preprocessor",
            "event_postprocessor",
            "run_preprocessor",
            "run_postprocessor",
        ):
            original = getattr(message, name)
            if getattr(original, "__zhenxun_runtime_wrapped__", False):
                continue

            def make_decorator(decorator):
                @wraps(decorator)
                def tracked(func):
                    owner = current_owner()
                    if not owner:
                        return decorator(func)
                    if inspect.iscoroutinefunction(func):

                        @wraps(func)
                        async def wrapped(*args, **kwargs):
                            with owner_context(manager._root_owner(owner) or owner):
                                return await func(*args, **kwargs)

                    else:

                        @wraps(func)
                        def wrapped(*args, **kwargs):
                            with owner_context(manager._root_owner(owner) or owner):
                                return func(*args, **kwargs)

                    return decorator(wrapped)

                tracked.__zhenxun_runtime_wrapped__ = True
                return tracked

            setattr(message, name, make_decorator(original))
            if getattr(nonebot, name, None) is original:
                setattr(nonebot, name, getattr(message, name))

    def _install_driver_hook_tracking(self, driver: Any) -> None:
        manager = self
        for name in (
            "on_startup",
            "on_ready",
            "on_shutdown",
            "on_bot_connect",
            "on_bot_disconnect",
        ):
            original = getattr(driver, name, None)
            if not original or getattr(original, "__zhenxun_runtime_wrapped__", False):
                continue

            @wraps(original)
            def tracked(func, _original=original):
                owner = current_owner()
                if not owner:
                    return _original(func)
                if inspect.iscoroutinefunction(func):

                    @wraps(func)
                    async def wrapped(*args, **kwargs):
                        with owner_context(manager._root_owner(owner) or owner):
                            return await func(*args, **kwargs)

                else:

                    @wraps(func)
                    def wrapped(*args, **kwargs):
                        with owner_context(manager._root_owner(owner) or owner):
                            return func(*args, **kwargs)

                return _original(wrapped)

            tracked.__zhenxun_runtime_wrapped__ = True
            setattr(driver, name, tracked)

    def _install_require_tracking(self) -> None:
        import nonebot
        import nonebot.plugin as plugin_module
        from nonebot.plugin import load as plugin_load

        original = plugin_load.require
        if getattr(original, "__zhenxun_runtime_wrapped__", False):
            return
        manager = self

        @wraps(original)
        def tracked_require(name):
            owner = current_owner()
            module = original(name)
            dependency_plugin = getattr(module, "__plugin__", None)
            if owner and dependency_plugin:
                manager._pending_dependencies[owner].add(dependency_plugin.id_)
            return module

        tracked_require.__zhenxun_runtime_wrapped__ = True
        plugin_load.require = tracked_require
        plugin_module.require = tracked_require
        nonebot.require = tracked_require

    def _install_thread_process_tracking(self) -> None:
        if not self._original_thread_start:
            original_start = threading.Thread.start
            self._original_thread_start = original_start
            manager = self

            @wraps(original_start)
            def tracked_start(thread, *args, **kwargs):
                owner = current_owner()
                if owner:
                    manager._owned_threads[owner].add(thread)
                return original_start(thread, *args, **kwargs)

            threading.Thread.start = tracked_start

        if not self._original_popen_init:
            original_init = subprocess.Popen.__init__
            self._original_popen_init = original_init
            manager = self

            @wraps(original_init)
            def tracked_init(process, *args, **kwargs):
                owner = current_owner()
                original_init(process, *args, **kwargs)
                if owner:
                    manager._owned_processes[owner].add(process)

            subprocess.Popen.__init__ = tracked_init

    def _collect_runtime_boundaries(self) -> None:
        for owner, threads in self._owned_threads.items():
            root = self._root_owner(owner)
            if root and (unit := self.units.get(root)):
                if any(thread.is_alive() for thread in threads):
                    unit.reasons.add("live_thread")
                    unit.classification = ReloadClassification.RESTART_REQUIRED
        for owner, processes in self._owned_processes.items():
            root = self._root_owner(owner)
            if root and (unit := self.units.get(root)):
                if any(process.poll() is None for process in processes):
                    unit.reasons.add("live_process")
                    unit.classification = ReloadClassification.RESTART_REQUIRED
        try:
            routes = nonebot.get_app().routes
        except (AttributeError, ValueError):
            routes = []
        for route in routes:
            endpoint = getattr(route, "endpoint", None)
            owner = self.owner_for_module(getattr(endpoint, "__module__", ""))
            if owner and (unit := self.units.get(owner)):
                unit.reasons.add("fastapi_route")
                unit.classification = ReloadClassification.RESTART_REQUIRED

    def _drained_event(self, plugin_id: str) -> asyncio.Event:
        event = self._drained_events.get(plugin_id)
        if event is None:
            event = asyncio.Event()
            event.set()
            self._drained_events[plugin_id] = event
        return event

    def _root_owner(self, plugin_id: str) -> str | None:
        if plugin_id in self.units:
            return plugin_id
        if owner := self.module_to_unit.get(plugin_id):
            return owner
        if owner := self.owner_for_module(plugin_id):
            return owner
        root = plugin_id.split(":", 1)[0]
        return root if root in self.units else None

    def _owned_keys_for_unit(self, plugin_id: str) -> set[str]:
        keys = (
            self._owned_tasks.keys()
            | self._owned_threads.keys()
            | self._owned_processes.keys()
        )
        return {owner for owner in keys if self._root_owner(owner) == plugin_id}

    def _install_task_factory(self) -> None:
        if self._task_factory_installed:
            return
        loop = asyncio.get_running_loop()
        self._original_task_factory = loop.get_task_factory()

        def factory(
            loop: asyncio.AbstractEventLoop, coro: Coroutine[Any, Any, Any], **kwargs
        ):
            if self._original_task_factory:
                task = self._original_task_factory(loop, coro, **kwargs)
            else:
                task = asyncio.Task(coro, loop=loop, **kwargs)
            owner = current_owner()
            if owner:
                self._owned_tasks[owner].add(task)
                task.add_done_callback(self._owned_tasks[owner].discard)
            return task

        loop.set_task_factory(factory)
        self._task_factory_installed = True

    def _restore_task_factory(self) -> None:
        if not self._task_factory_installed:
            return
        with contextlib.suppress(RuntimeError):
            asyncio.get_running_loop().set_task_factory(self._original_task_factory)
        self._task_factory_installed = False

    async def _cancel_all_owned_tasks(self) -> None:
        current = asyncio.current_task()
        tasks = [
            task
            for owned in self._owned_tasks.values()
            for task in owned
            if task is not current and not task.done()
        ]
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self._owned_tasks.clear()

    def affected_units(self, changed: set[Path]) -> set[str]:
        direct = {
            unit.plugin_id
            for unit in self.units.values()
            if unit.files & changed
            or (unit.root and any(path.is_relative_to(unit.root) for path in changed))
        }
        affected = set(direct)
        while True:
            dependents = {
                unit.plugin_id
                for unit in self.units.values()
                if unit.dependencies & affected
            }
            before = len(affected)
            affected.update(dependents)
            if len(affected) == before:
                return affected

    def _find_unit(self, module: str) -> PluginUnit | None:
        return next(
            (
                candidate
                for candidate in self.units.values()
                if candidate.plugin_id == module
                or candidate.module_name == module
                or candidate.module_name.endswith(f".{module}")
            ),
            None,
        )

    def _dependent_closure(self, direct: set[str]) -> set[str]:
        affected = set(direct)
        while True:
            dependents = {
                unit.plugin_id
                for unit in self.units.values()
                if unit.dependencies & affected
            }
            before = len(affected)
            affected.update(dependents)
            if len(affected) == before:
                return affected

    @staticmethod
    def _source_files(root: Path) -> set[Path]:
        if root.is_file():
            return {root.resolve()}
        if not root.is_dir():
            return set()
        return {
            path.resolve()
            for path in root.rglob("*")
            if path.is_file()
            and (
                path.suffix == ".py"
                or path.name in {"requirements.txt", "requirement.txt"}
            )
        }

    def _failed_operation(self, module: str, reason: str) -> RuntimeOperation:
        operation = RuntimeOperation(
            ApplyMode.FAILED,
            "failed",
            [module],
            reason,
            self.generation,
        )
        self.last_operation = operation
        self._persist_index()
        return operation

    async def reload_plugin(self, module: str) -> RuntimeOperation:
        """Reload one loaded plugin and all of its runtime dependents."""
        unit = self._find_unit(module)
        if unit is None:
            return self._failed_operation(module, "plugin_not_loaded")
        if not self.enabled:
            return self._failed_operation(module, "nonebot_compatibility")

        affected = self._dependent_closure({unit.plugin_id})
        for plugin_id in affected:
            candidate = self.units[plugin_id]
            if candidate.classification is ReloadClassification.FAILED:
                return self._failed_operation(module, "plugin_reload_failed")
            if candidate.classification is not ReloadClassification.HOT_RELOADABLE:
                return self._failed_operation(module, "plugin_not_hot_reloadable")
        return await self._reload_units(affected)

    async def load_new_plugin(
        self,
        module_name: str,
        root: Path,
        changed: set[Path] | None = None,
    ) -> RuntimeOperation:
        """Load a newly installed plugin when its source has no hard boundaries."""
        root = root.resolve()
        changed = {path.resolve() for path in (changed or set())}
        files = self._source_files(root)
        if not files:
            return self._failed_operation(module_name, "plugin_source_missing")
        self.claim_content_changes(files | changed)
        if unit := self._find_unit(module_name):
            return await self.reload_plugin(unit.plugin_id)
        if not self.enabled:
            return await self.request_restart({module_name}, "nonebot_compatibility")

        dependency_files = {
            path
            for path in files | changed
            if path.name in {"requirements.txt", "requirement.txt", "pyproject.toml"}
        }
        if dependency_files:
            return await self.request_dependency_restart(dependency_files)

        provisional = PluginUnit(
            plugin_id=module_name,
            module_name=module_name,
            manager=None,
            root=root,
            files=files,
        )
        classify_unit(provisional)
        if provisional.model_files:
            provisional.reasons.add("orm_model_new_plugin")
            provisional.classification = ReloadClassification.RESTART_REQUIRED
        if provisional.classification is not ReloadClassification.HOT_RELOADABLE:
            reason = sorted(provisional.reasons)[0]
            return await self.request_restart({module_name}, reason)

        async with self._reload_lock:
            from nonebot.matcher import matchers

            before_plugins = {plugin.id_ for plugin in get_loaded_plugins()}
            before_matchers = {
                matcher
                for priority_matchers in matchers.values()
                for matcher in priority_matchers
            }
            try:
                with owner_context(module_name):
                    plugin = nonebot.load_plugin(module_name)
                if plugin is None:
                    raise RuntimeError("plugin_import_failed")

                await self._run_plugin_install(plugin.module_name)
                self.discover_loaded_plugins()
                plugin_id = self._root_owner(plugin.id_) or plugin.id_
                unit = self.units.get(plugin_id)
                if unit is None:
                    raise RuntimeError("plugin_runtime_unit_missing")
                if unit.classification is not ReloadClassification.HOT_RELOADABLE:
                    reason = sorted(unit.reasons)[0]
                    return await self.request_restart({plugin_id}, reason)

                await self._run_reload_startup_hooks({plugin_id})
                self.discover_loaded_plugins()
                unit = self.units.get(plugin_id)
                if (
                    unit
                    and unit.classification is not ReloadClassification.HOT_RELOADABLE
                ):
                    reason = sorted(unit.reasons)[0]
                    return await self.request_restart({plugin_id}, reason)

                self.generation += 1
                await self._reconcile_runtime_metadata()
                await self._invalidate_generation_caches()
                operation = RuntimeOperation(
                    ApplyMode.HOT_RELOADED,
                    "completed",
                    [plugin_id],
                    generation=self.generation,
                )
            except Exception as e:
                logger.error("新安装插件热加载失败，已隔离本次加载", e=e)
                await self._cleanup_failed_new_plugin(
                    module_name, before_plugins, before_matchers
                )
                operation = RuntimeOperation(
                    ApplyMode.FAILED,
                    "failed",
                    [module_name],
                    reason=f"plugin_import_failed:{type(e).__name__}",
                    generation=self.generation,
                )
            self.last_operation = operation
            self._persist_index()
            return operation

    async def _cleanup_failed_new_plugin(
        self,
        module_name: str,
        before_plugins: set[str],
        before_matchers: set[type],
    ) -> None:
        from nonebot.matcher import matchers
        import nonebot.plugin as plugin_module
        from nonebot.plugin import _module_name_to_plugin_id

        plugin_id = _module_name_to_plugin_id(module_name)

        new_plugins = [
            plugin
            for plugin in get_loaded_plugins()
            if plugin.id_ not in before_plugins
            and (
                plugin.module_name == module_name
                or plugin.module_name.startswith(f"{module_name}.")
            )
        ]
        module_names = {
            name
            for name in sys.modules
            if name == module_name or name.startswith(f"{module_name}.")
        }
        module_names.add(module_name)
        new_matchers = {
            matcher
            for priority_matchers in matchers.values()
            for matcher in priority_matchers
            if matcher not in before_matchers
            and (
                (owner := getattr(getattr(matcher, "_source", None), "plugin_id", None))
                == plugin_id
                or bool(owner and owner.startswith(f"{plugin_id}:"))
            )
        }
        clean_matchers(new_matchers)
        self._remove_scheduler_jobs(module_names)
        remove_processors(module_names)
        remove_driver_hooks(nonebot.get_driver(), module_names)
        remove_priority_hooks(module_names)
        remove_plugin_init(module_names)
        self._remove_config_registrations(plugin_id)
        self._remove_config_registrations(module_name)
        self._remove_trie_entries(plugin_id)
        self._remove_trie_entries(module_name)
        owners = {
            owner
            for owner in self._owned_tasks
            if owner in {plugin_id, module_name}
            or owner.startswith(f"{plugin_id}:")
            or owner.startswith(f"{module_name}.")
        }
        tasks = [
            task
            for owner in owners
            for task in self._owned_tasks.pop(owner, set())
            if not task.done()
        ]
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        remove_plugins(new_plugins)
        failed_managers = {
            plugin.manager for plugin in new_plugins if plugin.manager is not None
        }
        for manager in list(plugin_module._managers):
            controlled = set(manager.controlled_modules.values())
            if manager in failed_managers or module_name in controlled:
                with contextlib.suppress(ValueError):
                    plugin_module._managers.remove(manager)
        for name in sorted(
            module_names, key=lambda item: item.count("."), reverse=True
        ):
            sys.modules.pop(name, None)
        self.discover_loaded_plugins()

    async def apply_plugin_changes(self, changed: set[Path]) -> RuntimeOperation:
        affected = self.affected_units(changed)
        if not affected:
            return await self.request_restart(set(), "core_source_changed")
        if not self.enabled:
            return await self.request_restart(affected, "nonebot_compatibility")
        if any(
            path.name in {"requirements.txt", "requirement.txt", "pyproject.toml"}
            for path in changed
        ):
            return await self.request_restart(affected, "plugin_dependencies_changed")
        removed = {
            plugin_id
            for plugin_id in affected
            if (root := self.units[plugin_id].root) is not None and not root.exists()
        }
        if removed:
            if removed != affected:
                return await self.request_restart(affected, "plugin_dependency_removed")
            for plugin_id in removed:
                unit = self.units[plugin_id]
                if unit.classification is not ReloadClassification.HOT_RELOADABLE:
                    return await self.request_restart(removed, sorted(unit.reasons)[0])
            return await self._unload_removed_units(removed)
        for plugin_id in affected:
            unit = self.units[plugin_id]
            if unit.classification is not ReloadClassification.HOT_RELOADABLE:
                return await self.request_restart(affected, sorted(unit.reasons)[0])
            if changed_model_file(unit, changed):
                return await self.request_restart(affected, "orm_model_changed")
        return await self._reload_units(affected)

    async def _unload_removed_units(self, affected: set[str]) -> RuntimeOperation:
        async with self._reload_lock:
            try:
                for plugin_id in self._reload_order(affected):
                    await self._drain_and_unload(self.units[plugin_id])
                self.generation += 1
                self.discover_loaded_plugins()
                await self._reconcile_runtime_metadata()
                await self._invalidate_generation_caches()
                operation = RuntimeOperation(
                    ApplyMode.HOT_RELOADED,
                    "completed",
                    sorted(affected),
                    generation=self.generation,
                )
            except Exception as e:
                logger.error("插件热卸载失败，已标记为需要重启", e=e)
                operation = await self.request_restart(affected, "plugin_unload_failed")
            self.last_operation = operation
            self._persist_index()
            return operation

    def classification_for(self, module: str) -> dict[str, Any]:
        unit = self._find_unit(module)
        if not unit:
            return {
                "reload_support": "restart_required",
                "reload_reasons": ["not_loaded"],
            }
        return {
            "reload_support": unit.classification.value,
            "reload_reasons": sorted(unit.reasons),
        }

    async def _reload_units(self, affected: set[str]) -> RuntimeOperation:
        async with self._reload_lock:
            order = self._reload_order(affected)
            try:
                for plugin_id in order:
                    await self._drain_and_unload(self.units[plugin_id])
                for plugin_id in reversed(order):
                    unit = self.units[plugin_id]
                    with owner_context(plugin_id):
                        plugin = unit.manager.load_plugin(unit.module_name)
                    if plugin is None:
                        raise RuntimeError(f"plugin_import_failed:{plugin_id}")
                    await self._run_plugin_install(plugin.module_name)
                await self._run_reload_startup_hooks(affected)
                self.generation += 1
                self.discover_loaded_plugins()
                await self._reconcile_runtime_metadata()
                await self._invalidate_generation_caches()
                operation = RuntimeOperation(
                    ApplyMode.HOT_RELOADED,
                    "completed",
                    sorted(affected),
                    generation=self.generation,
                )
            except Exception as e:
                reason = f"{type(e).__name__}:{e}"
                logger.error("插件热加载失败，已停止处理该插件代际", e=e)
                for plugin_id in affected:
                    if unit := self.units.get(plugin_id):
                        unit.classification = ReloadClassification.FAILED
                        unit.last_error = type(e).__name__
                operation = RuntimeOperation(
                    ApplyMode.FAILED,
                    "failed",
                    sorted(affected),
                    reason=reason,
                    generation=self.generation,
                )
            self.last_operation = operation
            self._persist_index()
            return operation

    def _reload_order(self, affected: set[str]) -> list[str]:
        result: list[str] = []
        visiting: set[str] = set()

        def visit(plugin_id: str) -> None:
            if plugin_id in result:
                return
            if plugin_id in visiting:
                raise RuntimeError("plugin_dependency_cycle")
            visiting.add(plugin_id)
            for dependency in self.units[plugin_id].dependencies & affected:
                visit(dependency)
            visiting.remove(plugin_id)
            result.append(plugin_id)

        for plugin_id in sorted(affected):
            visit(plugin_id)
        # Dependents are unloaded before providers.
        return list(reversed(result))

    async def _drain_and_unload(self, unit: PluginUnit) -> None:
        unit.draining = True
        if unit.in_flight:
            try:
                await asyncio.wait_for(
                    self._drained_event(unit.plugin_id).wait(), timeout=10
                )
            except TimeoutError as e:
                raise RuntimeError(f"plugin_drain_timeout:{unit.plugin_id}") from e

        tasks = [
            task
            for owner in self._owned_keys_for_unit(unit.plugin_id)
            for task in self._owned_tasks.pop(owner, set())
            if not task.done()
        ]
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

        await self._run_reload_shutdown_hooks(unit.module_names)

        plugins = [
            plugin
            for plugin in get_loaded_plugins()
            if plugin.id_ == unit.plugin_id
            or plugin.id_.startswith(f"{unit.plugin_id}:")
        ]
        clean_matchers(matcher for plugin in plugins for matcher in plugin.matcher)
        self._remove_scheduler_jobs(unit.module_names)
        remove_processors(unit.module_names)
        remove_driver_hooks(nonebot.get_driver(), unit.module_names)
        remove_priority_hooks(unit.module_names)
        remove_plugin_init(unit.module_names)
        self._remove_config_registrations(unit.plugin_id)
        self._remove_trie_entries(unit.plugin_id)
        remove_plugins(plugins)
        remove_nested_managers(unit.nested_managers)
        for module_name in sorted(
            unit.module_names, key=lambda name: name.count("."), reverse=True
        ):
            module = sys.modules.get(module_name)
            if module and _module_file(module) in unit.model_files:
                continue
            sys.modules.pop(module_name, None)

    def _remove_config_registrations(self, plugin_id: str) -> None:
        from zhenxun.configs.config import Config

        owners = {
            owner
            for owner in self._config_registrations
            if owner == plugin_id or owner.startswith(f"{plugin_id}:")
        }
        for owner in owners:
            for module, key in self._config_registrations.pop(owner, set()):
                group = Config._data.get(module)
                if group:
                    group.configs.pop(key, None)
                entry = f"{module}:{key}".lower()
                while entry in Config.add_module:
                    Config.add_module.remove(entry)

    def _remove_trie_entries(self, plugin_id: str) -> None:
        from nonebot.rule import TrieRule

        for prefix, entries in list(self._trie_entries.items()):
            removed = [
                item
                for item in entries
                if item[0] == plugin_id
                or bool(item[0] and item[0].startswith(f"{plugin_id}:"))
            ]
            if not removed:
                continue
            active = TrieRule.prefix.get(prefix)
            remaining = [item for item in entries if item not in removed]
            self._trie_entries[prefix] = remaining
            if any(value is active for _, value in removed):
                with contextlib.suppress(KeyError):
                    TrieRule.prefix.pop(prefix)
                if remaining:
                    TrieRule.prefix[prefix] = remaining[0][1]
            if not remaining:
                self._trie_entries.pop(prefix, None)

    def _remove_scheduler_jobs(self, module_names: set[str]) -> None:
        try:
            from nonebot_plugin_apscheduler import scheduler
        except (ImportError, RuntimeError):
            return
        for job in scheduler.get_jobs():
            owner = self._job_owners.get(job.id)
            if _callable_module(job.func) in module_names or (
                owner
                and self._root_owner(owner) in self.units
                and self.units[self._root_owner(owner)].module_names == module_names
            ):
                with contextlib.suppress(Exception):
                    scheduler.remove_job(job.id)
                self._job_owners.pop(job.id, None)

    async def _run_plugin_install(self, module_name: str) -> None:
        from zhenxun.services.plugin_init import PluginInitManager

        for registered in list(PluginInitManager.plugins):
            if registered == module_name or registered.startswith(f"{module_name}."):
                await PluginInitManager.install(registered)

    async def _run_reload_shutdown_hooks(self, module_names: set[str]) -> None:
        from zhenxun.utils.enum import PriorityLifecycleType
        from zhenxun.utils.manager.priority_manager import (
            PriorityLifecycle,
            _run_hook,
        )

        priority_data = PriorityLifecycle._data.get(PriorityLifecycleType.SHUTDOWN, {})
        for priority in sorted(priority_data):
            for func in list(priority_data[priority]):
                if getattr(func, "__module__", "") in module_names:
                    await _run_hook(func, priority, "shutdown")
        driver = nonebot.get_driver()
        funcs = [
            func
            for func in driver._lifespan._shutdown_funcs
            if getattr(func, "__module__", "") in module_names
        ]
        if funcs:
            await driver._lifespan._run_lifespan_func(funcs)

    async def _run_reload_startup_hooks(self, affected: set[str]) -> None:
        from zhenxun.utils.enum import PriorityLifecycleType
        from zhenxun.utils.manager.priority_manager import (
            PriorityLifecycle,
            _run_hook,
        )

        module_names = {
            module
            for plugin_id in affected
            if (unit := self.units.get(plugin_id))
            for module in unit.module_names
        }
        priority_data = PriorityLifecycle._data.get(PriorityLifecycleType.STARTUP, {})
        for priority in sorted(priority_data):
            for func in list(priority_data[priority]):
                if getattr(func, "__module__", "") in module_names:
                    await _run_hook(func, priority)
        driver = nonebot.get_driver()
        funcs = [
            func
            for func in driver._lifespan._startup_funcs
            if getattr(func, "__module__", "") in module_names
        ]
        if funcs:
            await driver._lifespan._run_lifespan_func(funcs)

    async def _reconcile_runtime_metadata(self) -> None:
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
            from zhenxun.builtin_plugins.init.init_task import (
                reconcile_task_runtime,
            )

            managed_config_writes = reconcile_config_runtime() or set()
            for path in managed_config_writes:
                self.mark_content_processed(path)
            if managed_config_writes:
                logger.debug(
                    "插件元数据产生的配置写入已纳入当前运行时操作，" "跳过后续重复重载"
                )
            await reconcile_plugin_runtime()
            await reconcile_task_runtime()
        except Exception as e:
            logger.error("插件代际元数据协调失败", e=e)
            raise

    async def _invalidate_generation_caches(self) -> None:
        registry_module = sys.modules.get("zhenxun.plugins.chatinter.plugin_registry")
        if registry_module:
            try:
                registry = getattr(registry_module, "PluginRegistry")
                shutdown = getattr(registry, "shutdown", None)
                if shutdown:
                    value = shutdown()
                    if inspect.isawaitable(value):
                        await value
            except (AttributeError, TypeError):
                pass
        cache_module = sys.modules.get("zhenxun.services.cache.runtime_cache")
        if not cache_module:
            return
        from tortoise import Tortoise

        if not Tortoise._inited:
            return
        try:
            plugin_cache = getattr(cache_module, "PluginInfoMemoryCache")
            task_cache = getattr(cache_module, "TaskInfoMemoryCache")
            await plugin_cache.refresh()
            await task_cache.refresh()
        except Exception as e:
            logger.warning(f"插件代际缓存刷新失败: {type(e).__name__}")

    async def reload_config_consumers(
        self,
        changed_dependencies: set[tuple[str, str]],
        *,
        submit_restart: bool = True,
    ) -> RuntimeOperation | None:
        affected = {
            unit.plugin_id
            for unit in self.units.values()
            if any(
                dependency in changed_dependencies
                or (dependency[0], "*") in changed_dependencies
                for dependency in unit.config_dependencies
            )
        }
        if not affected:
            return None
        logger.debug(
            f"配置变更匹配到 {len(affected)} 个导入期消费者，"
            f"处理方式: {'自动协调' if submit_restart else '等待用户确认'}"
        )
        if any(
            self.units[plugin_id].classification
            is not ReloadClassification.HOT_RELOADABLE
            for plugin_id in affected
        ):
            return await self.request_restart(
                affected,
                "import_time_config_consumer_requires_restart",
                submit_launcher=submit_restart,
            )
        return await self._reload_units(affected)

    def claim_content_changes(self, paths: set[Path]) -> set[Path]:
        from hashlib import sha256

        changed: set[Path] = set()
        for path in paths:
            try:
                digest = sha256(path.read_bytes()).hexdigest()
            except OSError:
                digest = "missing"
            if self._content_digests.get(path) == digest:
                continue
            self._content_digests[path] = digest
            changed.add(path)
        return changed

    async def process_changes(self, paths: set[Path]) -> RuntimeOperation | None:
        if self._change_coordinator is None:
            from .coordinator import RuntimeChangeCoordinator

            self._change_coordinator = RuntimeChangeCoordinator(self)
        return await self._change_coordinator.process(paths)

    def mark_content_processed(self, path: Path) -> None:
        self.claim_content_changes({path.resolve()})

    async def request_restart(
        self,
        affected: set[str],
        reason: str,
        *,
        submit_launcher: bool = True,
    ) -> RuntimeOperation:
        self.pending_restart.add(reason)
        mode = ApplyMode.RESTART_PENDING
        if submit_launcher and bool(
            __import__("os").environ.get("ZHENXUN_LAUNCHER_PID")
        ):
            try:
                from zhenxun.utils._restart_utils import request_restart

                accepted, _ = await request_restart(f"runtime_reload:{reason}")
                if accepted:
                    mode = ApplyMode.RESTART_REQUESTED
            except Exception as e:
                logger.warning(f"自动重启请求失败: {type(e).__name__}")
        operation = RuntimeOperation(
            mode,
            "pending_restart",
            sorted(affected),
            reason,
            self.generation,
        )
        self.last_operation = operation
        self._persist_index()
        return operation

    async def request_dependency_restart(self, changed: set[Path]) -> RuntimeOperation:
        reason = "dependencies_changed"
        self.pending_restart.add(reason)
        mode = ApplyMode.RESTART_PENDING
        if bool(__import__("os").environ.get("ZHENXUN_LAUNCHER_PID")):
            try:
                from zhenxun.utils._restart_utils import (
                    request_dependency_restart,
                )

                accepted, _ = await request_dependency_restart(
                    "runtime_reload:dependencies_changed", changed
                )
                if accepted:
                    mode = ApplyMode.RESTART_REQUESTED
            except Exception as e:
                logger.warning(f"依赖同步重启请求失败: {type(e).__name__}")
        operation = RuntimeOperation(
            mode,
            "pending_restart",
            sorted(path.name for path in changed),
            reason,
            self.generation,
        )
        self.last_operation = operation
        self._persist_index()
        return operation

    def status(self) -> dict[str, Any]:
        counts = {item.value: 0 for item in ReloadClassification}
        for unit in self.units.values():
            counts[unit.classification.value] += 1
        return {
            "watching": self._watcher_task is not None
            and not self._watcher_task.done(),
            "hot_reload_enabled": self.enabled,
            "compatibility_error": self.compatibility_error,
            "plugin_runtime_generation": self.generation,
            "webui_revision": self.webui_revision,
            "pending_restart": bool(self.pending_restart),
            "pending_restart_reasons": sorted(self.pending_restart),
            "classification_counts": counts,
            "plugins": [
                unit.public_dict()
                for unit in sorted(self.units.values(), key=lambda item: item.plugin_id)
            ],
            "last_operation": self.last_operation.public_dict()
            if self.last_operation
            else None,
        }

    def _persist_index(self) -> None:
        data = {
            "version": 1,
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "generation": self.generation,
            "plugins": [unit.public_dict() for unit in self.units.values()],
            "pending_restart_reasons": sorted(self.pending_restart),
        }
        try:
            _INDEX_FILE.parent.mkdir(parents=True, exist_ok=True)
            temp = _INDEX_FILE.with_suffix(".tmp")
            temp.write_text(
                json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8"
            )
            temp.replace(_INDEX_FILE)
        except OSError as e:
            logger.warning(f"运行时生命周期索引写入失败: {type(e).__name__}")

    @staticmethod
    def _load_index_cache() -> dict[str, dict[str, Any]]:
        try:
            data = json.loads(_INDEX_FILE.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            return {}
        if data.get("version") != 1 or not isinstance(data.get("plugins"), list):
            return {}
        return {
            str(item["plugin_id"]): item
            for item in data["plugins"]
            if isinstance(item, dict) and item.get("plugin_id")
        }

    @staticmethod
    def _read_webui_revision() -> str:
        version_file = Path("data/web_ui/public/version.json")
        try:
            data = json.loads(version_file.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            return ""
        return str(
            data.get("revision") or data.get("commit") or data.get("version") or ""
        )


plugin_runtime_manager = PluginRuntimeManager()
