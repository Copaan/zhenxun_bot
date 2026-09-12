from __future__ import annotations

import asyncio
from collections import defaultdict, deque
from collections.abc import Callable, Coroutine
from concurrent.futures import Future as ConcurrentFuture
import contextlib
from contextvars import Context, ContextVar
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime, timezone
from functools import wraps
import inspect
import json
import os
from pathlib import Path
import subprocess
import sys
import threading
import time
from types import MethodType, ModuleType
from typing import Any
from uuid import uuid4
import weakref

import nonebot
from nonebot.plugin import get_loaded_plugins

from zhenxun.services.lifecycle import (
    LeaseState,
    LifecycleError,
    PluginIncarnation,
    ResourceReceipt,
    capture_runtime_providers,
)
from zhenxun.services.lifecycle.deadline import (
    check_budget,
    current_budget,
    remaining_timeout,
    shutdown_budget,
)
from zhenxun.services.lifecycle.provider_undo import (
    active_undo,
    provider_capture,
    provider_transaction,
)
from zhenxun.services.log import logger
from zhenxun.services.runtime_mutation import (
    MutationCancelled,
    managed_mutation,
    runtime_mutation_coordinator,
)
from zhenxun.services.startup import startup_coordinator
from zhenxun.utils.atomic_json import write_json_locked

from .classifier import changed_model_file, classify_unit
from .compat import (
    NoneBotCompatibilityError,
    clean_matchers,
    remove_bot_api_hooks,
    remove_driver_hooks,
    remove_nested_managers,
    remove_plugin_init,
    remove_plugins,
    remove_priority_hooks,
    remove_processors,
    verify_nonebot_compatibility,
)
from .models import ApplyMode, PluginUnit, ReloadClassification, RuntimeOperation
from .ownership import (
    LifecycleWork,
    current_owner,
    import_owner,
    initialization_retainer,
    lifecycle_callback_context,
    lifecycle_work_context,
    lifecycle_work_phase,
    owner_context,
    resource_context,
    shared_executor_submission,
)
from .signatures import async_callable, typed_wraps

_INDEX_FILE = Path("data/runtime/lifecycle-index-v2.json")
_TASK_CANCEL_TIMEOUT = 2.0


class PluginRecoveryRequired(LifecycleError):
    """Old resources cannot be proven stopped; never reactivate a generation."""


def _static_env_dependencies(files: set[Path]) -> set[str]:
    """Compatibility helper backed by the per-file classifier cache."""
    unit = PluginUnit(
        plugin_id="static-env-scan",
        module_name="static-env-scan",
        manager=None,
        root=None,
        files=files,
    )
    classify_unit(unit)
    return unit.env_dependencies


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


def _module_belongs_to(module_name: str, module_names: set[str]) -> bool:
    return any(
        module_name == candidate or module_name.startswith(f"{candidate}.")
        for candidate in module_names
    )


@dataclass(slots=True)
class PluginReloadCheckpoint:
    affected: set[str]
    module_names: set[str]
    provider_snapshot: Any
    generation: int
    units: dict[str, PluginUnit]
    module_to_unit: dict[str, str]
    modules: dict[str, ModuleType]
    incarnations: dict[str, PluginIncarnation]
    incarnation_history: list[PluginIncarnation]
    incarnation_states: dict[str, LeaseState]
    unit_state: dict[str, dict[str, Any]]
    priority_entries: list[tuple[Any, int, int, Callable, Any]]
    plugin_init_entries: dict[str, Any]
    config_entries: dict[tuple[str, str], Any]
    config_modules: set[str]
    config_owners: dict[str, set[tuple[str, str]]]
    config_add_module: list[str]
    scheduler_jobs: list[dict[str, Any]]
    job_owners: dict[str, str]
    component_ids: set[str]
    component_states: dict[str, str]
    shared_globals: dict[str, dict[str, Any]]
    shared_dependency_owners: dict[str, set[str]]
    resource_summary: dict[str, dict[str, int]]


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
        self._owned_tasks: dict[str, set[asyncio.Task[Any]]] = defaultdict(set)
        self._entry_tasks: dict[str, dict[asyncio.Task, int]] = defaultdict(dict)
        self._entry_calls: dict[tuple, int] = {}
        self._owned_threads: dict[str, set[threading.Thread]] = defaultdict(set)
        self._owned_processes: dict[str, set[subprocess.Popen[Any]]] = defaultdict(set)
        self._owned_handles: dict[str, weakref.WeakSet[asyncio.Handle]] = defaultdict(
            weakref.WeakSet
        )
        self._owned_io_watchers: dict[str, set[tuple[str, int]]] = defaultdict(set)
        self._owned_executor_futures: dict[str, set[ConcurrentFuture[Any]]] = (
            defaultdict(set)
        )
        self._ownership_lock = threading.RLock()
        self._drained_events: dict[str, asyncio.Event] = {}
        self._original_task_factory: Callable[..., asyncio.Future[Any]] | None = None
        self._task_factory_installed = False
        self._loop_hook_originals: dict[str, Callable[..., Any]] = {}
        self._tracked_loop: asyncio.AbstractEventLoop | None = None
        self._shared_executors: weakref.WeakSet = weakref.WeakSet()
        self._cancellation_work: dict[tuple, LifecycleWork] = {}
        self._cancellation_requests: dict[asyncio.Task, tuple[str, int]] = {}
        self._executor_owners: weakref.WeakKeyDictionary = weakref.WeakKeyDictionary()
        self._original_executor_init = None
        self._connection_tasks: weakref.WeakSet = weakref.WeakSet()
        self._watcher_task: asyncio.Task[Any] | None = None
        self._watcher_refresh_task: asyncio.Task[Any] | None = None
        self._watcher_refresh_requested = False
        self.watcher_state = "idle"
        self.watcher_retry_count = 0
        self.watcher_last_error: str | None = None
        self.watcher_roots: list[str] = []
        self._change_coordinator: Any | None = None
        self._pending_config_dependencies: dict[str, set[tuple[str, str]]] = (
            defaultdict(set)
        )
        self._pending_env_dependencies: dict[str, set[str]] = defaultdict(set)
        self._original_matcher_run: Callable[..., Any] | None = None
        self._original_matcher_dispatch: Callable[..., Any] | None = None
        self._matcher_execution: ContextVar[Any] = ContextVar(
            "plugin_matcher_execution", default=None
        )
        self._original_matcher_new: Callable[..., Any] | None = None
        self._original_bot_api_hooks: dict[str, Callable[..., Any]] = {}
        self._original_asgi_methods: dict[str, Callable[..., Any]] = {}
        self._original_processor_hooks: dict[str, tuple[Any, Any]] = {}
        self._original_driver_hooks: dict[str, Callable[..., Any]] = {}
        self._original_require_hooks: dict[tuple[Any, str], Callable[..., Any]] = {}
        self._asgi_route_owners: dict[int, str] = {}
        self._unsafe_route_owners: set[str] = set()
        self._original_get_config: Callable[..., Any] | None = None
        self._original_get_plugin_config: Callable[..., Any] | None = None
        self._original_os_getenv: Callable[..., Any] | None = None
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
        self._content_change_holds: dict[Path, int] = defaultdict(int)
        self._content_changes_released = asyncio.Event()
        self._content_changes_released.set()
        self._classification_cache = self._load_index_cache()
        self._original_thread_start: Callable[..., Any] | None = None
        self._original_anyio_worker = None
        self._wrapped_anyio_worker = None
        self._original_popen_init: Callable[..., Any] | None = None
        self._internal_lifecycle_hooks: list[Callable[..., Any]] = []
        self._installed = False
        self._index_ready = asyncio.Event()
        self._incarnations: dict[str, PluginIncarnation] = {}
        self._incarnation_history: list[PluginIncarnation] = []
        self._candidate_roots: set[str] = set()
        self._initialization_work: dict[str, list[LifecycleWork]] = defaultdict(list)
        self._hook_failures: deque[dict[str, Any]] = deque(maxlen=64)
        self._entry_diagnostics: dict[str, int] = defaultdict(int)
        self._shared_dependency_evidence: dict[str, list[str]] = {}
        self._integrity_failures: set[str] = set()

    def install(self) -> None:
        if self._installed:
            return
        self._installed = True
        try:
            verify_nonebot_compatibility()
        except NoneBotCompatibilityError as e:
            self.enabled = False
            self.compatibility_error = str(e)
            self._installed = False
            logger.warning(
                "插件热加载兼容层不可用，将自动使用重启模式: "
                f"{self.compatibility_error}"
            )
            return

        try:
            driver = nonebot.get_driver()
            self._install_matcher_execution_wrapper()
            self._install_matcher_registration_tracking()
            self._install_priority_lifecycle_tracking()
            self._install_plugin_init_tracking()
            self._install_config_access_tracking()
            self._install_trie_tracking()
            self._install_scheduler_tracking()
            self._install_processor_tracking()
            self._install_driver_hook_tracking(driver)
            self._install_bot_api_hook_tracking()
            self._install_asgi_route_tracking()
            self._install_require_tracking()
            self._install_thread_process_tracking()
            self._verify_hook_canary(driver)
        except BaseException as error:
            self.compatibility_error = f"hook_canary_failed:{type(error).__name__}"
            self.enabled = False
            self.uninstall()
            logger.warning(
                "插件热加载 Hook 自检失败，已完整撤销并切换为重启模式: "
                f"{self.compatibility_error}"
            )
            return
        self.compatibility_error = None
        self.enabled = True

        from zhenxun.utils.manager.priority_manager import PriorityLifecycle

        @PriorityLifecycle.on_startup(
            priority=-90,
            stage="management",
            timeout=5,
            component_id="management:runtime_tracking",
            scope="worker",
        )
        async def _start_runtime_tracking() -> None:
            self._install_task_factory()

        @PriorityLifecycle.on_startup(
            priority=10,
            stage="warmup",
            timeout=60,
            parallel_safe=True,
            failure_policy="degrade",
            task_id="warmup:runtime_index",
            component_id="warmup:runtime_index",
            resource_group="runtime_index",
            restart_policy="component",
            config_keys=("RUNTIME_WATCH_MODE",),
            pass_context=True,
        )
        async def _start_runtime_manager(context) -> None:
            await self.discover_loaded_plugins_async()
            self._index_ready.set()
            from .watcher import watch_runtime_changes

            if self.runtime_watch_mode() == "disabled":
                self.watcher_state = "disabled"
                return
            self._watcher_task = context.spawn_task(
                watch_runtime_changes(self), name="zhenxun-runtime-watcher"
            )

        @PriorityLifecycle.on_shutdown(
            priority=900, component_id="management:runtime_tracking"
        )
        async def _stop_runtime_manager() -> None:
            if self._watcher_task:
                self._watcher_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await self._watcher_task
                self._watcher_task = None
            try:
                await self._cancel_all_owned_tasks()
            finally:
                self.uninstall()

        self._internal_lifecycle_hooks.extend(
            [_start_runtime_tracking, _start_runtime_manager, _stop_runtime_manager]
        )

    def uninstall(self) -> None:
        self._restore_task_factory()
        self._restore_thread_process_tracking()
        self._restore_global_hooks()
        if self._internal_lifecycle_hooks:
            from zhenxun.utils.manager.priority_manager import PriorityLifecycle

            hooks = set(self._internal_lifecycle_hooks)
            for priorities in PriorityLifecycle._data.values():
                for priority, funcs in list(priorities.items()):
                    priorities[priority] = [func for func in funcs if func not in hooks]
                    if not priorities[priority]:
                        priorities.pop(priority, None)
            for func in hooks:
                PriorityLifecycle._metadata.pop(func, None)
            self._internal_lifecycle_hooks.clear()
        self._installed = False

    @staticmethod
    def _container_contains_identity(container: Any, identity: int) -> bool:
        if isinstance(container, dict):
            return any(
                PluginRuntimeManager._container_contains_identity(value, identity)
                for value in container.values()
            )
        if isinstance(container, list | tuple | set):
            return any(
                id(value) == identity
                or PluginRuntimeManager._container_contains_identity(value, identity)
                for value in container
            )
        return id(container) == identity

    def _verify_hook_canary(self, driver: Any) -> None:
        snapshot = capture_runtime_providers(driver)
        sentinels: list[tuple[Any, object]] = []
        try:
            for index, provider in enumerate(snapshot.containers):
                sentinel = object()
                container = provider.container
                if provider.kind == "mapping":
                    key = f"__zhenxun_hook_canary__{id(self)}_{index}"
                    container[key] = [sentinel]
                elif provider.kind == "sequence":
                    container.append(sentinel)
                else:
                    container.add(sentinel)
                sentinels.append((container, sentinel))
        finally:
            snapshot.rollback()
        if any(
            self._container_contains_identity(container, id(sentinel))
            for container, sentinel in sentinels
        ):
            raise NoneBotCompatibilityError("provider_round_trip_failed")

    def _capture_loaded_plugin_index(
        self,
    ) -> tuple[dict[str, PluginUnit], dict[str, str]]:
        build_started = time.monotonic()
        plugins = list(get_loaded_plugins())
        roots: dict[str, list[Any]] = defaultdict(list)
        for plugin in plugins:
            root = plugin
            while root.parent_plugin:
                root = root.parent_plugin
            if all(item is not root for item in roots[root.id_]):
                roots[root.id_].append(root)
            if plugin is root:
                continue
            roots[root.id_].append(plugin)

        module_owner: dict[str, str] = {}
        module_names_by_root: dict[str, set[str]] = defaultdict(set)
        for root_id, members in roots.items():
            for plugin in members:
                module_owner[plugin.module_name] = root_id
                module_names_by_root[root_id].add(plugin.module_name)
        for name in list(sys.modules):
            candidate = name
            while candidate:
                if owner := module_owner.get(candidate):
                    module_names_by_root[owner].add(name)
                    break
                candidate = candidate.rpartition(".")[0]

        startup_coordinator.record_operation(
            "runtime_index:module_ownership",
            "warmup",
            "completed",
            (time.monotonic() - build_started) * 1000,
        )

        units: dict[str, PluginUnit] = {}
        module_to_unit: dict[str, str] = {}
        for root_id, members in roots.items():
            root_plugin = next(plugin for plugin in members if plugin.id_ == root_id)
            module_names = module_names_by_root[root_id]
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
            for owner, dependencies in self._pending_config_dependencies.items():
                if owner == root_id or owner.startswith(f"{root_id}:"):
                    unit.config_dependencies.update(dependencies)
            for owner, dependencies in self._pending_env_dependencies.items():
                if owner == root_id or owner.startswith(f"{root_id}:"):
                    unit.env_dependencies.update(dependencies)
            if any(plugin.matcher for plugin in members):
                unit.env_dependencies.update(
                    {"COMMAND_START", "COMMAND_SEP", "ALCONNA_USE_COMMAND_START"}
                )
            for owner, dependencies in self._pending_dependencies.items():
                if owner == root_id or owner.startswith(f"{root_id}:"):
                    unit.dependencies.update(dependencies)
            units[root_id] = unit
            for name in module_names:
                module_to_unit[name] = root_id

        return units, module_to_unit

    @staticmethod
    def _classify_loaded_plugin_index(index, classification_cache):
        started = time.monotonic()
        units, _ = index
        for root_id, unit in units.items():
            if unit.root and unit.root.is_dir():
                unit.files.update(unit.root.rglob("*.py"))
                unit.files.update(unit.root.glob("requirement*.txt"))
            env_dependencies = set(unit.env_dependencies)
            classify_unit(unit, classification_cache.get(root_id))
            unit.env_dependencies.update(env_dependencies)
        startup_coordinator.record_operation(
            "runtime_index:file_classification",
            "warmup",
            "completed",
            (time.monotonic() - started) * 1000,
        )
        return index

    def _build_loaded_plugin_index(self):
        return self._classify_loaded_plugin_index(
            self._capture_loaded_plugin_index(), deepcopy(self._classification_cache)
        )

    @staticmethod
    def _index_files_current(index) -> bool:
        for unit in index[0].values():
            for name, record in unit.file_cache.items():
                try:
                    stat = Path(name).stat()
                except OSError:
                    return False
                if any(
                    record.get(key) != value
                    for key, value in (
                        ("size", stat.st_size),
                        ("mtime_ns", stat.st_mtime_ns),
                        ("ctime_ns", stat.st_ctime_ns),
                    )
                ):
                    return False
            if unit.root and unit.root.is_dir():
                current = set(unit.root.rglob("*.py")) | set(
                    unit.root.glob("requirement*.txt")
                )
                previous = {
                    path for path in unit.files if path.is_relative_to(unit.root)
                }
                if current != previous:
                    return False
        return True

    def _index_source_revision(self):
        return (
            self.generation,
            tuple(
                sorted(
                    (plugin.id_, id(plugin), id(plugin.module))
                    for plugin in get_loaded_plugins()
                )
            ),
        )

    def _commit_loaded_plugin_index(
        self,
        units: dict[str, PluginUnit],
        module_to_unit: dict[str, str],
        *,
        activate: bool = True,
    ) -> None:
        commit_started = time.monotonic()
        self.units = units
        self.module_to_unit = module_to_unit
        self._candidate_roots = {
            self._root_owner(root) or root for root in self._candidate_roots
        }
        for unit in self.units.values():
            incarnation = self._incarnation_for_unit(unit)
            if incarnation is None or incarnation.lease_state in {
                LeaseState.REVOKED,
                LeaseState.FAILED,
            }:
                incarnation = self._new_incarnation(unit.plugin_id)
            incarnation.plugin_id = unit.plugin_id
            incarnation.source_digest = unit.fingerprint
            if activate:
                incarnation.lease_state = LeaseState.ACTIVE
            self._incarnations[unit.plugin_id] = incarnation
            for module_name in unit.module_names:
                module_incarnation = self._incarnations.get(module_name)
                if module_incarnation is None or module_incarnation.lease_state in {
                    LeaseState.REVOKED,
                    LeaseState.FAILED,
                }:
                    self._incarnations[module_name] = incarnation
            unit.incarnation_id = incarnation.incarnation_id
        for plugin in get_loaded_plugins():
            incarnation = self._incarnations.get(plugin.id_) or self._incarnations.get(
                plugin.module_name
            )
            if incarnation is None:
                incarnation = self._ensure_incarnation(plugin.id_)
            self._incarnations[plugin.id_] = incarnation
            self._incarnations[plugin.module_name] = incarnation
            if activate and incarnation.lease_state is LeaseState.PREPARED:
                incarnation.lease_state = LeaseState.ACTIVE
            for matcher in plugin.matcher:
                if not getattr(matcher, "__zhenxun_incarnation_id__", None):
                    matcher.__zhenxun_registration_owner__ = plugin.id_
                    matcher.__zhenxun_incarnation_id__ = incarnation.incarnation_id
        self._collect_dependencies()
        self._collect_runtime_boundaries()
        observation = self._lifecycle_observation_index()
        for unit in self.units.values():
            if unit.plugin_id not in self._candidate_roots:
                self._observe_plugin_scope(unit, observation=observation)
        self.webui_revision = self._read_webui_revision()
        startup_coordinator.record_operation(
            "runtime_index:dependency_commit",
            "warmup",
            "completed",
            (time.monotonic() - commit_started) * 1000,
        )
        persist_started = time.monotonic()
        self._persist_index()
        startup_coordinator.record_operation(
            "runtime_index:persist",
            "warmup",
            "completed",
            (time.monotonic() - persist_started) * 1000,
        )

    def discover_loaded_plugins(self, *, activate: bool = True) -> None:
        self._commit_loaded_plugin_index(
            *self._build_loaded_plugin_index(), activate=activate
        )

    async def discover_loaded_plugins_async(self) -> None:
        for _ in range(2):
            revision = self._index_source_revision()
            captured = self._capture_loaded_plugin_index()
            cached = deepcopy(self._classification_cache)
            index = await asyncio.to_thread(
                self._classify_loaded_plugin_index, captured, cached
            )
            files_current = await asyncio.to_thread(self._index_files_current, index)
            if files_current and revision == self._index_source_revision():
                self._commit_loaded_plugin_index(*index)
                return
        raise RuntimeError("runtime_index_source_changed")

    def activate_loaded_incarnations(self) -> None:
        for plugin in get_loaded_plugins():
            incarnation = self._incarnations.get(plugin.id_) or self._incarnations.get(
                plugin.module_name
            )
            if incarnation is None or incarnation.lease_state in {
                LeaseState.REVOKED,
                LeaseState.FAILED,
            }:
                incarnation = self._new_incarnation(plugin.id_)
            incarnation.plugin_id = plugin.id_
            incarnation.lease_state = LeaseState.ACTIVE
            self._incarnations[plugin.id_] = incarnation
            self._incarnations[plugin.module_name] = incarnation

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
        candidate = module_name
        while candidate:
            if owner := self.module_to_unit.get(candidate):
                return owner
            candidate = candidate.rpartition(".")[0]
        return None

    def track_config_access(self, module: str, key: str) -> None:
        owner = import_owner()
        if not owner:
            return
        dependency = (module, key.upper())
        self._pending_config_dependencies[owner].add(dependency)
        if owner in self.units:
            self.units[owner].config_dependencies.add(dependency)

    def track_env_access(self, key: str) -> None:
        owner = import_owner()
        if not owner:
            return
        normalized = key.upper()
        self._pending_env_dependencies[owner].add(normalized)
        if owner in self.units:
            self.units[owner].env_dependencies.add(normalized)

    def _install_matcher_execution_wrapper(self) -> None:
        from nonebot.matcher import Matcher
        import nonebot.message as message

        if self._original_matcher_run:
            return
        original = Matcher.run
        self._original_matcher_run = original
        manager = self

        @wraps(message.check_and_run_matcher)
        async def dispatch_with_owner(Matcher, *args, **kwargs):
            with manager._matcher_admission(Matcher) as admitted:
                if admitted:
                    return await manager._original_matcher_dispatch(
                        Matcher, *args, **kwargs
                    )
            return None

        self._original_matcher_dispatch = message.check_and_run_matcher
        dispatch_with_owner.__zhenxun_runtime_owner__ = self
        message.check_and_run_matcher = dispatch_with_owner

        @wraps(original)
        async def run_with_owner(matcher, *args, **kwargs):
            with manager._matcher_admission(type(matcher)) as admitted:
                if admitted:
                    from zhenxun.services.message_execution import current_execution
                    from zhenxun.services.pipeline_metrics import pipeline_metrics

                    execution = current_execution.get()
                    if execution is not None:
                        execution.handlers_started += 1
                        if execution.received_at is not None:
                            pipeline_metrics.observe(
                                "handler_wait_ms", time.time() - execution.received_at
                            )
                    started = time.monotonic()
                    try:
                        return await original(matcher, *args, **kwargs)
                    finally:
                        pipeline_metrics.observe(
                            "handler_execution_ms", time.monotonic() - started
                        )
            return None

        run_with_owner.__zhenxun_runtime_owner__ = self
        Matcher.run = run_with_owner

    @contextlib.contextmanager
    def _entry_admission(self, owner, incarnation_id, *, business=True):
        if not owner:
            yield True
            return
        task = asyncio.current_task()
        key = (owner, incarnation_id, task)
        nested = key in self._entry_calls
        if not nested and business:
            from zhenxun.services.ai.chat_switch import chat_plugin_enabled

            if not chat_plugin_enabled(owner):
                self._entry_diagnostics["ai_chat_plugin_disabled"] += 1
                yield False
                return
        root = self._root_owner(owner) or owner
        unit = self.units.get(root)
        if not nested and (
            (incarnation_id and not self._lease_is_current(owner, incarnation_id))
            or (unit and unit.draining)
        ):
            self._entry_diagnostics["plugin_entry_not_admitted"] += 1
            yield False
            return
        self._entry_calls[key] = self._entry_calls.get(key, 0) + 1
        release = self._retain_activity(owner) if not nested else lambda: None
        tasks = self._entry_tasks[owner]
        tasks[task] = tasks.get(task, 0) + 1
        try:
            with owner_context(owner):
                yield True
        finally:
            self._entry_calls[key] -= 1
            if not self._entry_calls[key]:
                del self._entry_calls[key]
            tasks[task] -= 1
            if not tasks[task]:
                del tasks[task]
            if not tasks:
                self._entry_tasks.pop(owner, None)
            release()

    def _retain_activity(self, owner):
        unit = self.units.get(self._root_owner(owner) or owner)
        if unit:
            unit.in_flight += 1
            self._drained_event(unit.plugin_id).clear()
        released = False

        def release():
            nonlocal released
            if released:
                return
            released = True
            if unit:
                unit.in_flight -= 1
                if not unit.in_flight:
                    self._drained_event(unit.plugin_id).set()

        return release

    @contextlib.contextmanager
    def _matcher_admission(self, matcher):
        execution = self._matcher_execution.get()
        task = asyncio.current_task()
        if execution is not None and execution[:2] == (matcher, task):
            yield True
            return
        source = getattr(matcher, "_source", None)
        owner = getattr(matcher, "__zhenxun_registration_owner__", None) or getattr(
            source, "plugin_id", None
        )
        incarnation_id = getattr(matcher, "__zhenxun_incarnation_id__", None)
        root = self._root_owner(owner) or owner if owner else None
        unit = self.units.get(root or "")
        if owner:
            from zhenxun.services.startup_load import startup_load_planner

            startup_owner = (
                startup_load_planner.owner_for_module(
                    getattr(source, "module_name", "") or ""
                )
                or str(owner).split(":", 1)[0]
            )
            if (
                (incarnation_id and not self._lease_is_current(owner, incarnation_id))
                or startup_owner in startup_load_planner.failed_plugins
                or startup_owner in startup_load_planner.warming_plugins
                or (unit and unit.draining)
            ):
                self._entry_diagnostics["plugin_entry_not_admitted"] += 1
                yield False
                return
        if incarnation_id is None:
            self._entry_diagnostics["plugin_entry_ownership_unobserved"] += 1
        token = self._matcher_execution.set((matcher, task, owner, incarnation_id))
        try:
            with self._entry_admission(owner, incarnation_id) as admitted:
                yield admitted
        finally:
            self._matcher_execution.reset(token)

    def _install_matcher_registration_tracking(self) -> None:
        from nonebot.matcher import Matcher

        if self._original_matcher_new:
            return
        original = Matcher.new.__func__
        self._original_matcher_new = original
        manager = self

        def tracked_new(cls, *args, **kwargs):
            execution = manager._matcher_execution.get()
            task = None
            with contextlib.suppress(RuntimeError):
                task = asyncio.current_task()
            if execution and execution[1] is task:
                owner, inherited_id = execution[2:]
            else:
                owner, inherited_id = current_owner(), None
            if owner:
                incarnation = manager._ensure_incarnation(owner)
                if incarnation.lease_state in {
                    LeaseState.REVOKED,
                    LeaseState.FAILED,
                } or (inherited_id and incarnation.incarnation_id != inherited_id):
                    raise RuntimeError("plugin_incarnation_revoked")
            else:
                incarnation = None
            with provider_capture(
                manager._root_owner(owner) or owner if owner else "unowned",
                incarnation.incarnation_id if incarnation else None,
            ):
                matcher = original(cls, *args, **kwargs)
            if incarnation is not None:
                setattr(matcher, "__zhenxun_registration_owner__", owner)
                setattr(
                    matcher,
                    "__zhenxun_incarnation_id__",
                    incarnation.incarnation_id,
                )
            return matcher

        tracked_new.__zhenxun_runtime_wrapped__ = True
        tracked_new.__zhenxun_runtime_owner__ = self
        Matcher.new = classmethod(tracked_new)

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

        tracked_get_config.__zhenxun_runtime_owner__ = self
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

        tracked_add_plugin_config.__zhenxun_runtime_owner__ = self
        Config.add_plugin_config = MethodType(tracked_add_plugin_config, Config)

        original_plugin_config = nonebot.get_plugin_config
        self._original_get_plugin_config = original_plugin_config

        def tracked_get_plugin_config(config_model):
            fields = getattr(config_model, "model_fields", None) or getattr(
                config_model, "__fields__", {}
            )
            for name, field in fields.items():
                manager.track_env_access(str(name))
                alias = getattr(field, "alias", None)
                if alias:
                    manager.track_env_access(str(alias))
            return original_plugin_config(config_model)

        tracked_get_plugin_config.__zhenxun_runtime_owner__ = self
        nonebot.get_plugin_config = tracked_get_plugin_config

        original_getenv = os.getenv
        self._original_os_getenv = original_getenv

        def tracked_getenv(key, default=None):
            if isinstance(key, str):
                manager.track_env_access(key)
            return original_getenv(key, default)

        tracked_getenv.__zhenxun_runtime_owner__ = self
        os.getenv = tracked_getenv

    def _install_priority_lifecycle_tracking(self) -> None:
        from zhenxun.utils.enum import PriorityLifecycleType
        from zhenxun.utils.manager.priority_manager import PriorityLifecycle

        if self._original_priority_add:
            return
        original = PriorityLifecycle.add.__func__
        self._original_priority_add = original
        manager = self

        def tracked_add(cls, hook_type, func, priority, **kwargs):
            owner = current_owner() or manager.owner_for_module(
                getattr(func, "__module__", "")
            )
            wrapped = func
            incarnation = manager._ensure_incarnation(owner) if owner else None
            if owner and async_callable(func):

                @wraps(func)
                async def async_hook(*args, **kwargs):
                    if (
                        hook_type is not PriorityLifecycleType.SHUTDOWN
                        and incarnation
                        and not manager._hook_lease_is_current(
                            owner, incarnation.incarnation_id, "on_startup"
                        )
                    ):
                        raise RuntimeError("plugin_initialization_not_admitted")
                    manager._install_task_factory()
                    runtime_owner = manager._root_owner(owner) or owner
                    unit = manager.units.get(runtime_owner)
                    if unit:
                        unit.in_flight += 1
                        manager._drained_event(unit.plugin_id).clear()
                    try:
                        with (
                            owner_context(owner),
                            lifecycle_work_context(
                                owner,
                                incarnation.incarnation_id,
                                "on_shutdown"
                                if hook_type is PriorityLifecycleType.SHUTDOWN
                                else "on_startup",
                            ),
                        ):
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
                    if (
                        hook_type is not PriorityLifecycleType.SHUTDOWN
                        and incarnation
                        and not manager._hook_lease_is_current(
                            owner, incarnation.incarnation_id, "on_startup"
                        )
                    ):
                        raise RuntimeError("plugin_initialization_not_admitted")
                    manager._install_task_factory()
                    with (
                        owner_context(owner),
                        lifecycle_work_context(
                            owner,
                            incarnation.incarnation_id,
                            "on_shutdown"
                            if hook_type is PriorityLifecycleType.SHUTDOWN
                            else "on_startup",
                        ),
                    ):
                        return func(*args, **kwargs)

                wrapped = sync_hook
            return original(cls, hook_type, wrapped, priority, **kwargs)

        tracked_add.__zhenxun_runtime_owner__ = self
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

        async def tracked_install(cls, module_path: str, **kwargs):
            manager._install_task_factory()
            owner = manager.owner_for_module(module_path) or module_path
            incarnation = manager._ensure_incarnation(owner)
            with (
                owner_context(owner),
                lifecycle_work_context(owner, incarnation.incarnation_id, "on_startup"),
            ):
                return await original_install(cls, module_path, **kwargs)

        async def tracked_remove(cls, module_path: str, **kwargs):
            manager._install_task_factory()
            owner = manager.owner_for_module(module_path) or module_path
            incarnation = manager._ensure_incarnation(owner)
            with (
                owner_context(owner),
                lifecycle_work_context(
                    owner, incarnation.incarnation_id, "on_shutdown"
                ),
            ):
                return await original_remove(cls, module_path, **kwargs)

        async def tracked_install_all(cls):
            for module_path in cls.snapshot_modules():
                await cls.install(module_path)

        tracked_install.__zhenxun_runtime_owner__ = self
        tracked_remove.__zhenxun_runtime_owner__ = self
        tracked_install_all.__zhenxun_runtime_owner__ = self
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
            owner = current_owner()
            incarnation = manager._ensure_incarnation(owner) if owner else None
            with provider_capture(
                manager._root_owner(owner) or owner if owner else "unowned",
                incarnation.incarnation_id if incarnation else None,
            ):
                return original(cls, prefix, value)

        tracked_add_prefix.__zhenxun_runtime_owner__ = self
        TrieRule.add_prefix = classmethod(tracked_add_prefix)

    def _install_scheduler_tracking(self) -> None:
        scheduler_module = sys.modules.get("nonebot_plugin_apscheduler")
        scheduler = getattr(scheduler_module, "scheduler", None)
        if scheduler is None:
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
            incarnation = manager._ensure_incarnation(owner) if owner else None
            if owner:
                if async_callable(func):

                    @wraps(func)
                    async def async_job(*job_args, **job_kwargs):
                        if incarnation and not manager._lease_is_current(
                            owner, incarnation.incarnation_id
                        ):
                            return None
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
                        if incarnation and not manager._lease_is_current(
                            owner, incarnation.incarnation_id
                        ):
                            return None
                        with owner_context(manager._root_owner(owner) or owner):
                            return func(*job_args, **job_kwargs)

                    wrapped = sync_job
            job = original(wrapped, *args, **kwargs)
            if owner and getattr(job, "id", None):
                manager._job_owners[job.id] = owner
            return job

        tracked_add_job.__zhenxun_runtime_owner__ = self
        scheduler.add_job = MethodType(tracked_add_job, scheduler)

    def _install_processor_tracking(self) -> None:
        import nonebot.message as message

        from .entries import manage_registration

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

            def make_decorator(decorator, registry_name):
                @wraps(decorator)
                def tracked(func):
                    owner = current_owner()
                    if not owner:
                        return decorator(func)
                    incarnation = manager._ensure_incarnation(owner)
                    if async_callable(func):

                        @typed_wraps(func)
                        async def wrapped(*args, **kwargs):
                            return await func(*args, **kwargs)

                    else:

                        @typed_wraps(func)
                        def wrapped(*args, **kwargs):
                            return func(*args, **kwargs)

                    with provider_capture(
                        manager._root_owner(owner) or owner, incarnation.incarnation_id
                    ):
                        registry = getattr(message, registry_name)
                        before = {id(item) for item in registry}
                        result = decorator(wrapped)
                        manage_registration(
                            registry, before, manager, owner, incarnation.incarnation_id
                        )
                        return result

                tracked.__zhenxun_runtime_wrapped__ = True
                return tracked

            nonebot_original = getattr(nonebot, name, None)
            tracked_decorator = make_decorator(original, f"_{name}s")
            tracked_decorator.__zhenxun_runtime_owner__ = self
            self._original_processor_hooks[name] = (original, nonebot_original)
            setattr(message, name, tracked_decorator)
            if nonebot_original is original:
                setattr(nonebot, name, tracked_decorator)

    def _install_driver_hook_tracking(self, driver: Any) -> None:
        from .entries import manage_registration

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
            def tracked(func, _original=original, _hook_name=name):
                owner = current_owner()
                if not owner:
                    return _original(func)
                incarnation = manager._ensure_incarnation(owner)
                if async_callable(func):

                    @typed_wraps(func)
                    async def wrapped(*args, **kwargs):
                        if (
                            _hook_name != "on_shutdown"
                            and not manager._hook_lease_is_current(
                                owner, incarnation.incarnation_id, _hook_name
                            )
                        ):
                            if _hook_name in {"on_startup", "on_ready"}:
                                raise RuntimeError("plugin_initialization_not_admitted")
                            return None
                        root_owner = manager._root_owner(owner) or owner
                        try:
                            with (
                                owner_context(owner),
                                lifecycle_work_context(
                                    owner, incarnation.incarnation_id, _hook_name
                                ),
                            ):
                                return await func(*args, **kwargs)
                        except Exception as error:
                            if manager._isolate_plugin_hook_error(
                                root_owner,
                                _hook_name,
                                error,
                                bot=kwargs.get("bot") or (args[0] if args else None),
                            ):
                                return None
                            raise

                else:

                    @typed_wraps(func)
                    def wrapped(*args, **kwargs):
                        if (
                            _hook_name != "on_shutdown"
                            and not manager._hook_lease_is_current(
                                owner, incarnation.incarnation_id, _hook_name
                            )
                        ):
                            if _hook_name in {"on_startup", "on_ready"}:
                                raise RuntimeError("plugin_initialization_not_admitted")
                            return None
                        root_owner = manager._root_owner(owner) or owner
                        try:
                            with (
                                owner_context(owner),
                                lifecycle_work_context(
                                    owner, incarnation.incarnation_id, _hook_name
                                ),
                            ):
                                return func(*args, **kwargs)
                        except Exception as error:
                            if manager._isolate_plugin_hook_error(
                                root_owner,
                                _hook_name,
                                error,
                                bot=kwargs.get("bot") or (args[0] if args else None),
                            ):
                                return None
                            raise

                with provider_capture(
                    manager._root_owner(owner) or owner, incarnation.incarnation_id
                ):
                    registry = (
                        getattr(driver, "_bot_connection_hook")
                        if _hook_name == "on_bot_connect"
                        else getattr(driver, "_bot_disconnection_hook")
                        if _hook_name == "on_bot_disconnect"
                        else None
                    )
                    before = (
                        {id(item) for item in registry}
                        if registry is not None
                        else set()
                    )
                    result = _original(wrapped)
                    if registry is not None:
                        manage_registration(
                            registry,
                            before,
                            manager,
                            owner,
                            incarnation.incarnation_id,
                            kind=_hook_name,
                        )
                    return result

            tracked.__zhenxun_runtime_wrapped__ = True
            tracked.__zhenxun_runtime_owner__ = self
            self._original_driver_hooks[name] = original
            setattr(driver, name, tracked)

    def _isolate_plugin_hook_error(
        self, owner: str, hook_name: str, error: Exception, *, bot=None
    ) -> bool:
        record = {
            "plugin_id": owner,
            "phase": hook_name,
            "code": "plugin_hook_invocation_failed",
            "error_type": type(error).__name__,
            "occurred_at": datetime.now(timezone.utc).isoformat(),
        }
        if bot is not None and hasattr(bot, "self_id"):
            from zhenxun.services.message_admission import connection_epochs
            from zhenxun.utils.platform import PlatformUtils

            scope = PlatformUtils.get_platform_scope(bot)
            epoch = connection_epochs.get(bot, scope)
            record.update(
                {
                    "bot_id": str(bot.self_id),
                    "platform_scope": scope,
                    "connection_id": epoch.connection_id if epoch else None,
                }
            )
        self._hook_failures.append(record)
        if (
            hook_name not in {"on_startup", "on_ready"}
            or runtime_mutation_coordinator.reentrant
            or active_undo.get() is not None
            or owner in self._candidate_roots
        ):
            # NoneBot reports connection errors for that invocation; shutdown and
            # transactional initialization must propagate to their supervisors.
            return False
        try:
            from zhenxun.services.startup_load import startup_load_planner

            if startup_load_planner.is_core_plugin(owner):
                return False
            startup_load_planner.mark_failed(
                owner, f"plugin_lifecycle_failed:{type(error).__name__}"
            )
        except Exception:
            return False
        if unit := self.units.get(owner):
            unit.last_error = type(error).__name__
            unit.reasons.add("plugin_lifecycle_hook_failed")
            unit.classification = ReloadClassification.RESTART_REQUIRED
        startup_coordinator.record_error(
            "runtime",
            f"plugin_hook_failed:{hook_name}:{type(error).__name__}",
            source_type="plugin",
            source_id=owner,
            display_name=owner,
        )
        logger.error(
            f"插件生命周期 Hook 已隔离: {owner} {hook_name}",
            e=error,
        )
        return True

    def _install_bot_api_hook_tracking(self) -> None:
        try:
            from nonebot.internal.adapter import Bot
        except ImportError:
            return
        manager = self
        for name in ("on_calling_api", "on_called_api"):
            original = getattr(Bot, name, None)
            if not original or getattr(original, "__zhenxun_runtime_wrapped__", False):
                continue
            original_func = getattr(original, "__func__", original)
            self._original_bot_api_hooks[name] = original_func

            def tracked(cls, func, _original=original_func):
                owner = current_owner()
                if not owner:
                    return _original(cls, func)
                incarnation = manager._ensure_incarnation(owner)
                if async_callable(func):

                    @typed_wraps(func)
                    async def wrapped(*args, **kwargs):
                        if not manager._lease_is_current(
                            owner, incarnation.incarnation_id
                        ):
                            return None
                        with owner_context(manager._root_owner(owner) or owner):
                            return await func(*args, **kwargs)

                else:

                    @typed_wraps(func)
                    def wrapped(*args, **kwargs):
                        if not manager._lease_is_current(
                            owner, incarnation.incarnation_id
                        ):
                            return None
                        with owner_context(manager._root_owner(owner) or owner):
                            return func(*args, **kwargs)

                with provider_capture(
                    manager._root_owner(owner) or owner, incarnation.incarnation_id
                ):
                    return _original(cls, wrapped)

            tracked.__zhenxun_runtime_wrapped__ = True
            tracked.__zhenxun_runtime_owner__ = self
            setattr(Bot, name, classmethod(tracked))

    def _install_asgi_route_tracking(self) -> None:
        try:
            from fastapi.routing import APIRouter
        except ImportError:
            return
        manager = self
        for method_name in ("add_api_route", "add_api_websocket_route"):
            original = getattr(APIRouter, method_name, None)
            if not original or getattr(original, "__zhenxun_runtime_wrapped__", False):
                continue
            self._original_asgi_methods[method_name] = original

            @wraps(original)
            def tracked(router, *args, _original=original, **kwargs):
                owner = current_owner()
                endpoint = args[1] if len(args) > 1 else kwargs.get("endpoint")
                if owner and callable(endpoint):
                    incarnation = manager._ensure_incarnation(owner)
                    endpoint_owner = getattr(
                        endpoint, "__zhenxun_runtime_route_owner__", None
                    )
                    endpoint_incarnation = getattr(
                        endpoint, "__zhenxun_runtime_incarnation_id__", None
                    )
                    if endpoint_owner == owner and endpoint_incarnation == (
                        incarnation.incarnation_id
                    ):
                        # include_router() registers the endpoint again. Reuse the
                        # existing lease proxy instead of nesting another proxy.
                        pass
                    elif async_callable(endpoint):

                        @typed_wraps(endpoint, asgi=True)
                        async def wrapped_endpoint(
                            *call_args, _endpoint=endpoint, **call_kwargs
                        ):
                            root_owner = manager._root_owner(owner) or owner
                            if not manager._lease_is_current(
                                owner, incarnation.incarnation_id
                            ):
                                raise RuntimeError("plugin_incarnation_revoked")
                            unit = manager.units.get(root_owner)
                            if unit and unit.draining:
                                raise RuntimeError("plugin_scope_quiescing")
                            if unit:
                                unit.in_flight += 1
                                manager._drained_event(root_owner).clear()
                            try:
                                with owner_context(root_owner):
                                    return await _endpoint(*call_args, **call_kwargs)
                            finally:
                                if unit:
                                    unit.in_flight = max(0, unit.in_flight - 1)
                                    if not unit.in_flight:
                                        manager._drained_event(root_owner).set()

                        wrapped_endpoint.__zhenxun_runtime_route_owner__ = owner
                        wrapped_endpoint.__zhenxun_runtime_incarnation_id__ = (
                            incarnation.incarnation_id
                        )
                        wrapped_endpoint.__zhenxun_runtime_original_endpoint__ = (
                            endpoint
                        )
                        endpoint = wrapped_endpoint
                    else:
                        manager._unsafe_route_owners.add(owner)
                    if len(args) > 1:
                        args = (args[0], endpoint, *args[2:])
                    else:
                        kwargs["endpoint"] = endpoint
                before = len(router.routes)
                with provider_capture(
                    manager._root_owner(owner) or owner if owner else "unowned",
                    incarnation.incarnation_id if owner else None,
                ):
                    result = _original(router, *args, **kwargs)
                if owner:
                    for route in router.routes[before:]:
                        manager._asgi_route_owners[id(route)] = owner
                return result

            tracked.__zhenxun_runtime_wrapped__ = True
            tracked.__zhenxun_runtime_owner__ = self
            setattr(APIRouter, method_name, tracked)

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
            if name == "nonebot_plugin_apscheduler":
                manager._install_scheduler_tracking()
            dependency_plugin = getattr(module, "__plugin__", None)
            if owner and dependency_plugin:
                manager._pending_dependencies[owner].add(dependency_plugin.id_)
            return module

        tracked_require.__zhenxun_runtime_wrapped__ = True
        tracked_require.__zhenxun_runtime_owner__ = self
        for target in (plugin_load, plugin_module, nonebot):
            self._original_require_hooks[(target, "require")] = getattr(
                target, "require"
            )
            target.require = tracked_require

    def _install_thread_process_tracking(self) -> None:
        from concurrent.futures import ThreadPoolExecutor
        from concurrent.futures.thread import _worker

        from anyio._backends._asyncio import AsyncIOBackend, WorkerThread

        manager = self
        if self._original_executor_init is None:
            original_executor_init = ThreadPoolExecutor.__init__
            self._original_executor_init = original_executor_init

            @wraps(original_executor_init)
            def tracked_executor_init(executor, *args, **kwargs):
                original_executor_init(executor, *args, **kwargs)
                if owner := current_owner():
                    manager._executor_owners[executor] = owner

            ThreadPoolExecutor.__init__ = tracked_executor_init

        if self._original_anyio_worker is None:
            original_worker = AsyncIOBackend.run_sync_in_worker_thread
            self._original_anyio_worker = AsyncIOBackend.__dict__[
                "run_sync_in_worker_thread"
            ]
            manager = self

            async def tracked_worker(cls, func, args, *options, **kwargs):
                owner = current_owner()
                if not owner:
                    return await original_worker(func, args, *options, **kwargs)
                incarnation = manager._ensure_incarnation(owner)
                phase = manager._current_work(owner, incarnation.incarnation_id)
                completion = ConcurrentFuture()
                owned = manager._owned_executor_futures[owner]
                owned.add(completion)
                loop = asyncio.get_running_loop()

                def execute():
                    if not completion.set_running_or_notify_cancel():
                        return None
                    try:
                        check_budget()
                        if not manager._work_lease_is_current(
                            owner, incarnation.incarnation_id, phase
                        ):
                            return None
                        with resource_context(owner):
                            return func(*args)
                    finally:
                        completion.set_result(None)

                def completed(_):
                    with contextlib.suppress(RuntimeError):
                        loop.call_soon_threadsafe(owned.discard, completion)

                completion.add_done_callback(completed)
                try:
                    return await original_worker(execute, (), *options, **kwargs)
                finally:
                    # Cancels queued work only; running work remains owned until exit.
                    completion.cancel()

            self._wrapped_anyio_worker = classmethod(tracked_worker)
            AsyncIOBackend.run_sync_in_worker_thread = self._wrapped_anyio_worker
        if not self._original_thread_start:
            original_start = threading.Thread.start
            self._original_thread_start = original_start
            manager = self

            @wraps(original_start)
            def tracked_start(thread, *args, **kwargs):
                owner = current_owner()
                shared_worker = isinstance(thread, WorkerThread)
                if getattr(thread, "_target", None) is _worker:
                    pool_args = getattr(thread, "_args", ())
                    pool_ref = pool_args[0] if pool_args else None
                    pool = pool_ref() if callable(pool_ref) else None
                    shared_worker = shared_executor_submission.get() or (
                        pool is not None and pool in manager._shared_executors
                    )
                    if shared_worker and pool is not None:
                        manager._shared_executors.add(pool)
                    elif pool in manager._executor_owners:
                        owner = manager._executor_owners[pool]
                    else:
                        manager._entry_diagnostics["executor_ownership_unobserved"] += 1
                        return Context().run(original_start, thread, *args, **kwargs)
                if shared_worker:
                    from zhenxun.services.shared_workers import register_shared_worker

                    register_shared_worker(thread)
                    # Ownership belongs to each submission, not the pool's first caller.
                    return Context().run(original_start, thread, *args, **kwargs)
                incarnation = (
                    manager._incarnations.get(manager._root_owner(owner) or owner)
                    if owner
                    else None
                )
                if incarnation and incarnation.lease_state in {
                    LeaseState.REVOKED,
                    LeaseState.FAILED,
                }:
                    raise RuntimeError("plugin_incarnation_revoked")
                if owner and not getattr(
                    thread, "__zhenxun_runtime_owner_wrapped__", False
                ):
                    original_run = thread.run

                    @wraps(original_run)
                    def run_with_owner(*run_args, **run_kwargs):
                        with resource_context(owner):
                            return original_run(*run_args, **run_kwargs)

                    thread.run = run_with_owner
                    thread.__zhenxun_runtime_owner_wrapped__ = True
                result = original_start(thread, *args, **kwargs)
                if owner:
                    with manager._ownership_lock:
                        manager._owned_threads[owner].add(thread)
                return result

            tracked_start.__zhenxun_runtime_owner__ = self
            threading.Thread.start = tracked_start

        if not self._original_popen_init:
            original_init = subprocess.Popen.__init__
            self._original_popen_init = original_init
            manager = self

            @wraps(original_init)
            def tracked_init(process, *args, **kwargs):
                owner = current_owner()
                incarnation = (
                    manager._incarnations.get(manager._root_owner(owner) or owner)
                    if owner
                    else None
                )
                if incarnation and incarnation.lease_state in {
                    LeaseState.REVOKED,
                    LeaseState.FAILED,
                }:
                    raise RuntimeError("plugin_incarnation_revoked")
                original_init(process, *args, **kwargs)
                if owner:
                    with manager._ownership_lock:
                        manager._owned_processes[owner].add(process)

            tracked_init.__zhenxun_runtime_owner__ = self
            subprocess.Popen.__init__ = tracked_init

    def _restore_thread_process_tracking(self) -> None:
        from concurrent.futures import ThreadPoolExecutor

        if self._original_executor_init is not None:
            ThreadPoolExecutor.__init__ = self._original_executor_init
            self._original_executor_init = None
        if self._original_anyio_worker is not None:
            from anyio._backends._asyncio import AsyncIOBackend

            if (
                AsyncIOBackend.__dict__["run_sync_in_worker_thread"]
                is self._wrapped_anyio_worker
            ):
                AsyncIOBackend.run_sync_in_worker_thread = self._original_anyio_worker
            self._original_anyio_worker = None
            self._wrapped_anyio_worker = None
        current_start = threading.Thread.start
        if (
            getattr(current_start, "__zhenxun_runtime_owner__", None) is self
            and self._original_thread_start is not None
        ):
            threading.Thread.start = self._original_thread_start
        current_popen_init = subprocess.Popen.__init__
        if (
            getattr(current_popen_init, "__zhenxun_runtime_owner__", None) is self
            and self._original_popen_init is not None
        ):
            subprocess.Popen.__init__ = self._original_popen_init
        self._original_thread_start = None
        self._original_popen_init = None

    def _restore_global_hooks(self) -> None:
        from fastapi.routing import APIRouter
        from nonebot.internal.adapter import Bot
        from nonebot.matcher import Matcher
        import nonebot.message as message
        from nonebot.rule import TrieRule

        from zhenxun.configs.config import Config
        from zhenxun.services.plugin_init import PluginInitManager
        from zhenxun.utils.manager.priority_manager import PriorityLifecycle

        def owned(value: Any) -> bool:
            func = getattr(value, "__func__", value)
            return getattr(func, "__zhenxun_runtime_owner__", None) is self

        if self._original_matcher_run is not None and owned(Matcher.run):
            Matcher.run = self._original_matcher_run
        if self._original_matcher_dispatch is not None and owned(
            message.check_and_run_matcher
        ):
            message.check_and_run_matcher = self._original_matcher_dispatch
        if self._original_matcher_new is not None and owned(Matcher.new):
            Matcher.new = classmethod(self._original_matcher_new)
        if self._original_get_config is not None and owned(Config.get_config):
            Config.get_config = self._original_get_config
        if self._original_add_plugin_config is not None and owned(
            Config.add_plugin_config
        ):
            Config.add_plugin_config = self._original_add_plugin_config
        if self._original_get_plugin_config is not None and owned(
            nonebot.get_plugin_config
        ):
            nonebot.get_plugin_config = self._original_get_plugin_config
        if self._original_os_getenv is not None and owned(os.getenv):
            os.getenv = self._original_os_getenv
        if self._original_priority_add is not None and owned(PriorityLifecycle.add):
            PriorityLifecycle.add = classmethod(self._original_priority_add)
        if self._original_plugin_init_install is not None and owned(
            PluginInitManager.install
        ):
            PluginInitManager.install = classmethod(self._original_plugin_init_install)
        if self._original_plugin_init_remove is not None and owned(
            PluginInitManager.remove
        ):
            PluginInitManager.remove = classmethod(self._original_plugin_init_remove)
        if self._original_plugin_init_install_all is not None and owned(
            PluginInitManager.install_all
        ):
            PluginInitManager.install_all = classmethod(
                self._original_plugin_init_install_all
            )
        if self._original_trie_add_prefix is not None and owned(TrieRule.add_prefix):
            TrieRule.add_prefix = classmethod(self._original_trie_add_prefix)

        scheduler_module = sys.modules.get("nonebot_plugin_apscheduler")
        scheduler = getattr(scheduler_module, "scheduler", None)
        if (
            scheduler is not None
            and self._original_scheduler_add_job is not None
            and owned(scheduler.add_job)
        ):
            scheduler.add_job = self._original_scheduler_add_job
        for name, (
            original,
            nonebot_original,
        ) in self._original_processor_hooks.items():
            if owned(getattr(message, name, None)):
                setattr(message, name, original)
            if owned(getattr(nonebot, name, None)):
                setattr(nonebot, name, nonebot_original)
        driver = None
        with contextlib.suppress(Exception):
            driver = nonebot.get_driver()
        if driver is not None:
            for name, original in self._original_driver_hooks.items():
                if owned(getattr(driver, name, None)):
                    setattr(driver, name, original)
        for name, original in self._original_bot_api_hooks.items():
            if owned(getattr(Bot, name, None)):
                setattr(Bot, name, classmethod(original))
        for name, original in self._original_asgi_methods.items():
            if owned(getattr(APIRouter, name, None)):
                setattr(APIRouter, name, original)
        for (target, name), original in self._original_require_hooks.items():
            if owned(getattr(target, name, None)):
                setattr(target, name, original)

        self._original_matcher_run = None
        self._original_matcher_dispatch = None
        self._original_matcher_new = None
        self._original_get_config = None
        self._original_add_plugin_config = None
        self._original_get_plugin_config = None
        self._original_os_getenv = None
        self._original_priority_add = None
        self._original_plugin_init_install = None
        self._original_plugin_init_remove = None
        self._original_plugin_init_install_all = None
        self._original_trie_add_prefix = None
        self._original_scheduler_add_job = None
        self._original_processor_hooks.clear()
        self._original_driver_hooks.clear()
        self._original_bot_api_hooks.clear()
        self._original_asgi_methods.clear()
        self._original_require_hooks.clear()

    def _collect_runtime_boundaries(self) -> None:
        for _ in self._iter_runtime_boundaries():
            pass

    def _iter_runtime_boundaries(self):
        from zhenxun.services.plugin_init import PluginInitManager

        units = [(unit, unit.incarnation_id) for unit in self.units.values()]
        identities = {
            unit.plugin_id: (unit, incarnation) for unit, incarnation in units
        }
        for owner in set(self._shared_dependency_evidence) - self.units.keys():
            self._shared_dependency_evidence.pop(owner, None)

        def current(unit, incarnation):
            return (
                self.units.get(unit.plugin_id) is unit
                and unit.incarnation_id == incarnation
            )

        with PluginInitManager._registry_lock:
            plugin_init_modules = set(PluginInitManager.plugins)
        for unit, incarnation in units:
            yield
            if not current(unit, incarnation):
                continue
            if any(
                module in unit.module_names
                or any(module.startswith(f"{name}.") for name in unit.module_names)
                for module in plugin_init_modules
            ):
                unit.reasons.add("legacy_lifecycle_not_transactional")
                unit.classification = ReloadClassification.RESTART_REQUIRED

        shared_owners = yield from self._iter_shared_dependency_owners()
        for unit, incarnation in units:
            yield
            if not current(unit, incarnation):
                continue
            self._shared_dependency_evidence.pop(unit.plugin_id, None)
            shared_calls = sorted(
                call
                for call in unit.import_time_dependency_calls
                if call.split(".", 1)[0] in shared_owners
            )
            if not shared_calls:
                continue
            unit.reasons.add("shared_dependency_global_mutation")
            unit.classification = ReloadClassification.RESTART_REQUIRED
            self._shared_dependency_evidence[unit.plugin_id] = shared_calls[:50]

        with self._ownership_lock:
            owned_threads = {
                owner: set(threads) for owner, threads in self._owned_threads.items()
            }
            owned_processes = {
                owner: set(processes)
                for owner, processes in self._owned_processes.items()
            }
            self._owned_threads = defaultdict(
                set,
                {
                    owner: {thread for thread in threads if thread.is_alive()}
                    for owner, threads in self._owned_threads.items()
                    if any(thread.is_alive() for thread in threads)
                },
            )
            self._owned_processes = defaultdict(
                set,
                {
                    owner: {process for process in processes if process.poll() is None}
                    for owner, processes in self._owned_processes.items()
                    if any(process.poll() is None for process in processes)
                },
            )
        for owner, threads in owned_threads.items():
            yield
            root = self._root_owner(owner)
            if root and (unit := self.units.get(root)):
                identity = identities.get(root)
                if identity is None or not current(*identity):
                    continue
                if any(thread.is_alive() for thread in threads):
                    unit.reasons.add("live_thread")
                    unit.classification = ReloadClassification.RESTART_REQUIRED
        for owner, processes in owned_processes.items():
            yield
            root = self._root_owner(owner)
            if root and (unit := self.units.get(root)):
                identity = identities.get(root)
                if identity is None or not current(*identity):
                    continue
                if any(process.poll() is None for process in processes):
                    unit.reasons.add("live_process")
                    unit.classification = ReloadClassification.RESTART_REQUIRED
        for owner, futures in list(self._owned_executor_futures.items()):
            yield
            root = self._root_owner(owner)
            if root and (unit := self.units.get(root)):
                identity = identities.get(root)
                if identity is None or not current(*identity):
                    continue
                if any(not future.done() for future in futures):
                    unit.reasons.add("executor_work_in_flight")
                    unit.classification = ReloadClassification.RESTART_REQUIRED
        try:
            routes = list(nonebot.get_app().routes)
        except (AssertionError, AttributeError, ValueError):
            routes = []
        for route in routes:
            yield
            endpoint = getattr(route, "endpoint", None)
            recorded_owner = self._asgi_route_owners.get(id(route))
            owner = self._root_owner(recorded_owner or "") or self.owner_for_module(
                getattr(endpoint, "__module__", "")
            )
            if owner and (unit := self.units.get(owner)):
                identity = identities.get(owner)
                if identity is None or not current(*identity):
                    continue
                if any(
                    self._root_owner(candidate) == owner or candidate == owner
                    for candidate in self._unsafe_route_owners
                ):
                    unit.reasons.add("sync_route_not_drainable")
                    unit.classification = ReloadClassification.RESTART_REQUIRED

    def _shared_dependency_owners(self) -> dict[str, set[str]]:
        scan = self._iter_shared_dependency_owners()
        while True:
            try:
                next(scan)
            except StopIteration as result:
                return result.value

    def _iter_shared_dependency_owners(self):
        excluded_roots = {
            "fastapi",
            "nonebot",
            "nonebot_plugin_alconna",
            "nonebot_plugin_apscheduler",
            "pydantic",
            "starlette",
            "zhenxun",
        }
        stdlib = getattr(sys, "stdlib_module_names", set())
        owners: dict[str, set[str]] = defaultdict(set)
        units = [(unit, unit.incarnation_id) for unit in self.units.values()]
        for unit, incarnation in units:
            yield
            if (
                self.units.get(unit.plugin_id) is not unit
                or unit.incarnation_id != incarnation
            ):
                continue
            for module_name in unit.imported_modules:
                root = module_name.split(".", 1)[0]
                if (
                    not root
                    or root in excluded_roots
                    or root in stdlib
                    or self.owner_for_module(root) is not None
                ):
                    continue
                owners[root].add(unit.plugin_id)
        return {
            module_name: plugin_ids
            for module_name, plugin_ids in owners.items()
            if len(plugin_ids) > 1
        }

    @staticmethod
    def _trackable_shared_global(value: Any) -> bool:
        if value is None or isinstance(value, bool | int | float | str | bytes):
            return True
        if isinstance(value, tuple | frozenset):
            return all(
                PluginRuntimeManager._trackable_shared_global(item) for item in value
            )
        return not (
            isinstance(value, ModuleType | dict | list | set)
            or inspect.isclass(value)
            or callable(value)
        )

    def _capture_shared_globals(
        self, affected: set[str]
    ) -> tuple[dict[str, dict[str, Any]], dict[str, set[str]]]:
        shared_owners = {
            module_name: owners
            for module_name, owners in self._shared_dependency_owners().items()
            if owners & affected
        }
        globals_snapshot: dict[str, dict[str, Any]] = {}
        for root in shared_owners:
            for module_name, module in list(sys.modules.items()):
                if module is None or not (
                    module_name == root or module_name.startswith(f"{root}.")
                ):
                    continue
                values = {
                    name: value
                    for name, value in vars(module).items()
                    if not name.startswith("__")
                    and self._trackable_shared_global(value)
                }
                if values:
                    globals_snapshot[module_name] = values
        return globals_snapshot, shared_owners

    @staticmethod
    def _shared_value_changed(previous: Any, current: Any) -> bool:
        if previous is None or isinstance(previous, bool | int | float | str | bytes):
            try:
                return type(previous) is not type(current) or previous != current
            except Exception:
                return previous is not current
        if isinstance(previous, tuple | frozenset):
            try:
                return type(previous) is not type(current) or previous != current
            except Exception:
                return previous is not current
        return previous is not current

    def _shared_global_mutations(
        self, checkpoint: PluginReloadCheckpoint
    ) -> dict[str, list[str]]:
        mutations: dict[str, list[str]] = defaultdict(list)
        for module_name, values in checkpoint.shared_globals.items():
            module = sys.modules.get(module_name)
            if module is None:
                mutations[module_name.split(".", 1)[0]].append(
                    f"{module_name}:module_removed"
                )
                continue
            for name, previous in values.items():
                current = vars(module).get(name, object())
                if self._shared_value_changed(previous, current):
                    mutations[module_name.split(".", 1)[0]].append(
                        f"{module_name}:{name}"
                    )
        return {root: sorted(values)[:50] for root, values in mutations.items()}

    @staticmethod
    def _restore_shared_globals(checkpoint: PluginReloadCheckpoint) -> None:
        for module_name, values in checkpoint.shared_globals.items():
            module = sys.modules.get(module_name)
            if module is None:
                continue
            for name, value in values.items():
                setattr(module, name, value)

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

    def _new_incarnation(self, owner: str) -> PluginIncarnation:
        previous = self._incarnations.get(owner)
        if previous and previous.lease_state in {
            LeaseState.PREPARED,
            LeaseState.ACTIVE,
        }:
            previous.lease_state = LeaseState.REVOKED
        incarnation = PluginIncarnation(owner, startup_coordinator.boot_id)
        self._incarnations[owner] = incarnation
        self._incarnation_history.append(incarnation)
        self._incarnation_history = self._incarnation_history[-500:]
        return incarnation

    def _ensure_incarnation(self, owner: str) -> PluginIncarnation:
        incarnation = self._incarnations.get(owner)
        if incarnation is None:
            incarnation = self._new_incarnation(owner)
        return incarnation

    def _prepare_candidate(self, owner: str) -> PluginIncarnation:
        # Checkpoints retain the old objects; new imports must not inherit their
        # revoked aliases, including child plugin IDs and full module paths.
        root = self._root_owner(owner) or owner
        self._candidate_roots.add(root)
        for alias in list(self._incarnations):
            if (
                alias == owner
                or self._root_owner(alias) == root
                or alias.startswith(f"{owner}:")
                or alias.startswith(f"{owner}.")
            ):
                self._incarnations.pop(alias)
        return self._new_incarnation(owner)

    def _publish_candidates(self, roots: set[str]) -> None:
        self._check_initialization_work(roots)
        for alias, incarnation in self._incarnations.items():
            if (self._root_owner(alias) or alias) in roots:
                if incarnation.lease_state is LeaseState.PREPARED:
                    incarnation.lease_state = LeaseState.ACTIVE
        self._candidate_roots.difference_update(roots)
        for root in roots:
            if unit := self.units.get(root):
                self._observe_plugin_scope(unit)
        self._finish_initialization_work(roots)
        from zhenxun.services.startup_load import startup_load_planner

        startup_load_planner.commit_runtime_recovery(
            {
                module
                for root in roots
                if (unit := self.units.get(root))
                for module in unit.module_names
            },
            self.generation,
            plugin_ids=roots,
        )

    def _check_initialization_work(self, roots):
        for owner, works in self._initialization_work.items():
            if (self._root_owner(owner) or owner) in roots:
                for work in works:
                    if work.expired or work.budget.remaining() <= 0:
                        raise TimeoutError("plugin_initialization_budget_exhausted")

    def _finish_initialization_work(self, roots, *, cancel=False):
        for owner in list(self._initialization_work):
            if (self._root_owner(owner) or owner) not in roots and not any(
                owner == root or owner.startswith((f"{root}:", f"{root}."))
                for root in roots
            ):
                continue
            for work in self._initialization_work.pop(owner):
                work.active = False
                if work.deadline_handle is not None:
                    work.deadline_handle.cancel()
                if cancel:
                    for task in list(work.children):
                        if not task.done():
                            task.cancel()

    def _incarnation_for_unit(self, unit: PluginUnit) -> PluginIncarnation | None:
        if incarnation := self._incarnations.get(unit.plugin_id):
            return incarnation
        for module_name in unit.module_names:
            if incarnation := self._incarnations.get(module_name):
                return incarnation
        return None

    def _lease_is_current(self, owner: str, incarnation_id: str) -> bool:
        root = self._root_owner(owner) or owner
        incarnation = self._incarnations.get(owner) or self._incarnations.get(root)
        return bool(
            incarnation
            and incarnation.incarnation_id == incarnation_id
            and incarnation.accepts_work
            and root not in self._candidate_roots
        )

    def _work_lease_is_current(
        self, owner: str, incarnation_id: str, phase: LifecycleWork | None
    ) -> bool:
        if phase is not None and phase.phase == "on_shutdown" and not phase.valid():
            return False
        root = self._root_owner(owner) or owner
        incarnation = self._incarnations.get(owner) or self._incarnations.get(root)
        return bool(
            incarnation
            and incarnation.incarnation_id == incarnation_id
            and (
                (incarnation.accepts_work and root not in self._candidate_roots)
                or (
                    phase is not None
                    and (phase.owner, phase.incarnation_id) == (owner, incarnation_id)
                    and phase.valid()
                    and (
                        phase.phase == "on_shutdown"
                        or (
                            incarnation.lease_state
                            in {LeaseState.PREPARED, LeaseState.ACTIVE}
                            and phase.phase in {"on_startup", "on_ready"}
                        )
                    )
                )
            )
        )

    def _hook_lease_is_current(
        self, owner: str, incarnation_id: str, hook_name: str
    ) -> bool:
        root = self._root_owner(owner) or owner
        incarnation = self._incarnations.get(owner) or self._incarnations.get(root)
        if not incarnation or incarnation.incarnation_id != incarnation_id:
            return False
        # Startup initializes resources before business entry points are admitted.
        return incarnation.accepts_work or (
            hook_name in {"on_startup", "on_ready"}
            and incarnation.lease_state is LeaseState.PREPARED
        )

    def _revoke_incarnation(self, owner: str, *, failed: bool = False) -> None:
        root = self._root_owner(owner) or owner
        self._finish_initialization_work({root}, cancel=failed)
        incarnations = {
            id(incarnation): incarnation
            for alias, incarnation in self._incarnations.items()
            if alias == root or self._root_owner(alias) == root
        }.values()
        for incarnation in incarnations:
            incarnation.lease_state = LeaseState.REVOKING
            incarnation.lease_state = (
                LeaseState.FAILED if failed else LeaseState.REVOKED
            )

    def _owned_keys_for_unit(self, plugin_id: str) -> set[str]:
        with self._ownership_lock:
            thread_keys = set(self._owned_threads)
            process_keys = set(self._owned_processes)
        keys = (
            set(self._owned_tasks)
            | set(self._entry_tasks)
            | set(self._owned_handles)
            | set(self._owned_io_watchers)
            | set(self._owned_executor_futures)
            | thread_keys
            | process_keys
        )
        return {owner for owner in keys if self._root_owner(owner) == plugin_id}

    def _install_task_factory(self) -> None:
        if self._task_factory_installed:
            return
        loop = asyncio.get_running_loop()
        for name in (
            "get_task_factory",
            "set_task_factory",
            "run_in_executor",
            "set_default_executor",
            "call_later",
            "call_at",
        ):
            if not callable(getattr(loop, name, None)):
                self.enabled = False
                self.compatibility_error = "event_loop_tracking_unsupported"
                return
        self._original_task_factory = loop.get_task_factory()
        self._tracked_loop = loop

        def factory(
            loop: asyncio.AbstractEventLoop, coro: Coroutine[Any, Any, Any], **kwargs
        ):
            if self._original_task_factory:
                task = self._original_task_factory(loop, coro, **kwargs)
            else:
                task = asyncio.Task(coro, loop=loop, **kwargs)
            context = kwargs.get("context")
            owner = (
                context.run(current_owner) if context is not None else current_owner()
            )
            if owner:
                incarnation = self._incarnations.get(owner) or self._incarnations.get(
                    self._root_owner(owner) or owner
                )
                phase = None
                if incarnation:
                    phase = (
                        context.run(
                            self._current_work, owner, incarnation.incarnation_id
                        )
                        if context is not None
                        else self._current_work(owner, incarnation.incarnation_id)
                    )
                if incarnation and not self._work_lease_is_current(
                    owner, incarnation.incarnation_id, phase
                ):
                    task.cancel()
                else:
                    if phase:
                        phase.children.add(task)
                        if phase.phase == "on_shutdown" and any(
                            phase is value for value in self._cancellation_work.values()
                        ):
                            self._cancellation_work[
                                (task, owner, incarnation.incarnation_id)
                            ] = LifecycleWork(
                                owner,
                                incarnation.incarnation_id,
                                weakref.ref(task),
                                "on_shutdown",
                                phase.budget,
                            )
                            task.add_done_callback(self._forget_cancellation_task)
                    self._owned_tasks[owner].add(task)
                    task.add_done_callback(self._owned_tasks[owner].discard)
            return task

        self._task_factory_installed = True
        try:
            loop.set_task_factory(factory)
            self._install_loop_resource_tracking(loop)
        except Exception as error:
            self._restore_task_factory()
            self.enabled = False
            if self.compatibility_error != "event_loop_tracking_restore_failed":
                self.compatibility_error = (
                    f"event_loop_tracking_unsupported:{type(error).__name__}"
                )
            logger.warning("Event loop tracking unavailable; hot operations disabled")

    def _current_work(self, owner, incarnation_id):
        phase = lifecycle_work_phase(owner, incarnation_id)
        if phase is not None:
            return phase
        try:
            task = asyncio.current_task()
        except RuntimeError:
            return None
        phase = self._cancellation_work.get((task, owner, incarnation_id))
        if phase is not None and (phase.owner, phase.incarnation_id) == (
            owner,
            incarnation_id,
        ):
            return phase
        return None

    def _forget_cancellation_task(self, task):
        self._cancellation_requests.pop(task, None)
        for key in list(self._cancellation_work):
            if key[0] is task:
                self._cancellation_work.pop(key, None)

    def consume_connection_cancellation(self, task, error):
        request = self._cancellation_requests.get(task)
        if (
            request is None
            or error.args != (request[0],)
            or request[1] != 0
            or (hasattr(task, "cancelling") and task.cancelling() != 1)
        ):
            return False
        if hasattr(task, "uncancel"):
            task.uncancel()
        return True

    def _install_loop_resource_tracking(self, loop: asyncio.AbstractEventLoop) -> None:
        if self._loop_hook_originals:
            return
        manager = self
        for method_name in ("call_later", "call_at"):
            original = getattr(loop, method_name)
            self._loop_hook_originals[method_name] = original

            def tracked(*args, _original=original, **kwargs):
                owner = current_owner()
                if not owner or len(args) < 2:
                    return _original(*args, **kwargs)
                callback_index = 1
                callback = args[callback_index]
                incarnation = manager._ensure_incarnation(owner)
                phase = manager._current_work(owner, incarnation.incarnation_id)
                holder: list[weakref.ReferenceType[asyncio.Handle]] = []

                @wraps(callback)
                def wrapped(*callback_args):
                    handle = holder[0]() if holder else None
                    if handle is not None:
                        manager._owned_handles[owner].discard(handle)
                    if not manager._work_lease_is_current(
                        owner, incarnation.incarnation_id, phase
                    ):
                        return None
                    with owner_context(owner), lifecycle_callback_context(phase):
                        return callback(*callback_args)

                replaced = list(args)
                replaced[callback_index] = wrapped
                handle = _original(*replaced, **kwargs)
                holder.append(weakref.ref(handle))
                manager._owned_handles[owner].add(handle)
                return handle

            setattr(loop, method_name, tracked)

        for method_name in ("add_reader", "add_writer"):
            original = getattr(loop, method_name, None)
            if original is None:
                continue
            self._loop_hook_originals[method_name] = original

            def tracked_io(
                fd,
                callback,
                *args,
                _original=original,
                _method_name=method_name,
            ):
                owner = current_owner()
                if not owner:
                    return _original(fd, callback, *args)
                incarnation = manager._ensure_incarnation(owner)
                phase = manager._current_work(owner, incarnation.incarnation_id)

                @wraps(callback)
                def wrapped(*callback_args):
                    if not manager._work_lease_is_current(
                        owner, incarnation.incarnation_id, phase
                    ):
                        return None
                    with owner_context(owner), lifecycle_callback_context(phase):
                        return callback(*callback_args)

                result = _original(fd, wrapped, *args)
                file_descriptor = fd if isinstance(fd, int) else int(fd.fileno())
                manager._owned_io_watchers[owner].add((_method_name, file_descriptor))
                return result

            setattr(loop, method_name, tracked_io)

        original_run_in_executor = loop.run_in_executor
        self._loop_hook_originals["run_in_executor"] = original_run_in_executor
        original_set_executor = loop.set_default_executor
        self._loop_hook_originals["set_default_executor"] = original_set_executor

        def tracked_set_executor(executor):
            result = original_set_executor(executor)
            manager._shared_executors.add(executor)
            return result

        loop.set_default_executor = tracked_set_executor

        def tracked_run_in_executor(executor, func, *args):
            owner = current_owner()
            if not owner:
                return original_run_in_executor(executor, func, *args)
            incarnation = manager._ensure_incarnation(owner)
            budget = current_budget.get()
            phase = manager._current_work(owner, incarnation.incarnation_id)
            completion: ConcurrentFuture[None] = ConcurrentFuture()

            @wraps(func)
            def run_with_owner():
                if not completion.set_running_or_notify_cancel():
                    return None
                try:
                    if budget is not None:
                        budget.check()
                    check_budget()
                    if not manager._work_lease_is_current(
                        owner, incarnation.incarnation_id, phase
                    ):
                        return None
                    with resource_context(owner):
                        return func(*args)
                finally:
                    completion.set_result(None)

            def shared_submit():
                token = shared_executor_submission.set(True)
                try:
                    return original_run_in_executor(executor, run_with_owner)
                finally:
                    shared_executor_submission.reset(token)

            future = (
                Context().run(shared_submit)
                if executor is None or executor in manager._shared_executors
                else original_run_in_executor(executor, run_with_owner)
            )
            owned = manager._owned_executor_futures[owner]
            owned.add(completion)

            def completed(_done):
                with contextlib.suppress(RuntimeError):
                    loop.call_soon_threadsafe(owned.discard, completion)

            completion.add_done_callback(completed)
            # Cancelling the asyncio wrapper does not mean its thread has exited.
            future.add_done_callback(
                lambda done: completion.cancel() if done.cancelled() else None
            )
            return future

        loop.run_in_executor = tracked_run_in_executor  # type: ignore[method-assign]

    def _restore_task_factory(self) -> None:
        if not self._task_factory_installed:
            return
        loop = self._tracked_loop
        failed = False
        if loop is not None:
            try:
                if loop.get_task_factory() is not self._original_task_factory:
                    loop.set_task_factory(self._original_task_factory)
            except Exception:
                failed = True
            for method_name, original in self._loop_hook_originals.items():
                try:
                    if getattr(loop, method_name) != original:
                        setattr(loop, method_name, original)
                except Exception:
                    failed = True
        self._loop_hook_originals.clear()
        self._task_factory_installed = False
        self._original_task_factory = None
        self._tracked_loop = None
        if failed:
            from zhenxun.services.lifecycle import lifecycle_kernel

            self.enabled = False
            self.compatibility_error = "event_loop_tracking_restore_failed"
            lifecycle_kernel.require_recovery("runtime_tracking_restore_failed")

    async def _cancel_all_owned_tasks(self) -> None:
        owners = (
            set(self._owned_tasks)
            | set(self._entry_tasks)
            | set(self._owned_handles)
            | set(self._owned_executor_futures)
            | set(self._owned_io_watchers)
        )
        try:
            await self._cancel_plugin_tasks(owners)
        finally:
            self._stop_owned_callbacks(owners)

    def _stop_owned_callbacks(self, owners: set[str]) -> None:
        loop = asyncio.get_running_loop()
        errors: list[Exception] = []
        for owner in owners:
            for future in self._owned_executor_futures.get(owner, set()):
                future.cancel()
            for handle in list(self._owned_handles.get(owner, set())):
                handle.cancel()
            for kind, fd in list(self._owned_io_watchers.get(owner, set())):
                remove = getattr(
                    loop,
                    "remove_reader" if kind == "add_reader" else "remove_writer",
                    None,
                )
                if remove is not None:
                    try:
                        remove(fd)
                    except Exception as error:
                        errors.append(error)
                    else:
                        self._owned_io_watchers[owner].discard((kind, fd))
        if errors:
            raise PluginRecoveryRequired("plugin_callback_stop_failed") from errors[0]

    def _remove_all_io_watchers(self) -> None:
        with contextlib.suppress(RuntimeError):
            loop = asyncio.get_running_loop()
            for watchers in self._owned_io_watchers.values():
                for kind, fd in list(watchers):
                    remove = getattr(
                        loop,
                        "remove_reader" if kind == "add_reader" else "remove_writer",
                        None,
                    )
                    if remove:
                        remove(fd)
        self._owned_io_watchers.clear()

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
            rollback_state=(
                "worker_recovery_required"
                if reason == "worker_recovery_required"
                else None
            ),
        )
        self.last_operation = operation
        self._persist_index()
        return operation

    def _indexing_operation(self, module: str) -> RuntimeOperation | None:
        if self._integrity_failures:
            return self._failed_operation(module, "worker_recovery_required")
        if not self._installed or self._index_ready.is_set():
            return None
        return self._failed_operation(module, "runtime_indexing")

    async def reload_plugin(self, module: str) -> RuntimeOperation:
        """Reload one loaded plugin and all of its runtime dependents."""
        if operation := self._indexing_operation(module):
            return operation
        unit = self._find_unit(module)
        if unit is None:
            return self._failed_operation(module, "plugin_not_loaded")
        if not self.enabled:
            return self._failed_operation(module, "nonebot_compatibility")

        self._collect_runtime_boundaries()

        affected = self._dependent_closure({unit.plugin_id})
        for plugin_id in affected:
            candidate = self.units[plugin_id]
            if candidate.classification is ReloadClassification.FAILED:
                return self._failed_operation(module, "plugin_reload_failed")
            if candidate.classification is not ReloadClassification.HOT_RELOADABLE:
                return self._failed_operation(module, "plugin_not_hot_reloadable")
        return await self._reload_units(affected)

    async def unload_plugin(self, module: str) -> RuntimeOperation:
        """Unload one managed plugin without requiring its files to disappear first."""
        if operation := self._indexing_operation(module):
            return operation
        unit = self._find_unit(module)
        if unit is None:
            return self._failed_operation(module, "plugin_not_loaded")
        if not self.enabled:
            return self._failed_operation(module, "nonebot_compatibility")

        self._collect_runtime_boundaries()

        affected = self._dependent_closure({unit.plugin_id})
        for plugin_id in affected:
            candidate = self.units[plugin_id]
            if candidate.classification is not ReloadClassification.HOT_RELOADABLE:
                return self._failed_operation(module, "plugin_not_hot_reloadable")
        return await self._unload_removed_units(affected, submit_restart=False)

    async def _request_restart_compat(
        self, affected: set[str], reason: str, *, submit_restart: bool
    ) -> RuntimeOperation:
        try:
            return await self.request_restart(
                affected, reason, submit_launcher=submit_restart
            )
        except TypeError as error:
            if "submit_launcher" not in str(error):
                raise
            return await self.request_restart(affected, reason)

    async def _request_dependency_restart_compat(
        self, changed: set[Path], *, submit_restart: bool
    ) -> RuntimeOperation:
        try:
            return await self.request_dependency_restart(
                changed, submit_launcher=submit_restart
            )
        except TypeError as error:
            if "submit_launcher" not in str(error):
                raise
            return await self.request_dependency_restart(changed)

    async def recover_plugin(
        self, module: str, *, submit_restart: bool = True
    ) -> RuntimeOperation:
        """Reload a plugin after its files were restored by a failed store update."""
        if operation := self._indexing_operation(module):
            return operation
        unit = self._find_unit(module)
        if unit is None:
            root = Path.cwd() / Path(*module.split("."))
            return await self.load_new_plugin(
                module, root, submit_restart=submit_restart
            )
        unit.reasons.clear()
        unit.last_error = None
        classify_unit(unit)
        if unit.classification is not ReloadClassification.HOT_RELOADABLE:
            return await self._request_restart_compat(
                {unit.plugin_id},
                sorted(unit.reasons)[0],
                submit_restart=submit_restart,
            )
        affected = self._dependent_closure({unit.plugin_id})
        for plugin_id in affected:
            candidate = self.units[plugin_id]
            if candidate is unit:
                continue
            if candidate.classification is not ReloadClassification.HOT_RELOADABLE:
                return await self._request_restart_compat(
                    affected,
                    sorted(candidate.reasons)[0],
                    submit_restart=submit_restart,
                )
        return await self._reload_units(affected)

    @managed_mutation("plugin_load")
    @provider_transaction
    async def load_new_plugin(
        self,
        module_name: str,
        root: Path,
        changed: set[Path] | None = None,
        *,
        submit_restart: bool = True,
    ) -> RuntimeOperation:
        """Load a newly installed plugin when its source has no hard boundaries."""
        if operation := self._indexing_operation(module_name):
            return operation
        root = root.resolve()
        changed = {path.resolve() for path in (changed or set())}
        files = self._source_files(root)
        if not files:
            return self._failed_operation(module_name, "plugin_source_missing")
        self.claim_content_changes(files | changed)
        if unit := self._find_unit(module_name):
            return await self.reload_plugin(unit.plugin_id)
        if not self.enabled:
            return await self._request_restart_compat(
                {module_name},
                "nonebot_compatibility",
                submit_restart=submit_restart,
            )

        dependency_files = {
            path
            for path in files | changed
            if path.name in {"requirements.txt", "requirement.txt", "pyproject.toml"}
        }
        if dependency_files:
            return await self._request_dependency_restart_compat(
                dependency_files, submit_restart=submit_restart
            )

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
            return await self._request_restart_compat(
                {module_name}, reason, submit_restart=submit_restart
            )

        async with runtime_mutation_coordinator.operation("plugin_load"):
            if self._integrity_failures:
                return self._failed_operation(module_name, "worker_recovery_required")
            from nonebot.matcher import matchers

            previous_generation = self.generation
            runtime_mutation_coordinator.checkpoint()
            provider_snapshot = active_undo.get()
            incarnation = self._prepare_candidate(module_name)
            before_plugins = {plugin.id_ for plugin in get_loaded_plugins()}
            before_matchers = {
                matcher
                for priority_matchers in matchers.values()
                for matcher in priority_matchers
            }
            try:
                with (
                    owner_context(module_name),
                    provider_capture(module_name, incarnation.incarnation_id),
                ):
                    plugin = nonebot.load_plugin(module_name)
                if plugin is None:
                    raise RuntimeError("plugin_import_failed")

                self._candidate_roots.discard(module_name)
                self._candidate_roots.add(plugin.id_.split(":", 1)[0])
                await self._run_plugin_install(plugin.module_name)
                runtime_mutation_coordinator.checkpoint()
                self.discover_loaded_plugins(activate=False)
                plugin_id = self._root_owner(plugin.id_) or plugin.id_
                unit = self.units.get(plugin_id)
                if unit is None:
                    raise RuntimeError("plugin_runtime_unit_missing")
                if unit.classification is not ReloadClassification.HOT_RELOADABLE:
                    reason = sorted(unit.reasons)[0]
                    unit.last_error = f"classification_miss:{reason}"
                    await self._rollback_new_plugin(
                        module_name, before_plugins, before_matchers, provider_snapshot
                    )
                    self._revoke_incarnation(module_name)
                    return await self._request_restart_compat(
                        {plugin_id},
                        "classification_miss",
                        submit_restart=submit_restart,
                    )

                await self._run_reload_startup_hooks({plugin_id})
                runtime_mutation_coordinator.checkpoint()
                self.discover_loaded_plugins(activate=False)
                unit = self.units.get(plugin_id)
                if (
                    unit
                    and unit.classification is not ReloadClassification.HOT_RELOADABLE
                ):
                    reason = sorted(unit.reasons)[0]
                    unit.last_error = f"classification_miss:{reason}"
                    await self._rollback_new_plugin(
                        module_name, before_plugins, before_matchers, provider_snapshot
                    )
                    self._revoke_incarnation(plugin_id)
                    return await self._request_restart_compat(
                        {plugin_id},
                        "classification_miss",
                        submit_restart=submit_restart,
                    )

                self.generation += 1
                active_incarnation = self._incarnation_for_unit(unit) or incarnation
                unit.resource_receipts = provider_snapshot.receipts(
                    plugin_id, active_incarnation.incarnation_id
                )
                self._observe_plugin_scope(unit)
                await self._reconcile_candidate_metadata({plugin_id})
                self._publish_candidates({plugin_id})
                operation = RuntimeOperation(
                    ApplyMode.HOT_RELOADED,
                    "completed",
                    [plugin_id],
                    generation=self.generation,
                )
            except MutationCancelled:
                try:
                    await self._rollback_new_plugin(
                        module_name, before_plugins, before_matchers, provider_snapshot
                    )
                    self.generation = previous_generation
                    self._revoke_incarnation(module_name)
                    self.last_operation = RuntimeOperation(
                        ApplyMode.FAILED,
                        "cancelled",
                        [module_name],
                        reason="plugin_load_cancelled",
                        generation=previous_generation,
                        rollback_state="semantic",
                    )
                    self._persist_index()
                except BaseException as cleanup_error:
                    await self._fail_integrity_operation(
                        {module_name},
                        "plugin_load_recovery_required",
                        cleanup_error,
                        generation=previous_generation,
                    )
                raise
            except asyncio.CancelledError as error:
                await self._fail_integrity_operation(
                    {module_name},
                    "plugin_load_cancelled",
                    error,
                    generation=previous_generation,
                )
                raise
            except PluginRecoveryRequired as error:
                return await self._fail_integrity_operation(
                    {module_name},
                    "plugin_load_recovery_required",
                    error,
                    generation=previous_generation,
                )
            except Exception as e:
                logger.error("新安装插件热加载失败，已隔离本次加载", e=e)
                try:
                    await self._rollback_new_plugin(
                        module_name, before_plugins, before_matchers, provider_snapshot
                    )
                except BaseException as cleanup_error:
                    operation = await self._fail_integrity_operation(
                        {module_name},
                        "plugin_load_recovery_required",
                        cleanup_error,
                        generation=previous_generation,
                    )
                    if isinstance(cleanup_error, asyncio.CancelledError):
                        raise
                    return operation
                self._revoke_incarnation(module_name, failed=True)
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

    async def apply_ext_paths(
        self, previous: set[Path], current: set[Path]
    ) -> RuntimeOperation | None:
        added = {path.resolve() for path in current - previous}
        removed = {path.resolve() for path in previous - current}
        if not added and not removed:
            return None
        if added and removed:
            return await self._request_restart_compat(
                set(), "ext_path_replaced", submit_restart=False
            )

        if removed:
            affected = {
                unit.plugin_id
                for unit in self.units.values()
                if unit.root
                and any(unit.root.resolve().is_relative_to(root) for root in removed)
            }
            closure = self._dependent_closure(affected) if affected else set()
            if closure != affected or any(
                self.units[plugin_id].classification
                is not ReloadClassification.HOT_RELOADABLE
                for plugin_id in affected
            ):
                return await self._request_restart_compat(
                    closure or affected,
                    "ext_path_remove_requires_restart",
                    submit_restart=False,
                )
            operation = (
                await self._unload_removed_units(affected, submit_restart=False)
                if affected
                else None
            )
            self.refresh_watcher()
            return operation

        candidates: list[tuple[str, Path]] = []
        for root in sorted(added):
            if not root.is_dir():
                return self._failed_operation(str(root), "ext_path_missing")
            if any(root.glob("requirement*.txt")):
                return await self._request_restart_compat(
                    set(), "ext_path_dependencies", submit_restart=False
                )
            for path in sorted(root.iterdir()):
                if path.name.startswith("_"):
                    continue
                if path.is_file() and path.suffix == ".py":
                    candidates.append((path.stem, path))
                elif path.is_dir() and (path / "__init__.py").is_file():
                    candidates.append((path.name, path))
        for module, root in candidates:
            classification = self.classification_for_source(module, root)
            if classification["reload_support"] != ReloadClassification.HOT_RELOADABLE:
                reasons = classification.get("reload_reasons") or [
                    "ext_path_plugin_requires_restart"
                ]
                return await self._request_restart_compat(
                    {module}, str(reasons[0]), submit_restart=False
                )

        from nonebot.matcher import matchers

        before_plugins = {plugin.id_ for plugin in get_loaded_plugins()}
        before_matchers = {
            matcher
            for priority_matchers in matchers.values()
            for matcher in priority_matchers
        }
        try:
            loaded = nonebot.load_plugins(*(str(path) for path in sorted(added)))
            if candidates and not loaded:
                raise RuntimeError("ext_path_plugin_import_failed")
            self.discover_loaded_plugins()
            new_ids = {
                self._root_owner(plugin.id_) or plugin.id_
                for plugin in loaded
                if plugin.id_ not in before_plugins
            }
            for plugin in loaded:
                await self._run_plugin_install(plugin.module_name)
            await self._run_reload_startup_hooks(new_ids)
            self.generation += 1
            self.discover_loaded_plugins()
            await self._reconcile_runtime_metadata()
            await self._invalidate_generation_caches()
            operation = RuntimeOperation(
                ApplyMode.HOT_RELOADED,
                "completed",
                sorted(new_ids),
                generation=self.generation,
            )
        except Exception as error:
            for module, _ in candidates:
                await self._cleanup_failed_new_plugin(
                    module, before_plugins, before_matchers
                )
            operation = RuntimeOperation(
                ApplyMode.FAILED,
                "failed",
                [str(path) for path in sorted(added)],
                reason=f"ext_path_load_failed:{type(error).__name__}",
                generation=self.generation,
            )
        self.last_operation = operation
        self._persist_index()
        if operation.mode is ApplyMode.HOT_RELOADED:
            self.refresh_watcher()
        return operation

    async def _rollback_new_plugin(
        self, module_name, before_plugins, before_matchers, provider_snapshot
    ) -> None:
        runtime_mutation_coordinator.set_phase("rolling_back")
        provider_snapshot.verify()
        provider_snapshot.recording = False
        with shutdown_budget(10.0) as budget:
            budget.check()
            task = asyncio.current_task()
            timer = asyncio.get_running_loop().call_later(
                budget.remaining(), task.cancel
            )
            try:
                await self._run_reload_shutdown_hooks({module_name})
                budget.check()
                await self._cleanup_failed_new_plugin(
                    module_name, before_plugins, before_matchers
                )
                budget.check()
                provider_snapshot.rollback()
            finally:
                timer.cancel()

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
        remove_bot_api_hooks(module_names)
        remove_driver_hooks(nonebot.get_driver(), module_names)
        self._remove_asgi_routes(module_names)
        remove_priority_hooks(module_names)
        remove_plugin_init(module_names)
        self._remove_config_registrations(plugin_id)
        self._remove_config_registrations(module_name)
        self._remove_trie_entries(plugin_id)
        self._remove_trie_entries(module_name)
        owners = {
            owner
            for owner in (
                set(self._owned_tasks)
                | set(self._owned_handles)
                | set(self._owned_io_watchers)
                | set(self._owned_executor_futures)
            )
            if owner in {plugin_id, module_name}
            or owner.startswith(f"{plugin_id}:")
            or owner.startswith(f"{module_name}.")
        }
        await self._cancel_plugin_tasks(owners)
        for owner in owners:
            for future in self._owned_executor_futures.pop(owner, set()):
                future.cancel()
            for handle in self._owned_handles.pop(owner, set()):
                handle.cancel()
            with contextlib.suppress(RuntimeError):
                loop = asyncio.get_running_loop()
                for kind, fd in self._owned_io_watchers.pop(owner, set()):
                    remove = getattr(
                        loop,
                        "remove_reader" if kind == "add_reader" else "remove_writer",
                        None,
                    )
                    if remove:
                        remove(fd)
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
        self._candidate_roots.difference_update({plugin_id, module_name})
        self._finish_initialization_work({plugin_id, module_name})

    async def apply_plugin_changes(
        self, changed: set[Path], *, submit_restart: bool = True
    ) -> RuntimeOperation:
        affected = self.affected_units(changed)
        if not affected:
            return await self._request_restart_compat(
                set(), "core_source_changed", submit_restart=submit_restart
            )
        if not self.enabled:
            return await self._request_restart_compat(
                affected, "nonebot_compatibility", submit_restart=submit_restart
            )
        if any(
            path.name in {"requirements.txt", "requirement.txt", "pyproject.toml"}
            for path in changed
        ):
            return await self._request_restart_compat(
                affected,
                "plugin_dependencies_changed",
                submit_restart=submit_restart,
            )
        removed = {
            plugin_id
            for plugin_id in affected
            if (root := self.units[plugin_id].root) is not None and not root.exists()
        }
        if removed:
            if removed != affected:
                return await self._request_restart_compat(
                    affected,
                    "plugin_dependency_removed",
                    submit_restart=submit_restart,
                )
            for plugin_id in removed:
                unit = self.units[plugin_id]
                if unit.classification is not ReloadClassification.HOT_RELOADABLE:
                    return await self._request_restart_compat(
                        removed,
                        sorted(unit.reasons)[0],
                        submit_restart=submit_restart,
                    )
            return await self._unload_removed_units(
                removed, submit_restart=submit_restart
            )
        for plugin_id in affected:
            unit = self.units[plugin_id]
            if unit.classification is not ReloadClassification.HOT_RELOADABLE:
                return await self._request_restart_compat(
                    affected,
                    sorted(unit.reasons)[0],
                    submit_restart=submit_restart,
                )
            if changed_model_file(unit, changed):
                return await self._request_restart_compat(
                    affected, "orm_model_changed", submit_restart=submit_restart
                )
        return await self._reload_units(affected)

    @managed_mutation("plugin_unload")
    @provider_transaction
    async def _unload_removed_units(
        self, affected: set[str], *, submit_restart: bool = True
    ) -> RuntimeOperation:
        async with runtime_mutation_coordinator.operation("plugin_reload"):
            if self._integrity_failures:
                return self._failed_operation(
                    "plugin_unload", "worker_recovery_required"
                )
            from zhenxun.utils.manager.priority_manager import lifecycle_component_ids

            checkpoint = self._capture_reload_checkpoint(affected)
            runtime_mutation_coordinator.checkpoint()
            retired_managers = [
                self.units[plugin_id].manager
                for plugin_id in affected
                if self.units[plugin_id].manager is not None
            ]
            component_ids = {
                component_id
                for plugin_id in affected
                for component_id in lifecycle_component_ids(
                    self.units[plugin_id].module_names
                )
            }
            try:
                for plugin_id in self._reload_order(affected):
                    await self._drain_and_unload(self.units[plugin_id])
                runtime_mutation_coordinator.checkpoint()
                self.generation += 1
                self.discover_loaded_plugins()
                await self._reconcile_runtime_metadata()
                await self._invalidate_generation_caches()
                remaining_manager_ids = {
                    id(plugin.manager) for plugin in get_loaded_plugins()
                }
                remove_nested_managers(
                    manager
                    for manager in retired_managers
                    if id(manager) not in remaining_manager_ids
                )
                from zhenxun.services.lifecycle import lifecycle_kernel

                for plugin_id in affected:
                    lifecycle_kernel.forget_plugin_incarnation(plugin_id)
                lifecycle_kernel.unregister_components(component_ids)
                operation = RuntimeOperation(
                    ApplyMode.HOT_RELOADED,
                    "completed",
                    sorted(affected),
                    generation=self.generation,
                )
            except MutationCancelled:
                await self._rollback_cancelled(checkpoint, "plugin_unload_cancelled")
                raise
            except asyncio.CancelledError as error:
                await self._fail_integrity_operation(
                    affected,
                    "plugin_unload_cancelled",
                    error,
                    generation=checkpoint.generation,
                )
                raise
            except PluginRecoveryRequired as error:
                return await self._fail_integrity_operation(
                    affected,
                    "plugin_unload_recovery_required",
                    error,
                    generation=checkpoint.generation,
                )
            except Exception as e:
                rollback_state = "semantic"
                reason = f"{type(e).__name__}:{e}"
                try:
                    await self._restore_reload_checkpoint(checkpoint)
                    for plugin_id in affected:
                        if unit := self.units.get(plugin_id):
                            unit.last_error = type(e).__name__
                    logger.error("插件热卸载失败，旧代运行资源已语义恢复", e=e)
                except BaseException as rollback_error:
                    rollback_state = "worker_recovery_required"
                    reason = "plugin_unload_recovery_required"
                    await self._fail_integrity_operation(
                        affected,
                        reason,
                        rollback_error,
                        generation=checkpoint.generation,
                    )
                    if isinstance(rollback_error, asyncio.CancelledError):
                        raise
                    logger.error(
                        "插件热卸载失败且旧代恢复不完整，已请求 worker 恢复",
                        e=rollback_error
                        if isinstance(rollback_error, Exception)
                        else None,
                    )
                operation = RuntimeOperation(
                    ApplyMode.FAILED,
                    "failed",
                    sorted(affected),
                    reason=reason,
                    generation=self.generation,
                    rollback_state=rollback_state,
                )
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
            "runtime_resources": self._resource_summary(unit),
            "dynamic_validation": (
                "verified"
                if unit.classification is ReloadClassification.HOT_RELOADABLE
                else "restart_boundary"
            ),
            "rollback_precision": self._rollback_precision(unit),
        }

    def _rollback_precision(self, unit: PluginUnit) -> str:
        if unit.classification is not ReloadClassification.HOT_RELOADABLE:
            return "worker_recovery_required"
        if any(
            not getattr(receipt, "reversible", False)
            for receipt in unit.resource_receipts
            if getattr(receipt, "state", "active") == "active"
        ):
            return "worker_recovery_required"
        summary = self._resource_summary(unit)
        if any(
            summary.get(resource_type, 0)
            for resource_type in (
                "tasks",
                "timers",
                "io_watchers",
                "executor_futures",
                "threads",
                "processes",
            )
        ):
            return "semantic"
        owners = self._owned_keys_for_unit(unit.plugin_id)
        if any(
            owner in self._config_registrations for owner in owners | {unit.plugin_id}
        ):
            return "semantic"
        if any(
            self._root_owner(owner) == unit.plugin_id
            for owner in self._job_owners.values()
        ):
            return "semantic"
        from zhenxun.services.plugin_init import PluginInitManager
        from zhenxun.utils.manager.priority_manager import lifecycle_component_ids

        with PluginInitManager._registry_lock:
            if any(
                _module_belongs_to(module_name, unit.module_names)
                for module_name in PluginInitManager.plugins
            ):
                return "worker_recovery_required"
        if lifecycle_component_ids(unit.module_names):
            return "semantic"
        semantic_provider_markers = (
            "preprocessor",
            "postprocessor",
            "lifespan",
            "bot_",
        )
        if any(
            any(marker in str(receipt.provider) for marker in semantic_provider_markers)
            for receipt in unit.resource_receipts
        ):
            return "semantic"
        return "exact"

    def _resource_summary(self, unit: PluginUnit) -> dict[str, int]:
        owners = self._owned_keys_for_unit(unit.plugin_id)
        counts = {
            "entry_tasks": len(
                {
                    task
                    for owner in owners
                    for task in self._entry_tasks.get(owner, {})
                    if not task.done()
                }
            ),
            "provider_registrations": sum(
                1
                for receipt in unit.resource_receipts
                if getattr(receipt, "state", "active") == "active"
            ),
            "tasks": sum(
                1
                for owner in owners
                for task in self._owned_tasks.get(owner, set())
                if not task.done()
            ),
            "timers": sum(
                1
                for owner in owners
                for handle in self._owned_handles.get(owner, set())
                if not handle.cancelled()
            ),
            "io_watchers": sum(
                len(self._owned_io_watchers.get(owner, set())) for owner in owners
            ),
            "executor_futures": sum(
                1
                for owner in owners
                for future in self._owned_executor_futures.get(owner, set())
                if not future.done()
            ),
            "threads": sum(
                1
                for owner in owners
                for thread in self._owned_threads.get(owner, set())
                if thread.is_alive()
            ),
            "processes": sum(
                1
                for owner in owners
                for process in self._owned_processes.get(owner, set())
                if process.poll() is None
            ),
        }
        return {name: value for name, value in counts.items() if value}

    def classification_for_source(self, module: str, root: Path) -> dict[str, Any]:
        """Classify an unimported plugin tree without registering runtime resources."""
        root = root.resolve()
        files = self._source_files(root)
        if not files:
            return {
                "reload_support": ReloadClassification.FAILED.value,
                "reload_reasons": ["plugin_source_missing"],
            }
        provisional = PluginUnit(
            plugin_id=module,
            module_name=module,
            manager=None,
            root=root,
            files=files,
        )
        classify_unit(provisional)
        if provisional.model_files:
            provisional.reasons.add("orm_model_new_plugin")
            provisional.classification = ReloadClassification.RESTART_REQUIRED
        return {
            "reload_support": provisional.classification.value,
            "reload_reasons": sorted(provisional.reasons),
        }

    def _preflight_reload(self, affected: set[str]) -> None:
        for plugin_id in sorted(affected):
            for path in sorted(self.units[plugin_id].files):
                if path.suffix != ".py":
                    continue
                source = path.read_bytes()
                compile(source, str(path), "exec", dont_inherit=True)

    def _capture_reload_checkpoint(self, affected: set[str]) -> PluginReloadCheckpoint:
        from zhenxun.configs.config import Config
        from zhenxun.services.plugin_init import PluginInitManager
        from zhenxun.utils.manager.priority_manager import (
            PriorityLifecycle,
            lifecycle_component_ids,
        )

        module_names = {
            module_name
            for plugin_id in affected
            for module_name in self.units[plugin_id].module_names
        }
        owners = {
            owner
            for plugin_id in affected
            for owner in self._owned_keys_for_unit(plugin_id)
        }
        owners.update(
            owner
            for owner in self._config_registrations
            if self._root_owner(owner) in affected
            or _module_belongs_to(owner.split(":", 1)[0], module_names)
        )
        priority_entries: list[tuple[Any, int, int, Callable, Any]] = []
        for hook_type, priority_map in PriorityLifecycle._data.items():
            for priority, funcs in priority_map.items():
                for index, func in enumerate(funcs):
                    if _module_belongs_to(
                        str(getattr(func, "__module__", "")), module_names
                    ):
                        priority_entries.append(
                            (
                                hook_type,
                                priority,
                                index,
                                func,
                                PriorityLifecycle._metadata.get(func),
                            )
                        )
        with PluginInitManager._registry_lock:
            plugin_init_entries = {
                name: value
                for name, value in PluginInitManager.plugins.items()
                if _module_belongs_to(name, module_names)
            }
        config_owners = {
            owner: set(values)
            for owner, values in self._config_registrations.items()
            if owner in owners
        }
        config_keys = {item for values in config_owners.values() for item in values}
        config_entries = {
            (module, key): deepcopy(Config._data[module].configs[key])
            for module, key in config_keys
            if module in Config._data and key in Config._data[module].configs
        }
        scheduler_jobs: list[dict[str, Any]] = []
        job_owners = {
            job_id: owner
            for job_id, owner in self._job_owners.items()
            if self._root_owner(owner) in affected
        }
        try:
            from nonebot_plugin_apscheduler import scheduler

            for job in scheduler.get_jobs():
                if job.id not in job_owners and not _module_belongs_to(
                    _callable_module(job.func), module_names
                ):
                    continue
                scheduler_jobs.append(
                    {
                        "id": job.id,
                        "func": job.func,
                        "trigger": job.trigger,
                        "args": job.args,
                        "kwargs": job.kwargs,
                        "name": job.name,
                        "executor": job.executor,
                        "misfire_grace_time": job.misfire_grace_time,
                        "coalesce": job.coalesce,
                        "max_instances": job.max_instances,
                        "next_run_time": job.next_run_time,
                    }
                )
        except (ImportError, RuntimeError):
            pass
        unit_state = {
            plugin_id: {
                "draining": unit.draining,
                "last_error": unit.last_error,
                "incarnation_id": unit.incarnation_id,
                "classification": unit.classification,
                "reasons": set(unit.reasons),
                "resource_receipts": list(unit.resource_receipts),
            }
            for plugin_id in affected
            if (unit := self.units.get(plugin_id)) is not None
        }
        component_ids = lifecycle_component_ids(module_names)
        from zhenxun.services.lifecycle import lifecycle_kernel

        component_states = {
            component_id: str(status["state"])
            for component_id in component_ids
            if (status := lifecycle_kernel.component_status(component_id)) is not None
        }
        shared_globals, shared_dependency_owners = self._capture_shared_globals(
            affected
        )
        return PluginReloadCheckpoint(
            affected=set(affected),
            module_names=module_names,
            provider_snapshot=active_undo.get(),
            generation=self.generation,
            units=dict(self.units),
            module_to_unit=dict(self.module_to_unit),
            modules={
                name: module
                for name in module_names
                if (module := sys.modules.get(name)) is not None
            },
            incarnations=dict(self._incarnations),
            incarnation_history=list(self._incarnation_history),
            incarnation_states={
                alias: incarnation.lease_state
                for alias, incarnation in self._incarnations.items()
            },
            unit_state=unit_state,
            priority_entries=priority_entries,
            plugin_init_entries=plugin_init_entries,
            config_entries=config_entries,
            config_modules=set(Config._data),
            config_owners=config_owners,
            config_add_module=list(Config.add_module),
            scheduler_jobs=scheduler_jobs,
            job_owners=job_owners,
            component_ids=component_ids,
            component_states=component_states,
            shared_globals=shared_globals,
            shared_dependency_owners=shared_dependency_owners,
            resource_summary={
                plugin_id: self._resource_summary(self.units[plugin_id])
                for plugin_id in affected
            },
        )

    async def _discard_reload_candidate(
        self, checkpoint: PluginReloadCheckpoint
    ) -> None:
        owners = {
            owner
            for plugin_id in checkpoint.affected
            for owner in self._owned_keys_for_unit(plugin_id)
        }
        await self._cancel_plugin_tasks(owners)
        self._finish_initialization_work(checkpoint.affected)
        for owner in owners:
            for future in self._owned_executor_futures.pop(owner, set()):
                future.cancel()
            for handle in self._owned_handles.pop(owner, set()):
                handle.cancel()
        self._remove_all_io_watchers_for(owners)

    def _remove_all_io_watchers_for(self, owners: set[str]) -> None:
        with contextlib.suppress(RuntimeError):
            loop = asyncio.get_running_loop()
            for owner in owners:
                for kind, fd in self._owned_io_watchers.pop(owner, set()):
                    remove = getattr(
                        loop,
                        "remove_reader" if kind == "add_reader" else "remove_writer",
                        None,
                    )
                    if remove:
                        remove(fd)

    async def _restore_reload_checkpoint(
        self, checkpoint: PluginReloadCheckpoint
    ) -> None:
        checkpoint.provider_snapshot.verify()
        checkpoint.provider_snapshot.recording = False
        runtime_mutation_coordinator.set_phase("rolling_back")
        with shutdown_budget(10.0) as budget:
            budget.check()
            task = asyncio.current_task()
            timer = asyncio.get_running_loop().call_later(
                budget.remaining(), task.cancel
            )
            try:
                await self._restore_reload_checkpoint_steps(checkpoint)
                budget.check()
            finally:
                timer.cancel()

    async def _restore_reload_checkpoint_steps(
        self, checkpoint: PluginReloadCheckpoint
    ) -> None:
        from zhenxun.configs.config import Config
        from zhenxun.services.lifecycle import lifecycle_kernel
        from zhenxun.services.plugin_init import PluginInitManager
        from zhenxun.utils.manager.priority_manager import (
            PriorityLifecycle,
            _sync_kernel_declarations,
            lifecycle_component_ids,
        )

        candidate_module_names = {
            name
            for name in sys.modules
            if any(
                name == module or name.startswith(f"{module}.")
                for module in checkpoint.module_names
            )
        }
        candidate_module_names.update(checkpoint.module_names)
        candidate_component_ids = lifecycle_component_ids(candidate_module_names)
        cleanup_error: BaseException | None = None
        try:
            await self._run_reload_shutdown_hooks(candidate_module_names)
        except BaseException as error:
            cleanup_error = error
        await self._discard_reload_candidate(checkpoint)
        check_budget()
        if isinstance(cleanup_error, asyncio.CancelledError):
            raise cleanup_error
        if cleanup_error is not None:
            raise PluginRecoveryRequired(
                "plugin_candidate_stop_failed"
            ) from cleanup_error
        checkpoint.provider_snapshot.rollback()
        self._restore_shared_globals(checkpoint)
        for name in list(sys.modules):
            if name in checkpoint.module_names or any(
                name.startswith(f"{module}.") for module in checkpoint.module_names
            ):
                sys.modules.pop(name, None)
        sys.modules.update(checkpoint.modules)
        self.units = checkpoint.units
        self.module_to_unit = checkpoint.module_to_unit
        self.generation = checkpoint.generation
        self._incarnations = checkpoint.incarnations
        self._incarnation_history = checkpoint.incarnation_history
        for alias, incarnation in self._incarnations.items():
            if self._root_owner(alias) in checkpoint.affected:
                incarnation.lease_state = checkpoint.incarnation_states[alias]
        for plugin_id, values in checkpoint.unit_state.items():
            unit = self.units[plugin_id]
            unit.draining = bool(values["draining"])
            unit.last_error = values["last_error"]
            unit.incarnation_id = values["incarnation_id"]
            unit.classification = values["classification"]
            unit.reasons = set(values["reasons"])
            unit.resource_receipts = list(values["resource_receipts"])
            incarnation = self._incarnations.get(plugin_id)
            if incarnation is not None:
                incarnation.lease_state = LeaseState.ACTIVE

        remove_priority_hooks(candidate_module_names)
        for hook_type, priority, index, func, metadata in checkpoint.priority_entries:
            funcs = PriorityLifecycle._data.setdefault(hook_type, {}).setdefault(
                priority, []
            )
            if func not in funcs:
                funcs.insert(min(index, len(funcs)), func)
            if metadata is not None:
                PriorityLifecycle._metadata[func] = metadata
        remove_plugin_init(candidate_module_names)
        with PluginInitManager._registry_lock:
            PluginInitManager.plugins.update(checkpoint.plugin_init_entries)

        candidate_config_owners = {
            owner
            for owner in self._config_registrations
            if self._root_owner(owner) in checkpoint.affected
            or any(
                owner == module or owner.startswith(f"{module}:")
                for module in checkpoint.module_names
            )
        }
        candidate_config_keys = {
            item
            for owner in candidate_config_owners
            for item in self._config_registrations.get(owner, set())
        }
        for owner in candidate_config_owners:
            self._config_registrations.pop(owner, None)
        retained_config_keys = {
            item for values in self._config_registrations.values() for item in values
        }
        for module, key in candidate_config_keys - retained_config_keys:
            group = Config._data.get(module)
            if group:
                group.configs.pop(key, None)
        for module in set(Config._data) - checkpoint.config_modules:
            if not Config._data[module].configs:
                Config._data.pop(module, None)
        for (module, key), value in checkpoint.config_entries.items():
            Config.get(module).configs[key] = deepcopy(value)
        self._config_registrations.update(
            {owner: set(values) for owner, values in checkpoint.config_owners.items()}
        )
        affected_config_entries = {
            f"{module}:{key}".lower() for module, key in checkpoint.config_entries
        }
        if affected_config_entries:
            retained_entries = [
                entry
                for entry in Config.add_module
                if entry not in affected_config_entries
            ]
            for index, entry in enumerate(checkpoint.config_add_module):
                if entry in affected_config_entries:
                    retained_entries.insert(min(index, len(retained_entries)), entry)
            Config.add_module[:] = retained_entries

        self._remove_scheduler_jobs(candidate_module_names)
        _sync_kernel_declarations()
        stale_component_ids = candidate_component_ids - checkpoint.component_ids
        lifecycle_kernel.unregister_components(stale_component_ids)
        active_component_ids = {
            component_id
            for component_id, state in checkpoint.component_states.items()
            if state in {"ready", "degraded"}
        }
        check_budget()
        await self._run_reload_startup_hooks(
            checkpoint.affected,
            component_ids=active_component_ids,
        )
        check_budget()
        try:
            from nonebot_plugin_apscheduler import scheduler

            for state in checkpoint.scheduler_jobs:
                scheduler.add_job(replace_existing=True, **state)
        except ImportError:
            if checkpoint.scheduler_jobs:
                raise RuntimeError("plugin_scheduler_restore_failed")
        self._job_owners.update(checkpoint.job_owners)
        for plugin_id in checkpoint.affected:
            unit = self.units[plugin_id]
            unit.draining = False
            self._observe_plugin_scope(unit)
        await self._reconcile_runtime_metadata()
        check_budget()
        await self._invalidate_generation_caches()
        self._validate_reload_checkpoint(checkpoint)
        self._candidate_roots.difference_update(checkpoint.affected)
        self._finish_initialization_work(checkpoint.affected)

    def _validate_reload_checkpoint(self, checkpoint: PluginReloadCheckpoint) -> None:
        from zhenxun.configs.config import Config
        from zhenxun.services.lifecycle import lifecycle_kernel
        from zhenxun.services.plugin_init import PluginInitManager
        from zhenxun.utils.manager.priority_manager import PriorityLifecycle

        if self.generation != checkpoint.generation:
            raise RuntimeError("plugin_rollback_generation_mismatch")
        for module_name, previous_module in checkpoint.modules.items():
            if sys.modules.get(module_name) is not previous_module:
                raise RuntimeError("plugin_rollback_module_identity_mismatch")

        for plugin_id in checkpoint.affected:
            unit = self.units.get(plugin_id)
            incarnation = self._incarnations.get(plugin_id)
            if unit is None or unit.draining or incarnation is None:
                raise RuntimeError("plugin_rollback_state_incomplete")
            if incarnation.lease_state is not LeaseState.ACTIVE:
                raise RuntimeError("plugin_rollback_lease_inactive")
            if self._resource_summary(unit) != checkpoint.resource_summary[plugin_id]:
                raise RuntimeError("plugin_rollback_resource_mismatch")
        for hook_type, priority, _, func, _ in checkpoint.priority_entries:
            if func not in PriorityLifecycle._data.get(hook_type, {}).get(priority, []):
                raise RuntimeError("plugin_rollback_priority_hook_missing")
        with PluginInitManager._registry_lock:
            if any(
                PluginInitManager.plugins.get(name) is not value
                for name, value in checkpoint.plugin_init_entries.items()
            ):
                raise RuntimeError("plugin_rollback_plugin_init_mismatch")
        for (module, key), value in checkpoint.config_entries.items():
            group = Config._data.get(module)
            if group is None or group.configs.get(key) != value:
                raise RuntimeError("plugin_rollback_config_mismatch")
        affected_config_entries = {
            f"{module}:{key}".lower() for module, key in checkpoint.config_entries
        }
        if [
            entry for entry in Config.add_module if entry in affected_config_entries
        ] != [
            entry
            for entry in checkpoint.config_add_module
            if entry in affected_config_entries
        ]:
            raise RuntimeError("plugin_rollback_config_index_mismatch")
        if checkpoint.scheduler_jobs:
            try:
                from nonebot_plugin_apscheduler import scheduler
            except ImportError as error:
                raise RuntimeError("plugin_rollback_scheduler_missing") from error
            for state in checkpoint.scheduler_jobs:
                job = scheduler.get_job(state["id"])
                if job is None or job.func is not state["func"]:
                    raise RuntimeError("plugin_rollback_scheduler_job_mismatch")
                if job.next_run_time != state["next_run_time"]:
                    raise RuntimeError("plugin_rollback_scheduler_schedule_mismatch")
        for component_id, previous_state in checkpoint.component_states.items():
            if previous_state not in {"ready", "degraded"}:
                continue
            status = lifecycle_kernel.component_status(component_id)
            if status is None or status["state"] not in {"ready", "degraded"}:
                raise RuntimeError("plugin_rollback_component_not_ready")

    @managed_mutation("plugin_reload")
    @provider_transaction
    async def _reload_units(self, affected: set[str]) -> RuntimeOperation:
        async with runtime_mutation_coordinator.operation("plugin_reload_batch"):
            if self._integrity_failures:
                return self._failed_operation(
                    "plugin_reload", "worker_recovery_required"
                )
            order = self._reload_order(affected)
            runtime_mutation_coordinator.checkpoint()
            try:
                self._preflight_reload(affected)
            except (OSError, SyntaxError, UnicodeError) as error:
                operation = RuntimeOperation(
                    ApplyMode.FAILED,
                    "failed",
                    sorted(affected),
                    reason=f"plugin_preflight_failed:{type(error).__name__}",
                    generation=self.generation,
                )
                self.last_operation = operation
                self._persist_index()
                return operation
            checkpoint = self._capture_reload_checkpoint(affected)
            provider_snapshot = checkpoint.provider_snapshot
            incarnations: dict[str, PluginIncarnation] = {}
            try:
                runtime_mutation_coordinator.set_phase("draining")
                for plugin_id in order:
                    await self._drain_and_unload(self.units[plugin_id])
                runtime_mutation_coordinator.checkpoint()
                runtime_mutation_coordinator.set_phase("activating")
                for plugin_id in reversed(order):
                    runtime_mutation_coordinator.checkpoint()
                    unit = self.units[plugin_id]
                    incarnations[plugin_id] = self._prepare_candidate(plugin_id)
                    with (
                        owner_context(plugin_id),
                        provider_capture(
                            plugin_id, incarnations[plugin_id].incarnation_id
                        ),
                    ):
                        plugin = unit.manager.load_plugin(unit.module_name)
                    if plugin is None:
                        raise RuntimeError(f"plugin_import_failed:{plugin_id}")
                    await self._run_plugin_install(plugin.module_name)
                runtime_mutation_coordinator.checkpoint()
                await self._run_reload_startup_hooks(affected)
                runtime_mutation_coordinator.checkpoint()
                shared_mutations = self._shared_global_mutations(checkpoint)
                if shared_mutations:
                    for plugin_id in affected:
                        evidence = sorted(
                            item
                            for root, items in shared_mutations.items()
                            if plugin_id
                            in checkpoint.shared_dependency_owners.get(root, set())
                            for item in items
                        )
                        if evidence:
                            self._shared_dependency_evidence[plugin_id] = evidence[:50]
                    raise RuntimeError(
                        "shared_dependency_global_mutation:"
                        + json.dumps(shared_mutations, sort_keys=True)
                    )
                runtime_mutation_coordinator.set_phase("committing")
                self.generation += 1
                self.discover_loaded_plugins(activate=False)
                classification_misses = {
                    plugin_id: sorted(self.units[plugin_id].reasons)
                    for plugin_id in affected
                    if plugin_id in self.units
                    and self.units[plugin_id].classification
                    is not ReloadClassification.HOT_RELOADABLE
                }
                if classification_misses:
                    raise RuntimeError(
                        "classification_miss:"
                        + json.dumps(classification_misses, sort_keys=True)
                    )
                for plugin_id, incarnation in incarnations.items():
                    if unit := self.units.get(plugin_id):
                        unit.resource_receipts = provider_snapshot.receipts(
                            plugin_id, incarnation.incarnation_id
                        )
                        self._observe_plugin_scope(unit)
                await self._reconcile_candidate_metadata(affected)
                self._publish_candidates(affected)
                operation = RuntimeOperation(
                    ApplyMode.HOT_RELOADED,
                    "completed",
                    sorted(affected),
                    generation=self.generation,
                )
            except MutationCancelled:
                await self._rollback_cancelled(checkpoint, "plugin_reload_cancelled")
                raise
            except asyncio.CancelledError as error:
                await self._fail_integrity_operation(
                    affected,
                    "plugin_reload_cancelled",
                    error,
                    generation=checkpoint.generation,
                )
                raise
            except PluginRecoveryRequired as error:
                return await self._fail_integrity_operation(
                    affected,
                    "plugin_reload_recovery_required",
                    error,
                    generation=checkpoint.generation,
                )
            except Exception as e:
                reason = (
                    "classification_miss"
                    if "classification_miss:" in str(e)
                    else "shared_dependency_global_mutation"
                    if "shared_dependency_global_mutation:" in str(e)
                    else f"{type(e).__name__}:{e}"
                )
                rollback_state = "semantic"
                try:
                    await self._restore_reload_checkpoint(checkpoint)
                    for plugin_id in affected:
                        if unit := self.units.get(plugin_id):
                            unit.last_error = type(e).__name__
                    logger.error("插件热加载失败，旧代运行资源已语义恢复", e=e)
                except BaseException as rollback_error:
                    rollback_state = "worker_recovery_required"
                    reason = "plugin_reload_recovery_required"
                    await self._fail_integrity_operation(
                        affected,
                        reason,
                        rollback_error,
                        generation=checkpoint.generation,
                    )
                    if isinstance(rollback_error, asyncio.CancelledError):
                        raise
                    logger.error(
                        "插件热加载失败且旧代恢复不完整，已请求 worker 恢复",
                        e=rollback_error
                        if isinstance(rollback_error, Exception)
                        else None,
                    )
                operation = RuntimeOperation(
                    ApplyMode.FAILED,
                    "failed",
                    sorted(affected),
                    reason=reason,
                    generation=self.generation,
                    rollback_state=rollback_state,
                )
            self.last_operation = operation
            self._persist_index()
            return operation

    async def _rollback_cancelled(
        self, checkpoint: PluginReloadCheckpoint, reason: str
    ) -> None:
        runtime_mutation_coordinator.set_phase("rolling_back")
        try:
            await self._restore_reload_checkpoint(checkpoint)
            self.last_operation = RuntimeOperation(
                ApplyMode.FAILED,
                "cancelled",
                sorted(checkpoint.affected),
                reason=reason,
                generation=checkpoint.generation,
                rollback_state="semantic",
            )
            self._persist_index()
        except BaseException as error:
            await self._fail_integrity_operation(
                checkpoint.affected,
                "plugin_rollback_recovery_required",
                error,
                generation=checkpoint.generation,
            )

    def _freeze_plugins_for_recovery(
        self, affected: set[str], error: BaseException
    ) -> None:
        self._integrity_failures.update(affected)
        for plugin_id in affected:
            self._revoke_incarnation(plugin_id, failed=True)
            if unit := self.units.get(plugin_id):
                unit.draining = True
                unit.last_error = f"rollback_failed:{type(error).__name__}"
                self._observe_plugin_scope(unit)

    async def _fail_integrity_operation(
        self,
        affected: set[str],
        reason: str,
        error: BaseException,
        *,
        generation: int,
    ) -> RuntimeOperation:
        # Persist the freeze before the first await, including repeated cancellation.
        affected = affected | {
            plugin_id
            for plugin_id, unit in self.units.items()
            if unit.module_name in affected
        }
        self.generation = generation
        self._freeze_plugins_for_recovery(affected, error)
        self.pending_restart.add(reason)
        operation = RuntimeOperation(
            ApplyMode.FAILED,
            "failed",
            sorted(affected),
            reason=reason,
            generation=generation,
            rollback_state="worker_recovery_required",
        )
        self.last_operation = operation
        self._persist_index()
        from zhenxun.services.lifecycle.operations import (
            OperationState,
            operation_registry,
        )

        operation_id = runtime_mutation_coordinator.current_operation_id
        if operation_id and operation_registry.get(operation_id):
            operation_registry.update(
                operation_id,
                state=OperationState.RECOVERY_REQUIRED,
                phase="recovery_required",
                error_code=reason,
            )
        notification_timeout = remaining_timeout(2.0)
        if notification_timeout <= 0:
            return operation
        task = asyncio.current_task()
        timed_out = False

        def expire() -> None:
            nonlocal timed_out
            timed_out = True
            if task is not None:
                task.cancel()

        timer = asyncio.get_running_loop().call_later(notification_timeout, expire)
        try:
            await self._request_integrity_recovery(affected, reason)
        except asyncio.CancelledError:
            if not timed_out and not isinstance(error, asyncio.CancelledError):
                raise
        finally:
            timer.cancel()
            self.last_operation = operation
            self._persist_index()
        return operation

    async def _cancel_plugin_tasks(self, owners: set[str], *, only_tasks=None) -> None:
        from zhenxun.services.lifecycle import lifecycle_kernel

        cleanup_tasks = lifecycle_kernel.owned_cleanup_task_ids()
        tasks = {
            task
            for owner in owners
            for task in self._owned_tasks.get(owner, set())
            if id(task) not in cleanup_tasks
        }
        tasks.update(
            task
            for owner in owners
            for task in self._entry_tasks.get(owner, {})
            if id(task) not in cleanup_tasks
        )
        if only_tasks is not None:
            tasks.intersection_update(only_tasks)
        if asyncio.current_task() in tasks:
            raise PluginRecoveryRequired("plugin_cleanup_owns_current_task")
        # Cancellation continuations keep only a task-bound, bounded cleanup lease.
        with shutdown_budget(_TASK_CANCEL_TIMEOUT) as budget:
            works = []
            for owner in owners:
                incarnation = self._incarnations.get(owner) or self._incarnations.get(
                    self._root_owner(owner)
                )
                if incarnation is None:
                    continue
                for task in tasks & (
                    self._owned_tasks.get(owner, set())
                    | set(self._entry_tasks.get(owner, {}))
                ):
                    if task.done():
                        continue
                    work = LifecycleWork(
                        owner,
                        incarnation.incarnation_id,
                        weakref.ref(task),
                        "on_shutdown",
                        budget,
                    )
                    self._cancellation_work[
                        (task, owner, incarnation.incarnation_id)
                    ] = work
                    works.append(work)
                    task.add_done_callback(self._forget_cancellation_task)
            try:
                for task in tasks:
                    task.add_done_callback(self._consume_cleanup_task)
                    if not task.done():
                        marker = f"plugin_cleanup_cancel:{uuid4().hex}"
                        self._cancellation_requests[task] = (
                            marker,
                            task.cancelling() if hasattr(task, "cancelling") else 0,
                        )
                        task.cancel(marker)
                if tasks:
                    done, pending = await asyncio.wait(
                        tasks, timeout=budget.remaining()
                    )
                else:
                    done, pending = set(), set()
                observed = set(tasks)
                while True:
                    children = {
                        key[0]
                        for key, work in self._cancellation_work.items()
                        if work.budget is budget
                    } - observed
                    if not children:
                        break
                    observed.update(children)
                    for child in children:
                        child.add_done_callback(self._consume_cleanup_task)
                        if not child.done():
                            child.cancel()
                    child_done, child_pending = await asyncio.wait(
                        children, timeout=budget.remaining()
                    )
                    done.update(child_done)
                    pending.update(child_pending)
                    if not budget.remaining():
                        break
            finally:
                for work in self._cancellation_work.values():
                    if work.budget is budget:
                        work.active = False
        for task in done:
            if not task.cancelled():
                task.exception()
        for owner in owners:
            # Keep the existing set: task callbacks may still reference it.
            owned = self._owned_tasks.get(owner)
            if owned is not None:
                owned.difference_update(done)
        if pending:
            raise PluginRecoveryRequired("plugin_task_cancel_timeout")

    @staticmethod
    def _consume_cleanup_task(task: asyncio.Task[Any]) -> None:
        if not task.cancelled():
            task.exception()

    async def _request_integrity_recovery(
        self, affected: set[str], reason: str
    ) -> None:
        try:
            await self._request_restart_compat(
                affected,
                reason,
                submit_restart=True,
            )
        except Exception as error:
            self.pending_restart.add(reason)
            logger.error("worker 完整性恢复请求失败，已保留待重启状态", e=error)

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
        running_executor_work = [
            future
            for owner in self._owned_keys_for_unit(unit.plugin_id)
            for future in self._owned_executor_futures.get(owner, set())
            if not future.done()
        ]
        if running_executor_work:
            raise RuntimeError(f"plugin_executor_in_flight:{unit.plugin_id}")
        unit.draining = True
        await self._cancel_plugin_tasks(
            self._owned_keys_for_unit(unit.plugin_id), only_tasks=self._connection_tasks
        )
        if unit.in_flight:
            try:
                await asyncio.wait_for(
                    self._drained_event(unit.plugin_id).wait(),
                    timeout=remaining_timeout(10),
                )
            except asyncio.TimeoutError as e:
                raise PluginRecoveryRequired("plugin_drain_timeout") from e

        self._revoke_incarnation(unit.plugin_id)

        # Give plugin lifecycle hooks the first chance to stop their workers.
        # Cancelling owned tasks first can make a shutdown hook re-await an already
        # cancelled task and propagate CancelledError out of the WebUI request.
        try:
            await self._run_reload_shutdown_hooks(unit.module_names)
        except Exception as error:
            raise PluginRecoveryRequired("plugin_shutdown_failed") from error

        await self._cancel_plugin_tasks(self._owned_keys_for_unit(unit.plugin_id))

        self._stop_owned_callbacks(self._owned_keys_for_unit(unit.plugin_id))

        with provider_capture(unit.plugin_id, unit.incarnation_id):
            self._remove_plugin_registrations(unit)
        from zhenxun.services.lifecycle import lifecycle_kernel

        lifecycle_kernel.release_plugin_incarnation(unit.plugin_id)
        await lifecycle_kernel.drain_scope_cleanups()

    def _remove_plugin_registrations(self, unit: PluginUnit) -> None:
        plugins = [
            plugin
            for plugin in get_loaded_plugins()
            if plugin.id_ == unit.plugin_id
            or plugin.id_.startswith(f"{unit.plugin_id}:")
        ]
        clean_matchers(matcher for plugin in plugins for matcher in plugin.matcher)
        self._remove_scheduler_jobs(unit.module_names)
        remove_processors(unit.module_names)
        remove_bot_api_hooks(unit.module_names)
        remove_driver_hooks(nonebot.get_driver(), unit.module_names)
        self._remove_asgi_routes(unit.module_names)
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

    def _lifecycle_observation_index(self):
        from zhenxun.utils.manager.priority_manager import lifecycle_component_index

        with self._ownership_lock:
            keys = (
                set(self._owned_tasks)
                | set(self._owned_handles)
                | set(self._owned_io_watchers)
                | set(self._owned_executor_futures)
                | set(self._owned_threads)
                | set(self._owned_processes)
            )
        owners: dict[str, set[str]] = {}
        for key in keys:
            root = self._root_owner(key)
            if root is not None:
                owners.setdefault(root, set()).add(key)
        return owners, lifecycle_component_index()

    def _observe_plugin_scope(self, unit: PluginUnit, *, observation=None) -> None:
        if not unit.incarnation_id:
            return
        from zhenxun.services.lifecycle import lifecycle_kernel
        from zhenxun.utils.manager.priority_manager import lifecycle_component_ids

        incarnation_id = unit.incarnation_id
        owners = (
            observation[0].get(unit.plugin_id, set())
            if observation is not None
            else self._owned_keys_for_unit(unit.plugin_id)
        )
        receipts = [
            *unit.resource_receipts,
            *self._scope_resource_receipts(unit, owners=owners),
        ]
        checks = self._scope_release_checks(unit, receipts, owners=owners)

        async def stop() -> None:
            if (
                self.units.get(unit.plugin_id) is not unit
                or unit.incarnation_id != incarnation_id
            ):
                raise PluginRecoveryRequired("plugin_stop_incarnation_changed")
            unit.draining = True
            owners = self._owned_keys_for_unit(unit.plugin_id)
            try:
                drain_error = None
                try:
                    await self._cancel_plugin_tasks(
                        owners, only_tasks=self._connection_tasks
                    )
                    if unit.in_flight:
                        timeout = remaining_timeout(2.0)
                        if timeout <= 0:
                            raise PluginRecoveryRequired("plugin_drain_timeout")
                        await asyncio.wait_for(
                            self._drained_event(unit.plugin_id).wait(), timeout=timeout
                        )
                except (TimeoutError, PluginRecoveryRequired) as error:
                    drain_error = error
                self._revoke_incarnation(unit.plugin_id)
                await self._cancel_plugin_tasks(owners)
                if drain_error is not None:
                    raise PluginRecoveryRequired(
                        "plugin_drain_timeout"
                    ) from drain_error
            finally:
                self._stop_owned_callbacks(owners)

        lifecycle_kernel.observe_plugin_incarnation(
            unit.plugin_id,
            unit.incarnation_id,
            source_digest=unit.fingerprint,
            receipts=receipts,
            classification=unit.classification.value,
            stop=stop,
            release_checks=checks,
            stop_after=lifecycle_component_ids(
                unit.module_names,
                index=observation[1] if observation is not None else None,
            ),
        )

    def _scope_release_checks(
        self,
        unit: PluginUnit,
        receipts: list[ResourceReceipt],
        *,
        owners: set[str] | None = None,
    ) -> dict[str, Callable[[], bool]]:
        checks: dict[str, Callable[[], bool]] = {}
        incarnation = self._incarnations.get(unit.plugin_id)
        for receipt in receipts:
            guarded_provider = receipt.provider.startswith(
                "nonebot."
            ) or receipt.provider in {
                "asgi.routes",
                "priority",
                "config",
                "scheduler",
                "plugin_init",
            }
            unsafe_route = receipt.provider == "asgi.routes" and any(
                self._root_owner(owner) == unit.plugin_id
                for owner in self._unsafe_route_owners
            )
            if (
                receipt.resource_type == "registration"
                and receipt.incarnation_id == unit.incarnation_id
                and guarded_provider
                and not unsafe_route
            ):
                receipt.detail["lease_deactivation"] = True
                if incarnation is not None and incarnation.accepts_work:
                    receipt.state = "active"
                    receipt.completed_at = None
                    receipt.error_code = None
                checks[receipt.receipt_id] = lambda inc=incarnation, observed=unit: (
                    inc is not None
                    and inc.lease_state in {LeaseState.REVOKED, LeaseState.FAILED}
                    and observed.in_flight == 0
                )
        for owner in (
            self._owned_keys_for_unit(unit.plugin_id) if owners is None else owners
        ):
            for task in self._owned_tasks.get(owner, set()):
                checks[f"task:{id(task)}"] = task.done
            for handle in self._owned_handles.get(owner, set()):
                checks[f"timer:{id(handle)}"] = lambda handle=handle, owner=owner: (
                    handle.cancelled()
                    or handle not in self._owned_handles.get(owner, set())
                )
            for future in self._owned_executor_futures.get(owner, set()):
                checks[f"executor:{id(future)}"] = future.done
            for thread in self._owned_threads.get(owner, set()):
                checks[f"thread:{id(thread)}"] = (
                    lambda thread=thread: not thread.is_alive()
                )
            for process in self._owned_processes.get(owner, set()):
                checks[f"process:{process.pid}"] = (
                    lambda process=process: process.poll() is not None
                )
            for kind, fd in self._owned_io_watchers.get(owner, set()):
                checks[f"io:{kind}:{fd}"] = lambda owner=owner, kind=kind, fd=fd: (
                    (kind, fd) not in self._owned_io_watchers.get(owner, set())
                )
        observed_ids = {receipt.receipt_id for receipt in receipts}
        return {key: check for key, check in checks.items() if key in observed_ids}

    def _scope_resource_receipts(
        self, unit: PluginUnit, *, owners: set[str] | None = None
    ) -> list[ResourceReceipt]:
        if owners is None:
            owners = self._owned_keys_for_unit(unit.plugin_id)
        incarnation_id = unit.incarnation_id
        receipts: list[ResourceReceipt] = []
        for owner in owners:
            receipts.extend(
                ResourceReceipt(
                    f"task:{id(task)}",
                    "asyncio",
                    "task",
                    unit.plugin_id,
                    incarnation_id,
                )
                for task in self._owned_tasks.get(owner, set())
                if not task.done()
            )
            receipts.extend(
                ResourceReceipt(
                    f"timer:{id(handle)}",
                    "asyncio",
                    "timer",
                    unit.plugin_id,
                    incarnation_id,
                )
                for handle in self._owned_handles.get(owner, set())
                if not handle.cancelled()
            )
            receipts.extend(
                ResourceReceipt(
                    f"executor:{id(future)}",
                    "asyncio",
                    "executor_future",
                    unit.plugin_id,
                    incarnation_id,
                    reversible=False,
                )
                for future in self._owned_executor_futures.get(owner, set())
                if not future.done()
            )
            receipts.extend(
                ResourceReceipt(
                    f"thread:{id(thread)}",
                    "threading",
                    "thread",
                    unit.plugin_id,
                    incarnation_id,
                    reversible=False,
                )
                for thread in self._owned_threads.get(owner, set())
                if thread.is_alive()
            )
            receipts.extend(
                ResourceReceipt(
                    f"process:{process.pid}",
                    "subprocess",
                    "process",
                    unit.plugin_id,
                    incarnation_id,
                    reversible=False,
                )
                for process in self._owned_processes.get(owner, set())
                if process.poll() is None
            )
            receipts.extend(
                ResourceReceipt(
                    f"io:{kind}:{fd}",
                    "asyncio",
                    "io_watcher",
                    unit.plugin_id,
                    incarnation_id,
                )
                for kind, fd in self._owned_io_watchers.get(owner, set())
            )
        return receipts

    def refresh_lifecycle_scopes(self) -> None:
        self._collect_runtime_boundaries()
        observation = self._lifecycle_observation_index()
        for unit in self.units.values():
            self._observe_plugin_scope(unit, observation=observation)

    async def refresh_lifecycle_scopes_async(self) -> None:
        from zhenxun.services.runtime_mutation import runtime_mutation_coordinator

        generation = self.generation

        def interrupted() -> bool:
            return (
                runtime_mutation_coordinator.locked
                or not runtime_mutation_coordinator.accepting
                or self.generation != generation
            )

        if interrupted():
            return
        units = [(unit, unit.incarnation_id) for unit in self.units.values()]
        for index, _ in enumerate(self._iter_runtime_boundaries(), 1):
            if index % 8 == 0:
                await asyncio.sleep(0)
                if interrupted():
                    return
        observation = self._lifecycle_observation_index()
        for index, (unit, incarnation) in enumerate(units, 1):
            if (
                self.units.get(unit.plugin_id) is unit
                and unit.incarnation_id == incarnation
                and not unit.draining
            ):
                self._observe_plugin_scope(unit, observation=observation)
            if index % 8 == 0:
                await asyncio.sleep(0)
                if interrupted():
                    return

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

    def _remove_asgi_routes(self, module_names: set[str]) -> None:
        try:
            app = nonebot.get_app()
        except (AssertionError, AttributeError, ValueError):
            return
        retained = []
        for route in app.routes:
            endpoint = getattr(route, "endpoint", None)
            module = str(getattr(endpoint, "__module__", ""))
            recorded_owner = self._asgi_route_owners.get(id(route))
            owned = module in module_names or bool(
                recorded_owner
                and any(
                    recorded_owner == name or recorded_owner.startswith(f"{name}:")
                    for name in module_names
                )
            )
            if owned:
                self._asgi_route_owners.pop(id(route), None)
            else:
                retained.append(route)
        if len(retained) != len(app.routes):
            app.routes[:] = retained
            app.openapi_schema = None

    async def _run_plugin_install(self, module_name: str) -> None:
        from zhenxun.services.plugin_init import PluginInitManager

        for registered in list(PluginInitManager.plugins):
            if registered == module_name or registered.startswith(f"{module_name}."):
                await PluginInitManager.install(registered, raise_on_error=True)

    async def _run_plugin_remove(self, module_names: set[str]) -> None:
        from zhenxun.services.plugin_init import PluginInitManager

        for registered in list(PluginInitManager.plugins):
            if registered in module_names:
                await PluginInitManager.remove(registered, raise_on_error=True)

    async def _run_reload_shutdown_hooks(self, module_names: set[str]) -> None:
        from zhenxun.utils.enum import PriorityLifecycleType
        from zhenxun.utils.manager.priority_manager import (
            PriorityLifecycle,
            _paired_shutdown_hooks,
            _run_hook,
            lifecycle_component_ids,
        )

        component_ids = lifecycle_component_ids(module_names)
        if component_ids:
            from zhenxun.services.lifecycle import lifecycle_kernel

            await lifecycle_kernel.stop_components(component_ids)
        paired = {item[1] for item in _paired_shutdown_hooks().values()}
        priority_data = PriorityLifecycle._data.get(PriorityLifecycleType.SHUTDOWN, {})
        for priority in sorted(priority_data):
            for func in list(priority_data[priority]):
                if (
                    func not in paired
                    and getattr(func, "__module__", "") in module_names
                ):
                    await _run_hook(func, priority, "shutdown")
        driver = nonebot.get_driver()
        funcs = [
            func
            for func in driver._lifespan._shutdown_funcs
            if getattr(func, "__module__", "") in module_names
        ]
        if funcs:
            await driver._lifespan._run_lifespan_func(reversed(funcs))

    async def _run_reload_startup_hooks(
        self,
        affected: set[str],
        *,
        component_ids: set[str] | None = None,
    ) -> None:
        supervisor = asyncio.current_task()

        def retain(work):
            root = self._root_owner(work.owner) or work.owner
            if root in affected and root in self._candidate_roots:
                work.retained = True
                work.supervisor = weakref.ref(supervisor)
                work.loop = supervisor.get_loop()

                def register():
                    if root in self._candidate_roots:
                        self._initialization_work[work.owner].append(work)
                    else:
                        work.active = False

                try:
                    loop = asyncio.get_running_loop()
                except RuntimeError:
                    loop = None
                if loop is work.loop:
                    register()
                else:
                    Context().run(work.loop.call_soon_threadsafe, register)

        token = initialization_retainer.set(retain)
        try:
            await self._run_reload_startup_hooks_impl(
                affected, component_ids=component_ids
            )
            self._check_initialization_work(affected)
        except asyncio.CancelledError:
            self._check_initialization_work(affected)
            raise
        finally:
            initialization_retainer.reset(token)

    async def _run_reload_startup_hooks_impl(
        self, affected: set[str], *, component_ids: set[str] | None = None
    ) -> None:
        from zhenxun.utils.manager.priority_manager import (
            _sync_kernel_declarations,
            lifecycle_component_ids,
        )

        module_names = {
            module
            for plugin_id in affected
            if (unit := self.units.get(plugin_id))
            for module in unit.module_names
        }
        _sync_kernel_declarations()
        selected_component_ids = (
            lifecycle_component_ids(module_names)
            if component_ids is None
            else component_ids
        )
        if selected_component_ids:
            from zhenxun.services.lifecycle import lifecycle_kernel

            await lifecycle_kernel.start_components(selected_component_ids)
            for component_id in selected_component_ids:
                status = lifecycle_kernel.component_status(component_id)
                if not status or status["state"] != "ready":
                    raise RuntimeError(f"plugin_initialization_failed:{component_id}")
        driver = nonebot.get_driver()
        for registry in (
            driver._lifespan._startup_funcs,
            driver._lifespan._ready_funcs,
        ):
            funcs = [
                func
                for func in registry
                if getattr(func, "__module__", "") in module_names
            ]
            if funcs:
                await driver._lifespan._run_lifespan_func(funcs)

    async def _reconcile_candidate_metadata(self, roots: set[str]) -> None:
        from zhenxun.services.startup_load import startup_load_planner

        modules = {
            module
            for root in roots
            if (unit := self.units.get(root))
            for module in unit.module_names
        }
        with startup_load_planner.preview_runtime_recovery(modules):
            await self._reconcile_runtime_metadata()
            await self._invalidate_generation_caches()

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
                    "插件元数据产生的配置写入已纳入当前运行时操作，跳过后续重复重载"
                )
            await reconcile_plugin_runtime()
            await reconcile_task_runtime()
            from zhenxun.services.plugin_policy import plugin_policy_service

            await plugin_policy_service.reconcile()
        except Exception as e:
            if e.__class__.__name__ == "OperationalError" and (
                "no such table" in str(e).lower() or "does not exist" in str(e).lower()
            ):
                logger.warning(
                    "插件已完成运行时加载，数据库元数据表尚未就绪，跳过本次协调"
                )
                return
            logger.error("插件代际元数据协调失败", e=e)
            raise

    async def _invalidate_generation_caches(self) -> None:
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
        restart_dependencies: set[tuple[str, str]] | None = None,
        submit_restart: bool = True,
    ) -> RuntimeOperation | None:
        if self._installed and not self._index_ready.is_set():
            return self._failed_operation("config", "runtime_indexing")
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
        restart_dependencies = (
            changed_dependencies
            if restart_dependencies is None
            else restart_dependencies
        )
        restart_affected = {
            plugin_id
            for plugin_id in affected
            if self.units[plugin_id].classification
            is not ReloadClassification.HOT_RELOADABLE
            and any(
                dependency in restart_dependencies
                or (dependency[0], "*") in restart_dependencies
                for dependency in self.units[plugin_id].config_dependencies
            )
        }
        if restart_affected:
            return await self._request_restart_compat(
                affected,
                "import_time_config_consumer_requires_restart",
                submit_restart=submit_restart,
            )
        unsafe_pending = any(
            unit.classification is not ReloadClassification.HOT_RELOADABLE
            and any(
                dependency in restart_dependencies
                or (dependency[0], "*") in restart_dependencies
                for dependency in unit.config_dependencies
            )
            for unit in self.units.values()
        )
        if not unsafe_pending:
            self.clear_pending_restart("import_time_config_consumer_requires_restart")
        hot_affected = {
            plugin_id
            for plugin_id in affected
            if self.units[plugin_id].classification
            is ReloadClassification.HOT_RELOADABLE
        }
        return await self._reload_units(hot_affected) if hot_affected else None

    async def reload_env_consumers(
        self,
        changed_keys: set[str],
        *,
        submit_restart: bool = False,
    ) -> RuntimeOperation | None:
        if self._installed and not self._index_ready.is_set():
            return self._failed_operation("environment", "runtime_indexing")
        normalized = {key.upper() for key in changed_keys}
        affected, unsafe = self.environment_consumers(normalized)
        if not affected:
            return None
        if unsafe:
            return await self._request_restart_compat(
                affected,
                "import_time_environment_consumer_requires_restart",
                submit_restart=submit_restart,
            )
        return await self._reload_units(affected)

    def environment_consumers(
        self, changed_keys: set[str]
    ) -> tuple[set[str], set[str]]:
        normalized = {key.upper() for key in changed_keys}
        affected = {
            unit.plugin_id
            for unit in self.units.values()
            if unit.env_dependencies & normalized
        }
        if not affected:
            return set(), set()
        affected = self._dependent_closure(affected)
        unsafe = {
            plugin_id
            for plugin_id in affected
            if self.units[plugin_id].classification
            is not ReloadClassification.HOT_RELOADABLE
        }
        return affected, unsafe

    def refresh_watcher(self) -> None:
        if not self._watcher_task or self._watcher_task.done():
            return
        self._watcher_refresh_requested = True

    def consume_watcher_refresh(self) -> bool:
        requested = self._watcher_refresh_requested
        self._watcher_refresh_requested = False
        return requested

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

    def _content_changes_held(self, paths: set[Path]) -> bool:
        return any(
            path == root or path.is_relative_to(root)
            for path in paths
            for root, count in self._content_change_holds.items()
            if count > 0
        )

    async def wait_for_content_change_holds(self, paths: set[Path]) -> None:
        resolved = {path.resolve() for path in paths}
        while self._content_changes_held(resolved):
            await self._content_changes_released.wait()

    @contextlib.asynccontextmanager
    async def hold_content_changes(self, roots: set[Path]):
        resolved = {root.resolve() for root in roots}
        for root in resolved:
            self._content_change_holds[root] += 1
        self._content_changes_released.clear()
        try:
            yield
        finally:
            for root in resolved:
                remaining = self._content_change_holds[root] - 1
                if remaining > 0:
                    self._content_change_holds[root] = remaining
                else:
                    self._content_change_holds.pop(root, None)
            if not self._content_change_holds:
                self._content_changes_released.set()

    async def process_changes(
        self, paths: set[Path], *, submit_restart: bool = True
    ) -> RuntimeOperation | None:
        if self._change_coordinator is None:
            from .coordinator import RuntimeChangeCoordinator

            self._change_coordinator = RuntimeChangeCoordinator(self)
        return await self._change_coordinator.process(
            paths, submit_restart=submit_restart
        )

    def mark_content_processed(self, path: Path) -> None:
        self.claim_content_changes({path.resolve()})

    def clear_pending_restart(self, reason: str | None = None) -> None:
        if reason is None:
            self.pending_restart.clear()
        else:
            self.pending_restart.discard(reason)
        self._persist_index()

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

    async def request_dependency_restart(
        self, changed: set[Path], *, submit_launcher: bool = True
    ) -> RuntimeOperation:
        reason = "dependencies_changed"
        self.pending_restart.add(reason)
        mode = ApplyMode.RESTART_PENDING
        if submit_launcher and bool(
            __import__("os").environ.get("ZHENXUN_LAUNCHER_PID")
        ):
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

    @staticmethod
    def runtime_watch_mode() -> str:
        value = str(os.getenv("RUNTIME_WATCH_MODE", "hot_only")).strip().lower()
        return (
            value if value in {"hot_only", "disabled", "auto_restart"} else "hot_only"
        )

    def status(self) -> dict[str, Any]:
        counts = {item.value: 0 for item in ReloadClassification}
        for unit in self.units.values():
            counts[unit.classification.value] += 1
        return {
            "index_ready": self._index_ready.is_set(),
            "watching": self._watcher_task is not None
            and not self._watcher_task.done(),
            "watcher": {
                "mode": self.runtime_watch_mode(),
                "state": self.watcher_state,
                "retry_count": self.watcher_retry_count,
                "last_error_code": self.watcher_last_error,
                "roots": list(self.watcher_roots),
            },
            "hot_reload_enabled": self.enabled and not self._integrity_failures,
            "integrity_recovery_required": sorted(self._integrity_failures),
            "compatibility_error": self.compatibility_error,
            "hook_failures": list(self._hook_failures),
            "entry_diagnostics": dict(self._entry_diagnostics),
            "plugin_runtime_generation": self.generation,
            "webui_revision": self.webui_revision,
            "pending_restart": bool(self.pending_restart),
            "pending_restart_reasons": sorted(self.pending_restart),
            "classification_counts": counts,
            "shared_dependency_evidence": {
                plugin_id: list(values)
                for plugin_id, values in self._shared_dependency_evidence.items()
            },
            "plugins": [
                {
                    **unit.public_dict(),
                    "runtime_resources": self._resource_summary(unit),
                    "dynamic_validation": (
                        "verified"
                        if unit.classification is ReloadClassification.HOT_RELOADABLE
                        else "restart_boundary"
                    ),
                    "rollback_precision": self._rollback_precision(unit),
                }
                for unit in sorted(self.units.values(), key=lambda item: item.plugin_id)
            ],
            "last_operation": self.last_operation.public_dict()
            if self.last_operation
            else None,
            "incarnations": [
                incarnation.public_dict()
                for incarnation in self._incarnation_history[-100:]
            ],
        }

    def _persist_index(self) -> None:
        data = {
            "version": 2,
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "generation": self.generation,
            "plugins": [
                {**unit.public_dict(), "file_cache": unit.file_cache}
                for unit in self.units.values()
            ],
            "pending_restart_reasons": sorted(self.pending_restart),
            "integrity_recovery_required": sorted(self._integrity_failures),
        }
        try:
            write_json_locked(_INDEX_FILE, data)
        except OSError as e:
            logger.warning(f"运行时生命周期索引写入失败: {type(e).__name__}")

    @staticmethod
    def _load_index_cache() -> dict[str, dict[str, Any]]:
        try:
            data = json.loads(_INDEX_FILE.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            return {}
        if data.get("version") != 2 or not isinstance(data.get("plugins"), list):
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
