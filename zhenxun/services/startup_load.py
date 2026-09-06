from __future__ import annotations

import ast
import asyncio
from collections import defaultdict
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from functools import wraps
import hashlib
import importlib.util
from pathlib import Path
import sys
import time
from typing import Any

from nonebot.log import logger as nonebot_logger
from nonebot.utils import is_coroutine_callable, run_sync

from zhenxun.utils.atomic_json import read_json_locked, write_json_locked

from .startup import startup_coordinator

_INDEX_PATH = Path("data/runtime/startup-load-index-v1.json")
_CORE_BUILTINS = {"hooks", "init", "web_ui"}
_PREBIND_HOOK_PREFIXES = (
    "nonebot_plugin_orm",
    "nonebot.drivers",
    "zhenxun.services.db_context",
)
_ROUTE_CALLS = {
    "add_exception_handler",
    "add_middleware",
    "lifespan",
    "middleware",
    "mount",
}
_PROCESS_CALLS = {"Popen", "Process", "Thread", "create_subprocess_exec"}
_MODEL_MODULES = {
    "nonebot_plugin_orm",
    "tortoise.models",
    "zhenxun.services.db_context",
    "zhenxun.services.db_context.base_model",
}


def _load_plugin_with_error(
    manager: Any, plugin_id: str
) -> tuple[Any, BaseException | None]:
    """Capture the import error NoneBot logs and intentionally swallows."""
    captured: list[BaseException] = []

    def capture(message: Any) -> None:
        record = message.record
        exception = record.get("exception")
        if "Failed to import" not in str(record.get("message") or ""):
            return
        value = getattr(exception, "value", None)
        if isinstance(value, BaseException):
            captured.append(value)

    sink_id = nonebot_logger.add(capture, level="ERROR", catch=True)
    try:
        result = manager.load_plugin(plugin_id)
    finally:
        nonebot_logger.remove(sink_id)
    return result, captured[-1] if captured else None


def _call_name(node: ast.Call) -> str:
    target = node.func
    if isinstance(target, ast.Name):
        return target.id
    if isinstance(target, ast.Attribute):
        return target.attr
    return ""


def _literal_string(node: ast.AST) -> str | None:
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    return None


class _ImportBoundaryVisitor(ast.NodeVisitor):
    """Inspect definitions and statements that execute while a module imports."""

    def __init__(self, module_name: str) -> None:
        self.module_name = module_name
        self.reasons: set[str] = set()
        self.imports: set[str] = set()
        self.requires: set[str] = set()
        self.import_time_dependency_calls: set[str] = set()
        self.aliases: dict[str, str] = {}
        self.env_dependencies: set[str] = set()

    def visit_Import(self, node: ast.Import) -> None:
        for item in node.names:
            self.imports.add(item.name)
            self.aliases[item.asname or item.name.split(".")[0]] = item.name

    def visit_ImportFrom(self, node: ast.ImportFrom) -> None:
        module = node.module or ""
        if node.level:
            package = self.module_name.rpartition(".")[0]
            try:
                module = importlib.util.resolve_name(
                    f"{'.' * node.level}{module}", package
                )
            except (ImportError, ValueError):
                module = ""
        if module:
            self.imports.add(module)
        for item in node.names:
            if item.name == "*":
                continue
            resolved = f"{module}.{item.name}" if module else item.name
            self.aliases[item.asname or item.name] = resolved

    def visit_ClassDef(self, node: ast.ClassDef) -> None:
        for base in node.bases:
            if isinstance(base, ast.Name):
                resolved = self.aliases.get(base.id, base.id)
            elif isinstance(base, ast.Attribute):
                owner = base.value.id if isinstance(base.value, ast.Name) else ""
                resolved = f"{self.aliases.get(owner, owner)}.{base.attr}"
            else:
                resolved = ""
            module, _, name = resolved.rpartition(".")
            if name in {"Model", "AbstractModel"} and module in _MODEL_MODULES:
                self.reasons.add("orm_model")
        for decorator in node.decorator_list:
            self.visit(decorator)
        for statement in node.body:
            if not isinstance(statement, ast.FunctionDef | ast.AsyncFunctionDef):
                self.visit(statement)

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        self._inspect_function_decorators(node.decorator_list)

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
        self._inspect_function_decorators(node.decorator_list)

    def _inspect_function_decorators(self, decorators: list[ast.expr]) -> None:
        for decorator in decorators:
            if isinstance(decorator, ast.Call):
                self.visit(decorator)

    def visit_Call(self, node: ast.Call) -> None:
        name = _call_name(node)
        resolved = ""
        if isinstance(node.func, ast.Name):
            resolved = self.aliases.get(node.func.id, "")
        elif isinstance(node.func, ast.Attribute):
            parts = [node.func.attr]
            owner = node.func.value
            while isinstance(owner, ast.Attribute):
                parts.append(owner.attr)
                owner = owner.value
            if isinstance(owner, ast.Name) and owner.id in self.aliases:
                resolved = ".".join([self.aliases[owner.id], *reversed(parts)])
        resolved_name = resolved.rsplit(".", 1)[-1]
        if resolved and resolved_name[:1].islower():
            self.import_time_dependency_calls.add(resolved)
        if name in _ROUTE_CALLS:
            self.reasons.add("fastapi_route")
        elif name == "register_adapter":
            self.reasons.add("adapter_registration")
        elif name in _PROCESS_CALLS:
            self.reasons.add("import_time_thread_or_process")
        elif name in {"import_module", "__import__"}:
            self.reasons.add("dynamic_import")
        elif name == "require" and node.args:
            if value := _literal_string(node.args[0]):
                self.requires.add(value)
                if value == "nonebot_plugin_orm":
                    self.reasons.add("nonebot_plugin_orm")
        if (
            isinstance(node.func, ast.Attribute)
            and isinstance(node.func.value, ast.Name)
            and node.func.value.id == "os"
            and node.func.attr == "getenv"
            and node.args
        ):
            if value := _literal_string(node.args[0]):
                self.env_dependencies.add(value.upper())
        self.generic_visit(node)


def _file_record(
    path: Path,
    module_name: str,
    cached: dict[str, Any] | None,
) -> dict[str, Any]:
    try:
        stat = path.stat()
    except OSError:
        return {
            "size": -1,
            "mtime_ns": -1,
            "digest": "missing",
            "reasons": ["source_unreadable"],
            "imports": [],
            "requires": [],
        }
    if (
        cached
        and cached.get("record_version") == 4
        and cached.get("size") == stat.st_size
        and cached.get("mtime_ns") == stat.st_mtime_ns
    ):
        return dict(cached)
    try:
        data = path.read_bytes()
        tree = ast.parse(data.decode("utf-8"), filename=str(path))
    except (OSError, UnicodeError, SyntaxError):
        return {
            "size": stat.st_size,
            "mtime_ns": stat.st_mtime_ns,
            "digest": "unreadable",
            "reasons": ["source_unreadable"],
            "imports": [],
            "requires": [],
        }
    visitor = _ImportBoundaryVisitor(module_name)
    visitor.visit(tree)
    runtime_reasons = set(visitor.reasons)
    return {
        "size": stat.st_size,
        "record_version": 4,
        "mtime_ns": stat.st_mtime_ns,
        "digest": hashlib.sha256(data).hexdigest(),
        "reasons": sorted(visitor.reasons),
        "imports": sorted(visitor.imports),
        "requires": sorted(visitor.requires),
        "import_time_dependency_calls": sorted(visitor.import_time_dependency_calls),
        "runtime_reasons": sorted(runtime_reasons),
        "defines_model": "orm_model" in visitor.reasons,
        "env_dependencies": sorted(visitor.env_dependencies),
    }


def _module_for_file(root_module: str, root: Path, path: Path) -> str:
    if root.is_file():
        return root_module
    relative = path.relative_to(root)
    parts = list(relative.with_suffix("").parts)
    if parts and parts[-1] == "__init__":
        parts.pop()
    return ".".join([root_module, *parts]) if parts else root_module


@dataclass(slots=True)
class PlannedPlugin:
    source: str
    plugin_id: str
    module_name: str
    root: Path
    manager: Any
    phase: str = "runtime_load"
    reasons: set[str] = field(default_factory=set)
    dependencies: set[str] = field(default_factory=set)
    requires: set[str] = field(default_factory=set)
    fingerprint: str = ""
    file_records: dict[str, dict[str, Any]] = field(default_factory=dict)
    cache_hits: int = 0
    scanned_files: int = 0
    status: str = "pending"
    duration_ms: float | None = None

    def public_dict(self, *, detail: bool = False) -> dict[str, Any]:
        result: dict[str, Any] = {
            "source": self.source,
            "plugin_id": self.plugin_id,
            "module": self.module_name,
            "phase": self.phase,
            "status": self.status,
            "reasons": sorted(self.reasons),
            "duration_ms": self.duration_ms,
            "cache_hits": self.cache_hits,
            "scanned_files": self.scanned_files,
            "fingerprint": self.fingerprint[:12],
        }
        if detail:
            result["dependencies"] = sorted(self.dependencies)
            result["requires"] = sorted(self.requires)
        return result


@dataclass(slots=True)
class NativeHook:
    owner: str
    func: Callable[..., Any]
    hook_type: str


class StartupLoadPlanner:
    def __init__(self) -> None:
        self.entries: dict[str, PlannedPlugin] = {}
        self.order: list[str] = []
        self._native_hooks: list[NativeHook] = []
        self.failed_plugins: set[str] = set()
        self.fingerprint = ""
        self.prepared = False
        self._file_lookup: dict[str, dict[str, Any]] = {}
        self.warming_plugins: set[str] = set()
        self._warmup_hook_counts: dict[str, int] = {}

    def reset(self) -> None:
        self.entries.clear()
        self.order.clear()
        self._native_hooks.clear()
        self.failed_plugins.clear()
        self.fingerprint = ""
        self.prepared = False
        self._file_lookup.clear()
        self.warming_plugins.clear()
        self._warmup_hook_counts.clear()

    def prepare(self, roots: Iterable[tuple[str, Path]]) -> None:
        from nonebot.plugin import _managers
        from nonebot.plugin.manager import PluginManager

        self.reset()
        cached_data = read_json_locked(_INDEX_PATH, {}, quarantine_corrupt=True)
        cached_plugins = (
            cached_data.get("plugins", {}) if cached_data.get("version") == 2 else {}
        )
        if not isinstance(cached_plugins, dict):
            cached_plugins = {}

        for source, raw_root in roots:
            root = raw_root.resolve()
            if not root.is_dir():
                continue
            manager = PluginManager(search_path=[str(root)])
            _managers.append(manager)
            for plugin_id, module_name in sorted(manager.controlled_modules.items()):
                plugin_root = self._source_path(module_name, root)
                if plugin_root is None:
                    continue
                entry = PlannedPlugin(
                    source=source,
                    plugin_id=plugin_id,
                    module_name=module_name,
                    root=plugin_root,
                    manager=manager,
                )
                self._classify_entry(entry, cached_plugins.get(plugin_id))
                if source == "builtin" and plugin_id in _CORE_BUILTINS:
                    entry.reasons.add("core_builtin")
                if entry.reasons:
                    entry.phase = "critical_preload"
                self.entries[plugin_id] = entry

        self._resolve_dependencies()
        self._promote_critical_dependencies()
        self.order = self._topological_order()
        digest = hashlib.sha256()
        for plugin_id in self.order:
            entry = self.entries[plugin_id]
            digest.update(plugin_id.encode())
            digest.update(entry.fingerprint.encode())
            digest.update(entry.phase.encode())
        self.fingerprint = digest.hexdigest()
        self._file_lookup = {
            path: record
            for entry in self.entries.values()
            for path, record in entry.file_records.items()
        }
        self.prepared = True
        self._persist_index()
        startup_coordinator.set_load_plan(self)

    @staticmethod
    def _source_path(module_name: str, root: Path) -> Path | None:
        leaf = module_name.rsplit(".", 1)[-1]
        package = root / leaf
        if (package / "__init__.py").is_file():
            return package.resolve()
        module = root / f"{leaf}.py"
        return module.resolve() if module.is_file() else None

    def _classify_entry(
        self, entry: PlannedPlugin, cached: dict[str, Any] | None
    ) -> None:
        cached_files = cached.get("files", {}) if isinstance(cached, dict) else {}
        if not isinstance(cached_files, dict):
            cached_files = {}
        files = (
            sorted(entry.root.rglob("*.py")) if entry.root.is_dir() else [entry.root]
        )
        digest = hashlib.sha256()
        records: dict[str, dict[str, Any]] = {}
        imports: set[str] = set()
        for path in files:
            key = str(path)
            previous = cached_files.get(key)
            before_hit = False
            try:
                stat = path.stat()
                before_hit = bool(
                    isinstance(previous, dict)
                    and previous.get("record_version") == 4
                    and previous.get("size") == stat.st_size
                    and previous.get("mtime_ns") == stat.st_mtime_ns
                )
            except OSError:
                pass
            module_name = _module_for_file(entry.module_name, entry.root, path)
            record = _file_record(
                path,
                module_name,
                previous if isinstance(previous, dict) else None,
            )
            records[key] = record
            entry.cache_hits += int(before_hit)
            entry.scanned_files += int(not before_hit)
            entry.reasons.update(str(item) for item in record.get("reasons", []))
            entry.requires.update(str(item) for item in record.get("requires", []))
            imports.update(str(item) for item in record.get("imports", []))
            digest.update(key.encode())
            digest.update(str(record.get("digest", "")).encode())
        entry.dependencies = imports
        entry.fingerprint = digest.hexdigest()
        entry.file_records = records

    def _resolve_dependencies(self) -> None:
        module_to_id = {
            entry.module_name: plugin_id for plugin_id, entry in self.entries.items()
        }
        requirement_to_id = {
            value: plugin_id
            for plugin_id, entry in self.entries.items()
            for value in (plugin_id, entry.module_name)
        }
        for entry in self.entries.values():
            resolved: set[str] = set()
            for imported in entry.dependencies:
                candidate = imported
                while candidate:
                    if owner := module_to_id.get(candidate):
                        if owner != entry.plugin_id:
                            resolved.add(owner)
                        break
                    candidate = candidate.rpartition(".")[0]
            for requirement in entry.requires:
                if owner := requirement_to_id.get(requirement):
                    if owner != entry.plugin_id:
                        resolved.add(owner)
            entry.dependencies = resolved

    def _promote_critical_dependencies(self) -> None:
        changed = True
        while changed:
            changed = False
            for entry in self.entries.values():
                if entry.phase != "critical_preload":
                    continue
                for dependency in entry.dependencies:
                    target = self.entries.get(dependency)
                    if target and target.phase != "critical_preload":
                        target.phase = "critical_preload"
                        target.reasons.add("critical_dependency")
                        changed = True

    def _topological_order(self) -> list[str]:
        remaining = set(self.entries)
        completed: set[str] = set()
        result: list[str] = []
        while remaining:
            ready = sorted(
                plugin_id
                for plugin_id in remaining
                if self.entries[plugin_id].dependencies <= completed
            )
            if not ready:
                ready = [min(remaining)]
                self.entries[ready[0]].reasons.add("dependency_cycle")
                self.entries[ready[0]].phase = "critical_preload"
            for plugin_id in ready:
                remaining.remove(plugin_id)
                completed.add(plugin_id)
                result.append(plugin_id)
        return result

    def load_critical(self) -> None:
        for plugin_id in self.order:
            entry = self.entries[plugin_id]
            if entry.phase == "critical_preload":
                self._load_entry(entry)

    def prepare_library_plugins(self) -> None:
        needs_htmlrender = any(
            any(
                imported == "nonebot_plugin_htmlrender"
                or imported.startswith("nonebot_plugin_htmlrender.")
                for record in entry.file_records.values()
                for imported in record.get("imports", [])
            )
            or "nonebot_plugin_htmlrender" in entry.requires
            for entry in self.entries.values()
        )
        if not needs_htmlrender:
            return
        import nonebot

        driver = nonebot.get_driver()
        before = set(driver._lifespan._startup_funcs)
        nonebot.require("nonebot_plugin_htmlrender")
        for func in list(driver._lifespan._startup_funcs):
            if func not in before and str(getattr(func, "__module__", "")).startswith(
                "nonebot_plugin_htmlrender"
            ):
                driver._lifespan._startup_funcs.remove(func)

    async def load_runtime(self) -> None:
        for plugin_id in self.order:
            entry = self.entries[plugin_id]
            if entry.phase == "runtime_load":
                self._load_entry(entry)
                await asyncio.sleep(0)
        await self._run_native_hooks()

    def instrument_prebind_hooks(self, driver: Any) -> None:
        for hook_type, registry in (
            ("startup", driver._lifespan._startup_funcs),
            ("ready", driver._lifespan._ready_funcs),
        ):
            for index, func in enumerate(list(registry)):
                if getattr(func, "__zhenxun_startup_profiled__", False):
                    continue
                name = (
                    f"native_prebind_{hook_type}:"
                    f"{getattr(func, '__module__', 'unknown')}:"
                    f"{getattr(func, '__name__', '?')}"
                )
                if is_coroutine_callable(func):

                    @wraps(func)
                    async def async_profiled(_func=func, _name=name):
                        startup_coordinator.begin_operation(_name, "management")
                        started = time.monotonic()
                        state = "completed"
                        error_code = None
                        try:
                            return await _func()
                        except Exception as error:
                            state = "failed"
                            error_code = f"native_hook_failed:{type(error).__name__}"
                            raise
                        finally:
                            startup_coordinator.record_operation(
                                _name,
                                "management",
                                state,
                                (time.monotonic() - started) * 1000,
                                error_code=error_code,
                            )

                    profiled = async_profiled
                else:

                    @wraps(func)
                    def sync_profiled(_func=func, _name=name):
                        startup_coordinator.begin_operation(_name, "management")
                        started = time.monotonic()
                        state = "completed"
                        error_code = None
                        try:
                            return _func()
                        except Exception as error:
                            state = "failed"
                            error_code = f"native_hook_failed:{type(error).__name__}"
                            raise
                        finally:
                            startup_coordinator.record_operation(
                                _name,
                                "management",
                                state,
                                (time.monotonic() - started) * 1000,
                                error_code=error_code,
                            )

                    profiled = sync_profiled
                setattr(profiled, "__zhenxun_startup_profiled__", True)
                registry[index] = profiled

    def _load_entry(self, entry: PlannedPlugin) -> None:
        if entry.status != "pending":
            return
        driver = __import__("nonebot").get_driver()
        before_startup = set(driver._lifespan._startup_funcs)
        before_ready = set(driver._lifespan._ready_funcs)
        before_models: list[str] = []
        before_routes: list[Any] = []
        before_adapters = dict(getattr(driver, "_adapters", {}))
        before_plugins: set[str] = set()
        before_matchers: set[type] = set()
        if entry.phase == "runtime_load":
            from nonebot.matcher import matchers
            from nonebot.plugin import get_loaded_plugins

            from zhenxun.services.db_context.config import db_model

            before_models = list(db_model.models)
            before_routes = list(getattr(__import__("nonebot").get_app(), "routes", []))
            before_plugins = {plugin.id_ for plugin in get_loaded_plugins()}
            before_matchers = {
                matcher
                for priority_matchers in matchers.values()
                for matcher in priority_matchers
            }
        startup_coordinator.begin_operation(
            f"plugin_import:{entry.plugin_id}",
            "worker" if entry.phase == "critical_preload" else "runtime",
            plugin_id=entry.plugin_id,
            phase=entry.phase,
        )
        started = time.monotonic()
        result, import_error = _load_plugin_with_error(entry.manager, entry.plugin_id)
        entry.duration_ms = round((time.monotonic() - started) * 1000, 2)
        entry.status = "loaded" if result is not None else "failed"
        if result is None:
            self.mark_failed(
                entry.plugin_id, "plugin_import_failed", error=import_error
            )
            if self.is_core_plugin(entry.plugin_id):
                raise RuntimeError(f"core_plugin_import_failed:{entry.plugin_id}")
        elif entry.phase == "runtime_load":
            from zhenxun.services.db_context.config import db_model

            classification_miss = None
            if set(db_model.models) - set(before_models):
                classification_miss = "classification_miss:orm_model"
            elif dict(getattr(driver, "_adapters", {})) != before_adapters:
                classification_miss = "classification_miss:adapter_registration"
            if classification_miss:
                self._rollback_classification_miss(
                    entry,
                    before_models,
                    before_routes,
                    before_adapters,
                    before_plugins,
                    before_matchers,
                )
                entry.reasons.add(classification_miss)
                entry.status = "failed"
                self.mark_failed(entry.plugin_id, classification_miss)
                result = None
        self._capture_native_hooks(
            entry,
            driver,
            before_startup,
            before_ready,
        )
        startup_coordinator.record_operation(
            f"plugin_import:{entry.plugin_id}",
            "worker" if entry.phase == "critical_preload" else "runtime",
            "completed" if result is not None else "failed",
            entry.duration_ms,
            error_code=None if result is not None else "plugin_import_failed",
            details={"plugin_id": entry.plugin_id, "phase": entry.phase},
        )

    @staticmethod
    def _rollback_classification_miss(
        entry: PlannedPlugin,
        before_models: list[str],
        before_routes: list[Any],
        before_adapters: dict[str, Any],
        before_plugins: set[str],
        before_matchers: set[type],
    ) -> None:
        import nonebot
        from nonebot.matcher import matchers
        from nonebot.plugin import get_loaded_plugins

        from zhenxun.services.db_context.config import db_model
        from zhenxun.services.runtime_reload.compat import (
            clean_matchers,
            remove_driver_hooks,
            remove_plugin_init,
            remove_plugins,
            remove_priority_hooks,
            remove_processors,
        )

        current_matchers = {
            matcher
            for priority_matchers in matchers.values()
            for matcher in priority_matchers
        }
        clean_matchers(current_matchers - before_matchers)
        new_plugins = [
            plugin
            for plugin in get_loaded_plugins()
            if plugin.id_ not in before_plugins
        ]
        module_names = {plugin.module_name for plugin in new_plugins} | {
            entry.module_name
        }
        remove_processors(module_names)
        remove_driver_hooks(nonebot.get_driver(), module_names)
        remove_priority_hooks(module_names)
        remove_plugin_init(module_names)
        remove_plugins(new_plugins)
        app = nonebot.get_app()
        app.routes[:] = before_routes
        db_model.models[:] = before_models
        adapters = getattr(nonebot.get_driver(), "_adapters", None)
        if isinstance(adapters, dict):
            adapters.clear()
            adapters.update(before_adapters)
        for name in list(sys.modules):
            if name == entry.module_name or name.startswith(f"{entry.module_name}."):
                sys.modules.pop(name, None)

    def _capture_native_hooks(
        self,
        entry: PlannedPlugin,
        driver: Any,
        before_startup: set[Callable[..., Any]],
        before_ready: set[Callable[..., Any]],
    ) -> None:
        force_prebind = bool(entry.reasons & {"adapter_registration", "fastapi_route"})
        for hook_type, registry, previous in (
            ("startup", driver._lifespan._startup_funcs, before_startup),
            ("ready", driver._lifespan._ready_funcs, before_ready),
        ):
            for func in list(registry):
                if func in previous:
                    continue
                module = str(getattr(func, "__module__", ""))
                if force_prebind or module.startswith(_PREBIND_HOOK_PREFIXES):
                    continue
                registry.remove(func)
                self._native_hooks.append(
                    NativeHook(owner=entry.plugin_id, func=func, hook_type=hook_type)
                )

    async def _run_native_hooks(self) -> None:
        # NoneBot completes the startup phase before invoking ready callbacks.
        for hook in sorted(
            self._native_hooks, key=lambda hook: hook.hook_type != "startup"
        ):
            if hook.owner in self.failed_plugins:
                continue
            name = (
                f"native_{hook.hook_type}:{hook.owner}:"
                f"{getattr(hook.func, '__name__', '?')}"
            )
            startup_coordinator.begin_operation(name, "runtime", plugin_id=hook.owner)
            started = time.monotonic()
            state = "completed"
            error_code = None
            try:
                if is_coroutine_callable(hook.func):
                    await hook.func()
                else:
                    await run_sync(hook.func)()
                if hook.owner in self.failed_plugins:
                    state = "failed"
                    error_code = "plugin_lifecycle_failed"
            except Exception as error:
                state = "failed"
                error_code = f"plugin_startup_hook_failed:{type(error).__name__}"
                self.mark_failed(hook.owner, error_code, error=error)
            startup_coordinator.record_operation(
                name,
                "runtime",
                state,
                (time.monotonic() - started) * 1000,
                error_code=error_code,
                details={"plugin_id": hook.owner, "hook_type": hook.hook_type},
            )

    def summary(self, *, detail: bool = False) -> dict[str, Any]:
        counts = defaultdict(int)
        status_counts = {"loaded": 0, "failed": 0, "pending": 0}
        completed = 0
        for entry in self.entries.values():
            counts[entry.phase] += 1
            completed += int(entry.status in {"loaded", "failed"})
            state = (
                "failed"
                if entry.plugin_id in self.failed_plugins or entry.status == "failed"
                else "loaded"
                if entry.status == "loaded"
                else "pending"
            )
            status_counts[state] += 1
        result: dict[str, Any] = {
            "fingerprint": self.fingerprint,
            "counts": dict(counts),
            "status_counts": status_counts,
            "total": len(self.entries),
            "completed": completed,
            "failed_plugins": sorted(self.failed_plugins),
        }
        if detail:
            result["plugins"] = [
                self.entries[plugin_id].public_dict(detail=True)
                for plugin_id in self.order
            ]
        return result

    def owner_for_module(self, module_name: str) -> str | None:
        best: tuple[int, str] | None = None
        for plugin_id, entry in self.entries.items():
            if module_name == entry.module_name or module_name.startswith(
                f"{entry.module_name}."
            ):
                candidate = (len(entry.module_name), plugin_id)
                if best is None or candidate[0] > best[0]:
                    best = candidate
        return best[1] if best else None

    def runtime_file_record(self, path: Path) -> dict[str, Any] | None:
        return self._file_lookup.get(str(path))

    def plugin_available(self, module_name: str) -> bool:
        owner = self.owner_for_module(module_name)
        if owner in self.failed_plugins or module_name in self.failed_plugins:
            return False
        if owner is not None and self.entries[owner].status != "loaded":
            return False
        return not any(
            module_name == entry.module_name
            or module_name.startswith(f"{entry.module_name}.")
            for plugin_id, entry in self.entries.items()
            if plugin_id in self.failed_plugins
        )

    def mark_failed(
        self,
        plugin_id: str,
        error_code: str,
        *,
        error: BaseException | None = None,
    ) -> None:
        self.failed_plugins.add(plugin_id)
        entry = self.entries.get(plugin_id)
        if entry is not None:
            entry.status = "failed"
        startup_coordinator.record_error(
            "runtime",
            error_code,
            source_type="plugin",
            source_id=plugin_id,
            display_name=plugin_id,
            error=error,
        )

    def prepare_warmup_gates(self) -> None:
        from zhenxun.utils.enum import PriorityLifecycleType
        from zhenxun.utils.manager.priority_manager import HookSpec, PriorityLifecycle

        counts: dict[str, int] = defaultdict(int)
        priority_data = PriorityLifecycle._data.get(PriorityLifecycleType.STARTUP, {})
        for funcs in priority_data.values():
            for func in funcs:
                spec = PriorityLifecycle._metadata.get(func, HookSpec())
                if spec.stage != "warmup":
                    continue
                owner = self.owner_for_module(str(getattr(func, "__module__", "")))
                if owner:
                    counts[owner] += 1
        self._warmup_hook_counts = counts
        self.warming_plugins = set(counts)

    def finish_warmup_hook(self, module_name: str) -> None:
        owner = self.owner_for_module(module_name)
        if owner is None:
            return
        remaining = self._warmup_hook_counts.get(owner, 0) - 1
        if remaining > 0:
            self._warmup_hook_counts[owner] = remaining
            return
        self._warmup_hook_counts.pop(owner, None)
        self.warming_plugins.discard(owner)

    def is_core_plugin(self, plugin_id: str) -> bool:
        entry = self.entries.get(plugin_id)
        return bool(entry and entry.source == "builtin" and plugin_id in _CORE_BUILTINS)

    def _persist_index(self) -> None:
        plugins: dict[str, Any] = {}
        for plugin_id, entry in self.entries.items():
            plugins[plugin_id] = {
                "module": entry.module_name,
                "source": entry.source,
                "phase": entry.phase,
                "fingerprint": entry.fingerprint,
                "reasons": sorted(entry.reasons),
                "files": entry.file_records,
            }
        write_json_locked(
            _INDEX_PATH,
            {
                "version": 2,
                "fingerprint": self.fingerprint,
                "plugins": plugins,
            },
        )


startup_load_planner = StartupLoadPlanner()

__all__ = ["PlannedPlugin", "StartupLoadPlanner", "startup_load_planner"]
