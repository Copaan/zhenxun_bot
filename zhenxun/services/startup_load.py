from __future__ import annotations

import asyncio
from collections import defaultdict
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
import hashlib
import importlib.util
from pathlib import Path
import re
import time
from typing import Any

from nonebot.log import logger as nonebot_logger
from nonebot.utils import is_coroutine_callable, run_sync

from zhenxun.utils.atomic_json import read_json_locked, write_json_locked

from .startup import startup_coordinator

_INDEX_PATH = Path("data/runtime/startup-load-index-v1.json")
_INDEX_VERSION = 4
_CORE_BUILTINS = {"hooks", "init", "nonebot_store", "web_ui"}
_PREBIND_HOOK_PREFIXES = (
    "nonebot_plugin_orm",
    "nonebot.drivers",
    "zhenxun.services.db_context",
)
_PRELOAD_MARKERS = {
    b"db_context": "orm_model",
    b"tortoise": "orm_model",
    b"register_adapter": "adapter_registration",
    b"add_middleware": "fastapi_route",
    b".middleware(": "fastapi_route",
    b"add_exception_handler": "fastapi_route",
}
# 导入或 require 库插件的语句，仅出现库名（如字符串常量）不算使用
_LIBRARY_PATTERN = re.compile(
    rb"^[ \t]*(?:from|import)[ \t][^\n]*?\b(nonebot_plugin_(?:orm|htmlrender))\b"
    rb"|require\([ \t]*[\"'](nonebot_plugin_(?:orm|htmlrender))[\"']",
    re.MULTILINE,
)


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


@dataclass(slots=True)
class PlannedPlugin:
    source: str
    plugin_id: str
    module_name: str
    root: Path
    manager: Any
    phase: str = "runtime_load"
    reasons: set[str] = field(default_factory=set)
    status: str = "pending"
    duration_ms: float | None = None
    source_digest: str = ""
    """源码文件路径与状态的摘要"""
    imports: set[str] | None = None
    """源码导入或 require 的模块，未解析时为 None"""
    _dependencies: set[str] | None = field(default=None, repr=False)

    @property
    def dependencies(self) -> set[str]:
        """插件源码导入或 require 的其他插件 id"""
        if self._dependencies is None:
            self._dependencies = startup_load_planner.dependencies_of(self)
        return self._dependencies


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
        self._stamps: dict[str, tuple[int, int]] = {}
        self._libraries: set[str] = set()

    def reset(self) -> None:
        self.entries.clear()
        self.order.clear()
        self._native_hooks.clear()
        self.failed_plugins.clear()
        self.fingerprint = ""
        self.prepared = False
        self._stamps.clear()
        self._libraries.clear()

    def prepare(self, roots: Iterable[tuple[str, Path]]) -> None:
        """登记各来源的插件，并划分 HTTP 绑定前预载与运行期加载

        核心内置插件、使用 ORM / 模型、注册适配器或中间件的插件，
        以及上次启动时被发现需要提前加载的插件在 HTTP 绑定前加载。

        参数:
            roots: 插件来源与目录
        """
        from nonebot.plugin import _managers
        from nonebot.plugin.manager import PluginManager

        self.reset()
        cached = read_json_locked(_INDEX_PATH, {}, quarantine_corrupt=True)
        if cached.get("version") != _INDEX_VERSION:
            cached = {}
        learned = cached.get("critical", {})
        known_imports = cached.get("imports", {})
        digest = hashlib.sha256()
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
                self._scan(entry, digest)
                known = known_imports.get(plugin_id) or {}
                if known.get("digest") == entry.source_digest:
                    entry.imports = set(known.get("modules", []))
                if reason := learned.get(plugin_id):
                    entry.reasons.add(str(reason))
                if source == "builtin" and plugin_id in _CORE_BUILTINS:
                    entry.reasons.add("core_builtin")
                if entry.reasons:
                    entry.phase = "critical_preload"
                digest.update(f"{plugin_id}:{entry.phase}".encode())
                self.entries[plugin_id] = entry
                self.order.append(plugin_id)
        self.fingerprint = digest.hexdigest()
        self.prepared = True
        self._persist_index(cached)
        startup_coordinator.set_load_plan(self)

    @staticmethod
    def _source_path(module_name: str, root: Path) -> Path | None:
        leaf = module_name.rsplit(".", 1)[-1]
        package = root / leaf
        if (package / "__init__.py").is_file():
            return package.resolve()
        module = root / f"{leaf}.py"
        return module.resolve() if module.is_file() else None

    def _scan(self, entry: PlannedPlugin, digest: Any) -> None:
        """按源码标记判断插件是否需要在 HTTP 绑定前加载，并记录文件状态

        参数:
            entry: 插件加载项
            digest: 插件集合指纹
        """
        files = (
            sorted(p for p in entry.root.rglob("*.py") if "__pycache__" not in p.parts)
            if entry.root.is_dir()
            else [entry.root]
        )
        source_digest = hashlib.sha256()
        for path in files:
            try:
                stat = path.stat()
                data = path.read_bytes()
            except OSError:
                continue
            stamp = (stat.st_size, stat.st_mtime_ns)
            self._stamps[str(path)] = stamp
            digest.update(f"{path}:{stamp}".encode())
            source_digest.update(f"{path}:{stamp}".encode())
            entry.reasons.update(
                reason for marker, reason in _PRELOAD_MARKERS.items() if marker in data
            )
            libraries = {
                (match[1] or match[2]).decode()
                for match in _LIBRARY_PATTERN.finditer(data)
            }
            if "nonebot_plugin_orm" in libraries:
                entry.reasons.add("nonebot_plugin_orm")
            self._libraries.update(libraries)
        entry.source_digest = source_digest.hexdigest()

    def dependencies_of(self, entry: PlannedPlugin) -> set[str]:
        """解析插件源码导入或 require 的其他插件

        首次需要时一次解析全部未缓存导入的插件，并写入启动索引供下次启动复用。

        参数:
            entry: 插件加载项

        返回:
            set[str]: 插件 id
        """
        from zhenxun.services.plugin_host import plugin_host

        if entry.imports is None:
            for item in self.entries.values():
                if item.imports is None:
                    item.imports = plugin_host.classifier.source_imports(
                        item.module_name, item.root
                    )
            self._persist_index()
        module_to_id = {
            item.module_name: plugin_id for plugin_id, item in self.entries.items()
        }
        resolved: set[str] = set()
        for imported in entry.imports or ():
            candidate = imported
            while candidate:
                owner = module_to_id.get(candidate) or (
                    candidate if candidate in self.entries else None
                )
                if owner:
                    resolved.add(owner)
                    break
                candidate = candidate.rpartition(".")[0]
        return resolved - {entry.plugin_id}

    def startup_stamp(self, path: Path) -> tuple[int, int] | None:
        """启动时记录的源码文件大小与修改时间

        参数:
            path: 源码文件

        返回:
            tuple[int, int] | None: 文件状态，未记录时为 None
        """
        return self._stamps.get(str(path))

    def load_critical(self, *, management_only: bool = False) -> None:
        for plugin_id in self.order:
            entry = self.entries[plugin_id]
            if management_only and "core_builtin" not in entry.reasons:
                continue
            if entry.phase == "critical_preload":
                self._load_entry(entry)

    def prepare_library_plugins(self) -> None:
        """在外部插件执行前注册库插件

        localstore 总是预加载，避免插件直接导入后 NoneBot 不再视其为插件；
        ORM 与 htmlrender 仅在有插件使用时预加载。
        """
        import nonebot

        for library in ("nonebot_plugin_localstore", "nonebot_plugin_orm"):
            if importlib.util.find_spec(library) is None:
                continue
            if library == "nonebot_plugin_orm" and library not in self._libraries:
                continue
            try:
                nonebot.require(library)
            except Exception as error:
                nonebot_logger.opt(exception=error).error(
                    f"核心依赖插件预加载失败: {library} ({type(error).__name__})"
                )

        if "nonebot_plugin_htmlrender" not in self._libraries:
            return

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

    def _load_entry(self, entry: PlannedPlugin) -> None:
        if entry.status != "pending":
            return
        driver = __import__("nonebot").get_driver()
        before_startup = set(driver._lifespan._startup_funcs)
        before_ready = set(driver._lifespan._ready_funcs)
        before_models: list[str] = []
        before_adapters = dict(getattr(driver, "_adapters", {}))
        if entry.phase == "runtime_load":
            from zhenxun.services.db_context.config import db_model

            before_models = list(db_model.models)
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
                self._handle_classification_miss(entry, classification_miss)
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

    def _handle_classification_miss(self, entry: PlannedPlugin, reason: str) -> None:
        """插件需要在 HTTP 绑定前加载却被分到运行期：停用并登记重启

        参数:
            entry: 插件加载项
            reason: 遗漏原因
        """
        from zhenxun.services.plugin_host.host import plugin_host

        entry.reasons.add(reason)
        self.mark_failed(entry.plugin_id, reason)
        self._persist_index()
        plugin_host.pending_restart.add("plugin_classification_miss")
        nonebot_logger.warning(
            f"插件 {entry.plugin_id} 需要在启动早期加载（{reason}），"
            "已暂停使用，重启后生效"
        )

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

    def summary(self) -> dict[str, Any]:
        counts = defaultdict(int)
        status_counts = {"loaded": 0, "failed": 0, "pending": 0, "unloaded": 0}
        completed = 0
        for entry in self.entries.values():
            counts[entry.phase] += 1
            completed += int(entry.status in {"loaded", "failed"})
            state = (
                "failed"
                if entry.plugin_id in self.failed_plugins or entry.status == "failed"
                else entry.status
                if entry.status in {"loaded", "unloaded"}
                else "pending"
            )
            status_counts[state] += 1
        return {
            "fingerprint": self.fingerprint,
            "counts": dict(counts),
            "status_counts": status_counts,
            "total": len(self.entries),
            "completed": completed,
            "failed_plugins": sorted(self.failed_plugins),
        }

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

    def commit_runtime_recovery(
        self,
        module_names: set[str],
        *,
        plugin_ids: set[str] | None = None,
    ) -> None:
        """热重载成功后恢复插件可用状态并清除其降级原因

        参数:
            module_names: 重新加载的模块
            plugin_ids: 重新加载的插件 id
        """
        owners = {
            owner for name in module_names if (owner := self.owner_for_module(name))
        } | (plugin_ids or set())
        for owner in owners:
            self.failed_plugins.discard(owner)
            if entry := self.entries.get(owner):
                entry.status = "loaded"
        self.failed_plugins.difference_update(module_names)
        startup_coordinator.clear_plugin_failures(owners)

    def is_core_plugin(self, plugin_id: str) -> bool:
        entry = self.entries.get(plugin_id)
        return bool(entry and entry.source == "builtin" and plugin_id in _CORE_BUILTINS)

    def _persist_index(self, previous: dict[str, Any] | None = None) -> None:
        """记录需要在 HTTP 绑定前加载的插件与各插件的导入，内容未变化时跳过

        参数:
            previous: 启动时读到的旧记录
        """
        payload = {
            "version": _INDEX_VERSION,
            "critical": {
                plugin_id: reason
                for plugin_id, entry in self.entries.items()
                for reason in sorted(entry.reasons)
                if reason.startswith("classification_miss")
            },
            "imports": {
                plugin_id: {
                    "digest": entry.source_digest,
                    "modules": sorted(entry.imports),
                }
                for plugin_id, entry in self.entries.items()
                if entry.imports is not None
            },
        }
        if payload != previous:
            write_json_locked(_INDEX_PATH, payload)


startup_load_planner = StartupLoadPlanner()

__all__ = ["PlannedPlugin", "StartupLoadPlanner", "startup_load_planner"]
