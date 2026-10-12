"""插件能否热处理的判定

内置插件、库插件、导入期注册路由/适配器、创建线程或进程、修改外部对象、
模型变化等情况无法安全热处理，需要重启；依赖它们的插件同样需要重启。
"""

from __future__ import annotations

import ast
from pathlib import Path
import sys
from types import ModuleType
from typing import Any

from nonebot.plugin import Plugin, _plugins

from . import registry
from .models import ReloadClassification

DEPENDENCY_FILES = ("requirements.txt", "requirement.txt")
_RESTART_FILE_REASONS = {
    "adapter_registration",
    "dynamic_import",
    "external_patch",
    "fastapi_route",
    "import_time_thread_or_process",
    "nonebot_plugin_orm",
    "source_unreadable",
    "thread_or_process",
}
_THREAD_MODULES = {"_thread", "multiprocessing", "threading"}
_THREAD_NAMES = {"Pool", "Process", "Thread", "Timer", "start_new_thread"}
_BUILTIN_PREFIX = "zhenxun.builtin_plugins."
_SOURCE_PREFIX = "zhenxun.plugins."


def find_plugin(ref: str) -> Plugin | None:
    """按插件 id 或模块名查找顶层插件

    参数:
        ref: 插件 id 或模块名

    返回:
        Plugin | None: 顶层插件
    """
    plugin = _plugins.get(ref) or next(
        (p for p in list(_plugins.values()) if p.module_name == ref), None
    )
    while plugin is not None and plugin.parent_plugin is not None:
        plugin = plugin.parent_plugin
    return plugin


def source_files(root: Path) -> list[Path]:
    """插件包目录下的全部源码文件，单文件插件为其自身"""
    if root.is_dir():
        return sorted(
            path for path in root.rglob("*.py") if "__pycache__" not in path.parts
        )
    return [root] if root.is_file() else []


def plugin_root(plugin: Plugin) -> Path | None:
    """插件包目录或模块文件"""
    file = getattr(plugin.module, "__file__", None)
    if not file:
        return None
    path = Path(file).resolve()
    return path.parent if path.name == "__init__.py" else path


def _source_hazards(path: Path, root_module: str) -> set[str]:
    """源码中无法随卸载撤销的行为

    给外部模块导入的对象赋值属性（如 ``Bot.send = ...``）记为 external_patch；
    在任意位置使用线程或进程（threading / multiprocessing / _thread）记为
    thread_or_process，这些线程不属于事件循环，卸载时无法结束。

    参数:
        path: 源码文件
        root_module: 插件顶层模块名

    返回:
        set[str]: 原因
    """
    try:
        tree = ast.parse(path.read_bytes().decode("utf-8"), filename=str(path))
    except (OSError, UnicodeError, SyntaxError):
        return set()
    imported: dict[str, str] = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for item in node.names:
                imported[item.asname or item.name.split(".")[0]] = item.name
        elif isinstance(node, ast.ImportFrom) and not node.level and node.module:
            for item in node.names:
                imported[item.asname or item.name] = node.module

    def external(node: ast.AST) -> bool:
        if not isinstance(node, ast.Name) or node.id not in imported:
            return False
        module = imported[node.id]
        return not (module == root_module or module.startswith(f"{root_module}."))

    def threaded(node: ast.AST) -> bool:
        if isinstance(node, ast.Name):
            return node.id in _THREAD_NAMES and imported.get(node.id) in _THREAD_MODULES
        return (
            isinstance(node, ast.Attribute)
            and node.attr in _THREAD_NAMES
            and isinstance(node.value, ast.Name)
            and imported.get(node.value.id) in _THREAD_MODULES
        )

    reasons = set()
    for node in ast.walk(tree):
        if threaded(node):
            reasons.add("thread_or_process")
        if isinstance(node, ast.Assign):
            targets = node.targets
        elif isinstance(node, ast.AugAssign | ast.AnnAssign):
            targets = [node.target]
        elif (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id == "setattr"
            and node.args
            and external(node.args[0])
        ):
            reasons.add("external_patch")
            continue
        else:
            continue
        if any(
            isinstance(target, ast.Attribute) and external(target.value)
            for target in targets
        ):
            reasons.add("external_patch")
    return reasons


def _classification(reasons: set[str]) -> dict[str, Any]:
    return {
        "reload_support": (
            ReloadClassification.RESTART_REQUIRED
            if reasons
            else ReloadClassification.HOT_RELOADABLE
        ).value,
        "reload_reasons": sorted(reasons),
    }


class PluginClassifier:
    """按源码与插件间依赖判定插件能否热处理"""

    def __init__(self) -> None:
        self.hosted: set[str] = set()
        """运行期由插件宿主加载的插件模块"""
        self._file_cache: dict[str, tuple[tuple[int, int] | None, dict]] = {}
        self._graph_cache: tuple[tuple[int, int], dict[str, set[str]]] | None = None

    def _file_info(self, path: Path, module_name: str, root_module: str) -> dict:
        from zhenxun.services.startup_load import _file_record, startup_load_planner

        try:
            stat = path.stat()
            stamp = (stat.st_size, stat.st_mtime_ns)
        except OSError:
            stamp = None
        key = str(path)
        if (cached := self._file_cache.get(key)) and cached[0] == stamp:
            return cached[1]
        record = _file_record(
            path, module_name, startup_load_planner.runtime_file_record(path)
        )
        reasons = set(record.get("reasons", []))
        if "source_unreadable" not in reasons:
            reasons |= _source_hazards(path, root_module)
        info = {
            "reasons": reasons,
            "digest": record.get("digest"),
            "defines_model": bool(record.get("defines_model")),
        }
        self._file_cache[key] = (stamp, info)
        return info

    def source_reasons(
        self,
        module_name: str,
        root: Path,
        *,
        new: bool,
        dependencies_verified: bool = False,
    ) -> set[str]:
        """插件源码中阻止热处理的原因

        参数:
            module_name: 模块名
            root: 插件包目录或模块文件
            new: 是否为尚未加载过的插件
            dependencies_verified: 调用方已确认依赖无需变更

        返回:
            set[str]: 原因
        """
        from zhenxun.services.startup_load import _module_for_file, startup_load_planner

        files = source_files(root)
        if not files:
            return {"plugin_source_missing"}
        reasons = set()
        if module_name.startswith(_BUILTIN_PREFIX):
            reasons.add("builtin_plugin")
        for path in files:
            info = self._file_info(
                path, _module_for_file(module_name, root, path), module_name
            )
            reasons |= info["reasons"] & _RESTART_FILE_REASONS
            if not info["defines_model"]:
                continue
            if new:
                reasons.add("orm_model_new_plugin")
            elif path.name == "__init__.py" and path.parent == root:
                reasons.add("orm_model_in_package_init")
            else:
                record = startup_load_planner.runtime_file_record(path)
                if not record or record.get("digest") != info["digest"]:
                    reasons.add("orm_model_changed")
        if (
            new
            and not dependencies_verified
            and root.is_dir()
            and any((root / name).is_file() for name in DEPENDENCY_FILES)
        ):
            reasons.add("dependencies_changed")
        return reasons

    def _manageable(self, plugin: Plugin) -> bool:
        from zhenxun.services.startup_load import startup_load_planner

        name = plugin.module_name
        if name.startswith(_SOURCE_PREFIX) or name in self.hosted:
            return True
        entry = startup_load_planner.entries.get(plugin.id_)
        if entry is not None and entry.source in {"source", "external"}:
            return True
        try:
            from zhenxun.services.nonebot_store.storage import load_manifest

            plugins = load_manifest().get("plugins", {})
        except Exception:
            return False
        return any(
            isinstance(item, dict)
            and item.get("state") == "managed"
            and item.get("module_name") == name
            for item in plugins.values()
        )

    def reasons(self, plugin: Plugin) -> set[str]:
        """插件自身阻止热处理的原因

        参数:
            plugin: 顶层插件

        返回:
            set[str]: 原因
        """
        root = plugin_root(plugin)
        if root is None:
            return {"source_unreadable"}
        reasons = self.source_reasons(plugin.module_name, root, new=False)
        if "builtin_plugin" not in reasons and not self._manageable(plugin):
            reasons.add("library_plugin")
        if not reasons and any(
            self._file_info(path, "", plugin.module_name)["defines_model"]
            for path in source_files(root)
        ):
            own = registry.Ownership.of(plugin)
            if plugin.module_name in registry.model_modules(own):
                reasons.add("orm_model_in_package_init")
        return reasons

    def _graph(self, generation: int) -> dict[str, set[str]]:
        """顶层插件 id 到直接依赖它的顶层插件 id"""
        key = (generation, len(_plugins))
        if self._graph_cache is not None and self._graph_cache[0] == key:
            return self._graph_cache[1]
        roots: dict[str, str] = {}
        graph: dict[str, set[str]] = {}
        for plugin in list(_plugins.values()):
            if (top := find_plugin(plugin.id_)) is not None:
                roots[plugin.module_name] = top.id_
                graph.setdefault(top.id_, set())

        def owner(name: str) -> str | None:
            while name:
                if name in roots:
                    return roots[name]
                name = name.rpartition(".")[0]
            return None

        for name, module in list(sys.modules.items()):
            source = owner(name)
            if source is None or module is None:
                continue
            for value in list(vars(module).values()):
                if isinstance(value, ModuleType):
                    ref = value.__name__
                elif callable(value) or isinstance(value, type):
                    ref = getattr(registry._unwrap(value), "__module__", None)
                else:
                    continue
                target = owner(ref) if isinstance(ref, str) else None
                if target is not None and target != source:
                    graph[target].add(source)
        from zhenxun.services.startup_load import startup_load_planner

        by_module = {p.module_name: p.id_ for p in list(_plugins.values())}
        for entry in startup_load_planner.entries.values():
            if entry.plugin_id not in graph:
                continue
            for required in entry.requires:
                target = required if required in graph else by_module.get(required)
                if target in graph and target != entry.plugin_id:
                    graph[target].add(entry.plugin_id)
        self._graph_cache = (key, graph)
        return graph

    def closure(self, plugins: list[Plugin], generation: int) -> list[Plugin]:
        """插件及所有依赖它的插件，被依赖者在前

        参数:
            plugins: 顶层插件
            generation: 当前插件代际

        返回:
            list[Plugin]: 顶层插件
        """
        graph = self._graph(generation)
        found: list[str] = []
        queue = [plugin.id_ for plugin in plugins]
        while queue:
            plugin_id = queue.pop(0)
            if plugin_id in found or plugin_id not in _plugins:
                continue
            found.append(plugin_id)
            queue.extend(sorted(graph.get(plugin_id, ())))
        requires = {
            plugin_id: {other for other in found if plugin_id in graph.get(other, ())}
            for plugin_id in found
        }
        ordered: list[str] = []
        while len(ordered) < len(found):
            pending = [plugin_id for plugin_id in found if plugin_id not in ordered]
            # 循环依赖时按发现顺序取第一个打破循环
            ordered.append(
                next(
                    (p for p in pending if requires[p] <= set(ordered)),
                    pending[0],
                )
            )
        return [_plugins[plugin_id] for plugin_id in ordered]

    def dependents(self, plugin: Plugin, generation: int) -> set[str]:
        """直接或间接依赖插件的顶层插件 id

        参数:
            plugin: 顶层插件
            generation: 当前插件代际

        返回:
            set[str]: 插件 id
        """
        return {p.id_ for p in self.closure([plugin], generation)} - {plugin.id_}

    def restart_reasons(self, plugin: Plugin, generation: int) -> set[str]:
        """插件及其依赖方中阻止热处理的原因

        参数:
            plugin: 顶层插件
            generation: 当前插件代际

        返回:
            set[str]: 原因
        """
        reasons = self.reasons(plugin)
        if any(self.reasons(other) for other in self.closure([plugin], generation)[1:]):
            reasons.add("dependent_requires_restart")
        return reasons

    def classification_for(self, module: str, generation: int) -> dict[str, Any]:
        """已加载插件的热处理分类

        参数:
            module: 插件 id 或模块名
            generation: 当前插件代际

        返回:
            dict[str, Any]: reload_support 与 reload_reasons
        """
        plugin = find_plugin(module)
        if plugin is None:
            return {
                "reload_support": ReloadClassification.RESTART_REQUIRED.value,
                "reload_reasons": ["not_loaded"],
            }
        return _classification(self.restart_reasons(plugin, generation))

    def classification_for_source(
        self, module: str, root: Path, *, dependencies_verified: bool = False
    ) -> dict[str, Any]:
        """尚未加载的插件源码的热加载分类

        参数:
            module: 模块名
            root: 插件包目录或模块文件
            dependencies_verified: 调用方已确认依赖无需变更

        返回:
            dict[str, Any]: reload_support 与 reload_reasons
        """
        return _classification(
            self.source_reasons(
                module,
                root.resolve(),
                new=True,
                dependencies_verified=dependencies_verified,
            )
        )
