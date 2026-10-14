"""插件能否热处理的判定

内置插件、库插件、导入期注册路由/适配器、创建线程或进程、修改外部对象、
模型变化等情况无法安全热处理，需要重启；依赖它们的插件同样需要重启。
"""

from __future__ import annotations

import ast
import importlib.util
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

    def __init__(self, module_name: str, *, is_package: bool = False) -> None:
        self.module_name = module_name
        self.package = module_name if is_package else module_name.rpartition(".")[0]
        self.reasons: set[str] = set()
        self.imports: set[str] = set()
        self.requires: set[str] = set()
        self.aliases: dict[str, str] = {}
        self.deferred_annotations = False

    def visit_Module(self, node: ast.Module) -> None:
        self.deferred_annotations = any(
            isinstance(statement, ast.ImportFrom)
            and statement.module == "__future__"
            and any(item.name == "annotations" for item in statement.names)
            for statement in node.body
        )
        self.generic_visit(node)

    def _resolve_expression(self, node: ast.expr) -> str:
        if isinstance(node, ast.Name):
            return self.aliases.get(node.id, node.id)
        if isinstance(node, ast.Attribute):
            owner = self._resolve_expression(node.value)
            return f"{owner}.{node.attr}" if owner else ""
        return ""

    def visit_Import(self, node: ast.Import) -> None:
        for item in node.names:
            self.imports.add(item.name)
            self.aliases[item.asname or item.name.split(".")[0]] = (
                item.name if item.asname else item.name.split(".")[0]
            )

    def visit_ImportFrom(self, node: ast.ImportFrom) -> None:
        module = node.module or ""
        if node.level:
            try:
                module = importlib.util.resolve_name(
                    f"{'.' * node.level}{module}", self.package
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
            if module:
                self.imports.add(resolved)

    def visit_ClassDef(self, node: ast.ClassDef) -> None:
        for base in node.bases:
            self.visit(base)
            resolved = self._resolve_expression(base)
            module, _, name = resolved.rpartition(".")
            if name in {"Model", "AbstractModel"} and module in _MODEL_MODULES:
                self.reasons.add("orm_model")
        for decorator in node.decorator_list:
            self.visit(decorator)
        for keyword in node.keywords:
            self.visit(keyword.value)
        for statement in node.body:
            self.visit(statement)

    def visit_FunctionDef(self, node: ast.FunctionDef | ast.AsyncFunctionDef) -> None:
        self._inspect_function_decorators(node.decorator_list)
        for value in [*node.args.defaults, *node.args.kw_defaults]:
            if value is not None:
                self.visit(value)
        if not self.deferred_annotations:
            arguments = [*node.args.posonlyargs, *node.args.args, *node.args.kwonlyargs]
            arguments.extend(arg for arg in (node.args.vararg, node.args.kwarg) if arg)
            for value in [*(arg.annotation for arg in arguments), node.returns]:
                if value is not None:
                    self.visit(value)

    def visit_Lambda(self, node: ast.Lambda) -> None:
        for value in [*node.args.defaults, *node.args.kw_defaults]:
            if value is not None:
                self.visit(value)

    def visit_AnnAssign(self, node: ast.AnnAssign) -> None:
        self.visit(node.target)
        if node.value is not None:
            self.visit(node.value)
        if not self.deferred_annotations:
            self.visit(node.annotation)

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
        self.visit_FunctionDef(node)

    def _inspect_function_decorators(self, decorators: list[ast.expr]) -> None:
        for decorator in decorators:
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
        if resolved in {
            "nonebot.require",
            "importlib.import_module",
            "builtins.__import__",
        }:
            name = resolved_name
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
        self.generic_visit(node)


def _module_for_file(root_module: str, root: Path, path: Path) -> str:
    if root.is_file():
        return root_module
    relative = path.relative_to(root)
    parts = list(relative.with_suffix("").parts)
    if parts and parts[-1] == "__init__":
        parts.pop()
    return ".".join([root_module, *parts]) if parts else root_module


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


def _source_hazards(tree: ast.Module, root_module: str) -> set[str]:
    """源码中无法随卸载撤销的行为

    给外部模块导入的对象赋值属性（如 ``Bot.send = ...``）记为 external_patch；
    在任意位置使用线程或进程（threading / multiprocessing / _thread）记为
    thread_or_process，这些线程不属于事件循环，卸载时无法结束。

    参数:
        tree: 源码语法树
        root_module: 插件顶层模块名

    返回:
        set[str]: 原因
    """
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
        """分析源码文件的导入期行为、导入的模块与模型定义，文件未变化时复用结果

        参数:
            path: 源码文件
            module_name: 文件对应的模块名
            root_module: 插件顶层模块名

        返回:
            dict: reasons、imports、defines_model 与文件状态 stamp
        """
        try:
            stat = path.stat()
            stamp = (stat.st_size, stat.st_mtime_ns)
        except OSError:
            stamp = None
        key = str(path)
        if (cached := self._file_cache.get(key)) and cached[0] == stamp:
            return cached[1]
        try:
            tree = ast.parse(path.read_bytes().decode("utf-8"), filename=str(path))
        except (OSError, UnicodeError, SyntaxError):
            info = {
                "reasons": {"source_unreadable"},
                "imports": set(),
                "defines_model": False,
                "stamp": stamp,
            }
        else:
            visitor = _ImportBoundaryVisitor(
                module_name, is_package=path.name == "__init__.py"
            )
            visitor.visit(tree)
            info = {
                "reasons": visitor.reasons | _source_hazards(tree, root_module),
                "imports": visitor.imports | visitor.requires,
                "defines_model": "orm_model" in visitor.reasons,
                "stamp": stamp,
            }
        self._file_cache[key] = (stamp, info)
        return info

    def _source_info(self, module_name: str, root: Path, path: Path) -> dict:
        return self._file_info(
            path, _module_for_file(module_name, root, path), module_name
        )

    def source_imports(self, module_name: str, root: Path) -> set[str]:
        """插件源码导入或 require 的模块

        参数:
            module_name: 模块名
            root: 插件包目录或模块文件

        返回:
            set[str]: 模块名
        """
        return {
            imported
            for path in source_files(root)
            for imported in self._source_info(module_name, root, path)["imports"]
        }

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
        from zhenxun.services.startup_load import startup_load_planner

        files = source_files(root)
        if not files:
            return {"plugin_source_missing"}
        reasons = set()
        if module_name.startswith(_BUILTIN_PREFIX):
            reasons.add("builtin_plugin")
        for path in files:
            info = self._source_info(module_name, root, path)
            reasons |= info["reasons"] & _RESTART_FILE_REASONS
            if not info["defines_model"]:
                continue
            if new:
                reasons.add("orm_model_new_plugin")
            elif path.name == "__init__.py" and path.parent == root:
                reasons.add("orm_model_in_package_init")
            elif startup_load_planner.startup_stamp(path) != info["stamp"]:
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
            self._source_info(plugin.module_name, root, path)["defines_model"]
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
