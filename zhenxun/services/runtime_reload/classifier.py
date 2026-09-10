from __future__ import annotations

import ast
import hashlib
from pathlib import Path

from .models import PluginUnit, ReloadClassification

_HARD_ASGI_CALLS = {
    "add_exception_handler",
    "add_middleware",
    "lifespan",
    "middleware",
    "mount",
}
_PROCESS_CALLS = {"Popen", "Process", "Thread", "create_subprocess_exec"}


def _call_name(node: ast.Call) -> str:
    target = node.func
    if isinstance(target, ast.Name):
        return target.id
    if isinstance(target, ast.Attribute):
        return target.attr
    return ""


def _inspect_python_source(
    source: str, path: Path
) -> tuple[set[str], bool, set[str], set[str], set[str]]:
    reasons: set[str] = set()
    defines_model = False
    env_dependencies: set[str] = set()
    try:
        tree = ast.parse(source, filename=str(path))
    except (SyntaxError, UnicodeError):
        return {"source_unreadable"}, False, set(), set(), set()

    aliases: dict[str, str] = {}
    imported_modules: set[str] = set()
    for node in tree.body:
        if isinstance(node, ast.Import):
            for item in node.names:
                aliases[item.asname or item.name.split(".")[0]] = item.name
                imported_modules.add(item.name)
        elif isinstance(node, ast.ImportFrom):
            module = node.module or ""
            if module:
                imported_modules.add(module)
            for item in node.names:
                aliases[item.asname or item.name] = f"{module}.{item.name}"

    def config_like(node: ast.AST) -> bool:
        if isinstance(node, ast.Name):
            return node.id in {"BotConfig", "config", "driver_config"}
        return isinstance(node, ast.Attribute) and node.attr == "config"

    class ImportScopeVisitor(ast.NodeVisitor):
        def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
            return

        def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
            return

        def visit_ClassDef(self, node: ast.ClassDef) -> None:
            return

        def visit_Attribute(self, node: ast.Attribute) -> None:
            if config_like(node.value):
                env_dependencies.add(node.attr.upper())
            self.generic_visit(node)

        def visit_Call(self, node: ast.Call) -> None:
            if (
                isinstance(node.func, ast.Attribute)
                and isinstance(node.func.value, ast.Name)
                and node.func.value.id == "os"
                and node.func.attr == "getenv"
                and node.args
                and isinstance(node.args[0], ast.Constant)
                and isinstance(node.args[0].value, str)
            ):
                env_dependencies.add(node.args[0].value.upper())
            elif (
                isinstance(node.func, ast.Name)
                and node.func.id == "getattr"
                and len(node.args) >= 2
                and config_like(node.args[0])
                and isinstance(node.args[1], ast.Constant)
                and isinstance(node.args[1].value, str)
            ):
                env_dependencies.add(node.args[1].value.upper())
            self.generic_visit(node)

    ImportScopeVisitor().visit(tree)

    class BoundaryVisitor(ast.NodeVisitor):
        def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
            for decorator in node.decorator_list:
                self.visit(decorator)

        def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
            for decorator in node.decorator_list:
                self.visit(decorator)

        def visit_ClassDef(self, node: ast.ClassDef) -> None:
            for decorator in node.decorator_list:
                self.visit(decorator)
            for statement in node.body:
                if not isinstance(statement, ast.FunctionDef | ast.AsyncFunctionDef):
                    self.visit(statement)

        def visit_Call(self, node: ast.Call) -> None:
            name = _call_name(node)
            if name in _HARD_ASGI_CALLS:
                reasons.add("asgi_root_registration")
            elif name == "register_adapter":
                reasons.add("adapter_registration")
            elif name in _PROCESS_CALLS:
                reasons.add("thread_or_process")
            elif name in {"import_module", "__import__"}:
                reasons.add("dynamic_import")
            self.generic_visit(node)

    BoundaryVisitor().visit(tree)

    import_time_dependency_calls: set[str] = set()

    class ImportCallVisitor(ast.NodeVisitor):
        def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
            for decorator in node.decorator_list:
                self.visit(decorator)

        def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
            for decorator in node.decorator_list:
                self.visit(decorator)

        def visit_Call(self, node: ast.Call) -> None:
            target = node.func
            resolved = ""
            if isinstance(target, ast.Name):
                resolved = aliases.get(target.id, "")
            elif isinstance(target, ast.Attribute):
                parts = [target.attr]
                owner = target.value
                while isinstance(owner, ast.Attribute):
                    parts.append(owner.attr)
                    owner = owner.value
                if isinstance(owner, ast.Name) and owner.id in aliases:
                    resolved = ".".join([aliases[owner.id], *reversed(parts)])
            name = resolved.rsplit(".", 1)[-1]
            if resolved and name[:1].islower():
                import_time_dependency_calls.add(resolved)
            self.generic_visit(node)

    ImportCallVisitor().visit(tree)
    orm_modules = {
        "nonebot_plugin_orm",
        "tortoise.models",
        "zhenxun.services.db_context",
        "zhenxun.services.db_context.base_model",
    }
    orm_classes: set[str] = set()
    unresolved = [node for node in tree.body if isinstance(node, ast.ClassDef)]
    for node in unresolved:
        for base in node.bases:
            if isinstance(base, ast.Name):
                resolved = aliases.get(base.id, "")
            elif isinstance(base, ast.Attribute) and isinstance(base.value, ast.Name):
                owner = aliases.get(base.value.id, base.value.id)
                resolved = f"{owner}.{base.attr}"
            else:
                resolved = ""
            if resolved == "zhenxun.services.plugin_init.PluginInit":
                reasons.add("legacy_lifecycle_not_transactional")
    while unresolved:
        changed = False
        for node in unresolved.copy():
            for base in node.bases:
                if isinstance(base, ast.Name):
                    resolved = aliases.get(base.id, base.id)
                elif isinstance(base, ast.Attribute) and isinstance(
                    base.value, ast.Name
                ):
                    owner = aliases.get(base.value.id, base.value.id)
                    resolved = f"{owner}.{base.attr}"
                else:
                    resolved = ""
                module, _, name = resolved.rpartition(".")
                if (
                    name in {"Model", "AbstractModel"} and module in orm_modules
                ) or resolved in orm_classes:
                    defines_model = True
                    orm_classes.add(node.name)
                    unresolved.remove(node)
                    changed = True
                    break
        if not changed:
            break
    return (
        reasons,
        defines_model,
        env_dependencies,
        imported_modules,
        import_time_dependency_calls,
    )


def inspect_python_file(path: Path) -> tuple[set[str], bool]:
    try:
        reasons, defines_model, _, _, _ = _inspect_python_source(
            path.read_text(encoding="utf-8"), path
        )
        return reasons, defines_model
    except (OSError, UnicodeError):
        return {"source_unreadable"}, False


def _inspect_file(path: Path, cached: dict[str, object] | None) -> dict[str, object]:
    try:
        stat = path.stat()
    except OSError:
        return {
            "size": -1,
            "mtime_ns": -1,
            "digest": "missing",
            "reasons": ["source_unreadable"],
            "defines_model": False,
            "imports": [],
            "import_time_dependency_calls": [],
        }
    if (
        cached
        and cached.get("analysis_version") == 5
        and cached.get("ctime_ns") == stat.st_ctime_ns
        and cached.get("size") == stat.st_size
        and cached.get("mtime_ns") == stat.st_mtime_ns
    ):
        return dict(cached)
    try:
        from zhenxun.services.startup_load import startup_load_planner

        shared = startup_load_planner.runtime_file_record(path)
    except ImportError:
        shared = None
    if (
        shared
        and shared.get("record_version") == 5
        and shared.get("ctime_ns") == stat.st_ctime_ns
        and shared.get("size") == stat.st_size
        and shared.get("mtime_ns") == stat.st_mtime_ns
        and "import_time_dependency_calls" in shared
    ):
        return {
            "analysis_version": 5,
            "ctime_ns": stat.st_ctime_ns,
            "size": stat.st_size,
            "mtime_ns": stat.st_mtime_ns,
            "digest": shared.get("digest", ""),
            "reasons": list(shared.get("runtime_reasons", [])),
            "defines_model": bool(shared.get("defines_model")),
            "env_dependencies": list(shared.get("env_dependencies", [])),
            "imports": list(shared.get("imports", [])),
            "import_time_dependency_calls": list(
                shared.get("import_time_dependency_calls", [])
            ),
        }
    try:
        data = path.read_bytes()
    except OSError:
        data = b""
        reasons, defines_model = {"source_unreadable"}, False
    else:
        if path.suffix == ".py":
            try:
                source = data.decode("utf-8")
            except UnicodeError:
                reasons, defines_model = {"source_unreadable"}, False
            else:
                (
                    reasons,
                    defines_model,
                    env_dependencies,
                    imported_modules,
                    import_time_dependency_calls,
                ) = _inspect_python_source(source, path)
        else:
            reasons, defines_model = set(), False
            env_dependencies = set()
    if "env_dependencies" not in locals():
        env_dependencies = set()
    if "imported_modules" not in locals():
        imported_modules = set()
    if "import_time_dependency_calls" not in locals():
        import_time_dependency_calls = set()
    return {
        "analysis_version": 5,
        "ctime_ns": stat.st_ctime_ns,
        "size": stat.st_size,
        "mtime_ns": stat.st_mtime_ns,
        "digest": hashlib.sha256(data).hexdigest(),
        "reasons": sorted(reasons),
        "defines_model": defines_model,
        "env_dependencies": sorted(env_dependencies),
        "imports": sorted(imported_modules),
        "import_time_dependency_calls": sorted(import_time_dependency_calls),
    }


def classify_unit(unit: PluginUnit, cached: dict[str, object] | None = None) -> None:
    unit.reasons.clear()
    unit.model_files.clear()
    unit.imported_modules.clear()
    unit.import_time_dependency_calls.clear()
    cached_files = cached.get("file_cache", {}) if cached else {}
    if not isinstance(cached_files, dict):
        cached_files = {}
    file_cache: dict[str, dict[str, object]] = {}
    fingerprint = hashlib.sha256()
    for path in sorted(unit.files):
        key = str(path)
        cached_item = cached_files.get(key)
        item = _inspect_file(
            path, cached_item if isinstance(cached_item, dict) else None
        )
        file_cache[key] = item
        fingerprint.update(key.encode())
        fingerprint.update(str(item.get("digest", "")).encode())
        unit.reasons.update(str(value) for value in item.get("reasons", []))
        if bool(item.get("defines_model")):
            unit.model_files.add(path)
            if path.name == "__init__.py":
                unit.reasons.add("orm_model_in_package_init")
        unit.env_dependencies.update(
            str(value) for value in item.get("env_dependencies", [])
        )
        unit.imported_modules.update(str(value) for value in item.get("imports", []))
        unit.import_time_dependency_calls.update(
            str(value) for value in item.get("import_time_dependency_calls", [])
        )
    unit.file_cache = file_cache
    unit.fingerprint = fingerprint.hexdigest()
    if unit.reasons:
        unit.classification = ReloadClassification.RESTART_REQUIRED
    else:
        unit.classification = ReloadClassification.HOT_RELOADABLE


def changed_model_file(unit: PluginUnit, changed: set[Path]) -> bool:
    return bool(unit.model_files & changed)
