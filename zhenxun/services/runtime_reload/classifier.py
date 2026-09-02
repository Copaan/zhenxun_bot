from __future__ import annotations

import ast
import hashlib
from pathlib import Path

from .models import PluginUnit, ReloadClassification

_ROUTE_CALLS = {"include_router", "add_api_route", "add_route", "mount"}
_ROUTE_DECORATORS = {
    "api_route",
    "delete",
    "get",
    "head",
    "options",
    "patch",
    "post",
    "put",
    "trace",
    "websocket",
    "websocket_route",
}
_PROCESS_CALLS = {"Popen", "Process", "Thread", "create_subprocess_exec"}


def _call_name(node: ast.Call) -> str:
    target = node.func
    if isinstance(target, ast.Name):
        return target.id
    if isinstance(target, ast.Attribute):
        return target.attr
    return ""


def _decorator_name(node: ast.expr) -> str:
    target = node.func if isinstance(node, ast.Call) else node
    return target.attr if isinstance(target, ast.Attribute) else ""


def _inspect_python_source(source: str, path: Path) -> tuple[set[str], bool, set[str]]:
    reasons: set[str] = set()
    defines_model = False
    env_dependencies: set[str] = set()
    try:
        tree = ast.parse(source, filename=str(path))
    except (SyntaxError, UnicodeError):
        return {"source_unreadable"}, False, set()

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

    for node in ast.walk(tree):
        if isinstance(node, ast.ClassDef):
            for base in node.bases:
                name = (
                    base.id
                    if isinstance(base, ast.Name)
                    else base.attr
                    if isinstance(base, ast.Attribute)
                    else ""
                )
                if name in {"Model", "AbstractModel", "BaseModel"}:
                    # Pydantic BaseModel is not an ORM boundary.
                    imported = source[: node.lineno and 4096]
                    if "tortoise" in imported or name != "BaseModel":
                        defines_model = True
        elif isinstance(node, ast.Call):
            name = _call_name(node)
            if name in _ROUTE_CALLS:
                reasons.add("fastapi_route")
            elif name == "register_adapter":
                reasons.add("adapter_registration")
            elif name in _PROCESS_CALLS:
                reasons.add("thread_or_process")
            elif name in {"import_module", "__import__"}:
                reasons.add("dynamic_import")
        elif isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef):
            if any(
                _decorator_name(decorator) in _ROUTE_DECORATORS
                for decorator in node.decorator_list
            ):
                reasons.add("fastapi_route")
    return reasons, defines_model, env_dependencies


def inspect_python_file(path: Path) -> tuple[set[str], bool]:
    try:
        reasons, defines_model, _ = _inspect_python_source(
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
        }
    if (
        cached
        and cached.get("size") == stat.st_size
        and cached.get("mtime_ns") == stat.st_mtime_ns
    ):
        return dict(cached)
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
                reasons, defines_model, env_dependencies = _inspect_python_source(
                    source, path
                )
        else:
            reasons, defines_model = set(), False
            env_dependencies = set()
    if "env_dependencies" not in locals():
        env_dependencies = set()
    return {
        "size": stat.st_size,
        "mtime_ns": stat.st_mtime_ns,
        "digest": hashlib.sha256(data).hexdigest(),
        "reasons": sorted(reasons),
        "defines_model": defines_model,
        "env_dependencies": sorted(env_dependencies),
    }


def classify_unit(unit: PluginUnit, cached: dict[str, object] | None = None) -> None:
    unit.reasons.clear()
    unit.model_files.clear()
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
    unit.file_cache = file_cache
    unit.fingerprint = fingerprint.hexdigest()
    if unit.reasons:
        unit.classification = ReloadClassification.RESTART_REQUIRED
    else:
        unit.classification = ReloadClassification.HOT_RELOADABLE


def changed_model_file(unit: PluginUnit, changed: set[Path]) -> bool:
    return bool(unit.model_files & changed)
