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


def inspect_python_file(path: Path) -> tuple[set[str], bool]:
    reasons: set[str] = set()
    defines_model = False
    try:
        source = path.read_text(encoding="utf-8")
        tree = ast.parse(source, filename=str(path))
    except (OSError, SyntaxError, UnicodeError):
        return {"source_unreadable"}, False

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
    return reasons, defines_model


def fingerprint_files(files: set[Path]) -> str:
    digest = hashlib.sha256()
    for path in sorted(files):
        try:
            data = path.read_bytes()
        except OSError:
            continue
        digest.update(str(path).encode())
        digest.update(data)
    return digest.hexdigest()


def classify_unit(unit: PluginUnit, cached: dict[str, object] | None = None) -> None:
    unit.reasons.clear()
    unit.model_files.clear()
    fingerprint = fingerprint_files(unit.files)
    if cached and cached.get("fingerprint") == fingerprint[:12]:
        classification = str(cached.get("classification", ""))
        try:
            unit.classification = ReloadClassification(classification)
        except ValueError:
            pass
        else:
            unit.reasons.update(str(item) for item in cached.get("reasons", []))
            unit.fingerprint = fingerprint
            # Model files still need to be known for per-file restart decisions.
            for path in unit.files:
                if path.suffix == ".py" and inspect_python_file(path)[1]:
                    unit.model_files.add(path)
            return
    for path in sorted(unit.files):
        try:
            path.read_bytes()
        except OSError:
            unit.reasons.add("source_unreadable")
            continue
        if path.suffix == ".py":
            reasons, defines_model = inspect_python_file(path)
            unit.reasons.update(reasons)
            if defines_model:
                unit.model_files.add(path)
                if path.name == "__init__.py":
                    unit.reasons.add("orm_model_in_package_init")
    unit.fingerprint = fingerprint
    if unit.reasons:
        unit.classification = ReloadClassification.RESTART_REQUIRED
    else:
        unit.classification = ReloadClassification.HOT_RELOADABLE


def changed_model_file(unit: PluginUnit, changed: set[Path]) -> bool:
    return bool(unit.model_files & changed)
