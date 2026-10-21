from __future__ import annotations

import ast
from datetime import datetime, timezone
from hashlib import sha256
from pathlib import Path
from typing import Any

from zhenxun.utils.atomic_json import (
    mutate_json_locked,
    read_json_locked,
    write_json_locked,
)

_RECEIPT_FILE = Path("data/runtime/plugin-store-receipts-v1.json")


def source_digest(path: Path, *, python_only: bool = False) -> str | None:
    """计算插件源码摘要

    参数:
        path: 插件目录或文件
        python_only: 只计入 .py/.pyi 文件

    返回:
        str | None: 摘要，路径不存在或无法读取时为 None
    """
    if not path.exists():
        return None
    files = (
        [path]
        if path.is_file()
        else sorted(item for item in path.rglob("*") if item.is_file())
    )
    digest = sha256()
    root = path.parent if path.is_file() else path
    try:
        for item in files:
            if "__pycache__" in item.parts or item.suffix in {".pyc", ".pyo"}:
                continue
            if python_only and item.suffix not in {".py", ".pyi"}:
                continue
            digest.update(item.relative_to(root).as_posix().encode("utf-8"))
            digest.update(b"\0")
            digest.update(item.read_bytes())
            digest.update(b"\0")
    except OSError:
        return None
    return digest.hexdigest()


class StoreReceiptStore:
    @staticmethod
    def load() -> dict[str, dict[str, Any]]:
        data = read_json_locked(_RECEIPT_FILE, {})
        return data if isinstance(data, dict) else {}

    @staticmethod
    def save(data: dict[str, dict[str, Any]]) -> None:
        write_json_locked(_RECEIPT_FILE, data)

    @classmethod
    def set(cls, store_key: str, receipt: dict[str, Any]) -> None:
        def update(data: dict[str, dict[str, Any]]) -> None:
            data[store_key] = receipt

        mutate_json_locked(_RECEIPT_FILE, {}, update)

    @classmethod
    def delete(cls, store_key: str) -> None:
        mutate_json_locked(
            _RECEIPT_FILE,
            {},
            lambda data: data.pop(store_key, None),
        )


def receipt_dependency_inputs(
    overrides: dict[str, list[str] | None] | None = None,
) -> dict[str, list[str]]:
    """读取商店插件回执中记录的依赖声明，源码目录已被删除的本地回执不再计入

    参数:
        overrides: 按 store_key 覆盖的依赖声明，None 表示移除

    返回:
        dict[str, list[str]]: store_key 到依赖声明的映射
    """
    result = {
        key: [str(value) for value in receipt["dependency_inputs"]]
        for key, receipt in StoreReceiptStore.load().items()
        if isinstance(receipt, dict)
        and isinstance(receipt.get("dependency_inputs"), list)
        and (not receipt.get("local_path") or Path(receipt["local_path"]).exists())
    }
    for key, values in (overrides or {}).items():
        if values is None:
            result.pop(key, None)
        else:
            result[key] = [str(value) for value in values]
    return result


def artifact_version(path: Path) -> str | None:
    """从插件元数据中静态读取插件版本

    参数:
        path: 插件源码路径

    返回:
        str | None: 插件版本
    """
    entrypoint = path / "__init__.py" if path.is_dir() else path
    try:
        tree = ast.parse(entrypoint.read_text(encoding="utf-8"))
    except (OSError, SyntaxError, UnicodeError):
        return None
    metadata_values = []
    for node in tree.body:
        if isinstance(node, ast.Assign) and any(
            isinstance(target, ast.Name) and target.id == "__plugin_meta__"
            for target in node.targets
        ):
            metadata_values.append(node.value)
        elif (
            isinstance(node, ast.AnnAssign)
            and isinstance(node.target, ast.Name)
            and node.target.id == "__plugin_meta__"
            and node.value is not None
        ):
            metadata_values.append(node.value)
    for metadata in metadata_values:
        for node in ast.walk(metadata):
            if not isinstance(node, ast.Call):
                continue
            name = (
                node.func.id
                if isinstance(node.func, ast.Name)
                else node.func.attr
                if isinstance(node.func, ast.Attribute)
                else ""
            )
            if name != "PluginExtraData":
                continue
            keyword = next(
                (item for item in node.keywords if item.arg == "version"), None
            )
            if keyword is None:
                continue
            try:
                value = ast.literal_eval(keyword.value)
            except (ValueError, TypeError):
                continue
            if isinstance(value, str) and value.strip():
                return value.strip()
    return None


def store_receipt(
    *,
    source: str,
    plugin_info: Any,
    runtime_module: str,
    path: Path,
    dependency_inputs: list[str] | None = None,
) -> dict[str, Any]:
    """生成商店插件安装回执

    参数:
        source: 插件来源
        plugin_info: 插件信息
        runtime_module: 运行时模块名
        path: 插件源码路径
        dependency_inputs: 插件声明的依赖

    返回:
        dict[str, Any]: 安装回执
    """
    receipt = {
        "source": source,
        "module": plugin_info.module,
        "module_path": plugin_info.module_path,
        "runtime_module": runtime_module,
        "installed_version": artifact_version(path),
        "source_digest": source_digest(path),
        "installed_at": datetime.now(timezone.utc).isoformat(),
    }
    if dependency_inputs is not None:
        receipt["dependency_inputs"] = sorted(set(dependency_inputs))
    return receipt


__all__ = [
    "StoreReceiptStore",
    "artifact_version",
    "receipt_dependency_inputs",
    "source_digest",
    "store_receipt",
]
