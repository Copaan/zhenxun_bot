from __future__ import annotations

from collections.abc import Callable, Iterator
from contextlib import contextmanager
from copy import deepcopy
from datetime import datetime, timezone
import os
from pathlib import Path
import shutil
import stat
from typing import Any
import uuid

from filelock import FileLock, Timeout
from packaging.utils import canonicalize_name

from zhenxun.services.plugin_store.operation_journal import settle_operations
from zhenxun.services.plugin_store.plugin_store_receipts import source_digest
from zhenxun.utils.atomic_json import (
    AtomicJsonLockTimeout,
    read_json_locked,
    write_json_locked,
)
from zhenxun.utils.bytecode import precompile_path

ROOT = Path("data/runtime/plugin-store-transaction")
PENDING_FILE = ROOT / "pending-v1.json"
LOADED_PLUGINS_INDEX = Path("data/runtime/loaded-plugins-v1.json")
_TRANSACTION_LOCK = ROOT / ".transaction.lock"


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


@contextmanager
def _locked_transaction(timeout: float = 30.0) -> Iterator[None]:
    ROOT.mkdir(parents=True, exist_ok=True)
    try:
        with FileLock(_TRANSACTION_LOCK, timeout=timeout):
            yield
    except Timeout as error:
        raise AtomicJsonLockTimeout(str(_TRANSACTION_LOCK)) from error


def _digest(path: Path, *, python_only: bool = False) -> str:
    """计算插件源码摘要，路径不存在时为 missing

    参数:
        path: 插件目录或文件
        python_only: 只计入 .py/.pyi 文件；待重启卸载期间插件仍会在目录内写数据，
            只核对源码

    返回:
        str: 摘要
    """
    if not path.exists():
        return "missing"
    digest = source_digest(path, python_only=python_only)
    if digest is None:
        raise RuntimeError("plugin_transaction_source_unreadable")
    return digest


def _copy(source: Path, target: Path) -> None:
    if target.exists():
        _remove(target)
    target.parent.mkdir(parents=True, exist_ok=True)
    if source.is_dir():
        shutil.copytree(source, target)
    else:
        shutil.copy2(source, target)


def _win_on_rm_error(func: Callable[[str], Any], path: str, _exc_info: object) -> None:
    os.chmod(path, stat.S_IWRITE)
    func(path)


def _remove(path: Path) -> None:
    if path.is_dir():
        shutil.rmtree(path, onerror=_win_on_rm_error)
    else:
        path.unlink(missing_ok=True)


def _read_pending() -> dict[str, Any] | None:
    value = read_json_locked(PENDING_FILE, None, quarantine_corrupt=True)
    return value if isinstance(value, dict) else None


def _write_pending(value: dict[str, Any] | None) -> None:
    write_json_locked(PENDING_FILE, value)


def pending_transaction() -> dict[str, Any] | None:
    return _read_pending()


def _public_operation(operation: dict[str, Any]) -> dict[str, Any]:
    return {
        key: operation.get(key)
        for key in (
            "operation_id",
            "action",
            "store_key",
            "module",
            "runtime_module",
            "reason",
            "created_at",
        )
    }


def public_transaction() -> dict[str, Any] | None:
    transaction = pending_transaction()
    if not transaction:
        return None
    return {
        "revision": transaction.get("revision"),
        "state": transaction.get("state"),
        "operations": [
            _public_operation(item) for item in transaction.get("operations", [])
        ],
        "failure_reasons": deepcopy(transaction.get("failure_reasons", [])),
        "created_at": transaction.get("created_at"),
        "updated_at": transaction.get("updated_at"),
    }


def _workspace_relative(path: Path) -> str:
    try:
        return path.resolve().relative_to(Path.cwd().resolve()).as_posix()
    except ValueError as error:
        raise RuntimeError("plugin_transaction_path_outside_workspace") from error


def stage_operation(
    *,
    action: str,
    store_key: str,
    module: str,
    runtime_module: str,
    live_path: Path,
    candidate_path: Path | None,
    base_source_path: Path | None = None,
    base_digest: str | None = None,
    receipt: dict[str, Any] | None,
    reason: str,
    dependency_inputs: list[str] | None = None,
    dependency_packages: dict[str, str] | None = None,
    source_build_confirmed: bool = False,
    operation_id: str | None = None,
) -> dict[str, Any]:
    if action not in {"install", "update", "uninstall"}:
        raise RuntimeError("plugin_transaction_action_invalid")
    with _locked_transaction():
        transaction = _read_pending()
        if transaction and transaction.get("state") != "pending_restart":
            raise RuntimeError("plugin_transaction_not_mutable")
        if transaction is None:
            transaction = {
                "version": 1,
                "created_at": _now(),
                "operations": [],
                "state": "pending_restart",
            }
        operations = list(transaction.get("operations", []))
        if operation_id:
            existing_id = next(
                (
                    item
                    for item in operations
                    if item.get("operation_id") == operation_id
                ),
                None,
            )
            if existing_id:
                if existing_id.get("store_key") != store_key:
                    raise RuntimeError("plugin_operation_id_conflict")
                return {"operation_id": operation_id, "apply_mode": "restart_pending"}
        previous_index = next(
            (
                index
                for index, item in enumerate(operations)
                if item.get("store_key") == store_key
            ),
            None,
        )
        previous = operations[previous_index] if previous_index is not None else None
        if previous and previous.get("action") == "install" and action == "uninstall":
            _remove(ROOT / str(previous["operation_id"]))
            operations.pop(previous_index)
            if not operations:
                _write_pending(None)
            else:
                transaction["operations"] = operations
                transaction["revision"] = uuid.uuid4().hex
                transaction["updated_at"] = _now()
                _write_pending(transaction)
            return {
                "operation_id": previous["operation_id"],
                "apply_mode": "rolled_back",
            }
        else:
            operation_id = operation_id or uuid.uuid4().hex
            bundle = ROOT / operation_id
            old_path = bundle / "old"
            new_path = bundle / "new"
            old_source = base_source_path if base_source_path is not None else live_path
            try:
                if old_source.exists():
                    _copy(old_source, old_path)
                if candidate_path is not None and candidate_path.exists():
                    _copy(candidate_path, new_path)
                operation = {
                    "operation_id": operation_id,
                    "action": action,
                    "store_key": store_key,
                    "module": module,
                    "runtime_module": runtime_module,
                    "live_path": _workspace_relative(live_path),
                    "base_digest": base_digest or _digest(old_source),
                    "base_source_digest": _digest(old_source, python_only=True),
                    "receipt": receipt,
                    "reason": reason,
                    "dependency_inputs": sorted(set(dependency_inputs or [])),
                    "dependency_packages": dict(
                        sorted((dependency_packages or {}).items())
                    ),
                    "source_build_confirmed": bool(source_build_confirmed),
                    "created_at": _now(),
                }
            except Exception:
                _remove(bundle)
                raise
            if previous_index is None:
                operations.append(operation)
            else:
                _remove(ROOT / str(previous["operation_id"]))
                operations[previous_index] = operation
        transaction["operations"] = operations
        transaction["revision"] = uuid.uuid4().hex
        transaction["updated_at"] = _now()
        transaction["state"] = "pending_restart"
        _write_pending(transaction)
        return {
            "operation_id": operation["operation_id"],
            "apply_mode": "restart_pending",
        }


def cancel_operation(operation_id: str | None = None) -> dict[str, Any]:
    with _locked_transaction():
        transaction = _read_pending()
        if not transaction or transaction.get("state") not in {
            "pending_restart",
            "failed",
        }:
            raise RuntimeError("plugin_transaction_not_cancelable")
        operations = list(transaction.get("operations", []))
        if operation_id is None:
            removed = operations
            operations = []
        else:
            removed = [
                item for item in operations if item.get("operation_id") == operation_id
            ]
            operations = [
                item for item in operations if item.get("operation_id") != operation_id
            ]
            if not removed:
                raise RuntimeError("plugin_transaction_operation_not_found")
        for item in removed:
            _remove(ROOT / str(item["operation_id"]))
        if not operations:
            _write_pending(None)
        else:
            transaction["operations"] = operations
            transaction["revision"] = uuid.uuid4().hex
            transaction["updated_at"] = _now()
            transaction["state"] = "pending_restart"
            transaction.pop("failure_reasons", None)
            _write_pending(transaction)
        settle_operations(
            [str(item["operation_id"]) for item in removed], "canceled", "rolled_back"
        )
        return {
            "apply_mode": "rolled_back",
            "removed_operation_ids": [item["operation_id"] for item in removed],
        }


def dependency_packages() -> dict[str, str]:
    transaction = pending_transaction() or {}
    packages: dict[str, str] = {}
    for operation in transaction.get("operations", []):
        for name, version in operation.get("dependency_packages", {}).items():
            existing = packages.get(str(name))
            if existing is not None and existing != str(version):
                raise RuntimeError("cross_store_dependency_conflict")
            packages[str(name)] = str(version)
    return packages


def fail_pending_transaction(code: str) -> None:
    """将待重启的源码事务标记为失败

    参数:
        code: 失败原因
    """
    with _locked_transaction():
        transaction = _read_pending()
        if not transaction or transaction.get("state") != "pending_restart":
            return
        transaction["state"] = "failed"
        transaction["failure_reasons"] = [{"code": code}]
        transaction["updated_at"] = _now()
        _write_pending(transaction)
        settle_operations(
            [str(item["operation_id"]) for item in transaction.get("operations", [])],
            "failed",
            "failed",
            code,
        )


def prepare_dependency_transaction() -> bool:
    """Merge source-store dependency changes into the managed generation."""
    source = pending_transaction()
    if not source or source.get("state") != "pending_restart":
        return True
    packages = dependency_packages()
    layer_packages: dict[str, str] = {}
    if packages:
        from zhenxun.services.nonebot_store.dependencies import protected_core

        core = protected_core()
        for raw_name, version in packages.items():
            name = canonicalize_name(raw_name)
            if name in core:
                if core[name] != version:
                    fail_pending_transaction("core_dependency_conflict")
                    return False
                continue
            layer_packages[name] = version
    from zhenxun.services.nonebot_store.storage import merge_source_dependencies

    result = merge_source_dependencies(layer_packages, source=source)
    if isinstance(result, str):
        fail_pending_transaction(result)
        return False
    return True


def apply_pending_transaction() -> bool:
    with _locked_transaction():
        transaction = _read_pending()
        if transaction and transaction.get("state") == "applying":
            _restore_operations(transaction)
            transaction["state"] = "failed"
            transaction["failure_reasons"] = [{"code": "plugin_apply_interrupted"}]
            transaction["updated_at"] = _now()
            _write_pending(transaction)
            return False
        if not transaction or transaction.get("state") != "pending_restart":
            return False
        switched: list[dict[str, Any]] = []
        try:
            for operation in transaction.get("operations", []):
                live = Path(str(operation["live_path"]))
                if operation.get("action") != "uninstall":
                    if _digest(live) != operation.get("base_digest"):
                        raise RuntimeError("plugin_transaction_stale")
                    continue
                # A staged uninstall cannot demand a whole-tree match: the plugin
                # is still running and writes its own state inside ``live``. It is
                # checked against its source digest instead, which ignores that
                # churn but still refuses to delete code that was replaced after
                # the operation was reviewed. A live path that is already gone has
                # nothing left to delete, and failing it here would roll the
                # plugin back into place -- the opposite of what was asked.
                expected_source = operation.get("base_source_digest")
                if expected_source is None:
                    continue
                current_source = _digest(live, python_only=True)
                if current_source not in {expected_source, "missing"}:
                    raise RuntimeError("plugin_transaction_stale")
            transaction["state"] = "applying"
            _write_pending(transaction)
            for operation in transaction.get("operations", []):
                live = Path(str(operation["live_path"]))
                new_path = ROOT / str(operation["operation_id"]) / "new"
                switched.append(operation)
                _remove(live)
                if operation.get("action") != "uninstall":
                    _copy(new_path, live)
        except BaseException as error:
            for operation in reversed(switched):
                live = Path(str(operation["live_path"]))
                old_path = ROOT / str(operation["operation_id"]) / "old"
                _remove(live)
                if old_path.exists():
                    _copy(old_path, live)
            transaction["state"] = "failed"
            transaction["failure_reasons"] = [{"code": str(error)}]
            transaction["updated_at"] = _now()
            _write_pending(transaction)
            if not isinstance(error, Exception):
                raise
            return False
        transaction["state"] = "verification_pending"
        for operation in switched:
            if operation.get("action") != "uninstall":
                precompile_path(Path(str(operation["live_path"])))
        transaction["updated_at"] = _now()
        _write_pending(transaction)
        return True


def startup_verification() -> tuple[bool, dict[str, Any]]:
    transaction = pending_transaction()
    if not transaction or transaction.get("state") != "verification_pending":
        return True, {"failed": []}
    index = read_json_locked(LOADED_PLUGINS_INDEX, {})
    modules = {str(module) for module in index.get("modules", [])}
    failures = []
    for operation in transaction.get("operations", []):
        runtime_module = str(operation.get("runtime_module") or "")
        loaded = runtime_module in modules
        expected = operation.get("action") != "uninstall"
        if loaded != expected:
            failures.append(
                {
                    "store_key": operation.get("store_key"),
                    "code": "plugin_startup_verification_failed",
                }
            )
    return not failures, {"failed": failures}


def finalize_pending_transaction() -> None:
    with _locked_transaction():
        transaction = _read_pending()
        if not transaction or transaction.get("state") != "verification_pending":
            return
        from zhenxun.services.plugin_store.plugin_store_receipts import (
            StoreReceiptStore,
        )

        for operation in transaction.get("operations", []):
            if operation.get("action") == "uninstall":
                StoreReceiptStore.delete(str(operation["store_key"]))
            elif isinstance(operation.get("receipt"), dict):
                StoreReceiptStore.set(str(operation["store_key"]), operation["receipt"])
            _remove(ROOT / str(operation["operation_id"]))
        _write_pending(None)
        settle_operations(
            [str(item["operation_id"]) for item in transaction.get("operations", [])],
            "completed",
            "applied",
        )


def _restore_operations(transaction: dict[str, Any]) -> None:
    for operation in reversed(transaction.get("operations", [])):
        live = Path(str(operation["live_path"]))
        old_path = ROOT / str(operation["operation_id"]) / "old"
        _remove(live)
        if old_path.exists():
            _copy(old_path, live)


def rollback_pending_transaction(
    failures: list[dict[str, Any]] | None = None,
) -> None:
    with _locked_transaction():
        transaction = _read_pending()
        if not transaction or transaction.get("state") != "verification_pending":
            return
        _restore_operations(transaction)
        transaction["state"] = "failed"
        transaction["failure_reasons"] = failures or [
            {"code": "plugin_startup_verification_failed"}
        ]
        transaction["updated_at"] = _now()
        _write_pending(transaction)
        settle_operations(
            [str(item["operation_id"]) for item in transaction.get("operations", [])],
            "failed",
            "failed",
            str(transaction["failure_reasons"][0].get("code") or ""),
        )


__all__ = [
    "apply_pending_transaction",
    "cancel_operation",
    "dependency_packages",
    "fail_pending_transaction",
    "finalize_pending_transaction",
    "pending_transaction",
    "prepare_dependency_transaction",
    "public_transaction",
    "rollback_pending_transaction",
    "stage_operation",
    "startup_verification",
]


async def preflight_source_requirements(
    files: list[Path], *, store_key: str = "", allow_build: bool = False
) -> dict[str, Any]:
    """按依赖层规则预检商店插件的 requirements 文件

    与其他依赖来源（包括已安装和待应用的商店插件）一起解析，确认可解析、不改动
    核心依赖与已安装的 ORM 包。

    参数:
        files: 插件源码中的 requirements 文件
        store_key: 商店插件标识，该插件原有的依赖声明被本次声明替换
        allow_build: 是否允许解析时构建源码包读取依赖信息

    返回:
        dict[str, Any]: 依赖层中新增或变化的包版本、包变化与是否需要源码构建

    异常:
        DependencyAnalysisError: 依赖声明无效、环境漂移、与核心依赖冲突、需要
            安装工具或会改变已安装的 ORM 包
    """
    from zhenxun.services.nonebot_store import dependencies
    from zhenxun.services.nonebot_store.dependencies import DependencyAnalysisError
    from zhenxun.services.nonebot_store.environment import capture_environment
    from zhenxun.services.nonebot_store.storage import (
        pending_transaction as layer_transaction,
    )
    from zhenxun.services.plugin_store.plugin_store_receipts import (
        receipt_dependency_inputs,
    )

    requirements = dependencies.read_requirement_files(files)
    if not requirements:
        return {
            "resolved_packages": {},
            "package_changes": {"added": [], "changed": [], "removed": []},
            "candidate_inputs": [],
            "source_build_required": False,
        }
    if dependencies.environment_drift():
        raise DependencyAnalysisError("environment_drift")
    tools = sorted(
        {dependencies.parse_requirement(raw).name for raw in requirements}
        & dependencies.FORBIDDEN_LAYER_PACKAGES
    )
    if tools:
        raise DependencyAnalysisError(
            "dependency_requirement_unsupported", "、".join(tools)
        )
    overrides: dict[str, list[str] | None] = {}
    for operation in (pending_transaction() or {}).get("operations", []):
        key = str(operation.get("store_key") or "")
        overrides[key] = (
            None
            if operation.get("action") == "uninstall"
            else [str(raw) for raw in operation.get("dependency_inputs") or []]
        )
    if store_key:
        overrides[store_key] = list(requirements)
    pending = layer_transaction() or {}
    manifest = (
        pending.get("target_manifest")
        if pending.get("state") == "pending_restart"
        else None
    ) or dependencies.load_manifest()
    result = await dependencies.resolve_layer(
        requirements,
        snapshot=capture_environment(),
        manifest=manifest,
        sources=receipt_dependency_inputs(overrides),
        allow_build=allow_build,
    )
    changes = [
        item
        for item in result["changes"]
        if item["to"] and item["name"] in result["resolved"]
    ]
    return {
        "resolved_packages": {item["name"]: item["to"] for item in changes},
        "package_changes": {
            "added": [
                {"name": item["name"], "version": item["to"]}
                for item in changes
                if not item["from"]
            ],
            "changed": [item for item in changes if item["from"]],
            "removed": [],
        },
        "candidate_inputs": list(requirements),
        "source_build_required": result["source_build_required"],
    }
