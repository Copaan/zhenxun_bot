from __future__ import annotations

from collections.abc import Callable, Iterator
from contextlib import contextmanager
from copy import deepcopy
from datetime import datetime, timezone
from hashlib import sha256
import os
from pathlib import Path
import shutil
import stat
from typing import Any
import uuid

from filelock import FileLock, Timeout

from zhenxun.utils.atomic_json import (
    AtomicJsonLockTimeout,
    read_json_locked,
    write_json_locked,
)
from zhenxun.utils.bytecode import precompile_path

ROOT = Path("data/runtime/plugin-store-transaction")
PENDING_FILE = ROOT / "pending-v1.json"
LIFECYCLE_INDEX = Path("data/runtime/lifecycle-index-v2.json")
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


def _digest(path: Path) -> str:
    if not path.exists():
        return "missing"
    digest = sha256()
    files = (
        [path]
        if path.is_file()
        else sorted(item for item in path.rglob("*") if item.is_file())
    )
    root = path.parent if path.is_file() else path
    for item in files:
        if "__pycache__" in item.parts or item.suffix in {".pyc", ".pyo"}:
            continue
        try:
            payload = item.read_bytes()
        except OSError as error:
            raise RuntimeError("plugin_transaction_source_unreadable") from error
        digest.update(item.relative_to(root).as_posix().encode())
        digest.update(b"\0")
        digest.update(payload)
        digest.update(b"\0")
    return digest.hexdigest()


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
                return {
                    "operation_id": operation_id,
                    "apply_mode": "restart_pending",
                    "transaction_revision": transaction.get("revision"),
                    "pending_operations": [
                        _public_operation(item) for item in operations
                    ],
                }
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
                "removed_operation_ids": [previous["operation_id"]],
                "pending_operations": [_public_operation(item) for item in operations],
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
                    "candidate_digest": (
                        _digest(candidate_path) if candidate_path else "missing"
                    ),
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
            "transaction_revision": transaction["revision"],
            "pending_operations": [_public_operation(item) for item in operations],
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


def _fail_pending(code: str) -> None:
    with _locked_transaction():
        transaction = _read_pending()
        if not transaction:
            return
        transaction["state"] = "failed"
        transaction["failure_reasons"] = [{"code": code}]
        transaction["updated_at"] = _now()
        _write_pending(transaction)


def prepare_dependency_transaction() -> bool:
    """Merge source-store dependency changes into the managed generation."""
    source = pending_transaction()
    if not source or source.get("state") != "pending_restart":
        return True
    packages = dependency_packages()
    if not packages:
        return True

    from packaging.utils import canonicalize_name

    from zhenxun.nonebot_store.dependencies import protected_core
    from zhenxun.nonebot_store.storage import (
        load_manifest,
        save_pending_transaction,
        utc_now,
    )
    from zhenxun.nonebot_store.storage import (
        pending_transaction as nonebot_pending,
    )

    core = protected_core()
    layer_packages: dict[str, str] = {}
    for raw_name, version in packages.items():
        name = canonicalize_name(raw_name)
        if name in core:
            if core[name] != version:
                _fail_pending("core_dependency_conflict")
                return False
            continue
        layer_packages[name] = version

    current = nonebot_pending()
    if current and current.get("state") != "pending_restart":
        _fail_pending("cross_store_transaction_not_mutable")
        return False
    base = deepcopy(current.get("base_manifest") if current else load_manifest())
    target = deepcopy(current.get("target_manifest") if current else base)
    target_packages = target.setdefault("packages", {})
    base_packages = base.get("packages", {})
    for name, version in layer_packages.items():
        existing = target_packages.get(name)
        existing_version = (
            str(existing.get("version")) if isinstance(existing, dict) else None
        )
        base_info = base_packages.get(name) if isinstance(base_packages, dict) else None
        base_version = (
            str(base_info.get("version")) if isinstance(base_info, dict) else None
        )
        if (
            existing_version is not None
            and existing_version != version
            and existing_version != base_version
        ):
            _fail_pending("cross_store_dependency_conflict")
            return False
        target_packages[name] = {"version": version}

    if current:
        transaction = deepcopy(current)
        transaction["target_manifest"] = target
        transaction["source_build_confirmed"] = bool(
            transaction.get("source_build_confirmed")
            or any(
                operation.get("source_build_confirmed")
                for operation in source.get("operations", [])
            )
        )
        transaction["source_transaction_revision"] = source.get("revision")
        transaction["updated_at"] = utc_now()
    else:
        transaction = {
            "version": 2,
            "revision": uuid.uuid4().hex,
            "base_manifest": base,
            "operations": [],
            "target_manifest": target,
            "source_build_confirmed": any(
                operation.get("source_build_confirmed")
                for operation in source.get("operations", [])
            ),
            "database_migration_confirmed": False,
            "database_migration_possible": False,
            "database_type": "none",
            "state": "pending_restart",
            "created_at": utc_now(),
            "updated_at": utc_now(),
            "source_transaction_revision": source.get("revision"),
        }
    save_pending_transaction(transaction)
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
                if operation.get("action") != "uninstall" and _digest(
                    live
                ) != operation.get("base_digest"):
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
        compile_warnings = []
        for operation in switched:
            if operation.get("action") == "uninstall":
                continue
            live = Path(str(operation["live_path"]))
            if not precompile_path(live):
                compile_warnings.append(
                    {
                        "code": "bytecode_precompile_failed",
                        "store_key": operation.get("store_key"),
                    }
                )
        if compile_warnings:
            transaction["warnings"] = compile_warnings
        transaction["updated_at"] = _now()
        _write_pending(transaction)
        return True


def startup_verification() -> tuple[bool, dict[str, Any]]:
    transaction = pending_transaction()
    if not transaction or transaction.get("state") != "verification_pending":
        return True, {"failed": []}
    index = read_json_locked(LIFECYCLE_INDEX, {})
    modules = {
        str(item.get("module"))
        for item in index.get("plugins", [])
        if isinstance(item, dict)
    }
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
        from zhenxun.plugin_store_receipts import StoreReceiptStore

        for operation in transaction.get("operations", []):
            if operation.get("action") == "uninstall":
                StoreReceiptStore.delete(str(operation["store_key"]))
            elif isinstance(operation.get("receipt"), dict):
                StoreReceiptStore.set(str(operation["store_key"]), operation["receipt"])
            _remove(ROOT / str(operation["operation_id"]))
        _write_pending(None)


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


__all__ = [
    "apply_pending_transaction",
    "cancel_operation",
    "dependency_packages",
    "finalize_pending_transaction",
    "pending_transaction",
    "prepare_dependency_transaction",
    "public_transaction",
    "rollback_pending_transaction",
    "stage_operation",
    "startup_verification",
]
