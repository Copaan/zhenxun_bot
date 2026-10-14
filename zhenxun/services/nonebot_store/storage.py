from __future__ import annotations

from contextlib import suppress
from copy import deepcopy
from datetime import datetime, timezone
from hashlib import sha256
from pathlib import Path
import shutil
from typing import Any
import uuid

from zhenxun.utils.atomic_json import read_json_locked, write_json_locked

STORE_ROOT = Path("data") / "runtime" / "nonebot-store"
LAYER_ROOT = Path("data") / "runtime" / "nonebot-site-packages"
MANIFEST_FILE = STORE_ROOT / "manifest-v1.json"
PENDING_FILE = STORE_ROOT / "pending-transaction-v1.json"
STARTUP_STATUS_FILE = STORE_ROOT / "startup-status-v1.json"
DEPENDENCY_SYNC_STATUS_FILE = STORE_ROOT / "dependency-sync-status-v1.json"
ROLLBACK_FILE = STORE_ROOT / "rollback-manifest-v1.json"
REGISTRY_CACHE_FILE = STORE_ROOT / "registry-plugins-v1.json"
REGISTRY_META_FILE = STORE_ROOT / "registry-meta-v1.json"
SETTLED_LIMIT = 20


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def read_json(path: Path, default: Any) -> Any:
    return read_json_locked(path, default)


def write_json(path: Path, value: Any) -> None:
    write_json_locked(path, value)


def default_manifest() -> dict[str, Any]:
    return {
        "version": 1,
        "active_generation": None,
        "previous_generation": None,
        "pending_verification": False,
        "plugins": {},
        "packages": {},
        "manual_requirements": {},
        "updated_at": utc_now(),
    }


def load_manifest() -> dict[str, Any]:
    manifest = read_json(MANIFEST_FILE, default_manifest())
    if not isinstance(manifest, dict) or manifest.get("version") != 1:
        return default_manifest()
    manifest.setdefault("plugins", {})
    manifest.setdefault("packages", {})
    manifest.setdefault("manual_requirements", {})
    manifest.setdefault("active_generation", None)
    manifest.setdefault("previous_generation", None)
    manifest.setdefault("pending_verification", False)
    return manifest


def save_manifest(manifest: dict[str, Any]) -> None:
    manifest = deepcopy(manifest)
    manifest["version"] = 1
    manifest["updated_at"] = utc_now()
    write_json(MANIFEST_FILE, manifest)


def generation_path(generation: int | str) -> Path:
    return LAYER_ROOT / f"generation-{generation}"


def next_generation(
    manifest: dict[str, Any] | None = None, layer_root: Path = LAYER_ROOT
) -> int:
    """计算下一个依赖层编号，取已有目录与清单当前层中的最大值加一

    参数:
        manifest: 依赖层清单，默认读取当前清单
        layer_root: 依赖层根目录

    返回:
        int: 新依赖层编号
    """
    manifest = load_manifest() if manifest is None else manifest
    generations = []
    for path in layer_root.glob("generation-*"):
        try:
            generations.append(int(path.name.rsplit("-", 1)[1]))
        except (IndexError, ValueError):
            continue
    active = manifest.get("active_generation")
    if isinstance(active, int):
        generations.append(active)
    return max(generations, default=0) + 1


_OUTCOME_KEYS = (
    "error",
    "error_code",
    "failure_reasons",
    "rollback_confirmed",
    "cleanup_error",
    "verification_confirmed",
)


def draft_transaction(
    pending: dict[str, Any] | None, target_manifest: dict[str, Any], **fields: Any
) -> dict[str, Any]:
    """在待应用事务上生成新的事务修订，没有待应用事务时以当前清单为基线

    参数:
        pending: 当前待应用事务
        target_manifest: 目标清单
        fields: 覆盖的事务字段

    返回:
        dict[str, Any]: 尚未保存的事务
    """
    now = utc_now()
    if pending:
        transaction = {
            key: deepcopy(value)
            for key, value in pending.items()
            if key not in _OUTCOME_KEYS
        }
    else:
        transaction = {
            "version": 2,
            "base_manifest": load_manifest(),
            "operations": [],
            "database_migration_possible": False,
            "database_migration_confirmed": False,
            "database_type": "none",
            "created_at": now,
        }
    transaction.update(
        revision=uuid.uuid4().hex,
        updated_at=now,
        target_manifest=deepcopy(target_manifest),
        **fields,
    )
    return transaction


def defer_transaction(transaction: dict[str, Any]) -> None:
    """保存推迟到重启后构建的事务，并移除已失效的候选层

    参数:
        transaction: 待保存的事务
    """
    generation = transaction.pop("generation", None)
    transaction.pop("generation_digest", None)
    transaction["native_extensions"] = []
    transaction["state"] = "pending_restart"
    save_pending_transaction(transaction)
    remove_generation(generation)


def remove_generation(generation: int | str | None) -> None:
    if generation is None:
        return
    path = generation_path(generation)
    if path.is_dir():
        shutil.rmtree(path)


def prune_generations(keep: set[int]) -> None:
    """Remove generations that can no longer be activated or rolled back to."""
    if not LAYER_ROOT.is_dir():
        return
    for path in LAYER_ROOT.glob("generation-*"):
        try:
            generation = int(path.name.rsplit("-", 1)[1])
        except (IndexError, ValueError):
            continue
        if generation not in keep and path.is_dir():
            shutil.rmtree(path)


def pending_transaction() -> dict[str, Any] | None:
    value = read_json(PENDING_FILE, None)
    if not isinstance(value, dict):
        return None
    if value.get("state") == "verification_pending" and value.get("revision"):
        manifest = load_manifest()
        if (
            manifest.get("verified_transaction_revision") == value["revision"]
            and manifest.get("active_generation") == value.get("generation")
            and not manifest.get("pending_verification")
        ):
            value.update(
                state="cleanup_pending",
                verification_confirmed=True,
                cleanup_error={"code": "dependency_commit_receipt_pending"},
            )
    return value


def save_pending_transaction(value: dict[str, Any]) -> None:
    write_json(PENDING_FILE, value)


def clear_pending_transaction() -> None:
    write_json(PENDING_FILE, None)


def remove_orm_backups() -> None:
    """删除依赖层事务结束后不再需要的数据库迁移备份"""
    root = STORE_ROOT / "orm-backups"
    if root.is_dir():
        shutil.rmtree(root)


def _settled_path(revision: str) -> Path:
    return STORE_ROOT / "settled" / (sha256(revision.encode()).hexdigest() + ".json")


def is_settled(transaction: dict[str, Any] | None) -> bool:
    """判断事务结果是否已被依赖层清单确认（提交并验证，或已回滚）

    参数:
        transaction: 依赖层事务

    返回:
        bool: 已结束
    """
    if not transaction or not transaction.get("revision"):
        return False
    manifest = load_manifest()
    if manifest.get("pending_verification"):
        return False
    if transaction.get("rollback_confirmed"):
        return manifest.get("active_generation") == transaction.get(
            "base_manifest", {}
        ).get("active_generation")
    return bool(
        transaction.get("verification_confirmed")
        and manifest.get("verified_transaction_revision") == transaction["revision"]
        and manifest.get("active_generation") == transaction.get("generation")
    )


def active_transaction() -> dict[str, Any] | None:
    """读取尚未结束的依赖层事务

    返回:
        dict[str, Any] | None: 事务，已结束或不存在时为 None
    """
    value = pending_transaction()
    return None if is_settled(value) else value


def settled_transactions() -> list[dict[str, Any]]:
    """读取已归档的事务记录，最新的在前

    返回:
        list[dict[str, Any]]: 事务记录
    """
    result = []
    for path in (STORE_ROOT / "settled").glob("*.json"):
        value = read_json(path, {})
        if isinstance(value, dict) and value.get("revision"):
            result.append(value)
    return sorted(result, key=lambda value: value.get("settled_at", ""), reverse=True)


def archive_settled() -> dict[str, Any] | None:
    """归档已结束的事务并释放事务槽，只保留最近的若干条记录

    返回:
        dict[str, Any] | None: 归档的事务，事务未结束时为 None
    """
    transaction = pending_transaction()
    if not transaction or not is_settled(transaction):
        return None
    record = {**deepcopy(transaction), "settled_at": utc_now()}
    write_json(_settled_path(record["revision"]), record)
    for old in settled_transactions()[SETTLED_LIMIT:]:
        _settled_path(old["revision"]).unlink(missing_ok=True)
    clear_pending_transaction()
    return record


def dependency_sync_status() -> dict[str, Any]:
    value = read_json(DEPENDENCY_SYNC_STATUS_FILE, {})
    return value if isinstance(value, dict) else {}


def save_dependency_sync_status(value: dict[str, Any]) -> None:
    write_json(
        DEPENDENCY_SYNC_STATUS_FILE,
        {**value, "updated_at": utc_now()},
    )


def _released_dependencies(
    target: dict[str, Any],
    overrides: dict[str, list[str] | None],
    removed_paths: list[Path],
    added_paths: list[Path],
) -> tuple[list[str], list[dict[str, Any]]]:
    """计算商店插件依赖变化后可释放的包，分析失败时不释放

    参数:
        target: 依赖层清单
        overrides: 按 store_key 的依赖声明覆盖
        removed_paths: 将移除的源码路径
        added_paths: 将加入的源码路径

    返回:
        tuple[list[str], list[dict[str, Any]]]: 可释放的包，因仍被导入而保留的包
    """
    from zhenxun.services.plugin_store.plugin_store_receipts import (
        receipt_dependency_inputs,
    )

    from .dependencies import DependencyAnalysisError
    from .environment import released_packages

    before = receipt_dependency_inputs()
    if not any(key in before for key in overrides):
        return [], []
    try:
        return released_packages(
            target,
            target,
            sources_before=before,
            sources_after=receipt_dependency_inputs(overrides),
            removed_paths=removed_paths,
            added_paths=added_paths,
        )
    except (DependencyAnalysisError, OSError, KeyError, TypeError):
        return [], []


def merge_source_dependencies(
    layer_packages: dict[str, str],
    overrides: dict[str, list[str] | None],
    removed_paths: list[Path],
    added_paths: list[Path],
    *,
    source: dict[str, Any] | None,
) -> dict[str, Any] | str | None:
    """把商店插件的新增依赖与可释放依赖合并进依赖层事务

    参数:
        layer_packages: 需要写入依赖层的包版本
        overrides: 按 store_key 的依赖声明覆盖
        removed_paths: 将移除的源码路径
        added_paths: 将加入的源码路径
        source: 商店插件源码事务

    返回:
        dict[str, Any] | str | None: 写入的事务，失败时为错误码，无变化时为 None
    """
    current = pending_transaction()
    if current and current.get("state") != "pending_restart":
        return "cross_store_transaction_not_mutable" if layer_packages else None
    base = deepcopy(current.get("base_manifest") if current else load_manifest())
    target = deepcopy(current.get("target_manifest") if current else base) or {}
    released, retained = _released_dependencies(
        target, overrides, removed_paths, added_paths
    )
    released = [name for name in released if name not in layer_packages]
    if not layer_packages and not released:
        return None
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
            return "cross_store_dependency_conflict"
        target_packages[name] = {"version": version}
    for name in released:
        target_packages.pop(name, None)

    source_build_confirmed = any(
        operation.get("source_build_confirmed")
        for operation in (source or {}).get("operations", [])
    )
    transaction = draft_transaction(
        current,
        target,
        source_build_confirmed=bool(
            (current or {}).get("source_build_confirmed") or source_build_confirmed
        ),
        state="pending_restart",
        **({"revision": current["revision"]} if current else {"base_manifest": base}),
    )
    if source is not None:
        transaction["source_transaction_revision"] = source.get("revision")
    transaction["released_packages"] = sorted(
        set(transaction.get("released_packages") or []) | set(released)
    )
    if retained:
        transaction["retained_imports"] = retained
    save_pending_transaction(transaction)
    return transaction


def release_source_dependencies(
    overrides: dict[str, list[str] | None], removed_paths: list[Path] | None = None
) -> list[str]:
    """商店插件热更新或热卸载后，将不再需要的依赖登记到待应用的依赖层事务

    依赖层在下次重启时重建，不会主动请求重启；分析失败或已有事务不可修改时跳过。

    参数:
        overrides: 按 store_key 的依赖声明覆盖，None 表示移除
        removed_paths: 已移除的源码路径

    返回:
        list[str]: 登记释放的包
    """
    with suppress(Exception):
        transaction = merge_source_dependencies(
            {}, overrides, removed_paths or [], [], source=None
        )
        if isinstance(transaction, dict):
            return list(transaction.get("released_packages") or [])
    return []
