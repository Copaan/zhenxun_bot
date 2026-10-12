from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
import shutil
from typing import Any

from zhenxun.utils.atomic_json import read_json_locked, write_json_locked

STORE_ROOT = Path("data") / "runtime" / "nonebot-store"
LAYER_ROOT = Path("data") / "runtime" / "nonebot-site-packages"
MANIFEST_FILE = STORE_ROOT / "manifest-v1.json"
PENDING_FILE = STORE_ROOT / "pending-transaction-v1.json"
STARTUP_STATUS_FILE = STORE_ROOT / "startup-status-v1.json"
DEPENDENCY_SYNC_STATUS_FILE = STORE_ROOT / "dependency-sync-status-v1.json"
ORM_MIGRATION_STATUS_FILE = STORE_ROOT / "orm-migration-status-v1.json"
ROLLBACK_FILE = STORE_ROOT / "rollback-manifest-v1.json"
REGISTRY_CACHE_FILE = STORE_ROOT / "registry-plugins-v1.json"
REGISTRY_META_FILE = STORE_ROOT / "registry-meta-v1.json"


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
    from zhenxun.services.plugin_store.plugin_store_transaction import (
        archive_dependency_policy,
    )

    with archive_dependency_policy(value):
        write_json(PENDING_FILE, value)


def clear_pending_transaction() -> None:
    write_json(PENDING_FILE, None)


def public_manifest() -> dict[str, Any]:
    manifest = load_manifest()
    return {
        "active_generation": manifest.get("active_generation"),
        "pending_verification": bool(manifest.get("pending_verification")),
        "plugins": deepcopy(manifest.get("plugins", {})),
    }


def dependency_sync_status() -> dict[str, Any]:
    value = read_json(DEPENDENCY_SYNC_STATUS_FILE, {})
    return value if isinstance(value, dict) else {}


def save_dependency_sync_status(value: dict[str, Any]) -> None:
    write_json(
        DEPENDENCY_SYNC_STATUS_FILE,
        {**value, "updated_at": utc_now()},
    )
