from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
import json
from pathlib import Path
import shutil
from typing import Any

STORE_ROOT = Path("data") / "runtime" / "nonebot-store"
LAYER_ROOT = Path("data") / "runtime" / "nonebot-site-packages"
MANIFEST_FILE = STORE_ROOT / "manifest-v1.json"
PENDING_FILE = STORE_ROOT / "pending-transaction-v1.json"
STARTUP_STATUS_FILE = STORE_ROOT / "startup-status-v1.json"
ROLLBACK_FILE = STORE_ROOT / "rollback-manifest-v1.json"
REGISTRY_CACHE_FILE = STORE_ROOT / "registry-plugins-v1.json"
REGISTRY_META_FILE = STORE_ROOT / "registry-meta-v1.json"


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def read_json(path: Path, default: Any) -> Any:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError, TypeError):
        return deepcopy(default)
    return value


def write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp")
    temporary.write_text(
        json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    temporary.replace(path)


def default_manifest() -> dict[str, Any]:
    return {
        "version": 1,
        "active_generation": None,
        "previous_generation": None,
        "pending_verification": False,
        "plugins": {},
        "packages": {},
        "updated_at": utc_now(),
    }


def load_manifest() -> dict[str, Any]:
    manifest = read_json(MANIFEST_FILE, default_manifest())
    if not isinstance(manifest, dict) or manifest.get("version") != 1:
        return default_manifest()
    manifest.setdefault("plugins", {})
    manifest.setdefault("packages", {})
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


def next_generation(manifest: dict[str, Any] | None = None) -> int:
    manifest = manifest or load_manifest()
    generations = []
    for path in LAYER_ROOT.glob("generation-*"):
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
        shutil.rmtree(path, ignore_errors=True)


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
            shutil.rmtree(path, ignore_errors=True)


def pending_transaction() -> dict[str, Any] | None:
    value = read_json(PENDING_FILE, None)
    return value if isinstance(value, dict) else None


def save_pending_transaction(value: dict[str, Any]) -> None:
    write_json(PENDING_FILE, value)


def clear_pending_transaction() -> None:
    PENDING_FILE.unlink(missing_ok=True)


def public_manifest() -> dict[str, Any]:
    manifest = load_manifest()
    return {
        "active_generation": manifest.get("active_generation"),
        "pending_verification": bool(manifest.get("pending_verification")),
        "plugins": deepcopy(manifest.get("plugins", {})),
    }
