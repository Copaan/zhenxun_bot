from __future__ import annotations

from copy import deepcopy
from hashlib import sha256
import importlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
from typing import Any

from packaging.utils import canonicalize_name

from .dependencies import protected_core, safe_process_error
from .storage import (
    LAYER_ROOT,
    PENDING_FILE,
    ROLLBACK_FILE,
    STARTUP_STATUS_FILE,
    clear_pending_transaction,
    generation_path,
    load_manifest,
    next_generation,
    pending_transaction,
    prune_generations,
    read_json,
    remove_generation,
    save_manifest,
    utc_now,
    write_json,
)


class LayerBuildError(RuntimeError):
    def __init__(self, code: str, detail: str | None = None):
        super().__init__(detail or code)
        self.code = code


def _active_path(manifest: dict[str, Any] | None = None) -> Path | None:
    manifest = manifest or load_manifest()
    generation = manifest.get("active_generation")
    if not isinstance(generation, int):
        return None
    path = generation_path(generation).resolve()
    return path if path.is_dir() else None


def activate_current_generation() -> Path | None:
    """Put exactly one managed dependency generation at the front of sys.path."""
    layer_root = LAYER_ROOT.resolve()
    sys.path[:] = [item for item in sys.path if not _is_layer_path(item, layer_root)]
    path = _active_path()
    if path is not None:
        sys.path.insert(0, str(path))
        importlib.invalidate_caches()
    return path


def _is_layer_path(value: str, layer_root: Path) -> bool:
    try:
        Path(value).resolve().relative_to(layer_root)
    except (OSError, ValueError):
        return False
    return True


def module_source_path(module_name: str, root: Path | None = None) -> Path | None:
    root = root or _active_path()
    if root is None:
        return None
    parts = module_name.split(".")
    package = root.joinpath(*parts)
    if (package / "__init__.py").is_file():
        return package
    module = package.with_suffix(".py")
    return module if module.is_file() else None


def generation_native_extensions(root: Path | None = None) -> set[str]:
    root = root or _active_path()
    if root is None:
        return set()
    metadata = read_json(root / ".zhenxun-generation.json", {})
    values = metadata.get("native_extensions", [])
    return {str(item) for item in values} if isinstance(values, list) else set()


def _scan_native_extensions(root: Path) -> list[str]:
    extensions = {".pyd", ".so", ".dylib", ".dll"}
    return [
        str(path.relative_to(root)).replace("\\", "/")
        for path in root.rglob("*")
        if path.is_file() and path.suffix.casefold() in extensions
    ]


def _layer_digest(root: Path) -> str:
    digest = sha256()
    for path in sorted(item for item in root.rglob("*") if item.is_file()):
        digest.update(str(path.relative_to(root)).replace("\\", "/").encode())
        digest.update(path.read_bytes())
    return digest.hexdigest()


def build_generation(transaction: dict[str, Any]) -> dict[str, Any]:
    packages = transaction.get("target_manifest", {}).get("packages", {})
    if not isinstance(packages, dict):
        raise LayerBuildError("dependency_plan_invalid")
    core = protected_core()
    requested: list[str] = []
    for raw_name, info in sorted(packages.items()):
        name = canonicalize_name(str(raw_name))
        if name in core:
            raise LayerBuildError("core_dependency_in_layer")
        if not isinstance(info, dict) or not info.get("version"):
            raise LayerBuildError("dependency_plan_invalid")
        requested.append(f"{name}=={info['version']}")

    manifest = load_manifest()
    generation = next_generation(manifest)
    LAYER_ROOT.mkdir(parents=True, exist_ok=True)
    final_path = generation_path(generation)
    if final_path.exists():
        raise LayerBuildError("generation_already_exists")

    with tempfile.TemporaryDirectory(
        prefix=f"generation-{generation}-", dir=str(LAYER_ROOT)
    ) as temporary:
        staging = Path(temporary)
        if requested:
            requirements = staging.parent / f"generation-{generation}.requirements.txt"
            requirements.write_text("\n".join(requested) + "\n", encoding="utf-8")
            command = [
                "uv",
                "pip",
                "install",
                "--target",
                str(staging),
                "--requirements",
                str(requirements),
                "--no-deps",
                "--strict",
                "--python",
                sys.executable,
            ]
            if not transaction.get("source_build_confirmed"):
                command.append("--only-binary=:all:")
            completed = subprocess.run(
                command,
                cwd=str(Path.cwd()),
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
                env={**os.environ, "UV_NO_PROGRESS": "1"},
                check=False,
            )
            requirements.unlink(missing_ok=True)
            if completed.returncode:
                raise LayerBuildError(
                    "dependency_layer_build_failed",
                    safe_process_error(completed.stderr or completed.stdout),
                )
        native_files = _scan_native_extensions(staging)
        metadata = {
            "version": 1,
            "generation": generation,
            "created_at": utc_now(),
            "packages": sorted(requested),
            "native_extensions": native_files,
        }
        (staging / ".zhenxun-generation.json").write_text(
            json.dumps(metadata, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        digest = _layer_digest(staging)
        staging.replace(final_path)
    return {
        "generation": generation,
        "path": final_path,
        "native_extensions": native_files,
        "digest": digest,
    }


def commit_generation(
    transaction: dict[str, Any], build: dict[str, Any], *, verify_on_start: bool
) -> dict[str, Any]:
    previous = load_manifest()
    target = deepcopy(transaction["target_manifest"])
    target["active_generation"] = int(build["generation"])
    target["previous_generation"] = previous.get("active_generation")
    target["pending_verification"] = bool(verify_on_start)
    target["generation_digest"] = build["digest"]
    write_json(ROLLBACK_FILE, previous)
    save_manifest(target)
    return target


def stage_generation(transaction: dict[str, Any], build: dict[str, Any]) -> None:
    """Persist a built generation without exposing it to the running worker."""
    previous = transaction.get("generation")
    if isinstance(previous, int) and previous != build["generation"]:
        remove_generation(previous)
    transaction["generation"] = int(build["generation"])
    transaction["generation_digest"] = str(build["digest"])
    transaction["native_extensions"] = list(build.get("native_extensions") or [])
    transaction["state"] = "pending_restart"
    write_json(PENDING_FILE, transaction)


def _staged_build(transaction: dict[str, Any]) -> dict[str, Any] | None:
    generation = transaction.get("generation")
    if not isinstance(generation, int):
        return None
    path = generation_path(generation)
    digest = str(transaction.get("generation_digest") or "")
    if not path.is_dir() or not digest:
        return None
    return {
        "generation": generation,
        "path": path,
        "digest": digest,
        "native_extensions": list(transaction.get("native_extensions") or []),
    }


def _run_orm_migration(action: str) -> int:
    completed = subprocess.run(
        [sys.executable, "-m", "zhenxun.nonebot_store.orm_migration", action],
        cwd=str(Path.cwd()),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        timeout=180,
        check=False,
    )
    return int(completed.returncode)


def finalize_orm_migration() -> None:
    _run_orm_migration("finalize")


def apply_pending_transaction() -> bool:
    """Build a queued layer after the launcher has stopped the old worker."""
    transaction = pending_transaction()
    if transaction is None:
        return False
    if transaction.get("state") not in {
        "pending_restart",
        "building",
        "migration_blocked",
    }:
        return False
    transaction["state"] = "building"
    write_json(PENDING_FILE, transaction)
    try:
        build = _staged_build(transaction) or build_generation(transaction)
        if transaction.get("database_migration_possible"):
            transaction["generation"] = int(build["generation"])
            transaction["generation_digest"] = str(build["digest"])
            transaction["native_extensions"] = list(
                build.get("native_extensions") or []
            )
            write_json(PENDING_FILE, transaction)
            migration_result = _run_orm_migration("apply")
            if migration_result:
                transaction["state"] = (
                    "migration_blocked" if migration_result == 3 else "failed"
                )
                transaction["error_code"] = (
                    "external_database_manual_migration_required"
                    if migration_result == 3
                    else "database_migration_failed"
                )
                write_json(PENDING_FILE, transaction)
                return False
        commit_generation(transaction, build, verify_on_start=True)
    except Exception as error:
        transaction["state"] = "failed"
        transaction["error_code"] = getattr(error, "code", type(error).__name__)
        transaction["error"] = safe_process_error(str(error))
        write_json(PENDING_FILE, transaction)
        return False
    transaction["state"] = "verification_pending"
    transaction["generation"] = build["generation"]
    write_json(PENDING_FILE, transaction)
    return True


def load_managed_plugins() -> dict[str, Any]:
    """Load managed modules without allowing one broken plugin to stop the worker."""
    manifest = load_manifest()
    generation = manifest.get("active_generation")
    loaded: list[str] = []
    failed: list[dict[str, str]] = []
    if generation is not None:
        for store_key, plugin in sorted(manifest.get("plugins", {}).items()):
            if not isinstance(plugin, dict) or plugin.get("state") != "managed":
                continue
            module_name = str(plugin.get("module_name") or "")
            if not module_name:
                failed.append({"store_key": store_key, "code": "module_name_missing"})
                continue
            try:
                _load_managed_plugin(module_name)
                loaded.append(module_name)
            except Exception as error:
                failure = _safe_plugin_import_failure(error)
                failed.append(
                    {
                        "store_key": store_key,
                        "module_name": module_name,
                        **failure,
                    }
                )
    status = {
        "generation": generation,
        "loaded": loaded,
        "failed": failed,
        "checked_at": utc_now(),
    }
    write_json(STARTUP_STATUS_FILE, status)
    return status


def _load_managed_plugin(module_name: str) -> None:
    """Load with NoneBot's finder while preserving the original import error."""
    from nonebot.plugin import _managers
    from nonebot.plugin.manager import PluginManager
    from nonebot.plugin.model import Plugin

    manager = PluginManager([module_name])
    _managers.append(manager)
    module = importlib.import_module(module_name)
    plugin = getattr(module, "__plugin__", None)
    if not isinstance(plugin, Plugin):
        raise RuntimeError("nonebot_plugin_registration_missing")


def _safe_plugin_import_failure(error: Exception) -> dict[str, Any]:
    """Reduce an import failure to stable diagnostics without exception contents."""
    current: BaseException | None = error
    seen: set[int] = set()
    for _ in range(8):
        if current is None or id(current) in seen:
            break
        seen.add(id(current))
        errors = getattr(current, "errors", None)
        if callable(errors) and type(current).__module__.startswith("pydantic"):
            try:
                entries = errors(
                    include_url=False,
                    include_context=False,
                    include_input=False,
                )
            except (TypeError, ValueError):
                entries = []
            paths: list[str] = []
            missing = False
            for entry in entries if isinstance(entries, list) else []:
                if not isinstance(entry, dict):
                    continue
                missing = missing or entry.get("type") == "missing"
                loc = entry.get("loc")
                if not isinstance(loc, list | tuple):
                    continue
                parts = [
                    str(part)
                    for part in loc
                    if isinstance(part, str | int) and len(str(part)) <= 80
                ]
                if parts:
                    paths.append(".".join(parts))
            return {
                "code": (
                    "plugin_configuration_required"
                    if missing
                    else "plugin_configuration_invalid"
                ),
                "paths": sorted(set(paths))[:20],
            }
        current = current.__cause__ or current.__context__
    return {"code": "plugin_import_failed", "paths": []}


def startup_verification() -> tuple[bool, dict[str, Any]]:
    manifest = load_manifest()
    status = read_json(STARTUP_STATUS_FILE, {})
    expected = manifest.get("active_generation")
    valid = (
        status.get("generation") == expected
        and isinstance(status.get("failed"), list)
        and not status["failed"]
    )
    return valid, status


def finalize_pending_transaction() -> None:
    manifest = load_manifest()
    manifest["pending_verification"] = False
    manifest["previous_generation"] = None
    save_manifest(manifest)
    finalize_orm_migration()
    clear_pending_transaction()
    ROLLBACK_FILE.unlink(missing_ok=True)
    active = manifest.get("active_generation")
    prune_generations({active} if isinstance(active, int) else set())


def rollback_pending_transaction() -> None:
    current = load_manifest()
    failed_status = read_json(STARTUP_STATUS_FILE, {})
    rollback = read_json(ROLLBACK_FILE, None)
    _run_orm_migration("restore")
    if isinstance(rollback, dict):
        save_manifest(rollback)
    remove_generation(current.get("active_generation"))
    transaction = pending_transaction()
    if transaction:
        transaction["state"] = "failed"
        transaction["error_code"] = "plugin_startup_verification_failed"
        failures = failed_status.get("failed")
        if isinstance(failures, list):
            transaction["failure_reasons"] = [
                {
                    "code": str(item.get("code") or "plugin_import_failed"),
                    "store_key": str(item.get("store_key") or ""),
                    "module_name": str(item.get("module_name") or ""),
                    "paths": [
                        str(path)
                        for path in item.get("paths", [])
                        if isinstance(path, str)
                    ][:20],
                }
                for item in failures
                if isinstance(item, dict)
                and (
                    not transaction.get("module_name")
                    or item.get("module_name") == transaction.get("module_name")
                )
            ][:10]
        write_json(PENDING_FILE, transaction)
    ROLLBACK_FILE.unlink(missing_ok=True)
    restored = load_manifest().get("active_generation")
    prune_generations({restored} if isinstance(restored, int) else set())
