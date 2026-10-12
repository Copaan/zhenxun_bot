from __future__ import annotations

import asyncio
from collections.abc import Callable
import contextlib
from copy import deepcopy
from hashlib import sha256
import importlib
import json
import os
from pathlib import Path
import shutil
import site
import subprocess
import sys
import tempfile
from threading import Event
import time
from typing import Any

from packaging.utils import canonicalize_name

from zhenxun.services.plugin_store.plugin_archive_dependencies import (
    ArchiveDependencyConflict,
)
from zhenxun.services.plugin_store.plugin_store_transaction import (
    ArchiveSourceBuildConflict,
    archive_dependency_policy,
)
from zhenxun.utils.pythonpath import subprocess_pythonpath

from .dependencies import protected_core, safe_process_error
from .storage import (
    LAYER_ROOT,
    ORM_MIGRATION_STATUS_FILE,
    PENDING_FILE,
    ROLLBACK_FILE,
    STARTUP_STATUS_FILE,
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
    def __init__(
        self,
        code: str,
        detail: str | None = None,
        *,
        details: dict[str, Any] | None = None,
    ):
        super().__init__(detail or code)
        self.code = code
        self.details = details


def recovery_required() -> bool:
    """Keep the management surface available while rollback is unconfirmed."""
    return (pending_transaction() or {}).get("state") == "recovery_required"


def rollback_for_launcher() -> bool:
    """Retain a failed rollback without restarting business components."""
    try:
        rollback_pending_transaction()
    except Exception as error:
        transaction = pending_transaction() or {}
        transaction.update(
            state="recovery_required",
            error_code=getattr(error, "code", type(error).__name__),
            rollback_error=safe_process_error(str(error)),
        )
        write_json(PENDING_FILE, transaction)
        with contextlib.suppress(Exception):
            record_manual_operations(
                transaction, "recovery_required", "recovery_required"
            )
        return False
    return True


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
        # The layer is already first on sys.path, so addsitedir only processes
        # its .pth files (e.g. pywin32, namespace packages).
        site.addsitedir(str(path))
        importlib.invalidate_caches()
    return path


def _is_layer_path(value: str, layer_root: Path) -> bool:
    try:
        Path(value).resolve().relative_to(layer_root)
    except (OSError, ValueError):
        return False
    return True


def referenced_generations() -> set[int]:
    """Return generations still referenced by modules loaded in this process."""
    layer_root = LAYER_ROOT.resolve()
    generations: set[int] = set()
    for module in tuple(sys.modules.values()):
        if module is None:
            continue
        candidates: list[Any] = [getattr(module, "__file__", None)]
        module_path = getattr(module, "__path__", None)
        if module_path is not None:
            try:
                candidates.extend(tuple(module_path))
            except (KeyError, TypeError):
                pass
        for candidate in candidates:
            if not isinstance(candidate, str | os.PathLike):
                continue
            try:
                relative = Path(candidate).resolve().relative_to(layer_root)
            except (OSError, ValueError):
                continue
            if not relative.parts or not relative.parts[0].startswith("generation-"):
                continue
            try:
                generations.add(int(relative.parts[0].removeprefix("generation-")))
            except ValueError:
                continue
    return generations


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


def layer_digest(root: Path) -> str:
    digest = sha256()
    for path in sorted(item for item in root.rglob("*") if item.is_file()):
        if path.suffix == ".pyc":
            try:
                source = Path(importlib.util.source_from_cache(str(path)))
            except ValueError:
                source = path.with_suffix(".py")
            if source.is_file():
                continue
        digest.update(str(path.relative_to(root)).replace("\\", "/").encode())
        digest.update(path.read_bytes())
    return digest.hexdigest()


def _package_runtime_details(
    root: Path, packages: dict[str, Any]
) -> dict[str, dict[str, Any]]:
    """Read installed distribution metadata without guessing import names."""
    details: dict[str, dict[str, Any]] = {}
    for dist_info in root.glob("*.dist-info"):
        metadata = dist_info / "METADATA"
        if not metadata.is_file():
            continue
        values: dict[str, str] = {}
        for line in metadata.read_text(encoding="utf-8", errors="replace").splitlines():
            if ": " in line:
                key, value = line.split(": ", 1)
                values.setdefault(key, value.strip())
        raw_name = values.get("Name")
        if not raw_name:
            continue
        name = canonicalize_name(raw_name)
        if name not in packages:
            continue
        top_level_file = dist_info / "top_level.txt"
        modules = (
            sorted(
                line.strip()
                for line in top_level_file.read_text(encoding="utf-8").splitlines()
                if line.strip() and line.strip().isidentifier()
            )
            if top_level_file.is_file()
            else []
        )
        details[name] = {
            "distribution_name": name,
            "version": str(packages[name].get("version")),
            "top_level_modules": modules,
            "nonebot_plugin_ids": [],
            "runtime_role": "python_dependency",
        }
    return details


def generation_packages(root: Path) -> dict[str, dict[str, Any]]:
    """读取依赖层中实际安装的包，生成清单中的包记录

    参数:
        root: 依赖层目录

    返回:
        dict[str, dict[str, Any]]: 规范化包名到包记录的映射
    """
    from importlib import metadata

    packages: dict[str, dict[str, Any]] = {
        canonicalize_name(dist.metadata["Name"]): {"version": dist.version}
        for dist in metadata.distributions(path=[str(root)])
        if dist.metadata["Name"]
    }
    for name, detail in _package_runtime_details(root, packages).items():
        packages[name].update(detail)
    return dict(sorted(packages.items()))


def build_generation(
    transaction: dict[str, Any], *, cancel_requested: Callable[[], bool] | None = None
) -> dict[str, Any]:
    try:
        with archive_dependency_policy(transaction):
            return _build_generation(transaction, cancel_requested=cancel_requested)
    except (ArchiveSourceBuildConflict, ArchiveDependencyConflict) as error:
        raise LayerBuildError(
            error.code,
            details=getattr(error, "details", None),
        ) from error


async def prepare_generation(transaction: dict[str, Any]) -> dict[str, Any]:
    """Build off the event loop and finish cleanup before releasing a canceled owner."""
    from zhenxun.services.runtime_mutation import runtime_mutation_coordinator

    cancel = Event()
    task = asyncio.create_task(
        asyncio.to_thread(
            build_generation,
            transaction,
            cancel_requested=lambda: cancel.is_set()
            or runtime_mutation_coordinator.cancellation_requested,
        )
    )
    built = None
    try:
        built = await asyncio.shield(task)
        runtime_mutation_coordinator.checkpoint()
        return built
    except asyncio.CancelledError:
        cancel.set()
        if built is None:
            with contextlib.suppress(Exception):
                built = await asyncio.shield(task)
        if built is not None:
            remove_generation(built["generation"])
        raise


def _run_installer(
    command: list[str],
    environment: dict[str, str],
    cancel_requested: Callable[[], bool] | None,
) -> subprocess.CompletedProcess:
    """Wait for an installer and its owned descendants before releasing staging."""
    import psutil

    from zhenxun.utils.process_tree import verified_descendants

    process = subprocess.Popen(
        command,
        cwd=str(Path.cwd()),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        encoding="utf-8",
        errors="replace",
        env=environment,
    )
    owned: dict[int, psutil.Process] = {}
    try:
        parent = psutil.Process(process.pid)
        owned[parent.pid] = parent
    except psutil.NoSuchProcess:
        parent = None
    deadline = time.monotonic() + 900
    finished = False
    try:
        while True:
            if parent is not None:
                with contextlib.suppress(psutil.NoSuchProcess):
                    owned.update(
                        (child.pid, child) for child in verified_descendants(parent)
                    )
            if cancel_requested and cancel_requested():
                raise LayerBuildError("dependency_operation_interrupted")
            if time.monotonic() >= deadline:
                raise LayerBuildError("dependency_layer_build_timeout")
            try:
                stdout, stderr = process.communicate(timeout=0.2)
                finished = True
                return subprocess.CompletedProcess(
                    command, process.returncode, stdout, stderr
                )
            except subprocess.TimeoutExpired:
                continue
    finally:
        unexpected_children = []
        if finished:
            _, unexpected_children = psutil.wait_procs(list(owned.values()), timeout=5)
        for child in reversed(list(owned.values())):
            with contextlib.suppress(psutil.NoSuchProcess):
                if child.is_running():
                    child.kill()
        if process.poll() is None:
            process.kill()
        process.wait(timeout=5)
        _, alive = psutil.wait_procs(list(owned.values()), timeout=5)
        for pipe in (process.stdout, process.stderr):
            if pipe is not None:
                pipe.close()
        if alive:
            raise LayerBuildError("dependency_tool_cleanup_unconfirmed")
        if unexpected_children:
            raise LayerBuildError("dependency_tool_child_incomplete")


def _build_generation(
    transaction: dict[str, Any], *, cancel_requested: Callable[[], bool] | None = None
) -> dict[str, Any]:
    from zhenxun.services.installer_network import (
        UV_BINARY_POLICY_VERSION,
        installer_environment,
        resolver_configuration_error,
        uv_binary_options,
        uv_command,
        uv_version,
    )
    from zhenxun.services.plugin_store.plugin_archive_dependencies import (
        archive_dependency_contract,
    )

    packages = transaction.get("target_manifest", {}).get("packages", {})
    if not isinstance(packages, dict):
        raise LayerBuildError("dependency_plan_invalid")
    from .environment import capture_environment
    from .policy import ORM_PACKAGES, guard_orm_changes

    before = capture_environment()
    effective = {name: dist.version for name, dist in before.base.items()}
    effective.update(
        {name: str(info.get("version")) for name, info in packages.items()}
    )
    guard_orm_changes(before.versions, effective)
    core = protected_core()
    requested: list[str] = []
    for raw_name, info in sorted(packages.items()):
        name = canonicalize_name(str(raw_name))
        if name in core:
            raise LayerBuildError("core_dependency_in_layer")
        if not isinstance(info, dict) or not info.get("version"):
            raise LayerBuildError("dependency_plan_invalid")
        if name in ORM_PACKAGES:
            continue
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
        for name in set(packages) & ORM_PACKAGES:
            distribution = before.layer.get(name)
            if distribution is None:
                raise LayerBuildError("orm_dependency_console_required", name)
            source_root = Path(distribution.locate_file("")).resolve()
            for file in distribution.files or ():
                source = Path(distribution.locate_file(file)).resolve()
                destination = (staging / str(file)).resolve()
                if not source.is_relative_to(
                    source_root
                ) or not destination.is_relative_to(staging.resolve()):
                    raise LayerBuildError("dependency_layer_state_mismatch", name)
                if source.is_file():
                    destination.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copy2(source, destination)
        if requested:
            requirements = staging.parent / f"generation-{generation}.requirements.txt"
            requirements.write_text("\n".join(requested) + "\n", encoding="utf-8")
            command = uv_command(
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
            )
            if transaction.get("archive_wheels_only") or not transaction.get(
                "source_build_confirmed"
            ):
                command.extend(uv_binary_options(wheels_only=True))
            else:
                forbidden_builds = set(
                    archive_dependency_contract().get("wheels_only_packages", [])
                )
                forbidden_builds.update(
                    transaction.get("archive_binary_only_packages", [])
                )
                if "archive_build_allowlist" in transaction:
                    forbidden_builds.update(
                        set(packages) - set(transaction["archive_build_allowlist"])
                    )
                command.extend(
                    uv_binary_options(
                        wheels_only=False,
                        restricted_packages=tuple(forbidden_builds),
                    )
                )
            try:
                completed = _run_installer(
                    command, installer_environment(), cancel_requested
                )
            finally:
                requirements.unlink(missing_ok=True)
            if completed.returncode:
                detail = safe_process_error(completed.stderr or completed.stdout)
                if resolver_configuration_error(detail):
                    raise LayerBuildError(
                        "dependency_resolver_configuration",
                        (
                            f"uv={uv_version()} "
                            f"binary_policy={UV_BINARY_POLICY_VERSION}: {detail}"
                        ),
                    )
                raise LayerBuildError(
                    "dependency_layer_build_failed",
                    detail,
                    details={
                        "stage": "dependency_build",
                        "exit_code": completed.returncode,
                        "diagnostic": detail,
                    },
                )
        from .environment import capture_environment, validate_candidate
        from .policy import bind_trials

        bind_trials(
            capture_environment(
                manifest=transaction["target_manifest"], candidate_path=staging
            )
        )
        verification = validate_candidate(staging, transaction)
        native_files = _scan_native_extensions(staging)
        package_details = _package_runtime_details(staging, packages)
        metadata = {
            "version": 1,
            "generation": generation,
            "created_at": utc_now(),
            "packages": sorted(requested),
            "package_details": package_details,
            "native_extensions": native_files,
            "dependency_verification": verification,
        }
        (staging / ".zhenxun-generation.json").write_text(
            json.dumps(metadata, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        digest = layer_digest(staging)
        staging.replace(final_path)
    return {
        "generation": generation,
        "path": final_path,
        "native_extensions": native_files,
        "digest": digest,
    }


def _validate_archive_generation(
    transaction: dict[str, Any], build: dict[str, Any]
) -> None:
    from .environment import validate_candidate

    path = generation_path(int(build["generation"]))
    if layer_digest(path) != build.get("digest"):
        raise LayerBuildError("dependency_layer_state_mismatch")
    validate_candidate(path, transaction)
    if not transaction.get("archive_wheels_only"):
        return
    path = generation_path(int(build["generation"]))
    metadata = read_json(path / ".zhenxun-generation.json", {})
    expected = sorted(
        f"{canonicalize_name(name)}=={info['version']}"
        for name, info in transaction["target_manifest"]["packages"].items()
    )
    # A later archive merge can change the manifest after this layer was built.
    # Its old digest alone does not prove it contains the newly owned pins.
    if (
        not isinstance(metadata, dict)
        or metadata.get("packages") != expected
        or layer_digest(path) != build.get("digest")
    ):
        raise LayerBuildError("archive_dependency_conflict")


def commit_generation(
    transaction: dict[str, Any], build: dict[str, Any], *, verify_on_start: bool
) -> dict[str, Any]:
    with archive_dependency_policy(transaction):
        _validate_archive_generation(transaction, build)
        previous = load_manifest()
        target = deepcopy(transaction["target_manifest"])
        generation_metadata = read_json(
            generation_path(int(build["generation"])) / ".zhenxun-generation.json", {}
        )
        package_details = generation_metadata.get("package_details", {})
        if isinstance(package_details, dict):
            for name, detail in package_details.items():
                if isinstance(target.get("packages", {}).get(name), dict):
                    target["packages"][name].update(detail)
        target["active_generation"] = int(build["generation"])
        target["previous_generation"] = previous.get("active_generation")
        target["pending_verification"] = bool(verify_on_start)
        target.pop("verified_transaction_revision", None)
        target["generation_digest"] = build["digest"]
        write_json(ROLLBACK_FILE, previous)
        save_manifest(target)
        return target


def stage_generation(transaction: dict[str, Any], build: dict[str, Any]) -> None:
    """Persist a built generation without exposing it to the running worker."""
    with archive_dependency_policy(transaction):
        _validate_archive_generation(transaction, build)
        previous = transaction.get("generation")
        transaction["generation"] = int(build["generation"])
        transaction["generation_digest"] = str(build["digest"])
        transaction["native_extensions"] = list(build.get("native_extensions") or [])
        transaction["state"] = "pending_restart"
        write_json(PENDING_FILE, transaction)
        if isinstance(previous, int) and previous != build["generation"]:
            remove_generation(previous)


def _staged_build(transaction: dict[str, Any]) -> dict[str, Any] | None:
    generation = transaction.get("generation")
    if not isinstance(generation, int):
        return None
    path = generation_path(generation)
    digest = str(transaction.get("generation_digest") or "")
    if not path.is_dir() or not digest:
        return None
    build = {
        "generation": generation,
        "path": path,
        "digest": digest,
        "native_extensions": list(transaction.get("native_extensions") or []),
    }
    try:
        _validate_archive_generation(transaction, build)
    except LayerBuildError:
        return None
    return build


def _run_orm_migration(action: str) -> int:
    try:
        completed = subprocess.run(
            [
                sys.executable,
                "-m",
                "zhenxun.services.nonebot_store.orm_migration",
                action,
            ],
            cwd=str(Path.cwd()),
            env={**os.environ, "PYTHONPATH": subprocess_pythonpath()},
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=180,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired) as error:
        completed = subprocess.CompletedProcess([], 124, "", type(error).__name__)
    if completed.returncode:
        status = read_json(ORM_MIGRATION_STATUS_FILE, {})
        write_json(
            ORM_MIGRATION_STATUS_FILE,
            {
                **status,
                "tool_action": action,
                "tool_exit_code": completed.returncode,
                "tool_diagnostic": safe_process_error(
                    completed.stderr or completed.stdout or ""
                )[-4000:],
                "updated_at": utc_now(),
            },
        )
    return int(completed.returncode)


def finalize_orm_migration() -> None:
    if _run_orm_migration("finalize"):
        raise LayerBuildError("database_migration_cleanup_failed")


def record_manual_operations(transaction: dict, mode: str, status: str) -> None:
    """Persist the outcome of store and manual requests sharing this generation."""
    operations = [
        item
        for item in transaction.get("operations", [])
        if item.get("operation_id")
        and (
            item.get("kind") in {"manual_dependencies", "environment_repair"}
            or item.get("project_link")
        )
    ]
    if not operations:
        return
    from zhenxun.services.plugin_store.operation_journal import (
        operation_status,
        record_operation,
    )

    for operation in operations:
        identity = operation["operation_id"]
        previous = (operation_status(identity) or {}).get("result") or {}
        previous.pop("cleanup_error", None)
        record_operation(
            "dependencies:manual"
            if operation.get("kind") == "manual_dependencies"
            else "dependencies:repair"
            if operation.get("kind") == "environment_repair"
            else f"nonebot:{operation['project_link']}",
            {
                **previous,
                "operation_id": identity,
                "apply_mode": mode,
                "status": status,
                "request_binding": operation.get("request_binding")
                or previous.get("request_binding"),
                "reason": transaction.get("error_code"),
            },
        )


def apply_pending_transaction() -> bool:
    """Build a queued layer after the launcher has stopped the old worker."""
    transaction = pending_transaction()
    if transaction is None:
        return False
    if transaction.get("state") in {"committed", "cleanup_pending"}:
        finalize_pending_transaction()
        return False
    if transaction.get("state") not in {
        "pending_restart",
        "building",
        "migration_blocked",
    }:
        return False
    try:
        # Check even when reusing a generation, before any migration execution.
        with archive_dependency_policy(transaction):
            transaction["state"] = "building"
            write_json(PENDING_FILE, transaction)
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
                    "migration_blocked"
                    if migration_result == 3
                    else "recovery_required"
                    if migration_result == 5
                    else "failed"
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
    if manifest.get("pending_verification"):
        from .dependencies import DependencyAnalysisError
        from .environment import capture_environment

        try:
            snapshot = capture_environment(manifest=manifest)
            status["loaded_mismatches"] = snapshot.loaded_mismatches
            status["environment_snapshot"] = snapshot.digest
        except DependencyAnalysisError as error:
            status["environment_error"] = {"code": error.code, "details": error.details}
    write_json(STARTUP_STATUS_FILE, status)
    return status


def _load_managed_plugin(module_name: str) -> None:
    """Load with NoneBot's finder while preserving the original import error."""
    from nonebot.plugin import _managers
    from nonebot.plugin.manager import PluginManager
    from nonebot.plugin.model import Plugin

    manager = PluginManager([module_name])
    _managers.append(manager)
    try:
        module = importlib.import_module(module_name)
        plugin = getattr(module, "__plugin__", None)
        if not isinstance(plugin, Plugin):
            raise RuntimeError("nonebot_plugin_registration_missing")
    except BaseException:
        with contextlib.suppress(ValueError):
            _managers.remove(manager)
        raise


def _safe_plugin_import_failure(error: Exception) -> dict[str, Any]:
    """Reduce an import failure to stable diagnostics without exception contents."""
    current: BaseException | None = error
    seen: set[int] = set()
    for _ in range(8):
        if current is None or id(current) in seen:
            break
        seen.add(id(current))
        if isinstance(current, ImportError):
            return {"code": "dependency_import_failed", "paths": []}
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
    from .dependencies import DependencyAnalysisError
    from .environment import capture_environment, validate_candidate

    manifest = load_manifest()
    status = read_json(STARTUP_STATUS_FILE, {})
    expected = manifest.get("active_generation")
    valid = (
        status.get("generation") == expected
        and isinstance(status.get("failed"), list)
        and not status["failed"]
        and set(status.get("loaded", []))
        == {
            item["module_name"]
            for item in manifest.get("plugins", {}).values()
            if item.get("state") == "managed"
        }
    )
    try:
        transaction = read_json(PENDING_FILE, {})
        if isinstance(expected, int):
            transaction = {**transaction, "target_manifest": manifest}
            if layer_digest(generation_path(expected)) != manifest.get(
                "generation_digest"
            ):
                raise DependencyAnalysisError("dependency_layer_state_mismatch")
            check = validate_candidate(generation_path(expected), transaction)
        else:
            snapshot = capture_environment(manifest=manifest)
            check = {
                "state": "failed" if snapshot.conflicts else "passed",
                "remaining_conflicts": snapshot.conflicts,
            }
        snapshot = capture_environment(manifest=manifest)
        status["dependency_verification"] = check
        valid = (
            valid
            and check["state"] != "failed"
            and status.get("environment_snapshot") == snapshot.digest
            and not status.get("loaded_mismatches")
            and not status.get("environment_error")
        )
    except DependencyAnalysisError as error:
        status["dependency_verification"] = {
            "state": "failed",
            "code": error.code,
            "details": error.details,
        }
        valid = False
    status["plugin_verification"] = "passed" if not status.get("failed") else "failed"
    write_json(STARTUP_STATUS_FILE, status)
    return valid, status


def finalize_pending_transaction() -> None:
    """Commit verification independently of retryable backup/journal cleanup."""
    transaction = pending_transaction() or {}
    manifest = load_manifest()
    manifest["pending_verification"] = False
    manifest["previous_generation"] = None
    for plugin in manifest.get("plugins", {}).values():
        for item in plugin.get("compatibility_overrides", []):
            if item.get("evidence"):
                item["verification"] = "startup_verified"
    if transaction.get("revision"):
        manifest["verified_transaction_revision"] = transaction["revision"]
    save_manifest(manifest)
    transaction.update(state="committed", verification_confirmed=True)
    transaction.pop("cleanup_error", None)
    try:
        write_json(PENDING_FILE, transaction)
        finalize_orm_migration()
        record_manual_operations(transaction, "applied", "completed")
        active = manifest.get("active_generation")
        retained = referenced_generations()
        if isinstance(active, int):
            retained.add(active)
        prune_generations(retained)
        ROLLBACK_FILE.unlink(missing_ok=True)
        from .cleanup import archive_settled

        transaction["cleanup_state"] = "completed"
        write_json(PENDING_FILE, transaction)
        archive_settled()
    except Exception as error:
        transaction.update(
            state="cleanup_pending",
            cleanup_error={
                "code": getattr(error, "code", type(error).__name__),
                "diagnostic": safe_process_error(str(error)),
            },
        )
        with contextlib.suppress(OSError):
            write_json(PENDING_FILE, transaction)
        from .cleanup import archive_settled

        with contextlib.suppress(Exception):
            archive_settled()
        return


def rollback_pending_transaction() -> None:
    current = load_manifest()
    transaction = pending_transaction() or {}
    failed_generation = transaction.get("generation", current.get("active_generation"))
    failed_status = read_json(STARTUP_STATUS_FILE, {})
    rollback = read_json(ROLLBACK_FILE, None)
    if not transaction.get("rollback_confirmed") and not isinstance(rollback, dict):
        transaction.update(
            state="recovery_required", error_code="dependency_rollback_receipt_missing"
        )
        write_json(PENDING_FILE, transaction)
        record_manual_operations(transaction, "recovery_required", "recovery_required")
        raise LayerBuildError("dependency_rollback_receipt_missing")
    if not transaction.get("rollback_confirmed") and _run_orm_migration("restore"):
        transaction.update(
            state="recovery_required", error_code="database_migration_rollback_failed"
        )
        write_json(PENDING_FILE, transaction)
        record_manual_operations(transaction, "recovery_required", "recovery_required")
        raise LayerBuildError("database_migration_rollback_failed")
    if isinstance(rollback, dict) and not transaction.get("rollback_confirmed"):
        save_manifest(rollback)
    if transaction:
        transaction["state"] = "failed"
        transaction["rollback_confirmed"] = True
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
            if not transaction["failure_reasons"]:
                transaction["failure_reasons"] = [
                    {"code": "plugin_startup_verification_failed"}
                ]
        write_json(PENDING_FILE, transaction)
        record_manual_operations(transaction, "rolled_back", "failed")
    restored = load_manifest().get("active_generation")
    try:
        if failed_generation != restored:
            remove_generation(failed_generation)
        prune_generations({restored} if isinstance(restored, int) else set())
        ROLLBACK_FILE.unlink(missing_ok=True)
        if transaction.get("cleanup_error"):
            transaction.pop("cleanup_error", None)
        transaction["cleanup_state"] = "completed"
        write_json(PENDING_FILE, transaction)
    except OSError as error:
        transaction["cleanup_error"] = {
            "code": "dependency_rollback_cleanup_pending",
            "diagnostic": safe_process_error(str(error)),
        }
        with contextlib.suppress(OSError):
            write_json(PENDING_FILE, transaction)
