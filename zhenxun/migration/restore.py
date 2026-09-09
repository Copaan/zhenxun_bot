from __future__ import annotations

from dataclasses import asdict, dataclass, replace
import hashlib
import json
import os
from pathlib import Path
import platform
import sys
import tempfile
from typing import Literal

from zhenxun.utils.atomic_json import write_json_locked

from .archive import CHUNK, file_hash, require_space
from .configuration import (
    configuration_yaml,
    merge_env,
    override_administrator,
    override_env,
    replace_env,
)
from .discovery import CATEGORIES, SourceBoundary, bounded_walk, category_for
from .errors import MigrationError
from .journal import FileJournal
from .paths import PathIndex, contained_path, is_link, logical_path

FileMode = Literal["missing", "overwrite", "exact"]
ConfigurationMode = Literal["keep", "missing", "backup"]
_NATIVE = {".so", ".pyd", ".dll", ".dylib", ".exe"}
_DATABASE = {".db", ".sqlite", ".sqlite3"}


def _configuration_read(path: Path) -> bytes:
    limit = 8 * CHUNK
    if path.stat().st_size > limit:
        raise MigrationError("migration_configuration_limit")
    with path.open("rb") as stream:
        result = stream.read(limit + 1)
    if len(result) > limit:
        raise MigrationError("migration_configuration_limit")
    return result


@dataclass(frozen=True)
class RestoreOptions:
    categories: frozenset[str] = CATEGORIES
    files: FileMode = "missing"
    configuration: ConfigurationMode = "keep"
    exact_directories: tuple[str, ...] = ()
    exact_files: tuple[str, ...] = ()
    replace_configuration_scope: bool = False

    def __post_init__(self) -> None:
        if not self.categories <= CATEGORIES:
            raise MigrationError("migration_categories_invalid")
        if self.files not in {"missing", "overwrite", "exact"}:
            raise MigrationError("migration_file_mode_invalid")
        if self.configuration not in {"keep", "missing", "backup"}:
            raise MigrationError("migration_configuration_mode_invalid")
        if self.files == "exact" and not (self.exact_directories or self.exact_files):
            raise MigrationError("migration_exact_scope_required")
        for path in self.exact_directories:
            logical_path(path)
        for path in self.exact_files:
            logical_path(path)
        if self.replace_configuration_scope and self.configuration != "backup":
            raise MigrationError("migration_configuration_mode_invalid")


def _fingerprint(path: Path, checkpoint=lambda: None) -> str | None:
    checkpoint()
    if not path.exists():
        return None
    if is_link(path) or not path.is_file():
        raise MigrationError("migration_path_type_conflict")
    return file_hash(path, checkpoint)


def _database_companion(path: Path) -> bool:
    for suffix in ("-wal", "-shm", "-journal"):
        if path.name.endswith(suffix) and path.name != suffix:
            base = path.with_name(path.name[: -len(suffix)])
            return base.suffix.lower() in _DATABASE or _database_file(base)
    return False


def _database_file(path: Path) -> bool:
    if path.suffix.lower() in _DATABASE:
        return True
    if path.is_file():
        if is_link(path):
            raise MigrationError("migration_link_forbidden")
        with path.open("rb") as stream:
            return stream.read(16) == b"SQLite format 3\0"
    return False


def _scope_revision(
    root: Path,
    scopes: tuple[str, ...],
    boundary: SourceBoundary,
    checkpoint=lambda: None,
    max_entries: int = 600_000,
) -> str:
    observed = {}
    inspected = 0
    for scope in scopes:
        directory = contained_path(root, scope)
        info = directory.stat() if directory.is_dir() else None
        observed[scope + "/"] = [info.st_dev, info.st_ino] if info else None
        if not directory.is_dir():
            continue
        for current, dirs, names in bounded_walk(directory, checkpoint=checkpoint):
            checkpoint()
            inspected += len(dirs) + len(names)
            if inspected > max_entries:
                raise MigrationError("migration_scan_work_limit")
            for name in list(dirs):
                child = Path(current) / name
                relative = child.relative_to(root).as_posix()
                if is_link(child):
                    raise MigrationError("migration_link_forbidden", path=relative)
                if boundary.protected(relative):
                    dirs.remove(name)
                else:
                    info = child.stat()
                    observed[relative + "/"] = [info.st_dev, info.st_ino]
            for name in names:
                child = Path(current) / name
                relative = child.relative_to(root).as_posix()
                if not boundary.protected(relative):
                    contained_path(root, relative, regular=True)
                    if _database_file(child) or _database_companion(child):
                        continue
                    observed[relative] = _fingerprint(child, checkpoint)
    return hashlib.sha256(json.dumps(observed, sort_keys=True).encode()).hexdigest()


def _configuration_bytes(
    path: str, incoming: bytes, existing: bytes | None, mode: ConfigurationMode
) -> bytes:
    if len(incoming) > 8 * CHUNK or len(existing or b"") > 8 * CHUNK:
        raise MigrationError("migration_configuration_limit", path=path)
    try:
        source = incoming.decode("utf-8-sig")
        target = (existing or b"").decode("utf-8-sig")
        name = path.rsplit("/", 1)[-1]
        if name.startswith(".env"):
            # Database selection, not the configuration file, owns DB_URL.
            keys = frozenset({"DB_URL"})
            result = (
                merge_env(target, source, preserve=keys)
                if mode == "missing"
                else replace_env(source, target, preserve=keys)
            )
            return result.encode()
        if path.endswith((".yaml", ".yml")):
            return configuration_yaml(
                path,
                source,
                target if existing is not None else None,
                missing=mode == "missing",
            ).encode()
        if mode == "missing" and existing is not None:
            raise MigrationError("migration_configuration_merge_unsupported", path=path)
        return incoming
    except UnicodeError:
        raise MigrationError("migration_configuration_encoding", path=path) from None


def prepare_files(
    root: Path,
    staging: Path,
    manifest: dict,
    options: RestoreOptions,
    *,
    checkpoint=lambda: None,
    configuration_overrides: dict | None = None,
    administrator_overrides: dict | None = None,
) -> dict:
    """Prepare candidate bytes without modifying the target instance."""
    boundary = SourceBoundary.read(root)
    actions, skipped, conflicts = [], [], []
    directory_candidates = set()
    removed_directories = []
    planned = PathIndex()
    selected = set()
    source = manifest.get("source", {})
    native_compatible = (
        source.get("system") == platform.system()
        and source.get("architecture") == platform.machine()
    )
    prepared = staging / "prepared"
    if prepared.exists():
        raise MigrationError("migration_plan_exists", status=409)
    prepared.mkdir(mode=0o700)
    for entry in manifest["files"]:
        checkpoint()
        path = entry["path"]
        if entry["category"] not in options.categories:
            skipped.append({"path": path, "reason": "category_not_selected"})
            continue
        if entry["root"] != "project":
            conflicts.append({"path": path, "reason": "external_root_mapping_required"})
            continue
        try:
            logical_path(path)
            if boundary.protected(path) or category_for(path) != entry["category"]:
                raise MigrationError("migration_target_protected", path=path)
            planned.add(path)
            target = contained_path(root, path)
            incoming = contained_path(staging, entry["payload"], regular=True)
            is_config = entry["category"] == "configuration"
            incoming_config = None
            target_config = None
            if is_config:
                incoming_config = _configuration_read(incoming)
                if target.exists():
                    target_config = _configuration_read(target)
            if file_hash(incoming, checkpoint) != entry["sha256"]:
                raise MigrationError("migration_payload_hash_mismatch", path=path)
            if (
                _database_file(target)
                or _database_companion(target)
                or _database_file(incoming)
                or Path(path).suffix.lower() in _DATABASE
            ):
                skipped.append(
                    {"path": path, "reason": "database_requires_separate_plan"}
                )
                continue
            selected.add(path)
            if Path(path).suffix.lower() in _NATIVE and not native_compatible:
                raise MigrationError("migration_native_binary_incompatible", path=path)
            if Path(path).suffix.lower() in {".so", ".pyd"} and (
                source.get("implementation") != sys.implementation.name
                or str(source.get("python", "")).split(".")[:2]
                != platform.python_version().split(".")[:2]
            ):
                raise MigrationError("migration_python_binary_incompatible", path=path)
            fingerprint = _fingerprint(target, checkpoint)
            if is_config and options.configuration == "keep":
                skipped.append({"path": path, "reason": "configuration_keep"})
                continue
            if not is_config and options.files == "missing" and fingerprint is not None:
                skipped.append({"path": path, "reason": "existing_file"})
                continue
            candidate = prepared / f"{len(actions):08d}"
            if is_config:
                configuration = _configuration_bytes(
                    path,
                    incoming_config,
                    target_config,
                    options.configuration,
                )
                if path in {".env", ".env.dev"} and configuration_overrides:
                    configuration = override_env(
                        configuration.decode("utf-8"), configuration_overrides
                    ).encode("utf-8")
                candidate.write_bytes(
                    override_administrator(path, configuration, administrator_overrides)
                )
            else:
                _atomic_copy(incoming, candidate, checkpoint=checkpoint)
            digest = file_hash(candidate, checkpoint)
            if fingerprint == digest:
                candidate.unlink()
                skipped.append({"path": path, "reason": "unchanged"})
                continue
            actions.append(
                {
                    "path": path,
                    "action": "replace" if fingerprint is not None else "add",
                    "before": fingerprint,
                    "after": digest,
                    "candidate": candidate.relative_to(staging).as_posix(),
                    "size": candidate.stat().st_size,
                }
            )
        except MigrationError as error:
            if error.code in {"migration_cancelled", "migration_budget_exhausted"}:
                raise
            conflicts.append({"path": path, "reason": error.code})
        except OSError:
            conflicts.append({"path": path, "reason": "migration_file_unreadable"})
    overrides = {}
    if configuration_overrides:
        overrides[".env.dev"] = "environment"
        if (root / ".env").exists():
            overrides[".env"] = "environment"
    if administrator_overrides:
        overrides["data/config.yaml"] = "administrator"
        overrides["data/configs/plugins2config.yaml"] = "administrator"
    if overrides:
        options = replace(
            options,
            exact_files=tuple(sorted(set(options.exact_files) | set(overrides))),
        )
    # Explicitly confirmed management values are independent of the package's
    # category selection. Persist them even when the package omits an env file.
    for path, kind in overrides.items():
        if path in selected:
            continue
        if boundary.protected(path):
            conflicts.append({"path": path, "reason": "migration_target_protected"})
            continue
        target = contained_path(root, path)
        before = _fingerprint(target, checkpoint)
        content = _configuration_read(target) if target.exists() else b""
        if kind == "environment":
            content = override_env(
                content.decode("utf-8-sig"), configuration_overrides
            ).encode("utf-8")
        else:
            content = override_administrator(
                path, content or b"{}", administrator_overrides
            )
        selected.add(path)
        planned.add(path)
        after = hashlib.sha256(content).hexdigest()
        if before == after:
            continue
        candidate = prepared / f"{len(actions):08d}"
        candidate.write_bytes(content)
        actions.append(
            {
                "path": path,
                "action": "replace" if before is not None else "add",
                "before": before,
                "after": after,
                "candidate": candidate.relative_to(staging).as_posix(),
                "size": len(content),
            }
        )
    if options.files == "exact":
        inspected = 0
        removals = set()
        for directory in options.exact_directories:
            if (
                boundary.protected(directory)
                or category_for(directory + "/x") not in options.categories
                or (
                    category_for(directory + "/x") == "configuration"
                    and not options.replace_configuration_scope
                )
            ):
                conflicts.append(
                    {"path": directory, "reason": "migration_exact_scope_protected"}
                )
                continue
            target_dir = contained_path(root, directory)
            if not target_dir.exists():
                continue
            for current, dirs, names in bounded_walk(target_dir, checkpoint=checkpoint):
                checkpoint()
                inspected += len(dirs) + len(names)
                if inspected > 600_000:
                    raise MigrationError("migration_scan_work_limit")
                for name in list(dirs):
                    child = Path(current) / name
                    relative = child.relative_to(root).as_posix()
                    if is_link(child) or boundary.protected(relative):
                        dirs.remove(name)
                    else:
                        directory_candidates.add(relative)
                for name in names:
                    target = Path(current) / name
                    path = target.relative_to(root).as_posix()
                    if is_link(target):
                        conflicts.append(
                            {"path": path, "reason": "migration_link_forbidden"}
                        )
                        continue
                    if (
                        path in selected
                        or path in removals
                        or boundary.protected(path)
                        or _database_file(target)
                        or _database_companion(target)
                    ):
                        continue
                    if category_for(path) not in options.categories:
                        continue
                    if (
                        category_for(path) == "configuration"
                        and not options.replace_configuration_scope
                    ):
                        continue
                    try:
                        contained_path(root, path, regular=True)
                        planned.add(path)
                        actions.append(
                            {
                                "path": path,
                                "action": "remove",
                                "before": _fingerprint(target, checkpoint),
                                "after": None,
                                "size": target.stat().st_size,
                            }
                        )
                        removals.add(path)
                    except MigrationError as error:
                        if error.code in {
                            "migration_cancelled",
                            "migration_budget_exhausted",
                        }:
                            raise
                        conflicts.append({"path": path, "reason": error.code})
        for path in options.exact_files:
            target = contained_path(root, path)
            if path in selected or path in removals or not target.exists():
                continue
            if boundary.protected(path) or category_for(path) not in options.categories:
                continue
            if (
                category_for(path) == "configuration"
                and not options.replace_configuration_scope
            ):
                continue
            try:
                planned.add(path)
                actions.append(
                    {
                        "path": path,
                        "action": "remove",
                        "before": _fingerprint(target, checkpoint),
                        "after": None,
                        "size": target.stat().st_size,
                    }
                )
                removals.add(path)
            except MigrationError as error:
                if error.code in {"migration_cancelled", "migration_budget_exhausted"}:
                    raise
                conflicts.append({"path": path, "reason": error.code})
        removed_files = {a["path"] for a in actions if a["action"] == "remove"}
        removable = set()
        selected_parents = {
            "/".join(parts[:index])
            for path in selected
            for parts in (path.split("/"),)
            for index in range(1, len(parts))
        }
        for relative in sorted(directory_candidates, key=lambda p: (-p.count("/"), p)):
            checkpoint()
            if relative in selected_parents:
                continue
            if category_for(relative + "/x") not in options.categories:
                continue
            if (
                category_for(relative + "/x") == "configuration"
                and not options.replace_configuration_scope
            ):
                continue
            directory = contained_path(root, relative)
            if all(
                child.relative_to(root).as_posix() in removed_files
                or child.relative_to(root).as_posix() in removable
                for child in directory.iterdir()
            ):
                info = directory.stat()
                removed_directories.append(
                    {"path": relative, "identity": [info.st_dev, info.st_ino]}
                )
                removable.add(relative)
    preview = {
        "package_id": manifest["package_id"],
        "actions": actions,
        "skipped": skipped,
        "conflicts": conflicts,
        "removed_directories": removed_directories,
        "options": {**asdict(options), "categories": sorted(options.categories)},
        "core_revision": boundary.revision,
        "core_boundary_digest": hashlib.sha256(
            "\n".join(sorted(boundary.tracked)).encode()
        ).hexdigest(),
        "scope_revision": _scope_revision(
            root, options.exact_directories, boundary, checkpoint
        ),
        "exact_file_state": {
            path: _fingerprint(contained_path(root, path), checkpoint)
            for path in options.exact_files
            if not boundary.protected(path)
        },
    }
    preview["target_revision"] = hashlib.sha256(
        json.dumps(preview, sort_keys=True).encode()
    ).hexdigest()
    return preview


def _prepare_parents(root: Path, relative: str, journal: FileJournal) -> None:
    parts = relative.split("/")[:-1]
    for count in range(1, len(parts) + 1):
        path = "/".join(parts[:count])
        directory = contained_path(root, path)
        if directory.exists():
            continue
        journal.append("directory_intent", {"path": path})
        directory.mkdir(mode=0o700)
        info = directory.stat()
        journal.append(
            "directory_created", {"path": path, "identity": [info.st_dev, info.st_ino]}
        )


def _atomic_copy(
    source: Path,
    destination: Path,
    *,
    before_replace=None,
    checkpoint=lambda: None,
) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    handle, name = tempfile.mkstemp(prefix=".zx-apply-", dir=destination.parent)
    temporary = Path(name)
    try:
        with os.fdopen(handle, "wb") as writer, source.open("rb") as reader:
            opened = os.fstat(reader.fileno())
            copied = 0
            while data := reader.read(CHUNK):
                checkpoint()
                copied += len(data)
                if copied > opened.st_size:
                    raise MigrationError("migration_source_changed")
                writer.write(data)
            writer.flush()
            os.fsync(writer.fileno())
            after = os.fstat(reader.fileno())
            if copied != opened.st_size or after.st_mtime_ns != opened.st_mtime_ns:
                raise MigrationError("migration_source_changed")
        checkpoint()
        if before_replace is not None:
            info = temporary.stat()
            before_replace([info.st_dev, info.st_ino])
        os.replace(temporary, destination)
    finally:
        temporary.unlink(missing_ok=True)


def apply_files(
    root: Path,
    staging: Path,
    plan: dict,
    journal: Path,
    *,
    expected_revision: str,
    destructive_confirmed: bool = False,
    checkpoint=lambda: None,
    rollback_checkpoint=lambda: None,
) -> dict:
    """Caller must hold the instance lease and keep all business writers stopped."""
    if plan["conflicts"]:
        raise MigrationError("migration_conflicts_unresolved", status=409)
    if plan["target_revision"] != expected_revision:
        raise MigrationError("migration_target_changed", status=409)
    if (
        any(a["action"] == "remove" for a in plan["actions"])
        or plan.get("removed_directories")
    ) and not destructive_confirmed:
        raise MigrationError("migration_destructive_confirmation_required")
    recheck_files(root, staging, plan, checkpoint=checkpoint)
    if journal.exists():
        raise MigrationError("migration_journal_exists", status=409)
    require_space(staging, sum(a["size"] for a in plan["actions"]) * 2)
    return _apply_checked_files(
        root,
        staging,
        plan,
        journal,
        expected_revision=expected_revision,
        checkpoint=checkpoint,
        rollback_checkpoint=rollback_checkpoint,
    )


def recheck_files(root: Path, staging: Path, plan: dict, *, checkpoint=lambda: None):
    """Revalidate the approved target and candidates before any mutation intent."""
    boundary = SourceBoundary.read(root)
    digest = hashlib.sha256("\n".join(sorted(boundary.tracked)).encode()).hexdigest()
    if (
        digest != plan["core_boundary_digest"]
        or boundary.revision != plan["core_revision"]
    ):
        raise MigrationError("migration_target_changed", status=409)
    if {
        path: _fingerprint(contained_path(root, path), checkpoint)
        for path in plan["options"].get("exact_files", [])
        if not boundary.protected(path)
    } != plan.get("exact_file_state", {}):
        raise MigrationError("migration_target_changed", status=409)
    if (
        _scope_revision(
            root, tuple(plan["options"]["exact_directories"]), boundary, checkpoint
        )
        != plan["scope_revision"]
    ):
        raise MigrationError("migration_target_changed", status=409)
    for action in plan["actions"]:
        checkpoint()
        target = contained_path(root, action["path"])
        if (
            boundary.protected(action["path"])
            or _fingerprint(target, checkpoint) != action["before"]
        ):
            raise MigrationError(
                "migration_target_changed", path=action["path"], status=409
            )
        if action["action"] != "remove":
            candidate = contained_path(staging, action["candidate"], regular=True)
            if file_hash(candidate, checkpoint) != action["after"]:
                raise MigrationError("migration_candidate_changed", status=409)


def _apply_checked_files(
    root, staging, plan, journal, *, expected_revision, checkpoint, rollback_checkpoint
):
    boundary = SourceBoundary.read(root)
    rollback = staging / "rollback"
    rollback.mkdir(mode=0o700)
    log = FileJournal(
        journal,
        {
            "stage": "applying_files",
            "package_id": plan["package_id"],
            "target_revision": expected_revision,
        },
    )
    state = log.state
    try:
        for index, action in enumerate(plan["actions"]):
            checkpoint()
            target = contained_path(root, action["path"])
            if _fingerprint(target, checkpoint) != action["before"]:
                raise MigrationError("migration_target_changed", status=409)
            backup = rollback / str(index)
            if action["before"] is not None:
                _atomic_copy(target, backup, checkpoint=checkpoint)
            receipt = {
                **action,
                "backup": backup.relative_to(staging).as_posix(),
                "index": index,
            }
            # Record the undo intent durably before touching the target.
            log.append("intent", receipt)
            if action["action"] == "remove":
                target.unlink()
            else:
                _prepare_parents(root, action["path"], log)
                _atomic_copy(
                    contained_path(staging, action["candidate"], regular=True),
                    target,
                    before_replace=lambda identity, index=index: log.append(
                        "materialized", {"index": index, "identity": identity}
                    ),
                    checkpoint=checkpoint,
                )
            log.append("applied", {"index": index})
        for index, directory in enumerate(plan.get("removed_directories", [])):
            checkpoint()
            target = contained_path(root, directory["path"])
            info = target.stat()
            if (
                boundary.protected(directory["path"])
                or [info.st_dev, info.st_ino] != directory["identity"]
                or not target.is_dir()
                or any(target.iterdir())
            ):
                raise MigrationError("migration_target_changed", status=409)
            backup = rollback / f"directory-{index}"
            log.append(
                "directory_remove_intent",
                {
                    **directory,
                    "backup": backup.relative_to(staging).as_posix(),
                },
            )
            os.replace(target, backup)
            log.append("directory_removed", {"path": directory["path"]})
        log.stage("files_applied_unverified")
        return state
    except BaseException as error:
        original_error = (
            error.code if isinstance(error, MigrationError) else type(error).__name__
        )
        log.stage("applying_files_failed", original_error=original_error)
        rollback_files(root, staging, journal, checkpoint=rollback_checkpoint)
        raise


def rollback_files(
    root: Path, staging: Path, journal: Path, *, checkpoint=lambda: None
) -> dict:
    boundary = SourceBoundary.read(root)
    log = FileJournal(journal)
    state = log.state
    if state.get("stage") == "files_rolled_back":
        return state

    def stage(name: str, **details) -> None:
        if state["schema"] == 1:
            state.update(stage=name, **details)
            write_json_locked(journal, state)
        else:
            log.stage(name, **details)

    stage("rolling_back_files")
    try:
        for directory in reversed(state.get("removed_directories", [])):
            checkpoint()
            if directory["state"] == "restored":
                continue
            if boundary.protected(directory["path"]):
                raise MigrationError("migration_rollback_target_protected")
            target = contained_path(root, directory["path"])
            backup = contained_path(staging, directory["backup"])
            if target.exists():
                info = target.stat()
                if (
                    backup.exists()
                    or not target.is_dir()
                    or [info.st_dev, info.st_ino] != directory["identity"]
                ):
                    raise MigrationError("migration_rollback_directory_conflict")
            else:
                if not backup.is_dir():
                    raise MigrationError("migration_rollback_directory_unconfirmed")
                info = backup.stat()
                if [info.st_dev, info.st_ino] != directory["identity"] or any(
                    backup.iterdir()
                ):
                    raise MigrationError("migration_rollback_directory_conflict")
                os.replace(backup, target)
            log.append("directory_restored", {"path": directory["path"]})
        for receipt in reversed(state["actions"]):
            checkpoint()
            if receipt["state"] == "rolled_back":
                continue
            if boundary.protected(receipt["path"]):
                raise MigrationError("migration_rollback_target_protected")
            target = contained_path(root, receipt["path"])
            current = _fingerprint(target, checkpoint)
            if (
                current != receipt["before"]
                and current == receipt["after"]
                and target.exists()
                and state["schema"] == 2
            ):
                info = target.stat()
                if receipt.get("after_identity") != [info.st_dev, info.st_ino]:
                    raise MigrationError(
                        "migration_rollback_identity_conflict", path=receipt["path"]
                    )
            if current == receipt["before"]:
                pass
            elif current != receipt["after"]:
                raise MigrationError(
                    "migration_rollback_conflict", path=receipt["path"]
                )
            elif receipt["before"] is None:
                target.unlink()
            else:
                backup = contained_path(staging, receipt["backup"], regular=True)
                if file_hash(backup, checkpoint) != receipt["before"]:
                    raise MigrationError("migration_rollback_backup_changed")
                _atomic_copy(backup, target, checkpoint=checkpoint)
            if state["schema"] == 1:
                receipt["state"] = "rolled_back"
                write_json_locked(journal, state)
            else:
                log.append("rolled_back", {"index": receipt["index"]})
        for directory in reversed(state.get("directories", [])):
            checkpoint()
            if directory["state"] == "rolled_back":
                continue
            path = contained_path(root, directory["path"])
            if path.exists():
                info = path.stat()
                if directory.get("identity") != [info.st_dev, info.st_ino]:
                    raise MigrationError("migration_rollback_directory_unconfirmed")
                try:
                    path.rmdir()
                except OSError:
                    raise MigrationError(
                        "migration_rollback_directory_conflict"
                    ) from None
            log.append("directory_rolled_back", {"path": directory["path"]})
        stage("files_rolled_back")
    except BaseException as error:
        rollback_error = (
            error.code if isinstance(error, MigrationError) else type(error).__name__
        )
        stage("recovery_required", rollback_error=rollback_error)
        raise
    return state
