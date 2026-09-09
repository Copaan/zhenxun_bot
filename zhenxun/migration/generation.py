from __future__ import annotations

import hashlib
import importlib
import json
from pathlib import Path
import shutil
import sys

from zhenxun.utils.atomic_json import read_json_locked, write_json_locked

from .access import private_directory
from .archive import file_hash
from .dependencies import _installed
from .discovery import bounded_walk
from .errors import MigrationError
from .paths import contained_path
from .tasks import TaskStore

_activated: dict[str, dict] = {}


def tree_revision(root: Path, checkpoint=lambda: None) -> str:
    files = []
    for directory, directories, names in bounded_walk(root, checkpoint=checkpoint):
        for name in directories:
            contained_path(root, (Path(directory) / name).relative_to(root).as_posix())
        for name in names:
            checkpoint()
            path = Path(directory) / name
            relative = path.relative_to(root).as_posix()
            # Python may create bytecode while validating an otherwise immutable layer.
            if "__pycache__" in path.parts or path.suffix == ".pyc":
                continue
            contained_path(root, relative, regular=True)
            files.append((relative, file_hash(path, checkpoint)))
            if len(files) > 200_000:
                raise MigrationError("migration_dependency_inventory_limit")
    return hashlib.sha256(json.dumps(sorted(files)).encode()).hexdigest()


def stage_generation(
    project, identity, candidate, *, core, lease, checkpoint=lambda: None
):
    lease.require_held()
    store = TaskStore(project)
    directory = store.path("jobs", identity).parent
    candidate = contained_path(directory, candidate.relative_to(directory).as_posix())
    if not candidate.is_dir() or any(name in core for name in _installed(candidate)):
        raise MigrationError("migration_core_dependency_conflict")
    manifest_path = contained_path(
        project, "data/runtime/nonebot-store/manifest-v1.json"
    )
    previous = file_hash(manifest_path) if manifest_path.is_file() else None
    layers = private_directory(
        contained_path(project, "data/runtime/nonebot-site-packages")
    )
    numbers = [
        int(p.name[11:]) for p in layers.glob("generation-*") if p.name[11:].isdigit()
    ]
    current = read_json_locked(manifest_path, {}).get("active_generation")
    if current is not None:
        if type(current) is not int or current < 0:
            raise MigrationError("migration_dependency_layer_invalid")
        numbers.append(current)
    generation = max(numbers, default=0) + 1
    target = contained_path(layers, f"generation-{generation}")
    receipt_path = directory / "dependency-generation.json"
    if receipt_path.exists():
        raise MigrationError("migration_dependency_generation_already_staged")
    backup = directory / "dependency-manifest-before.json"
    if previous is not None:
        shutil.copyfile(manifest_path, backup)
        if file_hash(backup) != previous:
            raise MigrationError("migration_dependency_manifest_changed")
    receipt = {
        "schema": 1,
        "job_id": identity,
        "generation": generation,
        "previous_sha256": previous,
        "tree_sha256": tree_revision(candidate, checkpoint),
        "state": "staging",
    }
    write_json_locked(receipt_path, receipt)
    candidate.rename(target)
    receipt["state"] = "staged"
    write_json_locked(receipt_path, receipt)
    return receipt


def candidate_generation(project: Path, identity: str) -> tuple[dict, Path]:
    directory = TaskStore(project).path("jobs", identity).parent
    receipt = read_json_locked(directory / "dependency-generation.json", None)
    if (
        not isinstance(receipt, dict)
        or receipt.get("job_id") != identity
        or type(receipt.get("generation")) is not int
        or receipt["generation"] < 1
        or receipt.get("state") not in {"staged", "publishing", "published"}
    ):
        raise MigrationError("migration_dependency_generation_unavailable")
    root = contained_path(
        project,
        f"data/runtime/nonebot-site-packages/generation-{receipt['generation']}",
    )
    if not root.is_dir() or tree_revision(root) != receipt["tree_sha256"]:
        raise MigrationError("migration_dependency_generation_changed")
    return receipt, root


def activate_candidate(project: Path, identity: str) -> Path:
    receipt, candidate = candidate_generation(project, identity)
    layer_root = contained_path(project, "data/runtime/nonebot-site-packages")
    sys.path[:] = [
        p for p in sys.path if not Path(p).absolute().is_relative_to(layer_root)
    ]
    sys.path.insert(0, str(candidate))
    importlib.invalidate_caches()
    _activated[identity] = dict(receipt)
    return candidate


def activated_generation(identity: str) -> dict:
    if identity not in _activated:
        raise MigrationError("migration_dependency_activation_unconfirmed")
    return dict(_activated[identity])


def publish_generation(project: Path, identity: str, *, lease) -> dict:
    from .commit import read_commit

    lease.require_held()
    store = TaskStore(project)
    if read_commit(store, identity) is None:
        raise MigrationError("migration_commit_receipt_missing")
    receipt, _ = candidate_generation(project, identity)
    directory = store.path("jobs", identity).parent
    manifest_path = contained_path(
        project, "data/runtime/nonebot-store/manifest-v1.json"
    )
    actual = file_hash(manifest_path) if manifest_path.exists() else None
    if actual == receipt.get("published_sha256") and actual is not None:
        receipt["state"] = "published"
        write_json_locked(directory / "dependency-generation.json", receipt)
        return receipt
    if actual != receipt["previous_sha256"]:
        raise MigrationError("migration_dependency_manifest_changed")
    manifest = read_json_locked(manifest_path, {"version": 1, "plugins": {}})
    manifest["previous_generation"] = manifest.get("active_generation")
    manifest["active_generation"] = receipt["generation"]
    # Persist expected bytes to identify an interrupted pointer change.
    prepared = directory / "dependency-manifest-after.json"
    write_json_locked(prepared, manifest)
    receipt.update(state="publishing", published_sha256=file_hash(prepared))
    write_json_locked(directory / "dependency-generation.json", receipt)
    private_directory(manifest_path.parent)
    temporary = contained_path(manifest_path.parent, f"migration-{identity}.tmp")
    if temporary.exists():
        if file_hash(temporary) != receipt["published_sha256"]:
            raise MigrationError("migration_dependency_manifest_changed")
    else:
        with temporary.open("xb") as stream:
            import os

            stream.write(prepared.read_bytes())
            stream.flush()
            os.fsync(stream.fileno())
    temporary.replace(manifest_path)
    receipt["state"] = "published"
    write_json_locked(directory / "dependency-generation.json", receipt)
    return receipt


def abandon_generation(project: Path, identity: str, *, lease) -> dict:
    from .commit import read_commit

    lease.require_held()
    store = TaskStore(project)
    if read_commit(store, identity) is not None:
        raise MigrationError("migration_committed_rollback_forbidden")
    directory = store.path("jobs", identity).parent
    receipt = read_json_locked(directory / "dependency-generation.json", None)
    if receipt is None:
        return {"state": "not_staged"}
    manifest = contained_path(project, "data/runtime/nonebot-store/manifest-v1.json")
    actual = file_hash(manifest) if manifest.exists() else None
    if receipt.get("job_id") != identity or actual != receipt.get("previous_sha256"):
        raise MigrationError("migration_dependency_manifest_changed")
    receipt["state"] = "abandoned"
    write_json_locked(directory / "dependency-generation.json", receipt)
    # Retain candidate evidence until the successful rollback retention deadline.
    return {"state": "abandoned", "generation": receipt["generation"]}
