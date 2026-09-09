from __future__ import annotations

import hashlib
from io import StringIO
import os
from pathlib import Path
import secrets

from ruamel.yaml import YAML

from zhenxun.utils.atomic_json import read_json_locked, write_json_locked

from .archive import file_hash
from .commit import read_commit
from .configuration import yaml_document
from .errors import MigrationError
from .generation import publish_generation
from .paths import contained_path
from .restore import _configuration_read
from .tasks import TaskStore


def publish_restore(
    project: Path, identity: str, *, lease, checkpoint=lambda: None
) -> dict:
    """Finish a committed restore without reopening business or accepting rollback."""
    lease.require_held()
    store = TaskStore(project)
    decision = read_commit(store, identity)
    if decision is None:
        raise MigrationError("migration_commit_receipt_missing")
    directory = store.path("jobs", identity).parent
    publication = directory / "restore-publication.json"
    previous = read_json_locked(publication, None)
    if previous is not None:
        if (
            previous.get("job_id") != identity
            or previous.get("decided_at") != decision["decided_at"]
            or previous.get("state") != "published"
            or previous.get("session_rotated") is not True
            or previous.get("configuration_published") is not True
        ):
            raise MigrationError("migration_publication_receipt_invalid")
        intent = read_json_locked(directory / "configuration-publication.json", {})
        if (
            intent.get("job_id") != identity
            or intent.get("decided_at") != decision["decided_at"]
            or len(intent.get("entries", [])) != 2
        ):
            raise MigrationError("migration_publication_receipt_invalid")
        for entry in intent["entries"]:
            if (
                file_hash(
                    contained_path(project, entry["path"], regular=True), checkpoint
                )
                != entry["after"]
            ):
                raise MigrationError("migration_configuration_publication_conflict")
        generation = publish_generation(project, identity, lease=lease)
        if generation["generation"] != previous.get("generation"):
            raise MigrationError("migration_dependency_manifest_changed")
        return previous
    intent_path = directory / "configuration-publication.json"
    intent = read_json_locked(intent_path, None)
    if intent is None:
        simple = contained_path(project, "data/config.yaml", regular=True)
        plugins = contained_path(
            project, "data/configs/plugins2config.yaml", regular=True
        )
        before_simple = _configuration_read(simple)
        before_plugins = _configuration_read(plugins)
        config = yaml_document(before_simple.decode("utf-8-sig"))
        registry = yaml_document(before_plugins.decode("utf-8-sig"))
        group = config.get("web-ui")
        if (
            not isinstance(group, dict)
            or not group.get("USERNAME")
            or not group.get("PASSWORD")
        ):
            raise MigrationError("migration_administrator_confirmation_required")
        group["SECRET"] = secrets.token_urlsafe(48)
        plugin_group = registry.setdefault("web-ui", {})
        for key in ("USERNAME", "PASSWORD", "SECRET"):
            entry = plugin_group.setdefault(key, {})
            if not isinstance(entry, dict):
                raise MigrationError("migration_configuration_invalid")
            entry["value"] = group[key]
        entries = []
        for name, target, value, original in (
            ("simple", simple, config, before_simple),
            ("plugins", plugins, registry, before_plugins),
        ):
            checkpoint()
            stream = StringIO()
            YAML().dump(value, stream)
            staged = directory / ("publish-" + name + ".yaml")
            with staged.open("w", encoding="utf-8", newline="") as output:
                output.write(stream.getvalue())
                output.flush()
                os.fsync(output.fileno())
            entries.append(
                {
                    "path": target.relative_to(project).as_posix(),
                    "staged": staged.name,
                    "before": hashlib.sha256(original).hexdigest(),
                    "after": file_hash(staged, checkpoint),
                }
            )
        intent = {
            "job_id": identity,
            "decided_at": decision["decided_at"],
            "entries": entries,
        }
        write_json_locked(intent_path, intent)
    if (
        intent.get("job_id") != identity
        or intent.get("decided_at") != decision["decided_at"]
    ):
        raise MigrationError("migration_publication_receipt_invalid")
    for entry in intent["entries"]:
        checkpoint()
        target = contained_path(project, entry["path"], regular=True)
        current = file_hash(target, checkpoint)
        if current == entry["after"]:
            continue
        if current != entry["before"]:
            raise MigrationError("migration_configuration_publication_conflict")
        staged = contained_path(directory, entry["staged"], regular=True)
        if file_hash(staged, checkpoint) != entry["after"]:
            raise MigrationError("migration_configuration_publication_changed")
        temporary = contained_path(target.parent, f"migration-{identity}.tmp")
        if temporary.exists():
            if file_hash(temporary, checkpoint) != entry["after"]:
                raise MigrationError("migration_configuration_publication_conflict")
        else:
            with temporary.open("xb") as output:
                output.write(_configuration_read(staged))
                output.flush()
                os.fsync(output.fileno())
        temporary.replace(target)
    generation = publish_generation(project, identity, lease=lease)
    result = {
        "job_id": identity,
        "decided_at": decision["decided_at"],
        "generation": generation["generation"],
        "session_rotated": True,
        "configuration_published": True,
        "state": "published",
    }
    write_json_locked(publication, result)
    return result
