from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
import hashlib
import importlib.metadata
import json
from pathlib import Path
import sys
import tempfile
import time

from packaging.version import Version

from zhenxun.utils.atomic_json import read_json_locked

from .archive import file_hash
from .dependencies import _consistency, _installed
from .errors import MigrationError
from .generation import candidate_generation
from .paths import contained_path
from .tasks import MigrationBudget, TaskStore


def digest(value: dict) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True).encode()).hexdigest()


def _simple_values_preserved(baseline: Path, current: Path) -> None:
    from .configuration import yaml_document
    from .restore import _configuration_read

    before = yaml_document(_configuration_read(baseline).decode("utf-8-sig"))
    after = yaml_document(_configuration_read(current).decode("utf-8-sig"))
    for module, fields in before.items():
        observed = after.get(module)
        if isinstance(fields, dict) and isinstance(observed, dict):
            normalized = {str(key).upper(): value for key, value in observed.items()}
            for key, value in fields.items():
                name = str(key).upper()
                if (
                    name not in normalized
                    or type(value) is not type(normalized[name])
                    or value != normalized[name]
                ):
                    raise MigrationError("migration_configuration_value_changed")
        elif type(fields) is not type(observed) or fields != observed:
            raise MigrationError("migration_configuration_value_changed")


def _registry_values_preserved(project, baseline: Path, current: Path) -> None:
    from .configuration import yaml_document
    from .restore import _configuration_read

    original = yaml_document(_configuration_read(baseline).decode("utf-8-sig"))
    observed = yaml_document(_configuration_read(current).decode("utf-8-sig"))
    simple_path = contained_path(project, "data/config.yaml")
    simple = (
        yaml_document(_configuration_read(simple_path).decode("utf-8-sig"))
        if simple_path.is_file()
        else {}
    )

    def preserved(before, after):
        if isinstance(before, dict) and isinstance(after, dict):
            return all(
                key in after and preserved(value, after[key])
                for key, value in before.items()
            )
        return type(before) is type(after) and before == after

    for module, fields in original.items():
        if not isinstance(fields, dict):
            raise MigrationError("migration_configuration_registry_invalid")
        overrides = simple.get(module, {})
        overrides = (
            {str(key).upper(): value for key, value in overrides.items()}
            if isinstance(overrides, dict)
            else {}
        )
        for key, field in fields.items():
            if not isinstance(field, dict):
                raise MigrationError("migration_configuration_registry_invalid")
            expected = overrides.get(str(key).upper(), field.get("value"))
            # The legacy registry uses None for unset; authoritative explicit
            # settings remain protected by the separate config.yaml hash.
            if expected is None:
                continue
            observed_fields = observed.get(module, {})
            if not isinstance(observed_fields, dict):
                raise MigrationError("migration_configuration_registry_invalid")
            actual = observed_fields.get(str(key).upper(), {})
            if not isinstance(actual, dict):
                raise MigrationError("migration_configuration_registry_invalid")
            if not preserved(expected, actual.get("value")):
                raise MigrationError("migration_configuration_value_changed")


def verify_file_plan(project, plan: dict) -> tuple[list, list]:
    files = []
    for action in plan["actions"]:
        path = contained_path(project, action["path"])
        actual = file_hash(path) if path.is_file() else None
        if actual != action["after"] or (
            action["action"] == "remove" and path.exists()
        ):
            raise MigrationError("migration_restored_file_changed", path=action["path"])
        files.append(
            {"path": action["path"], "sha256": actual, "action": action["action"]}
        )
    for item in plan.get("removed_directories", []):
        if contained_path(project, item["path"]).exists():
            raise MigrationError(
                "migration_restored_directory_present", path=item["path"]
            )
    preserved = []
    for item in plan.get("skipped", []):
        if item["reason"] not in {"unchanged", "existing_file", "configuration_keep"}:
            continue
        path = contained_path(project, item["path"])
        actual = file_hash(path) if path.is_file() else None
        if "sha256" in item and actual != item["sha256"]:
            raise MigrationError("migration_preserved_file_changed", path=item["path"])
        preserved.append({**item, "sha256": actual})
    return files, preserved


def verify_restore(project, identity: str) -> dict:
    """Re-read mutable restore inputs; never trust a cached success boolean."""
    directory = TaskStore(project).path("jobs", identity).parent
    plan = read_json_locked(directory / "restore-plan.json", {})
    revision = digest({key: value for key, value in plan.items() if key != "revision"})
    if plan.get("job_id") != identity or plan.get("revision") != revision:
        raise MigrationError("migration_restore_plan_changed")
    application = read_json_locked(directory / "restore-application.json", {})
    if (
        application.get("job_id") != identity
        or application.get("revision") != revision
        or application.get("state") not in {"applied_unverified", "verified"}
    ):
        raise MigrationError("migration_restore_application_incomplete")
    file_plan = deepcopy(plan["files"])
    from .commit import read_commit

    committed = read_commit(TaskStore(project), identity)
    publication = read_json_locked(directory / "configuration-publication.json", {})
    for item in [*file_plan["actions"], *file_plan.get("skipped", [])]:
        if committed and publication:
            break
        if item["path"] not in {"data/config.yaml", "data/configs/plugins2config.yaml"}:
            continue
        expected_key = "after" if "action" in item else "sha256"
        baseline_name = item.get("candidate") or item.get("baseline")
        if not baseline_name or not item.get(expected_key):
            continue
        current = contained_path(project, item["path"], regular=True)
        actual = file_hash(current)
        if actual != item[expected_key]:
            baseline = contained_path(
                directory / "restore-stage", baseline_name, regular=True
            )
            if file_hash(baseline) != item[expected_key]:
                raise MigrationError("migration_configuration_baseline_changed")
            if item["path"] == "data/config.yaml":
                _simple_values_preserved(baseline, current)
            else:
                _registry_values_preserved(project, baseline, current)
            item[expected_key] = actual
    # A durable commit may already have rotated management credentials. Recovery
    # verifies those authorized bytes instead of demanding pre-publication bytes.
    if committed and publication:
        if (
            publication.get("job_id") != identity
            or publication.get("decided_at") != committed["decided_at"]
        ):
            raise MigrationError("migration_publication_receipt_invalid")
        entries = publication.get("entries", [])
        if len(entries) != 2 or {entry["path"] for entry in entries} != {
            "data/config.yaml",
            "data/configs/plugins2config.yaml",
        }:
            raise MigrationError("migration_publication_receipt_invalid")
        for entry in entries:
            path = contained_path(project, entry["path"], regular=True)
            actual = file_hash(path)
            if actual not in {entry["before"], entry["after"]}:
                raise MigrationError("migration_configuration_publication_conflict")
            for action in file_plan["actions"]:
                if action["path"] == entry["path"]:
                    action["after"] = actual
            for item in file_plan.get("skipped", []):
                if item["path"] == entry["path"]:
                    item["sha256"] = actual
    files, preserved = verify_file_plan(project, file_plan)
    result = read_json_locked(directory / "dependency-result.json", {})
    generation, candidate = candidate_generation(project, identity)
    if (
        result.get("state") != "prepared"
        or result.get("missing")
        or result.get("consistency")
        or result.get("generation") != generation["generation"]
        or result.get("installed") != _installed(candidate)
        or _consistency(
            candidate, result.get("core", {}), tuple(result.get("declarations", ()))
        )
    ):
        raise MigrationError("migration_dependencies_incomplete")
    database_revision = None
    database_plan = plan.get("database") or {}
    database = database_plan.get("database", {})
    if database.get("engine") == "sqlite":
        from .database import snapshot_sqlite
        from .replacement_database import sqlite_content_revision

        deadline = time.monotonic() + 30
        target = contained_path(project, database["target_path"], regular=True)
        with tempfile.TemporaryDirectory(
            prefix="verify-db-", dir=directory
        ) as temporary:
            snapshot = Path(temporary) / "snapshot.sqlite3"
            snapshot_sqlite(target, snapshot, deadline=deadline)

            def checkpoint():
                if time.monotonic() >= deadline:
                    raise MigrationError("migration_database_timeout")

            database_revision = sqlite_content_revision(snapshot, checkpoint=checkpoint)
    elif database_plan.get("engine") in {"mysql", "postgres"}:
        from .native_database import DatabaseEndpoint, native_session
        from .restore_phases import _database_configuration

        endpoint = DatabaseEndpoint.parse(_database_configuration(project))
        if endpoint.identity() != database_plan["target"]:
            raise MigrationError("migration_database_target_changed")

        async def inspect_native():
            from .progress import ConsoleProgress
            from .tasks import MigrationProgress

            store = TaskStore(project)
            reporter = MigrationProgress(store, identity, "restore_verify")
            console = ConsoleProgress()

            def diagnostic(value):
                store.record_database_diagnostic(identity, value)
                console.show(store.read("jobs", identity))

            async with native_session(
                endpoint,
                directory,
                MigrationBudget.start(30),
                diagnostic=diagnostic,
                progress=reporter.update,
                phase="restore_verify",
            ) as client:
                inspected = await client.inspect(quiet=False)
                return inspected["revision"]

        # Commit is also called from the launcher's running loop. Use an isolated
        # loop for the existing supervised native tools, never nested asyncio.run.
        with ThreadPoolExecutor(max_workers=1) as executor:
            database_revision = executor.submit(
                lambda: asyncio.run(inspect_native())
            ).result()
    evidence = {
        "schema": 2,
        "job_id": identity,
        "plan_revision": revision,
        "generation": generation["generation"],
        "dependency_tree_sha256": generation["tree_sha256"],
        "dependency_result_sha256": digest(result),
        "database_revision": database_revision,
        "files": files,
        "preserved": preserved,
        "excluded": [
            item
            for item in plan["files"].get("skipped", [])
            if item["reason"]
            not in {"unchanged", "existing_file", "configuration_keep"}
        ],
    }
    return {
        "schema": 2,
        "job_id": identity,
        "plan_revision": revision,
        "generation": generation["generation"],
        "sha256": digest(evidence),
        "file_count": len(files),
        "preserved_count": len(preserved),
        "database_revision": database_revision,
    }


async def verify_runtime_database() -> dict:
    from tortoise import Tortoise

    from zhenxun.services.db_context import database_ready

    if not database_ready():
        raise MigrationError("migration_database_not_ready")
    tables = []
    for app in Tortoise.apps.values():
        for model in app.values():
            # Compile through the actual backend to check every required model column.
            await model.all().limit(1).values_list(*model._meta.fields_db_projection)
            tables.append(model._meta.db_table)
    connection = Tortoise.get_connection("default")
    if not tables:
        raise MigrationError("migration_database_models_missing")
    dialect = connection.capabilities.dialect
    if dialect == "sqlite":
        rows = await connection.execute_query_dict("PRAGMA quick_check")
        if not rows or any(list(row.values()) != ["ok"] for row in rows):
            raise MigrationError("migration_database_integrity_failed")
        if await connection.execute_query_dict("PRAGMA foreign_key_check"):
            raise MigrationError("migration_database_reference_invalid")
    return {"dialect": dialect, "tables": sorted(set(tables)), "ready": True}


def verify_runtime_configuration(project) -> None:
    from io import StringIO

    from ruamel.yaml import YAML

    from zhenxun.configs.config import Config
    from zhenxun.utils.pydantic_compat import model_dump

    from .configuration import yaml_document
    from .restore import _configuration_read

    path = contained_path(project, "data/configs/plugins2config.yaml", regular=True)
    observed = yaml_document(_configuration_read(path).decode("utf-8-sig"))
    expected = {
        module: {
            key: model_dump(value, exclude={"type", "arg_parser"})
            for key, value in group.configs.items()
        }
        for module, group in Config._data.items()
    }
    if observed != expected:
        raise MigrationError("migration_configuration_runtime_mismatch")
    simple_path = contained_path(project, "data/config.yaml", regular=True)
    simple = yaml_document(_configuration_read(simple_path).decode("utf-8-sig"))
    for module, fields in simple.items():
        group = Config._data.get(module)
        if group is None or not isinstance(fields, dict):
            continue
        for key, value in fields.items():
            if str(key).upper() in group.configs:
                effective = Config.get_config(module, str(key), build_model=False)
                # Match the startup writer's YAML representation (CommentedMap,
                # scalar wrappers and tuples need not retain their Python type).
                serialized = StringIO()
                YAML(pure=True).dump({"value": effective}, serialized)
                effective = yaml_document(serialized.getvalue())["value"]
                if type(value) is not type(effective) or value != effective:
                    raise MigrationError("migration_configuration_runtime_mismatch")


def verify_runtime_dependencies(project, identity: str) -> None:
    directory = TaskStore(project).path("jobs", identity).parent
    result = read_json_locked(directory / "dependency-result.json", {})
    _, candidate = candidate_generation(project, identity)
    if _consistency(
        candidate,
        result.get("core", {}),
        tuple(result.get("declarations", ())),
        include_core=True,
    ):
        raise MigrationError("migration_dependency_effective_closure_invalid")
    for name, expected in {**result.get("core", {}), **result["installed"]}.items():
        try:
            distribution = importlib.metadata.distribution(name)
        except importlib.metadata.PackageNotFoundError:
            raise MigrationError("migration_dependency_import_unavailable") from None
        if Version(distribution.version) != Version(expected):
            raise MigrationError("migration_dependency_effective_version_changed")
        in_candidate = (
            Path(distribution.locate_file(""))
            .resolve()
            .is_relative_to(candidate.resolve())
        )
        if in_candidate != (name in result["installed"]):
            raise MigrationError("migration_dependency_import_origin_changed")
        expected_root = Path(distribution.locate_file("")).resolve()
        if name == "zhenxun-bot":
            expected_root = project.resolve()
        modules = set((distribution.read_text("top_level.txt") or "").splitlines())
        for file in distribution.files or ():
            if file.suffix != ".py" or any(
                not part.isidentifier() for part in file.parts[:-1]
            ):
                continue
            parts = list(file.parts[:-1])
            if file.stem != "__init__":
                parts.append(file.stem)
            if parts and all(part.isidentifier() for part in parts):
                modules.add(".".join(parts))
        for name in modules:
            module = sys.modules.get(name.strip())
            origin = getattr(module, "__file__", None)
            if origin and not Path(origin).resolve().is_relative_to(expected_root):
                raise MigrationError("migration_dependency_import_origin_changed")
