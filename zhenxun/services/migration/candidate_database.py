from __future__ import annotations

from copy import deepcopy
from dataclasses import asdict
import re
import time
from urllib.parse import urlsplit, urlunsplit
import uuid

from zhenxun.utils.atomic_json import read_json_locked, write_json_locked

from .errors import MigrationError
from .native_database import DatabaseEndpoint, native_session


def _path(store, identity):
    return store.path("preflights", identity).parent / "candidate-resource.json"


def _save(store, identity, value):
    value["updated_at"] = time.time()
    write_json_locked(_path(store, identity), value)


def public_resource(store, identity):
    value = read_json_locked(_path(store, identity), {})
    return {
        key: value[key]
        for key in (
            "name",
            "state",
            "created_at",
            "updated_at",
            "cleanup_error",
            "creation_error",
            "target",
        )
        if key in value
    }


def record_prepared(store, identity, plan):
    path = _path(store, identity)
    record = read_json_locked(path, {})
    if not record:
        return
    record["candidate_revision"] = plan["candidate_revision"]
    record["revision_algorithm"] = plan.get("revision_algorithm", "native-content-v1")
    _save(store, identity, record)


def connection_binding(url, root):
    endpoint = DatabaseEndpoint.parse(url, root=root)
    from .database_capabilities import CAPABILITY_POLICY_VERSION

    value = {
        **asdict(endpoint),
        "certificates": endpoint.certificate_hashes(),
        "policy_version": CAPABILITY_POLICY_VERSION,
    }
    from .tasks import TaskStore

    return TaskStore(root).private_fingerprint(value)


def pending_resources(store):
    """List non-secret resource receipts even when their preflight has expired."""
    from .retention import _records

    items = []
    for preflight in _records(store, "preflights"):
        resource = public_resource(store, preflight["id"])
        if resource and resource.get("state") != "cleaned":
            items.append(
                {
                    **resource,
                    "preflight_id": preflight["id"],
                    "first_deployment": preflight["options"].get(
                        "first_deployment", False
                    ),
                    "expired": preflight.get("expires_at", 0) <= store.clock(),
                    "needs_authorization": True,
                    "stage": preflight.get("stage", "unknown"),
                    "job_id": preflight.get("job_id"),
                }
            )
    return items


def resolved_private(store, identity, private):
    """Reconstruct the owned candidate connection without persisting credentials."""
    value = deepcopy(private)
    database = value.get("database") or {}
    mode = database.get("candidate_mode", "manual")
    if mode not in {"manual", "auto"}:
        raise MigrationError("migration_candidate_mode_invalid")
    if mode == "manual":
        if _path(store, identity).exists():
            raise MigrationError("migration_database_preflight_stale", status=409)
        return value
    record = read_json_locked(_path(store, identity), {})
    url = database.get("target_url", "")
    if record.get("binding") != connection_binding(url, store.project):
        raise MigrationError("migration_database_connection_changed", status=409)
    if record.get("state") not in {"created", "cleanup_failed"}:
        raise MigrationError("migration_candidate_creation_unconfirmed", status=409)
    parsed = urlsplit(url)
    database["candidate_url"] = urlunsplit(parsed._replace(path="/" + record["name"]))
    value["database"] = database
    return value


async def prepare_candidate(store, identity, private, budget):
    """Create one task-owned database, retaining ambiguous creation as evidence."""
    database = private.get("database") or {}
    if database.get("candidate_mode", "manual") != "auto":
        return resolved_private(store, identity, private)
    target = DatabaseEndpoint.parse(database.get("target_url", ""), root=store.project)
    path = _path(store, identity)
    if path.exists():
        return resolved_private(store, identity, private)
    record = {
        "schema": 1,
        "task_id": identity,
        "name": "zx_candidate_" + uuid.uuid4().hex,
        "state": "creating",
        "binding": connection_binding(database["target_url"], store.project),
        "target": target.public(),
        "created_at": time.time(),
    }
    _save(store, identity, record)
    directory = path.parent / "candidate-tools"

    def diagnostic(value):
        record["database_diagnostic"] = value
        try:
            _save(store, identity, record)
        except Exception:
            pass

    try:
        async with native_session(
            target,
            directory,
            budget,
            budget.checkpoint,
            phase="candidate_create",
            diagnostic=diagnostic,
        ) as client:
            record["target_identity"] = await client.actual_identity()
            _save(store, identity, record)
            name = record["name"]
            sql = (
                f'CREATE DATABASE "{name}" TEMPLATE template0;'
                if target.engine == "postgres"
                else f"CREATE DATABASE `{name}` CHARACTER SET utf8mb4;"
            )
            await client.query(sql)
            record["state"] = "created"
            _save(store, identity, record)
        value = resolved_private(store, identity, private)
        candidate = DatabaseEndpoint.parse(
            value["database"]["candidate_url"], root=store.project
        )
        async with native_session(
            candidate, directory, budget, budget.checkpoint, phase="candidate_create"
        ) as client:
            record["actual_identity"] = await client.actual_identity()
            _save(store, identity, record)
        return value
    except BaseException as error:
        record["creation_error"] = getattr(
            error, "code", "migration_candidate_creation_interrupted"
        )
        try:
            _save(store, identity, record)
        except Exception:
            pass
        if isinstance(error, MigrationError):
            error.details.update(
                phase="candidate_create",
                candidate_resource={
                    key: record[key]
                    for key in (
                        "name",
                        "state",
                        "created_at",
                        "creation_error",
                        "target",
                    )
                    if key in record
                },
                suggestion="无法自动建库时，可改为手动指定独立空候选库",
                database_diagnostic=record.get("database_diagnostic"),
            )
        raise


async def cleanup_candidate(store, identity, private, budget):
    """Drop only a confirmed task-owned candidate after its consumers finish."""
    record = read_json_locked(_path(store, identity), {})
    if not record or record.get("state") == "cleaned":
        return public_resource(store, identity)
    preflight = store.read("preflights", identity)
    if job_id := preflight.get("job_id"):
        job = store.read("jobs", job_id)
        if job["stage"] not in {
            "completed",
            "partial",
            "rolled_back",
            "failed",
            "cancelled",
            "needs_preflight",
        }:
            raise MigrationError("migration_candidate_in_use", status=409)
    try:
        target_url = (private.get("database") or {}).get("target_url", "")
        name = record.get("name", "")
        if not re.fullmatch(r"zx_candidate_[a-f0-9]{32}", name):
            raise MigrationError("migration_candidate_creation_unconfirmed", status=409)
        target = DatabaseEndpoint.parse(target_url, root=store.project)
        directory = _path(store, identity).parent / "candidate-tools"
        async with native_session(
            target, directory, budget, budget.checkpoint, phase="candidate_cleanup"
        ) as client:
            if record.get("target_identity") != await client.actual_identity():
                raise MigrationError("migration_database_target_changed", status=409)
            present = await client.query(
                f"SELECT COUNT(*) FROM pg_database WHERE datname='{name}';"
                if target.engine == "postgres"
                else "SELECT COUNT(*) FROM information_schema.SCHEMATA "
                f"WHERE SCHEMA_NAME='{name}';"
            )
            if present == "0":
                record["state"] = "cleaned"
                record.pop("cleanup_error", None)
                _save(store, identity, record)
                return public_resource(store, identity)
            if record.get("state") == "dropping" and record.get("actual_identity"):
                record["state"] = "created"
                _save(store, identity, record)
        if not record.get("actual_identity"):
            raise MigrationError("migration_candidate_creation_unconfirmed", status=409)
        parsed = urlsplit(target_url)
        candidate = DatabaseEndpoint.parse(
            urlunsplit(parsed._replace(path="/" + name)), root=store.project
        )
        async with native_session(
            candidate, directory, budget, budget.checkpoint, phase="candidate_cleanup"
        ) as client:
            observed = await client.actual_identity()
            if (
                observed != record["actual_identity"]
                or observed == record["target_identity"]
            ):
                raise MigrationError("migration_database_target_changed", status=409)
            client.revision_algorithm = record.get(
                "revision_algorithm", "native-content-v1"
            )
            contents = await client.inspect()
            if record.get("candidate_revision"):
                if contents["revision"] != record["candidate_revision"]:
                    raise MigrationError(
                        "migration_database_candidate_changed", status=409
                    )
            elif (
                contents["tables"]
                or contents.get("objects")
                or contents["evidence"].get("sequences")
            ):
                raise MigrationError(
                    "migration_candidate_creation_unconfirmed", status=409
                )
        async with native_session(
            target, directory, budget, budget.checkpoint, phase="candidate_cleanup"
        ) as client:
            if await client.actual_identity() != record["target_identity"]:
                raise MigrationError("migration_database_target_changed", status=409)
            name = record["name"]
            if name != candidate.database or not name.startswith("zx_candidate_"):
                raise MigrationError(
                    "migration_candidate_creation_unconfirmed", status=409
                )
            record["state"] = "dropping"
            _save(store, identity, record)
            await client.query(
                f'DROP DATABASE "{name}";'
                if target.engine == "postgres"
                else f"DROP DATABASE `{name}`;"
            )
        record["state"] = "cleaned"
        record.pop("cleanup_error", None)
        _save(store, identity, record)
    except BaseException as error:
        # A drop with an unknown result must not be blindly repeated.
        if record.get("state") != "dropping":
            record["state"] = (
                "cleanup_failed" if record.get("actual_identity") else record["state"]
            )
        record["cleanup_error"] = getattr(
            error, "code", "migration_candidate_cleanup_interrupted"
        )
        try:
            _save(store, identity, record)
        except Exception:
            pass
        raise
    return public_resource(store, identity)
