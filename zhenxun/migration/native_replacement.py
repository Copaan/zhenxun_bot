from __future__ import annotations

from contextlib import asynccontextmanager
import hashlib
import json
from pathlib import Path

from zhenxun.utils.atomic_json import read_json_locked, write_json_locked

from .archive import file_hash
from .errors import MigrationError
from .native_database import DatabaseEndpoint, native_session, require_version
from .paths import contained_path
from .replacement_database import require_same_engine


def evidence_digests(evidence):
    return {
        name: hashlib.sha256(json.dumps(value, sort_keys=True).encode()).hexdigest()
        for name, value in evidence.items()
    }


@asynccontextmanager
async def clients(private, staging, budget, checkpoint):
    try:
        target = DatabaseEndpoint.parse(private["target_url"])
        candidate = DatabaseEndpoint.parse(private["candidate_url"])
    except (KeyError, TypeError):
        raise MigrationError("migration_database_credentials_required") from None
    require_same_engine(target.engine, candidate.engine)
    if (
        target.identity() == candidate.identity()
        or target.username == candidate.username
    ):
        raise MigrationError("migration_database_candidate_isolation_required")
    async with native_session(target, staging, budget, checkpoint) as target_client:
        async with native_session(
            candidate, staging, budget, checkpoint
        ) as candidate_client:
            yield target_client, candidate_client


def assert_identity(client, expected):
    if client.endpoint.identity() != expected:
        raise MigrationError("migration_database_target_changed", status=409)


async def verify_account_isolation(target, candidate):
    if target.endpoint.engine != "postgres":
        # MySQL inspect() already requires every effective schema grant to
        # name only this database, with no wildcard or role-based grants.
        return
    for client, other in ((target, candidate), (candidate, target)):
        name = other.endpoint.username.replace("'", "''")
        # CONNECT denial also covers table privileges granted without CREATE.
        accessible = await client.query(
            "SELECT COUNT(*) FROM pg_roles WHERE rolname='" + name + "' "
            "AND has_database_privilege(oid,current_database(),'CONNECT');"
        )
        if accessible != "0":
            raise MigrationError("migration_database_candidate_isolation_required")


async def snapshot_native(
    endpoint, staging, destination, *, budget, checkpoint, maximum
):
    async with native_session(endpoint, staging, budget, checkpoint) as client:
        before = await client.inspect()
        receipt = await client.dump(destination, maximum=maximum)
        after = await client.inspect()
        if before["revision"] != after["revision"]:
            write_json_locked(
                staging / "source-mismatch.json",
                {
                    "before": evidence_digests(before["evidence"]),
                    "after": evidence_digests(after["evidence"]),
                    "sequences_before": before["evidence"]["sequences"],
                    "sequences_after": after["evidence"]["sequences"],
                    "data_before": before["evidence"]["data"],
                    "data_after": after["evidence"]["data"],
                },
            )
            raise MigrationError("migration_database_source_changed")
        return {
            "engine": endpoint.engine,
            "engine_version": before["engine_version"],
            "revision": before["revision"],
            "tables": before["tables"],
            "evidence_sha256": evidence_digests(before["evidence"]),
            **receipt,
        }


async def prepare_native(
    staging: Path,
    source: Path,
    description: dict,
    *,
    private: dict,
    first_deployment: bool,
    source_trusted: bool,
    budget,
    checkpoint=lambda: None,
    live_analysis=False,
):
    if not source_trusted:
        raise MigrationError("migration_source_trust_required")
    if file_hash(source, checkpoint) != description["sha256"]:
        raise MigrationError("migration_database_payload_changed")
    async with clients(private, staging, budget, checkpoint) as (target, candidate):
        require_same_engine(description["engine"], target.endpoint.engine)
        expected_format = (
            "mysql_sql" if target.endpoint.engine == "mysql" else "postgres_custom"
        )
        if description.get("backup_format") != expected_format:
            raise MigrationError("migration_database_backup_format_invalid")
        original = (
            await target.inspect(quiet=False)
            if live_analysis
            else await target.inspect()
        )
        empty = await candidate.inspect()
        await verify_account_isolation(target, candidate)
        if (original["server"], original["database"]) == (
            empty["server"],
            empty["database"],
        ):
            raise MigrationError("migration_database_candidate_isolation_required")
        for observed in (original, empty):
            require_version(
                description["engine"],
                description["engine_version"],
                observed["engine_version"],
            )
        if empty["tables"] or empty["evidence"]["sequences"]:
            raise MigrationError("migration_database_candidate_not_empty")
        if first_deployment and (
            original["tables"] or original["evidence"]["sequences"]
        ):
            raise MigrationError("migration_database_not_empty")
        backup = staging / "database-target.backup"
        rollback = await target.dump(backup)
        checked = (
            await target.inspect(quiet=False)
            if live_analysis
            else await target.inspect()
        )
        if checked["revision"] != original["revision"]:
            raise MigrationError("migration_database_target_changed", status=409)
        await candidate.restore(source, confirmed_tables=[])
        verified = await candidate.inspect()
        if verified["revision"] != description.get("revision"):
            write_json_locked(
                staging / "candidate-mismatch.json",
                {
                    "expected_revision": description.get("revision"),
                    "observed_revision": verified["revision"],
                    "expected": description.get("evidence_sha256", {}),
                    "observed": evidence_digests(verified["evidence"]),
                },
            )
            raise MigrationError("migration_database_candidate_mismatch")
        plan = {
            "engine": target.endpoint.engine,
            "target": target.endpoint.identity(),
            "candidate": candidate.endpoint.identity(),
            "target_revision": original["revision"],
            "candidate_revision": verified["revision"],
            "source_sha256": description["sha256"],
            "backup_sha256": rollback["sha256"],
            "database": {"target_name": target.endpoint.database},
            "tables_before": original["tables"],
            "tables_after": verified["tables"],
        }
        write_json_locked(staging / "native-plan.json", plan)
        return plan


async def recheck_native(
    staging, source, plan, *, private, budget, checkpoint, live_analysis=False
):
    """Reject changed native data before touching configuration or files."""
    if file_hash(source, checkpoint) != plan["source_sha256"]:
        raise MigrationError("migration_database_payload_changed")
    backup = contained_path(staging, "database-target.backup", regular=True)
    if file_hash(backup, checkpoint) != plan["backup_sha256"]:
        raise MigrationError("migration_database_backup_changed")
    async with clients(private, staging, budget, checkpoint) as (target, candidate):
        assert_identity(target, plan["target"])
        assert_identity(candidate, plan["candidate"])
        before = (
            await target.inspect(quiet=False)
            if live_analysis
            else await target.inspect()
        )
        prepared = await candidate.inspect()
        await verify_account_isolation(target, candidate)
        if before["revision"] != plan["target_revision"]:
            raise MigrationError("migration_database_target_changed", status=409)
        if prepared["revision"] != plan["candidate_revision"]:
            raise MigrationError("migration_database_candidate_changed", status=409)


async def apply_native(
    staging,
    source,
    plan,
    journal,
    *,
    private,
    budget,
    confirmed_name,
    checkpoint=lambda: None,
):
    if confirmed_name != plan["database"]["target_name"]:
        raise MigrationError("migration_database_confirmation_required")
    if file_hash(source, checkpoint) != plan["source_sha256"]:
        raise MigrationError("migration_database_payload_changed")
    backup = contained_path(staging, "database-target.backup", regular=True)
    if file_hash(backup, checkpoint) != plan["backup_sha256"]:
        raise MigrationError("migration_database_backup_changed")
    async with clients(private, staging, budget, checkpoint) as (target, candidate):
        assert_identity(target, plan["target"])
        assert_identity(candidate, plan["candidate"])
        before, prepared = await target.inspect(), await candidate.inspect()
        await verify_account_isolation(target, candidate)
        if before["revision"] != plan["target_revision"]:
            raise MigrationError("migration_database_target_changed", status=409)
        if prepared["revision"] != plan["candidate_revision"]:
            raise MigrationError("migration_database_candidate_changed", status=409)
        write_json_locked(
            journal,
            {
                "state": "applying",
                "target": plan["target"],
                "before_revision": before["revision"],
            },
        )
        await target.restore(source, confirmed_tables=before["tables"])
        observed = await target.inspect()
        if observed["revision"] != plan["candidate_revision"]:
            raise MigrationError("migration_database_application_mismatch")
        write_json_locked(
            journal,
            {
                "state": "applied_unverified",
                "target": plan["target"],
                "after_revision": observed["revision"],
            },
        )
        return {"state": "applied_unverified"}


async def rollback_native(
    staging, plan, journal, *, private, budget, checkpoint=lambda: None
):
    state = read_json_locked(journal, None)
    if not isinstance(state, dict) or state.get("target") != plan["target"]:
        raise MigrationError("migration_database_rollback_unconfirmed")
    if state.get("state") == "rolled_back":
        return {"state": "rolled_back"}
    if state.get("state") not in {"applying", "applied_unverified", "rolling_back"}:
        raise MigrationError("migration_database_application_unconfirmed")
    try:
        endpoint = DatabaseEndpoint.parse(private["target_url"])
    except (KeyError, TypeError):
        raise MigrationError("migration_database_credentials_required") from None
    async with native_session(endpoint, staging, budget, checkpoint) as target:
        assert_identity(target, plan["target"])
        before = await target.inspect()
        backup = contained_path(staging, "database-target.backup", regular=True)
        if file_hash(backup, checkpoint) != plan["backup_sha256"]:
            raise MigrationError("migration_database_backup_changed")
        if before["revision"] == plan["target_revision"]:
            # A crash can occur before DDL or after restoring the original data,
            # but before the receipt. Matching the entire original scope is enough.
            write_json_locked(
                journal, {"state": "rolled_back", "target": plan["target"]}
            )
            return {"state": "rolled_back"}
        expected = state.get("after_revision", plan["candidate_revision"])
        if before["revision"] != expected or expected != plan["candidate_revision"]:
            # Partially applied DDL or unknown external writes are not owned
            # merely because an intent exists. Keep the instance in maintenance.
            raise MigrationError("migration_database_rollback_conflict")
        write_json_locked(
            journal,
            {
                "state": "rolling_back",
                "target": plan["target"],
                "after_revision": before["revision"],
            },
        )
        await target.restore(backup, confirmed_tables=before["tables"])
        restored = await target.inspect()
        if restored["revision"] != plan["target_revision"]:
            raise MigrationError("migration_database_rollback_mismatch")
        write_json_locked(journal, {"state": "rolled_back", "target": plan["target"]})
        return {"state": "rolled_back"}
