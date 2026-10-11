from __future__ import annotations

from contextlib import asynccontextmanager
import hashlib
import json
from pathlib import Path

from zhenxun.utils.atomic_json import read_json_locked, write_json_locked

from .archive import file_hash
from .database_capabilities import CAPABILITY_POLICY_VERSION
from .errors import MigrationError
from .native_database import DatabaseEndpoint, native_session, require_version
from .paths import contained_path
from .replacement_database import require_same_engine


def evidence_digests(evidence):
    return {
        name: hashlib.sha256(json.dumps(value, sort_keys=True).encode()).hexdigest()
        for name, value in evidence.items()
    }


def record_native_state(path, value):
    """Keep acknowledged object operations when advancing a restore receipt."""
    previous = read_json_locked(path, {})
    write_json_locked(path, {**previous, **value})


def verify_phase_payloads(staging, plan, checkpoint):
    for item in plan.get("mysql_phases") or []:
        path = contained_path(staging, item["payload"], regular=True)
        if file_hash(path, checkpoint) != item["sha256"]:
            raise MigrationError("migration_database_payload_changed")


def revision_matches(observed, revision, events=None):
    return observed["revision"] == revision and (
        events is None or observed.get("events", []) == events
    )


@asynccontextmanager
async def clients(
    private,
    staging,
    budget,
    checkpoint,
    *,
    diagnostic=None,
    progress=None,
    phase="database",
    capability="export",
    root=None,
):
    try:
        target = DatabaseEndpoint.parse(private["target_url"], root=root)
        candidate = DatabaseEndpoint.parse(private["candidate_url"], root=root)
    except (KeyError, TypeError):
        raise MigrationError("migration_database_credentials_required") from None
    require_same_engine(target.engine, candidate.engine)
    if target.identity() == candidate.identity():
        raise MigrationError("migration_database_candidate_isolation_required")
    async with native_session(
        target,
        staging,
        budget,
        checkpoint,
        diagnostic=diagnostic,
        progress=progress,
        phase=phase,
        capability=capability,
        account_role="target",
    ) as target_client:
        async with native_session(
            candidate,
            staging,
            budget,
            checkpoint,
            diagnostic=diagnostic,
            progress=progress,
            phase=phase,
            capability=capability,
            account_role="candidate",
        ) as candidate_client:
            yield target_client, candidate_client


def assert_identity(client, expected):
    if client.endpoint.identity() != expected:
        raise MigrationError("migration_database_target_changed", status=409)


async def verify_account_isolation(target, candidate):
    """Require distinct actual databases; shared accounts and grants are allowed."""
    identities = [await client.actual_identity() for client in (target, candidate)]
    if identities[0] == identities[1]:
        target._record_policy_diagnostic(
            error_code="migration_database_candidate_isolation_required",
            isolation_reasons=["same_database"],
            capability="restore",
        )
        raise MigrationError("migration_database_candidate_isolation_required")
    return identities


async def verify_prepared_identity(target, candidate, plan):
    """Bind destructive work to the databases inspected by this policy version."""
    if plan.get("policy_version") not in {2, CAPABILITY_POLICY_VERSION}:
        raise MigrationError("migration_database_preflight_stale", status=409)
    identities = await verify_account_isolation(target, candidate)
    if identities != plan.get("actual_databases"):
        raise MigrationError("migration_database_target_changed", status=409)


async def snapshot_native(
    endpoint,
    staging,
    destination,
    *,
    budget,
    checkpoint,
    maximum,
    diagnostic=None,
    progress=None,
):
    async with native_session(
        endpoint,
        staging,
        budget,
        checkpoint,
        diagnostic=diagnostic,
        progress=progress,
        phase="export_snapshot",
    ) as client:
        warnings = []

        async def inspect_backup():
            records = []
            publish = client.diagnostic
            client.diagnostic = records.append
            try:
                return await client.inspect()
            except MigrationError as error:
                if error.code not in {
                    "migration_database_tool_failed",
                    "migration_database_objects_unsupported",
                    "migration_database_analysis_limit",
                    "migration_database_row_analysis_limit",
                }:
                    raise
                warnings.append(
                    {
                        "code": error.code,
                        "phase": "backup_revision",
                        "diagnostic": next(
                            (r for r in reversed(records) if r.get("error_code")), None
                        ),
                    }
                )
                return None
            finally:
                client.diagnostic = publish

        before = await inspect_backup()
        receipt = await client.dump(destination, maximum=maximum)
        after = await inspect_backup()
        if (
            before
            and after
            and not revision_matches(after, before["revision"], before.get("events"))
        ):
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
        verified = before is not None and after is not None
        before = (
            before
            or after
            or {
                "engine_version": await client.query(
                    "SELECT VERSION();"
                    if endpoint.engine == "mysql"
                    else "SHOW server_version;"
                ),
                "revision": None,
                "tables": [],
                "evidence": {},
            }
        )
        return {
            "engine": endpoint.engine,
            "engine_version": before["engine_version"],
            "source_database": endpoint.database,
            "revision_algorithm": before.get("revision_algorithm", "native-content-v3"),
            "objects": before.get("objects", []),
            "schemas": before.get("schemas"),
            "events": before.get("events", []),
            "backup_verified": True,
            "restore_verified": False,
            "revision": before["revision"] if verified else None,
            "revision_verified": verified,
            "inspection_warnings": warnings,
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
    diagnostic=None,
    progress=None,
):
    if not source_trusted:
        raise MigrationError("migration_source_trust_required")
    if file_hash(source, checkpoint) != description["sha256"]:
        raise MigrationError("migration_database_payload_changed")
    async with clients(
        private,
        staging,
        budget,
        checkpoint,
        diagnostic=diagnostic,
        progress=progress,
        phase="restore_prepare",
        capability="restore",
    ) as (target, candidate):
        require_same_engine(description["engine"], target.endpoint.engine)
        expected_format = (
            "mysql_sql" if target.endpoint.engine == "mysql" else "postgres_custom"
        )
        if description.get("backup_format") != expected_format:
            raise MigrationError("migration_database_backup_format_invalid")
        dependencies = [
            item
            for item in description.get("objects", [])
            if item.get("kind") == "subscription"
        ]
        if dependencies:
            raise MigrationError(
                "migration_database_external_dependency",
                details={
                    "objects": dependencies,
                    "phase": "restore_prepare",
                },
            )
        target.revision_algorithm = candidate.revision_algorithm = description.get(
            "revision_algorithm", "native-content-v1"
        )
        original = (
            await target.inspect(quiet=False)
            if live_analysis
            else await target.inspect()
        )
        empty = await candidate.inspect()
        restore_privileges = None
        if target.endpoint.engine == "mysql":
            from .native_restore import mysql_restore_script

            preview = staging / "restore-capabilities.sql"
            restore_privileges = mysql_restore_script(
                source,
                preview,
                database=target.endpoint.database,
                source_database=description.get("source_database"),
                checkpoint=checkpoint,
            )
            preview.unlink()
            target.restore_privileges = candidate.restore_privileges = (
                restore_privileges
            )
        await target.check_restore_privileges(tables=description["tables"])
        await candidate.check_restore_privileges(tables=description["tables"])
        identities = await verify_account_isolation(target, candidate)
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
        if (
            empty["tables"]
            or empty["evidence"]["sequences"]
            or empty.get("objects")
            or empty["evidence"].get("extensions")
            or empty["evidence"].get("large_objects")
            or any(row["name"] != "public" for row in empty.get("schemas", []))
        ):
            raise MigrationError("migration_database_candidate_not_empty")
        if first_deployment and (
            original["tables"]
            or original["evidence"]["sequences"]
            or original.get("objects")
            or original["evidence"].get("extensions")
            or original["evidence"].get("large_objects")
            or any(row["name"] != "public" for row in original.get("schemas", []))
        ):
            raise MigrationError("migration_database_not_empty")
        backup = staging / "database-target.backup"
        rollback = await target.dump(backup)
        if restore_privileges is not None:
            restore_privileges |= mysql_restore_script(
                backup,
                preview,
                database=target.endpoint.database,
                source_database=target.endpoint.database,
                checkpoint=checkpoint,
            )
            preview.unlink()
            target.restore_privileges = restore_privileges
            await target.check_restore_privileges(
                tables=set(original["tables"]) | set(description["tables"])
            )
        checked = (
            await target.inspect(quiet=False)
            if live_analysis
            else await target.inspect()
        )
        if not revision_matches(checked, original["revision"], original.get("events")):
            raise MigrationError("migration_database_target_changed", status=409)
        await verify_account_isolation(target, candidate)
        await candidate.restore(
            source,
            confirmed_tables=[],
            restore_tables=description["tables"],
            source_database=description.get("source_database"),
            source_schemas=description.get("schemas"),
        )
        verified = await candidate.inspect()
        if (
            description.get("revision") is not None
            and verified["revision"] != description["revision"]
        ):
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
        mysql_phases = None
        if target.endpoint.engine == "mysql":
            from .native_restore import prepare_mysql_phases

            mysql_phases = await prepare_mysql_phases(
                candidate, staging, verified["revision"]
            )
        plan = {
            "policy_version": CAPABILITY_POLICY_VERSION,
            "revision_algorithm": description.get(
                "revision_algorithm", "native-content-v1"
            ),
            "source_database": description.get("source_database"),
            "schemas_after": description.get("schemas"),
            "schemas_before": original.get("schemas"),
            "events_after": description.get("events", []),
            "events_before": original.get("events", []),
            "events_candidate": verified.get("events", []),
            "empty_revision": empty["revision"],
            "mysql_phases": mysql_phases,
            "restore_privileges": sorted(restore_privileges)
            if restore_privileges is not None
            else None,
            "actual_databases": identities,
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
    staging,
    source,
    plan,
    *,
    private,
    budget,
    checkpoint,
    live_analysis=False,
    diagnostic=None,
    progress=None,
):
    """Reject changed native data before touching configuration or files."""
    verify_phase_payloads(staging, plan, checkpoint)
    if file_hash(source, checkpoint) != plan["source_sha256"]:
        raise MigrationError("migration_database_payload_changed")
    backup = contained_path(staging, "database-target.backup", regular=True)
    if file_hash(backup, checkpoint) != plan["backup_sha256"]:
        raise MigrationError("migration_database_backup_changed")
    async with clients(
        private,
        staging,
        budget,
        checkpoint,
        diagnostic=diagnostic,
        progress=progress,
        phase="restore_apply",
        capability="restore",
    ) as (target, candidate):
        target.revision_algorithm = plan.get("revision_algorithm", "native-content-v1")
        if plan.get("restore_privileges"):
            target.restore_privileges = set(plan["restore_privileges"])
        candidate.revision_algorithm = target.revision_algorithm
        assert_identity(target, plan["target"])
        assert_identity(candidate, plan["candidate"])
        before = (
            await target.inspect(quiet=False)
            if live_analysis
            else await target.inspect()
        )
        prepared = await candidate.inspect()
        await target.check_restore_privileges(
            tables=set(plan["tables_before"]) | set(plan["tables_after"])
        )
        await candidate.check_restore_privileges(tables=plan["tables_after"])
        await verify_prepared_identity(target, candidate, plan)
        if not revision_matches(
            before, plan["target_revision"], plan.get("events_before")
        ):
            raise MigrationError("migration_database_target_changed", status=409)
        if not revision_matches(
            prepared, plan["candidate_revision"], plan.get("events_candidate")
        ):
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
    diagnostic=None,
    progress=None,
):
    if confirmed_name != plan["database"]["target_name"]:
        raise MigrationError("migration_database_confirmation_required")
    verify_phase_payloads(staging, plan, checkpoint)
    if file_hash(source, checkpoint) != plan["source_sha256"]:
        raise MigrationError("migration_database_payload_changed")
    backup = contained_path(staging, "database-target.backup", regular=True)
    if file_hash(backup, checkpoint) != plan["backup_sha256"]:
        raise MigrationError("migration_database_backup_changed")
    async with clients(
        private,
        staging,
        budget,
        checkpoint,
        diagnostic=diagnostic,
        progress=progress,
        phase="restore_apply",
        capability="restore",
    ) as (target, candidate):
        target.revision_algorithm = plan.get("revision_algorithm", "native-content-v1")
        if plan.get("restore_privileges"):
            target.restore_privileges = set(plan["restore_privileges"])
        candidate.revision_algorithm = target.revision_algorithm
        assert_identity(target, plan["target"])
        assert_identity(candidate, plan["candidate"])
        before, prepared = await target.inspect(), await candidate.inspect()
        await target.check_restore_privileges(
            tables=set(plan["tables_before"]) | set(plan["tables_after"])
        )
        await verify_prepared_identity(target, candidate, plan)
        if not revision_matches(
            before, plan["target_revision"], plan.get("events_before")
        ):
            raise MigrationError("migration_database_target_changed", status=409)
        if not revision_matches(
            prepared, plan["candidate_revision"], plan.get("events_candidate")
        ):
            raise MigrationError("migration_database_candidate_changed", status=409)
        record_native_state(
            journal,
            {
                "state": "applying",
                "target": plan["target"],
                "before_revision": before["revision"],
            },
        )
        await verify_prepared_identity(target, candidate, plan)
        if target.endpoint.engine == "mysql" and plan.get("mysql_phases"):
            from .native_restore import apply_mysql_phases

            await apply_mysql_phases(target, candidate, staging, plan, journal, before)
        else:
            await target.restore(
                source,
                confirmed_tables=before["tables"],
                restore_tables=plan["tables_after"],
                source_database=plan.get("source_database"),
                source_schemas=plan.get("schemas_after"),
            )
        observed = await target.inspect()
        if not revision_matches(
            observed, plan["candidate_revision"], plan.get("events_candidate")
        ):
            raise MigrationError("migration_database_application_mismatch")
        record_native_state(
            journal,
            {
                "state": "applied_unverified",
                "target": plan["target"],
                "after_revision": observed["revision"],
            },
        )
        return {"state": "applied_unverified"}


async def rollback_native(
    staging,
    plan,
    journal,
    *,
    private,
    budget,
    checkpoint=lambda: None,
    diagnostic=None,
    progress=None,
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
    async with native_session(
        endpoint,
        staging,
        budget,
        checkpoint,
        diagnostic=diagnostic,
        progress=progress,
        phase="restore_rollback",
        capability="restore",
        account_role="target",
    ) as target:
        target.revision_algorithm = plan.get("revision_algorithm", "native-content-v1")
        if plan.get("restore_privileges"):
            target.restore_privileges = set(plan["restore_privileges"])
        assert_identity(target, plan["target"])
        if (
            plan.get("actual_databases")
            and await target.actual_identity() != plan["actual_databases"][0]
        ):
            raise MigrationError("migration_database_target_changed", status=409)
        before = await target.inspect()
        await target.check_restore_privileges(
            tables=set(plan["tables_before"]) | set(plan["tables_after"])
        )
        backup = contained_path(staging, "database-target.backup", regular=True)
        if file_hash(backup, checkpoint) != plan["backup_sha256"]:
            raise MigrationError("migration_database_backup_changed")
        if revision_matches(before, plan["target_revision"], plan.get("events_before")):
            # A crash can occur before DDL or after restoring the original data,
            # but before the receipt. Matching the entire original scope is enough.
            record_native_state(
                journal, {"state": "rolled_back", "target": plan["target"]}
            )
            return {"state": "rolled_back"}
        expected = state.get("after_revision", plan["candidate_revision"])
        known_empty = state.get("state") == "applying" and before[
            "revision"
        ] == plan.get("empty_revision")
        phase_revisions = {
            item["revision"]
            for item in plan.get("mysql_phases") or []
            if before.get("events", []) == item.get("events", [])
        }
        known_phase = (
            state.get("state") == "applying"
            and before["revision"] in phase_revisions
            and any(
                entry.get("expected_revision") == before["revision"]
                for entry in state.get("operations", [])
            )
        )
        if not (known_empty or known_phase) and (
            not revision_matches(before, expected, plan.get("events_candidate"))
            or expected != plan["candidate_revision"]
        ):
            # Partially applied DDL or unknown external writes are not owned
            # merely because an intent exists. Keep the instance in maintenance.
            raise MigrationError("migration_database_rollback_conflict")
        record_native_state(
            journal,
            {
                "state": "rolling_back",
                "target": plan["target"],
                "after_revision": before["revision"],
            },
        )
        await target.restore(
            backup,
            confirmed_tables=before["tables"],
            restore_tables=plan["tables_before"],
            source_database=target.endpoint.database,
            source_schemas=plan.get("schemas_before"),
        )
        from .native_restore import set_event_states

        await set_event_states(target, plan.get("events_before", []))
        restored = await target.inspect()
        if not revision_matches(
            restored, plan["target_revision"], plan.get("events_before")
        ):
            raise MigrationError("migration_database_rollback_mismatch")
        record_native_state(journal, {"state": "rolled_back", "target": plan["target"]})
        return {"state": "rolled_back"}
