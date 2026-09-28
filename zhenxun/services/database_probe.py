"""Read-only checks for applied database policy and native migration tools."""

import asyncio
from dataclasses import asdict
import os
from pathlib import Path
import tempfile
import time

from zhenxun.configs.database import (
    applied_database_connection,
    database_error_code,
    inspect_database_tls,
)


async def probe_runtime_database() -> dict:
    """Check the existing ORM pool without opening another runtime connection pool."""
    from tortoise import Tortoise

    started = time.monotonic()
    result = {"status": "error", "checked_at": time.time(), "source": "runtime"}
    try:
        endpoint = applied_database_connection()
        result.update(connection=endpoint.public(), fingerprint=endpoint.fingerprint())
        connection = Tortoise.get_connection("default")

        async def check():
            await connection.execute_query("SELECT 1")
            return await inspect_database_tls(endpoint, connection)

        result.update(
            status="ok",
            code="database_ready",
            observed_tls=await asyncio.wait_for(check(), 2),
        )
    except Exception as error:
        result["code"] = database_error_code(error)
        result["error_type"] = type(error).__name__
    result["latency_ms"] = round((time.monotonic() - started) * 1000)
    return result


async def probe_native_database(
    endpoint, project: Path, *, capability="export"
) -> dict:
    """Probe official tools with a bounded budget, using no data-changing SQL."""
    from zhenxun.migration.access import private_directory
    from zhenxun.migration.native_database import DatabaseEndpoint, native_session
    from zhenxun.migration.tasks import MigrationBudget

    started = time.monotonic()
    result = {
        "status": "error",
        "checked_at": time.time(),
        "connection": endpoint.public(),
        "capability": capability,
    }
    diagnostics = []
    try:
        from zhenxun.migration.errors import MigrationError

        if capability not in {"export", "restore"}:
            raise MigrationError("migration_database_capability_invalid")
        if endpoint.engine == "sqlite":
            if endpoint.database == ":memory:":
                raise MigrationError("migration_volatile_database_unsupported")
            path = Path(endpoint.database)
            if not path.is_relative_to(project.resolve()):
                raise MigrationError("migration_database_root_mapping_required")
            if capability == "restore":
                checked = probe_sqlite_restore_target(
                    project, path.relative_to(project.resolve()).as_posix()
                )
                result.update(checked)
                result.update(
                    status="ok" if checked["ready"] else "error",
                    code=checked.get("code", "migration_database_restore_path_ready"),
                    observed_tls=None,
                    latency_ms=round((time.monotonic() - started) * 1000),
                )
                return result

            def check_sqlite():
                import sqlite3

                connection = sqlite3.connect(
                    path.as_uri() + "?mode=ro", uri=True, timeout=2
                )
                try:
                    connection.execute("PRAGMA schema_version").fetchone()
                finally:
                    connection.close()

            await asyncio.wait_for(asyncio.to_thread(check_sqlite), 3)
            result.update(
                status="ok", code="sqlite_backup_available", observed_tls=None
            )
            result["latency_ms"] = round((time.monotonic() - started) * 1000)
            return result
        native = DatabaseEndpoint.from_connection(endpoint)
        endpoint.certificate_hashes()
        root = private_directory(project / "data/runtime/migration-connection-probes")
        with tempfile.TemporaryDirectory(dir=root) as temporary:
            async with native_session(
                native,
                Path(temporary),
                MigrationBudget.start(15),
                diagnostic=diagnostics.append,
                phase=(
                    "restore_preflight"
                    if capability == "restore"
                    else "export_preflight"
                ),
                capability=capability,
            ) as client:
                facts = await client.probe_connection(capability)
        result.update(status="ok", code="migration_database_ready", **facts)
    except Exception as error:
        code = str(error)
        result["code"] = getattr(error, "code", None) or (
            code
            if code.startswith("database_") and code.replace("_", "").isalnum()
            else "migration_database_connection_failed"
        )
        result["error_type"] = type(error).__name__
    if diagnostics:
        failed = next((item for item in diagnostics if item.get("error_code")), None)
        if failed:
            result["diagnostic"] = failed
    result["latency_ms"] = round((time.monotonic() - started) * 1000)
    return result


async def probe_export_connection(project: Path) -> tuple[dict, dict | None]:
    """Bind a tool check to the applied endpoint and its certificate contents."""
    runtime = await probe_runtime_database()
    result = {"runtime": runtime, "ready": False, "source": "runtime"}
    if runtime["status"] != "ok":
        return result, None
    try:
        endpoint = applied_database_connection()
        fingerprint = endpoint.fingerprint()
        native = await probe_native_database(endpoint, project)
        result.update(native=native, fingerprint=fingerprint)
        if (
            endpoint is not applied_database_connection()
            or fingerprint != endpoint.fingerprint()
            or fingerprint != runtime.get("fingerprint")
        ):
            raise ValueError("database_connection_changed")
        result["ready"] = native["status"] == "ok"
        private = {
            "endpoint": asdict(endpoint),
            "certificates": endpoint.certificate_hashes(),
        }
        return result, private if result["ready"] else None
    except (ValueError, OSError) as error:
        result.update(ready=False, code=database_error_code(error))
        return result, None


async def probe_restore_connections(project: Path, private: dict) -> dict:
    """Probe target and candidate restore accounts before worker handoff."""
    from zhenxun.migration.access import private_directory
    from zhenxun.migration.errors import MigrationError
    from zhenxun.migration.native_replacement import clients, verify_account_isolation
    from zhenxun.migration.tasks import MigrationBudget

    result = {"ready": False, "capability": "restore", "connections": {}}
    database = private.get("database") if isinstance(private, dict) else None
    if not isinstance(database, dict):
        result["code"] = "migration_database_credentials_required"
        return result
    diagnostics = []
    account_role = None
    started = time.monotonic()
    try:
        root = private_directory(project / "data/runtime/migration-connection-probes")
        with tempfile.TemporaryDirectory(dir=root) as temporary:
            async with clients(
                database,
                Path(temporary),
                MigrationBudget.start(30),
                lambda: None,
                diagnostic=diagnostics.append,
                phase="restore_preflight",
                capability="restore",
                root=project,
            ) as (target, candidate):
                for name, client in (("target", target), ("candidate", candidate)):
                    account_role = name
                    client.endpoint.certificate_hashes()
                    checked = await client.probe_connection("restore")
                    result["connections"][name] = {
                        **checked,
                        "status": "ok",
                        "code": "migration_database_ready",
                        "checked_at": time.time(),
                        "connection": client.endpoint.public(),
                    }
                account_role = None
                if (
                    result["connections"]["target"]["server"],
                    target.endpoint.database,
                ) == (
                    result["connections"]["candidate"]["server"],
                    candidate.endpoint.database,
                ):
                    target._record_policy_diagnostic(
                        error_code="migration_database_candidate_isolation_required",
                        isolation_reasons=["same_database"],
                    )
                    raise MigrationError(
                        "migration_database_candidate_isolation_required"
                    )
                await verify_account_isolation(target, candidate)
        result["ready"] = True
    except Exception as error:
        result["code"] = getattr(error, "code", "migration_database_connection_failed")
        failed = next((item for item in diagnostics if item.get("error_code")), None)
        if failed is None:
            failed = {
                "phase": "restore_preflight",
                "operation": "preflight",
                "capability": "restore",
                "error_code": result["code"],
                "stderr": "",
                "return_code": None,
                "recorded_at": time.time(),
            }
            if account_role:
                failed["account_role"] = account_role
            if result["code"] == "migration_database_candidate_isolation_required":
                failed["isolation_reasons"] = ["same_account_or_database"]
        result["diagnostic"] = failed
        role = failed.get("account_role") or account_role
        if role:
            result["connections"][role] = {
                **result["connections"].get(role, {}),
                "status": "error",
                "code": result["code"],
                "diagnostic": failed,
            }
    result["latency_ms"] = round((time.monotonic() - started) * 1000)
    return result


def probe_sqlite_restore_target(project: Path, target_path: str) -> dict:
    """Check SQLite replacement prerequisites without opening a writable DB."""
    from zhenxun.migration.errors import MigrationError
    from zhenxun.migration.replacement_database import _quiet_target

    result = {"ready": False, "capability": "restore", "engine": "sqlite"}
    try:
        target = _quiet_target(project, target_path, online=True)
        if target.exists() and not target.is_file():
            raise MigrationError("migration_database_target_invalid")
        ancestor = target.parent
        while not ancestor.exists() and ancestor != project.absolute():
            ancestor = ancestor.parent
        if not ancestor.is_dir() or not os.access(ancestor, os.W_OK | os.X_OK):
            raise MigrationError("migration_database_restore_target_not_writable")
        if target.exists() and not os.access(target, os.W_OK):
            raise MigrationError("migration_database_restore_target_not_writable")
    except MigrationError as error:
        result["code"] = error.code
    except OSError:
        result["code"] = "migration_database_restore_target_not_writable"
    if result.get("code"):
        result["diagnostic"] = {
            "engine": "sqlite",
            "phase": "restore_preflight",
            "operation": "filesystem",
            "capability": "restore",
            "error_code": result["code"],
            "stderr": "",
            "return_code": None,
            "recorded_at": time.time(),
        }
        return result
    result["ready"] = True
    result["checks"] = {
        "path": True,
        "access": True,
        "replacement": None,
        "writers_stopped": None,
    }
    return result
