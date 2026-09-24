"""Read-only checks for applied database policy and native migration tools."""

import asyncio
from dataclasses import asdict
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


async def probe_native_database(endpoint, project: Path) -> dict:
    """Probe official tools with a bounded budget, using no data-changing SQL."""
    from zhenxun.migration.access import private_directory
    from zhenxun.migration.native_database import DatabaseEndpoint, native_session
    from zhenxun.migration.tasks import MigrationBudget

    started = time.monotonic()
    result = {
        "status": "error",
        "checked_at": time.time(),
        "connection": endpoint.public(),
    }
    diagnostics = []
    try:
        if endpoint.engine == "sqlite":
            from zhenxun.migration.errors import MigrationError

            if endpoint.database == ":memory:":
                raise MigrationError("migration_volatile_database_unsupported")
            path = Path(endpoint.database)
            if not path.is_relative_to(project.resolve()):
                raise MigrationError("migration_database_root_mapping_required")

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
                phase="export_preflight",
            ) as client:
                facts = await client.probe_connection()
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
