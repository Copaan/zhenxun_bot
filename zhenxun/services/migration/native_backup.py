"""数据库原生备份、校验和同库恢复，不改变数据库容器及其授权。"""

from __future__ import annotations

import asyncio
from contextlib import closing
from hashlib import sha256
import os
from pathlib import Path
import shutil
import sqlite3

from zhenxun.configs.database import DatabaseConnection

from . import data_guard as guard


def tools_for(endpoint: DatabaseConnection) -> dict[str, str]:
    """查找备份和恢复工具，缺少任一个时拒绝开始覆盖。"""
    if endpoint.engine == "sqlite":
        return {}
    names = (
        ("pg_dump", "pg_restore")
        if endpoint.engine == "postgres"
        else ("mysqldump", "mysql")
    )
    directory = os.environ.get(
        "ZX_MIGRATION_POSTGRES_BIN"
        if endpoint.engine == "postgres"
        else "ZX_MIGRATION_MYSQL_BIN",
        "",
    )
    result = {}
    for name in names:
        path = shutil.which(name, path=directory or None)
        if not path:
            raise RuntimeError("migration_native_tool_required:" + name)
        result[name] = str(Path(path).resolve())
    return result


def _connection(endpoint: DatabaseConnection) -> tuple[list[str], dict[str, str]]:
    environment = dict(os.environ)
    if endpoint.engine == "sqlite":
        return [], environment
    if endpoint.engine == "postgres":
        for key in list(environment):
            if key.startswith("PG"):
                environment.pop(key)
        environment.update(
            PGHOST=endpoint.host,
            PGPORT=str(endpoint.port),
            PGUSER=endpoint.username,
            PGPASSWORD=endpoint.password,
            PGDATABASE=endpoint.database,
            PGSSLMODE=endpoint.tls,
            PGCONNECT_TIMEOUT="10",
        )
        for name in (
            "sslrootcert",
            "sslcert",
            "sslkey",
            "sslcrl",
            "sslpassword",
            "ssl_min_protocol_version",
            "ssl_max_protocol_version",
        ):
            if name in endpoint.options:
                environment["PG" + name.replace("_", "").upper()] = endpoint.options[
                    name
                ]
        return [], environment
    environment["MYSQL_PWD"] = endpoint.password
    arguments = [
        "--no-defaults",
        "--protocol=TCP",
        "--host=" + endpoint.host,
        "--port=" + str(endpoint.port),
        "--user=" + endpoint.username,
        "--ssl-mode=" + endpoint.tls,
    ]
    for name, option in (
        ("sslrootcert", "ssl-ca"),
        ("sslcert", "ssl-cert"),
        ("sslkey", "ssl-key"),
        ("charset", "default-character-set"),
    ):
        if name in endpoint.options:
            arguments.append("--" + option + "=" + endpoint.options[name])
    return arguments, environment


async def _run(arguments, environment, *, source: Path | None = None):
    stream = source.open("rb") if source else None
    process = None
    try:
        process = await asyncio.create_subprocess_exec(
            *arguments,
            env=environment,
            stdin=stream,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            _, errors = await asyncio.wait_for(process.communicate(), 600)
            code = process.returncode
        except BaseException:
            if process.returncode is None:
                process.kill()
            await process.wait()
            raise
        if code:
            from zhenxun.services.nonebot_store.dependencies import safe_process_error

            detail = errors.decode("utf-8", errors="replace")
            for key in ("PGPASSWORD", "MYSQL_PWD", "PGSSLPASSWORD"):
                if environment.get(key):
                    detail = detail.replace(environment[key], "***")
            detail = safe_process_error(detail).strip()[-1000:]
            raise RuntimeError(
                "migration_native_command_failed:" + str(code) + ":" + detail
            )
    except OSError as error:
        raise RuntimeError("migration_native_command_unavailable") from error
    finally:
        if stream:
            stream.close()


def file_digest(path: Path) -> str:
    value = sha256()
    with path.open("rb") as stream:
        while block := stream.read(1024 * 1024):
            value.update(block)
    return value.hexdigest()


def _sqlite_backup(endpoint, destination):
    if endpoint.database == ":memory:":
        raise RuntimeError("migration_native_memory_backup_unsupported")
    with (
        closing(
            sqlite3.connect(Path(endpoint.database).as_uri() + "?mode=ro", uri=True)
        ) as source,
        closing(sqlite3.connect(destination)) as target,
    ):
        source.backup(target)
        if target.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
            raise RuntimeError("migration_native_backup_invalid")


async def create(
    db_url: str,
    directory: Path,
    *,
    root: Path,
    identity: dict,
    objects: dict | None = None,
) -> dict:
    """生成经校验的完整原生备份，完成前不发布备份收据。"""
    endpoint = DatabaseConnection.parse(db_url, root=root)
    programs = tools_for(endpoint)
    public_schema = False
    if endpoint.engine == "postgres":
        if objects is None:
            from . import data, target

            client = await data.open_client(db_url, "zx_native_preview", read_only=True)
            try:
                objects = await target.database_objects(client)
            finally:
                await client.close()
        public_schema = any(item["name"] == "public" for item in objects["schemas"])
    directory.mkdir(parents=True, exist_ok=True)
    destination = directory / (
        "target.sqlite3" if endpoint.engine == "sqlite" else "target.dump"
    )
    partial = destination.with_suffix(".part")
    arguments, environment = _connection(endpoint)
    try:
        if endpoint.engine == "sqlite":
            task = asyncio.create_task(
                asyncio.to_thread(_sqlite_backup, endpoint, partial)
            )
            try:
                await asyncio.shield(task)
            except asyncio.CancelledError:
                await task
                raise
        elif endpoint.engine == "postgres":
            await _run(
                [programs["pg_dump"], "--format=custom", "--file=" + str(partial)],
                environment,
            )
            await _run([programs["pg_restore"], "--list", str(partial)], environment)
        else:
            await _run(
                [
                    programs["mysqldump"],
                    *arguments,
                    "--single-transaction",
                    "--routines",
                    "--events",
                    "--triggers",
                    "--hex-blob",
                    "--set-gtid-purged=OFF",
                    "--no-tablespaces",
                    "--result-file=" + str(partial),
                    "--",
                    endpoint.database,
                ],
                environment,
            )
        if not partial.is_file() or partial.stat().st_size == 0:
            raise RuntimeError("migration_native_backup_invalid")
        partial.replace(destination)
        return {
            "version": 1,
            "engine": endpoint.engine,
            "identity": identity,
            "path": str(destination),
            "sha256": file_digest(destination),
            "size": destination.stat().st_size,
            "state": "ready",
            "public_schema": public_schema,
        }
    finally:
        partial.unlink(missing_ok=True)


async def restore(db_url: str, receipt: dict, *, root: Path, client) -> None:
    """核对原生备份和目标身份后恢复，失败不宣称原数据已恢复。"""
    from . import target

    actual = await guard.identity(client)
    if {k: v for k, v in actual.items() if k != "schema_id"} != {
        k: v for k, v in receipt["identity"].items() if k != "schema_id"
    }:
        raise RuntimeError("migration_database_identity_changed")
    path = Path(receipt["path"])
    if not path.is_file() or file_digest(path) != receipt["sha256"]:
        raise RuntimeError("migration_native_backup_changed")
    endpoint = DatabaseConnection.parse(db_url, root=root)
    programs = tools_for(endpoint)
    arguments, environment = _connection(endpoint)
    if endpoint.engine == "sqlite":
        raise RuntimeError("migration_sqlite_restore_requires_closed_connections")
    await target.clear_database(client, recreate=False)
    if endpoint.engine == "postgres":
        if receipt.get("public_schema"):
            await client.execute_query("CREATE SCHEMA public")
        await _run(
            [
                programs["pg_restore"],
                "--exit-on-error",
                "--dbname=" + endpoint.database,
                str(path),
            ],
            environment,
        )
    else:
        await _run(
            [programs["mysql"], *arguments, "--", endpoint.database],
            environment,
            source=path,
        )
