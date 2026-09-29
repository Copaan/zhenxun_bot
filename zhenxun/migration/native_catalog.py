"""Native backup object coverage and complete database revision evidence."""

from __future__ import annotations

import hashlib
import json
import re

from .archive import Limits
from .errors import MigrationError
from .sql_tokens import mysql_sql, tokens

REVISION_ALGORITHM = "native-content-v3"
PG_USER_SCHEMA = (
    "n.nspname NOT LIKE 'pg\\_%' ESCAPE '\\' AND n.nspname<>'information_schema'"
)


def schema_digest(path, engine, database, checkpoint):
    digest = hashlib.sha256()
    if engine == "mysql":
        sql = mysql_sql(
            path.read_text("utf-8"),
            source_database=database,
            target_database="__migration_database__",
            disable_events=True,
        )
        for line in sql.splitlines():
            checkpoint()
            if line.strip() and not line.lstrip().startswith("--"):
                digest.update(line.rstrip().encode() + b"\n")
        return digest.hexdigest()
    sql = path.read_text("utf-8")
    # psql's randomized restrict keys are command guards, not database objects.
    parsed = list(tokens(sql, dialect="postgres"))
    guards = [
        (match.start(), match.end())
        for match in re.finditer(r"(?m)^\\(?:un)?restrict [^\r\n]+", sql)
        if not any(
            t.kind == "string" and t.start <= match.start() < t.end for t in parsed
        )
    ]
    statement = []
    for token in parsed:
        if any(start <= token.start < end for start, end in guards):
            continue
        checkpoint()
        statement.append(token)
        if token.text != ";" or token.kind == "string":
            continue
        text = " ".join(t.text for t in statement)
        if text not in {
            "CREATE SCHEMA public ;",
            "COMMENT ON SCHEMA public IS 'standard public schema' ;",
        }:
            for item in statement:
                raw = item.text.encode()
                digest.update(len(raw).to_bytes(8, "big") + raw)
        statement.clear()
    for item in statement:
        digest.update(item.text.encode())
    return digest.hexdigest()


async def inspect_native(client, *, quiet=True):
    engine = client.endpoint.engine
    for tool in client.tools:
        if tool not in client.tool_versions:
            await client.run(tool, ["--version"])
    version = await client.query(
        "SELECT VERSION();" if engine == "mysql" else "SHOW server_version;"
    )
    await client._check_migration_privileges()
    if quiet and client.capability == "restore":
        if engine == "mysql":
            grants = await client._mysql_capabilities()
            if "PROCESS" not in grants.global_privileges:
                client._record_policy_diagnostic(
                    reasons=["process_privilege_missing"],
                    capability_checks={"connection_inventory": 0},
                )
                raise MigrationError("migration_database_privileges_unsupported")
            sql = (
                "SELECT COUNT(*) FROM information_schema.PROCESSLIST "
                "WHERE DB=DATABASE() AND ID<>CONNECTION_ID();"
            )
        else:
            sql = (
                "SELECT COUNT(*) FROM pg_stat_activity "
                "WHERE datname=current_database() "
                "AND pid<>pg_backend_pid();"
            )
        if await client.query(sql) != "0":
            raise MigrationError("migration_database_other_connections")
    if engine == "postgres":
        tables = await client.rows(
            "SELECT n.nspname AS schema,c.relname AS name,c.relkind AS kind, "
            "c.relispopulated AS populated "
            "FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace "
            f"WHERE {PG_USER_SCHEMA} AND c.relkind IN ('r','p','m','f') "
            f"ORDER BY n.nspname,c.relname"
        )
        schemas = await client.rows(
            f"SELECT n.nspname AS name FROM pg_namespace n "
            f"WHERE {PG_USER_SCHEMA} ORDER BY n.nspname"
        )
        objects = await client.rows(
            "SELECT n.nspname AS schema,c.relname AS name,c.relkind AS kind "
            "FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace "
            f"WHERE {PG_USER_SCHEMA} ORDER BY n.nspname,c.relname"
        )
        extensions = await client.rows(
            "SELECT extname AS name,extversion AS version "
            "FROM pg_extension WHERE extname<>'plpgsql' ORDER "
            "BY extname"
        )
        objects.extend(
            await client.rows(
                "SELECT n.nspname AS schema,p.proname || '(' || "
                "pg_get_function_identity_arguments(p.oid) || ')' AS name, "
                "'routine' AS kind FROM pg_proc p JOIN pg_namespace n "
                "ON n.oid=p.pronamespace "
                f"WHERE {PG_USER_SCHEMA} ORDER BY n.nspname,name"
            )
        )
        objects.extend(
            await client.rows(
                "SELECT n.nspname AS schema,t.typname AS name,'type' AS kind "
                "FROM pg_type t JOIN pg_namespace n ON n.oid=t.typnamespace "
                f"WHERE {PG_USER_SCHEMA} AND t.typrelid=0 AND t.typelem=0 "
                "ORDER BY n.nspname,t.typname"
            )
        )
        for relation, field, kind in (
            ("pg_event_trigger", "evtname", "event_trigger"),
            ("pg_foreign_server", "srvname", "foreign_server"),
            ("pg_publication", "pubname", "publication"),
            ("pg_subscription", "subname", "subscription"),
        ):
            objects.extend(
                await client.rows(
                    f"SELECT {field} AS name,'{kind}' AS kind FROM {relation} "
                    + (
                        "WHERE subdbid=(SELECT oid FROM pg_database WHERE "
                        "datname=current_database()) "
                        if kind == "subscription"
                        else ""
                    )
                    + f"ORDER BY {field}"
                )
            )
        sequences = []
        for row in await client.rows(
            "SELECT schemaname AS schema,sequencename AS name,start_value,"
            "min_value,max_value,increment_by,cycle,cache_size "
            "FROM pg_sequences WHERE schemaname NOT LIKE 'pg\\_%' "
            "ESCAPE '\\' ORDER BY schemaname,sequencename"
        ):
            state = await client.rows(
                "SELECT last_value,is_called FROM "
                + client.identifier(row["schema"])
                + "."
                + client.identifier(row["name"])
            )
            sequences.append({**row, **state[0]})
        schema_path = await client.run(
            "pg_dump",
            [
                *client.connection,
                "--schema-only",
                "--no-owner",
                "--no-acl",
                "--no-tablespaces",
            ],
            maximum=Limits().expanded,
        )
        events = []
    else:
        tables = await client.rows(
            "SELECT JSON_OBJECT('name',TABLE_NAME,'engine',ENGINE,'kind',TABLE_TYPE) "
            "FROM information_schema.TABLES WHERE TABLE_SCHEMA=DATABASE() "
            "ORDER BY TABLE_NAME"
        )
        schemas, extensions = [], []
        objects = list(tables)
        for kind, source, scope, name in (
            ("trigger", "TRIGGERS", "TRIGGER_SCHEMA", "TRIGGER_NAME"),
            ("routine", "ROUTINES", "ROUTINE_SCHEMA", "ROUTINE_NAME"),
            ("event", "EVENTS", "EVENT_SCHEMA", "EVENT_NAME"),
        ):
            objects.extend(
                await client.rows(
                    f"SELECT JSON_OBJECT('kind','{kind}','name',{name}) "
                    f"FROM information_schema.{source} WHERE {scope}=DATABASE() "
                    f"ORDER BY {name}"
                )
            )
        events = await client.rows(
            "SELECT JSON_OBJECT('name',EVENT_NAME,'status',STATUS) "
            "FROM information_schema.EVENTS WHERE EVENT_SCHEMA=DATABASE() "
            "ORDER BY EVENT_NAME"
        )
        sequences = await client.rows(
            "SELECT JSON_OBJECT('table',TABLE_NAME,'next',AUTO_INCREMENT) "
            "FROM information_schema.TABLES WHERE TABLE_SCHEMA=DATABASE() "
            "ORDER BY TABLE_NAME"
        )
        schema_path = await client.run(
            "mysqldump",
            [
                *client.connection,
                "--no-data",
                "--no-tablespaces",
                "--set-gtid-purged=OFF",
                "--skip-lock-tables",
                "--triggers",
                "--routines",
                "--events",
                client.endpoint.database,
            ],
            maximum=Limits().expanded,
        )
    schema_hash = schema_digest(
        schema_path, engine, client.endpoint.database, client.check
    )
    schema_path.unlink()
    data = []
    for table in tables:
        client.check()
        if engine == "mysql" and table["kind"] != "BASE TABLE":
            continue
        if engine == "postgres" and table["kind"] not in {"r", "m"}:
            continue
        if engine == "postgres" and table["kind"] == "m" and not table["populated"]:
            data.append(
                {"table": table["name"], "schema": table["schema"], "populated": False}
            )
            continue
        name = client.identifier(table["name"])
        if engine == "postgres":
            name = client.identifier(table["schema"]) + "." + name
            sql = (
                f"COPY (SELECT encoded FROM (SELECT row_to_json(t)::text AS encoded "
                f'FROM {name} t) zx ORDER BY encoded COLLATE "C") TO STDOUT;'
            )
            tool, arguments = (
                "psql",
                [*client.connection, "-X", "-A", "-t", "-v", "ON_ERROR_STOP=1"],
            )
        else:
            columns = await client.rows(
                "SELECT JSON_OBJECT('name',COLUMN_NAME) "
                "FROM information_schema.COLUMNS "
                "WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME="
                + "'"
                + table["name"].replace("'", "''")
                + "' ORDER BY ORDINAL_POSITION"
            )
            cells = [
                f"IF({client.identifier(c['name'])} IS NULL,NULL,"
                f"HEX(CAST({client.identifier(c['name'])} AS BINARY)))"
                for c in columns
            ]
            sql = (
                f"SELECT JSON_ARRAY({','.join(cells)}) AS encoded "
                f"FROM {name} ORDER BY BINARY encoded;"
            )
            tool, arguments = (
                "mysql",
                [
                    *client.connection,
                    "--init-command=SET time_zone='+00:00'",
                    "--batch",
                    "--raw",
                    "--skip-column-names",
                    "--binary-mode",
                    f"--database={client.endpoint.database}",
                ],
            )
        output = await client.run(tool, arguments, sql=sql, maximum=Limits().expanded)
        digest, count, pending_cr = hashlib.sha256(), 0, False
        with output.open("rb") as stream:
            while chunk := stream.read(1024 * 1024):
                client.check()
                if pending_cr:
                    chunk = b"\r" + chunk
                pending_cr = chunk.endswith(b"\r")
                if pending_cr:
                    chunk = chunk[:-1]
                chunk = chunk.replace(b"\r\n", b"\n")
                digest.update(chunk)
                count += chunk.count(b"\n")
            if pending_cr:
                digest.update(b"\r")
        output.unlink()
        data.append(
            {
                "table": table["name"],
                "schema": table.get("schema"),
                "rows": count,
                "sha256": digest.hexdigest(),
            }
        )
    if engine == "mysql":
        sequences = await client.rows(
            "SELECT JSON_OBJECT('table',TABLE_NAME,'next',AUTO_INCREMENT) "
            "FROM information_schema.TABLES WHERE TABLE_SCHEMA=DATABASE() "
            "ORDER BY TABLE_NAME"
        )
    large_objects = []
    if engine == "postgres":
        for row in await client.rows(
            "SELECT oid FROM pg_largeobject_metadata ORDER BY oid"
        ):
            oid = int(row["oid"])
            offset = 0
            digest = hashlib.sha256()
            while True:
                client.check()
                value = await client.query(
                    f"SELECT encode(lo_get({oid},{offset},65536),'hex');"
                )
                chunk = bytes.fromhex(value)
                digest.update(chunk)
                offset += len(chunk)
                if len(chunk) < 65536:
                    break
            large_objects.append(
                {"oid": oid, "bytes": offset, "sha256": digest.hexdigest()}
            )
    evidence = {
        "schema": schema_hash,
        "schemas": schemas,
        "large_objects": large_objects,
        "objects": objects,
        "extensions": extensions,
        "sequences": sequences,
        "data": data,
    }
    return {
        "engine": engine,
        "engine_version": version,
        "server": await client.server_identity(),
        "database": client.endpoint.database,
        "tables": [t["name"] for t in tables],
        "schemas": schemas,
        "objects": objects,
        "events": events,
        "evidence": evidence,
        "revision_algorithm": REVISION_ALGORITHM,
        "revision": hashlib.sha256(
            json.dumps(evidence, sort_keys=True).encode()
        ).hexdigest(),
    }
