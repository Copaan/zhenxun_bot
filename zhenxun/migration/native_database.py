"""Bounded official-tool operations for same-engine database migration."""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from functools import partial
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
from urllib.parse import parse_qs, unquote, urlsplit
import uuid

from zhenxun.services.lifecycle.deadline import ShutdownBudget, current_budget
from zhenxun.services.lifecycle.kernel import LifecycleKernel
from zhenxun.services.lifecycle.launcher import LauncherSupervisor

from .access import private_directory
from .archive import Limits, file_hash, require_space
from .errors import MigrationError
from .paths import contained_path

_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_-]{0,62}$")
_TOOLS = {
    "mysql": ("mysql", "mysqldump"),
    "postgres": ("psql", "pg_dump", "pg_restore"),
}


@dataclass(frozen=True)
class DatabaseEndpoint:
    engine: str
    host: str
    port: int
    database: str
    username: str = field(repr=False)
    password: str = field(repr=False)
    tls: str = ""

    @classmethod
    def parse(cls, value: str) -> DatabaseEndpoint:
        try:
            parsed = urlsplit(value)
            engine = "postgres" if parsed.scheme == "postgresql" else parsed.scheme
            query = parse_qs(parsed.query, strict_parsing=True) if parsed.query else {}
            if (
                engine not in _TOOLS
                or not parsed.hostname
                or parsed.fragment
                or set(query) - {"sslmode", "ssl_mode"}
                or any(len(v) != 1 for v in query.values())
                or len(query) > 1
            ):
                raise ValueError
            database = unquote(parsed.path.removeprefix("/"))
            username = unquote(parsed.username or "")
            password = unquote(parsed.password or "")
            if not _NAME.fullmatch(database) or not username:
                raise ValueError
            if any(c in username + password for c in "\r\n\x00"):
                raise ValueError
            if len(username) > 128 or len(password) > 4096:
                raise ValueError
            local = parsed.hostname in {"localhost", "127.0.0.1", "::1"}
            tls = next(iter(query.values()), ["disable" if local else "verify-full"])[0]
            allowed = (
                {"disable", "verify-full"}
                if engine == "postgres"
                else {
                    "DISABLED",
                    "VERIFY_IDENTITY",
                }
            )
            if engine == "mysql":
                tls = {"disable": "DISABLED", "verify-full": "VERIFY_IDENTITY"}.get(
                    tls, tls
                )
            if tls not in allowed or (not local and tls in {"disable", "DISABLED"}):
                raise MigrationError("migration_database_tls_configuration_required")
            port = parsed.port or (3306 if engine == "mysql" else 5432)
            return cls(engine, parsed.hostname, port, database, username, password, tls)
        except (ValueError, TypeError, AttributeError):
            raise MigrationError("migration_database_connection_invalid") from None

    def identity(self) -> dict:
        return {
            "engine": self.engine,
            "host": self.host.lower(),
            "port": self.port,
            "database": self.database,
        }


def require_version(engine: str, source: str, target: str) -> None:
    def family(value):
        if not isinstance(value, str) or "mariadb" in value.lower():
            raise MigrationError("migration_database_version_unsupported")
        match = re.match(r"^(\d+)\.(\d+)(?:\D|$)", value)
        if not match:
            raise MigrationError("migration_database_version_invalid")
        return match.groups()[: 2 if engine == "mysql" else 1]

    if engine not in _TOOLS or family(source) != family(target):
        raise MigrationError("migration_database_version_unsupported")


def tool_version(engine: str, text: str) -> str:
    if "mariadb" in text.lower():
        raise MigrationError("migration_database_version_unsupported")
    pattern = (
        r"\bDistrib\s+(\d+\.\d+(?:\.\d+)?)"
        if engine == "mysql" and "Distrib" in text
        else r"\bVer\s+(\d+\.\d+(?:\.\d+)?)"
        if engine == "mysql"
        else r"\(PostgreSQL\)\s+(\d+\.\d+(?:\.\d+)?)"
    )
    match = re.search(pattern, text)
    if match is None:
        raise MigrationError("migration_database_tool_version_invalid")
    return match[1]


def mysql_grant_scope(value: str, *, literal: bool) -> str:
    if literal:
        return value
    result = []
    escaped = False
    for character in value:
        if escaped:
            result.append(character)
            escaped = False
        elif character == "\\":
            escaped = True
        elif character in "_%":
            raise MigrationError("migration_database_privilege_scope_wildcard")
        else:
            result.append(character)
    if escaped:
        raise MigrationError("migration_database_privilege_scope_wildcard")
    return "".join(result)


class NativeDatabase:
    def __init__(
        self, endpoint, directory, supervisor, budget, checkpoint=lambda: None
    ):
        self.endpoint = endpoint
        self.directory = private_directory(directory / uuid.uuid4().hex)
        self.supervisor, self.budget, self.checkpoint = supervisor, budget, checkpoint
        self.tools = {}
        self.tool_versions = {}
        for name in _TOOLS[endpoint.engine]:
            executable = shutil.which(name)
            if executable is None:
                raise MigrationError("migration_database_tool_missing")
            self.tools[name] = executable
        self.environment = {
            key: value
            for key, value in os.environ.items()
            if not key.upper().startswith(("PG", "MYSQL"))
        }
        self.environment.update(PGCLIENTENCODING="UTF8", PSQLRC=os.devnull)
        if endpoint.engine == "mysql":
            # MySQL 8.0 reads .mylogin.cnf even with --defaults-file. Redirect
            # that lookup to an absent private path, never the user's login file.
            self.environment["MYSQL_TEST_LOGIN_FILE"] = str(
                self.directory / "login.cnf"
            )
            credential = self.directory / "client.cnf"

            def escape(value):
                return '"' + value.replace("\\", "\\\\").replace('"', '\\"') + '"'

            credential.write_text(
                "[client]\n"
                + "\n".join(
                    f"{key}={escape(str(value))}"
                    for key, value in {
                        "host": endpoint.host,
                        "port": endpoint.port,
                        "user": endpoint.username,
                        "password": endpoint.password,
                        "ssl-mode": endpoint.tls,
                        "default-character-set": "utf8mb4",
                    }.items()
                )
                + "\n",
                encoding="utf-8",
            )
            self.connection = [f"--defaults-file={credential}"]
            if endpoint.tls == "DISABLED":
                # Only loopback endpoints may use plaintext. SHA2 authentication
                # still needs the server's RSA public key for password exchange.
                self.connection.append("--get-server-public-key")
        else:
            credential = self.directory / "pgpass"

            def escape(value):
                return str(value).replace("\\", "\\\\").replace(":", "\\:")

            credential.write_text(
                ":".join(
                    map(
                        escape,
                        (
                            endpoint.host,
                            endpoint.port,
                            endpoint.database,
                            endpoint.username,
                            endpoint.password,
                        ),
                    )
                )
                + "\n",
                encoding="utf-8",
            )
            credential.chmod(0o600)
            self.environment.update(
                PGPASSFILE=str(credential),
                PGSSLMODE=endpoint.tls,
                PGCONNECT_TIMEOUT="10",
                PGOPTIONS=(
                    "-c timezone=UTC -c datestyle=ISO,YMD -c extra_float_digits=3 "
                    "-c standard_conforming_strings=on"
                ),
            )
            self.connection = [
                "-h",
                endpoint.host,
                "-p",
                str(endpoint.port),
                "-U",
                endpoint.username,
                "-d",
                endpoint.database,
                "-w",
            ]

    def check(self):
        self.budget.checkpoint()
        self.checkpoint()

    async def run(
        self,
        tool,
        args,
        *,
        sql=None,
        input_path=None,
        output_path=None,
        maximum=16 * 1024 * 1024,
    ):
        self.check()
        identity = uuid.uuid4().hex
        output = output_path or self.directory / (identity + ".out")
        error = self.directory / (identity + ".err")
        if output.exists():
            raise MigrationError("migration_database_output_exists")
        sql_path = self.directory / (identity + ".sql")
        if sql is not None:
            sql_path.write_text(sql, encoding="utf-8")
            input_path = sql_path
        process = None
        try:
            with (
                output.open("xb") as stdout,
                error.open("xb") as stderr,
                # This adapter runs in a dedicated maintenance process.
                Path(input_path or os.devnull).open("rb") as stdin,  # noqa: ASYNC230
            ):
                process = await self.supervisor.start_process(
                    "migration_database",
                    partial(
                        subprocess.Popen,
                        [self.tools[tool], *map(str, args)],
                        cwd=self.directory,
                        env=self.environment,
                        stdin=stdin,
                        stdout=stdout,
                        stderr=stderr,
                        creationflags=subprocess.CREATE_NO_WINDOW
                        if os.name == "nt"
                        else 0,
                    ),
                    completion_expected=True,
                )
                while process.poll() is None:
                    self.check()
                    self.supervisor._publish_processes()
                    if (
                        output.stat().st_size > maximum
                        or error.stat().st_size > 1024**2
                    ):
                        raise MigrationError("migration_database_output_limit")
                    require_space(self.directory, 16 * 1024**2)
                    await asyncio.sleep(0.05)
            self.check()
            if output.stat().st_size > maximum:
                raise MigrationError("migration_database_output_limit")
            if process.returncode:
                raise MigrationError("migration_database_tool_failed")
            return output
        finally:
            if process is not None:
                await self.supervisor.stop_process(process)

    async def query(self, sql):
        if self.endpoint.engine == "mysql":
            args = [
                *self.connection,
                "--init-command=SET time_zone='+00:00',"
                "information_schema_stats_expiry=0",
                "--batch",
                "--raw",
                "--skip-column-names",
                "--binary-mode",
                f"--database={self.endpoint.database}",
            ]
            output = await self.run("mysql", args, sql=sql)
        else:
            output = await self.run(
                "psql",
                [*self.connection, "-X", "-A", "-t", "-v", "ON_ERROR_STOP=1"],
                sql=sql,
            )
        return output.read_text("utf-8").strip()

    async def rows(self, select):
        if self.endpoint.engine == "postgres":
            text = await self.query(f"SELECT row_to_json(zx) FROM ({select}) AS zx;")
        else:
            text = await self.query(select)
        try:
            result = [json.loads(line) for line in text.splitlines() if line]
            if len(result) > 10_000:
                raise ValueError
            return result
        except (ValueError, TypeError):
            raise MigrationError("migration_database_analysis_limit") from None

    def identifier(self, name):
        quote = "`" if self.endpoint.engine == "mysql" else '"'
        return quote + name.replace(quote, quote + quote) + quote

    async def verify_relations(self, keys):
        for key in keys:
            child, parent = map(self.identifier, (key["table"], key["parent"]))
            present = " AND ".join(
                f"c.{self.identifier(column)} IS NOT NULL" for column in key["columns"]
            )
            matches = " AND ".join(
                f"c.{self.identifier(column)}=p.{self.identifier(reference)}"
                for column, reference in zip(key["columns"], key["references"])
            )
            if (
                await self.query(
                    f"SELECT COUNT(*) FROM {child} c WHERE {present} AND NOT EXISTS "
                    f"(SELECT 1 FROM {parent} p WHERE {matches});"
                )
                != "0"
            ):
                raise MigrationError("migration_database_relations_invalid")

    async def inspect(self, *, quiet=True):
        engine = self.endpoint.engine
        if engine == "mysql":
            version = await self.query("SELECT VERSION();")
            require_version(engine, version, version)
            grantee = (
                "CONCAT(QUOTE(SUBSTRING_INDEX(CURRENT_USER(),'@',1)), '@', "
                "QUOTE(SUBSTRING_INDEX(CURRENT_USER(),'@',-1)))"
            )
            global_grants = await self.rows(
                "SELECT JSON_OBJECT('privilege',PRIVILEGE_TYPE) FROM "
                f"information_schema.USER_PRIVILEGES WHERE GRANTEE={grantee}"
            )
            privileges = {r["privilege"] for r in global_grants}
            if privileges - {"USAGE", "PROCESS"} or "PROCESS" not in privileges:
                raise MigrationError("migration_database_privileges_unsupported")
            if (
                await self.query(
                    "SELECT COUNT(*) FROM information_schema.APPLICABLE_ROLES;"
                )
                != "0"
            ):
                raise MigrationError("migration_database_privileges_unsupported")
            scopes = await self.rows(
                "SELECT JSON_OBJECT('scope',TABLE_SCHEMA,'grantable',IS_GRANTABLE) "
                f"FROM information_schema.SCHEMA_PRIVILEGES WHERE GRANTEE={grantee}"
            )
            literal_grants = await self.query("SELECT @@partial_revokes;") == "1"
            if not scopes or any(
                mysql_grant_scope(r["scope"], literal=literal_grants)
                != self.endpoint.database
                or r["grantable"] != "NO"
                for r in scopes
            ):
                raise MigrationError("migration_database_privileges_unsupported")
            for table in ("TABLE_PRIVILEGES", "COLUMN_PRIVILEGES"):
                if (
                    await self.query(
                        f"SELECT COUNT(*) FROM information_schema.{table} "
                        f"WHERE GRANTEE={grantee};"
                    )
                    != "0"
                ):
                    raise MigrationError("migration_database_privileges_unsupported")
            if (
                quiet
                and await self.query(
                    "SELECT COUNT(*) FROM information_schema.PROCESSLIST "
                    "WHERE DB=DATABASE() AND ID<>CONNECTION_ID();"
                )
                != "0"
            ):
                raise MigrationError("migration_database_other_connections")
            for table, column in (
                ("TRIGGERS", "TRIGGER_SCHEMA"),
                ("ROUTINES", "ROUTINE_SCHEMA"),
                ("EVENTS", "EVENT_SCHEMA"),
            ):
                if (
                    await self.query(
                        f"SELECT COUNT(*) FROM information_schema.{table} "
                        f"WHERE {column}=DATABASE();"
                    )
                    != "0"
                ):
                    raise MigrationError("migration_database_objects_unsupported")
            tables = await self.rows(
                "SELECT JSON_OBJECT('name',TABLE_NAME,'engine',ENGINE,"
                "'kind',TABLE_TYPE) FROM information_schema.TABLES "
                "WHERE TABLE_SCHEMA=DATABASE() ORDER BY TABLE_NAME"
            )
            if any(
                t["engine"] != "InnoDB" or t["kind"] != "BASE TABLE" for t in tables
            ):
                raise MigrationError("migration_database_objects_unsupported")
            columns = await self.rows(
                "SELECT JSON_OBJECT('table',TABLE_NAME,'name',COLUMN_NAME,"
                "'type',COLUMN_TYPE,"
                "'nullable',IS_NULLABLE,'default',COLUMN_DEFAULT,'extra',EXTRA,"
                "'generated',GENERATION_EXPRESSION,"
                "'charset',CHARACTER_SET_NAME,'collation',COLLATION_NAME) "
                "FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE() "
                "ORDER BY TABLE_NAME,ORDINAL_POSITION"
            )
            constraints = await self.rows(
                "SELECT JSON_OBJECT('table',TABLE_NAME,'constraint',CONSTRAINT_NAME,"
                "'column',COLUMN_NAME,'position',ORDINAL_POSITION,"
                "'reference_table',REFERENCED_TABLE_NAME,"
                "'reference_column',REFERENCED_COLUMN_NAME,"
                "'external',IF(REFERENCED_TABLE_SCHEMA IS NULL OR "
                "REFERENCED_TABLE_SCHEMA=DATABASE(),false,true)) "
                "FROM information_schema.KEY_COLUMN_USAGE "
                "WHERE TABLE_SCHEMA=DATABASE() "
                "ORDER BY TABLE_NAME,CONSTRAINT_NAME,ORDINAL_POSITION"
            )
            if any(c["external"] for c in constraints):
                raise MigrationError("migration_database_objects_unsupported")
            foreign = {}
            for key in constraints:
                if key["reference_table"]:
                    group = foreign.setdefault(
                        (key["table"], key["constraint"]),
                        {
                            "table": key["table"],
                            "parent": key["reference_table"],
                            "columns": [],
                            "references": [],
                        },
                    )
                    group["columns"].append(key["column"])
                    group["references"].append(key["reference_column"])
            foreign_keys = list(foreign.values())
            constraints += await self.rows(
                "SELECT JSON_OBJECT('table',TABLE_NAME,'name',CONSTRAINT_NAME,"
                "'update',UPDATE_RULE,'delete',DELETE_RULE,'match',MATCH_OPTION) "
                "FROM information_schema.REFERENTIAL_CONSTRAINTS "
                "WHERE CONSTRAINT_SCHEMA=DATABASE() ORDER BY TABLE_NAME,CONSTRAINT_NAME"
            )
            constraints += await self.rows(
                "SELECT JSON_OBJECT('name',CONSTRAINT_NAME,'check',CHECK_CLAUSE) "
                "FROM information_schema.CHECK_CONSTRAINTS "
                "WHERE CONSTRAINT_SCHEMA=DATABASE() ORDER BY CONSTRAINT_NAME"
            )
            constraints += await self.rows(
                "SELECT JSON_OBJECT('table',TABLE_NAME,'name',CONSTRAINT_NAME,"
                "'type',CONSTRAINT_TYPE,'enforced',ENFORCED) "
                "FROM information_schema.TABLE_CONSTRAINTS "
                "WHERE CONSTRAINT_SCHEMA=DATABASE() ORDER BY TABLE_NAME,CONSTRAINT_NAME"
            )
            indexes = await self.rows(
                "SELECT JSON_OBJECT('table',TABLE_NAME,'name',INDEX_NAME,"
                "'non_unique',NON_UNIQUE,'position',SEQ_IN_INDEX,'column',COLUMN_NAME,"
                "'collation',COLLATION,'prefix',SUB_PART,'type',INDEX_TYPE,"
                "'visible',IS_VISIBLE,'expression',EXPRESSION) "
                "FROM information_schema.STATISTICS WHERE TABLE_SCHEMA=DATABASE() "
                "ORDER BY TABLE_NAME,INDEX_NAME,SEQ_IN_INDEX"
            )
            sequence = []
            server = await self.query("SELECT @@server_uuid;")
        else:
            version = await self.query("SHOW server_version;")
            require_version(engine, version, version)
            privileged = await self.query(
                "SELECT COUNT(*) FROM pg_roles WHERE rolname=current_user AND "
                "(rolsuper OR rolcreaterole OR rolcreatedb OR rolreplication "
                "OR rolbypassrls);"
            )
            roles = await self.query(
                "SELECT COUNT(*) FROM pg_auth_members WHERE "
                "member=(SELECT oid FROM pg_roles WHERE rolname=current_user);"
            )
            other_create = await self.query(
                "SELECT COUNT(*) FROM pg_database WHERE datname<>current_database() "
                "AND has_database_privilege(current_user,oid,'CREATE');"
            )
            if privileged != "0" or roles != "0" or other_create != "0":
                raise MigrationError("migration_database_privileges_unsupported")
            if (
                quiet
                and await self.query(
                    "SELECT COUNT(*) FROM pg_stat_activity "
                    "WHERE datname=current_database() "
                    "AND pid<>pg_backend_pid();"
                )
                != "0"
            ):
                raise MigrationError("migration_database_other_connections")
            unsupported = await self.query(
                "SELECT (SELECT COUNT(*) FROM pg_namespace "
                "WHERE nspname NOT LIKE 'pg_%' "
                "AND nspname NOT IN ('public','information_schema')) + "
                "(SELECT COUNT(*) FROM pg_extension WHERE extname<>'plpgsql') + "
                "(SELECT COUNT(*) FROM pg_proc p JOIN pg_namespace n "
                "ON n.oid=p.pronamespace WHERE n.nspname='public') + "
                "(SELECT COUNT(*) FROM pg_trigger WHERE NOT tgisinternal) + "
                "(SELECT COUNT(*) FROM pg_class "
                "WHERE relnamespace='public'::regnamespace "
                "AND (relkind NOT IN ('r','i','S') OR relrowsecurity)) + "
                "(SELECT COUNT(*) FROM pg_type "
                "WHERE typnamespace='public'::regnamespace "
                "AND typtype IN ('e','d','r')) + "
                "(SELECT COUNT(*) FROM pg_largeobject_metadata) + "
                "(SELECT COUNT(*) FROM pg_event_trigger) + "
                "(SELECT COUNT(*) FROM pg_foreign_server) + "
                "(SELECT COUNT(*) FROM pg_publication) + "
                "(SELECT COUNT(oid) FROM pg_subscription WHERE subdbid="
                "(SELECT oid FROM pg_database WHERE datname=current_database())) + "
                "(SELECT COUNT(*) FROM pg_rewrite r JOIN pg_class c "
                "ON c.oid=r.ev_class WHERE c.relnamespace='public'::regnamespace "
                "AND r.rulename<>'_RETURN') + "
                "(SELECT COUNT(*) FROM pg_collation "
                "WHERE collnamespace='public'::regnamespace) + "
                "(SELECT COUNT(*) FROM pg_ts_config "
                "WHERE cfgnamespace='public'::regnamespace) + "
                "(SELECT COUNT(*) FROM pg_ts_dict "
                "WHERE dictnamespace='public'::regnamespace);"
            )
            if unsupported != "0":
                raise MigrationError("migration_database_objects_unsupported")
            tables = await self.rows(
                "SELECT tablename AS name FROM pg_tables "
                "WHERE schemaname='public' ORDER BY tablename"
            )
            columns = await self.rows(
                "SELECT table_name AS table,column_name AS name,data_type AS type,"
                "udt_name,is_nullable AS nullable,column_default AS default,"
                "numeric_precision,numeric_scale,character_maximum_length,"
                "is_identity,identity_generation,identity_start,identity_increment,"
                "identity_minimum,identity_maximum,identity_cycle,"
                "is_generated,generation_expression,collation_name "
                "FROM information_schema.columns WHERE table_schema='public' "
                "ORDER BY table_name,ordinal_position"
            )
            constraints = await self.rows(
                "SELECT c.relname AS table,con.conname AS name,"
                "con.convalidated AS validated,"
                "pg_get_constraintdef(con.oid) AS definition "
                "FROM pg_constraint con JOIN pg_class c ON c.oid=con.conrelid "
                "WHERE c.relnamespace='public'::regnamespace "
                "ORDER BY c.relname,con.conname"
            )
            indexes = await self.rows(
                "SELECT tablename AS table,indexname AS name,indexdef AS definition "
                "FROM pg_indexes WHERE schemaname='public' ORDER BY tablename,indexname"
            )
            foreign_keys = await self.rows(
                "SELECT c.relname AS table,p.relname AS parent,n.nspname AS schema,"
                "ARRAY(SELECT a.attname FROM unnest(con.conkey) "
                "WITH ORDINALITY k(num,pos) JOIN pg_attribute a "
                "ON a.attrelid=c.oid AND a.attnum=k.num ORDER BY k.pos) "
                "AS columns,"
                "ARRAY(SELECT a.attname FROM unnest(con.confkey) "
                "WITH ORDINALITY k(num,pos) JOIN pg_attribute a "
                "ON a.attrelid=p.oid AND a.attnum=k.num ORDER BY k.pos) "
                "AS references FROM pg_constraint con "
                "JOIN pg_class c ON c.oid=con.conrelid "
                "JOIN pg_class p ON p.oid=con.confrelid "
                "JOIN pg_namespace n ON n.oid=p.relnamespace WHERE con.contype='f' "
                "AND c.relnamespace='public'::regnamespace "
                "ORDER BY c.relname,con.conname"
            )
            if any(key["schema"] != "public" for key in foreign_keys):
                raise MigrationError("migration_database_objects_unsupported")
            sequence = []
            for row in await self.rows(
                "SELECT sequencename AS name,start_value,min_value,max_value,"
                "increment_by,cycle,cache_size FROM pg_sequences "
                "WHERE schemaname='public' ORDER BY sequencename"
            ):
                values = await self.rows(
                    "SELECT last_value,is_called FROM public."
                    + self.identifier(row["name"])
                )
                sequence.append({**row, **values[0]})
            server = await self.query(
                "SELECT COALESCE(inet_server_addr()::text,'local') "
                "|| ':' || inet_server_port()::text;"
            )
        for tool in self.tools:
            if tool not in self.tool_versions:
                output = await self.run(tool, ["--version"])
                self.tool_versions[tool] = tool_version(
                    engine, output.read_text("utf-8")
                )
            require_version(engine, version, self.tool_versions[tool])
        await self.verify_relations(foreign_keys)
        data = []
        for table in tables:
            names = [c["name"] for c in columns if c["table"] == table["name"]]
            if engine == "mysql":
                encoded = [
                    f"IF({self.identifier(n)} IS NULL,NULL,HEX(CAST("
                    + f"{self.identifier(n)} AS BINARY)))"
                    for n in names
                ]
                sql = (
                    "SELECT JSON_ARRAY("
                    + ",".join(encoded)
                    + ") AS encoded FROM "
                    + self.identifier(table["name"])
                    + " ORDER BY BINARY encoded;"
                )
                tool, args = (
                    "mysql",
                    [
                        *self.connection,
                        "--init-command=SET time_zone='+00:00'",
                        "--batch",
                        "--raw",
                        "--skip-column-names",
                        "--binary-mode",
                        f"--database={self.endpoint.database}",
                    ],
                )
            else:
                encoded = [
                    f"encode(convert_to({self.identifier(n)}::text,'UTF8'),'hex')"
                    for n in names
                ]
                sql = (
                    "SELECT encoded FROM (SELECT json_build_array("
                    + ",".join(encoded)
                    + ")::text AS encoded FROM public."
                    + self.identifier(table["name"])
                    + ') zx ORDER BY encoded COLLATE "C";'
                )
                tool, args = (
                    "psql",
                    [*self.connection, "-X", "-A", "-t", "-v", "ON_ERROR_STOP=1"],
                )
            output = await self.run(tool, args, sql=sql, maximum=Limits().expanded)
            digest = hashlib.sha256()
            count = 0
            with output.open("rb") as stream:
                while line := stream.readline(1024 * 1024 + 1):
                    self.check()
                    if len(line) > 1024 * 1024:
                        raise MigrationError("migration_database_row_analysis_limit")
                    digest.update(line.rstrip(b"\r\n") + b"\n")
                    count += 1
            data.append(
                {"table": table["name"], "rows": count, "sha256": digest.hexdigest()}
            )
            output.unlink()
        if engine == "mysql":
            # InnoDB may expose NULL for a never-opened empty table's counter.
            # The row reads above open every table. Inspect authoritative counters
            # afterwards so dump/restore does not turn NULL into a false change.
            sequence = await self.rows(
                "SELECT JSON_OBJECT('table',TABLE_NAME,'next',AUTO_INCREMENT) "
                "FROM information_schema.TABLES WHERE TABLE_SCHEMA=DATABASE() "
                "ORDER BY TABLE_NAME"
            )
        evidence = {
            "columns": columns,
            "constraints": constraints,
            "indexes": indexes,
            "sequences": sequence,
            "data": data,
        }
        return {
            "engine": engine,
            "engine_version": version,
            "server": server,
            "database": self.endpoint.database,
            "tables": [t["name"] for t in tables],
            "evidence": evidence,
            "revision": hashlib.sha256(
                json.dumps(evidence, sort_keys=True).encode()
            ).hexdigest(),
        }

    async def dump(self, destination, *, maximum=Limits().expanded):
        if self.endpoint.engine == "mysql":
            await self.run(
                "mysqldump",
                [
                    *self.connection,
                    "--single-transaction",
                    "--no-tablespaces",
                    "--set-gtid-purged=OFF",
                    "--hex-blob",
                    "--skip-triggers",
                    self.endpoint.database,
                ],
                output_path=destination,
                maximum=maximum,
            )
        else:
            await self.run(
                "pg_dump",
                [
                    *self.connection,
                    "--format=custom",
                    "--no-owner",
                    "--no-acl",
                    "--no-tablespaces",
                ],
                output_path=destination,
                maximum=maximum,
            )
        return {
            "sha256": file_hash(destination, self.check),
            "size": destination.stat().st_size,
            "backup_format": "mysql_sql"
            if self.endpoint.engine == "mysql"
            else "postgres_custom",
        }

    async def restore(self, payload, *, confirmed_tables):
        self.check()
        if self.endpoint.engine == "mysql":
            if confirmed_tables:
                await self.query(
                    "SET FOREIGN_KEY_CHECKS=0;DROP TABLE "
                    + ",".join(self.identifier(t) for t in confirmed_tables)
                    + ";SET FOREIGN_KEY_CHECKS=1;"
                )
            await self.run(
                "mysql",
                [
                    *self.connection,
                    "--binary-mode",
                    "--batch",
                    f"--database={self.endpoint.database}",
                ],
                input_path=payload,
            )
        else:
            # inspect() refuses all user schemas/objects outside this owned scope.
            await self.query("DROP SCHEMA public CASCADE;CREATE SCHEMA public;")
            await self.run(
                "pg_restore",
                [
                    *self.connection,
                    "--no-owner",
                    "--no-acl",
                    "--no-tablespaces",
                    "--exit-on-error",
                    payload,
                ],
            )

    def close(self):
        try:
            for path in self.directory.iterdir():
                contained_path(self.directory, path.name, regular=True).unlink()
            self.directory.rmdir()
        except OSError:
            raise MigrationError(
                "migration_sensitive_temporary_cleanup_failed"
            ) from None


@asynccontextmanager
async def native_session(endpoint, directory, budget, checkpoint=lambda: None):
    supervisor = LauncherSupervisor(LifecycleKernel())
    client = None
    token = current_budget.set(ShutdownBudget(budget.deadline))
    try:
        client = NativeDatabase.__new__(NativeDatabase)
        client.__init__(endpoint, directory, supervisor, budget, checkpoint)
        yield client
    finally:
        try:
            supervisor.shutdown_deadline = ShutdownBudget(budget.deadline)
            await supervisor.shutdown()
            if client is not None and hasattr(client, "directory"):
                client.close()
        finally:
            current_budget.reset(token)
