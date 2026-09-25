"""Bounded official-tool operations for same-engine database migration."""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from dataclasses import asdict
from functools import partial
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import time
from urllib.parse import quote
import uuid

from zhenxun.configs.database import DatabaseConnection
from zhenxun.services.lifecycle.deadline import ShutdownBudget, current_budget
from zhenxun.services.lifecycle.kernel import LifecycleKernel
from zhenxun.services.lifecycle.launcher import LauncherSupervisor

from .access import private_directory
from .archive import Limits, file_hash, require_space
from .errors import MigrationError
from .paths import contained_path

_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_-]{0,62}$")
_URL_CREDENTIAL = re.compile(r"(?i)(\b[a-z][a-z0-9+.-]{0,31}://)[^\s/@]+@")
_PASSWORD_VALUE = re.compile(
    r"(?im)(\b(?:[A-Z_]*(?:password|passwd|pwd|secret|token|credential|authorization)[A-Z_]*|PGPASSWORD)\s*[=:]\s*)(?:\"[^\"]*\"|'[^']*'|[^\r\n,;]+)"
)
_TOOLS = {
    "mysql": ("mysql", "mysqldump"),
    "postgres": ("psql", "pg_dump", "pg_restore"),
}


class DatabaseEndpoint(DatabaseConnection):
    @classmethod
    def from_connection(cls, endpoint):
        """Validate migration restrictions without resolving configuration again."""
        if endpoint.engine not in _TOOLS or not _NAME.fullmatch(endpoint.database):
            raise MigrationError("migration_database_connection_invalid")
        if endpoint.options.get("schema", "public") != "public":
            raise MigrationError("migration_database_schema_unsupported")
        if len(endpoint.username) > 128 or len(endpoint.password) > 4096:
            raise MigrationError("migration_database_connection_invalid")
        if endpoint.engine == "postgres" and endpoint.tls in {"allow", "prefer"}:
            if endpoint.options.get("sslrootcert") or endpoint.options.get("sslcrl"):
                raise MigrationError("migration_database_tls_policy_not_equivalent")
        if endpoint.engine == "mysql" and endpoint.tls in {
            "VERIFY_CA",
            "VERIFY_IDENTITY",
        }:
            if not endpoint.options.get("sslrootcert"):
                raise MigrationError("migration_database_tls_policy_not_equivalent")
        return cls(**asdict(endpoint))

    @classmethod
    def parse(cls, value: str, **kwargs):
        try:
            endpoint = DatabaseConnection.parse(value, **kwargs)
            return cls.from_connection(endpoint)
        except (ValueError, OSError) as error:
            code = str(error)
            raise MigrationError(
                "migration_" + code
                if code.startswith("database_")
                else "migration_database_connection_invalid"
            ) from None


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


def redact_database_output(value: str, secrets=()) -> str:
    """Remove credential-shaped values while retaining the tool diagnostic."""
    for secret in sorted(set(filter(None, secrets)), key=len, reverse=True):
        for encoded in {
            secret,
            quote(secret, safe=""),
            secret.replace("\\", "\\\\").replace(":", "\\:"),
        }:
            value = value.replace(encoded, "<redacted>")
    value = re.sub(r"\x1b\[[0-?]*[ -/]*[@-~]", "", value)
    value = "".join(c for c in value if c in "\n\t" or ord(c) >= 32)
    value = _URL_CREDENTIAL.sub(r"\1<redacted>@", value)
    return _PASSWORD_VALUE.sub(r"\1<redacted>", value)


class NativeDatabase:
    def __init__(
        self,
        endpoint,
        directory,
        supervisor,
        budget,
        checkpoint=lambda: None,
        *,
        diagnostic=None,
        progress=None,
        phase="database",
    ):
        self.endpoint = endpoint
        self.directory = private_directory(directory / uuid.uuid4().hex)
        self.supervisor, self.budget, self.checkpoint = supervisor, budget, checkpoint
        self.diagnostic, self.progress, self.phase = diagnostic, progress, phase
        self.secrets = [
            endpoint.password,
            endpoint.options.get("sslpassword"),
            *[
                value
                for key, value in os.environ.items()
                if re.search(
                    r"PASSWORD|SECRET|TOKEN|CREDENTIAL|DATABASE_URL|DB_URL", key, re.I
                )
            ],
        ]
        self.tools = {}
        self.tool_versions = {}
        self.last_diagnostic = None
        self.last_diagnostic_at = 0.0
        for name in _TOOLS[endpoint.engine]:
            executable = shutil.which(name)
            if executable is None:
                if diagnostic is not None:
                    diagnostic(
                        {
                            "tool": name,
                            "engine": endpoint.engine,
                            "phase": phase,
                            "operation": "locate",
                            "error_code": "migration_database_tool_missing",
                            "stderr": "",
                            "return_code": None,
                            "recorded_at": time.time(),
                        }
                    )
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
                        "default-character-set": endpoint.options.get(
                            "charset", "utf8mb4"
                        ),
                        **{
                            target: endpoint.options[source]
                            for source, target in {
                                "sslrootcert": "ssl-ca",
                                "sslcert": "ssl-cert",
                                "sslkey": "ssl-key",
                            }.items()
                            if source in endpoint.options
                        },
                    }.items()
                )
                + "\n",
                encoding="utf-8",
            )
            self.secrets.extend(
                (credential.read_text("utf-8"), escape(endpoint.password))
            )
            self.connection = [f"--defaults-file={credential}"]
            if endpoint.tls == "DISABLED":
                # SHA2 authentication needs the server key without TLS.
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
            self.secrets.extend(
                (credential.read_text("utf-8"), escape(endpoint.password))
            )
            service = self.directory / "pg_service.conf"
            service.write_text(
                "[migration]\n"
                + "\n".join(
                    f"{key}={value}"
                    for key, value in {
                        **endpoint.certificate_defaults,
                        **{
                            key: value
                            for key, value in endpoint.options.items()
                            if key.startswith("ssl")
                        },
                        "sslmode": endpoint.tls,
                        "gssencmode": "disable",
                    }.items()
                )
                + "\n",
                encoding="utf-8",
            )
            service.chmod(0o600)
            self.secrets.append(service.read_text("utf-8"))
            self.environment.update(
                PGPASSFILE=str(credential),
                PGSERVICEFILE=str(service),
                PGSERVICE="migration",
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
        started = time.monotonic()
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
        failure = None
        cleanup_error = None
        timed_out = False
        started_at = time.time()
        operation = (
            "version"
            if args == ["--version"]
            else "dump"
            if tool in {"mysqldump", "pg_dump"}
            else "restore"
            if (input_path is not None and sql is None) or tool == "pg_restore"
            else "query"
        )
        if self.progress is not None:
            self.progress(step=f"数据库 {tool} / {operation}")
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
                    if self.progress is not None:
                        self.progress(
                            step=f"数据库 {tool} / {operation}",
                            bytes_done=output.stat().st_size,
                            bytes_total=None,
                        )
                    require_space(self.directory, 16 * 1024**2)
                    await asyncio.sleep(0.05)
            self.check()
            if output.stat().st_size > maximum:
                raise MigrationError("migration_database_output_limit")
            if error.stat().st_size > 1024**2:
                raise MigrationError("migration_database_output_limit")
            if process.returncode:
                raise MigrationError("migration_database_tool_failed")
            if operation == "version":
                self.tool_versions[tool] = tool_version(
                    self.endpoint.engine, output.read_text("utf-8", errors="replace")
                )
            return output
        except BaseException as caught:
            failure = caught
            timed_out = getattr(caught, "code", None) in {
                "migration_budget_exhausted",
                "migration_control_timeout",
            } or isinstance(caught, asyncio.TimeoutError)
            raise
        finally:
            try:
                if process is not None:
                    await self.supervisor.stop_process(process, allow_force=True)
            except BaseException as caught:
                cleanup_error = caught
                if failure is None:
                    failure = caught
                    raise
            finally:
                if self.diagnostic is not None and (
                    failure is not None
                    or (tool, operation) != self.last_diagnostic
                    or time.monotonic() - self.last_diagnostic_at >= 1
                ):
                    try:
                        raw = b""
                        if error.exists():
                            with error.open("rb") as stream:
                                raw = stream.read(1024**2)
                        record = {
                            "tool": tool,
                            "engine": self.endpoint.engine,
                            "phase": self.phase,
                            "operation": operation,
                            "return_code": process.returncode
                            if process is not None
                            else None,
                            "started_at": started_at,
                            "duration_seconds": round(time.monotonic() - started, 2),
                            "stdout_bytes": output.stat().st_size
                            if output.exists()
                            else 0,
                            "stderr_bytes": error.stat().st_size
                            if error.exists()
                            else 0,
                            "stderr": redact_database_output(
                                raw.decode("utf-8", errors="replace"), self.secrets
                            ),
                            "truncated": error.exists()
                            and error.stat().st_size > 1024**2,
                            "tool_version": self.tool_versions.get(tool),
                            "environment": (
                                f"{sys.platform}; Python {sys.version.split()[0]}"
                            ),
                            "error_code": getattr(
                                failure, "code", type(failure).__name__
                            )
                            if failure
                            else None,
                            "cleanup_error": type(cleanup_error).__name__
                            if cleanup_error
                            else None,
                            "timed_out": timed_out,
                            "process_returned": process is not None
                            and process.poll() is not None,
                            "diagnostic_id": identity,
                            "recorded_at": time.time(),
                        }
                        if len(record["stderr"]) > 1024**2:
                            record["stderr"] = record["stderr"][: 1024**2]
                            record["truncated"] = True
                        self.diagnostic(record)
                        self.last_diagnostic = (tool, operation)
                        self.last_diagnostic_at = time.monotonic()
                    except Exception:
                        if failure is None:
                            raise

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

    async def probe_connection(self):
        """Check connection, tool versions and read privileges without a snapshot."""
        for tool in self.tools:
            await self.run(tool, ["--version"])
        if self.endpoint.engine == "postgres":
            version = await self.query("SHOW server_version;")
            tls = await self.query(
                "SELECT ssl FROM pg_stat_ssl WHERE pid=pg_backend_pid();"
            )
            denied = await self.query(
                "SELECT count(*) FROM pg_class c "
                "JOIN pg_namespace n ON n.oid=c.relnamespace "
                "WHERE n.nspname='public' AND c.relkind IN ('r','p','S') "
                "AND NOT CASE WHEN c.relkind='S' "
                "THEN has_sequence_privilege(c.oid,'SELECT') "
                "ELSE has_table_privilege(c.oid,'SELECT') END;"
            )
            if denied != "0":
                raise MigrationError("migration_database_read_permission_denied")
            observed = tls == "t" if tls in {"t", "f"} else None
        else:
            version = await self.query("SELECT VERSION();")
            tls = await self.query("SHOW SESSION STATUS LIKE 'Ssl_cipher';")
            observed = bool(tls.partition("\t")[2])
            tables = await self.query(
                "SELECT TABLE_NAME FROM information_schema.TABLES "
                "WHERE TABLE_SCHEMA=DATABASE();"
            )
            for table in tables.splitlines():
                await self.query(f"SELECT 1 FROM {self.identifier(table)} LIMIT 0;")
        for tool in self.tools:
            require_version(self.endpoint.engine, version, self.tool_versions[tool])
        return {
            "observed_tls": observed,
            "server_version": version,
            "tools": dict(self.tool_versions),
        }

    async def inspect(self, *, quiet=True):
        engine = self.endpoint.engine
        for tool in self.tools:
            if tool not in self.tool_versions:
                await self.run(tool, ["--version"])
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
            privilege_started = time.monotonic()
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
                checks = {
                    "privileged_roles": int(privileged),
                    "direct_role_memberships": int(roles),
                    "other_database_create": int(other_create),
                }
                reasons = [
                    label
                    for label, value in (
                        ("privileged_roles", checks["privileged_roles"]),
                        (
                            "direct_role_memberships",
                            checks["direct_role_memberships"],
                        ),
                        (
                            "other_database_create",
                            checks["other_database_create"],
                        ),
                    )
                    if value
                ]
                if self.diagnostic is not None:
                    self.diagnostic(
                        {
                            "tool": "psql",
                            "engine": "postgres",
                            "phase": self.phase,
                            "operation": "policy",
                            "return_code": 0,
                            "started_at": time.time()
                            - (time.monotonic() - privilege_started),
                            "duration_seconds": round(
                                time.monotonic() - privilege_started, 2
                            ),
                            "stdout_bytes": 0,
                            "stderr_bytes": 0,
                            "stderr": "",
                            "truncated": False,
                            "tool_version": self.tool_versions.get("psql"),
                            "environment": (
                                f"{sys.platform}; Python {sys.version.split()[0]}"
                            ),
                            "error_code": "migration_database_privileges_unsupported",
                            "timed_out": False,
                            "process_returned": True,
                            "diagnostic_id": uuid.uuid4().hex,
                            "permission_checks": checks,
                            "privilege_reasons": reasons,
                            "recorded_at": time.time(),
                        }
                    )
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
async def native_session(
    endpoint,
    directory,
    budget,
    checkpoint=lambda: None,
    *,
    diagnostic=None,
    progress=None,
    phase="database",
):
    supervisor = LauncherSupervisor(LifecycleKernel())
    client = None
    failure = None
    token = current_budget.set(ShutdownBudget(budget.deadline))
    try:
        client = NativeDatabase.__new__(NativeDatabase)
        client.__init__(
            endpoint,
            directory,
            supervisor,
            budget,
            checkpoint,
            diagnostic=diagnostic,
            progress=progress,
            phase=phase,
        )
        yield client
    except BaseException as caught:
        failure = caught
        raise
    finally:
        try:
            supervisor.shutdown_deadline = ShutdownBudget(budget.deadline)
            try:
                await supervisor.shutdown()
            finally:
                if client is not None and hasattr(client, "directory"):
                    client.close()
        except BaseException as cleanup_error:
            if diagnostic is not None:
                try:
                    diagnostic(
                        {
                            "tool": "session",
                            "engine": endpoint.engine,
                            "phase": phase,
                            "operation": "cleanup",
                            "stderr": "",
                            "error_code": getattr(failure, "code", None)
                            or getattr(
                                cleanup_error, "code", type(cleanup_error).__name__
                            ),
                            "cleanup_error": getattr(
                                cleanup_error, "code", type(cleanup_error).__name__
                            ),
                            "recorded_at": time.time(),
                        }
                    )
                except Exception:
                    # A diagnostic write must not replace the tool or cleanup failure.
                    pass
            if failure is None:
                raise
        finally:
            current_budget.reset(token)
