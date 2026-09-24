from __future__ import annotations

import asyncio
from dataclasses import asdict, dataclass, field
import hashlib
import hmac
import json
import os
from pathlib import Path
import re
import secrets
import ssl
from urllib.parse import parse_qsl, quote, unquote, urlencode, urlsplit

_FINGERPRINT_KEY = secrets.token_bytes(32)
_PG_TLS = {
    "sslmode",
    "sslrootcert",
    "sslcert",
    "sslkey",
    "sslcrl",
    "sslpassword",
    "ssl_min_protocol_version",
    "ssl_max_protocol_version",
}
_PG_ENV = {key: "PG" + key.replace("_", "").upper() for key in _PG_TLS}
_PG_DEFAULT_CERTIFICATES = {
    "sslrootcert": "root.crt",
    "sslcert": "postgresql.crt",
    "sslkey": "postgresql.key",
    "sslcrl": "root.crl",
}
_PG_OPTIONS = {
    "minsize",
    "maxsize",
    "timeout",
    "command_timeout",
    "statement_cache_size",
    "max_queries",
    "max_inactive_connection_lifetime",
    "schema",
    "application_name",
}
_MYSQL_OPTIONS = {"minsize", "maxsize", "connect_timeout", "pool_recycle", "charset"}


@dataclass(frozen=True)
class DatabaseConnection:
    """Resolved connection policy shared by runtime, probes and native tools."""

    engine: str
    host: str
    port: int
    database: str
    username: str = field(repr=False)
    password: str = field(repr=False)
    tls: str = ""
    options: dict[str, str] = field(default_factory=dict, repr=False)
    tls_source: str = "driver_default"
    certificate_defaults: dict[str, str] = field(default_factory=dict, repr=False)
    source: str = "target_configuration"

    @classmethod
    def parse(cls, value: str, *, root: Path | None = None, environment=None):
        environment = os.environ if environment is None else environment
        root = (root or Path.cwd()).resolve()
        try:
            parsed = urlsplit(value)
            engine = {"postgresql": "postgres", "asyncpg": "postgres"}.get(
                parsed.scheme, parsed.scheme
            )
            if engine == "sqlite":
                if parsed.query or parsed.fragment:
                    raise ValueError("database_options_unsupported")
                path = (
                    ":memory:"
                    if is_sqlite_memory_url(value)
                    else str(sqlite_path_from_url(value, root))
                )
                return cls(engine, "", 0, path, "", "", "not_applicable")
            if (
                engine not in {"postgres", "mysql"}
                or not parsed.hostname
                or parsed.fragment
            ):
                raise ValueError("database_connection_invalid")
            pairs = (
                parse_qsl(parsed.query, keep_blank_values=True, strict_parsing=True)
                if parsed.query
                else []
            )
            options = {}
            for key, val in pairs:
                key = {
                    "ssl_mode": "sslmode",
                    "ssl-ca": "sslrootcert",
                    "ssl-cert": "sslcert",
                    "ssl-key": "sslkey",
                }.get(key, key)
                if key in options or not val or any(c in val for c in "\r\n\x00"):
                    raise ValueError("database_options_invalid")
                options[key] = val
            allowed = _PG_OPTIONS if engine == "postgres" else _MYSQL_OPTIONS
            if set(options) - allowed - _PG_TLS - {"ssl"}:
                raise ValueError("database_options_unsupported")
            if "ssl" in options:
                if "sslmode" in options:
                    raise ValueError("database_tls_options_conflict")
                val = options.pop("ssl").lower()
                options["sslmode"] = {
                    "true": "verify-full",
                    "1": "verify-full",
                    "false": "disable",
                    "0": "disable",
                }.get(val, val)
            source = "url" if "sslmode" in options else "driver_default"
            certificate_defaults = {}
            if engine == "postgres":
                if (
                    environment.get("PGSERVICE")
                    or environment.get("PGSSLNEGOTIATION", "postgres") != "postgres"
                ):
                    raise ValueError("database_environment_policy_unsupported")
                for key in _PG_TLS:
                    env_key = _PG_ENV[key]
                    if key not in options and environment.get(env_key):
                        options[key] = environment[env_key]
                        if key == "sslmode":
                            source = "environment"
                mode = options.pop("sslmode", "prefer")
                if mode not in {
                    "disable",
                    "allow",
                    "prefer",
                    "require",
                    "verify-ca",
                    "verify-full",
                }:
                    raise ValueError("database_tls_mode_invalid")
                # Freeze implicit certificate locations for the process handoff.
                if mode != "disable":
                    for key, filename in _PG_DEFAULT_CERTIFICATES.items():
                        default = Path.home() / ".postgresql" / filename
                        if key not in options:
                            if default.is_file():
                                options[key] = str(default.resolve())
                            else:
                                certificate_defaults[key] = str(default.resolve())
            else:
                mode = options.pop("sslmode", "DISABLED")
                mode = {
                    "disable": "DISABLED",
                    "require": "REQUIRED",
                    "verify-ca": "VERIFY_CA",
                    "verify-full": "VERIFY_IDENTITY",
                }.get(mode, mode.upper())
                if mode not in {"DISABLED", "REQUIRED", "VERIFY_CA", "VERIFY_IDENTITY"}:
                    raise ValueError("database_tls_mode_invalid")
                if set(options) & {
                    "sslcrl",
                    "sslpassword",
                    "ssl_min_protocol_version",
                    "ssl_max_protocol_version",
                }:
                    raise ValueError("database_options_unsupported")
            for key in ("sslrootcert", "sslcert", "sslkey", "sslcrl"):
                if key in options:
                    if options[key] == "system":
                        raise ValueError("database_tls_system_roots_unsupported")
                    path = Path(options[key]).expanduser()
                    options[key] = str(
                        (root / path).resolve()
                        if not path.is_absolute()
                        else path.resolve()
                    )
            if any(any(c in value for c in "\r\n\x00") for value in options.values()):
                raise ValueError("database_options_invalid")
            username, password = (
                unquote(parsed.username or ""),
                unquote(parsed.password or ""),
            )
            database = unquote(parsed.path.removeprefix("/"))
            if (
                not username
                or not database
                or any(
                    c in username + password + database + parsed.hostname
                    for c in "\r\n\x00"
                )
            ):
                raise ValueError("database_connection_invalid")
            port = parsed.port
            if port is not None and not 1 <= port <= 65535:
                raise ValueError("database_port_invalid")
            return cls(
                engine,
                parsed.hostname,
                port or (5432 if engine == "postgres" else 3306),
                database,
                username,
                password,
                mode,
                options,
                source,
                certificate_defaults,
            )
        except ValueError as error:
            if str(error).startswith("database_"):
                raise
            raise ValueError("database_connection_invalid") from None
        except (TypeError, AttributeError):
            raise ValueError("database_connection_invalid") from None

    def identity(self) -> dict:
        return {
            "engine": self.engine,
            "host": self.host.lower(),
            "port": self.port,
            "database": self.database,
        }

    def public(self) -> dict:
        return {
            **self.identity(),
            "ssl_mode": self.tls,
            "ssl_source": self.tls_source,
            "has_root_certificate": bool(self.options.get("sslrootcert")),
            "has_client_certificate": bool(self.options.get("sslcert")),
            "source": self.source,
        }

    def certificate_hashes(self) -> dict:
        result = {}
        for key, filename in self.certificate_defaults.items():
            if Path(filename).exists():
                raise ValueError("database_connection_changed")
            result[key] = None
        for key in ("sslrootcert", "sslcert", "sslkey", "sslcrl"):
            if key in self.options:
                try:
                    result[key] = hashlib.sha256(
                        Path(self.options[key]).read_bytes()
                    ).hexdigest()
                except OSError:
                    raise ValueError("database_tls_certificate_unreadable") from None
        return result

    def fingerprint(self) -> str:
        value = {**asdict(self), "certificates": self.certificate_hashes()}
        return hmac.new(
            _FINGERPRINT_KEY, json.dumps(value, sort_keys=True).encode(), "sha256"
        ).hexdigest()

    def orm_config(self, defaults: dict | None = None) -> dict:
        """Translate the resolved policy into Tortoise driver credentials."""
        if self.engine == "sqlite":
            return {
                "engine": "tortoise.backends.sqlite",
                "credentials": {**(defaults or {}), "file_path": self.database},
            }
        self.certificate_hashes()
        credentials = {
            **(defaults or {}),
            "host": self.host,
            "port": self.port,
            "user": self.username,
            "password": self.password,
            "database": self.database,
        }
        for key, val in self.options.items():
            if key in _PG_TLS:
                continue
            credentials[key] = (
                val
                if key in {"schema", "application_name", "charset"}
                else float(val)
                if key
                in {
                    "timeout",
                    "command_timeout",
                    "connect_timeout",
                    "pool_recycle",
                    "max_inactive_connection_lifetime",
                }
                else int(val)
            )
        if self.engine == "postgres":
            query = {
                key: value for key, value in self.options.items() if key in _PG_TLS
            }
            query["sslmode"] = self.tls
            credentials["dsn"] = "postgresql:///?" + urlencode(query)
            return {
                "engine": "zhenxun.services.database_postgres",
                "credentials": credentials,
            }
        if self.tls != "DISABLED":
            context = ssl.create_default_context(cafile=self.options.get("sslrootcert"))
            context.check_hostname = self.tls == "VERIFY_IDENTITY"
            if self.tls == "REQUIRED":
                context.verify_mode = ssl.CERT_NONE
            if self.options.get("sslcert"):
                context.load_cert_chain(
                    self.options["sslcert"], self.options.get("sslkey")
                )
            credentials["ssl"] = context
        return {"engine": "tortoise.backends.mysql", "credentials": credentials}

    def url(self) -> str:
        if self.engine == "sqlite":
            return "sqlite:" + quote(self.database, safe="/:\\")
        host = f"[{self.host}]" if ":" in self.host else self.host
        query = urlencode({**self.options, "sslmode": self.tls})
        return (
            f"{self.engine}://{quote(self.username, safe='')}:"
            f"{quote(self.password, safe='')}@{host}:{self.port}/"
            f"{quote(self.database, safe='')}?{query}"
        )


_applied_connection: DatabaseConnection | None = None
_applied_certificates: dict | None = None


def database_error_code(error: BaseException) -> str:
    """Classify connection failures without exposing driver credential strings."""
    seen = set()
    while error is not None and id(error) not in seen:
        seen.add(id(error))
        if isinstance(error, ssl.SSLCertVerificationError):
            return "database_tls_verification_failed"
        if isinstance(error, OSError) and getattr(error, "winerror", None) == 87:
            return "database_tls_driver_incompatible"
        if isinstance(error, TimeoutError | asyncio.TimeoutError):
            return "database_timeout"
        if isinstance(error, ConnectionRefusedError):
            return "database_connection_refused"
        code = str(error)
        if code.startswith("database_") and code.replace("_", "").isalnum():
            return code
        error = error.__cause__ or error.__context__
    return "database_connection_failed"


async def inspect_database_tls(endpoint: DatabaseConnection, client) -> bool | None:
    """Read negotiated TLS on the current pool and enforce required encryption."""
    if endpoint.engine == "postgres":
        rows = await client.execute_query_dict(
            "SELECT ssl FROM pg_stat_ssl WHERE pid=pg_backend_pid()"
        )
        observed = bool(rows[0]["ssl"]) if rows else None
        required = endpoint.tls in {"require", "verify-ca", "verify-full"}
    elif endpoint.engine == "mysql":
        rows = await client.execute_query_dict("SHOW SESSION STATUS LIKE 'Ssl_cipher'")
        observed = bool(rows[0].get("Value")) if rows else None
        required = endpoint.tls != "DISABLED"
    else:
        return None
    if required and observed is not True:
        raise ValueError("database_tls_not_negotiated")
    return observed


def bind_database_connection(value: DatabaseConnection | None) -> None:
    """Publish only the connection policy used by the initialized database."""
    global _applied_connection, _applied_certificates, _FINGERPRINT_KEY
    _applied_certificates = value.certificate_hashes() if value is not None else None
    _FINGERPRINT_KEY = secrets.token_bytes(32)
    _applied_connection = value


def applied_database_connection() -> DatabaseConnection:
    if _applied_connection is None:
        raise ValueError("database_runtime_unconfirmed")
    if _applied_connection.certificate_hashes() != _applied_certificates:
        raise ValueError("database_connection_changed")
    return _applied_connection


def is_sqlite_memory_url(value: str) -> bool:
    scheme, separator, raw_path = value.partition(":")
    if not separator or scheme.casefold() != "sqlite":
        return False
    return raw_path.lstrip("/").partition("?")[0].casefold() == ":memory:"


def sqlite_path_from_url(value: str, root: Path | None = None) -> Path:
    """Resolve supported SQLite URLs without treating a relative path as a host."""
    scheme, separator, raw_path = value.partition(":")
    if not separator or scheme.casefold() != "sqlite":
        raise ValueError("database_url_not_sqlite")

    # ``sqlite://data/file.db`` was emitted by the first-setup UI. URL parsers
    # interpret ``data`` as a host, but it was always intended as a relative path.
    if raw_path.startswith("//"):
        raw_path = raw_path[2:]
    raw_path = unquote(raw_path.partition("?")[0])
    if re.match(r"^/[A-Za-z]:[/\\]", raw_path):
        raw_path = raw_path[1:]

    path = Path(raw_path)
    if not path.is_absolute():
        path = (root or Path.cwd()) / path
    return path.resolve()


__all__ = ["is_sqlite_memory_url", "sqlite_path_from_url"]
