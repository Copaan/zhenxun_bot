from __future__ import annotations

import hashlib
from io import StringIO
import os
from pathlib import Path
import socket
import tempfile

from dotenv.parser import parse_stream
from dotenv.variables import Variable, parse_variables

from zhenxun.configs.database import DatabaseConnection, sqlite_path_from_url
from zhenxun.utils.atomic_json import (
    mutate_json_locked,
    read_json_locked,
    write_json_locked,
)
from zhenxun.utils.passwords import hash_password, is_password_hash

from .archive import file_hash, open_archive, verify_archive
from .configuration import _CONFIRMED_ENV, confirmed_environment, yaml_document
from .errors import MigrationError
from .paths import contained_path
from .selection import ReplacementSelection, export_selection
from .tasks import MigrationBudget, TaskStore

RESTORE_FLOW_VERSION = 1
_CONFIGURATION_FILES = (
    ".env",
    ".env.dev",
    "data/config.yaml",
    "data/configs/plugins2config.yaml",
)


def _password_hash(store, upload, value, *, explicit=False):
    if not explicit and is_password_hash(value):
        return value
    identity = store.private_fingerprint(
        {"explicit_password" if explicit else "administrator_password": value}
    )
    path = store.path("uploads", upload["id"]).parent / (
        "administrator-" + identity + ".json"
    )

    def create(record):
        if not record.get("hash"):
            record["hash"] = hash_password(value)
        return record["hash"]

    return mutate_json_locked(path, {}, create)


def _environment(documents, *, inherited=None):
    values = {}
    variables = dict(os.environ)
    issues = []
    for name in (".env", ".env.dev"):
        for binding in parse_stream(StringIO(documents.get(name, ""))):
            if binding.error:
                issues.append({"field": name, "code": "migration_env_parse_failed"})
                continue
            if binding.key is None:
                continue
            key = binding.key.upper()
            scope = {**variables, **(inherited or {})}
            atoms = list(parse_variables(binding.value or ""))
            if any(
                isinstance(atom, Variable)
                and atom.name not in scope
                and atom.default is None
                for atom in atoms
            ):
                if key in _CONFIRMED_ENV:
                    issues.append(
                        {"field": key, "code": "migration_environment_value_missing"}
                    )
                continue
            values[key] = "".join(atom.resolve(scope) for atom in atoms)
            variables[binding.key] = values[key]
    values.update(inherited or {})
    return {
        key: value for key, value in values.items() if key in _CONFIRMED_ENV
    }, issues


def _administrator(documents):
    result = {}
    for name in reversed(_CONFIGURATION_FILES[2:]):
        if not documents.get(name):
            continue
        value = yaml_document(documents[name])
        groups = [
            item for key, item in value.items() if str(key).casefold() == "web-ui"
        ]
        if len(groups) > 1:
            raise MigrationError("migration_configuration_ambiguous_key", path=name)
        group = groups[0] if groups else {}
        if not isinstance(group, dict):
            raise MigrationError("migration_configuration_shape_invalid", path=name)
        seen = set()
        for key, item in group.items():
            key = str(key).upper()
            if key not in {"USERNAME", "PASSWORD"}:
                continue
            if key in seen:
                raise MigrationError("migration_configuration_ambiguous_key", path=name)
            seen.add(key)
            result[key] = item.get("value") if isinstance(item, dict) else item
    return result


def _administrator_defaults(documents, issues):
    try:
        return _administrator(documents)
    except MigrationError as error:
        issues.append({"field": "administrator", "code": error.code})
        return {}


def _package(store, upload, password, budget):
    directory = store.path("uploads", upload["id"]).parent
    path = contained_path(directory, "archive.zx.part", regular=True)
    digest = file_hash(path, budget.checkpoint)
    if digest != upload["sha256"]:
        raise MigrationError("migration_archive_changed", status=409)
    binding = store.private_fingerprint(
        {"sha256": digest, "password": password.decode() if password else None}
    )
    cached = read_json_locked(directory / "verified-manifest.json", {})
    if cached.get("binding") == binding:
        manifest = cached["manifest"]
    else:
        manifest = verify_archive(path, password=password, checkpoint=budget.checkpoint)
        write_json_locked(
            directory / "verified-manifest.json",
            {"binding": binding, "manifest": manifest},
        )
    documents = {}
    with open_archive(path, password=password) as (archive, checked):
        if checked != manifest:
            raise MigrationError("migration_archive_changed", status=409)
        for entry in manifest["files"]:
            if entry["path"] not in _CONFIGURATION_FILES:
                continue
            if entry["size"] > 8 * 1024 * 1024:
                raise MigrationError(
                    "migration_configuration_limit", path=entry["path"]
                )
            content = archive.read(entry["payload"])
            if hashlib.sha256(content).hexdigest() != entry["sha256"]:
                raise MigrationError(
                    "migration_payload_hash_mismatch", path=entry["path"]
                )
            documents[entry["path"]] = content.decode("utf-8-sig")
    return manifest, documents


def _listener_certificates(store, upload, manifest, password, environment, budget):
    """Validate the proposed listener certificates without loading application code."""
    from zhenxun.configs.webui_tls import _validate_certificate

    directory = store.path("uploads", upload["id"]).parent
    archive_path = contained_path(directory, "archive.zx.part", regular=True)
    entries = {entry["path"]: entry for entry in manifest["files"]}
    fingerprints = {}
    with tempfile.TemporaryDirectory(prefix="tls-preview-", dir=directory) as temporary:
        files = []
        for field in ("WEBUI_TLS_CERTFILE", "WEBUI_TLS_KEYFILE"):
            budget.checkpoint()
            path = Path(environment.get(field, "")).expanduser()
            if not path.is_absolute():
                path = contained_path(store.project, str(path))
            try:
                relative = path.relative_to(store.project).as_posix()
            except ValueError:
                relative = None
            if relative in entries:
                entry = entries[relative]
                if entry["size"] > 8 * 1024 * 1024:
                    raise ValueError("certificate_size")
                with open_archive(archive_path, password=password) as (archive, _):
                    content = archive.read(entry["payload"])
                if hashlib.sha256(content).hexdigest() != entry["sha256"]:
                    raise MigrationError(
                        "migration_payload_hash_mismatch", path=relative
                    )
            else:
                if path.stat().st_size > 8 * 1024 * 1024:
                    raise ValueError("certificate_size")
                content = path.read_bytes()
            destination = Path(temporary) / field
            destination.write_bytes(content)
            files.append(str(destination))
            fingerprints[field] = hashlib.sha256(content).hexdigest()
        _validate_certificate(*files)
    return fingerprints


def resolve_restore(
    project: Path,
    upload: dict,
    *,
    first_deployment: bool,
    private: dict,
    current: dict | None = None,
    budget: MigrationBudget | None = None,
) -> dict:
    """Resolve a read-only restore proposal without exposing its private inputs."""
    if not isinstance(private, dict) or set(private) - {
        "archive_password",
        "configuration",
        "administrator",
        "database",
        "source_path",
    }:
        raise MigrationError("migration_restore_options_invalid")
    store = TaskStore(project)
    budget = budget or MigrationBudget.start(3600)
    password = private.get("archive_password")
    if password is not None and (not isinstance(password, str) or len(password) > 4096):
        raise MigrationError("migration_password_invalid")
    manifest, documents = _package(
        store, upload, password.encode() if password else None, budget
    )
    legacy = None
    if manifest["source"].get("replacement_selection") is None:
        legacy = export_selection(
            frozenset(
                "data" if entry["category"] == "database" else entry["category"]
                for entry in manifest["files"]
            ),
            [entry["path"] for entry in manifest["files"]],
        ).public()
    selection = ReplacementSelection.from_manifest(manifest, legacy=legacy)
    origin = "package" if first_deployment else "current"
    if first_deployment:
        environment, issues = _environment(documents)
        administrator = _administrator_defaults(documents, issues)
    elif current is not None:
        environment = dict(current["configuration"])
        administrator = dict(current["administrator"])
        issues = []
    else:
        local = {}
        for name in _CONFIGURATION_FILES:
            path = contained_path(project, name)
            if path.is_file():
                local[name] = path.read_text(encoding="utf-8-sig")
        environment, issues = _environment(
            local, inherited={key.upper(): value for key, value in os.environ.items()}
        )
        administrator = _administrator_defaults(local, issues)
    overrides = confirmed_environment(private.get("configuration"))
    environment = {"HOST": "0.0.0.0", "PORT": "8080", **environment, **overrides}
    issues = [item for item in issues if item["field"] not in overrides]
    if "administrator" in private:
        supplied = private["administrator"]
        if (
            not isinstance(supplied, dict)
            or set(supplied) != {"username", "password"}
            or any(
                not isinstance(value, str) or not 0 < len(value) <= 4096
                for value in supplied.values()
            )
        ):
            raise MigrationError("migration_administrator_confirmation_required")
        # Explicit passwords are plaintext, including hash-shaped values.
        administrator = {
            "USERNAME": supplied["username"],
            "PASSWORD": _password_hash(
                store, upload, supplied["password"], explicit=True
            ),
        }
        issues = [item for item in issues if item["field"] != "administrator"]
    for field in ("USERNAME", "PASSWORD"):
        if not isinstance(administrator.get(field), str) or not administrator[field]:
            issues.append(
                {
                    "field": "administrator." + field.lower(),
                    "code": "migration_configuration_value_missing",
                }
            )
    if administrator.get("PASSWORD") and not is_password_hash(
        administrator["PASSWORD"]
    ):
        administrator["PASSWORD"] = _password_hash(
            store, upload, administrator["PASSWORD"]
        )
    network = {}
    listener_certificates = {}
    try:
        from zhenxun.configs.webui_tls import settings_from_values

        settings = settings_from_values(environment)
        network = {
            "host": settings.host,
            "port": settings.port,
            "https": settings.enabled,
        }
        # Check the address without touching the live listener.
        addresses = socket.getaddrinfo(settings.host, 0, type=socket.SOCK_STREAM)
        last_error = None
        for family, kind, protocol, _, address in addresses:
            try:
                with socket.socket(family, kind, protocol) as listener:
                    listener.bind(address)
                last_error = None
                break
            except OSError as error:
                last_error = error
        if last_error is not None:
            issues.append(
                {"field": "HOST", "code": "migration_listener_address_unavailable"}
            )
        else:
            current_listener = None
            if project.resolve() == Path.cwd().resolve():
                from zhenxun.configs.webui_tls import runtime_webui_settings
                from zhenxun.services.startup import startup_coordinator

                if startup_coordinator.snapshot().get("state") in {
                    "management_ready",
                    "warmup_ready",
                    "degraded",
                }:
                    current_listener = runtime_webui_settings()
            owned_port = (
                current_listener
                and current_listener.port == settings.port
                and (
                    current_listener.host == settings.host
                    or (current_listener.host == "0.0.0.0" and family == socket.AF_INET)
                    or (current_listener.host == "::" and family == socket.AF_INET6)
                )
            )
            if not owned_port:
                try:
                    with socket.socket(family, kind, protocol) as listener:
                        listener.bind((*address[:1], settings.port, *address[2:]))
                except OSError:
                    issues.append(
                        {"field": "PORT", "code": "migration_listener_port_unavailable"}
                    )
        if settings.enabled:
            available = {entry["path"] for entry in manifest["files"]}
            for field in ("WEBUI_TLS_CERTFILE", "WEBUI_TLS_KEYFILE"):
                relative = environment.get(field, "")
                try:
                    path = Path(relative).expanduser()
                    if not path.is_absolute():
                        path = contained_path(project, relative)
                    if not path.is_file() and relative not in available:
                        raise ValueError
                    environment[field] = str(path)
                except (ValueError, MigrationError):
                    issues.append(
                        {"field": field, "code": "migration_tls_file_unavailable"}
                    )
            if not any(item["field"].startswith("WEBUI_TLS_") for item in issues):
                try:
                    listener_certificates = _listener_certificates(
                        store,
                        upload,
                        manifest,
                        password.encode() if password else None,
                        environment,
                        budget,
                    )
                except (ValueError, OSError):
                    issues.append(
                        {
                            "field": "WEBUI_TLS_CERTFILE",
                            "code": "migration_tls_certificate_invalid",
                        }
                    )
    except (ValueError, OSError):
        issues.append(
            {"field": "network", "code": "migration_network_configuration_invalid"}
        )
    descriptions = manifest["source"].get("databases", [])
    primary = manifest["source"].get("primary_database") or {}
    source_path = private.get("source_path") or primary.get("path")
    if not source_path and len(descriptions) == 1:
        source_path = descriptions[0]["path"]
    if not source_path and descriptions:
        source_url, _ = _environment(documents)
        try:
            source_connection = DatabaseConnection.parse(
                source_url.get("DB_URL", ""), root=project
            )
            if source_connection.engine == "sqlite":
                source_file = sqlite_path_from_url(
                    source_url.get("DB_URL", ""), root=project
                )
                relative = source_file.relative_to(project).as_posix()
                if any(item["path"] == relative for item in descriptions):
                    source_path = relative
            else:
                matches = [
                    item
                    for item in descriptions
                    if item["engine"] == source_connection.engine
                ]
                if len(matches) == 1:
                    source_path = matches[0]["path"]
        except (ValueError, TypeError):
            pass
    chosen = next((item for item in descriptions if item["path"] == source_path), None)
    mapping, database, endpoint = None, {}, None
    database_overrides = private.get("database") or {}
    if not isinstance(database_overrides, dict) or set(database_overrides) - {
        "target_url",
        "candidate_url",
        "candidate_mode",
    }:
        raise MigrationError("migration_restore_options_invalid")
    if database_overrides.get("target_url"):
        environment["DB_URL"] = database_overrides["target_url"]
        issues = [item for item in issues if item["field"] != "DB_URL"]
    if descriptions and not chosen:
        issues.append(
            {"field": "source_path", "code": "migration_database_mapping_required"}
        )
    if chosen:
        if not environment.get("DB_URL") and chosen["engine"] == "sqlite":
            environment["DB_URL"] = "sqlite://" + chosen["path"]
        try:
            endpoint = DatabaseConnection.parse(
                environment.get("DB_URL", ""), root=project
            )
            if endpoint.engine != chosen["engine"]:
                raise MigrationError("migration_database_cross_engine_forbidden")
            mapping = {"engine": endpoint.engine, "source_path": chosen["path"]}
            if endpoint.engine == "sqlite":
                relative = Path(endpoint.database).relative_to(project).as_posix()
                contained_path(project, relative)
                mapping.update(target_path=relative, confirmed_name=Path(relative).name)
            else:
                mapping["confirmed_name"] = endpoint.database
                database = {"target_url": environment["DB_URL"], **database_overrides}
                database.setdefault(
                    "candidate_mode",
                    "manual" if database.get("candidate_url") else "auto",
                )
                if database["candidate_mode"] not in {"auto", "manual"}:
                    raise MigrationError("migration_candidate_mode_invalid")
                if database["candidate_mode"] == "manual" and not database.get(
                    "candidate_url"
                ):
                    issues.append(
                        {
                            "field": "database.candidate_url",
                            "code": "migration_database_credentials_required",
                        }
                    )
        except (ValueError, TypeError, MigrationError) as error:
            issues.append(
                {
                    "field": "DB_URL",
                    "code": getattr(
                        error, "code", "migration_database_connection_invalid"
                    ),
                }
            )
    resolved = {
        "configuration": confirmed_environment(environment),
        "archive_password": password,
        "administrator": {
            "username": administrator.get("USERNAME", ""),
            "password": administrator.get("PASSWORD", ""),
        },
    }
    if database:
        resolved["database"] = database
    options = {
        "first_deployment": first_deployment,
        "source_trusted": True,
        "database": mapping,
    }
    if legacy is not None:
        options["legacy_selection"] = legacy
    public_db = None
    if endpoint:
        public_db = {
            "engine": endpoint.engine,
            "host": endpoint.host,
            "port": endpoint.port,
            "database": mapping.get("target_path", endpoint.database)
            if mapping
            else endpoint.database,
            "username": endpoint.username,
            "tls": endpoint.tls,
            "candidate_mode": database.get("candidate_mode"),
        }
    summary = {
        "package_id": manifest["package_id"],
        "sha256": upload["sha256"],
        "files": len(manifest["files"]),
        "selection": selection.public(),
        "administrator": {
            "username": administrator.get("USERNAME", ""),
            "source": "override" if "administrator" in private else origin,
        },
        "network": {**network, "source": "override" if overrides else origin},
        "database": public_db,
        "databases": [
            {
                key: item[key]
                for key in ("id", "role", "engine", "path", "size")
                if key in item
            }
            for item in descriptions
        ],
        "configuration_source": origin,
    }
    from .candidate_database import connection_binding
    from .database_capabilities import CAPABILITY_POLICY_VERSION

    connections = {}
    if endpoint and endpoint.engine != "sqlite":
        for name, url in database.items():
            if name in {"target_url", "candidate_url"} and url:
                try:
                    connections[name] = connection_binding(url, project)
                except (OSError, ValueError, MigrationError):
                    issues.append(
                        {
                            "field": "database." + name,
                            "code": "migration_database_connection_invalid",
                        }
                    )
    revision = store.private_fingerprint(
        {
            "version": RESTORE_FLOW_VERSION,
            "archive": upload["sha256"],
            "policy_version": CAPABILITY_POLICY_VERSION,
            "connections": connections,
            "listener_certificates": listener_certificates,
            "options": options,
            "private": resolved,
        }
    )
    return {
        "options": options,
        "private": resolved,
        "summary": summary,
        "issues": issues,
        "ready": not issues,
        "revision": revision,
        "flow_version": RESTORE_FLOW_VERSION,
    }
