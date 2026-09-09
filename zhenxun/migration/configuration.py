from __future__ import annotations

from contextlib import contextmanager
from copy import deepcopy
import hashlib
import hmac
from io import StringIO
import json
import os

from dotenv.parser import parse_stream
from ruamel.yaml import YAML
from ruamel.yaml.events import AliasEvent, CollectionEndEvent, CollectionStartEvent

from .errors import MigrationError

_CONFIRMED_ENV = frozenset(
    {
        "DB_URL",
        "HOST",
        "PORT",
        "WEBUI_HTTPS_ENABLED",
        "WEBUI_TLS_CERTFILE",
        "WEBUI_TLS_KEYFILE",
        "WEBUI_HTTP_MODE",
        "WEBUI_HTTP_REDIRECT_ENABLED",
        "WEBUI_HTTP_REDIRECT_PORT",
    }
)


def confirmed_environment(value: dict | None) -> dict[str, str]:
    if value is None:
        return {}
    if not isinstance(value, dict) or set(value) - _CONFIRMED_ENV:
        raise MigrationError("migration_configuration_override_invalid")
    result = {}
    for key, item in value.items():
        if (
            not isinstance(item, str)
            or len(item) > 4096
            or any(c in item for c in "\r\n\x00")
            or "${" in item
        ):
            raise MigrationError("migration_configuration_literal_required")
        result[key] = item
    return result


def bound_environment(options: dict, value: dict | None) -> dict[str, str]:
    values = confirmed_environment(value)
    expected = options.get("configuration_sha256")
    if not values and expected is None:
        return values
    observed = hashlib.sha256(json.dumps(values, sort_keys=True).encode()).hexdigest()
    if not isinstance(expected, str) or not hmac.compare_digest(expected, observed):
        raise MigrationError("migration_configuration_confirmation_changed", status=409)
    return values


def publish_process_environment(store, identity, values, *, lease) -> None:
    from zhenxun.utils.atomic_json import read_json_locked

    from .commit import read_commit

    lease.require_held()
    if lease.project.absolute() != store.project:
        raise MigrationError("migration_commit_authorization_invalid")
    overrides = bound_environment(store.read("jobs", identity)["options"], values)
    decision = read_commit(store, identity)
    publication = read_json_locked(
        store.path("jobs", identity).parent / "restore-publication.json", None
    )
    if (
        decision is None
        or not isinstance(publication, dict)
        or publication.get("job_id") != identity
        or publication.get("decided_at") != decision["decided_at"]
        or publication.get("state") != "published"
    ):
        raise MigrationError("migration_publication_receipt_invalid")
    # Future children must not inherit the launcher's old DB/host overrides.
    # This changes only this process; it does not edit system environment values.
    for key in list(os.environ):
        if key.upper() in overrides:
            del os.environ[key]
    os.environ.update(overrides)


def committed_environment(store, identity) -> dict:
    """Read only explicitly bound settings from the applied target env file."""
    from .paths import contained_path
    from .restore import _configuration_read

    options = store.read("jobs", identity)["options"]
    keys = options.get("configuration_keys")
    if not isinstance(keys, list) or not set(keys) <= _CONFIRMED_ENV:
        raise MigrationError("migration_configuration_reauthorization_required")
    path = contained_path(store.project, ".env.dev", regular=True)
    bindings = _env_bindings(_configuration_read(path).decode("utf-8-sig"))
    values = {}
    for key in keys:
        matches = [b.value for name, b in bindings.items() if name.upper() == key]
        if len(matches) != 1:
            raise MigrationError("migration_configuration_confirmation_changed")
        values[key] = matches[0]
    return bound_environment(options, values)


def override_env(text: str, values: dict[str, str]) -> str:
    values = confirmed_environment(values)
    if not values:
        return text
    _env_bindings(text)
    content = [
        binding.original.string
        for binding in parse_stream(StringIO(text))
        if binding.key is None or binding.key.upper() not in values
    ]
    return (
        "".join(content).rstrip("\r\n")
        + "\n"
        + "".join(
            f"{key}={json.dumps(value, ensure_ascii=False)}\n"
            for key, value in sorted(values.items())
        )
    )


@contextmanager
def confirmed_process_environment(values):
    values = confirmed_environment(values)
    previous = {
        key: value for key, value in os.environ.items() if key.upper() in values
    }
    for key in previous:
        del os.environ[key]
    os.environ.update(values)
    try:
        yield
    finally:
        for key in list(os.environ):
            if key.upper() in values:
                del os.environ[key]
        os.environ.update(previous)


def merge_missing(target: dict, backup: dict) -> dict:
    result = deepcopy(target)
    for key, value in backup.items():
        if key not in result:
            result[key] = deepcopy(value)
        elif isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = merge_missing(result[key], value)
    return result


def _env_bindings(text: str) -> dict:
    result = {}
    for binding in parse_stream(StringIO(text)):
        if binding.error:
            raise MigrationError("migration_env_parse_failed")
        if binding.key is not None:
            if binding.key in result:
                raise MigrationError("migration_env_duplicate_key")
            result[binding.key] = binding
    return result


def merge_env(
    target: str, backup: str, *, preserve: frozenset[str] = frozenset()
) -> str:
    existing = _env_bindings(target)
    incoming = _env_bindings(backup)
    reserved = {key.casefold() for key in preserve}
    additions = [
        binding.original.string
        for key, binding in incoming.items()
        if key not in existing and key.casefold() not in reserved
    ]
    if not additions:
        return target
    return (
        target.rstrip("\r\n")
        + "\n"
        + "\n".join(s.rstrip("\r\n") for s in additions)
        + "\n"
    )


def replace_env(backup: str, target: str, *, preserve: frozenset[str]) -> str:
    existing = _env_bindings(target)
    _env_bindings(backup)
    reserved = {key.casefold() for key in preserve}
    preserved = {}
    for key, binding in existing.items():
        if key.casefold() in reserved:
            if key.casefold() in preserved:
                raise MigrationError("migration_env_ambiguous_key")
            preserved[key.casefold()] = binding
    output = []
    for binding in parse_stream(StringIO(backup)):
        if binding.key is None or binding.key.casefold() not in reserved:
            output.append(binding.original.string)
    for key in sorted(preserved):
        output.append("\n" + preserved[key].original.string)
    return "".join(output)


def yaml_document(text: str) -> dict:
    yaml = YAML(typ="safe")
    yaml.allow_duplicate_keys = False
    try:
        depth = 0
        for count, event in enumerate(yaml.parse(text)):
            if isinstance(event, AliasEvent):
                raise MigrationError("migration_yaml_alias_review_required")
            if isinstance(event, CollectionStartEvent):
                depth += 1
            if isinstance(event, CollectionEndEvent):
                depth -= 1
            if depth > 64 or count > 100_000:
                raise MigrationError("migration_yaml_structure_limit")
        result = yaml.load(text)
        if not isinstance(result, dict):
            raise MigrationError("migration_yaml_mapping_required")
        return result
    except MigrationError:
        raise
    except Exception:
        raise MigrationError("migration_yaml_parse_failed") from None


def dump_yaml(value: dict) -> str:
    output = StringIO()
    YAML(typ="safe").dump(value, output)
    return output.getvalue()


def override_administrator(
    path: str, content: bytes, administrator: dict | None
) -> bytes:
    if not administrator or path not in {
        "data/config.yaml",
        "data/configs/plugins2config.yaml",
    }:
        return content
    value = yaml_document(content.decode("utf-8-sig"))
    group = value.setdefault("web-ui", {})
    if not isinstance(group, dict):
        raise MigrationError("migration_configuration_shape_invalid")
    for key, item in administrator.items():
        if key not in {"USERNAME", "PASSWORD"}:
            raise MigrationError("migration_administrator_confirmation_required")
        if path == "data/configs/plugins2config.yaml":
            entry = group.setdefault(key, {})
            if not isinstance(entry, dict):
                raise MigrationError("migration_configuration_shape_invalid")
            entry["value"] = item
        else:
            group[key] = item
    return dump_yaml(value).encode("utf-8")


def preflight_administrator(store, identity: str, private: dict) -> dict | None:
    from zhenxun.utils.passwords import verify_password

    from .archive import file_hash
    from .paths import contained_path
    from .restore_phases import _read

    record = store.read("preflights", identity)
    expected = record["options"].get("administrator_sha256")
    if not expected:
        return None
    path = contained_path(
        store.path("preflights", identity).parent, "administrator.json", regular=True
    )
    if file_hash(path) != expected:
        raise MigrationError("migration_administrator_confirmation_changed", status=409)
    selected = _read(path)
    supplied = private.get("administrator") or {}
    if (
        supplied.get("username") != selected.get("USERNAME")
        or not isinstance(supplied.get("password"), str)
        or not verify_password(supplied["password"], selected["PASSWORD"])
    ):
        raise MigrationError("migration_administrator_confirmation_changed", status=409)
    return selected


def merge_yaml(target: str, backup: str) -> str:
    return dump_yaml(merge_missing(yaml_document(target), yaml_document(backup)))


def configuration_yaml(
    path: str, backup: str, target: str | None, *, missing: bool
) -> str:
    incoming = yaml_document(backup)
    existing = yaml_document(target) if target is not None else {}
    result = merge_missing(existing, incoming) if missing else incoming
    if path.casefold() in {"data/config.yaml", "data/configs/plugins2config.yaml"}:
        groups = [
            key for key in result if isinstance(key, str) and key.casefold() == "web-ui"
        ]
        old_groups = [
            key
            for key in existing
            if isinstance(key, str) and key.casefold() == "web-ui"
        ]
        if len(groups) > 1 or len(old_groups) > 1:
            raise MigrationError("migration_configuration_ambiguous_key")
        for key in groups:
            group = result[key]
            old = existing[old_groups[0]] if old_groups else {}
            if not isinstance(group, dict) or not isinstance(old, dict):
                raise MigrationError("migration_configuration_shape_invalid")
            for field in list(group):
                if isinstance(field, str) and field.casefold() == "secret":
                    del group[field]
            # Only the target's current management session is retained during
            # candidate preparation. The commit service must rotate it later.
            secret_keys = [
                field
                for field in old
                if isinstance(field, str) and field.casefold() == "secret"
            ]
            if len(secret_keys) > 1:
                raise MigrationError("migration_configuration_ambiguous_key")
            for field in secret_keys:
                group[field] = deepcopy(old[field])
    return dump_yaml(result)
