from __future__ import annotations

from io import StringIO
import json
import os
from pathlib import Path
import re
import tempfile
from typing import Any

from ruamel.yaml import YAML

from zhenxun.configs.config import Config

from ...passwords import hash_password
from .data_source import build_database_url, resolve_network_host
from .model import ApplyRequest

_SIMPLE_CONFIG = Path("data/config.yaml")
_PLUGIN_CONFIG = Path("data/configs/plugins2config.yaml")
_ENV_CONFIG = Path(".env.dev")
_ENV_TEMPLATE = Path(".env.example")


def _set_env_value(env_text: str, key: str, value: str | int) -> str:
    replacement = f"{key} = {value}"
    pattern = rf"(?m)^\s*#?\s*{re.escape(key)}\s*=.*$"
    if re.search(pattern, env_text):
        return re.sub(pattern, lambda _: replacement, env_text, count=1)
    return f"{env_text.rstrip()}\n{replacement}\n"


def _quote_env(value: str) -> str:
    return json.dumps(value, ensure_ascii=False)


def _yaml() -> YAML:
    parser = YAML()
    parser.preserve_quotes = True
    parser.indent(mapping=2, sequence=4, offset=2)
    return parser


def _load_yaml(path: Path) -> Any:
    if not path.exists():
        return {}
    parser = _yaml()
    with path.open(encoding="utf-8") as stream:
        return parser.load(stream) or {}


def _dump_yaml(data: Any) -> bytes:
    stream = StringIO()
    _yaml().dump(data, stream)
    return stream.getvalue().encode("utf-8")


def _credential_documents(
    username: str,
    password_hash: str,
    secret: str | None = None,
) -> tuple[bytes, bytes]:
    simple = _load_yaml(_SIMPLE_CONFIG)
    simple_group = simple.setdefault("web-ui", {})
    simple_group["USERNAME"] = username
    simple_group["PASSWORD"] = password_hash
    if secret is not None:
        simple_group["SECRET"] = secret

    plugins = _load_yaml(_PLUGIN_CONFIG)
    plugin_group = plugins.setdefault("web-ui", {})
    username_entry = plugin_group.setdefault("USERNAME", {})
    password_entry = plugin_group.setdefault("PASSWORD", {})
    secret_entry = plugin_group.setdefault("SECRET", {})
    username_entry["value"] = username
    password_entry["value"] = password_hash
    if secret is not None:
        secret_entry["value"] = secret
    return _dump_yaml(simple), _dump_yaml(plugins)


def _write_transaction(files: list[tuple[Path, bytes]]) -> None:
    originals = {
        path: path.read_bytes() if path.exists() else None for path, _ in files
    }
    staged: dict[Path, Path] = {}
    replaced: list[Path] = []
    try:
        for path, content in files:
            path.parent.mkdir(parents=True, exist_ok=True)
            handle, temporary_name = tempfile.mkstemp(
                prefix=f".{path.name}.", suffix=".tmp", dir=path.parent
            )
            temporary = Path(temporary_name)
            with os.fdopen(handle, "wb") as stream:
                stream.write(content)
                stream.flush()
                os.fsync(stream.fileno())
            staged[path] = temporary
        for path, _ in files:
            os.replace(staged[path], path)
            replaced.append(path)
    except Exception:
        for path in reversed(replaced):
            original = originals[path]
            if original is None:
                path.unlink(missing_ok=True)
                continue
            handle, rollback_name = tempfile.mkstemp(
                prefix=f".{path.name}.rollback.", suffix=".tmp", dir=path.parent
            )
            with os.fdopen(handle, "wb") as stream:
                stream.write(original)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(rollback_name, path)
        raise
    finally:
        for temporary in staged.values():
            temporary.unlink(missing_ok=True)


def persist_webui_credentials(
    username: str,
    password_hash: str,
    secret: str | None = None,
) -> None:
    simple_bytes, plugin_bytes = _credential_documents(
        username,
        password_hash,
        secret,
    )
    _write_transaction([(_PLUGIN_CONFIG, plugin_bytes), (_SIMPLE_CONFIG, simple_bytes)])
    Config.set_config("web-ui", "username", username)
    Config.set_config("web-ui", "password", password_hash)
    if secret is not None:
        Config.set_config("web-ui", "secret", secret)


def apply_configuration(setting: ApplyRequest) -> dict[str, Any]:
    password_hash = hash_password(setting.password)
    simple_bytes, plugin_bytes = _credential_documents(
        setting.username.strip(), password_hash
    )
    source = _ENV_CONFIG if _ENV_CONFIG.exists() else _ENV_TEMPLATE
    if not source.exists():
        raise FileNotFoundError("env_template_missing")
    env_text = source.read_text(encoding="utf-8")
    database_url = build_database_url(setting.database)
    host = resolve_network_host(setting.network)
    superusers = list(
        dict.fromkeys(value.strip() for value in setting.superusers if value.strip())
    )

    env_text = _set_env_value(env_text, "DB_URL", _quote_env(database_url))
    env_text = _set_env_value(
        env_text, "SUPERUSERS", json.dumps(superusers, ensure_ascii=False)
    )
    env_text = _set_env_value(env_text, "HOST", host)
    env_text = _set_env_value(env_text, "PORT", setting.network.port)
    env_text = _set_env_value(env_text, "CACHE_MODE", setting.cache.mode)
    if setting.cache.mode == "REDIS":
        env_text = _set_env_value(
            env_text, "REDIS_HOST", _quote_env(setting.cache.host.strip())
        )
        env_text = _set_env_value(env_text, "REDIS_PORT", setting.cache.port)
        env_text = _set_env_value(
            env_text, "REDIS_PASSWORD", _quote_env(setting.cache.password)
        )

    # config.yaml is the commit point and is replaced last.
    _write_transaction(
        [
            (_PLUGIN_CONFIG, plugin_bytes),
            (_ENV_CONFIG, env_text.encode("utf-8")),
            (_SIMPLE_CONFIG, simple_bytes),
        ]
    )
    Config.set_config("web-ui", "username", setting.username.strip())
    Config.set_config("web-ui", "password", password_hash)
    return {
        "host": host,
        "port": setting.network.port,
        "database_type": setting.database.mode,
        "cache_mode": setting.cache.mode,
    }


__all__ = ["apply_configuration", "persist_webui_credentials"]
