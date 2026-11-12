from __future__ import annotations

import json
import os
from pathlib import Path
import secrets
import tempfile
from typing import Any

from zhenxun.configs.config import Config
from zhenxun.configs.environment import environment_file, environment_target
from zhenxun.utils.webui_session import (
    PLUGIN_CONFIG,
    SIMPLE_CONFIG,
    credential_documents,
)

from ...passwords import hash_password
from .data_source import build_database_url, resolve_network_host
from .model import ApplyRequest

_ENV_CONFIG = Path(".env.dev")
_ENV_TEMPLATE = Path(".env.example")


def _set_env_value(env_text: str, key: str, value: str | int) -> str:
    from ..tabs.system.configuration import _update_env

    if isinstance(value, str):
        try:
            value = json.loads(value)
        except ValueError:
            pass
    return _update_env(env_text, {key: value})


def _quote_env(value: str) -> str:
    return json.dumps(value, ensure_ascii=False)


def _credential_documents(
    username: str,
    password_hash: str,
    secret: str | None = None,
    *,
    root: Path | None = None,
) -> tuple[bytes, bytes]:
    group = Config.get_data().get("web-ui")
    templates = {
        key: registered.to_dict(exclude={"type", "arg_parser"})
        for key in ("USERNAME", "PASSWORD", "SECRET")
        if group and (registered := group.configs.get(key))
    }
    return credential_documents(
        username, password_hash, secret, root=root, templates=templates
    )


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
    _write_transaction([(PLUGIN_CONFIG, plugin_bytes), (SIMPLE_CONFIG, simple_bytes)])
    Config.set_config("web-ui", "username", username)
    Config.set_config("web-ui", "password", password_hash)
    if secret is not None:
        Config.set_config("web-ui", "secret", secret)


def ensure_webui_secret() -> bool:
    username = str(Config.get_config("web-ui", "username", "") or "").strip()
    password_hash = str(Config.get_config("web-ui", "password", "") or "")
    if not username or not password_hash:
        return False
    if Config.get_config("web-ui", "secret"):
        return False
    persist_webui_credentials(
        username,
        password_hash,
        secrets.token_urlsafe(32),
    )
    return True


def apply_configuration(setting: ApplyRequest) -> dict[str, Any]:
    password_hash = hash_password(setting.password)
    simple_bytes, plugin_bytes = _credential_documents(
        setting.username.strip(), password_hash
    )
    source = environment_file(
        template=True, preferred=_ENV_CONFIG, template_path=_ENV_TEMPLATE
    )
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
    self_nickname = setting.self_nickname.strip()
    nicknames = list(
        dict.fromkeys(
            name.strip() for name in [self_nickname, *setting.nicknames] if name.strip()
        )
    )
    if self_nickname:
        env_text = _set_env_value(env_text, "SELF_NICKNAME", _quote_env(self_nickname))
    if nicknames:
        env_text = _set_env_value(
            env_text, "NICKNAME", json.dumps(nicknames, ensure_ascii=False)
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
            (PLUGIN_CONFIG, plugin_bytes),
            (environment_target(source), env_text.encode("utf-8")),
            (SIMPLE_CONFIG, simple_bytes),
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


__all__ = [
    "apply_configuration",
    "ensure_webui_secret",
    "persist_webui_credentials",
]
