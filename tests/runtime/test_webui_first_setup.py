from __future__ import annotations

import importlib
from pathlib import Path

from dotenv import dotenv_values
from fastapi import HTTPException
import pytest
from ruamel.yaml import YAML


def test_scrypt_password_round_trip_and_legacy_compatibility() -> None:
    from zhenxun.builtin_plugins.web_ui.passwords import (
        hash_password,
        is_password_hash,
        verify_password,
    )

    encoded = hash_password("ValidPass1")

    assert is_password_hash(encoded)
    assert "ValidPass1" not in encoded
    assert verify_password("ValidPass1", encoded)
    assert not verify_password("WrongPass1", encoded)
    assert verify_password("legacy", "legacy")
    assert not verify_password("value", "scrypt$v1$broken")


@pytest.mark.asyncio
async def test_console_code_is_digest_only_reusable_and_rate_limited(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = importlib.import_module("zhenxun.builtin_plugins.web_ui.console_access")
    manager = module.ConsoleAccessManager()

    code = await manager.prepare()
    assert code
    assert code not in vars(manager).values()
    boot_id = await manager.claim(code, "192.168.1.10")
    assert await manager.claim(code, "192.168.1.10") == boot_id
    assert manager.accepts_boot_id(boot_id)

    for _ in range(5):
        with pytest.raises(HTTPException) as error:
            await manager.claim("x" * 43, "192.168.1.11")
        assert error.value.status_code == 401
    with pytest.raises(HTTPException) as error:
        await manager.claim(code, "192.168.1.11")
    assert error.value.status_code == 429

    await manager.reset_for_tests()
    replacement = await manager.prepare()
    assert replacement
    assert replacement != code
    assert not manager.accepts_boot_id(boot_id)


@pytest.mark.asyncio
async def test_setup_session_remains_repairable_until_apply_finishes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = importlib.import_module(
        "zhenxun.builtin_plugins.web_ui.api.configure.setup_access"
    )
    values = {"password": None}
    monkeypatch.setattr(
        module.Config,
        "get_config",
        lambda *args, **kwargs: values["password"],
    )
    monkeypatch.setattr(module.BotConfig, "db_url", "")
    manager = module.SetupAccessManager()
    token, _ = await manager.create_session("127.0.0.1")
    session = await manager.authorize(token, "127.0.0.1")

    values["password"] = "scrypt$v1$stored"
    monkeypatch.setattr(module.BotConfig, "db_url", "sqlite://data/db/test.db")

    assert manager.state() == "unconfigured"
    await manager.mark_applied(session)
    assert manager.state() == "restart_pending"


@pytest.mark.asyncio
async def test_setup_session_is_ip_bound(monkeypatch: pytest.MonkeyPatch) -> None:
    module = importlib.import_module(
        "zhenxun.builtin_plugins.web_ui.api.configure.setup_access"
    )
    manager = module.SetupAccessManager()
    monkeypatch.setattr(manager, "state", lambda: "unconfigured")

    token, expires_in = await manager.create_session("192.168.1.10")
    assert expires_in == 15 * 60
    await manager.authorize(token, "192.168.1.10")
    with pytest.raises(HTTPException) as error:
        await manager.authorize(token, "192.168.1.11")
    assert error.value.status_code == 401


@pytest.mark.asyncio
async def test_sqlite_probe_checks_existing_and_creatable_files(tmp_path: Path) -> None:
    from zhenxun.builtin_plugins.web_ui.api.configure.data_source import (
        probe_database,
    )
    from zhenxun.builtin_plugins.web_ui.api.configure.model import DatabaseConfig

    new_result = await probe_database(
        DatabaseConfig(mode="sqlite", path="data/new.db"), root=tmp_path
    )
    assert new_result.status == "ok", new_result
    assert new_result.facts["existing_file"] is False
    assert not (tmp_path / "data/new.db").exists()

    database_file = tmp_path / "existing.db"
    database_file.touch()
    existing_result = await probe_database(
        DatabaseConfig(mode="sqlite", path="existing.db"), root=tmp_path
    )
    assert existing_result.status == "ok"
    assert existing_result.facts["existing_file"] is True

    outside_result = await probe_database(
        DatabaseConfig(mode="sqlite", path=str(tmp_path.parent / "outside.db")),
        root=tmp_path,
    )
    assert outside_result.code == "sqlite_path_outside_project"


def test_structured_database_url_encodes_credentials(tmp_path: Path) -> None:
    from zhenxun.builtin_plugins.web_ui.api.configure.data_source import (
        build_database_url,
    )
    from zhenxun.builtin_plugins.web_ui.api.configure.model import DatabaseConfig

    url = build_database_url(
        DatabaseConfig(
            mode="postgres",
            host="db.local",
            port=5432,
            username="setup user",
            password="p@ss:/word",
            database="zhen xun",
        ),
        root=tmp_path,
    )

    assert url == ("postgres://setup%20user:p%40ss%3A%2Fword@db.local:5432/zhen%20xun")


def test_structured_database_url_brackets_ipv6_host(tmp_path: Path) -> None:
    from zhenxun.builtin_plugins.web_ui.api.configure.data_source import (
        build_database_url,
    )
    from zhenxun.builtin_plugins.web_ui.api.configure.model import DatabaseConfig

    url = build_database_url(
        DatabaseConfig(
            mode="postgres",
            host="2001:db8::10",
            port=5432,
            username="owner",
            password="secret",
            database="zhenxun",
        ),
        root=tmp_path,
    )

    assert url == "postgres://owner:secret@[2001:db8::10]:5432/zhenxun"


def test_configuration_env_update_preserves_multiline_values() -> None:
    from zhenxun.builtin_plugins.web_ui.api.tabs.system.configuration import (
        _update_env,
        _validate_env,
    )

    original = (
        "HOST = 127.0.0.1\n"
        "QQ_BOTS='\n"
        "[\n"
        '  {"id": "example", "token": "retained"}\n'
        "]\n"
        "'\n"
        "# retained comment\n"
    )

    updated = _update_env(original, {"HOST": "0.0.0.0", "PORT": 8080})
    _validate_env(updated)

    assert '  {"id": "example", "token": "retained"}' in updated
    assert "# retained comment" in updated
    assert 'HOST = "0.0.0.0"' in updated
    assert "PORT = 8080" in updated


def test_database_menu_name_is_migrated(tmp_path: Path, monkeypatch) -> None:
    from zhenxun.builtin_plugins.web_ui.api.menu import data_source

    menu_dir = tmp_path / "web_ui"
    menu_dir.mkdir()
    (menu_dir / "menu.json").write_text(
        '[{"name":"数据库管理","module":"database",'
        '"router":"/database","icon":"database","default":false}]',
        encoding="utf-8",
    )
    monkeypatch.setattr(data_source, "DATA_PATH", tmp_path)

    manager = data_source.MenuManager()
    database_menu = next(
        item for item in manager.get_menus().menus if item.module == "database"
    )

    assert database_menu.name == "数据与缓存"


@pytest.mark.asyncio
async def test_configuration_summary_is_json_serializable() -> None:
    from zhenxun.builtin_plugins.web_ui.api.tabs.system.configuration import (
        configuration_summary,
    )

    result = await configuration_summary()

    assert result.suc
    assert result.data
    assert result.model_dump_json()


def test_database_runtime_public_configuration_hides_passwords() -> None:
    from zhenxun.builtin_plugins.web_ui.api.configure.model import (
        CacheConfig,
        DatabaseConfig,
    )
    from zhenxun.builtin_plugins.web_ui.api.tabs.database.runtime import (
        _cache_public,
        _database_public,
    )

    database = _database_public(
        DatabaseConfig(
            mode="postgres",
            host="db.internal",
            username="owner",
            password="database-secret",
            database="zhenxun",
        )
    )
    cache = _cache_public(
        CacheConfig(mode="REDIS", host="redis.internal", password="redis-secret")
    )

    assert database["password"] == ""
    assert database["has_password"] is True
    assert "database-secret" not in str(database)
    assert cache["password"] == ""
    assert cache["has_password"] is True
    assert "redis-secret" not in str(cache)


def test_apply_is_atomic_and_preserves_unrelated_configuration(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from zhenxun.builtin_plugins.web_ui.api.configure import persistence
    from zhenxun.builtin_plugins.web_ui.api.configure.model import (
        ApplyRequest,
        CacheConfig,
        DatabaseConfig,
        NetworkConfig,
    )
    from zhenxun.builtin_plugins.web_ui.passwords import verify_password

    monkeypatch.chdir(tmp_path)
    (tmp_path / "data/configs").mkdir(parents=True)
    (tmp_path / ".env.dev").write_text(
        '# retained\nQQ_ADAPTER_LOAD = False\nDB_URL = ""\n', encoding="utf-8"
    )
    (tmp_path / "data/config.yaml").write_text(
        "other:\n  VALUE: retained\nweb-ui:\n  USERNAME: admin\n  PASSWORD:\n",
        encoding="utf-8",
    )
    (tmp_path / "data/configs/plugins2config.yaml").write_text(
        "web-ui:\n  USERNAME:\n    value: admin\n  PASSWORD:\n    value:\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(persistence.Config, "set_config", lambda *args: None)

    persistence.apply_configuration(
        ApplyRequest(
            username="owner",
            password="ValidPass1",
            confirm_password="ValidPass1",
            superusers=["10001", "10001", " 10002 "],
            database=DatabaseConfig(mode="sqlite", path="data/db/zhenxun.db"),
            cache=CacheConfig(mode="MEMORY"),
            network=NetworkConfig(mode="lan", port=8080),
        )
    )

    environment = dotenv_values(tmp_path / ".env.dev")
    assert environment["QQ_ADAPTER_LOAD"] == "False"
    assert environment["DB_URL"] == "sqlite://data/db/zhenxun.db"
    assert environment["HOST"] == "0.0.0.0"
    simple = YAML().load((tmp_path / "data/config.yaml").read_text("utf-8"))
    assert simple["other"]["VALUE"] == "retained"
    stored = simple["web-ui"]["PASSWORD"]
    assert stored != "ValidPass1"
    assert verify_password("ValidPass1", stored)


def test_password_persistence_rotates_secret_on_both_config_surfaces(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from zhenxun.builtin_plugins.web_ui.api.configure import persistence

    monkeypatch.chdir(tmp_path)
    (tmp_path / "data/configs").mkdir(parents=True)
    (tmp_path / "data/config.yaml").write_text(
        "web-ui:\n  USERNAME: admin\n  PASSWORD: old\n  SECRET: old-secret\n",
        encoding="utf-8",
    )
    (tmp_path / "data/configs/plugins2config.yaml").write_text(
        "web-ui:\n"
        "  USERNAME:\n    value: admin\n"
        "  PASSWORD:\n    value: old\n"
        "  SECRET:\n    value: old-secret\n",
        encoding="utf-8",
    )
    in_memory: dict[str, str] = {}
    monkeypatch.setattr(
        persistence.Config,
        "set_config",
        lambda _module, key, value: in_memory.__setitem__(key, value),
    )

    persistence.persist_webui_credentials(
        "owner", "new-password-hash", "new-jwt-secret"
    )

    simple = YAML().load((tmp_path / "data/config.yaml").read_text("utf-8"))
    plugins = YAML().load(
        (tmp_path / "data/configs/plugins2config.yaml").read_text("utf-8")
    )
    assert simple["web-ui"]["SECRET"] == "new-jwt-secret"
    assert plugins["web-ui"]["SECRET"]["value"] == "new-jwt-secret"
    assert in_memory["secret"] == "new-jwt-secret"


def test_first_configuration_writes_reloadable_plugin_credentials(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from zhenxun.builtin_plugins.web_ui.api.configure import persistence
    from zhenxun.builtin_plugins.web_ui.api.configure.model import (
        ApplyRequest,
        CacheConfig,
        DatabaseConfig,
        NetworkConfig,
    )
    from zhenxun.configs.utils import ConfigsManager

    monkeypatch.chdir(tmp_path)
    (tmp_path / "data/configs").mkdir(parents=True)
    (tmp_path / ".env.example").write_text('DB_URL = ""\n', encoding="utf-8")
    monkeypatch.setattr(persistence.Config, "set_config", lambda *args: None)

    setting = ApplyRequest(
        username="owner",
        password="ValidPass1",
        confirm_password="ValidPass1",
        superusers=[],
        database=DatabaseConfig(mode="sqlite", path="data/db/zhenxun.db"),
        cache=CacheConfig(mode="MEMORY"),
        network=NetworkConfig(mode="local", port=8080),
    )
    persistence.apply_configuration(setting)
    persistence.apply_configuration(setting)

    config_file = tmp_path / "data/configs/plugins2config.yaml"
    reloaded = ConfigsManager(config_file)
    assert reloaded.get_config("web-ui", "username") == "owner"
    assert reloaded.get_data()["web-ui"].configs["PASSWORD"].help is not None
    assert reloaded.get_data()["web-ui"].configs["SECRET"].help is not None


def test_legacy_value_only_credentials_remain_loadable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import zhenxun.configs.utils as config_utils
    from zhenxun.configs.utils import ConfigsManager

    monkeypatch.setattr(config_utils, "DATA_PATH", tmp_path / "data")
    config_file = tmp_path / "plugins2config.yaml"
    config_file.write_text(
        "web-ui:\n"
        "  USERNAME:\n"
        "    value: owner\n"
        "  PASSWORD:\n"
        "    value: legacy-hash\n"
        "  SECRET: {}\n",
        encoding="utf-8",
    )

    reloaded = ConfigsManager(config_file)
    assert reloaded.get_config("web-ui", "username") == "owner"
    assert reloaded.get_data()["web-ui"].configs["USERNAME"].help is None
    assert reloaded.get_data()["web-ui"].configs["SECRET"].value is None


def test_missing_webui_secret_is_repaired_once(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from zhenxun.builtin_plugins.web_ui.api.configure import persistence

    monkeypatch.chdir(tmp_path)
    (tmp_path / "data/configs").mkdir(parents=True)
    (tmp_path / "data/config.yaml").write_text(
        "web-ui:\n  USERNAME: owner\n  PASSWORD: stored-hash\n",
        encoding="utf-8",
    )
    (tmp_path / "data/configs/plugins2config.yaml").write_text(
        "web-ui:\n"
        "  USERNAME:\n    value: owner\n"
        "  PASSWORD:\n    value: stored-hash\n"
        "  SECRET: {}\n",
        encoding="utf-8",
    )
    values = {
        "username": "owner",
        "password": "stored-hash",
        "secret": None,
    }
    monkeypatch.setattr(
        persistence.Config,
        "get_config",
        lambda _module, key, default=None: values.get(key, default),
    )
    monkeypatch.setattr(
        persistence.Config,
        "set_config",
        lambda _module, key, value: values.__setitem__(key, value),
    )

    assert persistence.ensure_webui_secret()
    generated = values["secret"]
    assert generated
    assert not persistence.ensure_webui_secret()

    simple = YAML().load((tmp_path / "data/config.yaml").read_text("utf-8"))
    plugins = YAML().load(
        (tmp_path / "data/configs/plugins2config.yaml").read_text("utf-8")
    )
    assert simple["web-ui"]["SECRET"] == generated
    assert plugins["web-ui"]["SECRET"]["value"] == generated
    assert plugins["web-ui"]["SECRET"]["help"] == "JWT密钥"
