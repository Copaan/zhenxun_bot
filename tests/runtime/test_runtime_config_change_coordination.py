from pathlib import Path

import pytest
from ruamel.yaml.comments import CommentedMap


def test_config_keys_are_deduplicated_for_runtime_fingerprint(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from zhenxun.builtin_plugins.init import init_config

    monkeypatch.setattr(
        init_config.Config,
        "add_module",
        ["alapi:alapi_token", "shared:value"],
    )

    assert init_config._normalize_config_keys(
        ["alapi:alapi_token", "alapi:alapi_token"]
    ) == ["alapi:alapi_token", "shared:value"]


def test_config_dependency_diff_ignores_format_and_tracks_top_level_keys() -> None:
    from zhenxun.services.runtime_config_reload import changed_config_dependencies

    before = CommentedMap(
        {
            "alapi": CommentedMap({"ALAPI_TOKEN": None}),
            "AI": CommentedMap({"PROVIDERS": [{"name": "old"}]}),
        }
    )
    after = CommentedMap(
        {
            "AI": CommentedMap({"PROVIDERS": [{"name": "new"}]}),
            "alapi": CommentedMap({"ALAPI_TOKEN": None}),
        }
    )

    assert changed_config_dependencies(before, after) == {("AI", "PROVIDERS")}
    assert changed_config_dependencies(after, CommentedMap(after)) == set()


@pytest.mark.asyncio
async def test_alapi_runtime_value_reload_does_not_request_restart(
    app, monkeypatch: pytest.MonkeyPatch
) -> None:
    del app
    from zhenxun.builtin_plugins.hooks.auth.auth_limit import LimitManager
    from zhenxun.builtin_plugins.init.manager import manager as init_manager
    from zhenxun.configs.config import Config
    from zhenxun.services.plugin_host import ApplyMode, plugin_host
    import zhenxun.services.runtime_config_reload as runtime_config

    monkeypatch.setattr(Config, "_simple_data", {"alapi": {"ALAPI_TOKEN": "old-token"}})

    def reload_config(*, strict: bool = False) -> None:
        assert strict is True
        Config._simple_data = {"alapi": {"ALAPI_TOKEN": "new-token"}}

    async def load_to_db() -> None:
        return None

    async def update_limits() -> None:
        return None

    captured: list[tuple[set[tuple[str, str]], bool]] = []

    async def reload_consumers(
        changed: set[tuple[str, str]],
        *,
        restart_dependencies: set[tuple[str, str]] | None = None,
        submit_restart: bool = True,
    ) -> None:
        captured.append((changed, submit_restart))
        return None

    monkeypatch.setattr(Config, "reload", reload_config)
    monkeypatch.setattr(runtime_config.get_llm_config, "cache_clear", lambda: None)
    monkeypatch.setattr(runtime_config, "clear_all_cache", lambda: None)
    monkeypatch.setattr(init_manager, "init", lambda: None)
    monkeypatch.setattr(init_manager, "load_to_db", load_to_db)
    monkeypatch.setattr(LimitManager, "update_limits", update_limits)
    monkeypatch.setattr(plugin_host, "reload_config_consumers", reload_consumers)

    operation = await runtime_config.reload_runtime_config(submit_restart=False)

    assert operation.mode is ApplyMode.CONFIG_RELOADED
    assert operation.config_keys == ["alapi.ALAPI_TOKEN"]
    assert captured == [({("alapi", "ALAPI_TOKEN")}, False)]


def test_strict_config_reload_rejects_invalid_registered_value_without_mutation(
    tmp_path: Path,
) -> None:
    from zhenxun.configs.utils import ConfigsManager, SimpleConfigValidationError

    config = ConfigsManager(tmp_path / "plugins2config.yaml")
    config._simple_file = tmp_path / "config.yaml"
    config.add_plugin_config(
        "demo",
        "COUNT",
        1,
        default_value=1,
        type=int,
    )
    config._simple_data = {"demo": {"COUNT": 1}}
    config._data["demo"].configs["COUNT"].value = 1
    config._simple_file.write_text("demo:\n  COUNT: invalid\n", encoding="utf-8")

    with pytest.raises(SimpleConfigValidationError) as exc_info:
        config.reload(strict=True)

    assert exc_info.value.path == "demo.COUNT"
    assert exc_info.value.line == 2
    assert config._simple_data == {"demo": {"COUNT": 1}}
    assert config.get_config("demo", "COUNT") == 1


@pytest.mark.asyncio
async def test_runtime_config_consumer_failure_restores_memory_and_caches(
    app, monkeypatch: pytest.MonkeyPatch
) -> None:
    del app
    from zhenxun.builtin_plugins.hooks.auth.auth_limit import LimitManager
    from zhenxun.builtin_plugins.init.manager import manager as init_manager
    from zhenxun.configs.config import Config
    from zhenxun.services.plugin_host import ApplyMode, RuntimeOperation, plugin_host
    import zhenxun.services.runtime_config_reload as runtime_config

    old_data = {"demo": {"VALUE": "old"}}
    new_data = {"demo": {"VALUE": "new"}}
    monkeypatch.setattr(Config, "_simple_data", old_data.copy())
    refresh_count = 0

    def snapshot_runtime_values():
        return old_data.copy(), {}

    def reload_config(*, strict: bool = False) -> None:
        assert strict is True
        Config._simple_data = new_data.copy()

    def restore_runtime_values(snapshot) -> None:
        Config._simple_data = snapshot[0].copy()

    async def load_to_db() -> None:
        nonlocal refresh_count
        refresh_count += 1

    async def update_limits() -> None:
        return None

    async def reload_consumers(*_args, **_kwargs) -> RuntimeOperation:
        return RuntimeOperation(
            ApplyMode.FAILED,
            "failed",
            ["demo.consumer"],
            "consumer_startup_failed",
        )

    monkeypatch.setattr(Config, "snapshot_runtime_values", snapshot_runtime_values)
    monkeypatch.setattr(Config, "reload", reload_config)
    monkeypatch.setattr(Config, "restore_runtime_values", restore_runtime_values)
    monkeypatch.setattr(Config, "save", lambda: None)
    monkeypatch.setattr(runtime_config.get_llm_config, "cache_clear", lambda: None)
    monkeypatch.setattr(runtime_config, "clear_all_cache", lambda: None)
    monkeypatch.setattr(init_manager, "init", lambda: None)
    monkeypatch.setattr(init_manager, "load_to_db", load_to_db)
    monkeypatch.setattr(LimitManager, "update_limits", update_limits)
    monkeypatch.setattr(plugin_host, "reload_config_consumers", reload_consumers)

    with pytest.raises(runtime_config.RuntimeConfigReloadError):
        await runtime_config.reload_runtime_config(submit_restart=False)

    assert Config._simple_data == old_data
    assert refresh_count == 2


def test_unchanged_simple_config_is_not_rewritten(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from zhenxun.builtin_plugins.init import init_config

    config_path = tmp_path / "config.yaml"
    monkeypatch.setattr(init_config, "SIMPLE_CONFIG_FILE", config_path)
    data = CommentedMap({"alapi": {"ALAPI_TOKEN": None}})

    assert init_config._write_simple_config_if_changed(data) is True
    first_content = config_path.read_bytes()
    first_mtime = config_path.stat().st_mtime_ns

    assert init_config._write_simple_config_if_changed(data) is False
    assert config_path.read_bytes() == first_content
    assert config_path.stat().st_mtime_ns == first_mtime


@pytest.mark.asyncio
@pytest.mark.usefixtures("bound_operation_registry")
async def test_webui_config_save_waits_for_explicit_restart(
    app, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    del app
    import zhenxun.builtin_plugins.web_ui.api.tabs.system.configuration as module
    from zhenxun.services.plugin_host import ApplyMode, RuntimeOperation
    from zhenxun.utils import _restart_utils, restart_state

    config_path = tmp_path / "config.yaml"
    config_path.write_text("demo:\n  VALUE: 1\n", encoding="utf-8")
    state_path = tmp_path / "restart.json"

    async def reload_config(*, submit_restart: bool = True) -> RuntimeOperation:
        assert submit_restart is False
        return RuntimeOperation(
            ApplyMode.RESTART_PENDING,
            "pending_restart",
            ["unsafe"],
            "import_time_config_consumer_requires_restart",
            config_keys=["demo.VALUE"],
        )

    monkeypatch.setattr(module, "_SIMPLE_FILE", config_path)
    monkeypatch.setattr(module, "reload_runtime_config", reload_config)
    monkeypatch.setattr(restart_state, "_RESTART_STATE_FILE", state_path)
    monkeypatch.setattr(_restart_utils, "_restart_pending", False)
    monkeypatch.setenv("ZHENXUN_LAUNCHER_PID", "123")

    current = config_path.read_text(encoding="utf-8")
    response = await module.update_configuration_file(
        "simple",
        module.ConfigurationFileUpdate(
            expected_revision=module._revision(current),
            content="demo:\n  VALUE: 2\n",
        ),
    )

    assert response.data["apply_mode"] == "restart_pending"
    assert response.data["restart_required"] is True
    assert response.data["restart_available"] is True
    assert response.data["changed_keys"] == ["demo.VALUE"]
    saved_state = restart_state.read_restart_state()
    assert saved_state["restart_tickets"]["webui.settings"]["source"] == (
        "webui.settings"
    )
    assert "launcher_action" not in saved_state

    accepted, _ = await _restart_utils.request_restart(
        "webui.settings", require_ticket="webui.settings"
    )

    assert accepted is True
    requested_state = restart_state.read_restart_state()
    assert requested_state["launcher_action"] == "restart"
