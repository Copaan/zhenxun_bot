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
    import zhenxun.services.runtime_config_reload as runtime_config
    from zhenxun.services.runtime_reload import plugin_runtime_manager
    from zhenxun.services.runtime_reload.models import ApplyMode

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
        changed: set[tuple[str, str]], *, submit_restart: bool = True
    ) -> None:
        captured.append((changed, submit_restart))
        return None

    monkeypatch.setattr(Config, "reload", reload_config)
    monkeypatch.setattr(runtime_config.get_llm_config, "cache_clear", lambda: None)
    monkeypatch.setattr(runtime_config, "clear_all_cache", lambda: None)
    monkeypatch.setattr(init_manager, "init", lambda: None)
    monkeypatch.setattr(init_manager, "load_to_db", load_to_db)
    monkeypatch.setattr(LimitManager, "update_limits", update_limits)
    monkeypatch.setattr(
        plugin_runtime_manager, "reload_config_consumers", reload_consumers
    )
    monkeypatch.setattr(
        plugin_runtime_manager, "mark_content_processed", lambda _path: None
    )

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
    import zhenxun.services.runtime_config_reload as runtime_config
    from zhenxun.services.runtime_reload import plugin_runtime_manager
    from zhenxun.services.runtime_reload.models import ApplyMode, RuntimeOperation

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
    monkeypatch.setattr(
        plugin_runtime_manager, "reload_config_consumers", reload_consumers
    )

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
async def test_runtime_metadata_claims_generated_config_write(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from tortoise import Tortoise

    from zhenxun.builtin_plugins.init import init_config, init_plugin, init_task
    from zhenxun.services.runtime_reload.manager import PluginRuntimeManager

    config_path = tmp_path / "config.yaml"

    def reconcile_config_runtime() -> set[Path]:
        config_path.write_text("alapi:\n  ALAPI_TOKEN:\n", encoding="utf-8")
        return {config_path.resolve()}

    async def reconcile_runtime() -> None:
        return None

    monkeypatch.setattr(Tortoise, "_inited", True)
    monkeypatch.setattr(
        init_config, "reconcile_config_runtime", reconcile_config_runtime
    )
    monkeypatch.setattr(init_plugin, "reconcile_plugin_runtime", reconcile_runtime)
    monkeypatch.setattr(init_task, "reconcile_task_runtime", reconcile_runtime)
    manager = PluginRuntimeManager()

    await manager._reconcile_runtime_metadata()

    assert manager.claim_content_changes({config_path.resolve()}) == set()


@pytest.mark.asyncio
async def test_delayed_internal_event_is_ignored_but_external_edit_reloads(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from zhenxun.services.runtime_reload import coordinator as coordinator_module
    from zhenxun.services.runtime_reload.coordinator import RuntimeChangeCoordinator
    from zhenxun.services.runtime_reload.manager import PluginRuntimeManager
    from zhenxun.services.runtime_reload.models import ApplyMode, RuntimeOperation

    config_path = tmp_path / "config.yaml"
    config_path.write_text("demo:\n  VALUE: 1\n", encoding="utf-8")
    manager = PluginRuntimeManager()
    coordinator = RuntimeChangeCoordinator(manager)
    reload_count = 0

    async def reload_runtime_config() -> RuntimeOperation:
        nonlocal reload_count
        reload_count += 1
        return RuntimeOperation(
            ApplyMode.CONFIG_RELOADED,
            "completed",
            ["demo.VALUE"],
            config_keys=["demo.VALUE"],
        )

    monkeypatch.setattr(coordinator_module, "_CONFIG_FILE", config_path.resolve())
    monkeypatch.setattr(
        coordinator_module, "reload_runtime_config", reload_runtime_config
    )
    manager.mark_content_processed(config_path)

    assert await coordinator.process({config_path}) is None
    assert reload_count == 0

    config_path.write_text("demo:\n  VALUE: 2\n", encoding="utf-8")
    operation = await coordinator.process({config_path})

    assert operation is not None
    assert operation.mode is ApplyMode.CONFIG_RELOADED
    assert reload_count == 1


@pytest.mark.asyncio
async def test_webui_env_transaction_is_ignored_but_external_edit_restarts(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from zhenxun.builtin_plugins.web_ui.api.configure import persistence
    from zhenxun.services import runtime_reload
    from zhenxun.services.runtime_reload import coordinator as coordinator_module
    from zhenxun.services.runtime_reload.coordinator import RuntimeChangeCoordinator
    from zhenxun.services.runtime_reload.manager import PluginRuntimeManager
    from zhenxun.services.runtime_reload.models import (
        ApplyMode,
        RuntimeOperation,
    )

    env_path = tmp_path / ".env.dev"
    manager = PluginRuntimeManager()
    coordinator = RuntimeChangeCoordinator(manager)
    restart_reasons: list[str] = []

    async def request_restart(affected: set[str], reason: str) -> RuntimeOperation:
        restart_reasons.append(reason)
        return RuntimeOperation(
            ApplyMode.RESTART_PENDING,
            "pending_restart",
            sorted(affected),
            reason,
        )

    monkeypatch.setattr(runtime_reload, "plugin_runtime_manager", manager)
    monkeypatch.setattr(coordinator_module, "_ENV_FILES", {env_path.resolve()})
    monkeypatch.setattr(manager, "request_restart", request_restart)

    persistence._write_transaction([(env_path, b"HOST = 0.0.0.0\n")])
    assert await coordinator.process({env_path}) is None
    assert restart_reasons == []

    env_path.write_text("HOST = 127.0.0.1\n", encoding="utf-8")
    operation = await coordinator.process({env_path})

    assert operation is not None
    assert operation.mode is ApplyMode.RESTART_PENDING
    assert restart_reasons == ["environment_changed"]


@pytest.mark.asyncio
async def test_real_import_time_config_change_keeps_restart_fallback(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from zhenxun.services.runtime_reload.manager import PluginRuntimeManager
    from zhenxun.services.runtime_reload.models import (
        ApplyMode,
        PluginUnit,
        ReloadClassification,
        RuntimeOperation,
    )

    source = tmp_path / "consumer.py"
    source.write_text("VALUE = 1\n", encoding="utf-8")
    manager = PluginRuntimeManager()
    unit = PluginUnit("consumer", "demo.consumer", object(), source, files={source})
    unit.config_dependencies.add(("demo", "VALUE"))
    unit.classification = ReloadClassification.RESTART_REQUIRED
    manager.units = {unit.plugin_id: unit}

    restart_submissions: list[bool] = []

    async def request_restart(
        affected: set[str], reason: str, *, submit_launcher: bool = True
    ) -> RuntimeOperation:
        restart_submissions.append(submit_launcher)
        return RuntimeOperation(
            ApplyMode.RESTART_PENDING,
            "pending_restart",
            sorted(affected),
            reason,
        )

    monkeypatch.setattr(manager, "request_restart", request_restart)

    assert await manager.reload_config_consumers({("other", "VALUE")}) is None

    operation = await manager.reload_config_consumers(
        {("demo", "VALUE")}, submit_restart=False
    )

    assert operation is not None
    assert operation.mode is ApplyMode.RESTART_PENDING
    assert operation.reason == "import_time_config_consumer_requires_restart"
    assert restart_submissions == [False]


@pytest.mark.asyncio
async def test_matching_hot_config_consumer_is_reloaded_without_unrelated_units(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from zhenxun.services.runtime_reload.manager import PluginRuntimeManager
    from zhenxun.services.runtime_reload.models import (
        ApplyMode,
        PluginUnit,
        ReloadClassification,
        RuntimeOperation,
    )

    manager = PluginRuntimeManager()
    hot = PluginUnit("hot", "demo.hot", object(), tmp_path / "hot.py")
    hot.config_dependencies.add(("alapi", "ALAPI_TOKEN"))
    hot.classification = ReloadClassification.HOT_RELOADABLE
    unrelated = PluginUnit("unsafe", "demo.unsafe", object(), tmp_path / "unsafe.py")
    unrelated.config_dependencies.add(("other", "VALUE"))
    unrelated.classification = ReloadClassification.RESTART_REQUIRED
    manager.units = {hot.plugin_id: hot, unrelated.plugin_id: unrelated}
    reloaded: list[set[str]] = []

    async def reload_units(affected: set[str]) -> RuntimeOperation:
        reloaded.append(affected)
        return RuntimeOperation(ApplyMode.HOT_RELOADED, "completed", sorted(affected))

    monkeypatch.setattr(manager, "_reload_units", reload_units)

    operation = await manager.reload_config_consumers({("alapi", "ALAPI_TOKEN")})

    assert operation is not None
    assert operation.mode is ApplyMode.HOT_RELOADED
    assert reloaded == [{"hot"}]


@pytest.mark.asyncio
async def test_webui_config_save_waits_for_explicit_restart(
    app, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    del app
    import zhenxun.builtin_plugins.web_ui.api.tabs.system.configuration as module
    from zhenxun.services.runtime_reload.models import ApplyMode, RuntimeOperation
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
    monkeypatch.setattr(_restart_utils, "_RESTART_STATE_FILE", state_path)
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
