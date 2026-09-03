from __future__ import annotations

from io import StringIO
from pathlib import Path

from dotenv import dotenv_values
import pytest


def test_custom_env_operations_preserve_secrets_and_delete() -> None:
    from zhenxun.builtin_plugins.web_ui.api.tabs.system.configuration import (
        CustomEnvOperation,
        _update_custom_env,
    )

    content = 'PUBLIC_VALUE = "old"\nPRIVATE_TOKEN = "secret"\n'
    updated = _update_custom_env(
        content,
        [
            CustomEnvOperation(key="PUBLIC_VALUE", operation="set", value="new value"),
            CustomEnvOperation(key="PRIVATE_TOKEN", operation="delete"),
            CustomEnvOperation(key="Plugin_Mode", operation="set", value="fast"),
        ],
    )

    values = dotenv_values(stream=StringIO(updated))
    assert values == {"PUBLIC_VALUE": "new value", "Plugin_Mode": "fast"}


@pytest.mark.asyncio
async def test_unknown_env_pending_is_cleared_when_restored(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from zhenxun.services import runtime_environment as module

    pending: list[list[str]] = []
    cleared: list[str] = []
    monkeypatch.setattr(
        module, "mark_restart_pending", lambda _source, reasons: pending.append(reasons)
    )
    monkeypatch.setattr(
        module, "clear_restart_pending", lambda source: cleared.append(source)
    )
    monkeypatch.setattr(module, "clear_restart_ticket_if_idle", lambda: None)

    manager = module.RuntimeEnvironmentManager()
    manager.startup_values = {}
    manager.effective_values = {}

    result = await manager.apply("", "PLUGIN_CUSTOM=value\n")
    assert result.apply_mode == "restart_pending"
    assert result.reason_codes == ["environment:PLUGIN_CUSTOM"]

    result = await manager.apply("PLUGIN_CUSTOM=value\n", "")
    assert result.apply_mode == "config_reloaded"
    assert result.restart_required is False
    assert cleared[-1] == module.ENV_APPLY_SOURCE


@pytest.mark.asyncio
async def test_proxy_change_uses_component_transaction(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from zhenxun.services import runtime_environment as module
    import zhenxun.services.lifecycle as lifecycle_module
    from zhenxun.services.lifecycle import ComponentSpec, LifecycleKernel
    from zhenxun.services.runtime_reload import plugin_runtime_manager

    calls: list[str] = []
    kernel = LifecycleKernel()

    async def start() -> None:
        calls.append("start")

    async def stop() -> None:
        calls.append("stop")

    kernel.register(
        ComponentSpec(
            "http",
            stage="management",
            restart_policy="component",
            config_keys=("SYSTEM_PROXY",),
        ),
        start,
        stop=stop,
    )
    await kernel.start_stage("management")
    calls.clear()
    monkeypatch.setattr(lifecycle_module, "lifecycle_kernel", kernel)
    monkeypatch.setattr(module, "clear_restart_pending", lambda _source: None)
    monkeypatch.setattr(module, "clear_restart_ticket_if_idle", lambda: None)
    monkeypatch.setattr(
        plugin_runtime_manager, "environment_consumers", lambda _keys: (set(), set())
    )

    async def no_consumers(*_args, **_kwargs):
        return None

    monkeypatch.setattr(plugin_runtime_manager, "reload_env_consumers", no_consumers)
    manager = module.RuntimeEnvironmentManager()
    manager.startup_values = {"SYSTEM_PROXY": None}
    manager.effective_values = {"SYSTEM_PROXY": None}

    async def apply_values(_values, _keys, *, rebuild_components=True) -> None:
        calls.append("apply" if not rebuild_components else "direct")

    monkeypatch.setattr(manager, "_apply_runtime_values", apply_values)

    result = await manager.apply("", "SYSTEM_PROXY=http://127.0.0.1:8080\n")

    assert result.apply_mode == "component_restarted"
    assert result.affected_components == ["http"]
    assert result.component_effects == {"SYSTEM_PROXY": "component_restarted"}
    assert calls == ["stop", "apply", "start"]


def test_resource_entry_swap_preserves_temp(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from zhenxun import update_service

    root = tmp_path / "project"
    resources = root / "resources"
    staged = root / "staged"
    (resources / "image").mkdir(parents=True)
    (resources / "temp").mkdir()
    (staged / "image").mkdir(parents=True)
    (resources / "image" / "asset.txt").write_text("old", encoding="utf-8")
    (resources / "temp" / "active.tmp").write_text("keep", encoding="utf-8")
    (staged / "image" / "asset.txt").write_text("new", encoding="utf-8")
    (staged / "__version__").write_text("2", encoding="utf-8")
    monkeypatch.setattr(update_service, "_ROOT", root)
    monkeypatch.setattr(update_service, "_STAGING_ROOT", root / "runtime")

    next_root, old_root = update_service._prepare_resource_swap("job", staged)
    swapped = update_service._swap_resource_entries(next_root, old_root)

    assert (resources / "image" / "asset.txt").read_text(encoding="utf-8") == "new"
    assert (resources / "temp" / "active.tmp").read_text(encoding="utf-8") == "keep"
    update_service._rollback_resource_entries(swapped, old_root)
    assert (resources / "image" / "asset.txt").read_text(encoding="utf-8") == "old"


def test_resource_swap_lock_restores_old_entry(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from zhenxun import update_service

    root = tmp_path / "project"
    resources = root / "resources"
    next_root = root / "next"
    old_root = root / "old"
    (resources / "image").mkdir(parents=True)
    (next_root / "image").mkdir(parents=True)
    old_root.mkdir()
    (resources / "image" / "asset.txt").write_text("old", encoding="utf-8")
    (next_root / "image" / "asset.txt").write_text("new", encoding="utf-8")
    monkeypatch.setattr(update_service, "_ROOT", root)
    original_replace = update_service.os.replace

    def locked_replace(source: Path, destination: Path) -> None:
        if Path(source) == next_root / "image":
            raise PermissionError("locked")
        original_replace(source, destination)

    monkeypatch.setattr(update_service.os, "replace", locked_replace)
    with pytest.raises(update_service.ResourceHotSwapUnavailable):
        update_service._swap_resource_entries(next_root, old_root)

    assert (resources / "image" / "asset.txt").read_text(encoding="utf-8") == "old"


def test_static_env_scan_only_records_import_scope(tmp_path: Path) -> None:
    from zhenxun.services.runtime_reload.manager import _static_env_dependencies

    source = tmp_path / "plugin.py"
    source.write_text(
        "from zhenxun.configs.config import BotConfig\n"
        "NAME = BotConfig.self_nickname\n"
        "def handler():\n"
        "    return BotConfig.system_proxy\n",
        encoding="utf-8",
    )

    assert _static_env_dependencies({source}) == {"SELF_NICKNAME"}
