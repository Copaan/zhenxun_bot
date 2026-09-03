import asyncio
from pathlib import Path
from types import SimpleNamespace

from nonebot.utils import path_to_module_name
import pytest


def test_runtime_module_name_matches_nonebot_for_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from zhenxun.builtin_plugins.web_ui.api.tabs.plugin_manage.store import (
        _resolve_runtime_module_name,
    )

    monkeypatch.chdir(tmp_path)
    plugin = tmp_path / "zhenxun" / "plugins" / "demo.py"
    plugin.parent.mkdir(parents=True)
    plugin.write_text("VALUE = 1\n", encoding="utf-8")

    assert _resolve_runtime_module_name(plugin) == "zhenxun.plugins.demo"
    assert _resolve_runtime_module_name(plugin) == path_to_module_name(plugin)


def test_runtime_module_name_matches_nonebot_for_package(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from zhenxun.builtin_plugins.web_ui.api.tabs.plugin_manage.store import (
        _resolve_runtime_module_name,
    )

    monkeypatch.chdir(tmp_path)
    entrypoint = tmp_path / "zhenxun" / "plugins" / "demo" / "__init__.py"
    entrypoint.parent.mkdir(parents=True)
    entrypoint.write_text("VALUE = 1\n", encoding="utf-8")

    assert _resolve_runtime_module_name(entrypoint.parent) == "zhenxun.plugins.demo"
    assert _resolve_runtime_module_name(entrypoint.parent) == path_to_module_name(
        entrypoint
    )


@pytest.mark.parametrize("case", ["outside", "missing_init", "invalid_identifier"])
def test_runtime_module_name_rejects_invalid_paths(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, case: str
) -> None:
    from zhenxun.builtin_plugins.web_ui.api.tabs.plugin_manage.store import (
        PluginRuntimeModuleError,
        _resolve_runtime_module_name,
    )

    monkeypatch.chdir(tmp_path)
    plugin_root = tmp_path / "zhenxun" / "plugins"
    plugin_root.mkdir(parents=True)
    if case == "outside":
        path = tmp_path / "outside.py"
        path.write_text("VALUE = 1\n", encoding="utf-8")
    elif case == "missing_init":
        path = plugin_root / "missing_init"
        path.mkdir()
    else:
        path = plugin_root / "invalid-name.py"
        path.write_text("VALUE = 1\n", encoding="utf-8")

    with pytest.raises(PluginRuntimeModuleError, match="plugin_runtime_module_invalid"):
        _resolve_runtime_module_name(path)


def test_uninstall_runtime_module_name_allows_runtime_data_only(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from zhenxun.builtin_plugins.web_ui.api.tabs.plugin_manage.store import (
        _resolve_uninstall_runtime_module_name,
    )

    monkeypatch.chdir(tmp_path)
    plugin = tmp_path / "zhenxun" / "plugins" / "demo"
    (plugin / "database").mkdir(parents=True)
    (plugin / "database" / "runtime.db").write_bytes(b"runtime data")

    assert _resolve_uninstall_runtime_module_name(plugin) == "zhenxun.plugins.demo"


@pytest.mark.asyncio
async def test_new_store_plugin_uses_installed_runtime_module_name(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from zhenxun.builtin_plugins.web_ui.api.tabs.plugin_manage import store
    from zhenxun.services.runtime_reload.models import ApplyMode, RuntimeOperation

    monkeypatch.chdir(tmp_path)
    plugin = tmp_path / "zhenxun" / "plugins" / "jitang.py"
    plugin.parent.mkdir(parents=True)
    plugin.write_text("VALUE = 1\n", encoding="utf-8")
    plugin_info = SimpleNamespace(module_path="plugins.jitang")
    store_manager = SimpleNamespace(
        _resolve_local_plugin_path=lambda *_args, **_kwargs: plugin
    )
    received: dict[str, object] = {}

    async def load_new_plugin(
        module_name: str, root: Path, changed: set[Path]
    ) -> RuntimeOperation:
        received.update(module_name=module_name, root=root, changed=changed)
        return RuntimeOperation(
            ApplyMode.HOT_RELOADED,
            "completed",
            [module_name],
            generation=1,
        )

    monkeypatch.setattr(
        store.plugin_runtime_manager, "load_new_plugin", load_new_plugin
    )
    processed: list[Path] = []
    monkeypatch.setattr(
        store.plugin_runtime_manager,
        "mark_content_processed",
        lambda path: processed.append(path),
    )

    operation = await store._apply_store_change(
        store_manager,
        plugin_info,
        False,
        {},
        newly_installed=True,
    )

    assert received["module_name"] == "zhenxun.plugins.jitang"
    assert received["root"] == plugin
    assert plugin.resolve() in received["changed"]
    assert processed == [plugin.resolve()]
    assert plugin_info.module_path == "plugins.jitang"
    assert operation["apply_mode"] == "hot_reloaded"


@pytest.mark.parametrize(
    ("mode", "expected"),
    [
        ("hot_reloaded", "已安装并热加载"),
        ("restart_requested", "已请求受控重启"),
        ("restart_pending", "等待重启后生效"),
        ("failed", "运行时应用失败"),
    ],
)
def test_store_operation_info_follows_apply_mode(mode: str, expected: str) -> None:
    from zhenxun.builtin_plugins.web_ui.api.tabs.plugin_manage.store import (
        _store_operation_info,
    )

    info = _store_operation_info("安装", "鸡汤", {"apply_mode": mode})

    assert expected in info


@pytest.mark.asyncio
async def test_store_change_does_not_wait_on_its_own_content_hold(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from zhenxun.builtin_plugins.web_ui.api.tabs.plugin_manage import store
    from zhenxun.services.runtime_reload.models import ApplyMode, RuntimeOperation

    monkeypatch.chdir(tmp_path)
    plugin = tmp_path / "zhenxun" / "plugins" / "demo.py"
    plugin.parent.mkdir(parents=True)
    plugin.write_text("VALUE = 2\n", encoding="utf-8")
    plugin_info = SimpleNamespace(module_path="plugins.demo")
    store_manager = SimpleNamespace(
        _resolve_local_plugin_path=lambda *_args, **_kwargs: plugin
    )

    async def apply_plugin_changes(
        changed: set[Path], *, submit_restart: bool
    ) -> RuntimeOperation:
        assert plugin.resolve() in changed
        assert submit_restart is False
        return RuntimeOperation(
            ApplyMode.HOT_RELOADED,
            "completed",
            ["demo"],
            generation=1,
        )

    monkeypatch.setattr(
        store.plugin_runtime_manager,
        "apply_plugin_changes",
        apply_plugin_changes,
    )
    before = {plugin.resolve(): "old"}
    async with store.plugin_runtime_manager.hold_content_changes({plugin}):
        operation = await asyncio.wait_for(
            store._apply_store_change(
                store_manager,
                plugin_info,
                False,
                before,
            ),
            timeout=0.5,
        )

    assert operation["apply_mode"] == "hot_reloaded"


def test_satisfied_requirements_file_does_not_force_runtime_restart(
    tmp_path: Path,
) -> None:
    from zhenxun.builtin_plugins.web_ui.api.tabs.plugin_manage.store import (
        _changed_plugin_files,
    )

    source = (tmp_path / "demo.py").resolve()
    requirements = (tmp_path / "requirements.txt").resolve()
    before = {source: "old", requirements: "old"}
    after = {source: "new", requirements: "new"}

    assert _changed_plugin_files(before, after, include_requirements=False) == {source}
