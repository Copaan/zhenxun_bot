from __future__ import annotations

import asyncio
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest


def _unit(path: Path) -> Any:
    from zhenxun.services.runtime_reload.models import PluginUnit

    return PluginUnit(
        plugin_id="fixture",
        module_name="fixture",
        manager=object(),
        root=path.parent,
        files={path},
    )


def test_classifier_allows_simple_matcher_module(tmp_path: Path) -> None:
    from zhenxun.services.runtime_reload.classifier import classify_unit
    from zhenxun.services.runtime_reload.models import ReloadClassification

    source = tmp_path / "plugin.py"
    source.write_text("from nonebot import on_command\non_command('demo')\n")
    unit = _unit(source)

    classify_unit(unit)

    assert unit.classification is ReloadClassification.HOT_RELOADABLE
    assert not unit.reasons


def test_classifier_allows_drainable_async_route_and_deferred_resources(
    tmp_path: Path,
) -> None:
    from zhenxun.services.runtime_reload.classifier import classify_unit
    from zhenxun.services.runtime_reload.models import ReloadClassification

    source = tmp_path / "plugin.py"
    source.write_text(
        "from fastapi import APIRouter\n"
        "from threading import Thread\n"
        "router = APIRouter()\n"
        "@router.get('/demo')\n"
        "async def demo(): return {'ok': True}\n"
        "def later(): Thread(target=lambda: None).start()\n",
        encoding="utf-8",
    )
    unit = _unit(source)

    classify_unit(unit)

    assert unit.classification is ReloadClassification.HOT_RELOADABLE
    assert not unit.reasons


def test_shared_dependency_import_time_function_call_is_restart_boundary(
    tmp_path: Path,
) -> None:
    from zhenxun.services.runtime_reload.classifier import classify_unit
    from zhenxun.services.runtime_reload.manager import PluginRuntimeManager
    from zhenxun.services.runtime_reload.models import (
        PluginUnit,
        ReloadClassification,
    )

    manager = PluginRuntimeManager()
    units = {}
    for plugin_id, backend in (("first", "a"), ("second", "b")):
        source = tmp_path / f"{plugin_id}.py"
        source.write_text(
            "from shared_dependency_fixture import select_backend\n"
            f"select_backend('{backend}')\n",
            encoding="utf-8",
        )
        unit = PluginUnit(
            plugin_id,
            plugin_id,
            object(),
            source,
            module_names={plugin_id},
            files={source},
        )
        classify_unit(unit)
        units[plugin_id] = unit
    manager.units = units
    manager.module_to_unit = {plugin_id: plugin_id for plugin_id in units}

    manager._collect_runtime_boundaries()

    for unit in units.values():
        assert unit.classification is ReloadClassification.RESTART_REQUIRED
        assert "shared_dependency_global_mutation" in unit.reasons
        assert manager._shared_dependency_evidence[unit.plugin_id]


@pytest.mark.asyncio
async def test_asgi_route_tracking_is_idempotent_when_router_is_included() -> None:
    from fastapi import APIRouter

    from zhenxun.services.lifecycle import LeaseState
    from zhenxun.services.runtime_reload.manager import PluginRuntimeManager
    from zhenxun.services.runtime_reload.ownership import owner_context

    original_api_route = APIRouter.add_api_route
    original_websocket_route = APIRouter.add_api_websocket_route
    manager = PluginRuntimeManager()
    try:
        manager._install_asgi_route_tracking()
        incarnation = manager._ensure_incarnation("route_fixture")
        incarnation.lease_state = LeaseState.ACTIVE
        child = APIRouter()

        with owner_context("route_fixture"):

            @child.get("/fixture")
            async def fixture_endpoint() -> dict[str, bool]:
                return {"ok": True}

            parent = APIRouter()
            parent.include_router(child)

        child_endpoint = child.routes[0].endpoint
        parent_endpoint = parent.routes[0].endpoint
        assert parent_endpoint is child_endpoint
        assert await parent_endpoint() == {"ok": True}
    finally:
        APIRouter.add_api_route = original_api_route
        APIRouter.add_api_websocket_route = original_websocket_route


def test_classifier_reuses_unchanged_file_cache(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from zhenxun.services.runtime_reload.classifier import classify_unit

    source = tmp_path / "plugin.py"
    source.write_text("import os\nTOKEN = os.getenv('DEMO_TOKEN')\n", encoding="utf-8")
    first = _unit(source)
    classify_unit(first)
    cached = {"file_cache": first.file_cache}
    original_read_bytes = Path.read_bytes

    def guarded_read_bytes(path: Path) -> bytes:
        if path == source:
            raise AssertionError("unchanged source was read again")
        return original_read_bytes(path)

    monkeypatch.setattr(Path, "read_bytes", guarded_read_bytes)
    second = _unit(source)
    classify_unit(second, cached)

    assert second.fingerprint == first.fingerprint
    assert "DEMO_TOKEN" in second.env_dependencies


@pytest.mark.parametrize(
    ("source", "reason"),
    [
        (
            "from fastapi import APIRouter\n"
            "router = APIRouter()\n"
            "router.mount('/demo', object())\n",
            "asgi_root_registration",
        ),
        (
            "from threading import Thread\nThread(target=lambda: None).start()\n",
            "thread_or_process",
        ),
        (
            "from zhenxun.services.plugin_init import PluginInit\n"
            "class Lifecycle(PluginInit):\n"
            "    async def install(self): pass\n"
            "    async def remove(self): pass\n",
            "legacy_lifecycle_not_transactional",
        ),
    ],
)
def test_classifier_marks_hard_boundaries(
    tmp_path: Path, source: str, reason: str
) -> None:
    from zhenxun.services.runtime_reload.classifier import classify_unit
    from zhenxun.services.runtime_reload.models import ReloadClassification

    path = tmp_path / "plugin.py"
    path.write_text(source, encoding="utf-8")
    unit = _unit(path)

    classify_unit(unit)

    assert unit.classification is ReloadClassification.RESTART_REQUIRED
    assert reason in unit.reasons


def test_dependency_restart_paths_are_restricted(tmp_path: Path) -> None:
    from zhenxun.utils._restart_utils import _validated_dependency_paths

    assert _validated_dependency_paths({Path("pyproject.toml")}) == ["pyproject.toml"]
    outside = tmp_path / "requirements.txt"
    outside.write_text("example\n", encoding="utf-8")
    with pytest.raises(ValueError, match="dependency_path_not_allowed"):
        _validated_dependency_paths({outside})


def test_runtime_dependency_generations_are_not_watched(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from watchfiles import Change

    from zhenxun.services.runtime_reload.models import PluginUnit
    from zhenxun.services.runtime_reload.watcher import _interesting, _watch_roots

    monkeypatch.chdir(tmp_path)
    source_root = tmp_path / "zhenxun"
    source_root.mkdir()
    public_root = tmp_path / "data" / "web_ui" / "public"
    public_root.mkdir(parents=True)
    generation_root = (
        tmp_path
        / "data"
        / "runtime"
        / "nonebot-site-packages"
        / "generation-3"
        / "nonebot_plugin_demo"
    )
    generation_root.mkdir(parents=True)
    module_path = generation_root / "__init__.py"
    module_path.write_text("VALUE = 1\n", encoding="utf-8")
    manager = SimpleNamespace(
        units={
            "demo": PluginUnit("demo", "nonebot_plugin_demo", object(), generation_root)
        }
    )

    roots = _watch_roots(manager)

    assert generation_root.resolve() not in roots
    assert source_root.resolve() in roots
    assert public_root.resolve() in roots
    assert _interesting(Change.deleted, str(module_path)) is False


def test_runtime_watcher_ignores_unrelated_json(tmp_path: Path) -> None:
    from watchfiles import Change

    from zhenxun.services.runtime_reload.watcher import _interesting

    assert not _interesting(Change.modified, str(tmp_path / "random.json"))
    assert _interesting(
        Change.modified,
        str(tmp_path / "data" / "web_ui" / "public" / "version.json"),
    )


@pytest.mark.parametrize(
    ("mode", "submit_restart"),
    [("hot_only", False), ("auto_restart", True)],
)
@pytest.mark.asyncio
async def test_runtime_watcher_mode_controls_restart_submission(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    mode: str,
    submit_restart: bool,
) -> None:
    from watchfiles import Change

    from zhenxun.services.runtime_reload.manager import PluginRuntimeManager
    import zhenxun.services.runtime_reload.watcher as watcher

    manager = PluginRuntimeManager()
    source = tmp_path / "plugin.py"
    source.write_text("VALUE = 1\n", encoding="utf-8")
    observed = []

    async def process(paths, *, submit_restart):
        observed.append((paths, submit_restart))

    async def changes(*_args, **_kwargs):
        yield {(Change.modified, str(source))}
        raise asyncio.CancelledError

    monkeypatch.setattr(watcher, "_watch_roots", lambda _manager: [tmp_path])
    monkeypatch.setattr(watcher, "_build_manifest", lambda _roots: {})
    monkeypatch.setattr(watcher, "awatch", changes)
    monkeypatch.setattr(manager, "process_changes", process)
    monkeypatch.setattr(manager, "runtime_watch_mode", lambda: mode)

    with pytest.raises(asyncio.CancelledError):
        await watcher.watch_runtime_changes(manager)

    assert observed == [({source}, submit_restart)]
    assert manager.watcher_state == "stopped"


@pytest.mark.asyncio
async def test_runtime_watcher_retries_without_disabling_manual_reload(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from zhenxun.services.runtime_reload.manager import PluginRuntimeManager
    import zhenxun.services.runtime_reload.watcher as watcher

    manager = PluginRuntimeManager()
    manager.enabled = True
    calls = 0
    delays = []

    async def failing_watch(*_args, **_kwargs):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("watch failed")
        raise asyncio.CancelledError
        yield  # pragma: no cover

    async def no_wait(delay):
        delays.append(delay)

    monkeypatch.setattr(watcher, "_watch_roots", lambda _manager: [tmp_path])
    monkeypatch.setattr(watcher, "_build_manifest", lambda _roots: {})
    monkeypatch.setattr(watcher, "awatch", failing_watch)
    monkeypatch.setattr(watcher.asyncio, "sleep", no_wait)

    with pytest.raises(asyncio.CancelledError):
        await watcher.watch_runtime_changes(manager)

    assert calls == 2
    assert delays == [1]
    assert manager.enabled
    assert manager.watcher_retry_count == 1
    assert manager.watcher_last_error == "watcher_failed:RuntimeError"


@pytest.mark.parametrize(
    ("configured", "expected"),
    [
        ("hot_only", "hot_only"),
        ("disabled", "disabled"),
        ("auto_restart", "auto_restart"),
        ("invalid", "hot_only"),
    ],
)
def test_runtime_watch_mode_validation(
    monkeypatch: pytest.MonkeyPatch, configured: str, expected: str
) -> None:
    from zhenxun.services.runtime_reload.manager import PluginRuntimeManager

    monkeypatch.setenv("RUNTIME_WATCH_MODE", configured)
    assert PluginRuntimeManager.runtime_watch_mode() == expected


@pytest.mark.asyncio
async def test_plugin_init_strict_mode_propagates_activation_failure() -> None:
    from zhenxun.services.plugin_init import (
        PluginInit,
        PluginInitManager,
    )

    class BrokenLifecycle(PluginInit):
        async def install(self) -> None:
            raise RuntimeError("activation failed")

        async def remove(self) -> None:
            return None

    module_name = BrokenLifecycle.__module__
    try:
        await PluginInitManager.install(module_name)
        with pytest.raises(RuntimeError, match="activation failed"):
            await PluginInitManager.install(module_name, raise_on_error=True)
    finally:
        PluginInitManager.remove_registrations({module_name})


@pytest.mark.asyncio
async def test_internal_store_write_absorbs_delayed_watcher_event(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from zhenxun.services.runtime_reload.manager import PluginRuntimeManager
    from zhenxun.utils import _restart_utils

    restart_requests: list[str] = []

    async def request_restart(source: str) -> tuple[bool, str]:
        restart_requests.append(source)
        return True, "restart_requested"

    monkeypatch.setenv("ZHENXUN_LAUNCHER_PID", "1234")
    monkeypatch.setattr(_restart_utils, "request_restart", request_restart)

    manager = PluginRuntimeManager()
    plugin_root = tmp_path / "zhenxun" / "plugins" / "pix_gallery"
    plugin_root.mkdir(parents=True)
    source = plugin_root / "__init__.py"
    source.write_text("VALUE = 1\n", encoding="utf-8")
    manager.mark_content_processed(source)

    async with manager.hold_content_changes({plugin_root}):
        source.write_text("VALUE = 2\n", encoding="utf-8")
        delayed_event = asyncio.create_task(
            manager.process_changes({source}, submit_restart=True)
        )
        await asyncio.sleep(0)
        assert not delayed_event.done()
        manager.mark_content_processed(source)

    assert await delayed_event is None
    assert manager.pending_restart == set()
    assert restart_requests == []


def test_launcher_consumes_dependency_action(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from zhenxun.utils import restart_state

    state_file = tmp_path / "restart.json"
    monkeypatch.setattr(restart_state, "_RESTART_STATE_FILE", state_file)
    restart_state.write_restart_state(
        {
            "launcher_action": "sync_dependencies_restart",
            "launcher_not_before": 0,
            "dependency_paths": ["zhenxun/plugins/demo/requirements.txt"],
        }
    )

    assert restart_state.consume_launcher_action() == (
        "sync_dependencies_restart",
        ["zhenxun/plugins/demo/requirements.txt"],
    )
    assert not state_file.exists()


@pytest.mark.asyncio
async def test_manual_reload_includes_runtime_dependents(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from zhenxun.services.runtime_reload.manager import PluginRuntimeManager
    from zhenxun.services.runtime_reload.models import (
        ApplyMode,
        PluginUnit,
        RuntimeOperation,
    )

    manager = PluginRuntimeManager()
    manager.enabled = True
    provider_path = tmp_path / "provider.py"
    dependent_path = tmp_path / "dependent.py"
    provider_path.write_text("VALUE = 1\n", encoding="utf-8")
    dependent_path.write_text("VALUE = 2\n", encoding="utf-8")
    manager.units = {
        "provider": PluginUnit(
            "provider", "demo.provider", object(), provider_path, files={provider_path}
        ),
        "dependent": PluginUnit(
            "dependent",
            "demo.dependent",
            object(),
            dependent_path,
            files={dependent_path},
            dependencies={"provider"},
        ),
    }
    received: set[str] = set()

    async def reload_units(affected: set[str]) -> RuntimeOperation:
        received.update(affected)
        return RuntimeOperation(ApplyMode.HOT_RELOADED, "completed", sorted(affected))

    monkeypatch.setattr(manager, "_reload_units", reload_units)

    operation = await manager.reload_plugin("demo.provider")

    assert operation.mode is ApplyMode.HOT_RELOADED
    assert received == {"provider", "dependent"}


@pytest.mark.asyncio
async def test_manual_reload_rejects_restart_required_plugin(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from zhenxun.services.runtime_reload.manager import PluginRuntimeManager
    from zhenxun.services.runtime_reload.models import (
        ApplyMode,
        PluginUnit,
        ReloadClassification,
    )

    manager = PluginRuntimeManager()
    manager.enabled = True
    source = tmp_path / "plugin.py"
    source.write_text("VALUE = 1\n", encoding="utf-8")
    unit = PluginUnit("demo", "demo", object(), source, files={source})
    unit.classification = ReloadClassification.RESTART_REQUIRED
    unit.reasons.add("fastapi_route")
    manager.units = {"demo": unit}
    monkeypatch.setattr(manager, "_persist_index", lambda: None)

    operation = await manager.reload_plugin("demo")

    assert operation.mode is ApplyMode.FAILED
    assert operation.reason == "plugin_not_hot_reloadable"


@pytest.mark.asyncio
async def test_manual_reload_refreshes_late_runtime_boundaries(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import zhenxun.services.runtime_reload.manager as manager_module
    from zhenxun.services.runtime_reload.manager import PluginRuntimeManager
    from zhenxun.services.runtime_reload.models import ApplyMode, PluginUnit

    manager = PluginRuntimeManager()
    manager.enabled = True
    source = tmp_path / "plugin.py"
    source.write_text("VALUE = 1\n", encoding="utf-8")
    unit = PluginUnit("demo", "demo", object(), source, files={source})
    manager.units = {"demo": unit}
    manager.module_to_unit = {"demo": "demo"}

    class AliveThread:
        def is_alive(self) -> bool:
            return True

    manager._owned_threads["demo"].add(AliveThread())  # type: ignore[arg-type]
    monkeypatch.setattr(
        manager_module.nonebot, "get_app", lambda: SimpleNamespace(routes=[])
    )
    monkeypatch.setattr(manager, "_persist_index", lambda: None)

    operation = await manager.reload_plugin("demo")

    assert operation.mode is ApplyMode.FAILED
    assert operation.reason == "plugin_not_hot_reloadable"
    assert "live_thread" in unit.reasons


@pytest.mark.asyncio
async def test_failed_reload_restores_previous_module_and_incarnation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import sys
    from types import ModuleType

    from zhenxun.services.lifecycle import LeaseState
    from zhenxun.services.runtime_reload.manager import PluginRuntimeManager
    from zhenxun.services.runtime_reload.models import ApplyMode, PluginUnit

    class FailingManager:
        def load_plugin(self, _module_name: str):
            return None

    manager = PluginRuntimeManager()
    source = tmp_path / "rollback_demo.py"
    source.write_text("VALUE = 1\n", encoding="utf-8")
    old_module = ModuleType("rollback_demo")
    monkeypatch.setitem(sys.modules, "rollback_demo", old_module)
    unit = PluginUnit(
        "rollback_demo",
        "rollback_demo",
        FailingManager(),
        source,
        module_names={"rollback_demo"},
        files={source},
    )
    incarnation = manager._new_incarnation(unit.plugin_id)
    incarnation.lease_state = LeaseState.ACTIVE
    unit.incarnation_id = incarnation.incarnation_id
    manager.units = {unit.plugin_id: unit}
    manager.module_to_unit = {unit.module_name: unit.plugin_id}

    async def unload(_unit: PluginUnit) -> None:
        sys.modules.pop("rollback_demo", None)
        manager._revoke_incarnation("rollback_demo")

    monkeypatch.setattr(manager, "_drain_and_unload", unload)
    monkeypatch.setattr(manager, "_persist_index", lambda: None)

    operation = await manager._reload_units({"rollback_demo"})

    assert operation.mode is ApplyMode.FAILED
    assert operation.rollback_state == "semantic"
    assert sys.modules["rollback_demo"] is old_module
    assert manager.units["rollback_demo"] is unit
    assert manager._incarnations["rollback_demo"].lease_state is LeaseState.ACTIVE


@pytest.mark.asyncio
async def test_cancelled_timer_handles_are_not_retained(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import gc

    from zhenxun.services.lifecycle import LeaseState
    from zhenxun.services.runtime_reload.manager import PluginRuntimeManager
    from zhenxun.services.runtime_reload.ownership import owner_context

    manager = PluginRuntimeManager()
    incarnation = manager._new_incarnation("timer_fixture")
    incarnation.lease_state = LeaseState.ACTIVE
    manager._install_task_factory()
    loop = asyncio.get_running_loop()
    try:
        with owner_context("timer_fixture"):
            for _ in range(2500):
                await asyncio.wait_for(asyncio.sleep(0), timeout=1)
            for _ in range(2500):
                with pytest.raises(asyncio.TimeoutError):
                    await asyncio.wait_for(asyncio.sleep(1), timeout=0)
            pending = [asyncio.create_task(asyncio.sleep(60)) for _ in range(2500)]
            await asyncio.sleep(0)
            for task in pending:
                task.cancel()
            await asyncio.gather(*pending, return_exceptions=True)
            handles = [loop.call_later(60, lambda: None) for _ in range(2500)]
            for handle in handles:
                handle.cancel()
        del handles, pending, handle, task
        await asyncio.sleep(0)
        gc.collect()

        assert not list(manager._owned_handles["timer_fixture"])
    finally:
        manager._restore_task_factory()


@pytest.mark.asyncio
async def test_failed_reload_restores_lifecycle_config_job_and_task(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from datetime import datetime, timedelta, timezone
    import sys
    from types import ModuleType

    from nonebot_plugin_apscheduler import scheduler

    from zhenxun.configs.config import Config
    from zhenxun.services.lifecycle import LeaseState, lifecycle_kernel
    from zhenxun.services.runtime_reload.manager import PluginRuntimeManager
    from zhenxun.services.runtime_reload.models import ApplyMode, PluginUnit
    from zhenxun.utils.enum import PriorityLifecycleType
    from zhenxun.utils.manager.priority_manager import (
        PriorityLifecycle,
        _sync_kernel_declarations,
    )

    module_name = "rollback_full_fixture"
    component_id = "rollback-full-fixture"
    job_id = "rollback-full-fixture-job"
    config_key = "ROLLBACK_VALUE"

    class FailingManager:
        def load_plugin(self, _module_name: str):
            return None

    manager = PluginRuntimeManager()
    source = tmp_path / f"{module_name}.py"
    source.write_text("VALUE = 1\n", encoding="utf-8")
    old_module = ModuleType(module_name)
    monkeypatch.setitem(sys.modules, module_name, old_module)
    unit = PluginUnit(
        module_name,
        module_name,
        FailingManager(),
        source,
        module_names={module_name},
        files={source},
    )
    manager.units = {module_name: unit}
    manager.module_to_unit = {module_name: module_name}
    incarnation = manager._new_incarnation(module_name)
    incarnation.lease_state = LeaseState.ACTIVE
    unit.incarnation_id = incarnation.incarnation_id
    task_started = 0

    async def startup() -> None:
        nonlocal task_started
        task_started += 1
        task = asyncio.create_task(asyncio.Event().wait())
        manager._owned_tasks[module_name].add(task)
        task.add_done_callback(manager._owned_tasks[module_name].discard)

    async def shutdown() -> None:
        tasks = list(manager._owned_tasks.get(module_name, set()))
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    startup.__module__ = module_name
    shutdown.__module__ = module_name
    PriorityLifecycle.add(
        PriorityLifecycleType.STARTUP,
        startup,
        1,
        component_id=component_id,
    )
    PriorityLifecycle.add(
        PriorityLifecycleType.SHUTDOWN,
        shutdown,
        1,
        component_id=component_id,
    )
    Config.add_plugin_config(module_name, config_key, "old")
    manager._config_registrations[module_name].add((module_name, config_key))

    scheduler_was_running = scheduler.running
    if not scheduler_was_running:
        scheduler.start(paused=True)

    async def scheduled() -> None:
        return None

    scheduled.__module__ = module_name
    next_run_time = datetime.now(timezone.utc) + timedelta(days=1)
    scheduler.add_job(
        scheduled,
        "date",
        id=job_id,
        run_date=next_run_time,
    )
    manager._job_owners[job_id] = module_name

    async def noop() -> None:
        return None

    monkeypatch.setattr(manager, "_reconcile_runtime_metadata", noop)
    monkeypatch.setattr(manager, "_invalidate_generation_caches", noop)
    monkeypatch.setattr(manager, "_persist_index", lambda: None)
    _sync_kernel_declarations()
    await lifecycle_kernel.start_components({component_id})
    manager._observe_plugin_scope(unit)

    try:
        operation = await manager._reload_units({module_name})

        assert operation.mode is ApplyMode.FAILED
        assert operation.rollback_state == "semantic"
        assert manager.generation == 0
        assert manager.units[module_name] is unit
        assert not unit.draining
        assert sys.modules[module_name] is old_module
        assert manager._incarnations[module_name].lease_state is LeaseState.ACTIVE
        assert task_started == 2
        assert len(manager._owned_tasks[module_name]) == 1
        assert Config.get(module_name).get(config_key) == "old"
        assert scheduler.get_job(job_id) is not None
        assert scheduler.get_job(job_id).next_run_time == next_run_time
        assert lifecycle_kernel.component_status(component_id)["state"] == "ready"
    finally:
        await lifecycle_kernel.stop_components({component_id})
        lifecycle_kernel.unregister_components({component_id})
        lifecycle_kernel.forget_plugin_incarnation(module_name)
        PriorityLifecycle._data.get(PriorityLifecycleType.STARTUP, {}).get(
            1, []
        ).remove(startup)
        PriorityLifecycle._data.get(PriorityLifecycleType.SHUTDOWN, {}).get(
            1, []
        ).remove(shutdown)
        PriorityLifecycle._metadata.pop(startup, None)
        PriorityLifecycle._metadata.pop(shutdown, None)
        manager._remove_config_registrations(module_name)
        if scheduler.get_job(job_id):
            scheduler.remove_job(job_id)
        if not scheduler_was_running:
            scheduler.shutdown(wait=False)


@pytest.mark.asyncio
async def test_reload_detects_and_restores_shared_dependency_global_mutation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import sys
    from types import ModuleType

    from zhenxun.services.lifecycle import LeaseState
    from zhenxun.services.runtime_reload.manager import PluginRuntimeManager
    from zhenxun.services.runtime_reload.models import ApplyMode, PluginUnit

    shared = ModuleType("shared_dependency_runtime_fixture")
    shared.BACKEND = "old"
    monkeypatch.setitem(sys.modules, shared.__name__, shared)
    old_module = ModuleType("shared_mutator")
    monkeypatch.setitem(sys.modules, "shared_mutator", old_module)

    class MutatingManager:
        def load_plugin(self, _module_name: str):
            shared.BACKEND = "new"
            return SimpleNamespace(module_name="shared_mutator")

    manager = PluginRuntimeManager()
    units = {
        "shared_mutator": PluginUnit(
            "shared_mutator",
            "shared_mutator",
            MutatingManager(),
            tmp_path / "shared_mutator.py",
            module_names={"shared_mutator"},
            imported_modules={shared.__name__},
        ),
        "shared_peer": PluginUnit(
            "shared_peer",
            "shared_peer",
            object(),
            tmp_path / "shared_peer.py",
            module_names={"shared_peer"},
            imported_modules={shared.__name__},
        ),
    }
    for unit in units.values():
        unit.root.write_text("VALUE = 1\n", encoding="utf-8")
        unit.files = {unit.root}
        incarnation = manager._new_incarnation(unit.plugin_id)
        incarnation.lease_state = LeaseState.ACTIVE
        unit.incarnation_id = incarnation.incarnation_id
    manager.units = units
    manager.module_to_unit = {
        "shared_mutator": "shared_mutator",
        "shared_peer": "shared_peer",
    }

    async def noop(*_args, **_kwargs) -> None:
        return None

    monkeypatch.setattr(manager, "_run_plugin_install", noop)
    monkeypatch.setattr(manager, "_reconcile_runtime_metadata", noop)
    monkeypatch.setattr(manager, "_invalidate_generation_caches", noop)
    monkeypatch.setattr(manager, "_persist_index", lambda: None)

    operation = await manager._reload_units({"shared_mutator"})

    assert operation.mode is ApplyMode.FAILED
    assert operation.reason == "shared_dependency_global_mutation"
    assert operation.rollback_state == "semantic"
    assert shared.BACKEND == "old"
    assert sys.modules["shared_mutator"] is old_module
    assert not manager.units["shared_mutator"].draining


@pytest.mark.asyncio
async def test_plugin_driver_hook_failure_is_isolated(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from zhenxun.services.runtime_reload.manager import PluginRuntimeManager
    from zhenxun.services.runtime_reload.ownership import resource_context

    class Driver:
        def __init__(self) -> None:
            self.hooks = {}

        def _register(self, name, func):
            self.hooks[name] = func
            return func

        def on_shutdown(self, func):
            return self._register("on_shutdown", func)

    driver = Driver()
    manager = PluginRuntimeManager()
    monkeypatch.setattr(manager, "_isolate_plugin_hook_error", lambda *_args: True)
    manager._install_driver_hook_tracking(driver)

    with resource_context("demo"):

        @driver.on_shutdown
        async def broken() -> None:
            raise RuntimeError("third-party shutdown failed")

    await driver.hooks["on_shutdown"]()


@pytest.mark.asyncio
async def test_core_driver_hook_failure_is_not_suppressed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from zhenxun.services.runtime_reload.manager import PluginRuntimeManager
    from zhenxun.services.runtime_reload.ownership import resource_context

    class Driver:
        def __init__(self) -> None:
            self.hook = None

        def on_shutdown(self, func):
            self.hook = func
            return func

    driver = Driver()
    manager = PluginRuntimeManager()
    monkeypatch.setattr(manager, "_isolate_plugin_hook_error", lambda *_args: False)
    manager._install_driver_hook_tracking(driver)

    with resource_context("core"):

        @driver.on_shutdown
        async def broken() -> None:
            raise RuntimeError("core shutdown failed")

    with pytest.raises(RuntimeError, match="core shutdown failed"):
        await driver.hook()


@pytest.mark.asyncio
async def test_hot_unload_runs_shutdown_before_cancelling_owned_tasks(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import zhenxun.services.runtime_reload.manager as manager_module
    from zhenxun.services.runtime_reload.manager import PluginRuntimeManager
    from zhenxun.services.runtime_reload.models import PluginUnit

    manager = PluginRuntimeManager()
    source = tmp_path / "plugin.py"
    source.write_text("VALUE = 1\n", encoding="utf-8")
    unit = PluginUnit(
        "fixture",
        "fixture",
        object(),
        source,
        module_names=set(),
        files={source},
    )
    manager.units = {"fixture": unit}
    stop = asyncio.Event()

    async def worker() -> None:
        await stop.wait()

    task = asyncio.create_task(worker())
    await asyncio.sleep(0)
    manager._owned_tasks["fixture"].add(task)

    async def shutdown(_module_names: set[str]) -> None:
        assert not task.cancelled()
        stop.set()
        await task

    monkeypatch.setattr(manager, "_run_reload_shutdown_hooks", shutdown)
    monkeypatch.setattr(manager_module, "get_loaded_plugins", lambda: set())
    monkeypatch.setattr(manager_module, "clean_matchers", lambda _items: None)
    monkeypatch.setattr(manager_module, "remove_processors", lambda _items: None)
    monkeypatch.setattr(manager_module, "remove_driver_hooks", lambda *_args: None)
    monkeypatch.setattr(manager_module, "remove_priority_hooks", lambda _items: None)
    monkeypatch.setattr(manager_module, "remove_plugin_init", lambda _items: None)
    monkeypatch.setattr(manager_module, "remove_plugins", lambda _items: None)
    monkeypatch.setattr(manager_module, "remove_nested_managers", lambda _items: None)
    monkeypatch.setattr(manager, "_remove_scheduler_jobs", lambda _items: None)
    monkeypatch.setattr(manager, "_remove_config_registrations", lambda _item: None)
    monkeypatch.setattr(manager, "_remove_trie_entries", lambda _item: None)
    monkeypatch.setattr(manager_module.nonebot, "get_driver", lambda: object())

    await manager._drain_and_unload(unit)

    assert task.done()
    assert not task.cancelled()


@pytest.mark.asyncio
async def test_new_safe_plugin_is_loaded_without_restart(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import nonebot

    from zhenxun.services.runtime_reload.manager import PluginRuntimeManager
    from zhenxun.services.runtime_reload.models import ApplyMode, PluginUnit

    manager = PluginRuntimeManager()
    manager.enabled = True
    source = tmp_path / "demo.py"
    source.write_text("from nonebot import on_command\non_command('demo')\n")
    loaded_plugin = SimpleNamespace(id_="demo", module_name="demo", manager=object())
    loaded_unit = PluginUnit(
        "demo", "demo", loaded_plugin.manager, source, files={source}
    )
    calls: list[str] = []

    def load_plugin(module_name: str):
        calls.append(f"load:{module_name}")
        return loaded_plugin

    def discover_loaded_plugins() -> None:
        manager.units = {"demo": loaded_unit}
        manager.module_to_unit = {"demo": "demo"}

    async def run_install(module_name: str) -> None:
        calls.append(f"install:{module_name}")

    async def run_startup(affected: set[str]) -> None:
        calls.append(f"startup:{','.join(sorted(affected))}")

    async def noop() -> None:
        return None

    monkeypatch.setattr(nonebot, "load_plugin", load_plugin)
    monkeypatch.setattr(manager, "discover_loaded_plugins", discover_loaded_plugins)
    monkeypatch.setattr(manager, "_run_plugin_install", run_install)
    monkeypatch.setattr(manager, "_run_reload_startup_hooks", run_startup)
    monkeypatch.setattr(manager, "_reconcile_runtime_metadata", noop)
    monkeypatch.setattr(manager, "_invalidate_generation_caches", noop)
    monkeypatch.setattr(manager, "_persist_index", lambda: None)

    operation = await manager.load_new_plugin("demo", source, {source})

    assert operation.mode is ApplyMode.HOT_RELOADED
    assert operation.generation == 1
    assert calls == ["load:demo", "install:demo", "startup:demo"]


@pytest.mark.asyncio
async def test_real_nonebot_plugin_load_and_reload_keeps_matcher_count_stable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import nonebot
    from nonebot.matcher import matchers
    import nonebot.plugin as plugin_module

    from zhenxun.services.runtime_reload.manager import PluginRuntimeManager
    from zhenxun.services.runtime_reload.models import ApplyMode

    module_name = "runtime_hot_reload_fixture"
    source = tmp_path / f"{module_name}.py"
    source.write_text(
        "from nonebot import get_app, on_command\n"
        "runtime_matcher = on_command('runtime-hot-reload-fixture')\n"
        "@get_app().get('/runtime-hot-reload-fixture')\n"
        "async def runtime_route(): return {'ok': True}\n",
        encoding="utf-8",
    )
    monkeypatch.syspath_prepend(str(tmp_path))
    manager = PluginRuntimeManager()
    manager.enabled = True
    monkeypatch.setattr(manager, "_persist_index", lambda: None)
    before_count = sum(len(items) for items in matchers.values())
    before_route_count = len(nonebot.get_app().routes)

    try:
        installed = await manager.load_new_plugin(module_name, source, {source})
        after_install_count = sum(len(items) for items in matchers.values())
        after_install_routes = len(nonebot.get_app().routes)
        reloaded = await manager.reload_plugin(module_name)
        after_reload_count = sum(len(items) for items in matchers.values())
        after_reload_routes = len(nonebot.get_app().routes)

        assert installed.mode is ApplyMode.HOT_RELOADED
        assert reloaded.mode is ApplyMode.HOT_RELOADED
        assert after_install_count == before_count + 1
        assert after_reload_count == after_install_count
        assert after_install_routes == before_route_count + 1
        assert after_reload_routes == after_install_routes
    finally:
        if unit := manager._find_unit(module_name):
            await manager._drain_and_unload(unit)
            if unit.manager in plugin_module._managers:
                plugin_module._managers.remove(unit.manager)
    assert len(nonebot.get_app().routes) == before_route_count


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("filename", "source", "expected_reason"),
    [
        ("requirements.txt", "example-package\n", "dependencies_changed"),
        (
            "plugin.py",
            "from fastapi import APIRouter\n"
            "router = APIRouter()\n"
            "router.mount('/demo', object())\n",
            "asgi_root_registration",
        ),
    ],
)
async def test_new_plugin_hard_boundaries_request_restart(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    filename: str,
    source: str,
    expected_reason: str,
) -> None:
    from zhenxun.services.runtime_reload.manager import PluginRuntimeManager
    from zhenxun.services.runtime_reload.models import ApplyMode, RuntimeOperation

    manager = PluginRuntimeManager()
    manager.enabled = True
    plugin_root = tmp_path / "demo"
    plugin_root.mkdir()
    path = plugin_root / filename
    path.write_text(source, encoding="utf-8")
    if filename == "requirements.txt":
        (plugin_root / "__init__.py").write_text("VALUE = 1\n", encoding="utf-8")

    async def request_restart(_affected: set[str], reason: str) -> RuntimeOperation:
        return RuntimeOperation(
            ApplyMode.RESTART_PENDING, "pending_restart", [], reason
        )

    async def request_dependency_restart(_changed: set[Path]) -> RuntimeOperation:
        return RuntimeOperation(
            ApplyMode.RESTART_PENDING,
            "pending_restart",
            [],
            "dependencies_changed",
        )

    monkeypatch.setattr(manager, "request_restart", request_restart)
    monkeypatch.setattr(
        manager, "request_dependency_restart", request_dependency_restart
    )

    operation = await manager.load_new_plugin("demo", plugin_root, {path})

    assert operation.mode is ApplyMode.RESTART_PENDING
    assert operation.reason == expected_reason
