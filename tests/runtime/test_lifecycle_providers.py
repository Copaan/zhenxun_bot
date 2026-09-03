from __future__ import annotations

import asyncio
import os
import threading
from types import SimpleNamespace

import pytest

from zhenxun.services.lifecycle.providers import (
    ProviderContainerSnapshot,
    RuntimeProviderSnapshot,
)


def test_provider_snapshot_restores_nested_matcher_mapping() -> None:
    first = object()
    added = object()
    registry = {1: [first]}
    snapshot = RuntimeProviderSnapshot(
        [ProviderContainerSnapshot("matchers", registry, "mapping", {1: [first]})]
    )
    registry[1].append(added)
    registry[2] = [object()]

    receipts = snapshot.receipts("plugin", "incarnation")
    snapshot.rollback()

    assert registry == {1: [first]}
    assert {receipt.provider for receipt in receipts} == {"matchers"}
    assert all(receipt.incarnation_id == "incarnation" for receipt in receipts)


def test_provider_rollback_does_not_replace_existing_container_objects() -> None:
    first = object()
    added = object()
    registry = {1: [first]}
    original_list = registry[1]
    snapshot = RuntimeProviderSnapshot(
        [ProviderContainerSnapshot("matchers", registry, "mapping", {1: [first]})]
    )
    registry[1].append(added)

    snapshot.rollback()

    assert registry[1] is original_list
    assert registry == {1: [first]}


def test_provider_rollback_restores_removed_objects_by_identity() -> None:
    first = object()
    second = object()
    registry = {1: [first, second]}
    original_list = registry[1]
    snapshot = RuntimeProviderSnapshot(
        [
            ProviderContainerSnapshot(
                "matchers", registry, "mapping", {1: [first, second]}
            )
        ]
    )
    registry[1].remove(first)

    snapshot.rollback()

    assert registry[1] is original_list
    assert registry == {1: [first, second]}


def test_provider_rollback_preserves_additions_after_undo_is_sealed() -> None:
    first = object()
    plugin_added = object()
    concurrent = object()
    registry = {1: [first]}
    snapshot = RuntimeProviderSnapshot(
        [ProviderContainerSnapshot("matchers", registry, "mapping", {1: [first]})]
    )
    registry[1].append(plugin_added)
    snapshot.receipts("plugin", "incarnation")
    registry[1].append(concurrent)

    snapshot.rollback()

    assert registry == {1: [first, concurrent]}


def test_hook_kernel_canary_and_uninstall_restore_global_entrypoints() -> None:
    import nonebot
    from nonebot.matcher import Matcher

    from zhenxun.configs.config import Config
    from zhenxun.services.lifecycle import HookKernel
    from zhenxun.services.runtime_reload.manager import PluginRuntimeManager

    manager = PluginRuntimeManager()
    kernel = HookKernel()
    original_matcher_run = Matcher.run
    original_matcher_new = Matcher.new.__func__
    original_get_config = Config.get_config
    original_getenv = os.getenv
    original_require = nonebot.require
    original_thread_start = threading.Thread.start

    try:
        kernel.install(manager)

        assert kernel.installed
        assert manager.enabled
        assert Matcher.run is not original_matcher_run
        assert Matcher.new.__func__ is not original_matcher_new
        assert Config.get_config is not original_get_config
        assert os.getenv is not original_getenv
        assert nonebot.require is not original_require
        assert threading.Thread.start is not original_thread_start
    finally:
        kernel.uninstall(manager)

    assert not kernel.installed
    assert Matcher.run is original_matcher_run
    assert Matcher.new.__func__ is original_matcher_new
    assert Config.get_config == original_get_config
    assert os.getenv is original_getenv
    assert nonebot.require is original_require
    assert threading.Thread.start is original_thread_start


def test_import_owner_precedes_outer_callback_owner() -> None:
    from nonebot.plugin import _current_plugin

    from zhenxun.services.runtime_reload.ownership import current_owner, owner_context

    token = _current_plugin.set(SimpleNamespace(id_="nested-plugin"))
    try:
        with owner_context("requesting-plugin"):
            assert current_owner() == "nested-plugin"
    finally:
        _current_plugin.reset(token)


@pytest.mark.asyncio
async def test_revoked_incarnation_cannot_run_late_timer() -> None:
    from zhenxun.services.lifecycle import LeaseState
    from zhenxun.services.runtime_reload.manager import PluginRuntimeManager
    from zhenxun.services.runtime_reload.ownership import owner_context

    manager = PluginRuntimeManager()
    incarnation = manager._new_incarnation("timer-plugin")
    incarnation.lease_state = LeaseState.ACTIVE
    called: list[bool] = []
    manager._install_task_factory()
    try:
        with owner_context("timer-plugin"):
            asyncio.get_running_loop().call_later(0.01, called.append, True)
        manager._revoke_incarnation("timer-plugin")
        await asyncio.sleep(0.03)
        assert called == []
    finally:
        await manager._cancel_all_owned_tasks()
        manager._restore_task_factory()


def test_thread_callback_inherits_plugin_resource_owner() -> None:
    from zhenxun.services.lifecycle import LeaseState
    from zhenxun.services.runtime_reload.manager import PluginRuntimeManager
    from zhenxun.services.runtime_reload.ownership import current_owner, owner_context

    manager = PluginRuntimeManager()
    incarnation = manager._new_incarnation("thread-plugin")
    incarnation.lease_state = LeaseState.ACTIVE
    observed: list[str | None] = []
    manager._install_thread_process_tracking()
    try:
        with owner_context("thread-plugin"):
            thread = threading.Thread(target=lambda: observed.append(current_owner()))
            thread.start()
        thread.join(timeout=2)
        assert observed == ["thread-plugin"]
    finally:
        manager._restore_thread_process_tracking()


@pytest.mark.asyncio
async def test_running_executor_work_becomes_restart_boundary(tmp_path) -> None:
    from zhenxun.services.lifecycle import LeaseState
    from zhenxun.services.runtime_reload.manager import PluginRuntimeManager
    from zhenxun.services.runtime_reload.models import (
        PluginUnit,
        ReloadClassification,
    )
    from zhenxun.services.runtime_reload.ownership import owner_context

    manager = PluginRuntimeManager()
    manager.units["executor-plugin"] = PluginUnit(
        plugin_id="executor-plugin",
        module_name="executor-plugin",
        manager=None,
        root=tmp_path,
    )
    incarnation = manager._new_incarnation("executor-plugin")
    incarnation.lease_state = LeaseState.ACTIVE
    release = threading.Event()
    manager._install_task_factory()
    try:
        with owner_context("executor-plugin"):
            future = asyncio.get_running_loop().run_in_executor(None, release.wait)
        await asyncio.sleep(0.01)
        manager._collect_runtime_boundaries()
        assert (
            manager.units["executor-plugin"].classification
            is ReloadClassification.RESTART_REQUIRED
        )
        assert "executor_work_in_flight" in manager.units["executor-plugin"].reasons
        release.set()
        await future
    finally:
        release.set()
        await manager._cancel_all_owned_tasks()
        manager._restore_task_factory()
