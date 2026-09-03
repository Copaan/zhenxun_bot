from __future__ import annotations

import asyncio
import json
from types import SimpleNamespace

import pytest


def test_startup_elapsed_time_freezes_after_final_state(monkeypatch) -> None:
    from zhenxun.services import startup

    now = {"value": 10.0}
    monkeypatch.setattr(startup.time, "monotonic", lambda: now["value"])
    coordinator = startup.StartupCoordinator()
    now["value"] = 12.0
    coordinator.begin_stage("warmup")
    now["value"] = 15.0
    coordinator.finish_stage("warmup")
    completed = coordinator.snapshot()["elapsed_ms"]
    now["value"] = 50.0

    assert completed == 5000.0
    assert coordinator.snapshot()["elapsed_ms"] == completed


def test_startup_coordinator_records_stage_durations(monkeypatch) -> None:
    from zhenxun.services.startup import StartupCoordinator

    coordinator = StartupCoordinator()
    monkeypatch.setattr(coordinator, "persist", lambda: None)

    coordinator.begin_stage("management")
    coordinator.finish_stage("management")
    coordinator.begin_stage("runtime")
    coordinator.finish_stage("runtime")
    coordinator.begin_stage("warmup")
    coordinator.finish_stage("warmup")

    snapshot = coordinator.snapshot()
    assert snapshot["state"] == "warmup_ready"
    assert snapshot["stages"]["management"]["duration_ms"] >= 0
    assert snapshot["stages"]["runtime"]["state"] == "completed"


@pytest.mark.asyncio
async def test_server_bound_waiter_is_released(monkeypatch) -> None:
    from zhenxun.services.startup import StartupCoordinator

    coordinator = StartupCoordinator()
    monkeypatch.setattr(coordinator, "persist", lambda: None)
    waiter = asyncio.create_task(coordinator.wait_server_bound())
    await asyncio.sleep(0)

    coordinator.mark_server_bound()

    await asyncio.wait_for(waiter, timeout=1)


@pytest.mark.asyncio
async def test_final_available_waits_for_warmup(monkeypatch) -> None:
    from zhenxun.services.startup import StartupCoordinator

    coordinator = StartupCoordinator()
    monkeypatch.setattr(coordinator, "persist", lambda: None)
    waiter = asyncio.create_task(coordinator.wait_final_available())

    coordinator.begin_stage("runtime")
    coordinator.finish_stage("runtime")
    await asyncio.sleep(0)
    assert not waiter.done()

    coordinator.begin_stage("warmup")
    coordinator.finish_stage("warmup")
    assert await asyncio.wait_for(waiter, timeout=1) is True


@pytest.mark.asyncio
async def test_final_available_accepts_degraded_warmup(monkeypatch) -> None:
    from zhenxun.services.startup import StartupCoordinator

    coordinator = StartupCoordinator()
    monkeypatch.setattr(coordinator, "persist", lambda: None)
    coordinator.begin_stage("runtime")
    coordinator.finish_stage("runtime")
    coordinator.record_error("warmup", "optional_service_failed")
    coordinator.begin_stage("warmup")
    coordinator.finish_stage("warmup")

    assert await asyncio.wait_for(coordinator.wait_final_available(), timeout=1)
    assert coordinator.state == "degraded"


@pytest.mark.asyncio
async def test_final_available_rejects_runtime_failure(monkeypatch) -> None:
    from zhenxun.services.startup import StartupCoordinator

    coordinator = StartupCoordinator()
    monkeypatch.setattr(coordinator, "persist", lambda: None)
    coordinator.begin_stage("runtime")
    coordinator.fail_stage("runtime", "runtime_failed")

    assert not await asyncio.wait_for(coordinator.wait_final_available(), timeout=1)
    snapshot = coordinator.snapshot()
    assert snapshot["operating_mode"] == "management_only"
    assert snapshot["accepts_bot_events"] is False
    assert snapshot["failure_terminal"] is True


@pytest.mark.asyncio
async def test_lifecycle_hooks_are_serial_unless_explicitly_parallel(
    monkeypatch,
) -> None:
    from zhenxun.utils.enum import PriorityLifecycleType
    from zhenxun.utils.manager.priority_manager import (
        HookSpec,
        PriorityLifecycle,
        _run_stage,
    )

    calls: list[str] = []

    async def first() -> None:
        calls.append("first:start")
        await asyncio.sleep(0)
        calls.append("first:end")

    async def second() -> None:
        calls.append("second")

    monkeypatch.setattr(
        PriorityLifecycle,
        "_data",
        {PriorityLifecycleType.STARTUP: {5: [first, second]}},
    )
    monkeypatch.setattr(
        PriorityLifecycle,
        "_metadata",
        {first: HookSpec(), second: HookSpec()},
    )

    await _run_stage("runtime")

    assert calls == ["first:start", "first:end", "second"]


@pytest.mark.asyncio
async def test_warmup_failure_is_isolated(monkeypatch) -> None:
    from zhenxun.utils.enum import PriorityLifecycleType
    from zhenxun.utils.manager.priority_manager import (
        HookSpec,
        PriorityLifecycle,
        _run_stage,
    )

    calls: list[str] = []

    async def broken() -> None:
        raise RuntimeError("warmup failed")

    async def healthy() -> None:
        calls.append("healthy")

    monkeypatch.setattr(
        PriorityLifecycle,
        "_data",
        {PriorityLifecycleType.STARTUP: {10: [broken, healthy]}},
    )
    monkeypatch.setattr(
        PriorityLifecycle,
        "_metadata",
        {
            broken: HookSpec(stage="warmup", failure_policy="degrade"),
            healthy: HookSpec(stage="warmup", failure_policy="degrade"),
        },
    )

    await _run_stage("warmup")

    assert calls == ["healthy"]


@pytest.mark.asyncio
async def test_renderer_concurrent_warmup_builds_one_generation(monkeypatch) -> None:
    import zhenxun.services.renderer.engine as renderer_engine

    class Page:
        async def goto(self, *_args, **_kwargs):
            return None

        async def set_content(self, *_args, **_kwargs):
            return None

        async def close(self):
            return None

    class Context:
        async def new_page(self):
            return Page()

        async def close(self):
            return None

    class Browser:
        calls = 0

        async def new_context(self, **_kwargs):
            self.calls += 1
            return Context()

    browser = Browser()

    async def get_browser():
        return browser

    monkeypatch.setattr(renderer_engine, "_get_browser_instance", get_browser)
    monkeypatch.setattr(
        renderer_engine, "_shutdown_browser_instance", lambda: asyncio.sleep(0)
    )
    engine = renderer_engine.PlaywrightEngine()
    await engine.initialize()

    await asyncio.gather(engine.warmup(), engine.warmup())

    snapshot = await engine.get_runtime_snapshot()
    assert browser.calls == engine._PREWARM_CONTEXT_COUNT
    assert snapshot["active_generation"]["generation_id"] == 1
    await engine.close()


@pytest.mark.parametrize(
    ("state", "expected"),
    [
        ("management_ready", False),
        ("runtime_ready", True),
        ("warmup_ready", True),
        ("degraded", True),
        ("failed", False),
    ],
)
def test_launcher_runtime_readiness_uses_startup_state(
    monkeypatch, state: str, expected: bool
) -> None:
    import zhenxun.cli as cli

    class Response:
        status = 200

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return None

        def read(self):
            return json.dumps({"data": {"state": state}}).encode()

    monkeypatch.setattr(cli, "_health_urlopen", lambda _url: Response())
    settings = SimpleNamespace(worker_connect_host="127.0.0.1", worker_port=8080)

    assert cli._worker_is_ready(settings) is expected


@pytest.mark.parametrize(
    ("state", "warmup_state", "expected"),
    [
        ("runtime_ready", None, False),
        ("warmup_ready", "completed", True),
        ("degraded", "completed", True),
        ("degraded", "failed", True),
        ("failed", None, False),
    ],
)
def test_launcher_plugin_verification_waits_for_warmup(
    monkeypatch, state: str, warmup_state: str | None, expected: bool
) -> None:
    import zhenxun.cli as cli

    class Response:
        status = 200

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return None

        def read(self):
            stages = {"warmup": {"state": warmup_state}} if warmup_state else {}
            return json.dumps({"data": {"state": state, "stages": stages}}).encode()

    monkeypatch.setattr(cli, "_health_urlopen", lambda _url: Response())
    settings = SimpleNamespace(worker_connect_host="127.0.0.1", worker_port=8080)

    assert cli._worker_is_ready(settings, require_warmup=True) is expected


def test_launcher_stops_waiting_for_management_only_worker(monkeypatch) -> None:
    import zhenxun.cli as cli

    worker = SimpleNamespace(poll=lambda: None)
    settings = SimpleNamespace(worker_connect_host="127.0.0.1", worker_port=8080)
    status = {
        "state": "failed",
        "operating_mode": "management_only",
        "accepts_bot_events": False,
        "pid": 4321,
        "boot_id": "worker-boot",
    }
    observed = []
    monkeypatch.setattr(cli, "_read_worker_runtime_status", lambda *_a, **_k: status)
    monkeypatch.setattr(
        cli,
        "_bind_worker_runtime_status",
        lambda _worker, value: observed.append(value),
    )

    assert not cli._wait_worker_ready(worker, settings)
    assert observed == [status]
