from __future__ import annotations

import asyncio

import pytest


@pytest.fixture
def isolated_startup_hooks(monkeypatch):
    from zhenxun.utils.manager.priority_manager import PriorityLifecycle

    monkeypatch.setattr(PriorityLifecycle, "_data", {})
    monkeypatch.setattr(PriorityLifecycle, "_metadata", {})
    return PriorityLifecycle


@pytest.mark.asyncio
async def test_parallel_hooks_keep_priority_barrier(isolated_startup_hooks) -> None:
    from zhenxun.utils.manager.priority_manager import _run_stage

    lifecycle = isolated_startup_hooks
    completed: list[str] = []

    @lifecycle.on_startup(
        priority=0,
        parallel_safe=True,
        task_id="slow",
        resource_group="slow",
    )
    async def slow() -> None:
        await asyncio.sleep(0.04)
        completed.append("slow")

    @lifecycle.on_startup(
        priority=0,
        parallel_safe=True,
        task_id="fast",
        resource_group="fast",
    )
    async def fast() -> None:
        await asyncio.sleep(0.005)
        completed.append("fast")

    @lifecycle.on_startup(
        priority=1,
        parallel_safe=True,
        task_id="later",
        resource_group="later",
    )
    async def later() -> None:
        assert "slow" in completed
        completed.append("later")

    await _run_stage("runtime")

    assert completed[-1] == "later"


@pytest.mark.asyncio
async def test_resource_group_serializes_parallel_hooks(
    isolated_startup_hooks,
) -> None:
    from zhenxun.utils.manager.priority_manager import _run_stage

    lifecycle = isolated_startup_hooks
    active = 0
    maximum_active = 0

    async def work() -> None:
        nonlocal active, maximum_active
        active += 1
        maximum_active = max(maximum_active, active)
        await asyncio.sleep(0.01)
        active -= 1

    lifecycle.add(
        hook_type=__import__(
            "zhenxun.utils.enum", fromlist=["PriorityLifecycleType"]
        ).PriorityLifecycleType.STARTUP,
        func=work,
        priority=0,
        parallel_safe=True,
        task_id="one",
        resource_group="shared",
    )
    lifecycle.add(
        hook_type=__import__(
            "zhenxun.utils.enum", fromlist=["PriorityLifecycleType"]
        ).PriorityLifecycleType.STARTUP,
        func=work,
        priority=0,
        parallel_safe=True,
        task_id="two",
        resource_group="shared",
    )

    await _run_stage("runtime")

    assert maximum_active == 1


@pytest.mark.asyncio
async def test_task_dependency_controls_execution(isolated_startup_hooks) -> None:
    from zhenxun.utils.enum import PriorityLifecycleType
    from zhenxun.utils.manager.priority_manager import _run_stage

    lifecycle = isolated_startup_hooks
    completed: list[str] = []

    async def first() -> None:
        completed.append("first")

    async def second() -> None:
        completed.append("second")

    lifecycle.add(
        PriorityLifecycleType.STARTUP,
        second,
        priority=0,
        task_id="second",
        depends_on=("first",),
    )
    lifecycle.add(
        PriorityLifecycleType.STARTUP,
        first,
        priority=1,
        task_id="first",
    )

    await _run_stage("runtime")

    assert completed == ["first", "second"]


@pytest.mark.asyncio
async def test_priority_lifecycle_passes_context_and_health_contract(
    isolated_startup_hooks,
) -> None:
    from zhenxun.services.lifecycle import lifecycle_kernel
    from zhenxun.utils.manager.priority_manager import _run_stage

    lifecycle = isolated_startup_hooks

    @lifecycle.on_startup(
        priority=0,
        task_id="context-health-contract",
        pass_context=True,
        health=lambda _value: True,
    )
    async def start(context) -> None:
        context.own_resource(
            receipt_id="fixture:resource",
            provider="fixture",
            resource_type="handle",
        )

    await _run_stage("runtime")
    await lifecycle_kernel.check_health()

    status = lifecycle_kernel.component_status("context-health-contract")
    assert status is not None
    assert status["health"] == "healthy"
    assert status["resource_counts"] == {"fixture:handle:active": 1}
    await lifecycle_kernel.stop_components({"context-health-contract"})


@pytest.mark.asyncio
async def test_run_hook_awaits_awaitable_from_sync_wrapper() -> None:
    from zhenxun.utils.manager.priority_manager import _run_hook

    calls: list[str] = []

    async def async_hook() -> None:
        calls.append("awaited")

    def sync_wrapper():
        return async_hook()

    await _run_hook(sync_wrapper, 0)

    assert calls == ["awaited"]


@pytest.mark.asyncio
async def test_runtime_executor_remains_available_until_explicit_finalize(
    monkeypatch,
) -> None:
    from concurrent.futures import ThreadPoolExecutor

    from zhenxun.services import runtime_bootstrap

    executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="late-shutdown")
    monkeypatch.setattr(runtime_bootstrap, "_thread_executor", executor)

    assert await asyncio.get_running_loop().run_in_executor(
        executor, lambda: "late-shutdown-hook"
    ) == ("late-shutdown-hook")

    runtime_bootstrap.finalize_runtime_executor()

    assert runtime_bootstrap._thread_executor is None
    with pytest.raises(RuntimeError, match="cannot schedule new futures"):
        executor.submit(lambda: None)


@pytest.mark.asyncio
async def test_priority_adapter_preserves_composite_runtime_handle(
    isolated_startup_hooks,
) -> None:
    from zhenxun.services.lifecycle import RuntimeHandle, lifecycle_kernel
    from zhenxun.utils.manager.priority_manager import _run_stage

    class Handle:
        async def quiesce(self) -> None:
            return None

        async def close(self) -> None:
            return None

        def health(self):
            return {"healthy": True}

        def snapshot(self):
            return {"composite": True}

        def resource_snapshot(self):
            return []

    @isolated_startup_hooks.on_startup(
        priority=0,
        component_id="composite-priority-adapter",
        pass_context=True,
    )
    async def start(_context):
        return RuntimeHandle(controller=Handle())

    await _run_stage("runtime")

    status = lifecycle_kernel.component_status("composite-priority-adapter")
    assert status is not None
    assert status["metadata"]["handle"] == {"composite": True}
    await lifecycle_kernel.stop_components({"composite-priority-adapter"})


def test_runtime_concurrency_limits_scale_with_cpu(monkeypatch) -> None:
    from zhenxun.services import runtime_bootstrap

    monkeypatch.setattr(runtime_bootstrap.os, "cpu_count", lambda: 2)
    workers = runtime_bootstrap._get_executor_workers()
    assert workers == 8
    assert runtime_bootstrap._get_anyio_tokens(workers) == 16

    monkeypatch.setattr(runtime_bootstrap.os, "cpu_count", lambda: 128)
    workers = runtime_bootstrap._get_executor_workers()
    assert workers == 32
    assert runtime_bootstrap._get_anyio_tokens(workers) == 64
