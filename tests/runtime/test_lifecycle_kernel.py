from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager

import pytest

from zhenxun.services.lifecycle import (
    ComponentSpec,
    ComponentState,
    LifecycleError,
    LifecycleKernel,
    ResourceReceipt,
    RuntimeHandle,
)


@pytest.mark.asyncio
async def test_lifecycle_kernel_starts_topologically_and_stops_in_reverse() -> None:
    kernel = LifecycleKernel()
    calls: list[str] = []

    async def start_a() -> None:
        calls.append("start:a")

    async def stop_a() -> None:
        calls.append("stop:a")

    async def start_b() -> None:
        calls.append("start:b")

    async def stop_b() -> None:
        calls.append("stop:b")

    kernel.register(ComponentSpec("a", stage="management"), start_a, stop=stop_a)
    kernel.register(
        ComponentSpec("b", stage="management", depends_on=("a",)),
        start_b,
        stop=stop_b,
    )

    await kernel.start_stage("management")
    await kernel.stop_all()

    assert calls == ["start:a", "start:b", "stop:b", "stop:a"]
    assert kernel.component_status("a")["state"] == "stopped"  # type: ignore[index]
    assert kernel.component_status("b")["state"] == "stopped"  # type: ignore[index]


@pytest.mark.asyncio
async def test_lifecycle_kernel_rolls_back_partial_fatal_start() -> None:
    kernel = LifecycleKernel()
    calls: list[str] = []

    async def start_ok() -> None:
        calls.append("start:ok")

    async def stop_ok() -> None:
        calls.append("stop:ok")

    async def start_failed() -> None:
        raise RuntimeError("failed")

    kernel.register(ComponentSpec("ok", stage="runtime"), start_ok, stop=stop_ok)
    kernel.register(
        ComponentSpec("failed", stage="runtime", depends_on=("ok",)),
        start_failed,
    )

    with pytest.raises(RuntimeError, match="failed"):
        await kernel.start_stage("runtime")

    assert calls == ["start:ok", "stop:ok"]
    assert kernel.component_status("ok")["state"] == "stopped"  # type: ignore[index]
    assert kernel.component_status("failed")["state"] == "failed"  # type: ignore[index]


@pytest.mark.asyncio
async def test_never_started_failed_component_does_not_run_stop_callback() -> None:
    kernel = LifecycleKernel()
    stops = 0

    async def failed_start() -> None:
        raise RuntimeError("start failed")

    async def stop() -> None:
        nonlocal stops
        stops += 1

    kernel.register(ComponentSpec("never-started"), failed_start, stop=stop)
    with pytest.raises(RuntimeError, match="start failed"):
        await kernel.start_stage("runtime")

    await kernel.stop_all()

    assert stops == 0
    assert kernel.component_status("never-started")["state"] == "failed"


@pytest.mark.asyncio
async def test_lifecycle_kernel_blocks_dependents_of_degraded_component() -> None:
    kernel = LifecycleKernel()
    calls: list[str] = []

    async def broken() -> None:
        raise RuntimeError("optional")

    async def dependent() -> None:
        calls.append("dependent")

    kernel.register(
        ComponentSpec(
            "optional",
            stage="warmup",
            failure_policy="degrade",
        ),
        broken,
    )
    kernel.register(
        ComponentSpec(
            "dependent",
            stage="warmup",
            depends_on=("optional",),
            failure_policy="degrade",
        ),
        dependent,
    )

    await kernel.start_stage("warmup")

    assert calls == []
    assert kernel.component_status("optional")["state"] == "degraded"  # type: ignore[index]
    assert kernel.component_status("dependent")["state"] == "degraded"  # type: ignore[index]


@pytest.mark.asyncio
async def test_lifecycle_context_cancels_owned_tasks_and_runs_finalizers() -> None:
    kernel = LifecycleKernel()
    stopped = asyncio.Event()
    finalized: list[str] = []

    async def start(context) -> None:
        async def worker() -> None:
            try:
                await asyncio.Event().wait()
            finally:
                stopped.set()

        context.spawn_task(worker(), name="lifecycle-test-worker")
        context.add_finalizer(finalized.append, "done")

    kernel.register(
        ComponentSpec("owned", stage="runtime"),
        start,
        pass_context=True,
    )

    await kernel.start_stage("runtime")
    await kernel.stop_all()

    assert stopped.is_set()
    assert finalized == ["done"]
    assert kernel.component_status("owned")["resource_count"] == 1  # type: ignore[index]


@pytest.mark.asyncio
async def test_supervised_task_failure_updates_component_and_receipt() -> None:
    kernel = LifecycleKernel()

    async def start(context) -> None:
        async def broken() -> None:
            await asyncio.sleep(0)
            raise RuntimeError("background failed")

        context.spawn_task(broken(), name="broken-background")

    kernel.register(
        ComponentSpec("supervised", stage="runtime", failure_policy="degrade"),
        start,
        pass_context=True,
    )
    await kernel.start_stage("runtime")
    await asyncio.sleep(0.01)

    status = kernel.component_status("supervised")
    assert status is not None
    assert status["state"] == "degraded"
    assert status["consecutive_health_failures"] == 1
    assert status["resource_counts"] == {"asyncio:task:failed": 1}
    await kernel.stop_all()


@pytest.mark.asyncio
async def test_config_stop_failure_does_not_rollback_unapplied_change() -> None:
    kernel = LifecycleKernel()
    calls: list[str] = []

    async def stop() -> None:
        raise RuntimeError("cannot stop")

    kernel.register(
        ComponentSpec(
            "component",
            stage="runtime",
            restart_policy="component",
            config_keys=("VALUE",),
        ),
        lambda: None,
        stop=stop,
    )
    await kernel.start_stage("runtime")

    result = await kernel.restart_for_config(
        {"VALUE"},
        apply_change=lambda: calls.append("apply"),
        rollback_change=lambda: calls.append("rollback"),
    )

    assert calls == []
    assert result.apply_effect == "rolled_back"


@pytest.mark.asyncio
async def test_component_restart_uses_dependency_closure() -> None:
    kernel = LifecycleKernel()
    calls: list[str] = []

    def component(name: str):
        async def start() -> None:
            calls.append(f"start:{name}")

        async def stop() -> None:
            calls.append(f"stop:{name}")

        return start, stop

    first_start, first_stop = component("first")
    second_start, second_stop = component("second")
    kernel.register(
        ComponentSpec(
            "first",
            stage="management",
            restart_policy="component",
            config_keys=("SYSTEM_PROXY",),
        ),
        first_start,
        stop=first_stop,
    )
    kernel.register(
        ComponentSpec(
            "second",
            stage="management",
            depends_on=("first",),
            restart_policy="component",
        ),
        second_start,
        stop=second_stop,
    )
    await kernel.start_stage("management")
    calls.clear()

    result = await kernel.restart_for_config(
        {"SYSTEM_PROXY"},
        apply_change=lambda: calls.append("apply"),
        rollback_change=lambda: calls.append("rollback"),
    )

    assert result.apply_effect == "component_restarted"
    assert result.affected_components == ["first", "second"]
    assert calls == [
        "stop:second",
        "stop:first",
        "apply",
        "start:first",
        "start:second",
    ]


def test_lifecycle_kernel_rejects_missing_dependencies_and_cycles() -> None:
    missing = LifecycleKernel()
    missing.register(ComponentSpec("a", depends_on=("missing",)), lambda: None)
    with pytest.raises(LifecycleError, match="component_dependency_missing"):
        missing.validate()

    cyclic = LifecycleKernel()
    cyclic.register(ComponentSpec("a", depends_on=("b",)), lambda: None)
    cyclic.register(ComponentSpec("b", depends_on=("a",)), lambda: None)
    with pytest.raises(LifecycleError, match="component_dependency_cycle"):
        cyclic.validate()


def test_lifecycle_kernel_rejects_duplicate_component_ids() -> None:
    kernel = LifecycleKernel()
    kernel.register(ComponentSpec("duplicate"), lambda: None)
    with pytest.raises(LifecycleError, match="component_duplicate"):
        kernel.register(ComponentSpec("duplicate"), lambda: None)


def test_component_state_values_are_stable() -> None:
    assert ComponentState.READY.value == "ready"


def test_external_process_receipt_tracks_launcher_child_lifecycle() -> None:
    kernel = LifecycleKernel()
    kernel.observe_external_process(
        "launcher:worker",
        pid=1234,
        scope="worker",
        metadata={"role": "worker"},
    )

    ready = kernel.component_status("launcher:worker")
    assert ready is not None
    assert ready["state"] == "ready"
    assert ready["resource_counts"] == {"subprocess:process:active": 1}

    kernel.release_external_process(1234, return_code=0, reason="normal")
    stopped = kernel.component_status("launcher:worker")
    assert stopped is not None
    assert stopped["state"] == "stopped"
    assert stopped["resource_counts"] == {"subprocess:process:released": 1}


def test_restart_required_plugin_is_healthy_but_not_hot_reloadable() -> None:
    kernel = LifecycleKernel()
    kernel.observe_plugin_incarnation(
        "route-plugin",
        "incarnation",
        source_digest="abc",
        receipts=[],
        classification="restart_required",
    )

    status = kernel.component_status("plugin:route-plugin")
    assert status is not None
    assert status["state"] == "ready"
    assert status["health"] == "healthy"
    assert status["metadata"]["classification"] == "restart_required"


@pytest.mark.asyncio
async def test_stop_all_releases_observed_plugin_resources() -> None:
    kernel = LifecycleKernel()
    kernel.observe_plugin_incarnation(
        "observed",
        "incarnation",
        source_digest="abc",
        receipts=[
            ResourceReceipt(
                receipt_id="timer",
                provider="asyncio",
                resource_type="timer",
                owner_id="observed",
            )
        ],
        classification="hot_reloadable",
    )

    await kernel.stop_all()

    status = kernel.component_status("plugin:observed")
    assert status is not None
    assert status["resource_counts"] == {"asyncio:timer:released": 1}


def test_launcher_lifecycle_state_path_can_be_isolated(monkeypatch, tmp_path) -> None:
    import importlib

    import zhenxun.services.lifecycle.launcher as launcher

    state_path = tmp_path / "launcher-state.json"
    monkeypatch.setenv("ZHENXUN_LAUNCHER_LIFECYCLE_STATE_PATH", str(state_path))
    launcher = importlib.reload(launcher)

    launcher.initialize_launcher_lifecycle()

    assert state_path.exists()
    assert launcher.launcher_lifecycle_snapshot()["process"]["role"] == "launcher"


@pytest.mark.asyncio
async def test_stop_failure_still_runs_context_finalizers() -> None:
    kernel = LifecycleKernel()
    finalized: list[str] = []

    async def start(context) -> RuntimeHandle:
        context.add_finalizer(finalized.append, "released")
        return RuntimeHandle(value="ready")

    async def stop() -> None:
        raise RuntimeError("stop failed")

    kernel.register(
        ComponentSpec("failing-stop", stage="runtime"),
        start,
        stop=stop,
        pass_context=True,
    )
    await kernel.start_stage("runtime")

    await kernel.stop_all()

    assert finalized == ["released"]
    assert kernel.component_status("failing-stop")["state"] == "failed"  # type: ignore[index]


@pytest.mark.asyncio
async def test_child_scope_closes_before_parent_resources() -> None:
    kernel = LifecycleKernel()
    calls: list[str] = []

    async def start(context) -> None:
        child = context.create_child_scope("task", "consumer")
        child.add_finalizer(calls.append, "child")
        context.add_finalizer(calls.append, "parent")

    kernel.register(
        ComponentSpec("parent", scope="infrastructure", stage="runtime"),
        start,
        pass_context=True,
    )
    await kernel.start_stage("runtime")
    await kernel.stop_all()

    assert calls == ["child", "parent"]


@pytest.mark.asyncio
async def test_task_cancel_timeout_is_bounded_and_marks_receipt_leaked() -> None:
    kernel = LifecycleKernel()
    release = asyncio.Event()
    task: asyncio.Task[None] | None = None

    async def start(context) -> None:
        async def cancellation_resistant() -> None:
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                await release.wait()

        nonlocal task
        task = context.spawn_task(cancellation_resistant(), name="resistant")

    kernel.register(
        ComponentSpec("resistant", cancel_timeout=0.01),
        start,
        pass_context=True,
    )
    await kernel.start_stage("runtime")

    started = asyncio.get_running_loop().time()
    await kernel.stop_all()
    elapsed = asyncio.get_running_loop().time() - started

    status = kernel.component_status("resistant")
    assert elapsed < 0.2
    assert status is not None
    assert status["state"] == "failed"
    assert status["resource_counts"] == {"asyncio:task:leaked": 1}

    release.set()
    assert task is not None
    await task


@pytest.mark.asyncio
async def test_component_restart_does_not_overlap_task_that_refused_cancel() -> None:
    kernel = LifecycleKernel()
    release = asyncio.Event()
    starts = 0
    task: asyncio.Task[None] | None = None

    async def start(context) -> None:
        nonlocal starts, task
        starts += 1

        async def cancellation_resistant() -> None:
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                await release.wait()

        task = context.spawn_task(cancellation_resistant(), name="resistant")

    kernel.register(
        ComponentSpec(
            "resistant-restart",
            restart_policy="component",
            config_keys=("VALUE",),
            cancel_timeout=0.01,
        ),
        start,
        pass_context=True,
    )
    await kernel.start_stage("runtime")

    result = await kernel.restart_for_config(
        {"VALUE"}, apply_change=lambda: None, rollback_change=lambda: None
    )

    assert result.apply_effect == "restart_pending"
    assert result.rollback_state == "worker_recovery_required"
    assert result.reason_codes == [
        "component_restart_failed:component_task_cancel_timeout"
    ]
    assert starts == 1
    release.set()
    assert task is not None
    await task


@pytest.mark.asyncio
async def test_finalizers_are_lifo_and_continue_after_failure() -> None:
    kernel = LifecycleKernel()
    calls: list[str] = []

    async def start(context) -> None:
        context.add_finalizer(calls.append, "first")

        def broken() -> None:
            calls.append("broken")
            raise RuntimeError("finalizer failed")

        context.add_finalizer(broken)
        context.add_finalizer(calls.append, "last")

    kernel.register(ComponentSpec("finalizers"), start, pass_context=True)
    await kernel.start_stage("runtime")
    await kernel.stop_all()

    assert calls == ["last", "broken", "first"]
    assert kernel.component_status("finalizers")["state"] == "failed"  # type: ignore[index]


@pytest.mark.asyncio
async def test_async_context_exit_timeout_is_bounded() -> None:
    kernel = LifecycleKernel()
    release = asyncio.Event()

    @asynccontextmanager
    async def resistant_context():
        try:
            yield
        finally:
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                await release.wait()

    async def start(context) -> None:
        await context.enter_async_context(resistant_context())

    kernel.register(
        ComponentSpec("resistant-context", finalizer_timeout=0.01),
        start,
        pass_context=True,
    )
    await kernel.start_stage("runtime")

    started = asyncio.get_running_loop().time()
    await kernel.stop_all()
    elapsed = asyncio.get_running_loop().time() - started

    assert elapsed < 0.2
    assert kernel.component_status("resistant-context")["state"] == "failed"  # type: ignore[index]
    release.set()
    await asyncio.sleep(0)


@pytest.mark.asyncio
async def test_start_failure_preserves_receipts_when_cleanup_also_fails() -> None:
    kernel = LifecycleKernel()

    async def start(context) -> None:
        context.own_resource(
            receipt_id="partial",
            provider="test",
            resource_type="partial",
        )
        context.add_finalizer(lambda: (_ for _ in ()).throw(RuntimeError("cleanup")))
        raise ValueError("start")

    kernel.register(ComponentSpec("partial"), start, pass_context=True)
    with pytest.raises(ValueError, match="start"):
        await kernel.start_stage("runtime")

    status = kernel.component_status("partial")
    assert status is not None
    assert status["error_code"] == "component_start_failed:ValueError"
    assert status["metadata"] == {
        "cleanup_error_code": "component_start_cleanup_failed:RuntimeError"
    }
    assert status["resource_counts"] == {"test:partial:released": 1}


def test_lifecycle_kernel_rejects_parent_dependency_on_child_scope() -> None:
    kernel = LifecycleKernel()
    kernel.register(ComponentSpec("request", scope="request"), lambda: None)
    kernel.register(
        ComponentSpec("worker", scope="worker", depends_on=("request",)),
        lambda: None,
    )

    with pytest.raises(LifecycleError, match="component_scope_dependency_invalid"):
        kernel.validate()


def test_plugin_incarnation_unload_does_not_grow_component_registry() -> None:
    kernel = LifecycleKernel()
    baseline = kernel.status()["component_count"]

    for index in range(100):
        plugin_id = f"fixture-{index}"
        kernel.observe_plugin_incarnation(
            plugin_id,
            f"incarnation-{index}",
            source_digest="digest",
            receipts=[],
            classification="hot_reloadable",
        )
        kernel.forget_plugin_incarnation(plugin_id)

    assert kernel.status()["component_count"] == baseline
