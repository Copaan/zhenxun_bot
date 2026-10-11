from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
import json

import pytest

from zhenxun.services.lifecycle import (
    ComponentSpec,
    ComponentState,
    LifecycleError,
    LifecycleKernel,
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

    await kernel.start_components({"a", "b"})
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
        await kernel.start_components({"ok", "failed"})

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
        await kernel.start_components({"never-started"})

    await kernel.stop_all()

    assert stops == 0
    assert kernel.component_status("never-started")["state"] == "failed"  # type: ignore[index]


@pytest.mark.asyncio
async def test_lifecycle_kernel_blocks_dependents_of_degraded_component() -> None:
    kernel = LifecycleKernel()
    calls: list[str] = []

    async def broken() -> None:
        raise RuntimeError("optional")

    async def dependent() -> None:
        calls.append("dependent")

    kernel.register(
        ComponentSpec("optional", stage="warmup", failure_policy="degrade"),
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

    await kernel.start_components({"optional", "dependent"})

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

    kernel.register(ComponentSpec("owned"), start, pass_context=True)

    await kernel.start_components({"owned"})
    await kernel.stop_all()

    assert stopped.is_set()
    assert finalized == ["done"]
    assert kernel.component_status("owned")["state"] == "stopped"  # type: ignore[index]


@pytest.mark.asyncio
async def test_background_task_failure_degrades_component() -> None:
    kernel = LifecycleKernel()

    async def start(context) -> None:
        async def broken() -> None:
            await asyncio.sleep(0)
            raise RuntimeError("background failed")

        async def operation() -> None:
            await asyncio.sleep(0)
            raise RuntimeError("operation failed")

        context.spawn_task(broken(), name="broken-background")
        context.spawn_task(operation(), name="operation", degrade_on_error=False)

    kernel.register(
        ComponentSpec("supervised", failure_policy="degrade"),
        start,
        pass_context=True,
    )
    await kernel.start_components({"supervised"})
    await asyncio.sleep(0.01)

    status = kernel.component_status("supervised")
    assert status is not None
    assert status["state"] == "degraded"
    assert status["error_code"] == "task_failed:RuntimeError"
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
            restart_policy="component",
            config_keys=("VALUE",),
        ),
        lambda: None,
        stop=stop,
    )
    await kernel.start_components({"component"})

    result = await kernel.restart_for_config(
        {"VALUE"},
        apply_change=lambda: calls.append("apply"),
        rollback_change=lambda: calls.append("rollback"),
    )

    assert calls == []
    assert result.apply_effect == "rolled_back"


@pytest.mark.asyncio
async def test_worker_policy_component_requires_restart() -> None:
    kernel = LifecycleKernel()
    kernel.register(ComponentSpec("cache", config_keys=("CACHE_MODE",)), lambda: None)
    await kernel.start_components({"cache"})

    result = await kernel.restart_for_config(
        {"cache_mode"}, apply_change=lambda: None, rollback_change=lambda: None
    )

    assert result.apply_effect == "restart_pending"
    assert result.reason_codes == ["component_not_restartable"]


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
    await kernel.start_components({"first", "second"})
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


@pytest.mark.asyncio
async def test_stop_failure_still_runs_context_finalizers() -> None:
    kernel = LifecycleKernel()
    finalized: list[str] = []

    async def start(context) -> str:
        context.add_finalizer(finalized.append, "released")
        return "ready"

    async def stop() -> None:
        raise RuntimeError("stop failed")

    kernel.register(ComponentSpec("failing-stop"), start, stop=stop, pass_context=True)
    await kernel.start_components({"failing-stop"})

    await kernel.stop_all()

    assert finalized == ["released"]
    status = kernel.component_status("failing-stop")
    assert status is not None
    assert status["state"] == "failed"
    assert kernel.status()["recovery_required"] == ["failing-stop"]


@pytest.mark.asyncio
async def test_task_cancel_timeout_is_bounded_and_requires_recovery() -> None:
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

    kernel.register(ComponentSpec("resistant"), start, pass_context=True)
    await kernel.start_components({"resistant"})

    started = asyncio.get_running_loop().time()
    await kernel.stop_all(timeout=0.05)
    elapsed = asyncio.get_running_loop().time() - started

    status = kernel.component_status("resistant")
    assert elapsed < 0.5
    assert status is not None
    assert status["state"] == "failed"
    assert status["error_code"] == "component_stop_failed:component_task_cancel_timeout"
    assert kernel.status()["recovery_required"] == ["resistant"]

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
    await kernel.start_components({"finalizers"})
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

    kernel.register(ComponentSpec("resistant-context"), start, pass_context=True)
    await kernel.start_components({"resistant-context"})

    started = asyncio.get_running_loop().time()
    await kernel.stop_all(timeout=0.05)
    elapsed = asyncio.get_running_loop().time() - started

    assert elapsed < 0.5
    assert kernel.component_status("resistant-context")["state"] == "failed"  # type: ignore[index]
    release.set()
    await asyncio.sleep(0)


@pytest.mark.asyncio
async def test_start_failure_with_failed_cleanup_requires_recovery() -> None:
    kernel = LifecycleKernel()

    async def start(context) -> None:
        context.add_finalizer(lambda: (_ for _ in ()).throw(RuntimeError("cleanup")))
        raise ValueError("start")

    kernel.register(ComponentSpec("partial"), start, pass_context=True)
    with pytest.raises(ValueError, match="start"):
        await kernel.start_components({"partial"})

    status = kernel.component_status("partial")
    assert status is not None
    assert status["state"] == "failed"
    assert status["error_code"] == "component_start_failed:ValueError"
    assert kernel.status()["recovery_required"] == ["partial"]


@pytest.mark.asyncio
async def test_terminal_receipt_confirms_clean_shutdown(tmp_path) -> None:
    receipt_path = tmp_path / "receipt.json"
    kernel = LifecycleKernel(receipt_path)
    kernel.set_process_metadata(boot_id="boot", startup_id="startup")
    kernel.register(ComponentSpec("clean"), lambda: None)
    await kernel.start_components({"clean"})

    await kernel.stop_all()
    kernel.finalize_terminal_receipt(exit_signals=[2])

    receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    assert receipt["result"] == "confirmed"
    assert receipt["identity"]["boot_id"] == "boot"
    assert receipt["identity"]["startup_id"] == "startup"
    assert receipt["exit_signals"] == [2]
    assert receipt["recovery_required"] == []


@pytest.mark.asyncio
async def test_terminal_receipt_is_unconfirmed_when_component_still_running(
    tmp_path,
) -> None:
    receipt_path = tmp_path / "receipt.json"
    kernel = LifecycleKernel(receipt_path)
    kernel.register(ComponentSpec("running"), lambda: None)
    await kernel.start_components({"running"})
    kernel.request_shutdown()

    kernel.finalize_terminal_receipt()

    receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    assert receipt["result"] == "unconfirmed"
    assert receipt["recovery_required"] == ["running"]
    await kernel.stop_all()
