from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from zhenxun.services.lifecycle import (
    ComponentSpec,
    LifecycleKernel,
    ResourceReceipt,
    RuntimeHandle,
)
from zhenxun.services.lifecycle.operations import (
    OperationRegistry,
    OperationState,
)


@pytest.mark.asyncio
async def test_dynamic_scope_is_visible_and_closes_with_parent() -> None:
    kernel = LifecycleKernel()
    holder = {}

    async def start(context) -> None:
        holder["context"] = context

    kernel.register(
        ComponentSpec("scope-host", scope="worker", stage="runtime"),
        start,
        pass_context=True,
    )
    await kernel.start_stage("runtime")
    child = holder["context"].create_child_scope("operation", "build")

    async with child.activity():
        status = kernel.status()
        assert status["active_scope_count"] == 1
        assert status["dynamic_scopes"][0]["active_activities"] == 1

    await kernel.stop_all()
    status = kernel.status()
    assert status["active_scope_count"] == 0
    assert status["dynamic_scopes"][0]["state"] == "closed"


@pytest.mark.asyncio
async def test_composite_handle_cleanup_order_and_resource_refresh() -> None:
    kernel = LifecycleKernel()
    order: list[str] = []

    class Handle:
        async def quiesce(self) -> None:
            order.append("quiesce")

        async def close(self) -> None:
            order.append("handle_close")

        def health(self):
            return {"healthy": True, "queue": 0}

        def snapshot(self):
            return {"state": "ready"}

        def resource_snapshot(self):
            return [
                ResourceReceipt(
                    "handle:worker",
                    "fixture",
                    "worker",
                    "composite",
                )
            ]

    async def start(context):
        context.add_finalizer(order.append, "finalizer")
        return RuntimeHandle(controller=Handle())

    async def stop() -> None:
        order.append("legacy_stop")

    kernel.register(
        ComponentSpec("composite", stage="runtime"),
        start,
        stop=stop,
        pass_context=True,
    )
    await kernel.start_stage("runtime")
    component = kernel.component_status("composite")
    assert component is not None
    assert component["metadata"]["handle"] == {"state": "ready"}
    assert component["resource_count"] == 1

    await kernel.stop_all()
    assert order == ["quiesce", "legacy_stop", "finalizer", "handle_close"]


async def _bound_registry(path: Path):
    kernel = LifecycleKernel()
    holder = {}

    async def start(context) -> None:
        holder["context"] = context

    kernel.register(
        ComponentSpec("operations", scope="worker", stage="management"),
        start,
        pass_context=True,
    )
    await kernel.start_stage("management")
    registry = OperationRegistry(kernel, path)
    registry.bind(holder["context"])
    return kernel, registry


@pytest.mark.asyncio
async def test_operation_shutdown_checkpoints_and_redacts_input(tmp_path: Path) -> None:
    kernel, registry = await _bound_registry(tmp_path / "operations.json")
    blocker = asyncio.Event()

    async def work() -> None:
        await blocker.wait()

    record, _ = registry.start(
        "fixture",
        work(),
        operation_id="checkpointed",
        public_input={"name": "demo", "api_token": "do-not-persist"},
        checkpoint=lambda: {"offset": 3},
    )
    await asyncio.sleep(0)
    await registry.shutdown()

    operation = registry.get(record.operation_id)
    assert operation is not None
    assert operation["state"] == OperationState.CHECKPOINTED
    assert operation["checkpoint"] == {"offset": 3}
    assert operation["public_input"]["api_token"] == "configured"
    assert kernel.status()["active_scope_count"] == 0


@pytest.mark.asyncio
async def test_commit_timeout_requires_recovery(tmp_path: Path) -> None:
    _, registry = await _bound_registry(tmp_path / "operations.json")
    blocker = asyncio.Event()

    async def work() -> None:
        try:
            await blocker.wait()
        except asyncio.CancelledError:
            raise

    record, _ = registry.start("fixture", work(), operation_id="commit")
    await asyncio.sleep(0)
    registry.mark_commit_critical(record.operation_id, "swap")
    await registry.shutdown(commit_timeout=0.01)

    operation = registry.get(record.operation_id)
    assert operation is not None
    assert operation["state"] == OperationState.RECOVERY_REQUIRED
    assert operation["error_code"] == "operation_commit_timeout"


@pytest.mark.asyncio
async def test_commit_shutdown_tolerates_another_operation_finishing(
    tmp_path: Path,
) -> None:
    _, registry = await _bound_registry(tmp_path / "operations.json")
    blocker = asyncio.Event()

    async def finish() -> None:
        await asyncio.sleep(0)

    async def wait_forever() -> None:
        await blocker.wait()

    finished, _ = registry.start("fixture", finish(), operation_id="finished")
    blocked, _ = registry.start("fixture", wait_forever(), operation_id="blocked")
    registry.mark_commit_critical(finished.operation_id, "swap")
    registry.mark_commit_critical(blocked.operation_id, "swap")

    await registry.shutdown(commit_timeout=0.01)

    assert registry.get(finished.operation_id)["state"] == OperationState.COMPLETED
    assert (
        registry.get(blocked.operation_id)["state"] == OperationState.RECOVERY_REQUIRED
    )


@pytest.mark.asyncio
async def test_operation_resume_preserves_checkpoint_across_repeated_crashes(
    tmp_path: Path,
) -> None:
    path = tmp_path / "operations.json"
    operation_id = "resumable"
    path.write_text(
        """{
          "version": 2,
          "operations": [{
            "operation_id": "resumable",
            "kind": "analysis",
            "owner_component_id": "operations",
            "state": "checkpointed",
            "progress": 73,
            "public_input": {"plugin": "demo"},
            "checkpoint": {"cursor": 12345},
            "recovery_policy": "resume",
            "phase": "checkpointed",
            "created_at": "2026-01-01T00:00:00+00:00",
            "updated_at": "2026-01-01T00:00:00+00:00"
          }]
        }""",
        encoding="utf-8",
    )

    for _ in range(3):
        kernel, registry = await _bound_registry(path)
        blocker = asyncio.Event()

        async def resume(_record):
            await blocker.wait()

        registry.register_recovery_handler("analysis", resume)
        await registry.recover_pending()
        await asyncio.sleep(0)
        running = registry.get(operation_id)
        assert running is not None
        assert running["progress"] == 73
        assert running["checkpoint"] == {"cursor": 12345}

        await registry.shutdown()
        checkpointed = registry.get(operation_id)
        assert checkpointed is not None
        assert checkpointed["progress"] == 73
        assert checkpointed["checkpoint"] == {"cursor": 12345}
        await kernel.stop_all()
