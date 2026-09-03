from __future__ import annotations

import asyncio

import pytest

from zhenxun.services.runtime_mutation import (
    RuntimeMutationBusyError,
    RuntimeMutationCoordinator,
)


@pytest.mark.asyncio
async def test_runtime_mutation_is_reentrant_and_serializes_other_tasks() -> None:
    coordinator = RuntimeMutationCoordinator()
    entered = asyncio.Event()
    release = asyncio.Event()
    order: list[str] = []

    async def first() -> None:
        async with coordinator.operation("first"):
            async with coordinator.operation("nested"):
                order.append("first")
                entered.set()
                await release.wait()

    async def second() -> None:
        await entered.wait()
        async with coordinator.operation("second"):
            order.append("second")

    first_task = asyncio.create_task(first())
    second_task = asyncio.create_task(second())
    await entered.wait()
    await asyncio.sleep(0)
    assert order == ["first"]
    release.set()
    await asyncio.gather(first_task, second_task)
    assert order == ["first", "second"]


@pytest.mark.asyncio
async def test_runtime_mutation_can_fail_fast() -> None:
    coordinator = RuntimeMutationCoordinator()
    entered = asyncio.Event()
    release = asyncio.Event()

    async def holder() -> None:
        async with coordinator.operation("holder"):
            entered.set()
            await release.wait()

    task = asyncio.create_task(holder())
    await entered.wait()
    with pytest.raises(RuntimeMutationBusyError):
        async with coordinator.operation("blocked", fail_if_busy=True):
            pass
    release.set()
    await task


@pytest.mark.asyncio
async def test_runtime_mutation_serializes_all_mutation_sources() -> None:
    coordinator = RuntimeMutationCoordinator()
    active = 0
    maximum_active = 0
    completed: list[str] = []
    sources = ["config", "plugin", "watcher", "resources", "shutdown"]

    async def mutate(source: str) -> None:
        nonlocal active, maximum_active
        async with coordinator.operation(source):
            active += 1
            maximum_active = max(maximum_active, active)
            await asyncio.sleep(0.005)
            completed.append(source)
            active -= 1

    await asyncio.gather(*(mutate(source) for source in sources))

    assert maximum_active == 1
    assert completed == sources
    assert coordinator.status() is None
