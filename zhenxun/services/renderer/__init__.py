import asyncio

from zhenxun.services.lifecycle import ResourceReceipt, RuntimeHandle
from zhenxun.utils.manager.priority_manager import PriorityLifecycle

from .engine import engine_manager
from .service import RendererService
from .types import Renderable, RenderResult

renderer_service = RendererService()


class RendererRuntimeHandle:
    async def quiesce(self) -> None:
        loop = asyncio.get_running_loop()
        deadline = loop.time() + 10
        while True:
            snapshot = await engine_manager.get_runtime_snapshot()
            if not snapshot.get("active_renders"):
                return
            if loop.time() >= deadline:
                raise TimeoutError("renderer_drain_timeout")
            await asyncio.sleep(0.05)

    async def close(self) -> None:
        await engine_manager.close()

    async def health(self) -> dict[str, object]:
        snapshot = await engine_manager.get_runtime_snapshot()
        return {
            "healthy": bool(snapshot.get("active_generation"))
            and not snapshot.get("closing", False),
            "active_renders": int(snapshot.get("active_renders") or 0),
        }

    async def snapshot(self) -> dict[str, object]:
        return await engine_manager.get_runtime_snapshot()

    async def resource_snapshot(self) -> list[ResourceReceipt]:
        snapshot = await engine_manager.get_runtime_snapshot()
        receipts = [
            ResourceReceipt(
                receipt_id=f"renderer:{id(engine_manager)}",
                provider="renderer",
                resource_type="engine",
                owner_id="warmup:renderer",
                detail={
                    "active_renders": int(snapshot.get("active_renders") or 0),
                    "inflight_tasks": int(snapshot.get("inflight_task_count") or 0),
                    "retiring_generations": int(
                        snapshot.get("retiring_generation_count") or 0
                    ),
                },
            )
        ]
        engine = engine_manager._instance
        if engine is not None:
            tasks = [
                getattr(engine, "_idle_recycle_task", None),
                *list(getattr(engine, "_inflight_tasks", {}).values()),
            ]
            receipts.extend(
                ResourceReceipt(
                    receipt_id=f"task:{id(task)}",
                    provider="renderer",
                    resource_type="task",
                    owner_id="warmup:renderer",
                    detail={"name": task.get_name()},
                )
                for task in tasks
                if isinstance(task, asyncio.Task) and not task.done()
            )
        return receipts


async def _renderer_healthy(_value=None) -> bool:
    snapshot = await engine_manager.get_runtime_snapshot()
    return bool(snapshot.get("active_generation")) and not snapshot.get(
        "closing", False
    )


@PriorityLifecycle.on_startup(
    priority=10,
    stage="warmup",
    timeout=300,
    parallel_safe=True,
    failure_policy="degrade",
    task_id="warmup:renderer",
    component_id="warmup:renderer",
    depends_on=("warmup:resources",),
    resource_group="renderer",
    restart_policy="component",
    config_keys=("RENDERER", "THEME", "RESOURCE_VERSION"),
    pass_context=True,
    health=_renderer_healthy,
)
async def _init_renderer_service(context):
    """在Bot启动时初始化渲染服务及其依赖。"""
    await renderer_service.initialize()
    return RuntimeHandle(
        value=renderer_service,
        controller=RendererRuntimeHandle(),
        metadata={"ownership": "composite"},
    )


__all__ = ["RenderResult", "Renderable", "renderer_service"]
