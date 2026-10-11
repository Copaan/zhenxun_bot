import asyncio

from zhenxun.utils.manager.priority_manager import PriorityLifecycle

from .engine import engine_manager
from .service import RendererService
from .types import Renderable, RenderResult

renderer_service = RendererService()


async def _drain_renders() -> None:
    """等待进行中的渲染完成，最多 10 秒"""
    loop = asyncio.get_running_loop()
    deadline = loop.time() + 10
    while (await engine_manager.get_runtime_snapshot()).get("active_renders"):
        if loop.time() >= deadline:
            raise TimeoutError("renderer_drain_timeout")
        await asyncio.sleep(0.05)


@PriorityLifecycle.on_startup(
    priority=10,
    stage="warmup",
    timeout=300,
    parallel_safe=True,
    failure_policy="degrade",
    component_id="warmup:renderer",
    depends_on=("warmup:resources",),
    restart_policy="component",
    config_keys=("RENDERER", "THEME", "RESOURCE_VERSION"),
    pass_context=True,
)
async def _init_renderer_service(context):
    """在Bot启动时初始化渲染服务及其依赖。"""
    renderer_service.bind_context(context)
    context.add_finalizer(renderer_service.close)
    context.add_finalizer(_drain_renders)
    await renderer_service.initialize()
    from zhenxun.services.startup import startup_coordinator

    snapshot = await engine_manager.get_runtime_snapshot()
    for name, elapsed in snapshot.get("preparation_timings", {}).items():
        startup_coordinator.record_operation(
            f"renderer:{name}", "warmup", "completed", elapsed
        )
    return renderer_service


__all__ = ["RenderResult", "Renderable", "renderer_service"]
