from zhenxun.utils.manager.priority_manager import PriorityLifecycle

from .engine import engine_manager
from .service import RendererService
from .types import Renderable, RenderResult

renderer_service = RendererService()


@PriorityLifecycle.on_startup(
    priority=10,
    stage="warmup",
    timeout=300,
    parallel_safe=True,
    failure_policy="degrade",
    component_id="warmup:renderer",
    depends_on=("warmup:resources",),
    pass_context=True,
)
async def _init_renderer_service(context):
    """在Bot启动时初始化渲染服务及其依赖。"""
    context.add_finalizer(renderer_service.close)
    await renderer_service.initialize()
    return renderer_service


__all__ = ["RenderResult", "Renderable", "engine_manager", "renderer_service"]
