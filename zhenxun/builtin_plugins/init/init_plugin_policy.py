from zhenxun.services.plugin_policy import plugin_policy_service
from zhenxun.utils.manager.priority_manager import PriorityLifecycle


@PriorityLifecycle.on_startup(
    priority=7,
    task_id="runtime:plugin_policy_reconcile",
    component_id="runtime:plugin_policy_reconcile",
    depends_on=("runtime:runtime_cache", "runtime:reconcile_tasks"),
    failure_policy="degrade",
    restart_policy="component",
)
async def reconcile_plugin_policies():
    return await plugin_policy_service.reconcile()
