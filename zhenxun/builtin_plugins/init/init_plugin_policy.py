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


@PriorityLifecycle.on_startup(
    priority=7,
    component_id="runtime:bot_group_policy",
    depends_on=("runtime:plugin_policy_reconcile",),
    pass_context=True,
)
async def start_bot_group_policy(context):
    from zhenxun.services.bot_group_policy import bot_group_policy_service
    from zhenxun.services.bot_group_sync import bot_group_sync

    await bot_group_policy_service.start(context)
    bot_group_sync.start(context)
