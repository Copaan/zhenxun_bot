import os

from zhenxun.services.plugin_policy import plugin_policy_service
from zhenxun.utils.manager.priority_manager import PriorityLifecycle


@PriorityLifecycle.on_startup(
    priority=7,
    component_id="runtime:plugin_policy_reconcile",
    depends_on=("runtime:runtime_cache", "runtime:reconcile_tasks"),
    failure_policy="degrade",
)
async def reconcile_plugin_policies():
    from zhenxun.services.migration import state

    request = state.load()
    if request and request.get("state") in {
        "preparing",
        "verifying",
        "rollback_verifying",
    }:
        startup_id = os.environ.get("ZHENXUN_WORKER_STARTUP_ID")
        identity_key = (
            "preparation_startup_id"
            if request["state"] == "preparing"
            else "expected_worker_startup_id"
        )
        if not startup_id or startup_id != request.get(identity_key):
            raise RuntimeError("migration_worker_identity_changed")
        with state.owned(request):
            return await plugin_policy_service.reconcile()
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
