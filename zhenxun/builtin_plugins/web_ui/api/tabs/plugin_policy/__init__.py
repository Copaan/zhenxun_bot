from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import JSONResponse

from zhenxun.services.plugin_policy import (
    PluginPolicyConflict,
    PluginPolicyError,
    PluginPolicyInUse,
    PluginPolicyNotFound,
    plugin_policy_service,
)
from zhenxun.services.runtime_mutation import RuntimeMutationBusyError

from ....base_model import Result
from ....utils import authentication
from .model import (
    AccountPolicyUpdate,
    PolicyBindingUpdate,
    PolicyCopy,
    PolicyCreate,
    PolicyDocument,
    PolicyUpdate,
    ScopedPolicyImport,
)

router = APIRouter(prefix="/plugin-policy")


def _saved(result: dict, label: str):
    suffix = "已保存，缓存发布待核验" if result.get("publication_pending") else "已保存"
    return Result.ok(result, label + suffix)


@router.get("/accounts/{bot_id}/private", dependencies=[authentication()])
async def get_account_private(bot_id: str):
    from zhenxun.services.bot_group_policy import bot_group_policy_service

    return Result.ok(await bot_group_policy_service.get_private(bot_id))


@router.put("/accounts/{bot_id}/private", dependencies=[authentication()])
async def update_account_private(bot_id: str, payload: AccountPolicyUpdate):
    from zhenxun.services.bot_group_policy import bot_group_policy_service

    try:
        return _saved(
            await bot_group_policy_service.update_private(
                bot_id,
                expected_revision=payload.expected_revision,
                block_plugins=payload.block_plugins,
                block_tasks=payload.block_tasks,
            ),
            "私聊设置",
        )
    except (PluginPolicyError, RuntimeMutationBusyError) as error:
        _raise_api_error(error)


@router.get("/accounts/{bot_id}/groups", dependencies=[authentication()])
async def get_account_groups(bot_id: str):
    from zhenxun.services.bot_group_policy import bot_group_policy_service
    from zhenxun.services.bot_group_sync import bot_group_sync

    try:
        groups = await bot_group_policy_service.list_groups(bot_id)
        return Result.ok({"groups": groups, "sync": bot_group_sync.status.get(bot_id)})
    except PluginPolicyError as error:
        _raise_api_error(error)


@router.get("/accounts/{bot_id}/groups/{group_id}", dependencies=[authentication()])
async def get_account_group(
    bot_id: str, group_id: str, platform_scope: str, channel_id: str = ""
):
    from zhenxun.services.bot_group_policy import bot_group_policy_service

    try:
        return Result.ok(
            await bot_group_policy_service.get_group(
                bot_id, platform_scope, group_id, channel_id
            )
        )
    except PluginPolicyError as error:
        _raise_api_error(error)


@router.put("/accounts/{bot_id}/groups/{group_id}", dependencies=[authentication()])
async def update_account_group(
    bot_id: str,
    group_id: str,
    payload: AccountPolicyUpdate,
    platform_scope: str,
    channel_id: str = "",
):
    from zhenxun.services.bot_group_policy import bot_group_policy_service

    try:
        return _saved(
            await bot_group_policy_service.update_group(
                bot_id,
                platform_scope,
                group_id,
                channel_id,
                expected_revision=payload.expected_revision,
                block_plugins=payload.block_plugins,
                block_tasks=payload.block_tasks,
            ),
            "单群设置",
        )
    except (PluginPolicyError, RuntimeMutationBusyError) as error:
        _raise_api_error(error)


def _raise_api_error(error: Exception) -> None:
    if isinstance(error, PluginPolicyNotFound):
        status_code = 404
    elif isinstance(error, PluginPolicyConflict | PluginPolicyInUse):
        status_code = 409
    elif isinstance(error, RuntimeMutationBusyError):
        status_code = 409
    else:
        status_code = 422
    raise HTTPException(
        status_code=status_code,
        detail={
            "code": getattr(error, "code", "runtime_mutation_busy"),
            "message": str(error),
            "details": getattr(error, "details", {}),
        },
    ) from error


@router.get(
    "/catalog",
    dependencies=[authentication()],
    response_model=Result[dict],
    response_class=JSONResponse,
)
async def get_catalog() -> Result[dict]:
    return Result.ok(await plugin_policy_service.catalog())


@router.get(
    "/accounts",
    dependencies=[authentication()],
    response_model=Result[list[dict]],
    response_class=JSONResponse,
)
async def get_accounts() -> Result[list[dict]]:
    return Result.ok(await plugin_policy_service.list_accounts())


@router.get(
    "/accounts/{bot_id}",
    dependencies=[authentication()],
    response_model=Result[dict],
    response_class=JSONResponse,
)
async def get_account(bot_id: str) -> Result[dict]:
    try:
        return Result.ok(await plugin_policy_service.get_account(bot_id))
    except PluginPolicyError as error:
        _raise_api_error(error)


@router.put(
    "/accounts/{bot_id}",
    dependencies=[authentication()],
    response_model=Result[dict],
    response_class=JSONResponse,
)
async def update_account(bot_id: str, payload: AccountPolicyUpdate) -> Result[dict]:
    try:
        result = await plugin_policy_service.update_account(
            bot_id,
            expected_revision=payload.expected_revision,
            block_plugins=payload.block_plugins,
            block_tasks=payload.block_tasks,
        )
        return _saved(result, "账号插件设置")
    except (PluginPolicyError, RuntimeMutationBusyError) as error:
        _raise_api_error(error)


@router.get(
    "/policies",
    dependencies=[authentication()],
    response_model=Result[list[dict]],
    response_class=JSONResponse,
)
async def get_policies() -> Result[list[dict]]:
    return Result.ok(await plugin_policy_service.list_policies())


@router.get(
    "/policies/{policy_id}",
    dependencies=[authentication()],
    response_model=Result[dict],
    response_class=JSONResponse,
)
async def get_policy(policy_id: int) -> Result[dict]:
    try:
        return Result.ok(await plugin_policy_service.get_policy(policy_id))
    except PluginPolicyError as error:
        _raise_api_error(error)


@router.post(
    "/policies",
    dependencies=[authentication()],
    response_model=Result[dict],
    response_class=JSONResponse,
)
async def create_policy(payload: PolicyCreate) -> Result[dict]:
    try:
        result = await plugin_policy_service.create_policy(
            name=payload.name,
            description=payload.description,
            source_bot_id=payload.source_bot_id,
            source_revision=payload.source_revision,
            block_plugins=payload.block_plugins,
            block_tasks=payload.block_tasks,
            bind_bot_ids=payload.bind_bot_ids,
            expected_revisions=payload.expected_revisions,
        )
        return Result.ok(result, "插件策略已创建")
    except (PluginPolicyError, RuntimeMutationBusyError) as error:
        _raise_api_error(error)


@router.put(
    "/policies/{policy_id}",
    dependencies=[authentication()],
    response_model=Result[dict],
    response_class=JSONResponse,
)
async def update_policy(policy_id: int, payload: PolicyUpdate) -> Result[dict]:
    try:
        result = await plugin_policy_service.update_policy(
            policy_id,
            expected_revision=payload.expected_revision,
            name=payload.name,
            description=payload.description,
            block_plugins=payload.block_plugins,
            block_tasks=payload.block_tasks,
        )
        return _saved(result, "共享策略")
    except (PluginPolicyError, RuntimeMutationBusyError) as error:
        _raise_api_error(error)


@router.delete(
    "/policies/{policy_id}",
    dependencies=[authentication()],
    response_model=Result,
    response_class=JSONResponse,
)
async def delete_policy(
    policy_id: int,
    expected_revision: str = Query(min_length=64, max_length=64),
) -> Result:
    try:
        await plugin_policy_service.delete_policy(policy_id, expected_revision)
        return Result.ok(info="插件策略已删除")
    except (PluginPolicyError, RuntimeMutationBusyError) as error:
        _raise_api_error(error)


@router.put(
    "/bindings",
    dependencies=[authentication()],
    response_model=Result[dict],
    response_class=JSONResponse,
)
async def bind_accounts(payload: PolicyBindingUpdate) -> Result[dict]:
    try:
        result = await plugin_policy_service.bind_accounts(
            policy_id=payload.policy_id,
            bot_ids=payload.bot_ids,
            expected_revisions=payload.expected_revisions,
        )
        return _saved(result, "策略分配")
    except (PluginPolicyError, RuntimeMutationBusyError) as error:
        _raise_api_error(error)


@router.post(
    "/copy",
    dependencies=[authentication()],
    response_model=Result[dict],
    response_class=JSONResponse,
)
async def copy_account(payload: PolicyCopy) -> Result[dict]:
    try:
        result = await plugin_policy_service.copy_account(
            source_bot_id=payload.source_bot_id,
            source_revision=payload.source_revision,
            target_bot_ids=payload.target_bot_ids,
            expected_revisions=payload.expected_revisions,
        )
        return Result.ok(result, "账号设置已复制")
    except (PluginPolicyError, RuntimeMutationBusyError) as error:
        _raise_api_error(error)


@router.get("/migration/preview", dependencies=[authentication()])
async def policy_migration_preview():
    from zhenxun.services.bot_group_policy import bot_group_policy_service

    return Result.ok(await bot_group_policy_service.migration_preview())


@router.get("/export", dependencies=[authentication()])
async def export_policy_configuration():
    from zhenxun.models.bot_group_policy import BotGroupPluginPolicy
    from zhenxun.services.bot_group_policy import bot_group_policy_service, group_key

    scopes = []
    for row in await BotGroupPluginPolicy.all().order_by(
        "bot_id", "platform_scope", "group_id", "channel_id"
    ):
        _, state = await bot_group_policy_service._state(
            group_key(row.bot_id, row.platform_scope, row.group_id, row.channel_id)
        )
        state["expected_revision"] = state.pop("revision")
        scopes.append(state)
    accounts = []
    for account in await plugin_policy_service.list_accounts():
        state = await plugin_policy_service.get_account(account["bot_id"])
        state["expected_revision"] = state.pop("revision")
        accounts.append(state)
    policies = await plugin_policy_service.list_policies()
    for policy in policies:
        policy["expected_revision"] = policy.pop("revision")
    global_policy = await plugin_policy_service.global_state()
    global_policy["expected_revision"] = global_policy.pop("revision")
    global_tasks = await plugin_policy_service.global_state(task=True)
    global_tasks["expected_revision"] = global_tasks.pop("revision")
    return Result.ok(
        {
            "version": 1,
            "scopes": scopes,
            "accounts": accounts,
            "policies": policies,
            "global_policy": global_policy,
            "global_tasks": global_tasks,
            "authority": "database",
        }
    )


@router.post("/import", dependencies=[authentication()])
async def import_policy_configuration(payload: PolicyDocument):
    from tortoise.transactions import in_transaction

    from zhenxun.models.bot_console import BotConsole
    from zhenxun.models.plugin_info import PluginInfo
    from zhenxun.models.plugin_policy import PluginPolicy
    from zhenxun.models.task_info import TaskInfo
    from zhenxun.services.bot_group_policy import bot_group_policy_service, group_key
    from zhenxun.services.runtime_mutation import runtime_mutation_coordinator

    scopes = sorted(
        payload.scopes,
        key=lambda row: (row.bot_id, row.platform_scope, row.group_id, row.channel_id),
    )
    keys = [
        group_key(row.bot_id, row.platform_scope, row.group_id, row.channel_id)
        for row in scopes
    ]
    if len(set(keys)) != len(keys):
        raise HTTPException(422, detail="策略作用域重复")
    try:
        async with runtime_mutation_coordinator.operation("plugin_policy_import"):
            async with in_transaction() as connection:
                await (
                    PluginPolicy.all()
                    .using_db(connection)
                    .select_for_update()
                    .order_by("id")
                )
                await (
                    BotConsole.all()
                    .using_db(connection)
                    .select_for_update()
                    .order_by("id")
                )
                if len({row.bot_id for row in payload.accounts}) != len(
                    payload.accounts
                ) or len({row.id for row in payload.policies}) != len(payload.policies):
                    raise PluginPolicyError("重复的账号或模板")
                for row in payload.accounts:
                    current = await plugin_policy_service.get_account(row.bot_id)
                    plugin_policy_service._check_revision(
                        row.expected_revision, current["revision"]
                    )
                if payload.global_policy:
                    await (
                        PluginInfo.all()
                        .using_db(connection)
                        .select_for_update()
                        .order_by("id")
                    )
                    global_state = await plugin_policy_service.global_state()
                    plugin_policy_service._check_revision(
                        payload.global_policy.expected_revision,
                        global_state["revision"],
                    )
                    for module, values in sorted(payload.global_policy.plugins.items()):
                        await plugin_policy_service.set_global_settings(
                            module,
                            default_status=values.default_status,
                            block_type=values.block_type
                            or (None if values.status else "ALL"),
                        )
                if payload.global_tasks:
                    await (
                        TaskInfo.all()
                        .using_db(connection)
                        .select_for_update()
                        .order_by("id")
                    )
                    task_state = await plugin_policy_service.global_state(task=True)
                    plugin_policy_service._check_revision(
                        payload.global_tasks.expected_revision, task_state["revision"]
                    )
                    for module, values in sorted(payload.global_tasks.tasks.items()):
                        await plugin_policy_service.set_global(
                            module, values.status, task=True
                        )
                        await plugin_policy_service.set_global(
                            module, values.default_status, task=True, default=True
                        )
                for row in sorted(payload.policies, key=lambda item: item.id):
                    await plugin_policy_service.update_policy(
                        row.id,
                        expected_revision=row.expected_revision,
                        name=row.name,
                        description=row.description,
                        block_plugins=row.block_plugins,
                        block_tasks=row.block_tasks,
                    )
                for row in sorted(payload.accounts, key=lambda item: item.bot_id):
                    current = await plugin_policy_service.get_account(row.bot_id)
                    if row.policy_id is not None:
                        await plugin_policy_service.bind_accounts(
                            policy_id=row.policy_id,
                            bot_ids=[row.bot_id],
                            expected_revisions={row.bot_id: current["revision"]},
                        )
                    else:
                        await plugin_policy_service.update_account(
                            row.bot_id,
                            expected_revision=current["revision"],
                            block_plugins=row.block_plugins,
                            block_tasks=row.block_tasks,
                        )
                for row in scopes:
                    if row.platform_scope == "private":
                        if (
                            row.group_id != bot_group_policy_service.PRIVATE_KEY
                            or row.channel_id
                        ):
                            raise PluginPolicyError("私聊策略范围无效")
                    else:
                        await bot_group_policy_service.get_group(
                            row.bot_id, row.platform_scope, row.group_id, row.channel_id
                        )
                    key = group_key(
                        await plugin_policy_service._resolve_bot_id(row.bot_id),
                        row.platform_scope,
                        row.group_id,
                        row.channel_id,
                    )
                    await bot_group_policy_service._write(
                        key,
                        lambda state, item=row: {
                            **state,
                            "block_plugins": item.block_plugins,
                            "block_tasks": item.block_tasks,
                            "forced_plugins": item.forced_plugins,
                            "forced_tasks": item.forced_tasks,
                        },
                        row.expected_revision,
                    )
        return Result.ok({"applied": len(scopes), "source": "database"})
    except (PluginPolicyError, RuntimeMutationBusyError) as error:
        _raise_api_error(error)


@router.post("/migration/apply", dependencies=[authentication()])
async def migrate_policy_scope(payload: ScopedPolicyImport):
    from zhenxun.services.bot_group_policy import bot_group_policy_service

    try:
        result = await bot_group_policy_service.migrate_scope(
            payload.bot_id,
            payload.platform_scope,
            payload.group_id,
            payload.channel_id,
            payload.expected_revision,
        )
        return Result.ok(result, "此作用域已切换至数据库策略")
    except (PluginPolicyError, RuntimeMutationBusyError) as error:
        _raise_api_error(error)
