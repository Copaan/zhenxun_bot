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
    PolicyUpdate,
)

router = APIRouter(prefix="/plugin-policy")


@router.get("/accounts/{bot_id}/private", dependencies=[authentication()])
async def get_account_private(bot_id: str):
    from zhenxun.services.bot_group_policy import bot_group_policy_service

    return Result.ok(await bot_group_policy_service.get_private(bot_id))


@router.put("/accounts/{bot_id}/private", dependencies=[authentication()])
async def update_account_private(bot_id: str, payload: AccountPolicyUpdate):
    from zhenxun.services.bot_group_policy import bot_group_policy_service

    try:
        return Result.ok(
            await bot_group_policy_service.update_private(
                bot_id,
                expected_revision=payload.expected_revision,
                block_plugins=payload.block_plugins,
                block_tasks=payload.block_tasks,
            ),
            "私聊设置已保存并生效",
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
        return Result.ok(
            await bot_group_policy_service.update_group(
                bot_id,
                platform_scope,
                group_id,
                channel_id,
                expected_revision=payload.expected_revision,
                block_plugins=payload.block_plugins,
                block_tasks=payload.block_tasks,
            ),
            "单群设置已保存并生效",
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
        return Result.ok(result, "账号插件设置已保存并生效")
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
        return Result.ok(result, "共享策略已保存并同步")
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
        return Result.ok(result, "策略已分配并生效")
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
