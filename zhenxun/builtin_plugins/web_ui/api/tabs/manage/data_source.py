import nonebot

from zhenxun.models.ban_console import BanConsole
from zhenxun.models.chat_history import ChatHistory
from zhenxun.models.fg_request import FgRequest
from zhenxun.models.group_console import GroupConsole
from zhenxun.models.plugin_info import PluginInfo
from zhenxun.models.statistics import Statistics
from zhenxun.models.task_info import TaskInfo
from zhenxun.services.bot_group_policy import bot_group_policy_service
from zhenxun.services.plugin_policy import (
    PluginPolicyError,
    plugin_policy_service,
    policy_transaction,
)
from zhenxun.services.runtime_mutation import managed_mutation
from zhenxun.utils.common_utils import CommonUtils
from zhenxun.utils.enum import RequestType
from zhenxun.utils.platform import PlatformUtils

from ....config import AVA_URL, GROUP_AVA_URL
from ....utils import webui_db_call
from ..main.data_source import top_plugins
from .model import (
    FriendRequestResult,
    GroupDetail,
    GroupRequestResult,
    Plugin,
    ReqResult,
    Task,
    UpdateGroup,
    UserDetail,
)


class ApiDataSource:
    @staticmethod
    async def _group_policy(group_id: str, bot_id: str | None):
        from zhenxun.models.bot_group_policy import BotGroupMembership

        if bot_id is None:
            bot_id, scope = await bot_group_policy_service._legacy_identity(group_id)
        else:
            bot_id = await plugin_policy_service._resolve_bot_id(bot_id)
            rows = await BotGroupMembership.filter(
                bot_id=bot_id, group_id=group_id, channel_id=""
            )
            scopes = {row.platform_scope for row in rows}
            if len(scopes) != 1:
                raise PluginPolicyError(
                    "缺少唯一群关联，请使用插件策略页面指定平台及账号"
                )
            scope = scopes.pop()
        if scope != "qq_client":
            raise PluginPolicyError("此旧群资料页面仅支持 OneBot，请使用插件策略页面")
        return await bot_group_policy_service.get_group(bot_id, scope, group_id)

    @classmethod
    @managed_mutation("group_configuration")
    async def update_group(cls, group: UpdateGroup):
        """更新群组数据

        参数:
            group: UpdateGroup
        """
        policy = await cls._group_policy(group.group_id, group.bot_id)
        task_list = await TaskInfo.get_modules(load_status=None)
        async with policy_transaction():
            await bot_group_policy_service.update_group(
                policy["bot_id"],
                policy["platform_scope"],
                group.group_id,
                "",
                expected_revision=group.expected_revision or policy["revision"],
                block_plugins=[
                    module
                    for module in group.close_plugins
                    if module not in policy["forced_plugins"]
                ],
                block_tasks=[
                    module
                    for module in task_list
                    if module not in group.task and module not in policy["forced_tasks"]
                ],
            )
            db_group = await GroupConsole.get_group_db(group.group_id)
            if db_group is None:
                raise PluginPolicyError("群资料不存在")
            db_group.level, db_group.status = group.level, group.status
            await db_group.save(update_fields=["level", "status"])

    @classmethod
    async def get_request_list(cls) -> ReqResult:
        """获取好友与群组请求列表

        返回:
            ReqResult: 数据内容
        """
        req_result = ReqResult()
        data_list = await FgRequest.filter(handle_type__isnull=True).all()
        for req in data_list:
            if req.request_type == RequestType.FRIEND:
                req_result.friend.append(
                    FriendRequestResult(
                        oid=req.id,
                        bot_id=req.bot_id,
                        id=req.user_id,
                        flag=req.flag,
                        nickname=req.nickname,
                        comment=req.comment,
                        ava_url=AVA_URL.format(req.user_id),
                        type=str(req.request_type).lower(),
                    )
                )
            else:
                req_result.group.append(
                    GroupRequestResult(
                        oid=req.id,
                        bot_id=req.bot_id,
                        id=req.user_id,
                        flag=req.flag,
                        nickname=req.nickname,
                        comment=req.comment,
                        ava_url=GROUP_AVA_URL.format(req.group_id, req.group_id),
                        type=str(req.request_type).lower(),
                        invite_group=req.group_id,
                        group_name=None,
                    )
                )
        req_result.friend.reverse()
        req_result.group.reverse()
        return req_result

    @classmethod
    async def get_friend_detail(cls, bot_id: str, user_id: str) -> UserDetail | None:
        """获取好友详情

        参数:
            bot_id: bot id
            user_id: 用户id

        返回:
            UserDetail | None: 详情数据
        """
        bot = nonebot.get_bot(bot_id)
        friend_list, _ = await PlatformUtils.get_friend_list(bot)
        fd = [x for x in friend_list if x.user_id == user_id]
        if not fd:
            return None
        storage_bot_id = PlatformUtils.get_storage_bot_id(bot)
        like_plugin = {
            plugin.name: plugin.count
            for plugin in await top_plugins(
                Statistics.filter(bot_id=storage_bot_id, user_id=user_id),
                "Manage.like_plugin",
            )
        }
        user = fd[0]
        return UserDetail(
            user_id=user_id,
            ava_url=AVA_URL.format(user_id),
            nickname=user.user_name,
            remark="",
            is_ban=await BanConsole.is_ban(user_id),
            chat_count=await webui_db_call(
                ChatHistory.filter(bot_id=storage_bot_id, user_id=user_id).count(),
                "Manage.friend_chat_count",
            ),
            call_count=await webui_db_call(
                Statistics.filter(bot_id=storage_bot_id, user_id=user_id).count(),
                "Manage.friend_call_count",
            ),
            like_plugin=like_plugin,
        )

    @classmethod
    async def __get_group_detail_disable_plugin(
        cls, group: GroupConsole
    ) -> list[Plugin]:
        """获取群组禁用插件

        参数:
            group: GroupConsole

        返回:
            list[Plugin]: 禁用插件数据列表
        """
        disable_plugins: list[Plugin] = []
        plugins = await PluginInfo.get_plugins()
        module2name = {p.module: p.name for p in plugins}
        if group.block_plugin:
            for module in CommonUtils.convert_module_format(group.block_plugin):
                if module:
                    plugin = Plugin(
                        module=module,
                        plugin_name=module,
                        is_super_block=False,
                    )
                    plugin.plugin_name = module2name.get(module) or module
                    disable_plugins.append(plugin)
        exists_modules = [p.module for p in disable_plugins]
        if group.superuser_block_plugin:
            for module in CommonUtils.convert_module_format(
                group.superuser_block_plugin
            ):
                if module and module not in exists_modules:
                    plugin = Plugin(
                        module=module,
                        plugin_name=module,
                        is_super_block=True,
                    )
                    plugin.plugin_name = module2name.get(module) or module
                    disable_plugins.append(plugin)
        return disable_plugins

    @classmethod
    async def __get_group_detail_task(cls, group: GroupConsole) -> list[Task]:
        """获取群组被动技能状态

        参数:
            group: GroupConsole

        返回:
            list[Task]: 群组被动列表
        """
        all_task = await TaskInfo.get_tasks(load_status=None)
        task_module2name = {task.module: task.name for task in all_task}
        sbp = CommonUtils.convert_module_format(group.superuser_block_task)
        tasks = CommonUtils.convert_module_format(group.block_task)
        return [
            Task(
                name=task.module,
                zh_name=task_module2name.get(task.module) or task.module,
                status=task.module not in tasks and task.module not in sbp,
                is_super_block=task.module in sbp,
            )
            for task in all_task
        ]

    @classmethod
    async def get_group_detail(
        cls, group_id: str, bot_id: str | None = None
    ) -> GroupDetail | None:
        """获取群组详情

        参数:
            group_id: 群组id

        返回:
            GroupDetail | None: 群组详情数据
        """
        group = await GroupConsole.get_group_db(group_id=group_id)
        if not group:
            return None
        policy = await cls._group_policy(group_id, bot_id)
        group.block_plugin = CommonUtils.convert_module_format(policy["block_plugins"])
        group.block_task = CommonUtils.convert_module_format(policy["block_tasks"])
        group.superuser_block_plugin = CommonUtils.convert_module_format(
            policy["forced_plugins"]
        )
        group.superuser_block_task = CommonUtils.convert_module_format(
            policy["forced_tasks"]
        )
        storage_bot_id = policy["bot_id"]
        like_plugin = {
            plugin.name: plugin.count
            for plugin in await top_plugins(
                Statistics.filter(bot_id=storage_bot_id, group_id=group_id),
                "Manage.like_plugin",
            )
        }
        disable_plugins: list[Plugin] = await cls.__get_group_detail_disable_plugin(
            group
        )
        task_list = await cls.__get_group_detail_task(group)
        return GroupDetail(
            group_id=group_id,
            bot_id=storage_bot_id,
            policy_revision=policy["revision"],
            policy_effective=policy["effective"],
            ava_url=GROUP_AVA_URL.format(group_id, group_id),
            name=group.group_name,
            member_count=group.member_count,
            max_member_count=group.max_member_count,
            chat_count=await webui_db_call(
                ChatHistory.filter(bot_id=storage_bot_id, group_id=group_id).count(),
                "Manage.group_chat_count",
            ),
            call_count=await webui_db_call(
                Statistics.filter(bot_id=storage_bot_id, group_id=group_id).count(),
                "Manage.group_call_count",
            ),
            like_plugin=like_plugin,
            level=group.level,
            status=group.status,
            close_plugins=disable_plugins,
            task=task_list,
        )
