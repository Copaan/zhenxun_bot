from urllib.parse import urlsplit

import nonebot
from tortoise import timezone
from tortoise.expressions import Q

from zhenxun.models.ban_console import BanConsole
from zhenxun.models.chat_history import ChatHistory
from zhenxun.models.fg_request import FgRequest
from zhenxun.models.group_console import GroupConsole
from zhenxun.models.group_member_info import GroupInfoUser
from zhenxun.models.statistics import Statistics
from zhenxun.services.bot_group_policy import bot_group_policy_service
from zhenxun.services.plugin_policy import (
    PluginPolicyError,
    plugin_policy_service,
)
from zhenxun.services.runtime_mutation import managed_mutation
from zhenxun.utils.enum import RequestType
from zhenxun.utils.platform import PlatformUtils

from ....config import AVA_URL, GROUP_AVA_URL
from ....utils import webui_db_call
from ..main.data_source import top_plugins
from .model import (
    FriendRequestResult,
    GroupDetail,
    GroupRequestResult,
    ReqResult,
    UpdateGroup,
    UserDetail,
)


class ApiDataSource:
    @classmethod
    async def recent_messages(cls, bot_id: str, group_id: str, limit: int) -> dict:
        """读取指定 OneBot 账号与群的最近已存消息，不触发协议端补拉。"""
        from zhenxun.configs.config import Config

        bot = nonebot.get_bot(bot_id)
        scope = PlatformUtils.get_platform_scope(bot)
        if scope != "qq_client" or not group_id.isdecimal() or int(group_id) <= 0:
            raise ValueError("当前历史消息入口仅支持 OneBot 的有效群号。")
        rows = await webui_db_call(
            ChatHistory.scoped_query(
                platform_scope=scope,
                bot_id=PlatformUtils.get_storage_bot_id(bot),
                group_id=group_id,
            )
            .order_by("-create_time", "-id")
            .limit(limit + 1),
            "Manage.recent_messages",
        )
        has_more = len(rows) > limit
        rows = list(reversed(rows[:limit]))
        user_ids = {row.user_id for row in rows}
        names = (
            dict(
                await webui_db_call(
                    GroupInfoUser.filter(
                        Q(platform="qq") | Q(platform__isnull=True) | Q(platform=""),
                        group_id=group_id,
                        user_id__in=user_ids,
                    ).values_list("user_id", "user_name"),
                    "Manage.recent_message_names",
                )
            )
            if user_ids
            else {}
        )
        items = []
        for row in rows:
            created = row.create_time
            if timezone.is_naive(created):
                created = timezone.make_aware(created)
            stamp = created.isoformat()
            parts = []
            for segment in row.segments if isinstance(row.segments, list) else []:
                if not isinstance(segment, dict):
                    continue
                data = segment.get("data", segment)
                if not isinstance(data, dict):
                    continue
                kind = str(segment.get("type", "")).lower()
                if kind == "text" and isinstance(data.get("text"), str):
                    parts.append({"type": "text", "msg": data["text"], "time": stamp})
                elif kind == "image":
                    url = data.get("url")
                    try:
                        parsed = urlsplit(url) if isinstance(url, str) else None
                        valid = (
                            parsed
                            and parsed.scheme in {"http", "https"}
                            and parsed.hostname
                            and not parsed.username
                            and not parsed.password
                        )
                    except ValueError:
                        valid = False
                    parts.append(
                        {
                            "type": "img" if valid else "text",
                            "msg": url if valid else "[图片暂不可用]",
                            "time": stamp,
                        }
                    )
                elif kind == "at":
                    parts.append(
                        {
                            "type": "text",
                            "msg": "@" + str(data.get("target", "成员")),
                            "time": stamp,
                        }
                    )
                elif kind:
                    parts.append({"type": "text", "msg": "[其他消息]", "time": stamp})
            if not parts:
                parts = [
                    {
                        "type": "text",
                        "msg": row.plain_text or row.text or "[无可显示内容]",
                        "time": stamp,
                    }
                ]
            items.append(
                {
                    "history_id": row.id,
                    "message_id": row.message_id,
                    "bot_id": str(bot.self_id),
                    "object_id": group_id,
                    "group_id": group_id,
                    "user_id": row.user_id,
                    "name": names.get(row.user_id) or row.user_id,
                    "ava_url": AVA_URL.format(row.user_id),
                    "message": parts,
                    "isSelf": row.is_bot or row.user_id == str(bot.self_id),
                }
            )
        return {
            "items": items,
            "limit": limit,
            "has_more": has_more,
            "source": "stored_history",
            "recording_enabled": bool(Config.get_config("chat_history", "FLAG", True)),
        }

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
        """更新群唤醒状态与群权限，只修改页面上改动过的项

        参数:
            group: UpdateGroup
        """
        changes = {
            name: value
            for name, value in (("level", group.level), ("status", group.status))
            if value is not None
        }
        if not changes:
            return
        db_group = await GroupConsole.get_group_db(group.group_id)
        if db_group is None:
            raise PluginPolicyError("群资料不存在")
        for name, value in changes.items():
            setattr(db_group, name, value)
        await db_group.save(update_fields=list(changes))

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
        storage_bot_id = policy["bot_id"]
        like_plugin = {
            plugin.name: plugin.count
            for plugin in await top_plugins(
                Statistics.filter(bot_id=storage_bot_id, group_id=group_id),
                "Manage.like_plugin",
            )
        }
        return GroupDetail(
            group_id=group_id,
            bot_id=storage_bot_id,
            ava_url=GROUP_AVA_URL.format(group_id, group_id),
            name=group.group_name,
            member_count=group.member_count,
            max_member_count=group.max_member_count,
            chat_count=await webui_db_call(
                ChatHistory.filter(
                    bot_id=storage_bot_id, group_id=group_id, is_bot=False
                ).count(),
                "Manage.group_chat_count",
            ),
            call_count=await webui_db_call(
                Statistics.filter(bot_id=storage_bot_id, group_id=group_id).count(),
                "Manage.group_call_count",
            ),
            like_plugin=like_plugin,
            level=group.level,
            status=group.status,
        )
