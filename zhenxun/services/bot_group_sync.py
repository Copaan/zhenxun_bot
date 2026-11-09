import asyncio
from datetime import datetime
import re
import time
from typing import ClassVar

import nonebot
from nonebot.adapters import Bot
from nonebot_plugin_uninfo import Member, Scene, SceneType, get_interface
from nonebot_plugin_uninfo.adapters import alter_get_fetcher
from tortoise.transactions import in_transaction

from zhenxun.models.group_console import GroupConsole
from zhenxun.models.group_member_info import GroupInfoUser
from zhenxun.services.bot_group_policy import bot_group_policy_service
from zhenxun.services.cache.runtime_cache import GroupMemoryCache
from zhenxun.services.db_context.write import defer, write_scope
from zhenxun.services.hot_query_cache import (
    invalidate_group_members,
    invalidate_member_names,
)
from zhenxun.services.log import logger
from zhenxun.services.runtime_mutation import runtime_mutation_coordinator
from zhenxun.utils.platform import PlatformUtils
from zhenxun.utils.time_utils import TimeUtils


class BotGroupSync:
    """One bounded query sequence per live connection; no network under DB locks."""

    def __init__(self):
        self.context = None
        self.connections = {}
        self.status = {}
        self.final_logs = set()

    def start(self, context):
        self.context = context
        context.add_finalizer(self.close)
        for bot in nonebot.get_bots().values():
            self.connect(bot)

    def close(self):
        self.context = None
        for _, task in self.connections.values():
            task.cancel()
        self.connections.clear()
        self.status.clear()
        self.final_logs.clear()

    def connect(self, bot):
        if not self.context or not self.context.accepting:
            return
        key = PlatformUtils.get_storage_bot_id(bot)
        previous = self.connections.get(key)
        if previous and previous[0] is bot:
            return
        if previous:
            previous[1].cancel()
        self.final_logs.discard(key)
        task = self.context.spawn_task(
            self._sync(bot, key), name="bot-group-sync", degrade_on_error=False
        )
        self.connections[key] = (bot, task)

    def disconnect(self, bot):
        key = PlatformUtils.get_storage_bot_id(bot)
        current = self.connections.get(key)
        if current and current[0] is bot:
            current[1].cancel()
            self.connections.pop(key, None)
            self.status.pop(key, None)
            self.final_logs.discard(key)

    def _current(self, bot, key):
        current = self.connections.get(key)
        return bool(
            self.context
            and self.context.accepting
            and current
            and current[0] is bot
            and current[1] is asyncio.current_task()
        )

    async def _sync(self, bot, key):
        started = time.monotonic()
        scope = PlatformUtils.get_platform_scope(bot)
        if not self._current(bot, key):
            return
        if scope == "qq_api":
            self.status[key] = {
                "source": "uninfo",
                "attempt": 1,
                "code": "group_query_unsupported",
            }
            if key not in self.final_logs:
                logger.info(
                    "官方 QQ 未提供可用于群认证的群目录，保留已有群认证 | "
                    f"bot={key} | scope={scope} | "
                    "source=uninfo | attempt=1 | code=group_query_unsupported",
                    "群认证同步",
                )
                self.final_logs.add(key)
            return
        for attempt, offset in enumerate((0, 1, 4, 14, 44), 1):
            await asyncio.sleep(max(0, started + offset - time.monotonic()))
            if not self._current(bot, key):
                return
            groups = []
            code = "group_query_empty"
            try:
                if get_interface(bot) is None:
                    alter_get_fetcher(bot.adapter.get_name())
                if get_interface(bot) is None:
                    code = "group_query_interface_missing"
                else:
                    groups, _ = await asyncio.wait_for(
                        PlatformUtils.get_group_list(bot), timeout=5.0
                    )
                    if groups:
                        code = "group_query_ok"
            except asyncio.TimeoutError:
                code = "group_query_timeout"
            except NotImplementedError:
                code = "group_query_unsupported"
            except Exception as error:
                from nonebot.exception import NetworkError

                code = (
                    "group_query_network_failed"
                    if isinstance(error, NetworkError | OSError)
                    else "group_query_failed"
                )
            if not self._current(bot, key):
                return
            self.status[key] = {"source": "uninfo", "attempt": attempt, "code": code}
            if groups:
                async with runtime_mutation_coordinator.operation("bot_group_sync"):
                    if not self._current(bot, key):
                        return
                    if scope == "qq_client":
                        for candidate in groups:
                            group, _ = await GroupConsole.get_or_create_root_group(
                                candidate.group_id,
                                defaults={
                                    "group_name": candidate.group_name,
                                    "group_flag": 1,
                                },
                            )
                            await GroupMemoryCache.upsert_from_model(group)
                    await bot_group_policy_service.record_memberships(
                        [
                            {
                                "bot_id": key,
                                "platform_scope": scope,
                                "group_id": str(group.group_id),
                                "channel_id": str(group.channel_id or ""),
                                "group_name": group.group_name or "",
                            }
                            for group in groups
                        ],
                        source="sync",
                    )
                logger.info(
                    "群同步完成 | source=uninfo | "
                    f"attempt={attempt} | count={len(groups)}",
                    "群认证同步",
                )
                return
            if code == "group_query_unsupported":
                break
        if key not in self.final_logs:
            logger.info(
                "群同步未取得数据，保留已有群认证 | source=uninfo | "
                f"bot={key} | scope={scope} | attempt={attempt} | code={code}",
                "群认证同步",
            )
            self.final_logs.add(key)


bot_group_sync = BotGroupSync()


class MemberUpdateManage:
    """群成员全量同步及刷新节奏。"""

    _last_updates: ClassVar[dict[tuple[str, str], float]] = {}
    _semaphore = asyncio.Semaphore(1)
    SUCCESS_MESSAGE = "群组成员信息更新完成!"

    @classmethod
    async def build_scene_map(cls, bot: Bot) -> dict[str, Scene]:
        """获取机器人当前可见的群场景。"""
        if not (interface := get_interface(bot)):
            return {}
        scenes = await interface.get_scenes(SceneType.GROUP)
        return {scene.id: scene for scene in scenes if scene.is_group}

    @classmethod
    def last_update(cls, bot: Bot, group_id: str) -> float:
        """读取机器人对应群的最近成功刷新时间。"""
        key = (PlatformUtils.get_storage_bot_id(bot), group_id)
        return cls._last_updates.get(key, 0)

    @classmethod
    async def run_update(
        cls,
        bot: Bot,
        group_id: str,
        *,
        scene_map: dict[str, Scene] | None = None,
        platform: str | None = None,
        only_if_due: bool = False,
    ) -> str | None:
        """串行同步群成员，夜间按本地自然日去重，仅在提交成功后记录时间。"""
        async with cls._semaphore:
            if only_if_due:
                previous = cls.last_update(bot, group_id)
                if (
                    previous
                    and datetime.fromtimestamp(previous, TimeUtils.zone()).date()
                    == TimeUtils.now().date()
                ):
                    return None
            result = await cls.update_group_member(
                bot, group_id, scene_map=scene_map, platform=platform
            )
            if result == cls.SUCCESS_MESSAGE:
                key = (PlatformUtils.get_storage_bot_id(bot), group_id)
                cls._last_updates[key] = time.time()
            return result

    @classmethod
    def __handle_user(
        cls,
        member: Member,
        db_user_map: dict[str, list[GroupInfoUser]],
        group_id: str,
        data_list: tuple[list[GroupInfoUser], list[GroupInfoUser], list[int]],
        platform: str | None,
    ):
        """单个成员操作

        参数:
            member: Member
            db_user: db成员数据
            group_id: 群组id
            data_list: 数据列表
            platform: 平台
        """
        nickname = re.sub(
            r"[\x00-\x09\x0b-\x1f\x7f-\x9f]", "", member.nick or member.user.name or ""
        )
        member_id = str(member.id)
        if users := db_user_map.get(member_id):
            if len(users) > 1:
                data_list[2].extend(u.id for u in users[1:])
            if nickname != users[0].user_name:
                user = users[0]
                user.user_name = nickname
                data_list[1].append(user)
        else:
            data_list[0].append(
                GroupInfoUser(
                    user_id=member_id,
                    group_id=group_id,
                    user_name=nickname,
                    user_join_time=member.joined_at or datetime.now(),
                    platform=platform,
                )
            )

    @classmethod
    async def update_group_member(
        cls,
        bot: Bot,
        group_id: str,
        *,
        scene_map: dict[str, Scene] | None = None,
        platform: str | None = None,
    ) -> str:
        """更新群组成员信息

        参数:
            bot: Bot
            group_id: 群组id

        返回:
            str: 返回消息
        """
        if not group_id:
            logger.warning(f"bot: {bot.self_id}，group_id为空，无法更新群成员信息...")
            return "群组id为空..."
        if interface := get_interface(bot):
            if scene_map is None:
                scenes = await interface.get_scenes(SceneType.GROUP)
                scene_map = {scene.id: scene for scene in scenes if scene.is_group}
            if platform is None:
                platform = PlatformUtils.get_platform(bot)
            group_scene = scene_map.get(group_id) if scene_map else None
            if not group_scene:
                logger.warning(
                    f"bot: {bot.self_id}，group_id: {group_id}，群组不存在，"
                    "无法更新群成员信息..."
                )
                return "更新群组失败，群组不存在..."
            members = await interface.get_members(SceneType.GROUP, group_scene.id)
            if not members:
                return "更新群组失败，成员列表为空，保留现有成员信息..."

            async with in_transaction() as connection, write_scope(connection):
                group_console, _ = await GroupConsole.get_or_create_root_group(
                    group_id=group_id, defaults={"platform": platform}
                )
                group_console.member_count = len(members)
                group_console.group_name = group_scene.name or ""
                await group_console.save(
                    using_db=connection, update_fields=["member_count", "group_name"]
                )
                db_user = (
                    await GroupInfoUser.filter(group_id=group_id)
                    .using_db(connection)
                    .all()
                )
                db_user_map: dict[str, list[GroupInfoUser]] = {}
                for user in db_user:
                    db_user_map.setdefault(user.user_id, []).append(user)
                db_user_ids = set(db_user_map)
                data_list: tuple[
                    list[GroupInfoUser], list[GroupInfoUser], list[int]
                ] = (
                    [],
                    [],
                    [],
                )
                exist_member_ids: set[str] = set()
                for member in members:
                    member_id = str(member.id)
                    cls.__handle_user(
                        member, db_user_map, group_id, data_list, platform
                    )
                    exist_member_ids.add(member_id)
                if data_list[0]:
                    await GroupInfoUser.bulk_create(
                        data_list[0], 30, ignore_conflicts=True, using_db=connection
                    )
                for user in data_list[1]:
                    await user.save(using_db=connection, update_fields=["user_name"])
                if data_list[2]:
                    await (
                        GroupInfoUser.filter(id__in=data_list[2])
                        .using_db(connection)
                        .delete()
                    )
                delete_member_ids = db_user_ids - exist_member_ids
                if delete_member_ids:
                    await (
                        GroupInfoUser.filter(
                            user_id__in=list(delete_member_ids), group_id=group_id
                        )
                        .using_db(connection)
                        .delete()
                    )
                changed_user_ids = (
                    {user.user_id for user in data_list[0]}
                    | {user.user_id for user in data_list[1]}
                    | delete_member_ids
                )
                if data_list[0] or data_list[1] or data_list[2] or delete_member_ids:

                    async def publish():
                        await invalidate_group_members(group_id, changed_user_ids)
                        await invalidate_member_names(changed_user_ids)

                    if not defer(("bulk", "group_info_users", str(group_id)), publish):
                        await publish()
            logger.debug(
                f"群成员同步已提交: members={len(members)} "
                f"created={len(data_list[0])} renamed={len(data_list[1])} "
                f"removed={len(delete_member_ids) + len(data_list[2])}",
                "更新群组成员信息",
                target=group_id,
            )
            return cls.SUCCESS_MESSAGE
        return "更新群组失败，当前协议接口不可用..."
