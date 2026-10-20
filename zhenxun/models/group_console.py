import asyncio
from typing import TYPE_CHECKING, Any, ClassVar, cast
from typing_extensions import Self

from tortoise import fields
from tortoise.backends.base.client import BaseDBAsyncClient

from zhenxun.models.plugin_info import PluginInfo
from zhenxun.models.task_info import TaskInfo
from zhenxun.services.cache.runtime_cache import GroupMemoryCache
from zhenxun.services.db_context import Model
from zhenxun.services.db_context.schema_ops import AlterColumnType, CreateIndex
from zhenxun.services.platform_identity import scoped_group_id
from zhenxun.utils.enum import DbLockType

if TYPE_CHECKING:
    from zhenxun.services.cache.runtime_snapshot import GroupSnapshot


class GroupConsole(Model):
    id = fields.IntField(pk=True, generated=True, auto_increment=True)
    """自增id"""
    group_id = fields.CharField(255, description="群组id")
    """群聊id"""
    channel_id = fields.CharField(255, null=True, description="频道id")
    """频道id"""
    group_name = fields.TextField(default="", description="群组名称")
    """群聊名称"""
    max_member_count = fields.IntField(default=0, description="最大人数")
    """最大人数"""
    member_count = fields.IntField(default=0, description="当前人数")
    """当前人数"""
    status = fields.BooleanField(default=True, description="群状态")
    """群状态"""
    level = fields.IntField(default=5, description="群权限")
    """群权限"""
    is_super = fields.BooleanField(
        default=False, description="超级用户指定，可以使用全局关闭的功能"
    )
    """超级用户指定群，可以使用全局关闭的功能"""
    group_flag = fields.IntField(default=0, description="群认证标记")
    """群认证标记"""
    block_plugin = fields.TextField(default="", description="旧版禁用插件")
    """旧版禁用插件，仅作导入群策略的来源"""
    superuser_block_plugin = fields.TextField(
        default="", description="旧版超级用户禁用插件"
    )
    """旧版超级用户禁用插件，仅作导入群策略的来源"""
    block_task = fields.TextField(default="", description="旧版禁用被动技能")
    """旧版禁用被动技能，仅作导入群策略的来源"""
    superuser_block_task = fields.TextField(
        default="", description="旧版超级用户禁用被动"
    )
    """旧版超级用户禁用被动，仅作导入群策略的来源"""
    platform = fields.CharField(255, default="qq", description="所属平台")
    """所属平台"""

    class Meta:  # pyright: ignore [reportIncompatibleVariableOverride]
        table = "group_console"
        table_description = "群组信息表"
        unique_together = ("group_id", "channel_id")
        indexes = [  # noqa: RUF012
            ("group_id",)
        ]

    enable_lock: ClassVar[list[DbLockType]] = [DbLockType.CREATE, DbLockType.UPSERT]
    """开启锁"""
    _root_group_locks: ClassVar[dict[str, asyncio.Lock]] = {}
    """普通群记录应用层锁，规避 channel_id=NULL 唯一键语义差异。"""

    @classmethod
    async def _get_task_modules(cls, *, default_status: bool) -> list[str]:
        """获取默认禁用的任务模块

        返回:
            list[str]: 任务模块列表
        """
        return cast(
            list[str],
            await TaskInfo.get_modules(
                default_status=default_status,
                load_status=None,
            ),
        )

    @classmethod
    async def _get_plugin_modules(cls, *, default_status: bool) -> list[str]:
        """获取进群默认状态为 default_status 的插件模块，范围为插件策略管理的插件类型

        参数:
            default_status: 进群默认状态

        返回:
            list[str]: 插件模块列表
        """
        from zhenxun.services.plugin_policy import _MANAGED_PLUGIN_TYPES

        return cast(
            list[str],
            await PluginInfo.get_plugins_values_list(
                "module",
                load_status=None,
                filter_parent=False,
                plugin_type__in=list(_MANAGED_PLUGIN_TYPES),
                default_status=default_status,
            ),
        )

    @classmethod
    async def create(
        cls, using_db: BaseDBAsyncClient | None = None, **kwargs: Any
    ) -> Self:
        """覆盖create方法"""
        group = await super().create(using_db=using_db, **kwargs)
        await GroupMemoryCache.upsert_from_model(group)

        return group

    @classmethod
    async def get_or_create(
        cls,
        defaults: dict | None = None,
        using_db: BaseDBAsyncClient | None = None,
        **kwargs: Any,
    ) -> tuple[Self, bool]:
        """覆盖get_or_create方法"""
        group, is_create = await super().get_or_create(
            defaults=defaults, using_db=using_db, **kwargs
        )
        if is_create:
            await GroupMemoryCache.upsert_from_model(group)

        return group, is_create

    @classmethod
    async def update_or_create(
        cls,
        defaults: dict | None = None,
        using_db: BaseDBAsyncClient | None = None,
        **kwargs: Any,
    ) -> tuple[Self, bool]:
        """覆盖update_or_create方法"""
        group, is_create = await super().update_or_create(
            defaults=defaults, using_db=using_db, **kwargs
        )
        if is_create:
            await GroupMemoryCache.upsert_from_model(group)

        return group, is_create

    @classmethod
    def _clean_root_group_defaults(cls, defaults: dict | None) -> dict[str, Any]:
        cleaned = {}
        for field, value in (defaults or {}).items():
            if field in {"id", "group_id", "channel_id", "channel_id__isnull"}:
                continue
            if value is None:
                continue
            cleaned[field] = value
        return cleaned

    @classmethod
    def _root_group_score(cls, group: Self) -> tuple[int, int]:
        score = 0
        score += 8 if group.group_name else 0
        score += 4 if group.max_member_count else 0
        score += 4 if group.member_count else 0
        score += 4 if group.group_flag else 0
        score += 4 if group.is_super else 0
        score += 3 if not group.status else 0
        score += 3 if group.level != 5 else 0
        return score, int(group.id or 0)

    @classmethod
    async def _deduplicate_root_group_records(cls, groups: list[Self]) -> Self:
        if len(groups) == 1:
            return groups[0]

        keep = max(groups, key=cls._root_group_score)
        newest_first = sorted(
            groups, key=lambda group: int(group.id or 0), reverse=True
        )

        merged = {
            "group_name": next(
                (g.group_name for g in newest_first if g.group_name), ""
            ),
            "max_member_count": max(g.max_member_count for g in groups),
            "member_count": max(g.member_count for g in groups),
            "status": all(g.status for g in groups),
            "level": min(g.level for g in groups),
            "is_super": any(g.is_super for g in groups),
            "group_flag": max(g.group_flag for g in groups),
            "platform": next((g.platform for g in newest_first if g.platform), "qq"),
        }
        for field in (
            "block_plugin",
            "superuser_block_plugin",
            "block_task",
            "superuser_block_task",
        ):
            values = {getattr(g, field) or "" for g in groups}
            merged[field] = values.pop() if len(values) == 1 else ""

        update_fields = []
        for field, value in merged.items():
            if getattr(keep, field) != value:
                setattr(keep, field, value)
                update_fields.append(field)
        if update_fields:
            await keep.save(update_fields=update_fields)

        duplicate_ids = [
            group.id for group in groups if group.id and group.id != keep.id
        ]
        if duplicate_ids:
            await cls.filter(id__in=duplicate_ids).delete()
        await GroupMemoryCache.upsert_from_model(keep)
        return keep

    @classmethod
    async def get_or_create_root_group(
        cls,
        group_id: str | int,
        defaults: dict | None = None,
        *,
        update_defaults: bool = False,
    ) -> tuple[Self, bool]:
        """获取或创建普通群记录，并收敛 channel_id=NULL 重复数据。

        普通群固定使用 ``channel_id IS NULL``；频道记录必须继续显式传
        ``channel_id`` 走原有 get_or_create/update_or_create。
        """
        gid = str(scoped_group_id(group_id)).strip()
        if not gid:
            raise ValueError("group_id cannot be empty")

        lock = cls._root_group_locks.setdefault(gid, asyncio.Lock())
        async with lock:
            defaults = cls._clean_root_group_defaults(defaults)
            records = await cls.filter(group_id=gid, channel_id__isnull=True).all()
            if records:
                group = await cls._deduplicate_root_group_records(records)
                if update_defaults:
                    update_fields = []
                    for field, value in defaults.items():
                        if hasattr(group, field) and getattr(group, field) != value:
                            setattr(group, field, value)
                            update_fields.append(field)
                    if update_fields:
                        await group.save(update_fields=update_fields)
                await GroupMemoryCache.upsert_from_model(group)
                return group, False

            group = await cls.create(group_id=gid, channel_id=None, **defaults)
            return group, True

    @classmethod
    async def _get_or_create_group_for_write(
        cls,
        group_id: str,
        channel_id: str | None,
        defaults: dict | None = None,
    ) -> tuple[Self, bool]:
        defaults = cls._clean_root_group_defaults(defaults)
        if channel_id:
            return await cls.get_or_create(
                group_id=group_id,
                channel_id=channel_id,
                defaults=defaults,
            )
        return await cls.get_or_create_root_group(group_id, defaults=defaults)

    async def save(self, *args, **kwargs):
        await super().save(*args, **kwargs)
        await GroupMemoryCache.upsert_from_model(self)

    async def delete(self, *args, **kwargs):
        group_id = self.group_id
        channel_id = self.channel_id
        await super().delete(*args, **kwargs)
        await GroupMemoryCache.remove(group_id, channel_id)

    @classmethod
    async def get_group(
        cls,
        group_id: str,
        channel_id: str | None = None,
        clean_duplicates: bool = True,
    ) -> "GroupSnapshot | None":
        return GroupMemoryCache.get_if_ready(scoped_group_id(group_id), channel_id)

    @classmethod
    async def get_group_db(
        cls,
        group_id: str,
        channel_id: str | None = None,
        clean_duplicates: bool = True,
    ) -> Self | None:
        """获取群组（数据库）"""
        group_id = scoped_group_id(group_id)
        if channel_id:
            return await cls.safe_get_or_none(
                group_id=group_id,
                channel_id=channel_id,
                clean_duplicates=clean_duplicates,
            )
        return await cls.safe_get_or_none(
            group_id=group_id,
            channel_id__isnull=True,
            clean_duplicates=clean_duplicates,
        )

    @classmethod
    async def is_super_group(cls, group_id: str) -> bool:
        group = GroupMemoryCache.get_if_ready(scoped_group_id(group_id), None)
        return bool(group and group.is_super)

    @classmethod
    async def is_superuser_block_plugin(cls, group_id: str, module: str) -> bool:
        from zhenxun.services.bot_group_policy import bot_group_policy_service

        return bool(
            await bot_group_policy_service.legacy_blocked(group_id, module, forced=True)
        )

    @classmethod
    async def is_block_plugin(cls, group_id: str, module: str) -> bool:
        from zhenxun.services.bot_group_policy import bot_group_policy_service

        return bool(await bot_group_policy_service.legacy_blocked(group_id, module))

    @classmethod
    async def set_block_plugin(
        cls,
        group_id: str,
        module: str,
        is_superuser: bool = False,
        platform: str | None = None,
        channel_id: str | None = None,
    ):
        from zhenxun.services.bot_group_policy import bot_group_policy_service

        return await bot_group_policy_service.legacy_set_feature(
            group_id,
            module,
            False,
            task=False,
            is_superuser=is_superuser,
            platform=platform,
            channel_id=channel_id,
        )

    @classmethod
    async def set_unblock_plugin(
        cls,
        group_id: str,
        module: str,
        is_superuser: bool = False,
        platform: str | None = None,
        channel_id: str | None = None,
    ):
        from zhenxun.services.bot_group_policy import bot_group_policy_service

        return await bot_group_policy_service.legacy_set_feature(
            group_id,
            module,
            True,
            task=False,
            is_superuser=is_superuser,
            platform=platform,
            channel_id=channel_id,
        )

    @classmethod
    async def is_normal_block_plugin(
        cls, group_id: str, module: str, channel_id: str | None = None
    ) -> bool:
        from zhenxun.services.bot_group_policy import bot_group_policy_service

        return bool(
            await bot_group_policy_service.legacy_blocked(
                group_id, module, channel_id=channel_id, forced=False
            )
        )

    @classmethod
    async def is_superuser_block_task(cls, group_id: str, task: str) -> bool:
        from zhenxun.services.bot_group_policy import bot_group_policy_service

        return bool(
            await bot_group_policy_service.legacy_blocked(
                group_id, task, kind="tasks", forced=True
            )
        )

    @classmethod
    async def is_block_task(
        cls, group_id: str, task: str, channel_id: str | None = None
    ) -> bool:
        from zhenxun.services.bot_group_policy import bot_group_policy_service

        return bool(
            await bot_group_policy_service.legacy_blocked(
                group_id, task, channel_id=channel_id, kind="tasks"
            )
        )

    @classmethod
    async def set_block_task(
        cls,
        group_id: str,
        task: str,
        is_superuser: bool = False,
        platform: str | None = None,
        channel_id: str | None = None,
    ):
        from zhenxun.services.bot_group_policy import bot_group_policy_service

        return await bot_group_policy_service.legacy_set_feature(
            group_id,
            task,
            False,
            task=True,
            is_superuser=is_superuser,
            platform=platform,
            channel_id=channel_id,
        )

    @classmethod
    async def set_unblock_task(
        cls,
        group_id: str,
        task: str,
        is_superuser: bool = False,
        platform: str | None = None,
        channel_id: str | None = None,
    ):
        from zhenxun.services.bot_group_policy import bot_group_policy_service

        return await bot_group_policy_service.legacy_set_feature(
            group_id,
            task,
            True,
            task=True,
            is_superuser=is_superuser,
            platform=platform,
            channel_id=channel_id,
        )

    @classmethod
    def _run_script(cls):
        return [
            CreateIndex(
                "group_console",
                ("group_id",),
                name="idx_group_console_group_null_channel",
                where="channel_id IS NULL",
            ),
            AlterColumnType("group_console", "block_plugin", "TEXT"),
            AlterColumnType("group_console", "block_task", "TEXT"),
        ]
