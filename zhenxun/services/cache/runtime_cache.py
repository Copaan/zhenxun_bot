"""运行时内存缓存：权限检查热路径使用的插件、Bot、群组、权限、被动任务、限制与封禁快照"""

from __future__ import annotations

import asyncio
from dataclasses import fields
import time
from typing import TYPE_CHECKING, Any, ClassVar

from zhenxun.services.db_context.write import deferred_mutation, write_scope
from zhenxun.services.log import logger
from zhenxun.utils.manager.priority_manager import PriorityLifecycle

from .runtime_snapshot import (
    BanEntry,
    BotSnapshot,
    GroupSnapshot,
    LevelUserSnapshot,
    PluginInfoSnapshot,
    PluginLimitSnapshot,
    TaskInfoSnapshot,
)
from .runtime_snapshot import _parse_block_modules as _parse_block_modules
from .runtime_sync import (
    LOG_COMMAND,
    RUNTIME_CACHES,
    RuntimeCacheMutation,
    RuntimeCacheSync,
    RuntimeMemoryCache,
    _spawn_runtime_task,
    runtime_cache_refresh_coordinator,
    set_lifecycle_context,
)

if TYPE_CHECKING:
    from zhenxun.models.plugin_info import PluginInfo

# RuntimeCache 以模型 save/delete 主动失效为主，周期 refresh 只做兜底。
# 这些默认值避免低压力运行时频繁全量扫表。
# 权限检查热路径以 RuntimeCache/AuthSnapshot 为唯一数据入口。
PLUGININFO_MEM_REFRESH_INTERVAL = 3600  # 60分钟
BAN_MEM_REFRESH_INTERVAL = 300
BAN_MEM_CLEAN_INTERVAL = 60
BAN_MEM_CLEANUP_DB = True
BOT_MEM_REFRESH_INTERVAL = 900  # 15分钟
GROUP_MEM_REFRESH_INTERVAL = 900  # 15分钟
LEVEL_MEM_REFRESH_INTERVAL = 900  # 15分钟
TASK_MEM_REFRESH_INTERVAL = 1800
LIMIT_MEM_REFRESH_INTERVAL = 900  # 15分钟

_CACHE_READY_EVENT = asyncio.Event()


def is_cache_ready() -> bool:
    return _CACHE_READY_EVENT.is_set()


async def wait_cache_ready(timeout: float) -> bool:
    """等待运行时缓存预热完成

    参数:
        timeout: 最长等待秒数

    返回:
        bool: 缓存是否已就绪
    """
    if _CACHE_READY_EVENT.is_set():
        return True
    try:
        await asyncio.wait_for(_CACHE_READY_EVENT.wait(), timeout)
    except asyncio.TimeoutError:
        return False
    return True


class PluginInfoMemoryCache(RuntimeMemoryCache):
    label = "plugin"
    table = "plugin_info"
    refresh_interval = PLUGININFO_MEM_REFRESH_INTERVAL
    indexes = ("_by_module",)
    _by_module: ClassVar[dict[str, PluginInfoSnapshot]] = {}
    _by_module_path: ClassVar[dict[str, PluginInfoSnapshot]] = {}

    @classmethod
    def _to_model(cls, snapshot: PluginInfoSnapshot | None) -> "PluginInfo | None":
        return snapshot.to_model() if snapshot else None

    @classmethod
    def _store_snapshot(cls, snapshot: PluginInfoSnapshot) -> None:
        if snapshot.module:
            old = cls._by_module.get(snapshot.module)
            if old and old.module_path != snapshot.module_path:
                cls._by_module_path.pop(old.module_path, None)
            cls._by_module[snapshot.module] = snapshot
        if snapshot.module_path:
            old = cls._by_module_path.get(snapshot.module_path)
            if old and old.module != snapshot.module:
                cls._by_module.pop(old.module, None)
            cls._by_module_path[snapshot.module_path] = snapshot

    @staticmethod
    def _module_snapshot_rank(snapshot: PluginInfoSnapshot) -> tuple[int, int]:
        return (1 if snapshot.load_status else 0, snapshot.id)

    @classmethod
    async def _refresh_snapshot(cls) -> None:
        from zhenxun.models.plugin_info import PluginInfo

        async with cls._lock:
            plugins = await RuntimeCacheMutation.read_db(
                cls,
                PluginInfo.all().values(
                    *(item.name for item in fields(PluginInfoSnapshot))
                ),
                operation="PluginInfoMemoryCache.refresh",
            )
            if plugins is None:
                return
            by_module: dict[str, PluginInfoSnapshot] = {}
            by_module_path: dict[str, PluginInfoSnapshot] = {}
            for plugin in plugins:
                snapshot = PluginInfoSnapshot.from_values(plugin)
                if snapshot.module:
                    current = by_module.get(snapshot.module)
                    if current is None or cls._module_snapshot_rank(
                        snapshot
                    ) >= cls._module_snapshot_rank(current):
                        by_module[snapshot.module] = snapshot
                if snapshot.module_path:
                    by_module_path[snapshot.module_path] = snapshot
            cls._by_module = by_module
            cls._by_module_path = by_module_path
            RuntimeCacheMutation.mark_refreshed(cls)
            logger.debug(
                f"plugin cache refreshed: {len(by_module)} entries", LOG_COMMAND
            )

    @classmethod
    async def get_by_module(cls, module: str) -> "PluginInfo | None":
        if not cls._loaded:
            await cls.ensure_loaded()
        return cls._to_model(cls._by_module.get(module))

    @classmethod
    async def get_all(cls) -> dict[str, "PluginInfo"]:
        if not cls._loaded:
            await cls.ensure_loaded()
        return {
            module: snapshot.to_model() for module, snapshot in cls._by_module.items()
        }

    @classmethod
    def get_by_module_path(cls, module_path: str) -> "PluginInfo | None":
        RuntimeCacheMutation.require_fresh(cls)
        return cls._to_model(cls._by_module_path.get(module_path))

    @classmethod
    async def upsert_from_model(cls, plugin) -> None:
        if plugin:
            await cls._upsert(PluginInfoSnapshot.from_model(plugin))

    @classmethod
    @deferred_mutation
    async def _upsert(cls, snapshot: PluginInfoSnapshot) -> None:
        async with cls._lock:
            cls._store_snapshot(snapshot)
        RuntimeCacheMutation.publish("plugin", "upsert", snapshot.to_payload())

    @classmethod
    async def upsert_from_payload(cls, payload: dict[str, Any]) -> None:
        try:
            snapshot = PluginInfoSnapshot.from_payload(payload)
        except Exception:
            return
        async with cls._lock:
            cls._store_snapshot(snapshot)

    @classmethod
    @deferred_mutation
    async def remove(
        cls, module: str | None = None, module_path: str | None = None
    ) -> None:
        if not module and not module_path:
            return
        async with cls._lock:
            if module:
                snapshot = cls._by_module.pop(module, None)
                if snapshot and snapshot.module_path:
                    cls._by_module_path.pop(snapshot.module_path, None)
            if module_path:
                snapshot = cls._by_module_path.pop(module_path, None)
                if snapshot and snapshot.module:
                    cls._by_module.pop(snapshot.module, None)
        RuntimeCacheMutation.publish(
            "plugin",
            "delete",
            {"module": module, "module_path": module_path},
        )

    @classmethod
    async def apply_sync_event(cls, action: str, data: dict[str, Any]) -> None:
        if action == "upsert":
            await cls.upsert_from_payload(data)
        elif action == "delete":
            await cls.remove(data.get("module"), data.get("module_path"))
        elif action == "refresh":
            await cls.refresh()


class BotMemoryCache(RuntimeMemoryCache):
    label = "bot"
    table = "bot_console"
    refresh_interval = BOT_MEM_REFRESH_INTERVAL
    indexes = ("_by_id",)
    _by_id: ClassVar[dict[str, BotSnapshot]] = {}

    @classmethod
    def _normalize(cls, bot_id: str | None) -> str | None:
        if bot_id is None:
            return None
        bot_id = bot_id.strip()
        return bot_id if bot_id else None

    @classmethod
    async def _refresh_snapshot(cls) -> None:
        from zhenxun.models.bot_console import BotConsole

        async with cls._lock:
            records = await RuntimeCacheMutation.read_db(
                cls,
                BotConsole.all(),
                operation="BotMemoryCache.refresh",
            )
            if records is None:
                return
            cls._by_id = {str(r.bot_id): BotSnapshot.from_model(r) for r in records}
            RuntimeCacheMutation.mark_refreshed(cls)
            logger.debug(f"bot cache refreshed: {len(cls._by_id)} entries", LOG_COMMAND)

    @classmethod
    async def get(cls, bot_id: str | None) -> BotSnapshot | None:
        bot_id = cls._normalize(bot_id)
        if not bot_id:
            return None
        if not cls._loaded:
            await cls.ensure_loaded()
        return cls._by_id.get(bot_id)

    @classmethod
    async def get_all(cls) -> dict[str, BotSnapshot]:
        if not cls._loaded:
            await cls.ensure_loaded()
        return dict(cls._by_id)

    @classmethod
    async def update_status(cls, bot_id: str | None, status: bool) -> None:
        bot_id = cls._normalize(bot_id)
        if not bot_id:
            return
        async with cls._lock:
            entry = cls._by_id.get(bot_id)
            if not entry:
                return
            updated = BotSnapshot(
                bot_id=entry.bot_id,
                status=bool(status),
                platform=entry.platform,
                block_plugins=entry.block_plugins,
                block_tasks=entry.block_tasks,
                available_plugins=entry.available_plugins,
                available_tasks=entry.available_tasks,
            )
            cls._by_id[bot_id] = updated
        RuntimeCacheMutation.publish("bot", "upsert", updated.to_payload())

    @classmethod
    async def upsert_from_model(cls, record) -> None:
        await cls._upsert(BotSnapshot.from_model(record))

    @classmethod
    @deferred_mutation
    async def _upsert(cls, entry: BotSnapshot) -> None:
        async with cls._lock:
            cls._by_id[entry.bot_id] = entry
        RuntimeCacheMutation.publish("bot", "upsert", entry.to_payload())

    @classmethod
    async def upsert_from_payload(cls, payload: dict[str, Any]) -> None:
        entry = BotSnapshot.from_payload(payload)
        if not entry.bot_id:
            return
        async with cls._lock:
            cls._by_id[entry.bot_id] = entry

    @classmethod
    @deferred_mutation
    async def remove(cls, bot_id: str | None) -> None:
        bot_id = cls._normalize(bot_id)
        if not bot_id:
            return
        async with cls._lock:
            cls._by_id.pop(bot_id, None)
        RuntimeCacheMutation.publish("bot", "delete", {"bot_id": bot_id})

    @classmethod
    async def apply_sync_event(cls, action: str, data: dict[str, Any]) -> None:
        if action == "upsert":
            await cls.upsert_from_payload(data)
        elif action == "delete":
            await cls.remove(data.get("bot_id"))
        elif action == "refresh":
            await cls.refresh()


class GroupMemoryCache(RuntimeMemoryCache):
    label = "group"
    table = "group_console"
    refresh_interval = GROUP_MEM_REFRESH_INTERVAL
    indexes = ("_by_key",)
    _by_key: ClassVar[dict[tuple[str, str], GroupSnapshot]] = {}

    @classmethod
    def _normalize(cls, value: str | None) -> str | None:
        if value is None:
            return None
        value = value.strip()
        return value if value else None

    @classmethod
    def _key(
        cls, group_id: str | None, channel_id: str | None
    ) -> tuple[str, str] | None:
        group_id = cls._normalize(group_id)
        if not group_id:
            return None
        channel_id = cls._normalize(channel_id) or ""
        return (group_id, channel_id)

    @classmethod
    async def _refresh_snapshot(cls) -> None:
        from zhenxun.models.group_console import GroupConsole

        async with cls._lock:
            records = await RuntimeCacheMutation.read_db(
                cls,
                GroupConsole.all(),
                operation="GroupMemoryCache.refresh",
            )
            if records is None:
                return
            by_key: dict[tuple[str, str], GroupSnapshot] = {}
            for record in records:
                entry = GroupSnapshot.from_model(record)
                key = cls._key(entry.group_id, entry.channel_id)
                if key:
                    by_key[key] = entry
            cls._by_key = by_key
            RuntimeCacheMutation.mark_refreshed(cls)
            logger.debug(f"group cache refreshed: {len(by_key)} entries", LOG_COMMAND)

    @classmethod
    async def get(
        cls, group_id: str | None, channel_id: str | None = None
    ) -> GroupSnapshot | None:
        key = cls._key(group_id, channel_id)
        if not key:
            return None
        if not cls._loaded:
            await cls.ensure_loaded()
        return cls._by_key.get(key)

    @classmethod
    def get_if_ready(
        cls, group_id: str | None, channel_id: str | None = None
    ) -> GroupSnapshot | None:
        RuntimeCacheMutation.require_fresh(cls, allow_unloaded=True)
        key = cls._key(group_id, channel_id)
        if not key:
            return None
        if not cls._loaded:
            return None
        return cls._by_key.get(key)

    @classmethod
    async def upsert_from_model(cls, record) -> None:
        await cls._upsert(GroupSnapshot.from_model(record))

    @classmethod
    @deferred_mutation
    async def _upsert(cls, entry: GroupSnapshot) -> None:
        key = cls._key(entry.group_id, entry.channel_id)
        if not key:
            return
        async with cls._lock:
            cls._by_key[key] = entry
        RuntimeCacheMutation.publish("group", "upsert", entry.to_payload())

    @classmethod
    async def upsert_from_payload(cls, payload: dict[str, Any]) -> None:
        entry = GroupSnapshot.from_payload(payload)
        key = cls._key(entry.group_id, entry.channel_id)
        if not key:
            return
        async with cls._lock:
            cls._by_key[key] = entry

    @classmethod
    @deferred_mutation
    async def remove(cls, group_id: str | None, channel_id: str | None = None) -> None:
        key = cls._key(group_id, channel_id)
        if not key:
            return
        async with cls._lock:
            cls._by_key.pop(key, None)
        RuntimeCacheMutation.publish(
            "group", "delete", {"group_id": key[0], "channel_id": key[1] or None}
        )

    @classmethod
    async def apply_sync_event(cls, action: str, data: dict[str, Any]) -> None:
        if action == "upsert":
            await cls.upsert_from_payload(data)
        elif action == "delete":
            await cls.remove(data.get("group_id"), data.get("channel_id"))
        elif action == "refresh":
            await cls.refresh()


class LevelUserMemoryCache(RuntimeMemoryCache):
    label = "level"
    table = "level_users"
    refresh_interval = LEVEL_MEM_REFRESH_INTERVAL
    indexes = ("_by_key",)
    _by_key: ClassVar[dict[tuple[str, str], LevelUserSnapshot]] = {}
    _by_user_max: ClassVar[dict[str, int]] = {}

    @classmethod
    def _normalize(cls, value: str | None) -> str | None:
        if value is None:
            return None
        value = value.strip()
        return value if value else ""

    @classmethod
    def _key(cls, user_id: str | None, group_id: str | None) -> tuple[str, str] | None:
        user_id = cls._normalize(user_id)
        if not user_id:
            return None
        group_id = cls._normalize(group_id) or ""
        return (user_id, group_id)

    @classmethod
    async def _refresh_snapshot(cls) -> None:
        from zhenxun.models.level_user import LevelUser

        async with cls._lock:
            records = await RuntimeCacheMutation.read_db(
                cls,
                LevelUser.all(),
                operation="LevelUserMemoryCache.refresh",
            )
            if records is None:
                return
            by_key: dict[tuple[str, str], LevelUserSnapshot] = {}
            by_user_max: dict[str, int] = {}
            for record in records:
                entry = LevelUserSnapshot.from_model(record)
                key = cls._key(entry.user_id, entry.group_id)
                if key:
                    by_key[key] = entry
                    current = by_user_max.get(entry.user_id, 0)
                    if entry.user_level > current:
                        by_user_max[entry.user_id] = entry.user_level
            cls._by_key = by_key
            cls._by_user_max = by_user_max
            RuntimeCacheMutation.mark_refreshed(cls)
            logger.debug(f"level cache refreshed: {len(by_key)} entries", LOG_COMMAND)

    @classmethod
    async def get(
        cls, user_id: str | None, group_id: str | None
    ) -> LevelUserSnapshot | None:
        key = cls._key(user_id, group_id)
        if not key:
            return None
        if not cls._loaded:
            await cls.ensure_loaded()
        return cls._by_key.get(key)

    @classmethod
    async def get_levels(
        cls, user_id: str | None, group_id: str | None
    ) -> tuple[LevelUserSnapshot | None, LevelUserSnapshot | None]:
        if not cls._loaded:
            await cls.ensure_loaded()
        global_user = None
        group_user = None
        global_key = cls._key(user_id, "")
        if global_key:
            global_user = cls._by_key.get(global_key)
        if group_id:
            group_key = cls._key(user_id, group_id)
            if group_key:
                group_user = cls._by_key.get(group_key)
        return global_user, group_user

    @classmethod
    async def get_max_level(cls, user_id: str | None) -> int:
        user_id = cls._normalize(user_id)
        if not user_id:
            return 0
        if not cls._loaded:
            await cls.ensure_loaded()
        return cls._by_user_max.get(user_id, 0)

    @classmethod
    async def upsert_from_model(cls, record) -> None:
        await cls._upsert(LevelUserSnapshot.from_model(record))

    @classmethod
    @deferred_mutation
    async def _upsert(cls, entry: LevelUserSnapshot) -> None:
        key = cls._key(entry.user_id, entry.group_id)
        if not key:
            return
        async with cls._lock:
            prev = cls._by_key.get(key)
            cls._by_key[key] = entry
            current = cls._by_user_max.get(entry.user_id, 0)
            if entry.user_level >= current:
                cls._by_user_max[entry.user_id] = entry.user_level
            elif prev and prev.user_level == current and entry.user_level < current:
                cls._recalc_user_max(entry.user_id)
        RuntimeCacheMutation.publish("level", "upsert", entry.to_payload())

    @classmethod
    async def upsert_from_payload(cls, payload: dict[str, Any]) -> None:
        entry = LevelUserSnapshot.from_payload(payload)
        key = cls._key(entry.user_id, entry.group_id)
        if not key:
            return
        async with cls._lock:
            prev = cls._by_key.get(key)
            cls._by_key[key] = entry
            current = cls._by_user_max.get(entry.user_id, 0)
            if entry.user_level >= current:
                cls._by_user_max[entry.user_id] = entry.user_level
            elif prev and prev.user_level == current and entry.user_level < current:
                cls._recalc_user_max(entry.user_id)

    @classmethod
    @deferred_mutation
    async def remove(cls, user_id: str | None, group_id: str | None) -> None:
        key = cls._key(user_id, group_id)
        if not key:
            return
        async with cls._lock:
            removed = cls._by_key.pop(key, None)
            if removed and cls._by_user_max.get(removed.user_id) == removed.user_level:
                cls._recalc_user_max(removed.user_id)
        RuntimeCacheMutation.publish(
            "level", "delete", {"user_id": key[0], "group_id": key[1] or None}
        )

    @classmethod
    def _recalc_user_max(cls, user_id: str) -> None:
        max_level = 0
        for entry in cls._by_key.values():
            if entry.user_id == user_id and entry.user_level > max_level:
                max_level = entry.user_level
        if max_level:
            cls._by_user_max[user_id] = max_level
        else:
            cls._by_user_max.pop(user_id, None)

    @classmethod
    async def apply_sync_event(cls, action: str, data: dict[str, Any]) -> None:
        if action == "upsert":
            await cls.upsert_from_payload(data)
        elif action == "delete":
            await cls.remove(data.get("user_id"), data.get("group_id"))
        elif action == "refresh":
            await cls.refresh()


class TaskInfoMemoryCache(RuntimeMemoryCache):
    label = "task"
    table = "task_info"
    refresh_interval = TASK_MEM_REFRESH_INTERVAL
    indexes = ("_by_module",)
    _by_module: ClassVar[dict[str, TaskInfoSnapshot]] = {}
    _by_name: ClassVar[dict[str, TaskInfoSnapshot]] = {}

    @classmethod
    def _normalize(cls, module: str | None) -> str | None:
        if module is None:
            return None
        module = module.strip()
        return module if module else None

    @classmethod
    async def _refresh_snapshot(cls) -> None:
        from zhenxun.models.task_info import TaskInfo

        async with cls._lock:
            records = await RuntimeCacheMutation.read_db(
                cls,
                TaskInfo.all(),
                operation="TaskInfoMemoryCache.refresh",
            )
            if records is None:
                return
            by_module: dict[str, TaskInfoSnapshot] = {}
            by_name: dict[str, TaskInfoSnapshot] = {}
            for record in records:
                entry = TaskInfoSnapshot.from_model(record)
                by_module[entry.module] = entry
                if entry.name:
                    by_name[entry.name] = entry
            cls._by_module = by_module
            cls._by_name = by_name
            RuntimeCacheMutation.mark_refreshed(cls)
            logger.debug(
                f"task info cache refreshed: {len(cls._by_module)} entries",
                LOG_COMMAND,
            )

    @classmethod
    async def get(cls, module: str | None) -> TaskInfoSnapshot | None:
        module = cls._normalize(module)
        if not module:
            return None
        if not cls._loaded:
            await cls.ensure_loaded()
        return cls._by_module.get(module)

    @classmethod
    async def get_by_name(cls, name: str | None) -> TaskInfoSnapshot | None:
        name = (name or "").strip()
        if not name:
            return None
        if not cls._loaded:
            await cls.ensure_loaded()
        return cls._by_name.get(name)

    @classmethod
    async def get_all(cls) -> list[TaskInfoSnapshot]:
        if not cls._loaded:
            await cls.ensure_loaded()
        return sorted(cls._by_module.values(), key=lambda item: (item.id, item.module))

    @classmethod
    async def is_runtime_disabled(cls, module: str | None) -> bool:
        """Return whether a passive task is unavailable at runtime.

        Runtime passive availability is defined by TaskInfo.status and
        TaskInfo.load_status. Bot/group scoped block lists are checked by
        CommonUtils.task_is_block().
        """
        entry = await cls.get(module)
        if not entry:
            return False
        return not entry.status or not entry.load_status

    @classmethod
    async def upsert_from_model(cls, record) -> None:
        await cls._upsert(TaskInfoSnapshot.from_model(record))

    @classmethod
    @deferred_mutation
    async def _upsert(cls, entry: TaskInfoSnapshot) -> None:
        async with cls._lock:
            cls._by_module[entry.module] = entry
            if entry.name:
                cls._by_name[entry.name] = entry
        RuntimeCacheMutation.publish("task", "upsert", entry.to_payload())

    @classmethod
    async def upsert_from_payload(cls, payload: dict[str, Any]) -> None:
        entry = TaskInfoSnapshot.from_payload(payload)
        if not entry.module:
            return
        async with cls._lock:
            cls._by_module[entry.module] = entry
            if entry.name:
                cls._by_name[entry.name] = entry

    @classmethod
    @deferred_mutation
    async def remove(cls, module: str | None) -> None:
        module = cls._normalize(module)
        if not module:
            return
        async with cls._lock:
            removed = cls._by_module.pop(module, None)
            if removed and removed.name:
                current = cls._by_name.get(removed.name)
                if current and current.module == removed.module:
                    cls._by_name.pop(removed.name, None)
        RuntimeCacheMutation.publish("task", "delete", {"module": module})

    @classmethod
    async def apply_sync_event(cls, action: str, data: dict[str, Any]) -> None:
        if action == "upsert":
            await cls.upsert_from_payload(data)
        elif action == "delete":
            await cls.remove(data.get("module"))
        elif action == "refresh":
            await cls.refresh()


class PluginLimitMemoryCache(RuntimeMemoryCache):
    label = "plugin_limit"
    table = "plugin_limit"
    refresh_interval = LIMIT_MEM_REFRESH_INTERVAL
    indexes = ("_by_id",)
    _by_id: ClassVar[dict[int, PluginLimitSnapshot]] = {}
    _by_module: ClassVar[dict[str, list[PluginLimitSnapshot]]] = {}

    @classmethod
    def _normalize(cls, value: str | None) -> str | None:
        if value is None:
            return None
        value = value.strip()
        return value if value else None

    @classmethod
    async def _refresh_snapshot(cls) -> None:
        from zhenxun.models.plugin_limit import PluginLimit

        async with cls._lock:
            records = await RuntimeCacheMutation.read_db(
                cls,
                PluginLimit.filter(status=True).all(),
                operation="PluginLimitMemoryCache.refresh",
            )
            if records is None:
                return
            by_id: dict[int, PluginLimitSnapshot] = {}
            by_module: dict[str, list[PluginLimitSnapshot]] = {}
            for record in records:
                entry = PluginLimitSnapshot.from_model(record)
                by_id[entry.id] = entry
                by_module.setdefault(entry.module, []).append(entry)
            cls._by_id = by_id
            cls._by_module = by_module
            RuntimeCacheMutation.mark_refreshed(cls)
            logger.debug(
                f"plugin limit cache refreshed: {len(by_id)} entries",
                LOG_COMMAND,
            )

    @classmethod
    async def get_limits(cls, module: str) -> list[PluginLimitSnapshot]:
        normalized = cls._normalize(module)
        if not normalized:
            return []
        module = normalized
        if not cls._loaded:
            await cls.ensure_loaded()
        return cls._by_module.get(module, [])

    @classmethod
    def get_all_limits(cls) -> list[PluginLimitSnapshot]:
        RuntimeCacheMutation.require_fresh(cls)
        return list(cls._by_id.values())

    @classmethod
    async def upsert_from_model(cls, record) -> None:
        await cls._upsert(PluginLimitSnapshot.from_model(record))

    @classmethod
    @deferred_mutation
    async def _upsert(cls, entry: PluginLimitSnapshot) -> None:
        await cls._upsert_entry(entry)
        RuntimeCacheMutation.publish("plugin_limit", "upsert", entry.to_payload())

    @classmethod
    async def upsert_from_payload(cls, payload: dict[str, Any]) -> None:
        try:
            entry = PluginLimitSnapshot.from_payload(payload)
        except Exception:
            return
        await cls._upsert_entry(entry)

    @classmethod
    async def _upsert_entry(cls, entry: PluginLimitSnapshot) -> None:
        async with cls._lock:
            prev = cls._by_id.get(entry.id)
            if prev and prev.module != entry.module:
                cls._by_module[prev.module] = [
                    item
                    for item in cls._by_module.get(prev.module, [])
                    if item.id != prev.id
                ]
            if not entry.status:
                cls._by_id.pop(entry.id, None)
                cls._by_module[entry.module] = [
                    item
                    for item in cls._by_module.get(entry.module, [])
                    if item.id != entry.id
                ]
                return
            cls._by_id[entry.id] = entry
            module_limits = [
                item
                for item in cls._by_module.get(entry.module, [])
                if item.id != entry.id
            ]
            module_limits.append(entry)
            cls._by_module[entry.module] = module_limits

    @classmethod
    @deferred_mutation
    async def remove_by_id(cls, limit_id: int | None) -> None:
        if not limit_id:
            return
        async with cls._lock:
            entry = cls._by_id.pop(limit_id, None)
            if entry:
                cls._by_module[entry.module] = [
                    item
                    for item in cls._by_module.get(entry.module, [])
                    if item.id != entry.id
                ]
        RuntimeCacheMutation.publish("plugin_limit", "delete", {"id": int(limit_id)})

    @classmethod
    async def apply_sync_event(cls, action: str, data: dict[str, Any]) -> None:
        if action == "upsert":
            await cls.upsert_from_payload(data)
        elif action == "delete":
            await cls.remove_by_id(data.get("id"))
        elif action == "refresh":
            await cls.refresh()


class BanMemoryCache(RuntimeMemoryCache):
    label = "ban"
    table = "ban_console"
    refresh_interval = BAN_MEM_REFRESH_INTERVAL
    indexes = (
        "_by_user",
        "_by_group",
        "_by_user_group",
    )
    _by_user: ClassVar[dict[str, BanEntry]] = {}
    _by_group: ClassVar[dict[str, BanEntry]] = {}
    _by_user_group: ClassVar[dict[tuple[str, str], BanEntry]] = {}
    _cleanup_task: ClassVar[asyncio.Task | None] = None
    _removing: ClassVar[dict[tuple[str | None, str | None], asyncio.Task]] = {}

    @classmethod
    def _normalize_id(cls, value: str | None) -> str | None:
        if value is None:
            return None
        value = value.strip()
        return value if value else None

    @classmethod
    def _build_entry(cls, record) -> BanEntry | None:
        user_id = cls._normalize_id(record.user_id)
        group_id = cls._normalize_id(record.group_id)
        duration = int(record.duration)
        if duration == -1:
            expire_at = None
        else:
            expire_at = float(record.ban_time + duration)
        return BanEntry(
            user_id=user_id,
            group_id=group_id,
            ban_level=int(record.ban_level),
            ban_time=int(record.ban_time),
            duration=duration,
            expire_at=expire_at,
        )

    @classmethod
    async def _refresh_snapshot(cls) -> None:
        from zhenxun.models.ban_console import BanConsole

        async with cls._lock:
            now_ts = time.time()
            records = await RuntimeCacheMutation.read_db(
                cls,
                BanConsole.all(),
                operation="BanMemoryCache.refresh",
            )
            if records is None:
                return
            by_user: dict[str, BanEntry] = {}
            by_group: dict[str, BanEntry] = {}
            by_user_group: dict[tuple[str, str], BanEntry] = {}
            for record in records:
                entry = cls._build_entry(record)
                if not entry:
                    continue
                if entry.expire_at is not None and entry.expire_at <= now_ts:
                    continue
                if entry.user_id and entry.group_id:
                    by_user_group[(entry.user_id, entry.group_id)] = entry
                elif entry.user_id:
                    by_user[entry.user_id] = entry
                elif entry.group_id:
                    by_group[entry.group_id] = entry
            cls._by_user = by_user
            cls._by_group = by_group
            cls._by_user_group = by_user_group
            RuntimeCacheMutation.mark_refreshed(cls)
            logger.debug(
                "ban cache refreshed: "
                f"user={len(by_user)} group={len(by_group)} "
                f"user_group={len(by_user_group)}",
                LOG_COMMAND,
            )

    @classmethod
    async def upsert_from_model(cls, record) -> None:
        if entry := cls._build_entry(record):
            await cls._upsert(entry)

    @classmethod
    @deferred_mutation
    async def _upsert(cls, entry: BanEntry) -> None:
        async with cls._lock:
            if entry.user_id and entry.group_id:
                cls._by_user_group[(entry.user_id, entry.group_id)] = entry
            elif entry.user_id:
                cls._by_user[entry.user_id] = entry
            elif entry.group_id:
                cls._by_group[entry.group_id] = entry
        RuntimeCacheMutation.publish("ban", "upsert", entry.to_payload())

    @classmethod
    @deferred_mutation
    async def remove(cls, user_id: str | None, group_id: str | None) -> None:
        await cls._remove_local(user_id, group_id)
        RuntimeCacheMutation.publish(
            "ban", "delete", {"user_id": user_id, "group_id": group_id}
        )

    @classmethod
    async def _remove_local(cls, user_id: str | None, group_id: str | None) -> None:
        user_id = cls._normalize_id(user_id)
        group_id = cls._normalize_id(group_id)
        async with cls._lock:
            if user_id and group_id:
                cls._by_user_group.pop((user_id, group_id), None)
            elif user_id:
                cls._by_user.pop(user_id, None)
            elif group_id:
                cls._by_group.pop(group_id, None)

    @classmethod
    def _get_entry(cls, user_id: str | None, group_id: str | None) -> BanEntry | None:
        user_id = cls._normalize_id(user_id)
        group_id = cls._normalize_id(group_id)
        if user_id and group_id:
            entry = cls._by_user_group.get((user_id, group_id))
            if entry:
                return entry
            entry = cls._by_user.get(user_id)
            if entry:
                return entry
            return None
        if user_id:
            return cls._by_user.get(user_id)
        if group_id:
            return cls._by_group.get(group_id)
        return None

    @classmethod
    def _active_entry(
        cls, user_id: str | None, group_id: str | None
    ) -> BanEntry | None:
        """获取未过期的封禁记录，已过期的记录异步移除

        参数:
            user_id: 用户id
            group_id: 群组id

        返回:
            BanEntry | None: 未过期的封禁记录
        """
        if not cls._loaded:
            return None
        entry = cls._get_entry(user_id, group_id)
        if entry is None or entry.remaining() != 0:
            return entry
        key = (entry.user_id, entry.group_id)
        if key not in cls._removing:
            task = _spawn_runtime_task(
                cls.remove(*key),
                name=f"{cls.__name__}-remove",
                degrade_on_error=False,
            )
            cls._removing[key] = task
            task.add_done_callback(lambda _: cls._removing.pop(key, None))
        return None

    @classmethod
    def is_banned(cls, user_id: str | None, group_id: str | None) -> bool:
        RuntimeCacheMutation.require_fresh(cls)
        return cls._active_entry(user_id, group_id) is not None

    @classmethod
    def remaining_time(cls, user_id: str | None, group_id: str | None) -> int:
        RuntimeCacheMutation.require_fresh(cls)
        entry = cls._active_entry(user_id, group_id)
        return entry.remaining() if entry else 0

    @classmethod
    def check_ban_level(
        cls, user_id: str | None, group_id: str | None, level: int
    ) -> bool:
        entry = cls._active_entry(user_id, group_id)
        return entry is not None and entry.ban_level <= level

    @classmethod
    async def cleanup_expired(cls, delete_db: bool = True) -> None:
        now_ts = time.time()
        expired: list[BanEntry] = []
        async with cls._lock:
            for entry in list(cls._by_user.values()):
                if entry.expire_at is not None and entry.expire_at <= now_ts:
                    expired.append(entry)
            for entry in list(cls._by_group.values()):
                if entry.expire_at is not None and entry.expire_at <= now_ts:
                    expired.append(entry)
            for entry in list(cls._by_user_group.values()):
                if entry.expire_at is not None and entry.expire_at <= now_ts:
                    expired.append(entry)
            for entry in expired:
                if entry.user_id and entry.group_id:
                    cls._by_user_group.pop((entry.user_id, entry.group_id), None)
                elif entry.user_id:
                    cls._by_user.pop(entry.user_id, None)
                elif entry.group_id:
                    cls._by_group.pop(entry.group_id, None)
        if not delete_db or not expired:
            return
        from tortoise.expressions import Q

        from zhenxun.models.ban_console import BanConsole

        async with write_scope():
            for entry in expired:
                query = BanConsole.filter()
                if entry.user_id:
                    query = query.filter(user_id=entry.user_id)
                else:
                    query = query.filter(Q(user_id__isnull=True) | Q(user_id=""))
                if entry.group_id:
                    query = query.filter(group_id=entry.group_id)
                else:
                    query = query.filter(Q(group_id__isnull=True) | Q(group_id=""))
                await query.delete()

    @classmethod
    async def _cleanup_loop(cls, interval: int, delete_db: bool) -> None:
        while True:
            await asyncio.sleep(interval)
            try:
                await cls.cleanup_expired(delete_db=delete_db)
            except Exception as exc:
                logger.error("ban cache cleanup failed", LOG_COMMAND, e=exc)

    @classmethod
    def start_cleanup_task(cls) -> None:
        interval = BAN_MEM_CLEAN_INTERVAL
        if interval > 0 and (not cls._cleanup_task or cls._cleanup_task.done()):
            cls._cleanup_task = _spawn_runtime_task(
                cls._cleanup_loop(interval, BAN_MEM_CLEANUP_DB),
                name=f"{cls.__name__}-cleanup",
            )

    @classmethod
    def stop_tasks(cls) -> None:
        if cls._cleanup_task and not cls._cleanup_task.done():
            cls._cleanup_task.cancel()
        cls._cleanup_task = None

    @classmethod
    async def upsert_from_payload(cls, payload: dict[str, Any]) -> None:
        entry = BanEntry.from_payload(payload)
        async with cls._lock:
            if entry.user_id and entry.group_id:
                cls._by_user_group[(entry.user_id, entry.group_id)] = entry
            elif entry.user_id:
                cls._by_user[entry.user_id] = entry
            elif entry.group_id:
                cls._by_group[entry.group_id] = entry

    @classmethod
    async def apply_sync_event(cls, action: str, data: dict[str, Any]) -> None:
        if action == "upsert":
            await cls.upsert_from_payload(data)
        elif action == "delete":
            await cls._remove_local(data.get("user_id"), data.get("group_id"))
        elif action == "refresh":
            await cls.refresh()


def health_snapshot() -> dict[str, dict[str, Any]]:
    """Return in-memory runtime cache health without touching the database."""
    return {label: cache.health() for label, cache in RUNTIME_CACHES.items()}


async def refresh_all_runtime_caches() -> dict[str, bool]:
    """Refresh authoritative runtime snapshots without clearing them first."""
    return await runtime_cache_refresh_coordinator.refresh_all()


@PriorityLifecycle.on_startup(
    priority=6,
    component_id="runtime:runtime_cache",
    depends_on=(
        "runtime:reconcile_config",
        "runtime:reconcile_tasks",
        "runtime:legacy_data",
    ),
    pass_context=True,
)
async def _init_runtime_cache(context):
    set_lifecycle_context(context)
    context.add_finalizer(_stop_runtime_cache)
    await RuntimeCacheSync.start()
    await runtime_cache_refresh_coordinator.start()
    outcome = await runtime_cache_refresh_coordinator.refresh_all(startup=True)
    if failed := sorted(name for name, ok in outcome.items() if not ok):
        logger.warning(
            f"运行时缓存启动加载未全部成功: {', '.join(failed)}（已排入退避重试）",
            LOG_COMMAND,
        )
    BanMemoryCache.start_cleanup_task()
    _CACHE_READY_EVENT.set()


async def _stop_runtime_cache():
    _CACHE_READY_EVENT.clear()
    await runtime_cache_refresh_coordinator.stop()
    BanMemoryCache.stop_tasks()
    await RuntimeCacheSync.stop()
    set_lifecycle_context(None)
