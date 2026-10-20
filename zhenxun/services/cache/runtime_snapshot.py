"""运行时缓存使用的不可变快照"""

from __future__ import annotations

from dataclasses import dataclass, fields
from functools import cached_property
import time
from typing import Any

from zhenxun.utils.enum import (
    BlockType,
    LimitCheckType,
    LimitWatchType,
    PluginLimitType,
    PluginType,
)


def _parse_block_modules(value: str) -> frozenset[str]:
    if not value:
        return frozenset()
    items = []
    for part in value.split("<"):
        part = part.strip()
        if not part:
            continue
        part = part.strip(",").strip()
        if part:
            items.append(part)
    return frozenset(items)


@dataclass(frozen=True)
class PluginInfoSnapshot:
    id: int
    module: str
    module_path: str
    name: str
    status: bool
    block_type: BlockType | None
    load_status: bool
    author: str | None
    version: str | None
    level: int
    default_status: bool
    limit_superuser: bool
    menu_type: str
    plugin_type: PluginType | None
    cost_gold: int
    admin_level: int | None
    ignore_prompt: bool
    is_delete: bool
    parent: str | None
    is_show: bool
    ignore_statistics: bool
    impression: float

    @classmethod
    def from_model(cls, model) -> "PluginInfoSnapshot":
        return cls.from_values(
            {
                item.name: getattr(model, item.name, None)
                for item in fields(cls)
                if hasattr(model, item.name)
            }
        )

    @classmethod
    def from_values(cls, values: dict[str, Any]) -> "PluginInfoSnapshot":
        """Construct a normalized snapshot from selected database fields."""
        return cls(
            id=int(values.get("id", 0) or 0),
            module=str(values.get("module", "") or ""),
            module_path=str(values.get("module_path", "") or ""),
            name=str(values.get("name", "") or ""),
            status=bool(values.get("status", True)),
            block_type=values.get("block_type", None),
            load_status=bool(values.get("load_status", True)),
            author=values.get("author", None),
            version=values.get("version", None),
            level=int(values.get("level", 0) or 0),
            default_status=bool(values.get("default_status", True)),
            limit_superuser=bool(values.get("limit_superuser", False)),
            menu_type=str(values.get("menu_type", "") or ""),
            plugin_type=values.get("plugin_type", None),
            cost_gold=int(values.get("cost_gold", 0) or 0),
            admin_level=values.get("admin_level", None),
            ignore_prompt=bool(values.get("ignore_prompt", False)),
            is_delete=bool(values.get("is_delete", False)),
            parent=values.get("parent", None),
            is_show=bool(values.get("is_show", True)),
            ignore_statistics=bool(values.get("ignore_statistics", False)),
            impression=float(values.get("impression", 0) or 0),
        )

    def to_model(self):
        from zhenxun.models.plugin_info import PluginInfo

        plugin = PluginInfo(
            id=self.id,
            module=self.module,
            module_path=self.module_path,
            name=self.name,
            status=self.status,
            block_type=self.block_type,
            load_status=self.load_status,
            author=self.author,
            version=self.version,
            level=self.level,
            default_status=self.default_status,
            limit_superuser=self.limit_superuser,
            menu_type=self.menu_type,
            plugin_type=self.plugin_type,
            cost_gold=self.cost_gold,
            admin_level=self.admin_level,
            ignore_prompt=self.ignore_prompt,
            is_delete=self.is_delete,
            parent=self.parent,
            is_show=self.is_show,
            ignore_statistics=self.ignore_statistics,
            impression=self.impression,
        )
        plugin._saved_in_db = True
        return plugin

    def to_payload(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "module": self.module,
            "module_path": self.module_path,
            "name": self.name,
            "status": self.status,
            "block_type": self.block_type.value if self.block_type else None,
            "load_status": self.load_status,
            "author": self.author,
            "version": self.version,
            "level": self.level,
            "default_status": self.default_status,
            "limit_superuser": self.limit_superuser,
            "menu_type": self.menu_type,
            "plugin_type": self.plugin_type.value if self.plugin_type else None,
            "cost_gold": self.cost_gold,
            "admin_level": self.admin_level,
            "ignore_prompt": self.ignore_prompt,
            "is_delete": self.is_delete,
            "parent": self.parent,
            "is_show": self.is_show,
            "ignore_statistics": self.ignore_statistics,
            "impression": self.impression,
        }

    @classmethod
    def from_payload(cls, payload: dict[str, Any]) -> "PluginInfoSnapshot":
        block_type = payload.get("block_type")
        if block_type is not None and not isinstance(block_type, BlockType):
            block_type = BlockType(block_type)
        plugin_type = payload.get("plugin_type")
        if plugin_type is not None and not isinstance(plugin_type, PluginType):
            plugin_type = PluginType(plugin_type)
        return cls(
            id=int(payload.get("id", 0) or 0),
            module=str(payload.get("module", "") or ""),
            module_path=str(payload.get("module_path", "") or ""),
            name=str(payload.get("name", "") or ""),
            status=bool(payload.get("status", True)),
            block_type=block_type,
            load_status=bool(payload.get("load_status", True)),
            author=payload.get("author"),
            version=payload.get("version"),
            level=int(payload.get("level", 0) or 0),
            default_status=bool(payload.get("default_status", True)),
            limit_superuser=bool(payload.get("limit_superuser", False)),
            menu_type=str(payload.get("menu_type", "") or ""),
            plugin_type=plugin_type,
            cost_gold=int(payload.get("cost_gold", 0) or 0),
            admin_level=payload.get("admin_level"),
            ignore_prompt=bool(payload.get("ignore_prompt", False)),
            is_delete=bool(payload.get("is_delete", False)),
            parent=payload.get("parent"),
            is_show=bool(payload.get("is_show", True)),
            ignore_statistics=bool(payload.get("ignore_statistics", False)),
            impression=float(payload.get("impression", 0) or 0),
        )


@dataclass(frozen=True)
class BanEntry:
    user_id: str | None
    group_id: str | None
    ban_level: int
    ban_time: int
    duration: int
    expire_at: float | None

    def remaining(self, now: float | None = None) -> int:
        if self.duration == -1:
            return -1
        now_ts = time.time() if now is None else now
        left = int(self.ban_time + self.duration - now_ts)
        return left if left > 0 else 0

    def to_payload(self) -> dict[str, Any]:
        return {
            "user_id": self.user_id,
            "group_id": self.group_id,
            "ban_level": self.ban_level,
            "ban_time": self.ban_time,
            "duration": self.duration,
            "expire_at": self.expire_at,
        }

    @classmethod
    def from_payload(cls, payload: dict[str, Any]) -> "BanEntry":
        user_id = payload.get("user_id")
        group_id = payload.get("group_id")
        ban_time = int(payload.get("ban_time", 0) or 0)
        duration = int(payload.get("duration", 0) or 0)
        expire_at = payload.get("expire_at")
        if expire_at is None and duration != -1:
            expire_at = float(ban_time + duration)
        return cls(
            user_id=str(user_id) if user_id else None,
            group_id=str(group_id) if group_id else None,
            ban_level=int(payload.get("ban_level", 0) or 0),
            ban_time=ban_time,
            duration=duration,
            expire_at=expire_at,
        )


@dataclass(frozen=True)
class BotSnapshot:
    bot_id: str
    status: bool
    platform: str | None
    block_plugins: str
    block_tasks: str

    @cached_property
    def block_plugins_set(self) -> frozenset[str]:
        return _parse_block_modules(self.block_plugins)

    @cached_property
    def block_tasks_set(self) -> frozenset[str]:
        return _parse_block_modules(self.block_tasks)

    @classmethod
    def from_model(cls, model) -> "BotSnapshot":
        return cls(
            bot_id=str(model.bot_id),
            status=bool(model.status),
            platform=getattr(model, "platform", None),
            block_plugins=getattr(model, "block_plugins", "") or "",
            block_tasks=getattr(model, "block_tasks", "") or "",
        )

    def to_payload(self) -> dict[str, Any]:
        return {
            "bot_id": self.bot_id,
            "status": self.status,
            "platform": self.platform,
            "block_plugins": self.block_plugins,
            "block_tasks": self.block_tasks,
        }

    @classmethod
    def from_payload(cls, payload: dict[str, Any]) -> "BotSnapshot":
        return cls(
            bot_id=str(payload.get("bot_id", "")),
            status=bool(payload.get("status", True)),
            platform=payload.get("platform"),
            block_plugins=payload.get("block_plugins", "") or "",
            block_tasks=payload.get("block_tasks", "") or "",
        )


@dataclass(frozen=True)
class GroupSnapshot:
    group_id: str
    channel_id: str | None
    group_name: str
    max_member_count: int
    member_count: int
    status: bool
    level: int
    is_super: bool
    group_flag: int
    platform: str | None

    @classmethod
    def from_model(cls, model) -> "GroupSnapshot":
        return cls(
            group_id=str(model.group_id),
            channel_id=getattr(model, "channel_id", None),
            group_name=getattr(model, "group_name", "") or "",
            max_member_count=int(getattr(model, "max_member_count", 0) or 0),
            member_count=int(getattr(model, "member_count", 0) or 0),
            status=bool(getattr(model, "status", True)),
            level=int(getattr(model, "level", 0) or 0),
            is_super=bool(getattr(model, "is_super", False)),
            group_flag=int(getattr(model, "group_flag", 0) or 0),
            platform=getattr(model, "platform", None),
        )

    def to_payload(self) -> dict[str, Any]:
        return {
            "group_id": self.group_id,
            "channel_id": self.channel_id,
            "group_name": self.group_name,
            "max_member_count": self.max_member_count,
            "member_count": self.member_count,
            "status": self.status,
            "level": self.level,
            "is_super": self.is_super,
            "group_flag": self.group_flag,
            "platform": self.platform,
        }

    @classmethod
    def from_payload(cls, payload: dict[str, Any]) -> "GroupSnapshot":
        return cls(
            group_id=str(payload.get("group_id", "")),
            channel_id=payload.get("channel_id"),
            group_name=payload.get("group_name", "") or "",
            max_member_count=int(payload.get("max_member_count", 0) or 0),
            member_count=int(payload.get("member_count", 0) or 0),
            status=bool(payload.get("status", True)),
            level=int(payload.get("level", 0) or 0),
            is_super=bool(payload.get("is_super", False)),
            group_flag=int(payload.get("group_flag", 0) or 0),
            platform=payload.get("platform"),
        )


@dataclass(frozen=True)
class LevelUserSnapshot:
    user_id: str
    group_id: str | None
    user_level: int
    group_flag: int

    @classmethod
    def from_model(cls, model) -> "LevelUserSnapshot":
        return cls(
            user_id=str(model.user_id),
            group_id=getattr(model, "group_id", None),
            user_level=int(getattr(model, "user_level", 0) or 0),
            group_flag=int(getattr(model, "group_flag", 0) or 0),
        )

    def to_payload(self) -> dict[str, Any]:
        return {
            "user_id": self.user_id,
            "group_id": self.group_id,
            "user_level": self.user_level,
            "group_flag": self.group_flag,
        }

    @classmethod
    def from_payload(cls, payload: dict[str, Any]) -> "LevelUserSnapshot":
        return cls(
            user_id=str(payload.get("user_id", "")),
            group_id=payload.get("group_id"),
            user_level=int(payload.get("user_level", 0) or 0),
            group_flag=int(payload.get("group_flag", 0) or 0),
        )


@dataclass(frozen=True)
class PluginLimitSnapshot:
    id: int
    module: str
    module_path: str
    limit_type: PluginLimitType
    watch_type: LimitWatchType
    check_type: LimitCheckType
    status: bool
    result: str | None
    cd: int | None
    max_count: int | None

    @classmethod
    def from_model(cls, model) -> "PluginLimitSnapshot":
        return cls(
            id=int(model.id),
            module=str(model.module),
            module_path=str(model.module_path),
            limit_type=model.limit_type,
            watch_type=model.watch_type,
            check_type=model.check_type,
            status=bool(model.status),
            result=getattr(model, "result", None),
            cd=getattr(model, "cd", None),
            max_count=getattr(model, "max_count", None),
        )

    def to_payload(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "module": self.module,
            "module_path": self.module_path,
            "limit_type": self.limit_type.value,
            "watch_type": self.watch_type.value,
            "check_type": self.check_type.value,
            "status": self.status,
            "result": self.result,
            "cd": self.cd,
            "max_count": self.max_count,
        }

    @classmethod
    def from_payload(cls, payload: dict[str, Any]) -> "PluginLimitSnapshot":
        return cls(
            id=int(payload.get("id", 0) or 0),
            module=str(payload.get("module", "")),
            module_path=str(payload.get("module_path", "")),
            limit_type=PluginLimitType(payload.get("limit_type", PluginLimitType.CD)),
            watch_type=LimitWatchType(payload.get("watch_type", LimitWatchType.USER)),
            check_type=LimitCheckType(payload.get("check_type", LimitCheckType.ALL)),
            status=bool(payload.get("status", True)),
            result=payload.get("result"),
            cd=payload.get("cd"),
            max_count=payload.get("max_count"),
        )


@dataclass(frozen=True)
class TaskInfoSnapshot:
    id: int
    module: str
    name: str
    status: bool
    load_status: bool
    default_status: bool
    run_time: str | None

    @classmethod
    def from_model(cls, model) -> "TaskInfoSnapshot":
        return cls(
            id=int(getattr(model, "id", 0) or 0),
            module=str(model.module),
            name=str(getattr(model, "name", "") or ""),
            status=bool(getattr(model, "status", True)),
            load_status=bool(getattr(model, "load_status", True)),
            default_status=bool(getattr(model, "default_status", True)),
            run_time=getattr(model, "run_time", None),
        )

    def to_payload(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "module": self.module,
            "name": self.name,
            "status": self.status,
            "load_status": self.load_status,
            "default_status": self.default_status,
            "run_time": self.run_time,
        }

    @classmethod
    def from_payload(cls, payload: dict[str, Any]) -> "TaskInfoSnapshot":
        return cls(
            id=int(payload.get("id", 0) or 0),
            module=str(payload.get("module", "")),
            name=str(payload.get("name", "") or ""),
            status=bool(payload.get("status", True)),
            load_status=bool(payload.get("load_status", True)),
            default_status=bool(payload.get("default_status", True)),
            run_time=payload.get("run_time"),
        )
