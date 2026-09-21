import asyncio
from contextlib import nullcontext
from copy import deepcopy
from dataclasses import dataclass
import time
from typing import Any, TypeVar, overload

from pydantic import BaseModel, ValidationError
from tortoise.expressions import Q
from tortoise.transactions import in_transaction
import ujson as json

from zhenxun.configs.config import Config
from zhenxun.models.group_plugin_setting import GroupPluginSetting
from zhenxun.services.cache import BoundedTTLCache
from zhenxun.services.cache.keyed import KeyedLocks
from zhenxun.services.cache.write import CacheUnavailable, defer, in_write_transaction
from zhenxun.services.db_context import with_db_timeout
from zhenxun.services.log import logger
from zhenxun.utils.pydantic_compat import model_dump, parse_as

T = TypeVar("T", bound=BaseModel)
GROUP_SETTINGS_STALE_GRACE_SECONDS = 300.0


@dataclass(frozen=True, slots=True)
class GroupSettingScope:
    bot_id: str
    platform_scope: str
    channel_id: str


class GroupSettingsService:
    """
    一个用于管理插件分群配置的服务。
    集成了聚合缓存、批量操作和版本迁移功能。
    """

    def __init__(self):
        self._epoch = 0
        self._cache_version = 0
        self._locks = KeyedLocks()
        self._cache = BoundedTTLCache(
            "GROUP_PLUGIN_SETTINGS_VIEW",
            ttl_seconds=600,
            max_items=10000,
        )
        self._unavailable_log_at: dict[str, float] = {}

    def mark_stale(self) -> None:
        self._epoch += 1
        self._cache_version += 1

    @staticmethod
    def _clean(value: object | None) -> str:
        return str(value or "").strip()

    @staticmethod
    def _scope_values(scope: GroupSettingScope) -> dict[str, str]:
        return {
            "bot_id": scope.bot_id,
            "platform_scope": scope.platform_scope,
            "channel_id": scope.channel_id,
        }

    @classmethod
    def _current_scope(cls) -> GroupSettingScope | None:
        try:
            from nonebot.matcher import current_bot, current_event

            bot = current_bot.get()
            event = current_event.get()
            if bot is None or event is None:
                return None
            from zhenxun.utils.platform import PlatformUtils

            return GroupSettingScope(
                bot_id=cls._clean(PlatformUtils.get_storage_bot_id(bot)),
                platform_scope=cls._clean(PlatformUtils.get_platform_scope(bot)),
                channel_id=cls._clean(getattr(event, "channel_id", None)),
            )
        except Exception:
            return None

    @classmethod
    def _resolve_scope(
        cls,
        *,
        bot_id: str | None = None,
        platform_scope: str | None = None,
        channel_id: str | None = None,
    ) -> GroupSettingScope | None:
        if bot_id is None and platform_scope is None and channel_id is None:
            return cls._current_scope()
        values = GroupSettingScope(
            bot_id=cls._clean(bot_id),
            platform_scope=cls._clean(platform_scope),
            channel_id=cls._clean(channel_id),
        )
        return values if any(cls._scope_values(values).values()) else None

    @classmethod
    def _build_cache_key(
        cls, group_id: str, plugin_name: str, scope: GroupSettingScope | None
    ) -> str:
        scope_key = (
            "legacy" if scope is None else tuple(cls._scope_values(scope).values())
        )
        # JSON encoding keeps separators unambiguous even if an upstream ID
        # contains a colon or another delimiter used by older cache keys.
        return json.dumps(
            [scope_key, str(group_id), str(plugin_name)],
            ensure_ascii=False,
            separators=(",", ":"),
        )

    @staticmethod
    def _legacy_scope_q() -> Q:
        return (
            (Q(bot_id__isnull=True) | Q(bot_id=""))
            & (Q(platform_scope__isnull=True) | Q(platform_scope=""))
            & (Q(channel_id__isnull=True) | Q(channel_id=""))
        )

    @classmethod
    def _scope_filter(
        cls, group_id: str, plugin_name: str, scope: GroupSettingScope | None
    ) -> Q:
        base = Q(group_id=group_id, plugin_name=plugin_name)
        if scope is None:
            return base & cls._legacy_scope_q()
        return base & Q(
            bot_id=scope.bot_id,
            platform_scope=scope.platform_scope,
            channel_id=scope.channel_id,
        )

    @classmethod
    async def _find_exact_row(cls, group_id, plugin_name, scope, *, using_db=None):
        query = (
            GroupPluginSetting.filter(
                cls._scope_filter(group_id, plugin_name, scope)
            ).using_db(using_db)
            if using_db is not None
            else GroupPluginSetting.filter(
                cls._scope_filter(group_id, plugin_name, scope)
            )
        )
        return await query.first()

    @classmethod
    async def _find_row(cls, group_id, plugin_name, scope, *, using_db=None):
        row = await cls._find_exact_row(group_id, plugin_name, scope, using_db=using_db)
        if row is not None or scope is None:
            return row
        legacy = GroupPluginSetting.filter(
            cls._scope_filter(group_id, plugin_name, None)
        )
        if using_db is not None:
            legacy = legacy.using_db(using_db)
        return await legacy.first()

    async def invalidate_all(self) -> None:
        if defer(("group_settings_all", id(self)), self.invalidate_all):
            return
        self._epoch += 1
        self._cache_version += 1
        await self._cache.clear()

    async def _clear_merged_cache(
        self, group_id: str, plugin_name: str, scope: GroupSettingScope | None
    ) -> None:
        key = self._build_cache_key(group_id, plugin_name, scope)
        if defer(
            ("group_settings", id(self), key),
            lambda: self._clear_merged_cache(group_id, plugin_name, scope),
        ):
            return
        self._epoch += 1
        if scope is None:
            # A legacy row is the fallback for every scoped view. Removing only
            # the legacy cache key would leave those derived views stale.
            self._cache_version += 1
            await self._cache.clear()
            return
        await self._cache.delete(key)

    async def _mutate(
        self,
        group_id,
        plugin_name,
        change,
        *,
        create=True,
        bot_id=None,
        platform_scope=None,
        channel_id=None,
    ):
        scope = self._resolve_scope(
            bot_id=bot_id, platform_scope=platform_scope, channel_id=channel_id
        )
        key = self._build_cache_key(group_id, plugin_name, scope)
        async with nullcontext() if in_write_transaction() else self._locks.hold(key):
            async with in_transaction() as connection:
                # A scoped write must never mutate the legacy fallback row.  The
                # fallback is read-only inheritance; create an exact row when the
                # requested scope has no override yet.
                row = await self._find_exact_row(
                    group_id, plugin_name, scope, using_db=connection
                )
                if row is not None:
                    row = (
                        await GroupPluginSetting.filter(id=row.id)
                        .using_db(connection)
                        .select_for_update()
                        .get()
                    )
                created = False
                if row is None and create:
                    values = {
                        "group_id": group_id,
                        "plugin_name": plugin_name,
                        "settings": {},
                    }
                    if scope is not None:
                        values.update(self._scope_values(scope))
                    row, created = await GroupPluginSetting.get_or_create(
                        defaults=values,
                        using_db=connection,
                        **self._scope_filter_values(group_id, plugin_name, scope),
                    )
                    if not created:
                        row = (
                            await GroupPluginSetting.filter(id=row.id)
                            .using_db(connection)
                            .select_for_update()
                            .get()
                        )
                if row is None:
                    return False, False
                values = row.settings
                if isinstance(values, str):
                    values = json.loads(values)
                values = deepcopy(values) if isinstance(values, dict) else {}
                changed = change(values)
                if changed is None:
                    return False, created
                if changed:
                    row.settings = changed
                    await row.save(using_db=connection, update_fields=["settings"])
                else:
                    await row.delete(using_db=connection)
                await self._clear_merged_cache(group_id, plugin_name, scope)
                return True, created

    @staticmethod
    def _scope_filter_values(group_id, plugin_name, scope):
        values = {"group_id": group_id, "plugin_name": plugin_name}
        if scope is None:
            values.update({"bot_id": None, "platform_scope": None, "channel_id": None})
        else:
            values.update(GroupSettingsService._scope_values(scope))
        return values

    async def set(
        self,
        group_id: str,
        plugin_name: str,
        settings_model: BaseModel,
        *,
        bot_id: str | None = None,
        platform_scope: str | None = None,
        channel_id: str | None = None,
    ) -> None:
        values = deepcopy(model_dump(settings_model))
        await self._mutate(
            group_id,
            plugin_name,
            lambda _: values,
            bot_id=bot_id,
            platform_scope=platform_scope,
            channel_id=channel_id,
        )

    async def set_key_value(
        self,
        group_id: str,
        plugin_name: str,
        key: str,
        value: Any,
        *,
        bot_id: str | None = None,
        platform_scope: str | None = None,
        channel_id: str | None = None,
    ) -> None:
        def change(values):
            values[key] = deepcopy(value)
            return values

        await self._mutate(
            group_id,
            plugin_name,
            change,
            bot_id=bot_id,
            platform_scope=platform_scope,
            channel_id=channel_id,
        )

    async def reset_key(
        self,
        group_id: str,
        plugin_name: str,
        key: str,
        *,
        bot_id: str | None = None,
        platform_scope: str | None = None,
        channel_id: str | None = None,
    ) -> bool:
        def change(values):
            if key not in values:
                return None
            values.pop(key)
            return values

        changed, _ = await self._mutate(
            group_id,
            plugin_name,
            change,
            create=False,
            bot_id=bot_id,
            platform_scope=platform_scope,
            channel_id=channel_id,
        )
        return changed

    async def get(
        self,
        group_id: str,
        plugin_name: str,
        key: str,
        default: Any = None,
        *,
        bot_id: str | None = None,
        platform_scope: str | None = None,
        channel_id: str | None = None,
    ) -> Any:
        """
        获取一个分群配置项的值，如果群组未单独设置，则回退到全局默认值。

        参数:
            group_id: 目标群组ID。
            plugin_name: 插件的模块名。
            key: 配置项的键。
            default: 如果找不到配置项，返回的默认值。

        返回:
            配置项的值。
        """
        full_settings = await self.get_all_for_plugin(
            group_id,
            plugin_name,
            bot_id=bot_id,
            platform_scope=platform_scope,
            channel_id=channel_id,
        )
        return full_settings.get(key, default)

    async def reset_all_for_plugin(
        self,
        group_id: str,
        plugin_name: str,
        *,
        bot_id: str | None = None,
        platform_scope: str | None = None,
        channel_id: str | None = None,
    ) -> bool:
        changed, _ = await self._mutate(
            group_id,
            plugin_name,
            lambda _: {},
            create=False,
            bot_id=bot_id,
            platform_scope=platform_scope,
            channel_id=channel_id,
        )
        return changed

    @overload
    async def get_all_for_plugin(
        self, group_id: str, plugin_name: str, *, parse_model: type[T]
    ) -> T: ...

    @overload
    async def get_all_for_plugin(
        self, group_id: str, plugin_name: str, *, parse_model: None = None
    ) -> dict[str, Any]: ...

    async def get_all_for_plugin(
        self,
        group_id: str,
        plugin_name: str,
        *,
        parse_model: type[T] | None = None,
        bot_id: str | None = None,
        platform_scope: str | None = None,
        channel_id: str | None = None,
    ) -> T | dict[str, Any]:
        """
        获取一个插件在指定群组中的完整配置，应用了“继承与覆盖”逻辑。
        它首先获取全局默认配置，然后用数据库中存储的群组特定配置覆盖它。

        参数:
            group_id: 目标群组ID。
            plugin_name: 插件的模块名。
            parse_model: (可选) Pydantic模型，用于解析和验证配置。
        """
        scope = self._resolve_scope(
            bot_id=bot_id, platform_scope=platform_scope, channel_id=channel_id
        )
        cache_key = self._build_cache_key(group_id, plugin_name, scope)

        def defaults():
            group = Config.get(plugin_name)
            return deepcopy(
                {key: group.get(key, build_model=False) for key in group.configs}
            )

        global_values = defaults()
        transactional = in_write_transaction()
        cached = None if transactional else await self._cache.get(cache_key)
        if (
            cached is not None
            and isinstance(cached, tuple)
            and len(cached) >= 3
            and cached[2] == self._cache_version
            and cached[0] == global_values
        ):
            final_settings = deepcopy(cached[1])
        else:
            async with nullcontext() if transactional else self._locks.hold(cache_key):
                global_values = defaults()
                cached = None if transactional else await self._cache.get(cache_key)
                if (
                    cached is not None
                    and isinstance(cached, tuple)
                    and len(cached) >= 3
                    and cached[2] == self._cache_version
                    and cached[0] == global_values
                ):
                    final_settings = deepcopy(cached[1])
                else:
                    epoch = self._epoch
                    # A timeout must not be cached as an absent override.
                    loaded = True
                    try:
                        row = await with_db_timeout(
                            self._find_row(group_id, plugin_name, scope),
                            operation="group_settings.get",
                            source="group_settings",
                        )
                    except (TimeoutError, asyncio.TimeoutError) as error:
                        stale = self._stale_cached_value(cached, global_values)
                        if stale is None:
                            self._log_unavailable(cache_key)
                            raise CacheUnavailable(
                                f"group_settings_unavailable:{group_id}:{plugin_name}"
                            ) from error
                        final_settings = stale
                        loaded = False
                        row = None
                    if loaded:
                        final_settings = defaults()
                        global_values = deepcopy(final_settings)
                        if row and isinstance(row.settings, dict):
                            final_settings.update(deepcopy(row.settings))
                        await self._cache.set(
                            cache_key,
                            (
                                global_values,
                                deepcopy(final_settings),
                                self._cache_version,
                                time.monotonic(),
                            ),
                            valid_if=lambda: not transactional
                            and epoch == self._epoch
                            and defaults() == global_values,
                        )
        if parse_model:
            try:
                return parse_as(parse_model, final_settings)
            except (ValidationError, TypeError):
                logger.warning(
                    f"Plugin '{plugin_name}' group settings validation failed"
                )
                return parse_as(parse_model, {})
        return final_settings

    @staticmethod
    def _stale_cached_value(cached, global_values):
        if not isinstance(cached, tuple) or len(cached) < 4:
            return None
        if cached[0] != global_values:
            return None
        if time.monotonic() - float(cached[3]) > GROUP_SETTINGS_STALE_GRACE_SECONDS:
            return None
        return deepcopy(cached[1])

    def _log_unavailable(self, key: str) -> None:
        now = time.monotonic()
        if now - self._unavailable_log_at.get(key, 0.0) < 60:
            return
        self._unavailable_log_at[key] = now
        logger.warning(f"群组插件配置暂不可用，未回退到默认值: {key}")

    async def set_bulk(
        self,
        group_ids: list[str],
        plugin_name: str,
        key: str,
        value: Any,
        *,
        bot_id: str | None = None,
        platform_scope: str | None = None,
        channel_id: str | None = None,
    ) -> tuple[int, int]:
        """
        为多个群组批量设置同一个配置项。

        参数:
            group_ids: 目标群组ID列表。
            plugin_name: 插件模块名。
            key: 配置项的键。
            value: 要设置的值。

        返回:
            一个元组 (updated_count, created_count)。
        """
        if not group_ids:
            return 0, 0

        updated = created = 0
        for group_id in dict.fromkeys(group_ids):

            def change(values):
                values[key] = deepcopy(value)
                return values

            changed, is_new = await self._mutate(
                group_id,
                plugin_name,
                change,
                bot_id=bot_id,
                platform_scope=platform_scope,
                channel_id=channel_id,
            )
            if changed:
                created += int(is_new)
                updated += int(not is_new)
        return updated, created


group_settings_service = GroupSettingsService()
