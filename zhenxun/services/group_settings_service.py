import asyncio
from contextlib import nullcontext
from copy import deepcopy
from typing import Any, TypeVar, overload

from pydantic import BaseModel, ValidationError
from tortoise.transactions import in_transaction
import ujson as json

from zhenxun.configs.config import Config
from zhenxun.models.group_plugin_setting import GroupPluginSetting
from zhenxun.services.cache import BoundedTTLCache
from zhenxun.services.cache.keyed import KeyedLocks
from zhenxun.services.cache.write import defer, in_write_transaction
from zhenxun.services.db_context import with_db_timeout
from zhenxun.services.log import logger
from zhenxun.utils.pydantic_compat import model_dump, parse_as

T = TypeVar("T", bound=BaseModel)


class GroupSettingsService:
    """
    一个用于管理插件分群配置的服务。
    集成了聚合缓存、批量操作和版本迁移功能。
    """

    def __init__(self):
        self._epoch = 0
        self._locks = KeyedLocks()
        self._cache = BoundedTTLCache(
            "GROUP_PLUGIN_SETTINGS_VIEW",
            ttl_seconds=600,
            max_items=10000,
        )

    @staticmethod
    def _build_cache_key(group_id: str, plugin_name: str) -> str:
        return f"{group_id}:{plugin_name}"

    async def invalidate_all(self) -> None:
        if defer(("group_settings_all", id(self)), self.invalidate_all):
            return
        self._epoch += 1
        await self._cache.clear()

    async def _clear_merged_cache(self, group_id: str, plugin_name: str) -> None:
        key = self._build_cache_key(group_id, plugin_name)
        if defer(
            ("group_settings", id(self), key),
            lambda: self._clear_merged_cache(group_id, plugin_name),
        ):
            return
        self._epoch += 1
        await self._cache.delete(key)

    async def _mutate(self, group_id, plugin_name, change, *, create=True):
        key = self._build_cache_key(group_id, plugin_name)
        async with nullcontext() if in_write_transaction() else self._locks.hold(key):
            async with in_transaction() as connection:
                row = (
                    await GroupPluginSetting.filter(
                        group_id=group_id,
                        plugin_name=plugin_name,
                    )
                    .using_db(connection)
                    .select_for_update()
                    .first()
                )
                created = False
                if row is None and create:
                    row, created = await GroupPluginSetting.get_or_create(
                        group_id=group_id,
                        plugin_name=plugin_name,
                        defaults={"settings": {}},
                        using_db=connection,
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
                await self._clear_merged_cache(group_id, plugin_name)
                return True, created

    async def set(
        self, group_id: str, plugin_name: str, settings_model: BaseModel
    ) -> None:
        values = deepcopy(model_dump(settings_model))
        await self._mutate(group_id, plugin_name, lambda _: values)

    async def set_key_value(
        self, group_id: str, plugin_name: str, key: str, value: Any
    ) -> None:
        def change(values):
            values[key] = deepcopy(value)
            return values

        await self._mutate(group_id, plugin_name, change)

    async def reset_key(self, group_id: str, plugin_name: str, key: str) -> bool:
        def change(values):
            if key not in values:
                return None
            values.pop(key)
            return values

        changed, _ = await self._mutate(group_id, plugin_name, change, create=False)
        return changed

    async def get(
        self, group_id: str, plugin_name: str, key: str, default: Any = None
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
        full_settings = await self.get_all_for_plugin(group_id, plugin_name)
        return full_settings.get(key, default)

    async def reset_all_for_plugin(self, group_id: str, plugin_name: str) -> bool:
        changed, _ = await self._mutate(
            group_id, plugin_name, lambda _: {}, create=False
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
        self, group_id: str, plugin_name: str, *, parse_model: type[T] | None = None
    ) -> T | dict[str, Any]:
        """
        获取一个插件在指定群组中的完整配置，应用了“继承与覆盖”逻辑。
        它首先获取全局默认配置，然后用数据库中存储的群组特定配置覆盖它。

        参数:
            group_id: 目标群组ID。
            plugin_name: 插件的模块名。
            parse_model: (可选) Pydantic模型，用于解析和验证配置。
        """
        cache_key = self._build_cache_key(group_id, plugin_name)

        def defaults():
            group = Config.get(plugin_name)
            return deepcopy(
                {key: group.get(key, build_model=False) for key in group.configs}
            )

        global_values = defaults()
        transactional = in_write_transaction()
        cached = None if transactional else await self._cache.get(cache_key)
        if cached is not None and cached[0] == global_values:
            final_settings = deepcopy(cached[1])
        else:
            async with nullcontext() if transactional else self._locks.hold(cache_key):
                global_values = defaults()
                cached = None if transactional else await self._cache.get(cache_key)
                if cached is not None and cached[0] == global_values:
                    final_settings = deepcopy(cached[1])
                else:
                    epoch = self._epoch
                    # A timeout must not be cached as an absent override.
                    loaded = True
                    try:
                        row = await with_db_timeout(
                            GroupPluginSetting.get_or_none(
                                group_id=group_id, plugin_name=plugin_name
                            ),
                            operation="group_settings.get",
                            source="group_settings",
                        )
                    except (TimeoutError, asyncio.TimeoutError):
                        row = None
                        loaded = False
                    final_settings = defaults()
                    global_values = deepcopy(final_settings)
                    if row and isinstance(row.settings, dict):
                        final_settings.update(deepcopy(row.settings))
                    await self._cache.set(
                        cache_key,
                        (global_values, deepcopy(final_settings)),
                        valid_if=lambda: loaded
                        and not transactional
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

    async def set_bulk(
        self, group_ids: list[str], plugin_name: str, key: str, value: Any
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

            changed, is_new = await self._mutate(group_id, plugin_name, change)
            if changed:
                created += int(is_new)
                updated += int(not is_new)
        return updated, created


group_settings_service = GroupSettingsService()
