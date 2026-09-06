from pathlib import Path

import nonebot
from nonebot import get_loaded_plugins
from nonebot.drivers import Driver
from nonebot.plugin import Plugin, PluginMetadata
from ruamel.yaml import YAML

from zhenxun.configs.utils import PluginExtraData, PluginSetting
from zhenxun.models.plugin_info import PluginInfo
from zhenxun.models.plugin_limit import PluginLimit
from zhenxun.services.log import logger
from zhenxun.services.startup_reconcile import (
    commit as commit_reconcile,
)
from zhenxun.services.startup_reconcile import (
    matches as reconcile_matches,
)
from zhenxun.services.startup_reconcile import (
    payload_fingerprint,
)
from zhenxun.utils.enum import PluginType
from zhenxun.utils.manager.priority_manager import PriorityLifecycle

from .manager import manager

_yaml = YAML(pure=True)
_yaml.allow_unicode = True
_yaml.indent = 2

driver: Driver = nonebot.get_driver()


async def _handle_setting(
    plugin: Plugin,
    plugin_list: list[PluginInfo],
    limit_list: list[PluginLimit],
):
    """处理插件设置

    参数:
        plugin: Plugin
        plugin_list: 插件列表
        limit_list: 插件限制列表
    """
    metadata = plugin.metadata
    if not metadata:
        if not plugin.sub_plugins:
            return
        """父插件"""
        metadata = PluginMetadata(name=plugin.name, description="", usage="")
    extra = metadata.extra
    extra_data = PluginExtraData(**extra)
    logger.debug(f"{metadata.name}:{plugin.name} -> {extra}", "初始化插件数据")
    setting = extra_data.setting or PluginSetting()
    if metadata.type == "library":
        extra_data.plugin_type = PluginType.HIDDEN
    if extra_data.plugin_type == PluginType.HIDDEN:
        extra_data.menu_type = ""
    if plugin.sub_plugins:
        extra_data.plugin_type = PluginType.PARENT
    plugin_list.append(
        PluginInfo(
            module=plugin.name,
            module_path=plugin.module_name,
            name=metadata.name,
            author=extra_data.author,
            version=extra_data.version,
            level=setting.level,
            default_status=setting.default_status,
            limit_superuser=setting.limit_superuser,
            menu_type=extra_data.menu_type,
            cost_gold=setting.cost_gold,
            plugin_type=extra_data.plugin_type,
            admin_level=extra_data.admin_level,
            is_show=extra_data.is_show,
            ignore_prompt=extra_data.ignore_prompt,
            parent=(plugin.parent_plugin.module_name if plugin.parent_plugin else None),
            impression=setting.impression,
            ignore_statistics=extra_data.ignore_statistics,
        )
    )
    if extra_data.limits:
        limit_list.extend(
            PluginLimit(
                module=plugin.name,
                module_path=plugin.module_name,
                limit_type=limit._type,
                watch_type=limit.watch_type,
                status=limit.status,
                check_type=limit.check_type,
                result=limit.result,
                cd=getattr(limit, "cd", None),
                max_count=getattr(limit, "max_count", None),
            )
            for limit in extra_data.limits
        )


_PLUGIN_FIELDS = [
    "module",
    "name",
    "author",
    "version",
    "level",
    "default_status",
    "limit_superuser",
    "menu_type",
    "cost_gold",
    "plugin_type",
    "admin_level",
    "is_show",
    "ignore_prompt",
    "parent",
    "impression",
    "ignore_statistics",
]

# Existing administrator settings are not plugin declaration defaults.
_PLUGIN_REFRESH_FIELDS = [
    "module",
    "name",
    "author",
    "version",
    "admin_level",
    "parent",
    "is_show",
    "ignore_prompt",
    "ignore_statistics",
]


def _value(value):
    return getattr(value, "value", value)


def _plugin_payload(plugin: PluginInfo) -> dict:
    return {field: _value(getattr(plugin, field, None)) for field in _PLUGIN_FIELDS} | {
        "module_path": plugin.module_path
    }


def _file_digest(path: Path) -> str:
    try:
        return payload_fingerprint(path.read_text(encoding="utf-8"))
    except OSError:
        return "missing"


@PriorityLifecycle.on_startup(
    priority=4,
    task_id="runtime:reconcile_plugins",
    depends_on=("runtime:reconcile_config",),
)
async def reconcile_plugin_runtime():
    """
    初始化插件数据配置
    """
    plugin_list: list[PluginInfo] = []
    limit_list: list[PluginLimit] = []
    load_plugin = []
    from zhenxun.services.startup_load import startup_load_planner

    for plugin in get_loaded_plugins():
        if startup_load_planner.plugin_available(plugin.module_name):
            load_plugin.append(plugin.module_name)
        await _handle_setting(plugin, plugin_list, limit_list)
    manager.init()
    plugin_payload = {
        "plugins": sorted(
            (_plugin_payload(plugin) for plugin in plugin_list),
            key=lambda item: item["module_path"],
        ),
        "loaded": sorted(load_plugin),
    }
    limit_payload = {
        "limits": sorted(
            (
                {
                    "module": limit.module,
                    "module_path": limit.module_path,
                    "limit_type": _value(limit.limit_type),
                    "watch_type": _value(limit.watch_type),
                    "status": limit.status,
                    "check_type": _value(limit.check_type),
                    "result": limit.result,
                    "cd": limit.cd,
                    "max_count": limit.max_count,
                }
                for limit in limit_list
            ),
            key=lambda item: (item["module_path"], str(item["limit_type"])),
        ),
        "limit_files": {
            str(path): _file_digest(path)
            for path in (manager.cd_file, manager.block_file, manager.count_file)
        },
    }
    plugin_fingerprint = payload_fingerprint(plugin_payload)
    limit_fingerprint = payload_fingerprint(limit_payload)
    plugins_changed = not reconcile_matches("plugins", plugin_fingerprint)
    limits_changed = not reconcile_matches("plugin_limits", limit_fingerprint)
    if not plugins_changed:
        database_plugins = await PluginInfo.all().values("module_path", "load_status")
        known_paths = {str(item["module_path"]) for item in database_plugins}
        loaded_paths = {
            str(item["module_path"]) for item in database_plugins if item["load_status"]
        }
        desired_paths = {plugin.module_path for plugin in plugin_list}
        plugins_changed = not desired_paths <= known_paths or loaded_paths != set(
            load_plugin
        )
    if not plugins_changed and not limits_changed:
        logger.debug("插件元数据与限制声明未变化，跳过启动期数据库写入")
        return

    existing_plugins = await PluginInfo.all()
    existing_by_path = {plugin.module_path: plugin for plugin in existing_plugins}
    create_list = []
    update_list = []
    type_updates = []
    for plugin in plugin_list:
        existing = existing_by_path.get(plugin.module_path)
        if existing is None:
            create_list.append(plugin)
        else:
            changed = False
            if (
                existing.plugin_type is None
                or plugin.plugin_type in {PluginType.HIDDEN, PluginType.PARENT}
            ) and existing.plugin_type != plugin.plugin_type:
                existing.plugin_type = plugin.plugin_type
                type_updates.append(existing)
            for field in _PLUGIN_REFRESH_FIELDS:
                desired = getattr(plugin, field, None)
                if _value(getattr(existing, field, None)) != _value(desired):
                    setattr(existing, field, desired)
                    changed = True
            if changed:
                update_list.append(existing)
    if plugins_changed:
        if create_list:
            await PluginInfo.bulk_create(create_list, 10)
        if update_list:
            await PluginInfo.bulk_update(
                update_list,
                _PLUGIN_REFRESH_FIELDS,
                20,
            )
        for plugin in type_updates:
            await PluginInfo.filter(id=plugin.id).update(plugin_type=plugin.plugin_type)
        current_loaded = {
            plugin.module_path for plugin in existing_plugins if plugin.load_status
        }
        desired_loaded = set(load_plugin)
        if current_loaded != desired_loaded:
            if desired_loaded:
                await PluginInfo.filter(module_path__in=desired_loaded).update(
                    load_status=True
                )
            await PluginInfo.filter(module_path__not_in=desired_loaded).update(
                load_status=False
            )
    from zhenxun.services.cache.runtime_cache import PluginInfoMemoryCache

    if plugins_changed:
        await PluginInfoMemoryCache.refresh()
        commit_reconcile("plugins", plugin_fingerprint)
    if limits_changed:
        if limit_list:
            for limit in limit_list:
                if not manager.exists(limit.module, limit.limit_type):
                    manager.add(limit.module, limit)
        manager.save_file()
        await manager.load_to_db()
        limit_payload["limit_files"] = {
            str(path): _file_digest(path)
            for path in (manager.cd_file, manager.block_file, manager.count_file)
        }
        commit_reconcile("plugin_limits", payload_fingerprint(limit_payload))
