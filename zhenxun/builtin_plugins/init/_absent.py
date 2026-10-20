from importlib.machinery import PathFinder, all_suffixes
import os
from pathlib import Path
import sys

from tortoise.expressions import F

from zhenxun.configs.config import BotConfig, Config
from zhenxun.configs.path_config import DATA_PATH
from zhenxun.models.bot_console import BotConsole
from zhenxun.models.bot_group_policy import BotGroupPluginPolicy
from zhenxun.models.group_plugin_setting import GroupPluginSetting
from zhenxun.models.plugin_info import PluginInfo
from zhenxun.models.plugin_policy import PluginPolicy
from zhenxun.models.task_info import TaskInfo
from zhenxun.services.log import logger
from zhenxun.utils.atomic_json import read_json_locked, write_json_locked
from zhenxun.utils.common_utils import CommonUtils

from .manager import manager

ABSENCE_FILE = DATA_PATH / "runtime" / "plugin-absence-v1.json"
"""插件连续未找到的启动次数"""
ABSENCE_LIMIT = 5
"""连续未找到达到该次数后清理插件数据"""
LOG_COMMAND = "插件清理"


def _has_source(path: Path) -> bool:
    """目录下（不含 __pycache__）存在 Python 模块文件

    参数:
        path: 目录

    返回:
        bool: 存在模块文件
    """
    suffixes = tuple(all_suffixes())
    for _, dirs, files in os.walk(path):
        dirs[:] = [name for name in dirs if name != "__pycache__"]
        if any(name.endswith(suffixes) for name in files):
            return True
    return False


def _module_exists(module_path: str, roots: list[str]) -> bool:
    """按导入路径查找模块文件，不执行插件代码，没有模块文件的残留目录视为不存在

    参数:
        module_path: 模块路径
        roots: 顶层模块的搜索目录

    返回:
        bool: 模块文件存在
    """
    top, *parts = module_path.split(".")
    spec = PathFinder.find_spec(top, roots)
    if spec is None:
        return False
    locations = [Path(path) for path in spec.submodule_search_locations or []]
    if spec.origin is None:
        locations = [path for path in locations if _has_source(path)]
        if not locations:
            return False
    for name in parts:
        package = next(
            (
                path / name
                for path in locations
                if (path / name).is_dir() and _has_source(path / name)
            ),
            None,
        )
        if package is not None:
            locations = [package]
        elif any(
            (path / f"{name}{suffix}").is_file()
            for path in locations
            for suffix in all_suffixes()
        ):
            locations = []
        else:
            return False
    return True


def _search_roots() -> list[str]:
    """插件可能所在的顶层目录：项目根目录、额外插件目录与 sys.path"""
    roots = [str(Path.cwd())]
    roots += [str(Path(path.strip()).resolve()) for path in BotConfig.ext_path if path]
    roots += [path for path in sys.path if isinstance(path, str)]
    return list(dict.fromkeys(roots))


def _layer_unsettled() -> bool:
    """依赖层有未结束的事务，或最近一次事务失败/回滚

    此时商店插件可能只是暂时缺失，不计入未找到次数
    """
    from zhenxun.services.nonebot_store.storage import (
        active_transaction,
        pending_transaction,
        settled_transactions,
    )

    if active_transaction():
        return True
    latest = pending_transaction() or next(iter(settled_transactions()), None)
    return bool(
        latest and (latest.get("state") == "failed" or latest.get("rollback_confirmed"))
    )


def _without(values, modules: set[str]) -> list[str]:
    return [value for value in values or [] if value not in modules]


def _without_text(value: str, modules: set[str]) -> str:
    return CommonUtils.convert_module_format(
        _without(CommonUtils.convert_module_format(value), modules)
    )


async def _clean_switches(plugins: set[str], tasks: set[str]) -> None:
    """从账号与插件策略的开关列表中移除插件与被动技能

    参数:
        plugins: 插件模块名
        tasks: 被动技能模块名
    """
    policy_fields = {
        PluginPolicy: {"block_plugins": plugins, "block_tasks": tasks},
        BotGroupPluginPolicy: {
            "block_plugins": plugins,
            "forced_plugins": plugins,
            "block_tasks": tasks,
            "forced_tasks": tasks,
        },
    }
    for model, fields in policy_fields.items():
        for policy in await model.all():
            values = {
                field: _without(getattr(policy, field), modules)
                for field, modules in fields.items()
            }
            if any((getattr(policy, k) or []) != v for k, v in values.items()):
                await model.filter(id=policy.id).update(
                    **values, revision=F("revision") + 1
                )
    fields = {"block_plugins": plugins, "block_tasks": tasks}
    for item in await BotConsole.all():
        values = {
            field: _without_text(getattr(item, field), modules)
            for field, modules in fields.items()
        }
        if any(getattr(item, k) != v for k, v in values.items()):
            await BotConsole.filter(id=item.id).update(**values)


async def _clean_plugins(module_paths: set[str], tasks: set[str]) -> list[str]:
    """清理插件的配置、限制、开关、被动技能与插件记录，不删除插件自己的数据表

    参数:
        module_paths: 插件模块路径
        tasks: 插件的被动技能模块名

    返回:
        list[str]: 已清理的插件名称
    """
    records = await PluginInfo.filter(module_path__in=module_paths)
    remaining = {
        str(module)
        for module in await PluginInfo.filter(
            module_path__not_in=module_paths
        ).values_list("module", flat=True)
    }
    modules = {record.module for record in records} - remaining
    if modules or tasks:
        await _clean_switches(modules, tasks)
        await TaskInfo.filter(module__in=tasks).delete()
    if modules:
        await GroupPluginSetting.filter(plugin_name__in=modules).delete()
        for data in (manager.cd_data, manager.block_data, manager.count_data):
            for module in modules:
                data.pop(module, None)
        manager.save_file()
        registered = {item.split(":", 1)[0] for item in Config.add_module}
        Config.remove_groups(modules - registered)
    await PluginInfo.filter(module_path__in=module_paths).delete()
    return sorted(f"{record.name}({record.module_path})" for record in records)


async def prune_absent_plugins(loaded: set[str]) -> None:
    """记录未加载且文件已不存在的插件，连续 ABSENCE_LIMIT 次启动未找到时清理其数据

    插件文件仍在（导入失败、被禁用）时重新计数；依赖层事务未结束或最近一次失败/回滚时
    不累加次数。首次未找到时从插件索引记下其被动技能，索引中已没有记录的插件清理时
    一并删除不属于任何现存插件的被动技能。清理范围为配置、限制、开关列表、被动技能与
    插件记录，插件自己的数据表保留。

    参数:
        loaded: 本次启动已加载的插件模块路径
    """
    from zhenxun.services.plugin_host import plugin_host

    saved = read_json_locked(ABSENCE_FILE, {})
    if not isinstance(saved, dict):
        saved = {}
    saved = {
        path: record if isinstance(record, dict) else {"count": record}
        for path, record in saved.items()
    }
    roots = _search_roots()
    absent = {
        str(path)
        for path in await PluginInfo.all().values_list("module_path", flat=True)
        if path not in loaded and not _module_exists(str(path), roots)
    }
    frozen = _layer_unsettled()
    records: dict[str, dict] = {}
    for path in absent:
        record = saved.get(path, {})
        if frozen and not record.get("count"):
            continue
        tasks = record.get("tasks")
        if tasks is None and path in plugin_host.duties:
            tasks = plugin_host.duties[path].get("tasks", [])
        records[path] = {
            "count": int(record.get("count") or 0) + (0 if frozen else 1),
            "tasks": tasks,
        }
    counts = {path: record["count"] for path, record in records.items()}
    expired = {path for path, count in counts.items() if count >= ABSENCE_LIMIT}
    if pending := sorted(set(counts) - expired):
        logger.warning(
            f"{len(pending)} 个插件的文件已不存在，连续 {ABSENCE_LIMIT} 次启动未找到后"
            "将清理其配置与插件数据: "
            + ", ".join(f"{path}({counts[path]}/{ABSENCE_LIMIT})" for path in pending),
            LOG_COMMAND,
        )
    if expired:
        declared = {
            task
            for name, duty in plugin_host.duties.items()
            if name not in expired
            for task in duty.get("tasks", [])
        }
        tasks = {task for path in expired for task in records[path]["tasks"] or []}
        if declared and any(records[path]["tasks"] is None for path in expired):
            tasks |= {
                str(module)
                for module in await TaskInfo.all().values_list("module", flat=True)
            }
        cleaned = await _clean_plugins(expired, tasks - declared)
        from zhenxun.services.cache.runtime_cache import (
            PluginInfoMemoryCache,
            TaskInfoMemoryCache,
        )

        await PluginInfoMemoryCache.refresh()
        await TaskInfoMemoryCache.refresh()
        logger.info(
            f"已清理连续 {ABSENCE_LIMIT} 次启动未找到的插件: {', '.join(cleaned)}，"
            "插件自己的数据表保留",
            LOG_COMMAND,
        )
    write_json_locked(
        ABSENCE_FILE,
        {path: record for path, record in records.items() if path not in expired},
    )
