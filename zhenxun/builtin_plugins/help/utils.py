from collections.abc import Callable

from nonebot_plugin_uninfo import Uninfo

from zhenxun.models.bot_console import BotConsole
from zhenxun.models.group_console import GroupConsole
from zhenxun.models.plugin_info import PluginInfo
from zhenxun.services.startup_load import startup_load_planner
from zhenxun.utils.enum import PluginType


async def sort_type() -> dict[str, list[PluginInfo]]:
    """
    对插件按照菜单类型分类
    """
    data = await PluginInfo.get_plugins(
        menu_type__not="",
        load_status=True,
        plugin_type__in=[PluginType.NORMAL, PluginType.DEPENDANT],
        is_show=True,
    )
    sort_data = {}
    for plugin in data:
        if not startup_load_planner.plugin_available(plugin.module_path):
            continue
        menu_type = plugin.menu_type or "normal"
        if menu_type == "normal":
            menu_type = "功能"
        if not sort_data.get(menu_type):
            sort_data[menu_type] = []
        sort_data[menu_type].append(plugin)
    return sort_data


async def classify_plugin(
    session: Uninfo, group_id: str | None, is_detail: bool, handle: Callable
) -> dict[str, list]:
    """对插件进行分类并判断状态

    参数:
        session: Uninfo对象
        group_id: 群组id
        is_detail: 是否详细帮助
        handle: 回调方法

    返回:
        dict[str, list[Item]]: 分类插件数据
    """
    sort_data = await sort_type()
    classify: dict[str, list] = {}
    group = await GroupConsole.get_group(group_id=group_id) if group_id else None
    from zhenxun.services.plugin_policy import plugin_policy_service
    from zhenxun.utils.platform import PlatformUtils

    scope = PlatformUtils.get_platform_scope(session)
    bot_id = f"qq_api:{session.self_id}" if scope == "qq_api" else str(session.self_id)
    bot = await BotConsole.get_or_none(bot_id=bot_id)
    channel_id = getattr(getattr(session, "channel", None), "id", None)
    for menu, value in sort_data.items():
        for plugin in value:
            if not classify.get(menu):
                classify[menu] = []
            item = handle(bot, plugin, group, is_detail)
            reason = plugin_policy_service.availability(
                plugin,
                bot,
                group,
                bot_id=bot_id,
                platform_scope=scope,
                group_id=group_id,
                channel_id=channel_id,
            )
            item["status"] = (
                3
                if reason == "global_disabled"
                else 2
                if reason in {"account_disabled", "administrator_disabled"}
                else 1
                if reason
                else 0
            )
            item["blocked_by"] = reason
            classify[menu].append(item)
    for value in classify.values():
        value.sort(key=lambda x: int(x["id"]))
    return classify
