from dataclasses import dataclass
from datetime import datetime, timedelta

from tortoise.expressions import Q

from zhenxun import ui
from zhenxun.models.group_console import GroupConsole
from zhenxun.models.plugin_info import PluginInfo
from zhenxun.services.help_service import is_night_mode
from zhenxun.services.hot_query_cache import (
    get_member_name,
    get_statistics_plugin_counts_cached,
    get_statistics_trend_cached,
)
from zhenxun.utils.enum import PluginType
from zhenxun.utils.time_utils import TimeUtils

RANK_LIMIT = 15
"""排行最多展示的功能数量，其余合并为一行"""


@dataclass(frozen=True)
class _StatisticsPeriod:
    label: str
    """时段名称"""
    start_time: datetime | None
    """排行统计起点，None 为全部时间"""
    trend_start: datetime
    """趋势起点"""
    step: timedelta
    """趋势分段长度"""
    buckets: int
    """趋势分段数量"""
    trend_label: str
    """趋势说明"""


def _get_statistics_period(search_type: str | None) -> _StatisticsPeriod:
    """根据查询类型获取统计时段

    参数:
        search_type: day / week / month，其余为全部时间

    返回:
        _StatisticsPeriod: 统计时段
    """
    today = TimeUtils.get_day_start()
    day = timedelta(days=1)
    if search_type == "day":
        return _StatisticsPeriod(
            "今天", today, today, timedelta(hours=1), 24, "今天 · 按小时"
        )
    if search_type == "week":
        start = today - 6 * day
        return _StatisticsPeriod("近 7 天", start, start, day, 7, "近 7 天 · 按天")
    month_start = today - 29 * day
    if search_type == "month":
        return _StatisticsPeriod(
            "近 30 天", month_start, month_start, day, 30, "近 30 天 · 按天"
        )
    return _StatisticsPeriod("全部时间", None, month_start, day, 30, "近 30 天 · 按天")


def _trend_labels(period: _StatisticsPeriod) -> list[str]:
    """生成趋势横轴标签，分段较多时间隔显示

    参数:
        period: 统计时段

    返回:
        list[str]: 每段的标签，不显示的为空字符串
    """
    if period.step < timedelta(days=1):
        return [f"{i}时" if i % 6 == 0 else "" for i in range(period.buckets)]
    labels = []
    for index in range(period.buckets):
        date = period.trend_start + period.step * index
        rest = period.buckets - 1 - index
        if rest == 0:
            labels.append("今天")
        elif period.buckets <= 7 or rest % 5 == 0:
            labels.append(f"{date.month}/{date.day}")
        else:
            labels.append("")
    return labels


class StatisticsManage:
    @classmethod
    async def resolve_plugin(cls, name: str) -> PluginInfo | None:
        """按插件名称或模块名查找插件

        参数:
            name: 插件名称或模块名

        返回:
            PluginInfo | None: 插件信息
        """
        return await PluginInfo.filter(
            Q(name=name) | Q(module=name), plugin_type=PluginType.NORMAL
        ).first()

    @classmethod
    async def get_statistics(
        cls,
        plugin_name: str | None,
        is_global: bool,
        search_type: str | None,
        user_id: str | None = None,
        group_id: str | None = None,
    ) -> bytes | str:
        """获取功能调用统计图片

        参数:
            plugin_name: 只统计该功能，支持插件名称或模块名
            is_global: 是否全局统计
            search_type: 统计时段 day / week / month
            user_id: 用户id，统计个人调用
            group_id: 群组id，统计群组调用

        返回:
            bytes | str: 统计图片，无数据时为提示文本
        """
        module = None
        chips = []
        if plugin_name:
            plugin = await cls.resolve_plugin(plugin_name)
            if not plugin:
                return f"没有找到功能 {plugin_name}..."
            module = plugin.module
        if user_id:
            scope = "user"
            chips.append(await get_member_name(user_id, group_id) or user_id)
            if not group_id:
                chips.append("全部群聊")
        elif group_id and not is_global:
            scope = "group"
            group = await GroupConsole.get_group(group_id=group_id)
            chips.append(group.group_name if group else group_id)
        else:
            scope, group_id = "global", None
            chips.append("全局")
        period = _get_statistics_period(search_type)
        chips.append(period.label)
        if plugin_name:
            chips.append(plugin_name)
        counts = await get_statistics_plugin_counts_cached(
            scope,
            plugin_name=module,
            start_time=period.start_time,
            user_id=user_id,
            group_id=group_id,
        )
        items = await cls.build_rank(counts)
        if not items:
            return "统计数据为空..."
        edges = [
            period.trend_start + period.step * i for i in range(period.buckets + 1)
        ]
        trend = await get_statistics_trend_cached(
            scope, plugin_name=module, edges=edges, user_id=user_id, group_id=group_id
        )
        total = sum(count for _, count in items)
        top_count = items[0][1]
        peak = max(trend) or 1
        rest = items[RANK_LIMIT:]
        data = {
            "title": "功能调用统计",
            "chips": chips,
            "summary": [
                ["总调用", f"{total:,}"],
                ["使用功能", f"{len(items)} 个"],
                ["最常用", items[0][0]],
            ],
            "trend": {
                "unit": period.trend_label,
                "show_value": period.buckets <= 7,
                "points": [
                    {"label": label, "value": value, "ratio": value / peak * 100}
                    for label, value in zip(_trend_labels(period), trend)
                ],
            },
            "items": [
                {
                    "rank": rank,
                    "name": name,
                    "count": f"{count:,}",
                    "percent": f"{count / total:.1%}",
                    "ratio": round(count / top_count * 100, 1),
                }
                for rank, (name, count) in enumerate(items[:RANK_LIMIT], 1)
            ],
            "others": (
                f"其余 {len(rest)} 个功能共 {sum(c for _, c in rest):,} 次"
                if rest
                else ""
            ),
            "night": is_night_mode(),
        }
        return await ui.render_template(
            "pages/builtin/statistics",
            data=data,
            clip_selector=".page",
            clip_padding=0,
            disable_animations=True,
        )

    @classmethod
    async def build_rank(cls, counts: list[tuple[str, int]]) -> list[tuple[str, int]]:
        """将模块调用次数转为按次数降序的功能排行

        参数:
            counts: (模块名, 次数) 列表

        返回:
            list[tuple[str, int]]: (功能名称, 次数) 列表，只包含已加载的普通插件
        """
        module2count = dict(counts)
        plugins = await PluginInfo.get_plugins(
            module__in=list(module2count),
            load_status=True,
            filter_parent=False,
            plugin_type=PluginType.NORMAL,
        )
        rank = [
            (plugin.name, module2count[plugin.module])
            for plugin in plugins
            if module2count.get(plugin.module)
        ]
        return sorted(rank, key=lambda item: item[1], reverse=True)
