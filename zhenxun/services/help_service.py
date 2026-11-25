from collections import defaultdict
import contextlib
from datetime import datetime
from functools import cache
import math
import re

import nonebot
from nonebot.plugin import PluginMetadata
from PIL import ImageFont
from pydantic import BaseModel, ConfigDict

from zhenxun import ui
from zhenxun.configs.config import BotConfig, Config
from zhenxun.configs.path_config import FONT_PATH
from zhenxun.models.plugin_info import PluginInfo
from zhenxun.models.task_info import TaskInfo
from zhenxun.services.renderer.result_cache import RenderResultMemoryCache
from zhenxun.ui.models import (
    HelpBlock,
    HelpCategory,
    HelpItem,
    HelpSection,
    PluginHelpPageData,
)
from zhenxun.utils.common_utils import format_usage_for_markdown
from zhenxun.utils.enum import PluginType

_PLUGIN_HELP_IMAGE_CACHE = RenderResultMemoryCache(
    ttl_seconds=300,
    max_items=48,
    max_total_bytes=48 * 1024 * 1024,
)


class PluginData(BaseModel):
    plugin: PluginInfo
    metadata: PluginMetadata

    model_config = ConfigDict(arbitrary_types_allowed=True)


def is_night_mode(now: datetime | None = None) -> bool:
    """判断帮助、插件商店等图片是否使用夜间配色

    参数:
        now: 判断用的时间，默认为当前时间

    返回:
        bool: 开启了自动切换且处于夜间时段时为 True
    """
    if not Config.get_config("help", "AUTO_NIGHT_MODE", True):
        return False
    start, end = 19, 7
    hours = str(Config.get_config("help", "NIGHT_HOURS", "19-7"))
    with contextlib.suppress(ValueError):
        start, end = (int(value) % 24 for value in hours.split("-"))
    hour = (now or datetime.now()).hour
    if start > end:
        return hour >= start or hour < end
    return start <= hour < end


async def _get_plugins_by_types(plugin_types: list[PluginType]) -> list[PluginData]:
    """根据指定的插件类型列表获取插件数据"""
    plugin_list = await PluginInfo.get_plugins(
        load_status=None,
        filter_parent=False,
        plugin_type__in=plugin_types,
    )
    data_list = []
    for plugin in plugin_list:
        if _plugin := nonebot.get_plugin_by_module_name(plugin.module_path):
            if _plugin.metadata:
                data_list.append(PluginData(plugin=plugin, metadata=_plugin.metadata))
    return data_list


async def _get_task_category() -> dict:
    """获取被动技能帮助类别"""
    task_items = []
    if task_list := await TaskInfo.get_tasks(load_status=True):
        task_names = "\n".join([task.name for task in task_list])
        task_items.append(
            {
                "name": "被动技能",
                "description": "控制群组中的被动技能状态",
                "usage": "通过 开启/关闭群被动 来控制群被动\n"
                + " 示例：开启/关闭群被动早晚安\n 示例：开启/关闭全部群被动"
                + " \n ---------- \n "
                + task_names,
            }
        )

    return {
        "title": "被动技能管理",
        "icon_svg_path": "M10,20V14H14V20H19V12H22L12,3L2,12H5V20H10Z",
        "items": task_items,
    }


_COLUMNS = 3
"""帮助页每个分类的列数"""
_COLUMN_WIDTH = (1200 - 24 * 2 - 16 * (_COLUMNS - 1)) / _COLUMNS
"""每列宽度，与模板中页面宽度、内边距和列间距一致"""
_FONT = "AlibabaHealthFont2.0CN-85B.ttf"
"""帮助页正文字体"""


@cache
def _font(size: int) -> ImageFont.FreeTypeFont | None:
    with contextlib.suppress(OSError):
        return ImageFont.truetype(str(FONT_PATH / _FONT), size)
    return None


def _text_width(text: str, size: float) -> float:
    """按帮助页字体测量文字宽度，字体缺失时按全角一字、半角半字估算"""
    if font := _font(round(size)):
        return font.getlength(text)
    return sum(size if ord(ch) > 0x2E7F else size / 2 for ch in text)


def _markdown_height(
    text: str, width: float, size: float, line_height: float, gap: float
) -> float:
    """估算 Markdown 文本渲染后的高度，按块级元素的上下边距折叠计算

    参数:
        text: Markdown 文本
        width: 可用宽度
        size: 字号
        line_height: 行高倍数
        gap: 段落下边距

    返回:
        float: 估算高度 (像素)
    """
    blocks: list[tuple[float, float, list[list]]] = []
    kind, blank, joinable = "", False, False
    for raw in text.strip().split("\n"):
        line = raw.strip()
        if not line:
            blank = True
            continue
        indented = raw[:1] == " "
        if re.fullmatch(r"[-*_]{3,}", line):
            blocks.append((8, 8, [["", 0, 2 / line_height]]))
            kind = ""
        elif heading := re.match(r"#+\s*", line):
            blocks.append((8, 4, [[line[heading.end() :], width, 14]]))
            kind = ""
        elif item := re.match(r"[-*+]\s+", line):
            if kind != "list":
                blocks.append((4, 4, []))
                kind = "list"
            depth = 1 + (len(raw) - len(raw.lstrip())) // 4
            blocks[-1][2].append([line[item.end() :], width - 18 * depth, size])
        elif kind and (not blank or (kind == "list" and indented)):
            rows = blocks[-1][2]
            if joinable:
                rows[-1][0] += f" {line}"
            else:
                rows.append([line, rows[-1][1], size])
        else:
            blocks.append((0, gap, [[line, width, size]]))
            kind = "p"
        blank = False
        joinable = kind != "" and not raw.endswith("  ")
    if not blocks:
        return 0.0
    if blocks[-1][1] == gap:
        blocks[-1] = (blocks[-1][0], 0, blocks[-1][2])
    height = blocks[0][0] + blocks[-1][1]
    for index, (top, _, rows) in enumerate(blocks):
        if index:
            height += max(blocks[index - 1][1], top)
        for line, line_width, line_size in rows:
            if not line_width:
                height += line_size * line_height
                continue
            extra = 8 * line.count("`") / 2
            used = _text_width(re.sub(r"\*\*|`|^>\s*", "", line), line_size) + extra
            height += max(1, math.ceil(used / line_width)) * line_size * line_height
    return height


def _item_height(item: HelpItem) -> float:
    """估算单张帮助卡片的高度，含卡片间距"""
    inner = _COLUMN_WIDTH - 38
    height = 37 + 16 + 28
    height += _markdown_height(item.description, inner, 13, 1.55, 0)
    if item.usage:
        height += 32 + _markdown_height(item.usage, inner - 26, 13, 1.7, 6)
    return height


def _balance(units: list[tuple[float, HelpBlock]]) -> list[list[HelpBlock]]:
    """从高到低依次把内容块放进当前最矮的一列，列内保持原顺序

    参数:
        units: (估算高度, 内容块) 列表

    返回:
        list[list[HelpBlock]]: 每一列的内容块
    """
    heights = [0.0] * _COLUMNS
    columns: list[list[int]] = [[] for _ in range(_COLUMNS)]
    for index in sorted(range(len(units)), key=lambda i: -units[i][0]):
        column = heights.index(min(heights))
        columns[column].append(index)
        heights[column] += units[index][0]
    return [[units[i][1] for i in sorted(column)] for column in columns]


def _layout(categories: list[HelpCategory]) -> list[HelpSection]:
    """把分类排成三列；卡片少于列数的分类并入上一分类，减少空白

    参数:
        categories: 帮助分类列表

    返回:
        list[HelpSection]: 分好列的分类区域
    """
    groups: list[tuple[HelpCategory, list[tuple[float, HelpBlock]]]] = []
    for category in categories:
        heights = [_item_height(item) for item in category.items]
        if groups and len(category.items) < _COLUMNS:
            block = HelpBlock(
                title=category.title,
                icon_svg_path=category.icon_svg_path,
                items=category.items,
            )
            groups[-1][1].append((sum(heights) + 46, block))
            continue
        units = [
            (height, HelpBlock(items=[item]))
            for height, item in zip(heights, category.items)
        ]
        groups.append((category, units))
    return [
        HelpSection(
            title=category.title,
            icon_svg_path=category.icon_svg_path,
            count=len(category.items),
            columns=_balance(units),
        )
        for category, units in groups
    ]


async def create_plugin_help_image(
    plugin_types: list[PluginType], page_title: str
) -> bytes:
    """
    一个通用的函数，用于创建插件帮助图片。

    参数:
        plugin_types: 要包含在帮助中的插件类型列表。
        page_title: 生成图片的标题。

    返回:
        bytes: 生成的图片字节流。
    """
    plugins_data = await _get_plugins_by_types(plugin_types)

    grouped_plugins = defaultdict(list)
    for data in plugins_data:
        menu_type = data.plugin.menu_type or "功能"
        grouped_plugins[menu_type].append(
            HelpItem(
                name=data.plugin.name,
                description=format_usage_for_markdown(data.metadata.description),
                usage=format_usage_for_markdown(data.metadata.usage),
            )
        )

    # 直接构建 HelpCategory 列表
    categories = []

    for menu_type, items in grouped_plugins.items():
        categories.append(
            HelpCategory(
                title=menu_type,
                icon_svg_path="M12,2L15.09,8.26L22,9.27L17,14.14L18.18,21.02L12,17.77L5.82,21.02L7,14.14L2,9.27L8.91,8.26L12,2Z",
                items=sorted(items, key=lambda x: x.name),
            )
        )

    task_category_data = await _get_task_category()
    if task_category_data["items"]:
        task_items = [HelpItem(**item) for item in task_category_data["items"]]
        categories.append(
            HelpCategory(
                title=task_category_data["title"],
                icon_svg_path=task_category_data["icon_svg_path"],
                items=task_items,
            )
        )

    # 直接实例化 Data Model
    page_data = PluginHelpPageData(
        bot_nickname=BotConfig.self_nickname,
        page_title=page_title,
        categories=categories,
        sections=_layout(categories),
        night=is_night_mode(),
    )

    cache_payload = {
        "plugin_types": sorted([plugin_type.value for plugin_type in plugin_types]),
        "page_title": page_title,
        "theme": Config.get_config("UI", "THEME", "default"),
        "page_data": page_data,
    }
    cache_key = RenderResultMemoryCache.build_key(cache_payload)
    if cached_image := await _PLUGIN_HELP_IMAGE_CACHE.get(cache_key):
        return cached_image

    image_bytes = await ui.render(
        page_data,
        use_cache=True,
        clip_selector=".page",
        clip_padding=0,
        disable_animations=True,
    )
    await _PLUGIN_HELP_IMAGE_CACHE.set(cache_key, image_bytes)

    return image_bytes
