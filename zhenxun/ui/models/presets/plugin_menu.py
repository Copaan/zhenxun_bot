from math import ceil

from pydantic import BaseModel, Field

from zhenxun.utils.pydantic_compat import compat_computed_field

from ..core.base import RenderableComponent

__all__ = [
    "PluginMenuCategory",
    "PluginMenuData",
    "PluginMenuItem",
]


class PluginMenuItem(BaseModel):
    """插件菜单中的单个插件项"""

    id: str
    """插件的唯一ID"""
    name: str
    """插件名称"""
    status: int
    """插件状态码: 0:开启, 1:群关, 2:系统关, 3:全局关"""
    has_superuser_help: bool
    """插件是否有超级用户专属帮助"""
    commands: list[str] = Field(default_factory=list, description="插件的主要命令列表")
    """插件的主要命令列表"""


class PluginMenuCategory(BaseModel):
    """插件菜单中的一个分类"""

    name: str
    """插件分类名称"""
    items: list[PluginMenuItem] = Field(..., description="该分类下的插件项列表")
    """该分类下的插件项列表"""


class PluginMenuData(RenderableComponent):
    """通用插件帮助菜单的数据模型"""

    style_name: str | None = None
    """页面样式名称"""
    bot_name: str
    """机器人名称"""
    is_detail: bool
    """是否为详细菜单模式"""
    plugin_count: int
    """总插件数量"""
    active_count: int
    """已启用插件数量"""
    categories: list[PluginMenuCategory]
    """插件分类列表"""

    @property
    def template_name(self) -> str:
        return "pages/core/plugin_menu"

    def _item_weight(self, item: PluginMenuItem) -> float:
        """估算插件项的高度，单位为一行插件项

        参数:
            item: 插件项

        返回:
            float: 估算高度
        """
        if not self.is_detail or not item.commands:
            return 1
        width = sum(len(command) * 13 + 22 for command in item.commands)
        return 1 + 0.8 * ceil(width / 340)

    @compat_computed_field
    @property
    def columns(self) -> list[list[dict]]:
        """按估算高度把分类分成左右两列，左列读完接右列

        在分类之间或分类内部选择切分点，使两列中较高的一列尽量矮；切在分类内部时
        右列以续卡片接上，两段各至少保留 3 项。

        返回:
            list[list[dict]]: 两列分类，每项包含 name、plugins、count、color_index
                与 continued
        """
        card = 2.5
        rows = []
        for index, category in enumerate(self.categories):
            weights = [self._item_weight(item) for item in category.items]
            rows.append((index, category, weights))
        total = sum(card + sum(weights or [1]) for _, _, weights in rows)

        best = None
        before = 0.0
        for position, (index, category, weights) in enumerate(rows):
            cuts = [0] if position else []
            if len(weights) >= 6:
                cuts.extend(range(3, len(weights) - 2))
            for cut in cuts:
                left = before + (card + sum(weights[:cut]) if cut else 0)
                right = total - left + (card if cut else 0)
                height = max(left, right)
                if best is None or height < best[0]:
                    best = (height, position, cut)
            before += card + sum(weights or [1])

        def entry(index: int, category: PluginMenuCategory, items, continued: bool):
            return {
                "name": category.name,
                "plugins": items,
                "count": len(category.items),
                "color_index": index,
                "continued": continued,
            }

        if best is None:
            return [[entry(i, c, c.items, False) for i, c, _ in rows], []]
        _, position, cut = best
        left = [entry(i, c, c.items, False) for i, c, _ in rows[:position]]
        right = []
        index, category, _ = rows[position]
        if cut:
            left.append(entry(index, category, category.items[:cut], False))
            right.append(entry(index, category, category.items[cut:], True))
        else:
            right.append(entry(index, category, category.items, False))
        right.extend(entry(i, c, c.items, False) for i, c, _ in rows[position + 1 :])
        return [left, right]
