from pydantic import BaseModel, Field

from ..core.base import RenderableComponent

__all__ = [
    "HelpBlock",
    "HelpCategory",
    "HelpItem",
    "HelpSection",
    "PluginHelpPageData",
]


class HelpItem(BaseModel):
    """帮助菜单中的单个功能项"""

    name: str
    """功能名称"""
    description: str
    """功能描述"""
    usage: str
    """功能用法说明"""


class HelpCategory(BaseModel):
    """帮助菜单中的一个功能类别"""

    title: str
    """分类标题"""
    icon_svg_path: str
    """分类图标的SVG路径数据"""
    items: list[HelpItem]
    """该分类下的功能项列表"""


class HelpBlock(BaseModel):
    """排版后放进某一列的内容块"""

    title: str | None = None
    """并入上一分类的小分类标题，为空时只是一张卡片"""
    icon_svg_path: str = ""
    """小分类图标的SVG路径数据"""
    items: list[HelpItem]
    """块内的功能项"""


class HelpSection(BaseModel):
    """排版后的一个分类区域"""

    title: str
    """分类标题"""
    icon_svg_path: str
    """分类图标的SVG路径数据"""
    count: int
    """分类下的功能数量"""
    columns: list[list[HelpBlock]]
    """每一列依次排列的内容块"""


class PluginHelpPageData(RenderableComponent):
    """通用插件帮助页面的数据模型"""

    style_name: str | None = None
    """页面样式名称"""
    bot_nickname: str
    """机器人昵称"""
    page_title: str
    """页面主标题"""
    categories: list[HelpCategory]
    """帮助分类列表"""
    sections: list[HelpSection] = Field(default_factory=list)
    """按卡片高度分好列的分类区域"""
    night: bool = False
    """是否使用夜间配色"""

    @property
    def template_name(self) -> str:
        return "pages/core/plugin_help_page"
