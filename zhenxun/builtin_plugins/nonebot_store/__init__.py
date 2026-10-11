from nonebot.plugin import PluginMetadata

from zhenxun.configs.utils import PluginExtraData
from zhenxun.utils.enum import PluginType

__plugin_meta__ = PluginMetadata(
    name="NoneBot 托管商店基础",
    description="提供托管插件、依赖层和运行时环境管理基础能力。",
    usage="",
    extra=PluginExtraData(
        author="HibiKier",
        version="1.0",
        plugin_type=PluginType.HIDDEN,
    ).to_dict(),
)
