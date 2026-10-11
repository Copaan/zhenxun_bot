from nonebot.plugin import PluginMetadata

from zhenxun.configs.utils import PluginExtraData
from zhenxun.utils.enum import PluginType

__plugin_meta__ = PluginMetadata(
    name="迁移基础",
    description="提供 Bot 配置迁移、恢复和运行时交接基础能力。",
    usage="",
    extra=PluginExtraData(
        author="HibiKier",
        version="1.0",
        plugin_type=PluginType.HIDDEN,
    ).to_dict(),
)
