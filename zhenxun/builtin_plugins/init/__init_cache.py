"""
缓存初始化模块

负责注册各种缓存类型，实现按需缓存机制
"""

from zhenxun.models.bot_console import BotConsole
from zhenxun.models.level_user import LevelUser
from zhenxun.models.plugin_info import PluginInfo
from zhenxun.models.user_console import UserConsole
from zhenxun.services.cache import CacheRegistry, cache_config
from zhenxun.services.cache.config import CacheMode
from zhenxun.services.log import logger
from zhenxun.utils.enum import CacheType


# 注册缓存类型
def register_cache_types():
    """注册所有缓存类型"""
    CacheRegistry.register(CacheType.PLUGINS, PluginInfo)
    CacheRegistry.register(CacheType.BOT, BotConsole)
    CacheRegistry.register(CacheType.USERS, UserConsole)
    CacheRegistry.register(
        CacheType.LEVEL, LevelUser, key_format="{user_id}_{group_id}"
    )

    if cache_config.cache_mode == CacheMode.REDIS and cache_config.redis_host:
        logger.info(f"已注册 Redis 模型缓存类型，缓存模式: {cache_config.cache_mode}")


# 注册表本身只是内存字典，且 CacheRegistry.register 幂等。
# 这里在导入期先注册一次，避免 runtime 阶段中先启动的组件
# （如 runtime:qq_official，priority=2）连接 Bot 触发模型写入时，
# runtime:cache_types（priority=5）尚未执行而抛出「缓存类型 ... 不存在」。
register_cache_types()
