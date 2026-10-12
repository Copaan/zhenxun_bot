"""
缓存系统模块

提供运行态内存快照缓存（runtime_cache）与通用内存缓存容器

使用示例:
```python
from zhenxun.services.cache import CacheRoot

# 获取具名缓存字典（expire 为过期秒数，0 表示永不过期）
request_cache = CacheRoot.cache_dict("REQUEST_CACHE", 60, str)
request_cache["key"] = "value"
value = request_cache.get("key")
```
"""

from typing import ClassVar, TypeVar, cast
from typing_extensions import Self

import nonebot
from pydantic import BaseModel

from .bounded_ttl import BoundedTTLCache
from .cache_containers import CacheDict
from .config import CacheMode, normalize_cache_mode

__all__ = [
    "BoundedTTLCache",
    "CacheDict",
    "CacheManager",
    "CacheRoot",
    "Config",
    "cache_config",
]

U = TypeVar("U")


class Config(BaseModel):
    """缓存配置"""

    cache_mode: str = CacheMode.NONE
    """缓存模式: MEMORY(内存缓存), REDIS(Redis缓存), NONE(不使用缓存)"""
    redis_host: str | None = None
    """redis地址"""
    redis_port: int | None = None
    """redis端口"""
    redis_password: str | None = None
    """redis密码"""

    def __init__(self, **data):
        data["cache_mode"] = normalize_cache_mode(data.get("cache_mode"))
        super().__init__(**data)


# 获取配置
cache_config = nonebot.get_plugin_config(Config)


class CacheManager:
    """缓存管理器"""

    _instance: ClassVar["CacheManager | None"] = None
    _dict_caches: ClassVar[dict[str, "CacheDict"]] = {}

    def __new__(cls) -> Self:
        """单例模式"""
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cast(Self, cls._instance)

    def cache_dict(
        self, cache_type: str, expire: int = 0, value_type: type[U] = str
    ) -> CacheDict[U]:
        """获取缓存字典
        参数:
            cache_type: 缓存类型
            expire: 过期时间（秒）
            value_type: 值类型

        返回:
            CacheDict: 缓存字典
        """
        if cache_type not in self._dict_caches:
            self._dict_caches[cache_type] = CacheDict[value_type](cache_type, expire)
        return self._dict_caches[cache_type]


# 全局缓存管理器实例
CacheRoot = CacheManager()

from . import runtime_cache as _runtime_cache  # noqa: F401
