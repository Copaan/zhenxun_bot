"""
头像缓存服务

提供一个统一的、带缓存的头像获取服务，支持多平台和可配置的过期策略。
"""

import asyncio
from collections import OrderedDict
from hashlib import sha256
import os
from pathlib import Path
import threading
import time

from nonebot_plugin_apscheduler import scheduler

from zhenxun.configs.config import Config
from zhenxun.configs.path_config import DATA_PATH
from zhenxun.services.lifecycle import lifecycle_kernel
from zhenxun.services.log import logger
from zhenxun.utils.http_utils import AsyncHttpx
from zhenxun.utils.manager.priority_manager import PriorityLifecycle
from zhenxun.utils.platform import PlatformUtils

Config.add_plugin_config(
    "avatar_cache",
    "ENABLED",
    True,
    help="是否启用头像缓存功能",
    default_value=True,
    type=bool,
)
Config.add_plugin_config(
    "avatar_cache",
    "TTL_DAYS",
    7,
    help="头像缓存的有效期（天）",
    default_value=7,
    type=int,
)
Config.add_plugin_config(
    "avatar_cache",
    "CLEANUP_INTERVAL_HOURS",
    24,
    help="后台清理过期缓存的间隔时间（小时）",
    default_value=24,
    type=int,
)


class AvatarService:
    """
    一个集中式的头像缓存服务，提供L1（内存）和L2（文件）两级缓存。
    """

    _MEMORY_CACHE_MAX_ITEMS = 2000

    def __init__(self):
        self.cache_path = (DATA_PATH / "cache" / "avatars").resolve()
        self.cache_path.mkdir(parents=True, exist_ok=True)
        self._memory_cache: OrderedDict[str, Path] = OrderedDict()
        self._cleanup_lock = asyncio.Lock()
        self._cleanup_stop = threading.Event()
        self._file_lock = threading.Lock()
        self._writing: dict[Path, int] = {}

    def _get_cache_path(self, platform: str, identifier: str) -> Path:
        """
        根据平台和ID生成存储的文件路径。
        例如: data/cache/avatars/qq/123456789.png
        """
        identifier = str(identifier)
        if any(char in identifier for char in ':<>"/\\|?*'):
            identifier = sha256(identifier.encode()).hexdigest()
        return self.cache_path / platform / f"{identifier}.png"

    async def get_session_avatar_path(self, session) -> Path | None:
        """Official avatars use protocol metadata, never the linked QQ account."""
        if PlatformUtils.get_platform_scope(session) != "qq_api":
            return await self.get_avatar_path(
                PlatformUtils.get_platform(session), session.user.id
            )
        url = getattr(session.user, "avatar", None)
        if not url or not Config.get_config("avatar_cache", "ENABLED"):
            return None
        key = sha256(f"{session.self_id}:{session.user.id}:{url}".encode()).hexdigest()
        path = self._get_cache_path("qq_api", key)
        ttl = Config.get_config("avatar_cache", "TTL_DAYS", 7) * 86400
        if path.exists() and time.time() - path.stat().st_mtime < ttl:
            return path
        path.parent.mkdir(parents=True, exist_ok=True)
        return path if await self._download_avatar(url, path) else None

    async def _download_avatar(self, url: str, path: Path) -> bool:
        """下载头像期间保护目标文件，避免后台清理删除写入中的文件。"""
        with self._file_lock:
            self._writing[path] = self._writing.get(path, 0) + 1
        try:
            return await AsyncHttpx.download_file(url, path)
        finally:
            with self._file_lock:
                remaining = self._writing[path] - 1
                if remaining:
                    self._writing[path] = remaining
                else:
                    self._writing.pop(path)

    def clear_memory_cache(self) -> int:
        size = len(self._memory_cache)
        self._memory_cache.clear()
        return size

    async def get_avatar_path(
        self, platform: str, identifier: str, force_refresh: bool = False
    ) -> Path | None:
        """
        获取用户或群组的头像本地路径。

        参数:
            platform: 平台名称 (e.g., 'qq')
            identifier: 用户ID或群组ID
            force_refresh: 是否强制刷新缓存

        返回:
            Path | None: 头像的本地文件路径，如果获取失败则返回None。
        """
        if not Config.get_config("avatar_cache", "ENABLED"):
            return None

        if str(identifier).startswith(("principal:", "identity:")):
            return None

        cache_key = f"{platform}-{identifier}"
        if not force_refresh and cache_key in self._memory_cache:
            cached_path = self._memory_cache[cache_key]
            if cached_path.exists():
                self._memory_cache.move_to_end(cache_key)
                return cached_path
            self._memory_cache.pop(cache_key, None)

        local_path = self._get_cache_path(platform, identifier)
        ttl_seconds = Config.get_config("avatar_cache", "TTL_DAYS", 7) * 86400

        if not force_refresh and local_path.exists():
            try:
                file_mtime = os.path.getmtime(local_path)
                if time.time() - file_mtime < ttl_seconds:
                    self._memory_cache[cache_key] = local_path
                    self._memory_cache.move_to_end(cache_key)
                    while len(self._memory_cache) > self._MEMORY_CACHE_MAX_ITEMS:
                        self._memory_cache.popitem(last=False)
                    return local_path
            except FileNotFoundError:
                pass

        avatar_url = PlatformUtils.get_user_avatar_url(identifier, platform)
        if not avatar_url:
            return None

        local_path.parent.mkdir(parents=True, exist_ok=True)

        if await self._download_avatar(avatar_url, local_path):
            self._memory_cache[cache_key] = local_path
            self._memory_cache.move_to_end(cache_key)
            while len(self._memory_cache) > self._MEMORY_CACHE_MAX_ITEMS:
                self._memory_cache.popitem(last=False)
            return local_path
        else:
            logger.warning(f"下载头像失败: {avatar_url}", "AvatarService")
            return None

    async def _cleanup_cache(self) -> dict:
        """在线程清理过期文件，取消时等待文件操作结束再释放互斥。"""
        if not Config.get_config("avatar_cache", "ENABLED"):
            return {"deleted": 0, "errors": 0, "reason": "disabled"}
        async with self._cleanup_lock:
            if lifecycle_kernel.shutdown_requested:
                return {"deleted": 0, "errors": 0, "reason": "shutdown"}
            self._cleanup_stop.clear()
            snapshot = tuple(self._memory_cache.items())
            worker = asyncio.create_task(
                asyncio.to_thread(
                    self._cleanup_files,
                    time.time(),
                    Config.get_config("avatar_cache", "TTL_DAYS", 7) * 86400,
                    snapshot,
                )
            )
            try:
                result, stale = await asyncio.shield(worker)
            except asyncio.CancelledError:
                self._cleanup_stop.set()
                while not worker.done():
                    try:
                        await asyncio.shield(worker)
                    except asyncio.CancelledError:
                        continue
                worker.result()
                raise
            for key, path in stale:
                if self._memory_cache.get(key) is path:
                    self._memory_cache.pop(key, None)
            while len(self._memory_cache) > self._MEMORY_CACHE_MAX_ITEMS:
                self._memory_cache.popitem(last=False)
            return result

    def _cleanup_files(self, now: float, ttl: float, snapshot: tuple) -> tuple:
        """扫描缓存目录并逐文件隔离异常，内存缓存仅返回待核销快照。"""
        result = {"deleted": 0, "errors": 0, "reason": ""}
        stale = []

        def stopping():
            return self._cleanup_stop.is_set() or lifecycle_kernel.shutdown_requested

        def failed(error):
            result["errors"] += 1
            logger.warning(f"头像缓存清理文件失败: {error}", "AvatarService")

        for root, _, files in os.walk(self.cache_path, onerror=failed):
            for name in files:
                if stopping():
                    result["reason"] = (
                        "shutdown"
                        if lifecycle_kernel.shutdown_requested
                        else "cancelled"
                    )
                    return result, stale
                file_path = Path(root) / name
                try:
                    with self._file_lock:
                        if (
                            file_path not in self._writing
                            and now - file_path.stat().st_mtime > ttl
                        ):
                            file_path.unlink()
                            result["deleted"] += 1
                except FileNotFoundError:
                    continue
                except OSError as error:
                    failed(error)
        for key, cached_path in snapshot:
            if stopping():
                result["reason"] = (
                    "shutdown" if lifecycle_kernel.shutdown_requested else "cancelled"
                )
                break
            try:
                with self._file_lock:
                    if cached_path in self._writing:
                        continue
                    if now - cached_path.stat().st_mtime > ttl:
                        stale.append((key, cached_path))
            except FileNotFoundError:
                stale.append((key, cached_path))
            except OSError as error:
                failed(error)
        return result, stale

    async def stop_cleanup(self):
        """请求停止并等待当前清理线程退出。"""
        self._cleanup_stop.set()
        async with self._cleanup_lock:
            pass


avatar_service = AvatarService()


@PriorityLifecycle.on_shutdown(priority=5)
async def _stop_avatar_cleanup():
    await avatar_service.stop_cleanup()


@scheduler.scheduled_job(
    "cron",
    hour=4,
    minute=30,
)
async def _run_avatar_cache_cleanup():
    from zhenxun.services.scheduler.lifecycle import core_task_run

    async with core_task_run("avatar_cleanup") as run:
        result = await avatar_service._cleanup_cache()
        run.update(
            processed=result["deleted"],
            errors=result["errors"],
            reason=result.get("reason", ""),
        )
        if result.get("reason"):
            run["status"] = (
                "cancelled"
                if result["reason"] in {"shutdown", "cancelled"}
                else "skipped"
            )
