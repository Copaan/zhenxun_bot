import asyncio
from pathlib import Path
from typing import ClassVar

import aiofiles
import nonebot
from pydantic import BaseModel

from zhenxun import ui
from zhenxun.configs.config import BotConfig, Config
from zhenxun.configs.path_config import DATA_PATH
from zhenxun.configs.utils.models import PluginExtraData
from zhenxun.models.statistics import Statistics
from zhenxun.models.user_console import UserConsole
from zhenxun.services.avatar_service import avatar_service
from zhenxun.services.help_service import is_night_mode
from zhenxun.services.log import logger
from zhenxun.utils.platform import PlatformUtils
from zhenxun.utils.pydantic_compat import model_dump

DIR_PATH = DATA_PATH / "bot_profile"
"""自我介绍目录：profile.md / avatar.png 为全部 Bot 共用，<bot_id>/ 下的同名文件优先"""

LEGACY_PATH = DIR_PATH / "profile"
"""旧版 profile/<bot_id>/profile.txt 存放目录"""

PROFILE_FILE = "profile.md"

AVATAR_SUFFIXES = (".png", ".jpg", ".jpeg", ".webp", ".gif")


Config.add_plugin_config(
    "bot_profile",
    "AUTO_SEND_PROFILE",
    True,
    help="在添加好友/群组时是否自动发送BOT自我介绍图片",
    default_value=True,
    type=bool,
)


class Profile(BaseModel):
    bot_id: str
    """BOT ID"""
    introduction: str
    """BOT自我介绍"""
    avatar: Path | None
    """BOT头像"""
    name: str
    """BOT名称"""


class PluginProfile(BaseModel):
    name: str
    """插件名称"""
    introduction: str
    """插件自我介绍"""
    precautions: list[str] | None = None
    """BOT自我介绍时插件的注意事项"""


class BotProfileManager:
    """BOT自我介绍管理器"""

    _bot_data: ClassVar[dict[str, Profile]] = {}

    _plugin_data: ClassVar[dict[str, PluginProfile]] = {}

    _migrated: ClassVar[bool] = False

    @classmethod
    def clear_profile_image(cls, bot_id: str | None = None):
        """清除BOT自我介绍的内存缓存"""
        if bot_id:
            if bot_id in cls._bot_data:
                del cls._bot_data[bot_id]
        else:
            cls._bot_data.clear()

    @classmethod
    def migrate_legacy(cls):
        """将旧版 profile/<bot_id>/profile.txt 与 <bot_id>.png 移到 <bot_id>/ 下"""
        cls._migrated = True
        if not LEGACY_PATH.is_dir():
            return
        for old_dir in LEGACY_PATH.iterdir():
            if not old_dir.is_dir():
                continue
            new_dir = DIR_PATH / old_dir.name
            moves = [
                (old_dir / "profile.txt", new_dir / PROFILE_FILE),
                (old_dir / f"{old_dir.name}.png", new_dir / "avatar.png"),
            ]
            for old, new in moves:
                if not old.is_file():
                    continue
                if old.stat().st_size and not new.exists():
                    new_dir.mkdir(parents=True, exist_ok=True)
                    old.replace(new)
                    logger.info(f"自我介绍文件已迁移: {old} -> {new}")
                elif not old.stat().st_size:
                    old.unlink()
            if not any(old_dir.iterdir()):
                old_dir.rmdir()
        if not any(LEGACY_PATH.iterdir()):
            LEGACY_PATH.rmdir()

    @classmethod
    def find_file(cls, bot_id: str, names: list[str]) -> Path | None:
        """按 <bot_id>/ 优先、再到共用目录的顺序查找文件

        参数:
            bot_id: BOT ID
            names: 候选文件名

        返回:
            Path | None: 找到的文件
        """
        for folder in (DIR_PATH / bot_id, DIR_PATH):
            for name in names:
                if (path := folder / name).is_file():
                    return path
        return None

    @classmethod
    async def _read_profile(cls, bot_id: str):
        """读取已有的BOT自我介绍；未配置时跳过，不创建目录或文件。

        参数:
            bot_id: BOT ID
        """
        if not cls._migrated:
            cls.migrate_legacy()
        profile_file = cls.find_file(bot_id, [PROFILE_FILE])
        if not profile_file:
            logger.debug(f"BOT自我介绍文件不存在: {DIR_PATH / PROFILE_FILE}, 跳过读取")
            return
        async with aiofiles.open(profile_file, encoding="utf-8") as f:
            introduction = await f.read()
        avatar = cls.find_file(bot_id, [f"avatar{s}" for s in AVATAR_SUFFIXES])
        name = ""
        try:
            bot = nonebot.get_bot(bot_id)
            if user := await PlatformUtils.get_user(bot, bot_id):
                name = user.name
            if not avatar:
                avatar = await avatar_service.get_avatar_path(
                    PlatformUtils.get_platform(bot), bot_id
                )
        except Exception as e:
            logger.debug("获取BOT信息失败，使用默认名称与头像", e=e)
        cls._bot_data[bot_id] = Profile(
            bot_id=bot_id,
            introduction=introduction,
            avatar=avatar,
            name=name or BotConfig.self_nickname or "绪山真寻",
        )

    @classmethod
    async def get_bot_profile(cls, bot_id: str) -> Profile | None:
        if bot_id not in cls._bot_data:
            await cls._read_profile(bot_id)
        return cls._bot_data.get(bot_id)

    @classmethod
    def load_plugin_profile(cls):
        """加载插件自我介绍"""
        for plugin in nonebot.get_loaded_plugins():
            if plugin.module_name in cls._plugin_data:
                continue
            metadata = plugin.metadata
            if not metadata:
                continue
            extra = metadata.extra
            if not extra:
                continue
            extra_data = PluginExtraData.from_extra(extra, plugin.name)
            if extra_data.introduction or extra_data.precautions:
                cls._plugin_data[plugin.name] = PluginProfile(
                    name=metadata.name,
                    introduction=extra_data.introduction or "",
                    precautions=extra_data.precautions or [],
                )

    @classmethod
    def get_plugin_profile(cls) -> list[dict]:
        """获取插件自我介绍"""
        if not cls._plugin_data:
            cls.load_plugin_profile()
        return [model_dump(e) for e in cls._plugin_data.values()]

    @classmethod
    def is_auto_send_profile(cls) -> bool:
        """是否自动发送BOT自我介绍图片"""
        return Config.get_config("bot_profile", "AUTO_SEND_PROFILE")

    @classmethod
    async def build_bot_profile_image(
        cls, bot_id: str, tags: list[dict[str, str]] | None = None
    ) -> bytes | None:
        """构建BOT自我介绍图片"""
        profile, service_count, call_count = await asyncio.gather(
            cls.get_bot_profile(bot_id),
            UserConsole.get_new_uid(),
            Statistics.filter(bot_id=bot_id).count(),
        )
        if not profile:
            return None
        if not tags:
            tags = [
                {"text": f"服务人数: {service_count}"},
                {"text": f"调用次数: {call_count}"},
            ]
        profile_data = {
            "avatar": profile.avatar.absolute().as_uri() if profile.avatar else None,
            "bot_name": profile.name,
            "bot_description": profile.introduction,
            "service_count": service_count,
            "call_count": call_count,
            "plugin_list": cls.get_plugin_profile(),
            "tags": tags,
            "title": f"{BotConfig.self_nickname}简介",
            "night": is_night_mode(),
        }
        return await ui.render_template(
            "pages/builtin/bot_profile",
            data=profile_data,
            clip_selector=".page",
            clip_padding=0,
            disable_animations=True,
            use_cache=True,
        )


BotProfileManager.clear_profile_image()
