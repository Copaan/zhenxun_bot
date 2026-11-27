import re
import time
from typing import Literal

from nonebot_plugin_session import EventSession

from zhenxun.models.ban_console import BanConsole
from zhenxun.services.log import logger
from zhenxun.services.platform_identity import scoped_group_id
from zhenxun.utils.image_utils import BuildImage, ImageTemplate

ADMIN_BAN_LEVEL = 8
"""群管理的封禁等级，超级用户为 9999，系统自动封禁为 9"""
_ID_PATTERN = re.compile(r"[\w\-:.@]+", re.ASCII)
"""用户/群组 id 只能由字母、数字和 _-:.@ 组成"""


async def call_ban(user_id: str, reason: str | None = None, duration: int = 1):
    """调用ban

    参数:
        user_id: 用户id
    """
    await BanConsole.ban(user_id, None, 9, reason, duration * 60)
    logger.info("被讨厌了，已将用户加入黑名单...", "ban", session=user_id)


class BanManage:
    @classmethod
    def valid_id(cls, value: str) -> bool:
        """判断输入的用户/群组 id 格式是否有效

        参数:
            value: 用户/群组 id

        返回:
            bool: 是否有效
        """
        return bool(_ID_PATTERN.fullmatch(value))

    @classmethod
    async def build_ban_image(
        cls,
        filter_type: Literal["group", "user"] | None,
        user_id: str | None = None,
        group_id: str | None = None,
    ) -> BuildImage | None:
        """构造Ban列表图片

        参数:
            filter_type: 过滤类型
            user_id: 用户id
            group_id: 群组id

        返回:
            BuildImage | None: Ban列表图片
        """
        data_list = None
        query = BanConsole
        if user_id:
            query = query.filter(user_id=user_id)
        elif group_id:
            query = query.filter(group_id=group_id)
        elif filter_type == "user":
            query = query.filter(group_id__isnull=True)
        elif filter_type == "group":
            query = query.filter(user_id__isnull=True)
        data_list = await query.all()
        if not data_list:
            return None
        column_name = [
            "ID",
            "用户ID",
            "群组ID",
            "BAN LEVEL",
            "封禁原因",
            "剩余时长(分钟)",
            "操作员ID",
        ]
        row_data = []
        for data in data_list:
            if data.duration == -1:
                duration = "∞"
            else:
                duration = int((data.ban_time + data.duration - time.time()) / 60)
            if isinstance(duration, int) and duration < 0:
                await data.delete()
                continue
            row_data.append(
                [
                    data.id,
                    data.user_id,
                    data.group_id,
                    data.ban_level,
                    data.ban_reason,
                    duration,
                    data.operator,
                ]
            )
        return await ImageTemplate.table_page(
            "Ban / UnBan 列表", "在黑屋中狠狠调教!", column_name, row_data
        )

    @classmethod
    async def is_ban(cls, user_id: str, group_id: str | None):
        """判断用户是否被ban

        参数:
            user_id: 用户id

        返回:
            bool: 是否被ban
        """
        return await BanConsole.is_ban(user_id, group_id)

    @classmethod
    async def unban(
        cls,
        user_id: str | None,
        group_id: str | None,
        session: EventSession,
        idx: int | None = None,
        is_superuser: bool = False,
    ) -> tuple[bool, str]:
        """unban目标用户

        群管理只能解除群管理设下的封禁，按 id 解除时只能解除该群的记录。
        群内解除用户时先找该群的记录，没有时超级用户可解除该用户的全局封禁；
        只有确实删除了未过期的记录才算成功。

        参数:
            user_id: 用户id
            group_id: 群组id
            session: Session
            idx: 指定id
            is_superuser: 是否为超级用户操作

        返回:
            tuple[bool, str]: 是否unban成功, 用户/群组id或提示
        """
        user_level = 9999 if is_superuser else ADMIN_BAN_LEVEL
        if idx:
            ban_data = await BanConsole.get_ban(id=idx)
            if not ban_data:
                return False, "该用户/群组不在黑名单中捏..."
            if not is_superuser and ban_data.group_id != scoped_group_id(group_id):
                return False, "只能解除本群的黑名单记录捏..."
            if ban_data.ban_level > user_level:
                return False, "unBan权限等级不足捏..."
            await ban_data.delete()
            return True, ban_data.user_id or ban_data.group_id or ""
        if not user_id and not group_id:
            return False, "请指定要解除封禁的用户或群组捏..."
        ban_data = await BanConsole.get_active(user_id, group_id)
        if ban_data is None and user_id and group_id:
            ban_data = await BanConsole.get_active(user_id, None)
            if ban_data and not is_superuser:
                return False, "该用户被全局封禁，只有超级用户能解除捏..."
        if ban_data is None:
            return False, "该用户/群组不在黑名单中捏..."
        if ban_data.ban_level > user_level:
            return False, "unBan权限等级不足捏..."
        await ban_data.delete()
        return True, user_id or group_id or ""

    @classmethod
    async def ban(
        cls,
        user_id: str | None,
        group_id: str | None,
        reason: str | None,
        duration: int,
        session: EventSession,
        is_superuser: bool,
    ):
        """ban掉目标用户

        参数:
            user_id: 用户id
            group_id: 群组id
            reason: 理由
            duration: 时长，秒
            session: Session
            is_superuser: 是否为超级用户操作
        """
        level = 9999 if is_superuser else ADMIN_BAN_LEVEL
        await BanConsole.ban(user_id, group_id, level, reason, duration, session.id1)
