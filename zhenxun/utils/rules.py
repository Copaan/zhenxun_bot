from nonebot.adapters import Bot, Event
from nonebot.internal.rule import Rule
from nonebot.permission import SUPERUSER
from nonebot_plugin_session import EventSession
from nonebot_plugin_uninfo import Uninfo

from zhenxun.configs.config import Config
from zhenxun.models.group_console import GroupConsole
from zhenxun.utils.platform import PlatformUtils


def admin_check(a: int | str = 1, key: str | None = None) -> Rule:
    """
    群管理员检查，超级用户总是通过

    参数:
        a: 权限等级或 配置项 module，等级大于 0 时要求群主或管理员
        key: 配置项 key.

    返回:
        Rule: Rule
    """

    async def _rule(bot: Bot, event: Event, session: Uninfo) -> bool:
        if await SUPERUSER(bot, event):
            return True
        if not (session.group or session.guild):
            return False
        level = Config.get_config(a, key) if isinstance(a, str) and key else a
        if level is None:
            return False
        return int(level) <= 0 or await PlatformUtils.is_group_admin(bot, session)

    return Rule(_rule)


def ensure_group(session: Uninfo) -> bool:
    """
    是否在群聊中

    参数:
        session: Uninfo

    返回:
        bool: bool
    """
    return bool(session.group)


def ensure_private(session: EventSession) -> bool:
    """
    是否在私聊中

    参数:
        session: session

    返回:
        bool: bool
    """
    return not session.id3 and not session.id2


def notice_rule(event_type: type | list[type]) -> Rule:
    """
    Notice限制

    参数:
        event_type: Event类型

    返回:
        Rule: Rule
    """

    async def _rule(event: Event) -> bool:
        if isinstance(event_type, list):
            for et in event_type:
                if isinstance(event, et):
                    return True
        else:
            return isinstance(event, event_type)
        return False

    return Rule(_rule)


def is_allowed_call() -> Rule:
    """是否允许调用插件"""

    async def _rule(session: Uninfo) -> bool:
        from zhenxun.models.ban_console import BanConsole

        group_id = session.group.id if session.group else None
        if group_id and await BanConsole.is_ban(None, group_id):
            return False
        if await BanConsole.is_ban(session.user.id, group_id):
            return False
        if group_id:
            if g := await GroupConsole.get_group(group_id):
                if g.level < 0:
                    return False
        return True

    return Rule(_rule)
