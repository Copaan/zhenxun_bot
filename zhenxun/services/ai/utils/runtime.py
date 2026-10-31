from typing import Any


class ContextUtils:
    """
    从底层依赖容器 (deps) 中提取运行环境信息的纯静态工具类。
    """

    @staticmethod
    def extract_user_id(deps: Any) -> str | None:
        """从依赖容器中提取当前用户的 ID"""
        if not deps:
            return None
        if hasattr(deps, "user_id") and getattr(deps, "user_id") is not None:
            return str(getattr(deps, "user_id"))
        event = getattr(deps, "event", None)
        if event:
            try:
                return str(event.get_user_id())
            except Exception:
                return (
                    str(getattr(event, "user_id", ""))
                    or str(getattr(event, "sender_id", ""))
                    or None
                )
        return None

    @staticmethod
    def extract_group_id(deps: Any) -> str | None:
        """从依赖容器中提取当前群聊或频道的 ID

        依次使用 group_id、QQ 官方群聊的 group_openid、QQ 频道消息的
        channel_id（频道私信视为私聊）。

        参数:
            deps: 依赖容器

        返回:
            str | None: 群聊或频道 ID，私聊为 None
        """
        if not deps:
            return None
        if getattr(deps, "group_id", None) is not None:
            return str(getattr(deps, "group_id"))
        event = getattr(deps, "event", None)
        if event is None:
            return None
        for key in ("group_id", "group_openid"):
            if value := getattr(event, key, None):
                return str(value)
        if channel_id := getattr(event, "channel_id", None):
            from nonebot.adapters.qq import DirectMessageCreateEvent

            if not isinstance(event, DirectMessageCreateEvent):
                return str(channel_id)
        return None

    @staticmethod
    def extract_platform(deps: Any) -> str:
        """从依赖容器的 Bot 实例中提取当前聊天平台名称"""
        if not deps:
            return "unknown"
        if hasattr(deps, "platform") and getattr(deps, "platform") is not None:
            return str(getattr(deps, "platform"))
        bot = getattr(deps, "bot", None)
        if bot:
            from zhenxun.utils.platform import PlatformUtils

            return PlatformUtils.get_platform(bot)
        return "unknown"

    @staticmethod
    def default_session_id(deps: Any) -> str:
        """按平台、Bot、群聊与用户生成默认会话 ID

        参数:
            deps: 依赖容器

        返回:
            str: 形如“平台:Bot:群号或 private:用户”的会话 ID
        """
        bot = getattr(deps, "bot", None)
        return ":".join(
            [
                ContextUtils.extract_platform(deps),
                str(getattr(bot, "self_id", "") or "unknown"),
                ContextUtils.extract_group_id(deps) or "private",
                ContextUtils.extract_user_id(deps) or "unknown",
            ]
        )


class PermissionUtils:
    """运行时权限校验通用工具类"""

    @staticmethod
    async def check_superuser(context: Any) -> bool:
        """异步校验当前运行上下文中的用户是否为超级用户"""
        bot = context.get_bot()
        event = context.get_event()
        if bot and event:
            from nonebot.permission import SUPERUSER

            return await SUPERUSER(bot, event)
        return False

    @staticmethod
    async def check_admin_level(context: Any, min_level: int) -> bool:
        """校验当前用户的管理权限

        参数:
            context: 运行上下文
            min_level: 权限等级，大于 0 时要求超级用户或当前群的群主、管理员

        返回:
            bool: 是否满足
        """
        if min_level <= 0 or await PermissionUtils.check_superuser(context):
            return True
        bot = context.get_bot()
        event = context.get_event()
        if not bot or not event:
            return False

        from nonebot_plugin_uninfo import get_session

        from zhenxun.utils.platform import PlatformUtils

        session = await get_session(bot, event)
        return bool(session) and await PlatformUtils.is_group_admin(bot, session)
