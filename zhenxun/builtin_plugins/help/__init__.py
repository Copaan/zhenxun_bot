from nonebot import on_notice
from nonebot.adapters import Bot, Event
from nonebot.adapters.qq.event import FriendAddEvent, GroupAddRobotEvent
from nonebot.matcher import Matcher
from nonebot.plugin import PluginMetadata
from nonebot.rule import to_me
from nonebot_plugin_alconna import (
    Alconna,
    AlconnaQuery,
    Args,
    Match,
    Option,
    Query,
    on_alconna,
    store_true,
)
from nonebot_plugin_uninfo import Uninfo

from zhenxun.adapters.qq_official.presentation import send_configured_card
from zhenxun.configs.config import Config
from zhenxun.configs.utils import PluginExtraData, RegisterConfig
from zhenxun.services.log import logger
from zhenxun.utils.enum import PluginType
from zhenxun.utils.message import MessageUtils
from zhenxun.utils.platform import PlatformUtils

from .data_source import create_help_img, get_plugin_help

__plugin_meta__ = PluginMetadata(
    name="帮助",
    description="帮助",
    usage="",
    extra=PluginExtraData(
        author="HibiKier",
        version="0.2",
        plugin_type=PluginType.DEPENDANT,
        is_show=False,
        configs=[
            RegisterConfig(
                key="HELP_STYLE",
                value="default",
                help="帮助页面的显示样式 (可选值: 'default', 'simple')",
                default_value="default",
                type=str,
            ),
            RegisterConfig(
                key="AUTO_NIGHT_MODE",
                value=True,
                help="帮助图片是否按时间自动切换日间/夜间配色",
                default_value=True,
                type=bool,
            ),
            RegisterConfig(
                key="NIGHT_HOURS",
                value="19-7",
                help="夜间配色时段，格式为 开始时-结束时，例如 19-7",
                default_value="19-7",
                type=str,
            ),
        ],
    ).to_dict(),
)


_matcher = on_alconna(
    Alconna(
        "功能",
        Args["name?", str],
        Option("-d|--detail", action=store_true, help_text="详细帮助"),
    ),
    aliases={"help", "帮助"},
    rule=to_me(),
    priority=1,
    block=True,
)

_menu_matcher = on_alconna(
    Alconna(
        "菜单",
        Args["name?", str],
        Option("-d|--detail", action=store_true, help_text="详细帮助"),
    ),
    rule=to_me(),
    priority=1,
    block=True,
)


_matcher.shortcut(
    r"详细帮助",
    command="功能",
    arguments=["--detail"],
    prefix=True,
)


@_matcher.handle()
@_menu_matcher.handle()
async def _(
    bot: Bot,
    event: Event,
    name: Match[str],
    session: Uninfo,
    matcher: Matcher,
    is_detail: Query[bool] = AlconnaQuery("detail.value", False),
):
    if (
        isinstance(matcher, _menu_matcher)
        and not name.available
        and not is_detail.result
    ):
        if await send_configured_card(bot, event, "help"):
            return
    _is_superuser = PlatformUtils.is_superuser(bot, session.user.id)

    if name.available:
        is_admin = await PlatformUtils.is_group_admin(bot, session)
        help_style = Config.get_config("help", "HELP_STYLE")
        variant = help_style if help_style != "default" else None

        traditional_help_result = await get_plugin_help(
            is_admin, name.result, _is_superuser, variant=variant
        )

        if traditional_help_result is not None:
            await MessageUtils.build_message(traditional_help_result).send(
                reply_to=True
            )
            logger.info(f"查看帮助详情: {name.result}", "帮助", session=session)
        else:
            await MessageUtils.build_message("没有查找到这个功能噢...").send(
                reply_to=True
            )
            logger.info(
                f"查看帮助详情失败，未找到: {name.result}", "帮助", session=session
            )
    elif session.group and (gid := session.group.id):
        image_bytes = await create_help_img(session, gid, is_detail.result)
        await MessageUtils.build_message(image_bytes).finish()
    else:
        image_bytes = await create_help_img(session, None, is_detail.result)
        await MessageUtils.build_message(image_bytes).finish()


_qq_welcome = on_notice(
    rule=lambda event: isinstance(event, FriendAddEvent | GroupAddRobotEvent),
    priority=10,
    block=False,
)


@_qq_welcome.handle()
async def _send_qq_welcome(bot: Bot, event: Event):
    """按账号设置回复加好友或机器人入群事件。"""
    kind = "welcome_friend" if isinstance(event, FriendAddEvent) else "welcome_group"
    await send_configured_card(bot, event, kind)
