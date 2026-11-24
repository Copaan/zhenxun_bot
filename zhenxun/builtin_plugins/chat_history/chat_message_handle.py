from datetime import datetime, timedelta
from typing import cast

from nonebot.plugin import PluginMetadata
from nonebot_plugin_alconna import (
    Alconna,
    Args,
    Arparma,
    Match,
    Option,
    Query,
    on_alconna,
    store_true,
)
from nonebot_plugin_session import EventSession

from zhenxun import ui
from zhenxun.configs.config import BotConfig, Config
from zhenxun.configs.utils import Command, PluginExtraData, RegisterConfig
from zhenxun.models.chat_history import ChatHistory
from zhenxun.models.friend_user import FriendUser
from zhenxun.services.avatar_service import avatar_service
from zhenxun.services.help_service import is_night_mode
from zhenxun.services.hot_query_cache import get_group_member_map, get_member_names
from zhenxun.services.log import logger
from zhenxun.utils.enum import PluginType
from zhenxun.utils.message import MessageUtils
from zhenxun.utils.time_utils import TimeUtils

__plugin_meta__ = PluginMetadata(
    name="消息统计",
    description="消息统计查询",
    usage="""
    格式:
    消息排行 ?[type [日,周,月,季,年]] ?[--des]

    快捷:
    [日,周,月,季,年]消息排行 ?[数量]

    示例:
    消息排行             : 所有记录排行
    日消息排行           : 今日记录排行
    周消息排行           : 本周记录排行
    月消息排行           : 本月记录排行
    季消息排行           : 本季度记录排行
    年消息排行           : 本年记录排行
    消息排行 周 --des    : 逆序周记录排行
    """.strip(),
    extra=PluginExtraData(
        author="HibiKier",
        version="0.3",
        plugin_type=PluginType.NORMAL,
        menu_type="数据统计",
        supported_platform_scopes={"qq_client"},
        commands=[
            Command(command="消息统计"),
            Command(command="日消息统计"),
            Command(command="周消息排行"),
            Command(command="月消息排行"),
            Command(command="季消息排行"),
            Command(command="年消息排行"),
        ],
        configs=[
            RegisterConfig(
                module="chat_history",
                key="SHOW_QUIT_MEMBER",
                value=True,
                help="是否在消息排行中显示已退群用户",
                default_value=True,
                type=bool,
            )
        ],
    ).to_dict(),
)


_matcher = on_alconna(
    Alconna(
        "消息排行",
        Option("--des", action=store_true, help_text="逆序"),
        Args["type?", ["日", "周", "月", "季", "年"]]["count?", int, 10],
    ),
    aliases={"消息统计"},
    priority=5,
    block=True,
)

_matcher.shortcut(
    r"(?P<type>['日', '周', '月', '季', '年'])?消息(排行|统计)\s?(?P<cnt>\d+)?",
    command="消息排行",
    arguments=["{type}", "{cnt}"],
    prefix=True,
)


@_matcher.handle()
async def _(
    session: EventSession,
    arparma: Arparma,
    type: Match[str],
    count: Query[int] = Query("count", 10),
):
    group_id = session.id3 or session.id2
    time_now = datetime.now().replace(microsecond=0)
    date = type.result if type.available else None
    date_scope = None
    if date == "日":
        date_scope = (time_now.replace(hour=0, minute=0, second=0), time_now)
    elif days := {"周": 7, "月": 30, "季": 90, "年": 365}.get(date or ""):
        date_scope = (time_now - timedelta(days=days), time_now)
    show_quit_member = Config.get_config("chat_history", "SHOW_QUIT_MEMBER", True)

    fetch_count = count.result
    has_group_context = bool(group_id)
    if has_group_context and not show_quit_member:
        fetch_count = count.result * 2

    rank_data = cast(
        list[tuple[str, int]],
        await ChatHistory.get_group_msg_rank(
            group_id, fetch_count, "DES" if arparma.find("des") else "DESC", date_scope
        )
        or [],
    )
    platform = getattr(session, "platform", None) or "qq"
    user_ids_in_rank = [str(uid) for uid, _ in rank_data]
    users_in_group = {}
    user_names: dict[str, str] = {}
    if rank_data and has_group_context:
        users_in_group = await get_group_member_map(group_id, user_ids_in_rank)
    elif rank_data:
        friend_users = await FriendUser.filter(
            user_id__in=user_ids_in_rank
        ).values_list("user_id", "user_name")
        user_names.update(dict(friend_users))
        group_user_names = await get_member_names(user_ids_in_rank)
        for user_id, user_name in group_user_names.items():
            if user_name and user_id not in user_names:
                user_names[user_id] = user_name

    rows = []
    for uid_str, (_, num) in zip(user_ids_in_rank, rank_data):
        if len(rows) >= count.result:
            break
        quit = False
        if has_group_context:
            user_in_group = users_in_group.get(uid_str)
            if not user_in_group and not show_quit_member:
                continue
            quit = not user_in_group
            user_name = user_in_group.user_name if user_in_group else uid_str
        else:
            user_name = user_names.get(uid_str) or uid_str
        avatar_path = await avatar_service.get_avatar_path(platform, uid_str)
        rows.append(
            {
                "rank": len(rows) + 1,
                "name": user_name,
                "count": num,
                "quit": quit,
                "avatar": avatar_path.as_uri() if avatar_path else "",
            }
        )
    if not rows:
        await MessageUtils.build_message("群组消息记录为空...").finish()

    if date_scope:
        date_str = f"{date_scope[0]} - {date_scope[1]}"
    elif first_msg_time := await ChatHistory.get_group_first_msg_datetime(group_id):
        start = first_msg_time.astimezone(TimeUtils.zone()).replace(microsecond=0)
        date_str = f"{start.replace(tzinfo=None)} - 至今"
    else:
        date_str = f"{time_now} - 至今"

    image_bytes = await ui.render_template(
        "pages/builtin/chat_rank",
        data={
            "title": f"{date or ''}消息排行",
            "bot_nickname": BotConfig.self_nickname,
            "night": is_night_mode(),
            "date_range": date_str,
            "total": sum(row["count"] for row in rows),
            "rows": rows,
        },
        clip_selector=".page",
        clip_padding=0,
        disable_animations=True,
    )
    logger.info(
        f"查看消息排行 数量={count.result}", arparma.header_result, session=session
    )
    await MessageUtils.build_message(image_bytes).finish(reply_to=True)
