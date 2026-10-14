import asyncio
from datetime import datetime
import random
import time

from nonebot import on_message, on_request
from nonebot.adapters.onebot.v11 import (
    ActionFailed,
    FriendRequestEvent,
    GroupRequestEvent,
)
from nonebot.adapters.onebot.v11 import Bot as v11Bot
from nonebot.plugin import PluginMetadata
from nonebot_plugin_session import EventSession

from zhenxun.configs.config import BotConfig, Config
from zhenxun.configs.utils import PluginExtraData, RegisterConfig
from zhenxun.models.ban_console import BanConsole
from zhenxun.models.event_log import EventLog
from zhenxun.models.fg_request import FgRequest
from zhenxun.models.friend_user import FriendUser
from zhenxun.models.group_console import GroupConsole
from zhenxun.services.cache import CacheRoot
from zhenxun.services.log import logger
from zhenxun.utils.enum import EventLogType, PluginType, RequestHandleType, RequestType
from zhenxun.utils.exception import NotFindSuperuser
from zhenxun.utils.platform import PlatformUtils

base_config = Config.get("invite_manager")

__plugin_meta__ = PluginMetadata(
    name="记录请求",
    description="记录 好友/群组 请求",
    usage="",
    extra=PluginExtraData(
        author="HibiKier",
        version="0.1",
        plugin_type=PluginType.HIDDEN,
        configs=[
            RegisterConfig(
                module="invite_manager",
                key="AUTO_ADD_FRIEND",
                value=False,
                help="是否自动同意好友添加",
                type=bool,
                default_value=False,
            ),
            RegisterConfig(
                module="invite_manager",
                key="AUTO_ADD_GROUP",
                value=False,
                help="是否自动同意邀请入群",
                type=bool,
                default_value=False,
            ),
            RegisterConfig(
                module="invite_manager",
                key="TIP_MESSAGE_LIMIT",
                value=360,
                help="重复申请与退群提醒过滤时间（分钟）",
                type=int,
                default_value=360,
            ),
        ],
    ).to_dict(),
)


# TODO: 其他平台请求

friend_req = on_request(priority=5, block=True)
group_req = on_request(priority=5, block=True)
_t = on_message(priority=999, block=False, rule=lambda: False)


cache = CacheRoot.cache_dict("FG_REQUEST_CACHE", 0, float)
_API_TIMEOUT = 5.0


def _recently_notified(key: str) -> bool:
    """是否在 TIP_MESSAGE_LIMIT 分钟内已通知过同一请求

    参数:
        key: 请求键

    返回:
        bool: 是否跳过本次通知
    """
    limit = (base_config.get("TIP_MESSAGE_LIMIT") or 360) * 60
    return time.time() - (cache.get(key) or 0) < limit


async def _notify_superuser(bot, request: FgRequest, message: str) -> bool:
    """通知超级用户，并记录通知消息id用于引用处理

    参数:
        bot: Bot
        request: 请求记录
        message: 通知内容

    返回:
        bool: 是否至少有一位超级用户收到通知
    """
    try:
        results = await PlatformUtils.send_superuser(bot, message)
    except NotFindSuperuser:
        logger.warning(
            "未配置超级用户，无法发送请求通知，请检查 SUPERUSERS / PLATFORM_SUPERUSERS",
            "请求通知",
        )
        return False
    if message_ids := [
        str(r[1].msg_ids[0]["message_id"]) for r in results if r[1] and r[1].msg_ids
    ]:
        request.message_ids = ",".join(message_ids)
        await request.save(update_fields=["message_ids"])
    if not any(r[1] for r in results):
        logger.warning("请求通知未能发送给任何超级用户", "请求通知")
        return False
    return True


async def _safe_get_group_info(bot, group_id: str):
    try:
        return await asyncio.wait_for(
            bot.get_group_info(group_id=group_id),
            timeout=_API_TIMEOUT,
        )
    except (asyncio.TimeoutError, ActionFailed, Exception) as e:
        logger.warning("获取群信息失败", "群邀请", e=e)
        return None


async def _get_nickname(bot, user_id: str) -> str:
    """获取请求人昵称，失败时使用好友记录或用户id

    参数:
        bot: Bot
        user_id: 用户id

    返回:
        str: 昵称
    """
    try:
        user = await asyncio.wait_for(
            bot.get_stranger_info(user_id=int(user_id)), timeout=_API_TIMEOUT
        )
        if nickname := (user or {}).get("nickname"):
            return str(nickname)
    except Exception as e:
        logger.warning("获取用户信息失败", "请求通知", target=user_id, e=e)
    return await FriendUser.get_user_name(user_id) or user_id


async def _send_private(bot, user_id: str, message: str):
    """给请求人发送私聊提示，失败只记录日志

    参数:
        bot: Bot
        user_id: 用户id
        message: 消息内容
    """
    try:
        await bot.send_private_msg(user_id=int(user_id), message=message)
    except Exception as e:
        logger.warning("给请求人发送私聊失败", "请求通知", target=user_id, e=e)


def _format_ban_target(ban_data: BanConsole) -> str:
    user_id = ban_data.user_id or ""
    group_id = ban_data.group_id or ""
    if user_id and group_id:
        return f"用户 {user_id} 在群组 {group_id}"
    if user_id:
        return f"用户 {user_id}"
    return f"群组 {group_id}"


async def _build_permanent_ban_tip(
    *targets: tuple[str | None, str | None],
) -> str:
    ban_list: list[BanConsole] = []
    seen: set[tuple[str, str]] = set()
    for user_id, group_id in targets:
        ban_data = await BanConsole.get_ban(user_id=user_id, group_id=group_id)
        if not ban_data or ban_data.duration != -1:
            continue
        key = (ban_data.user_id or "", ban_data.group_id or "")
        if key in seen:
            continue
        seen.add(key)
        ban_list.append(ban_data)
    if not ban_list:
        return ""
    lines = ["", "永久黑名单提示："]
    for ban_data in ban_list:
        lines.extend(
            [
                f"- {_format_ban_target(ban_data)} 在黑名单中",
                f"  操作员ID：{ban_data.operator}",
                f"  封禁原因：{ban_data.ban_reason or '无'}",
            ]
        )
    return "\n".join(lines)


async def _auto_approve_friend(bot, event: FriendRequestEvent, nickname: str) -> bool:
    """自动同意好友请求

    参数:
        bot: Bot
        event: 好友请求事件
        nickname: 昵称

    返回:
        bool: 是否同意成功
    """
    await asyncio.sleep(random.randint(1, 10))
    try:
        await bot.set_friend_add_request(flag=event.flag, approve=True)
    except Exception as e:
        logger.error(
            "自动同意好友请求失败，转为人工处理", "好友请求", target=event.user_id, e=e
        )
        return False
    await FriendUser.update_or_create(
        user_id=str(event.user_id), defaults={"user_name": nickname}
    )
    return True


@friend_req.handle()
async def _(bot: v11Bot, event: FriendRequestEvent, session: EventSession):
    user_id = str(event.user_id)
    logger.debug("收录好友请求...", "好友请求", target=user_id)
    nickname = await _get_nickname(bot, user_id)
    if base_config.get("AUTO_ADD_FRIEND") and await _auto_approve_friend(
        bot, event, nickname
    ):
        logger.debug("已自动同意好友请求", "好友请求", target=user_id)
        return
    # 旧请求全部设置为过期
    await FgRequest.filter(
        request_type=RequestType.FRIEND,
        bot_id=bot.self_id,
        user_id=user_id,
        handle_type__isnull=True,
    ).update(handle_type=RequestHandleType.EXPIRE)
    f = await FgRequest.create(
        request_type=RequestType.FRIEND,
        platform=session.platform,
        bot_id=bot.self_id,
        flag=event.flag,
        user_id=user_id,
        nickname=nickname,
        comment=event.comment,
    )
    cache_key = f"friend:{bot.self_id}:{user_id}"
    if _recently_notified(cache_key):
        logger.debug(
            f"好友请求在提醒间隔内重复，已记录为 ID {f.id}，不再提醒",
            "好友请求",
            target=user_id,
        )
        return
    ban_tip = await _build_permanent_ban_tip((user_id, None))
    if await _notify_superuser(
        bot,
        f,
        f"*****一份好友申请*****\n"
        f"ID: {f.id}\n"
        f"昵称：{nickname}({user_id})\n"
        f"自动同意：{'√' if base_config.get('AUTO_ADD_FRIEND') else '×'}\n"
        f"日期：{datetime.now().replace(microsecond=0)}\n"
        f"备注：{event.comment}"
        f"{ban_tip}",
    ):
        cache[cache_key] = time.time()


async def _auto_approve_group(bot, event: GroupRequestEvent) -> bool:
    """自动同意群组邀请并写入群认证

    参数:
        bot: Bot
        event: 群组请求事件

    返回:
        bool: 是否同意成功
    """
    try:
        await bot.set_group_add_request(
            flag=event.flag, sub_type="invite", approve=True
        )
    except Exception as e:
        logger.error(
            "自动同意群组邀请失败，转为人工处理",
            "群聊请求",
            session=event.user_id,
            target=event.group_id,
            e=e,
        )
        return False
    group, _ = await GroupConsole.get_or_create_root_group(
        group_id=str(event.group_id),
        defaults={
            "group_name": "",
            "max_member_count": 0,
            "member_count": 0,
            "group_flag": 1,
        },
        update_defaults=True,
    )
    if group_info := await _safe_get_group_info(bot, str(event.group_id)):
        group.max_member_count = group_info.get("max_member_count", 0)
        group.member_count = group_info.get("member_count", 0)
        group.group_name = group_info.get("group_name", "")
        await group.save(
            update_fields=["group_name", "max_member_count", "member_count"]
        )
    return True


async def _record_auto_approved_group(
    bot, event: GroupRequestEvent, session: EventSession, nickname: str
):
    """记录非超级用户的自动同意入群，并通知超级用户与邀请人

    参数:
        bot: Bot
        event: 群组请求事件
        session: EventSession
        nickname: 邀请人昵称
    """
    user_id = str(event.user_id)
    group_id = str(event.group_id)
    f = await FgRequest.create(
        request_type=RequestType.GROUP,
        platform=session.platform,
        bot_id=bot.self_id,
        flag=event.flag,
        user_id=user_id,
        nickname=nickname,
        group_id=group_id,
        handle_type=RequestHandleType.APPROVE,
    )
    ban_tip = await _build_permanent_ban_tip(
        (user_id, None), (user_id, group_id), (None, group_id)
    )
    await _notify_superuser(
        bot,
        f,
        f"*****一份入群申请*****\n"
        f"ID：{f.id}\n"
        f"申请人：{nickname}({user_id})\n群聊："
        f"{group_id}\n邀请日期：{datetime.now().replace(microsecond=0)}\n"
        "注: 该请求已自动同意"
        f"{ban_tip}",
    )
    await asyncio.sleep(random.randint(1, 5))
    await _send_private(
        bot,
        user_id,
        f"管理员已开启自动同意群组邀请，请不要让{BotConfig.self_nickname}受委屈哦（狠狠监控）"
        "\n在群组中 群组管理员与群主 允许使用管理员帮助"
        "（包括ban与功能开关等）\n请在群组中发送 '管理员帮助'",
    )


@group_req.handle()
async def _(bot: v11Bot, event: GroupRequestEvent, session: EventSession):
    if event.sub_type != "invite":
        return
    user_id = str(event.user_id)
    group_id = str(event.group_id)
    is_superuser = user_id in bot.config.superusers
    nickname = await _get_nickname(bot, user_id)
    if is_superuser or base_config.get("AUTO_ADD_GROUP"):
        logger.debug(
            "超级用户邀请或开启自动同意入群，自动同意加入群聊",
            "群聊请求",
            session=user_id,
            target=group_id,
        )
        if await _auto_approve_group(bot, event):
            if not is_superuser:
                await _record_auto_approved_group(bot, event, session, nickname)
            return
    logger.debug(
        f"收录 用户[{user_id}] 群聊[{group_id}] 群聊请求",
        "群聊请求",
        target=group_id,
    )
    # 旧请求全部设置为过期
    await FgRequest.filter(
        request_type=RequestType.GROUP,
        bot_id=bot.self_id,
        user_id=user_id,
        group_id=group_id,
        handle_type__isnull=True,
    ).update(handle_type=RequestHandleType.EXPIRE)
    f = await FgRequest.create(
        request_type=RequestType.GROUP,
        platform=session.platform,
        bot_id=bot.self_id,
        flag=event.flag,
        user_id=user_id,
        nickname=nickname,
        group_id=group_id,
    )
    cache_key = f"group:{bot.self_id}:{user_id}:{group_id}"
    if _recently_notified(cache_key):
        logger.debug(
            f"群聊请求在提醒间隔内重复，已记录为 ID {f.id}，不再提醒",
            "群聊请求",
            target=f"{user_id}:{group_id}",
        )
        return
    kick_count = await EventLog.filter(
        group_id=group_id, event_type=EventLogType.KICK_BOT
    ).count()
    kick_message = (
        f"\n该群累计踢出{BotConfig.self_nickname} <{kick_count}>次"
        if kick_count
        else ""
    )
    ban_tip = await _build_permanent_ban_tip(
        (user_id, None), (user_id, group_id), (None, group_id)
    )
    if await _notify_superuser(
        bot,
        f,
        f"*****一份入群申请*****\n"
        f"ID：{f.id}\n"
        f"申请人：{nickname}({user_id})\n群聊："
        f"{group_id}\n邀请日期：{datetime.now().replace(microsecond=0)}"
        f"{kick_message}"
        f"{ban_tip}",
    ):
        cache[cache_key] = time.time()
    await _send_private(
        bot,
        user_id,
        f"想要邀请我偷偷入群嘛~已经提醒{BotConfig.self_nickname}的管理员大人了\n"
        "请确保已经群主或群管理沟通过！\n"
        "等待管理员处理吧！",
    )
