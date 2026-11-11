import asyncio
from datetime import datetime
import random
import time

from nonebot import on_request
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
from zhenxun.models.fg_request import FgRequest, notice_cache
from zhenxun.models.friend_user import FriendUser
from zhenxun.models.group_console import GroupConsole
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

_API_TIMEOUT = 5.0


def _reserve_notice(request: FgRequest) -> bool:
    """占用 TIP_MESSAGE_LIMIT 分钟的提醒间隔，通知失败时需调用 _release_notice

    参数:
        request: 请求记录

    返回:
        bool: 是否需要提醒，间隔内已提醒过或刚被拒绝时为 False
    """
    key = request.notice_key()
    if key in notice_cache:
        return False
    limit = (base_config.get("TIP_MESSAGE_LIMIT") or 360) * 60
    notice_cache.set(key, time.time(), limit)
    return True


async def _create_request(**fields) -> FgRequest:
    """记录请求，并将同一 Bot、用户与群组下更早的未处理请求设为过期

    参数:
        fields: 请求字段

    返回:
        FgRequest: 新请求，并发重复事件下只保留 id 最大的一条未处理请求
    """
    request = await FgRequest.create(**fields)
    await FgRequest.filter(
        request_type=request.request_type,
        bot_id=request.bot_id,
        user_id=request.user_id,
        group_id=request.group_id,
        handle_type__isnull=True,
        id__lt=request.id,
    ).update(handle_type=RequestHandleType.EXPIRE)
    return request


def _release_notice(request: FgRequest):
    """释放提醒间隔，下次相同请求重新提醒

    参数:
        request: 请求记录
    """
    notice_cache.pop(request.notice_key(), None)


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
            f"请求 ID {request.id}（Bot {bot.self_id}）没有可用超级用户，"
            "请检查 SUPERUSERS / PLATFORM_SUPERUSERS；请求仍可在 WebUI 处理",
            "请求通知",
        )
        return False
    except Exception as error:
        logger.error(
            f"请求 ID {request.id}（Bot {bot.self_id}）通知失败，"
            f"错误类型：{type(error).__name__}；请求仍可在 WebUI 处理",
            "请求通知",
        )
        return False
    if message_ids := [
        str(r[1].msg_ids[0]["message_id"]) for r in results if r[1] and r[1].msg_ids
    ]:
        request.message_ids = ",".join(message_ids)
        try:
            await request.save(update_fields=["message_ids"])
        except Exception as error:
            logger.warning(
                f"请求 ID {request.id}（Bot {bot.self_id}）通知已发送，"
                f"引用消息记录保存失败：{type(error).__name__}；可按请求 ID 处理",
                "请求通知",
            )
    if not any(r[1] for r in results):
        logger.warning(
            f"请求 ID {request.id}（Bot {bot.self_id}）未送达任何超级用户，"
            "请检查接收账号及私聊发送错误；请求仍可在 WebUI 处理",
            "请求通知",
        )
        return False
    logger.info(
        f"请求 ID {request.id}（Bot {bot.self_id}）已通知"
        f" {sum(bool(result[1]) for result in results)} 位超级用户",
        "请求通知",
    )
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


async def _refuse_blacklisted(
    bot,
    event: FriendRequestEvent | GroupRequestEvent,
    *targets: tuple[str | None, str | None],
) -> bool:
    """目标在永久黑名单中时直接拒绝请求，不提醒超级用户

    参数:
        bot: Bot
        event: 好友或群组请求事件
        targets: (用户id, 群组id) 检查目标

    返回:
        bool: 是否已拒绝
    """
    for user_id, group_id in targets:
        ban_data = await BanConsole.get_ban(user_id=user_id, group_id=group_id)
        if ban_data and ban_data.duration == -1:
            break
    else:
        return False
    try:
        if isinstance(event, FriendRequestEvent):
            await bot.set_friend_add_request(flag=event.flag, approve=False)
        else:
            await bot.set_group_add_request(
                flag=event.flag, sub_type="invite", approve=False
            )
    except Exception as e:
        logger.warning("拒绝永久黑名单请求失败", "请求处理", e=e)
    logger.info(
        f"{_format_ban_target(ban_data)} 在永久黑名单中，已拒绝请求"
        f"（原因：{ban_data.ban_reason or '无'}）",
        "请求处理",
        target=event.user_id,
    )
    return True


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
    if await _refuse_blacklisted(bot, event, (user_id, None)):
        return
    nickname = await _get_nickname(bot, user_id)
    auto_add = base_config.get("AUTO_ADD_FRIEND")
    if auto_add and await _auto_approve_friend(bot, event, nickname):
        logger.debug("已自动同意好友请求", "好友请求", target=user_id)
        return
    f = await _create_request(
        request_type=RequestType.FRIEND,
        platform=session.platform,
        bot_id=bot.self_id,
        flag=event.flag,
        user_id=user_id,
        nickname=nickname,
        comment=event.comment,
    )
    if not _reserve_notice(f):
        logger.debug(
            f"好友请求在提醒间隔内重复，已记录为 ID {f.id}，不再提醒",
            "好友请求",
            target=user_id,
        )
        return
    if not await _notify_superuser(
        bot,
        f,
        f"*****一份好友申请*****\n"
        f"ID: {f.id}\n"
        f"昵称：{nickname}({user_id})\n"
        f"自动同意：{'失败，需人工处理' if auto_add else '×'}\n"
        f"日期：{datetime.now().replace(microsecond=0)}\n"
        f"备注：{event.comment}",
    ):
        _release_notice(f)


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
    await _notify_superuser(
        bot,
        f,
        f"*****一份入群申请*****\n"
        f"ID：{f.id}\n"
        f"申请人：{nickname}({user_id})\n群聊："
        f"{group_id}\n邀请日期：{datetime.now().replace(microsecond=0)}\n"
        "注: 该请求已自动同意",
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
    is_superuser = PlatformUtils.is_superuser(bot, user_id)
    if not is_superuser and await _refuse_blacklisted(
        bot, event, (user_id, None), (user_id, group_id), (None, group_id)
    ):
        return
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
    f = await _create_request(
        request_type=RequestType.GROUP,
        platform=session.platform,
        bot_id=bot.self_id,
        flag=event.flag,
        user_id=user_id,
        nickname=nickname,
        group_id=group_id,
    )
    if not _reserve_notice(f):
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
    notified = await _notify_superuser(
        bot,
        f,
        f"*****一份入群申请*****\n"
        f"ID：{f.id}\n"
        f"申请人：{nickname}({user_id})\n群聊："
        f"{group_id}\n邀请日期：{datetime.now().replace(microsecond=0)}"
        f"{kick_message}",
    )
    if not notified:
        _release_notice(f)
    await _send_private(
        bot,
        user_id,
        (
            f"入群邀请已记录，ID：{f.id}，已提醒管理员，请等待审核。"
            if notified
            else f"入群邀请已记录，ID：{f.id}，但管理员通知暂未送达。"
            "请联系管理员在 WebUI 的好友/群组请求中处理。"
        ),
    )
