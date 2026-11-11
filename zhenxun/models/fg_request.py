import asyncio
import contextlib
import time
from typing import ClassVar
from typing_extensions import Self

import nonebot
from nonebot.adapters import Bot
from tortoise import fields

from zhenxun.configs.config import BotConfig
from zhenxun.models.ban_console import BanConsole
from zhenxun.models.group_console import GroupConsole
from zhenxun.services.cache import CacheRoot
from zhenxun.services.db_context import Model
from zhenxun.services.log import logger
from zhenxun.utils.enum import RequestHandleType, RequestType
from zhenxun.utils.exception import NotFoundError, RequestHandledError
from zhenxun.utils.manager.bot_profile_manager import BotProfileManager
from zhenxun.utils.message import MessageUtils
from zhenxun.utils.platform import PlatformUtils

REFUSE_NOTICE_DELAY = 300
"""拒绝后同一请求恢复提醒超级用户的间隔（秒）"""
REFUSE_BLACKLIST_COUNT = 2
"""同一用户的好友请求或同一群组的邀请被拒绝达到该次数后加入永久黑名单"""
notice_cache = CacheRoot.cache_dict("FG_REQUEST_NOTICE", 0, float)
"""请求提醒静默缓存，键存在期间不再提醒超级用户"""


class FgRequest(Model):
    id = fields.IntField(pk=True, generated=True, auto_increment=True)
    """自增id"""
    request_type = fields.CharEnumField(
        RequestType, default=None, description="请求类型"
    )
    """请求类型"""
    platform = fields.CharField(255, description="平台")
    """平台"""
    bot_id = fields.CharField(255, description="Bot Id")
    """botId"""
    flag = fields.CharField(max_length=255, default="", description="flag")
    """flag"""
    user_id = fields.CharField(max_length=255, description="请求用户id")
    """请求用户id"""
    group_id = fields.CharField(max_length=255, null=True, description="邀请入群id")
    """邀请入群id"""
    nickname = fields.CharField(max_length=255, description="请求人名称")
    """对象名称"""
    comment = fields.CharField(max_length=255, null=True, description="验证信息")
    """验证信息"""
    handle_type = fields.CharEnumField(
        RequestHandleType, null=True, description="处理类型"
    )
    """处理类型"""
    message_ids = fields.CharField(max_length=255, null=True, description="消息id列表")
    """消息id列表"""
    blacklisted = False
    """本次拒绝后是否已加入永久黑名单，不入库"""

    class Meta:  # pyright: ignore [reportIncompatibleVariableOverride]
        table = "fg_request"
        table_description = "好友群组请求"
        indexes: ClassVar = [("handle_type", "request_type"), ("bot_id", "handle_type")]

    def notice_key(self) -> str:
        """提醒静默缓存键

        返回:
            str: 好友请求按 Bot 与用户区分，群组请求另按群组区分
        """
        if self.request_type == RequestType.FRIEND:
            return f"friend:{self.bot_id}:{self.user_id}"
        return f"group:{self.bot_id}:{self.user_id}:{self.group_id}"

    @classmethod
    async def approve(cls, bot: Bot, id: int) -> Self:
        """同意请求

        参数:
            bot: Bot
            id: 请求id

        异常:
            NotFoundError: 未发现请求
        """
        return await cls._handle_request(bot, id, RequestHandleType.APPROVE)

    @classmethod
    async def refused(cls, bot: Bot, id: int) -> Self:
        """拒绝请求

        参数:
            bot: Bot
            id: 请求id

        异常:
            NotFoundError: 未发现请求
        """
        return await cls._handle_request(bot, id, RequestHandleType.REFUSED)

    @classmethod
    async def ignore(cls, id: int) -> Self:
        """忽略请求

        参数:
            id: 请求id

        异常:
            NotFoundError: 未发现请求
        """
        return await cls._handle_request(None, id, RequestHandleType.IGNORE)

    @classmethod
    async def expire(cls, id: int):
        """忽略请求

        参数:
            id: 请求id

        异常:
            NotFoundError: 未发现请求
        """
        await cls._handle_request(None, id, RequestHandleType.EXPIRE)

    @classmethod
    async def _handle_request(
        cls,
        bot: Bot | None,
        id: int,
        handle_type: RequestHandleType,
    ) -> Self:
        """处理请求，同意/拒绝时先调用协议接口，成功后才保存处理状态

        已因重复申请过期的请求会转为处理同一 Bot、用户与群组下最新的未处理请求；
        拒绝后该请求 REFUSE_NOTICE_DELAY 秒内不再提醒，并检查是否需加入永久黑名单

        参数:
            bot: Bot，优先使用请求记录中的 bot，未连接时使用传入的 bot
            id: 请求id
            handle_type: 处理类型

        返回:
            Self: 实际处理的请求

        异常:
            NotFoundError: 未发现请求
            RequestHandledError: 请求已被处理
        """
        req = await cls.get_or_none(id=id)
        if not req:
            raise NotFoundError
        if (
            req.handle_type == RequestHandleType.EXPIRE
            and handle_type != RequestHandleType.EXPIRE
        ):
            req = await cls._latest_pending(req) or req
        if req.handle_type is not None and handle_type != RequestHandleType.EXPIRE:
            raise RequestHandledError
        if bot and handle_type in [
            RequestHandleType.APPROVE,
            RequestHandleType.REFUSED,
        ]:
            if req.bot_id and req.bot_id != bot.self_id:
                with contextlib.suppress(KeyError, ValueError):
                    bot = nonebot.get_bot(req.bot_id)
            approve = handle_type == RequestHandleType.APPROVE
            if req.request_type == RequestType.FRIEND:
                await bot.set_friend_add_request(flag=req.flag, approve=approve)
            else:
                if approve:
                    await GroupConsole.get_or_create_root_group(
                        group_id=req.group_id,
                        defaults={"group_flag": 1},
                        update_defaults=True,
                    )
                if req.flag == "0":
                    if approve:
                        # 用户手动申请入群，创建群认证后提醒用户拉群
                        await bot.send_private_msg(
                            user_id=req.user_id,
                            message=f"已同意你对{BotConfig.self_nickname}的申请群组："
                            f"{req.group_id}，可以直接手动拉入群组，{BotConfig.self_nickname}会自动同意。",
                        )
                else:
                    await bot.set_group_add_request(
                        flag=req.flag, sub_type="invite", approve=approve
                    )
        req.handle_type = handle_type
        await req.save(update_fields=["handle_type"])
        if handle_type == RequestHandleType.REFUSED:
            notice_cache.set(req.notice_key(), time.time(), REFUSE_NOTICE_DELAY)
            req.blacklisted = await cls._blacklist_repeated(req)
        if (
            bot
            and handle_type == RequestHandleType.APPROVE
            and req.request_type == RequestType.FRIEND
        ):
            await cls._send_bot_profile(bot, req.user_id)
        return req

    @classmethod
    async def _latest_pending(cls, req: Self) -> Self | None:
        """查找替代该请求的最新未处理请求

        参数:
            req: 已过期的请求

        返回:
            Self | None: 同一 Bot、用户与群组下更新的未处理请求
        """
        return (
            await cls.filter(
                request_type=req.request_type,
                bot_id=req.bot_id,
                user_id=req.user_id,
                group_id=req.group_id,
                handle_type__isnull=True,
                id__gt=req.id,
            )
            .order_by("-id")
            .first()
        )

    @classmethod
    async def _blacklist_repeated(cls, req: Self) -> bool:
        """好友请求按用户、群组邀请按群组累计拒绝次数，达到上限时加入永久黑名单

        参数:
            req: 刚被拒绝的请求

        返回:
            bool: 是否加入永久黑名单
        """
        friend = req.request_type == RequestType.FRIEND
        user_id, group_id = (req.user_id, None) if friend else (None, req.group_id)
        try:
            count = await cls.filter(
                request_type=req.request_type,
                handle_type=RequestHandleType.REFUSED,
                **({"user_id": user_id} if friend else {"group_id": group_id}),
            ).count()
            if count < REFUSE_BLACKLIST_COUNT:
                return False
            await BanConsole.ban(
                user_id,
                group_id,
                9999,
                f"{'好友请求' if friend else '入群邀请'}累计被拒绝 {count} 次",
                -1,
            )
        except Exception as e:
            logger.warning("拒绝请求后加入永久黑名单失败", "请求处理", e=e)
            return False
        logger.info(
            f"{'用户' if friend else '群组'} {user_id or group_id} 已累计被拒绝"
            f" {count} 次，加入永久黑名单",
            "请求处理",
        )
        return True

    @classmethod
    async def _send_bot_profile(cls, bot: Bot, user_id: str):
        """同意好友后按配置发送 Bot 自我介绍

        参数:
            bot: Bot
            user_id: 用户id
        """
        if not BotProfileManager.is_auto_send_profile():
            return
        try:
            if file_path := await BotProfileManager.build_bot_profile_image(
                bot.self_id
            ):
                await asyncio.sleep(1)
                await PlatformUtils.send_message(
                    bot,
                    user_id,
                    None,
                    MessageUtils.build_message(
                        [
                            f"你好，我是{BotConfig.self_nickname}， "
                            "初次见面，希望我们可以好好相处！",
                            file_path,
                        ]
                    ),
                )
                logger.info("添加好友自动发送BOT自我介绍图片", session=user_id)
        except Exception as e:
            logger.warning("发送BOT自我介绍失败", session=user_id, e=e)

    @classmethod
    async def _run_script(cls):
        return []
