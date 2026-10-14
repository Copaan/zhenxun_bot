import asyncio
import contextlib
from typing import ClassVar
from typing_extensions import Self

import nonebot
from nonebot.adapters import Bot
from tortoise import fields

from zhenxun.configs.config import BotConfig
from zhenxun.models.group_console import GroupConsole
from zhenxun.services.db_context import Model
from zhenxun.services.log import logger
from zhenxun.utils.enum import RequestHandleType, RequestType
from zhenxun.utils.exception import NotFoundError, RequestHandledError
from zhenxun.utils.manager.bot_profile_manager import BotProfileManager
from zhenxun.utils.message import MessageUtils
from zhenxun.utils.platform import PlatformUtils


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

    class Meta:  # pyright: ignore [reportIncompatibleVariableOverride]
        table = "fg_request"
        table_description = "好友群组请求"
        indexes: ClassVar = [("handle_type", "request_type"), ("bot_id", "handle_type")]

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

        参数:
            bot: Bot，优先使用请求记录中的 bot，未连接时使用传入的 bot
            id: 请求id
            handle_type: 处理类型

        异常:
            NotFoundError: 未发现请求
            RequestHandledError: 请求已被处理
        """
        req = await cls.get_or_none(id=id)
        if not req:
            raise NotFoundError
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
        if (
            bot
            and handle_type == RequestHandleType.APPROVE
            and req.request_type == RequestType.FRIEND
        ):
            await cls._send_bot_profile(bot, req.user_id)
        return req

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
