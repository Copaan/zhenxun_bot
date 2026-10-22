from __future__ import annotations

from contextlib import suppress
from datetime import datetime, timezone
from typing import Any
from typing_extensions import override

from nonebot.adapters.qq.bot import Bot as QQBot
from nonebot.adapters.qq.event import (
    C2CMessageCreateEvent,
    Event,
    GroupMessageCreateEvent,
    InteractionCreateEvent,
)
from nonebot.adapters.qq.exception import ActionFailed
from nonebot.adapters.qq.message import Message, MessageSegment
from nonebot_plugin_alconna import Target, UniMessage
from nonebot_plugin_uninfo import SupportScope

from zhenxun.services.log import logger
from zhenxun.services.platform_identity import native_id

from .context import (
    ANSWERABLE_INTERACTION_TYPES,
    OfficialQQEventContext,
    OfficialReplyExpired,
    OfficialReplyUnavailable,
    _proactive_fallback_enabled,
    event_official_context,
    finish_reply,
    get_current_official_context,
    reserve_passive_reply,
    with_official_event_context,
)
from .proactive import (
    DENIED_CODES,
    ProactiveMessageDenied,
    acquire_proactive_slot,
    proactive_allowed,
    record_switch_event,
    set_proactive_allowed,
)

# 处理中的互动事件 ID -> 是否已回应
_pending_interactions: dict[str, bool] = {}
_MESSAGE_APIS = frozenset({"post_c2c_messages", "post_group_messages"})


def _message_reference_index(event: Event) -> str | None:
    scene = getattr(event, "message_scene", None)
    if not scene:
        return None
    return next(
        (
            extension.partition("=")[-1]
            for extension in scene.ext
            if extension.startswith("msg_idx=")
        ),
        None,
    )


def _proactive_address(api: str, data: dict[str, Any]) -> tuple[str, str] | None:
    """判断发送请求是否为主动消息

    参数:
        api: 接口名
        data: 接口参数，已去掉空的 msg_id

    返回:
        tuple[str, str] | None: 主动消息的场景与群或用户 openid，非主动消息为 None
    """
    if api not in _MESSAGE_APIS or data.get("msg_id") or data.get("event_id"):
        return None
    if api == "post_group_messages":
        return "group", str(data.get("group_openid") or "")
    return "c2c", str(data.get("openid") or "")


class ZhenxunQQBot(QQBot):
    @override
    async def send(
        self,
        event: Event,
        message: str | Message | MessageSegment,
        **kwargs: Any,
    ) -> Any:
        context = event_official_context(event)
        if (
            context is not None
            and context.source_kind == "event_id"
            and not context.interaction_id
            and context.scene != "guild"
        ):
            if datetime.now(timezone.utc) < context.reply_deadline:
                return await self._send_to_scene(
                    context, message, event_id=context.source_id
                )
            if not _proactive_fallback_enabled():
                raise OfficialReplyExpired("QQ passive reply window expired")
            return await self._send_to_scene(context, message)
        if context is None or not isinstance(
            event, C2CMessageCreateEvent | GroupMessageCreateEvent
        ):
            return await super().send(event, message, **kwargs)

        # 与上游一致，忽略 reply_message、at_sender 等通用发送参数
        reserved = await reserve_passive_reply(context)
        if reserved is None:
            return await self._send_to_scene(context, message)
        state, sequence = reserved
        successful = False
        try:
            result = await self._send_to_scene(
                context,
                message,
                msg_id=context.source_id,
                msg_seq=sequence,
                msg_ref_id=_message_reference_index(event),
            )
            successful = True
            return result
        finally:
            with suppress(Exception):
                await finish_reply(state, successful=successful)

    async def _send_to_scene(
        self,
        context: OfficialQQEventContext,
        message: str | Message | MessageSegment,
        **kwargs: Any,
    ) -> Any:
        """发送到事件所在的单聊或群聊，不带 msg_id 与 event_id 时为主动消息

        参数:
            context: 官方事件上下文
            message: 消息
            kwargs: 透传给发送接口的参数

        返回:
            Any: 发送结果
        """
        if context.scene == "c2c":
            return await self.send_to_c2c(
                openid=context.actor_openid, message=message, **kwargs
            )
        return await self.send_to_group(
            group_openid=context.group_openid, message=message, **kwargs
        )

    @override
    async def call_api(self, api: str, **data: Any) -> Any:
        if api in _MESSAGE_APIS and not data.get("msg_id"):
            # 主动消息不带 msg_id 与 msg_seq，空字符串也按未提供处理
            data.pop("msg_id", None)
            data.pop("msg_seq", None)
        address = _proactive_address(api, data)
        if address is not None:
            await self._before_proactive(*address)
        try:
            result = await super().call_api(api, **data)
        except ActionFailed as exc:
            if address is not None and exc.code in DENIED_CODES:
                await self._record_proactive(*address, allowed=False)
            raise
        if address is not None:
            await self._record_proactive(*address, allowed=True)
        interaction_id = str(data.get("interaction_id") or "")
        if api == "put_interaction" and interaction_id in _pending_interactions:
            _pending_interactions[interaction_id] = True
        return result

    async def _before_proactive(self, scene: str, target: str) -> None:
        """主动消息发送前检查许可并等待频率限制

        参数:
            scene: group 或 c2c
            target: 群或用户 openid

        异常:
            ProactiveMessageDenied: 群未开启或用户关闭了主动消息
        """
        try:
            allowed = await proactive_allowed(self.self_id, scene, target)
        except Exception as exc:
            logger.warning(
                "QQ 主动消息许可查询失败", "QQOfficial", target=self.self_id, e=exc
            )
            allowed = None
        if allowed is False:
            raise ProactiveMessageDenied(
                "该群未开启机器人主动发言，需要群主在机器人资料页开启"
                if scene == "group"
                else "该用户已关闭机器人主动消息"
            )
        await acquire_proactive_slot(self.self_id, scene, target)

    async def _record_proactive(self, scene: str, target: str, allowed: bool) -> None:
        try:
            await set_proactive_allowed(self.self_id, scene, target, allowed)
        except Exception as exc:
            logger.warning(
                "QQ 主动消息许可记录失败", "QQOfficial", target=self.self_id, e=exc
            )

    @with_official_event_context
    async def handle_event(self, event: Event) -> None:
        """处理事件，记录主动消息开关；按钮与快捷菜单互动未被插件回应时在处理结束后回应成功

        参数:
            event: Event
        """
        try:
            await record_switch_event(self.self_id, event)
        except Exception as exc:
            logger.warning(
                "QQ 主动消息开关记录失败", "QQOfficial", target=self.self_id, e=exc
            )
        if (
            not isinstance(event, InteractionCreateEvent)
            or event.type not in ANSWERABLE_INTERACTION_TYPES
        ):
            await super().handle_event(event)
            return
        _pending_interactions[event.id] = False
        try:
            await super().handle_event(event)
        finally:
            if not _pending_interactions.pop(event.id, True):
                try:
                    await self.put_interaction(interaction_id=event.id, code=0)
                except Exception as exc:
                    logger.warning(
                        "QQ 互动回应失败", "QQOfficial", target=self.self_id, e=exc
                    )


async def send_official_message(
    bot: ZhenxunQQBot,
    message: UniMessage,
    group_id: str | None,
    official_context: object | None = None,
) -> Any:
    """通过 QQ 官方 Bot 发送消息

    指定的群与事件所在群不同或不在事件中时，向该群发送主动消息；
    未指定群时回复到事件所在会话，被动回复的序号、期限与主动消息兜底由
    ZhenxunQQBot.send 处理。

    参数:
        bot: 官方 Bot
        message: 消息
        group_id: 群 openid 或存储 id
        official_context: 官方事件上下文，默认取当前事件

    返回:
        Any: 发送回执

    异常:
        OfficialReplyUnavailable: 没有可回复的事件且未指定群，或群 id 不属于该 Bot
    """
    app_id = str(bot.self_id)
    group_openid = None
    if group_id:
        group_openid = native_id(group_id, "qq_api", group=True, bot_id=app_id)
        if group_openid is None:
            raise OfficialReplyUnavailable("群 id 不属于当前 QQ 官方 Bot")
    context = official_context or get_current_official_context()
    if isinstance(context, OfficialQQEventContext) and group_openid in (
        None,
        context.group_openid,
    ):
        if context.app_id == app_id:
            if context.reply_event is None:
                raise OfficialReplyUnavailable("QQ 官方事件上下文缺少回复事件")
            return await message.send(target=context.reply_event, bot=bot)
        if group_openid is None:
            raise OfficialReplyUnavailable("事件上下文不属于当前 QQ 官方 Bot")
    if group_openid is None:
        raise OfficialReplyUnavailable(
            "QQ 官方 Bot 不在事件中时只能向群发送主动消息，不能主动私聊"
        )
    target = Target(
        id=group_openid, self_id=app_id, adapter="QQ", scope=SupportScope.qq_api
    )
    return await message.send(target=target, bot=bot)


__all__ = ["ProactiveMessageDenied", "ZhenxunQQBot", "send_official_message"]
