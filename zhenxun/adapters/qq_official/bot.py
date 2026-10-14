from __future__ import annotations

from contextlib import suppress
from typing import Any
from typing_extensions import override

from nonebot.adapters.qq.bot import Bot as QQBot
from nonebot.adapters.qq.event import (
    C2CMessageCreateEvent,
    Event,
    GroupMessageCreateEvent,
    InteractionCreateEvent,
)
from nonebot.adapters.qq.message import Message, MessageSegment

from zhenxun.services.log import logger

from .context import (
    ANSWERABLE_INTERACTION_TYPES,
    OfficialQQEventContext,
    event_official_context,
    finish_reply,
    reserve_passive_reply,
    with_official_event_context,
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


class ZhenxunQQBot(QQBot):
    @override
    async def send(
        self,
        event: Event,
        message: str | Message | MessageSegment,
        **kwargs: Any,
    ) -> Any:
        context = event_official_context(event)
        if context is None or not isinstance(
            event, C2CMessageCreateEvent | GroupMessageCreateEvent
        ):
            return await super().send(event, message, **kwargs)

        reserved = await reserve_passive_reply(context)
        if reserved is None:
            return await self._send_to_scene(context, message, **kwargs)
        state, sequence = reserved
        successful = False
        try:
            result = await self._send_to_scene(
                context,
                message,
                msg_id=context.source_id,
                msg_seq=sequence,
                msg_ref_id=_message_reference_index(event),
                **kwargs,
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
        """发送到事件所在的单聊或群聊，不带 msg_id 时为主动消息

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
        result = await super().call_api(api, **data)
        interaction_id = str(data.get("interaction_id") or "")
        if api == "put_interaction" and interaction_id in _pending_interactions:
            _pending_interactions[interaction_id] = True
        return result

    @with_official_event_context
    async def handle_event(self, event: Event) -> None:
        """处理事件，按钮与快捷菜单互动未被插件回应时在处理结束后回应成功

        参数:
            event: Event
        """
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


__all__ = ["ZhenxunQQBot"]
