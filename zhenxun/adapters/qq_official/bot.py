from __future__ import annotations

from contextlib import suppress
from typing import Any
from typing_extensions import override

from nonebot.adapters.qq.bot import Bot as QQBot
from nonebot.adapters.qq.event import (
    C2CMessageCreateEvent,
    Event,
    GroupMessageCreateEvent,
)
from nonebot.adapters.qq.message import Message, MessageSegment

from .context import (
    allocate_reply_sequence,
    event_official_context,
    finish_reply,
)


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

        state, sequence = await allocate_reply_sequence(context)
        successful = False
        try:
            reference = _message_reference_index(event)
            if isinstance(event, C2CMessageCreateEvent):
                result = await self.send_to_c2c(
                    openid=context.actor_openid,
                    message=message,
                    msg_id=context.source_id,
                    msg_seq=sequence,
                    msg_ref_id=reference,
                    **kwargs,
                )
            else:
                result = await self.send_to_group(
                    group_openid=context.group_openid,
                    message=message,
                    msg_id=context.source_id,
                    msg_seq=sequence,
                    msg_ref_id=reference,
                    **kwargs,
                )
            successful = True
            return result
        finally:
            with suppress(Exception):
                await finish_reply(state, successful=successful)


__all__ = ["ZhenxunQQBot"]
