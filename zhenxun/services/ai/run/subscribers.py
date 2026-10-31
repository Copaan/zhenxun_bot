from typing import Any

from nonebot_plugin_alconna import UniMessage

from zhenxun.services.ai.core.exceptions import ControlFlowExit
from zhenxun.services.ai.core.messages import BaseContentPart, ImagePart, TextPart
from zhenxun.services.ai.core.stream_events import EventBus, UserCustomEvent
from zhenxun.utils.message import MessageUtils
from zhenxun.utils.platform import PlatformUtils

from .context import RunContext


async def send_to_platform(context: RunContext, display: Any) -> None:
    """把展示内容发送到运行上下文所在的会话

    参数:
        context: 运行上下文
        display: 文本、UniMessage 或文本/图片片段列表
    """
    bot = context.get_bot()
    if not bot or not display:
        return
    if isinstance(display, list) and isinstance(display[0], BaseContentPart):
        msg = UniMessage()
        for part in display:
            if isinstance(part, TextPart) and part.text:
                msg = msg.text(part.text)
            elif isinstance(part, ImagePart):
                if part.raw:
                    msg = msg.image(raw=part.raw)
                elif part.url:
                    msg = msg.image(url=part.url)
                elif part.path:
                    msg = msg.image(path=part.path)
        display = msg
    if not isinstance(display, UniMessage):
        display = MessageUtils.build_message(str(display))
    if event := context.get_event():
        await display.send(event, bot=bot)
    elif target := PlatformUtils.get_target(
        user_id=context.get_user_id(), group_id=context.get_group_id()
    ):
        await display.send(target=target, bot=bot)


async def show_exit_notice(e: ControlFlowExit, context: RunContext) -> None:
    """发送运行中止异常附带的提示语，发送失败时忽略

    参数:
        e: 中止异常
        context: 运行上下文
    """
    display = getattr(e, "display", None) or getattr(e, "display_content", None)
    if not display:
        return
    try:
        if context.get_bot():
            await send_to_platform(context, display)
        else:
            await MessageUtils.build_message(str(display)).send()
    except Exception:
        pass


class DefaultUISubscriber:
    """把工具发出的自定义展示事件发送到聊天平台"""

    def __init__(self, context: RunContext):
        """初始化

        参数:
            context: 运行上下文
        """
        self.context = context

    def attach(self, bus: EventBus):
        """订阅自定义展示事件

        参数:
            bus: 事件总线
        """
        bus.subscribe(UserCustomEvent, self.on_custom_event)

    async def on_custom_event(self, event: UserCustomEvent):
        """发送自定义展示事件的内容

        参数:
            event: 自定义展示事件
        """
        await send_to_platform(self.context, event.display)
