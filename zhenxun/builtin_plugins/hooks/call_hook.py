from collections.abc import Mapping
from typing import Any

from nonebot.adapters import Bot, Message

from zhenxun.services.log import logger
from zhenxun.utils.log_sanitizer import sanitize_for_logging
from zhenxun.utils.manager.message_manager import MessageManager
from zhenxun.utils.platform import PlatformUtils

LOG_COMMAND = "MessageHook"


@Bot.on_called_api
async def handle_api_result(
    bot: Bot, exception: Exception | None, api: str, data: dict[str, Any], result: Any
):
    if (
        exception
        or api != "send_msg"
        or PlatformUtils.get_platform_scope(bot) != "qq_client"
    ):
        return
    user_id = data.get("user_id")
    message_id = result.get("message_id") if isinstance(result, Mapping) else None
    message: Message = data.get("message", "")
    try:
        if user_id and message_id:
            MessageManager.add(str(user_id), str(message_id))
            if logger.is_enabled("debug"):
                sanitized_message = sanitize_for_logging(
                    message, context="nonebot_message"
                )
                logger.debug(
                    f"消息发送记录，user_id: {user_id}, msg_id: {message_id}, "
                    f"message: {sanitized_message}",
                    LOG_COMMAND,
                )
    except Exception as e:
        logger.warning(
            f"收集消息id发生错误...data: {data}, result: {result}", LOG_COMMAND, e=e
        )
