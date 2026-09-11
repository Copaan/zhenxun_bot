"""History-only recovery never invokes a business matcher."""

from zhenxun.configs.config import Config
from zhenxun.models.chat_history import ChatHistory
from zhenxun.services.low_priority_writer import append_low_priority_record


async def record_expired_message(bot, event):
    if not Config.get_config("chat_history", "FLAG"):
        return
    # The current history plugin declares qq_client support only.
    if bot.adapter.get_name() != "OneBot V11":
        return
    message = event.get_message()
    if not message:
        return

    def clean(value):
        return str(value).replace("\x00", "") if value is not None else None

    record = ChatHistory(
        user_id=clean(event.user_id),
        group_id=clean(getattr(event, "group_id", None)),
        bot_id=clean(bot.self_id),
        platform="qq",
        text=clean(message),
        plain_text=clean(message.extract_plain_text()),
    )
    await append_low_priority_record("chat_history", record)
