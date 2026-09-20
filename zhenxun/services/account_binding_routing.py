"""Cheap, shared binding routing; never resolves accounts or reads the DB."""

import re

from nonebot.adapters import Bot, Event
from nonebot.adapters.onebot.v11 import Bot as OneBot
from nonebot.adapters.onebot.v11 import MessageEvent as OneBotMessage

_COMMAND = re.compile(
    r"^\s*(绑定\s*qq|qq\s*绑定|绑定\s*状态|解绑\s*qq)(?:\s+(.*))?\s*$",
    re.IGNORECASE | re.DOTALL,
)
_CODE = re.compile(r"ZX-[23456789ABCDEFGHJKLMNPQRSTUVWXYZ]{12}", re.IGNORECASE)


def binding_command(event: Event) -> tuple[str, str] | None:
    if event.get_type() != "message":
        return None
    match = _COMMAND.fullmatch(event.get_plaintext())
    if match is None:
        return None
    head = re.sub(r"\s", "", match[1]).upper()
    kind = "status" if head == "绑定状态" else "unbind" if head == "解绑QQ" else "bind"
    return kind, (match[2] or "").strip()


def bare_binding_code(bot: Bot, event: Event) -> str | None:
    if not isinstance(bot, OneBot) or not isinstance(event, OneBotMessage):
        return None
    if (
        str(event.self_id) != str(bot.self_id)
        or event.user_id <= 0
        or str(event.user_id) == str(bot.self_id)
        or getattr(event, "anonymous", None) is not None
        or event.reply is not None
    ):
        return None
    text = []
    mentions = 0
    for segment in event.original_message:
        if segment.type == "text":
            text.append(str(segment.data.get("text", "")))
        elif (
            segment.type == "at"
            and event.message_type == "group"
            and str(segment.data.get("qq")) == str(bot.self_id)
        ):
            mentions += 1
            text.append(" ")
        else:
            return None
    if event.message_type == "group" and mentions != 1:
        return None
    code = "".join(text).strip()
    return code.upper() if _CODE.fullmatch(code) else None


def binding_argument(bot: Bot, event: Event) -> str | None:
    command = binding_command(event)
    if command is not None and command[0] == "bind":
        return command[1]
    return bare_binding_code(bot, event)
