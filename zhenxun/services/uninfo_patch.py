import asyncio
from collections.abc import Awaitable, Callable
import contextlib
import importlib
from typing import Any, cast

from nonebot.adapters import Bot, Event
from nonebot.adapters.onebot.v11.event import GroupMessageEvent
from nonebot.adapters.qq.event import DirectMessageCreateEvent
from nonebot.log import logger

_PATCHED = False
_ORIGINAL_FETCH: Callable[..., Awaitable[Any]] | None = None
_ORIGINAL_ONEBOT11_GROUP_MESSAGE: Callable[..., Awaitable[dict[str, Any]]] | None = None
_ORIGINAL_QQ_C2C_MESSAGE: Callable[..., Awaitable[dict[str, Any]]] | None = None
_ORIGINAL_QQ_GROUP_AT_MESSAGE: Callable[..., Awaitable[dict[str, Any]]] | None = None
_ORIGINAL_QQ_GUILD_MESSAGE: Callable[..., Awaitable[dict[str, Any]]] | None = None

_QQ_ROLE_INFO = {
    "4": ("OWNER", 100, "创建者"),
    "2": ("ADMINISTRATOR", 10, "管理员"),
    "5": ("CHANNEL_ADMINISTRATOR", 8, "子频道管理员"),
    "1": ("MEMBER", 1, "成员"),
}


def _sender_value(sender: Any, key: str, default: Any = None) -> Any:
    value = getattr(sender, key, default)
    return default if value is None else value


def _event_value(event: Event, key: str, default: Any = None) -> Any:
    value = getattr(event, key, default)
    return default if value is None else value


def _event_group_name(event: Event) -> str | None:
    group_name = _event_value(event, "group_name")
    if isinstance(group_name, str) and group_name:
        return group_name
    group = _event_value(event, "group")
    if group is not None:
        name = _sender_value(group, "name") or _sender_value(group, "group_name")
        if isinstance(name, str) and name:
            return name
    return None


def _has_compatible_onebot11_sender(event: Event) -> bool:
    if getattr(event, "_zx_uninfo_full_fetch", False):
        return False
    sender = _event_value(event, "sender")
    if sender is None:
        return False
    return (
        _event_value(event, "user_id") is not None
        and _event_value(event, "group_id") is not None
        and _sender_value(sender, "nickname") is not None
        and _sender_value(sender, "role") is not None
    )


async def _fast_onebot11_group_message(bot: Bot, event: Event) -> dict[str, Any]:
    """Build Uninfo session data from OneBot v11 group message event fields.

    nonebot-plugin-uninfo's default OneBot v11 fetcher always calls
    get_group_info and get_group_member_info for group messages. For normal
    matcher rule checks, event-provided sender fields are enough and avoid
    multiplying protocol API calls by the number of candidate matchers.
    """

    original = _ORIGINAL_ONEBOT11_GROUP_MESSAGE
    if not _has_compatible_onebot11_sender(event):
        if original is not None:
            return await original(bot, event)
        logger.debug("Uninfo OneBot11 fast fetch fallback unavailable")

    sender = _event_value(event, "sender")
    user_id = str(_event_value(event, "user_id", ""))
    group_id = str(_event_value(event, "group_id", ""))
    nickname = _sender_value(sender, "nickname", "")
    card = _sender_value(sender, "card", "") or nickname
    return {
        "group_id": group_id,
        "group_name": _event_group_name(event),
        "user_id": user_id,
        "name": nickname,
        "nickname": card,
        "card": card,
        "role": _sender_value(sender, "role", "member"),
        "join_time": _event_value(event, "join_time"),
        "gender": _sender_value(sender, "sex", "unknown") or "unknown",
    }


def _qq_bot_app_id(bot: Bot) -> str:
    bot_info = getattr(bot, "bot_info", None)
    app_id = getattr(bot_info, "id", None)
    return str(app_id or getattr(bot, "self_id", ""))


def _qq_user_payload(
    bot: Bot, user_id: str, name: str = "", **fields: Any
) -> dict[str, Any]:
    """Build the common part of an official QQ Uninfo payload."""

    payload: dict[str, Any] = {
        "user_id": user_id,
        "name": name,
        "nickname": name,
        "avatar": f"https://q.qlogo.cn/qqapp/{_qq_bot_app_id(bot)}/{user_id}/100",
    }
    payload.update(fields)
    return payload


async def _fast_qq_c2c_message(bot: Bot, event: Event) -> dict[str, Any]:
    """Build Uninfo session for QQ official C2C messages from event fields."""

    author = _event_value(event, "author")
    user_id = str(
        _sender_value(author, "user_openid")
        or _sender_value(author, "id")
        or _event_value(event, "user_id", "")
    )
    if not user_id:
        if _ORIGINAL_QQ_C2C_MESSAGE is not None:
            return await _ORIGINAL_QQ_C2C_MESSAGE(bot, event)
        return {}
    username = str(_sender_value(author, "username", "") or "")
    return _qq_user_payload(bot, user_id, username)


async def _fast_qq_group_at_message(bot: Bot, event: Event) -> dict[str, Any]:
    """Build Uninfo session for QQ official group-at messages from event fields."""

    author = _event_value(event, "author")
    user_id = str(
        _sender_value(author, "member_openid")
        or _sender_value(author, "id")
        or _event_value(event, "user_id", "")
    )
    username = str(_sender_value(author, "username", "") or "")
    group_id = str(
        _event_value(event, "group_openid") or _event_value(event, "group_id") or ""
    )
    if not user_id or not group_id:
        if _ORIGINAL_QQ_GROUP_AT_MESSAGE is not None:
            return await _ORIGINAL_QQ_GROUP_AT_MESSAGE(bot, event)
        return {}
    return _qq_user_payload(bot, user_id, username, group_id=group_id)


async def _fast_qq_guild_message(bot: Bot, event: Event) -> dict[str, Any]:
    """Build Uninfo session for QQ official guild/channel messages locally.

    nonebot-plugin-uninfo enriches guild messages through remote guild/channel
    APIs. Runtime auth only needs stable scene/user ids, so avoid remote calls
    during matcher fanout.
    """

    author = _event_value(event, "author")
    member = _event_value(event, "member")
    guild_id = str(_event_value(event, "guild_id", "") or "")
    channel_id = str(_event_value(event, "channel_id", "") or "")
    user_id = str(_sender_value(author, "id", "") or "")
    nickname = str(_sender_value(member, "nick", "") or "")
    username = str(_sender_value(author, "username", "") or "")
    if not user_id or not guild_id or not channel_id:
        if _ORIGINAL_QQ_GUILD_MESSAGE is not None:
            return await _ORIGINAL_QQ_GUILD_MESSAGE(bot, event)
        return {}
    base: dict[str, Any] = _qq_user_payload(
        bot,
        user_id,
        username,
        nickname=nickname or username,
        avatar=_sender_value(author, "avatar"),
        guild_id=guild_id,
        channel_id=channel_id,
        guild_name="",
        guild_avatar=None,
        channel_name="",
        channel_type=-1 if isinstance(event, DirectMessageCreateEvent) else 0,
    )
    roles = _sender_value(member, "roles")
    if roles is not None:
        from nonebot_plugin_uninfo.model import Role

        base["roles"] = [
            Role(*_QQ_ROLE_INFO.get(str(role), _QQ_ROLE_INFO["1"]))
            if isinstance(role, str)
            else role
            for role in roles
        ]
    joined_at = _sender_value(member, "joined_at")
    if joined_at is not None:
        base["joined_at"] = joined_at
    return base


async def _singleflight_fetch(self: Any, bot: Bot, event: Event) -> Any:
    original = _ORIGINAL_FETCH
    if original is None:
        return None

    try:
        sess_id = self.get_session_id(event)
    except ValueError:
        return await original(self, bot, event)

    session_cache = getattr(self, "session_cache", None)
    bot_id = str(getattr(bot, "self_id", ""))
    per_bot = getattr(self, "_zx_session_cache_by_bot", None)
    if not isinstance(per_bot, dict):
        per_bot = {}
        setattr(self, "_zx_session_cache_by_bot", per_bot)
    elif isinstance(session_cache, dict) and not session_cache and per_bot:
        # InfoFetcher.clean() clears the upstream cache.  Keep the added
        # per-bot cache lifecycle aligned with it for reconnect and reload.
        per_bot.clear()
    bot_cache = per_bot.setdefault(bot_id, {})
    if sess_id in bot_cache:
        return bot_cache[sess_id]
    # Preserve old manually populated caches only when their session belongs
    # to this bot.  A plain session id cannot otherwise distinguish bots.
    legacy = session_cache.get(sess_id) if isinstance(session_cache, dict) else None
    if legacy is not None and str(getattr(legacy, "self_id", "")) == bot_id:
        bot_cache[sess_id] = legacy
        return legacy

    inflight = getattr(self, "_zx_fetch_inflight", None)
    if not isinstance(inflight, dict):
        inflight = {}
        setattr(self, "_zx_fetch_inflight", inflight)

    key = (bot_id, event.__class__, sess_id)
    task = inflight.get(key)
    if task is None or task.done():
        lock = getattr(self, "_zx_fetch_cache_lock", None)
        if lock is None:
            lock = asyncio.Lock()
            setattr(self, "_zx_fetch_cache_lock", lock)

        async def fetch_for_bot():
            async with lock:
                old_legacy = (
                    session_cache.pop(sess_id, None)
                    if isinstance(session_cache, dict)
                    else None
                )
                try:
                    result = await original(self, bot, event)
                    bot_cache[sess_id] = result
                    return result
                finally:
                    # Do not restore a value created for another bot.  The
                    # per-bot cache above is the authoritative lookup path.
                    if (
                        old_legacy is not None
                        and str(getattr(old_legacy, "self_id", "")) == bot_id
                        and isinstance(session_cache, dict)
                    ):
                        session_cache[sess_id] = old_legacy

        task = asyncio.ensure_future(fetch_for_bot())
        inflight[key] = task
        task.add_done_callback(
            lambda completed: (
                inflight.pop(key, None) if inflight.get(key) is completed else None
            )
        )
    try:
        return await asyncio.shield(task)
    finally:
        if inflight.get(key) is task and task.done():
            inflight.pop(key, None)


def apply_uninfo_onebot11_patch() -> None:
    global _ORIGINAL_FETCH, _ORIGINAL_ONEBOT11_GROUP_MESSAGE, _PATCHED
    global _ORIGINAL_QQ_C2C_MESSAGE, _ORIGINAL_QQ_GROUP_AT_MESSAGE
    global _ORIGINAL_QQ_GUILD_MESSAGE
    if _PATCHED:
        return

    with contextlib.suppress(Exception):
        from nonebot_plugin_uninfo.adapters.onebot11.main import fetcher

        original_endpoint = fetcher.endpoint.get(GroupMessageEvent)
        if not getattr(original_endpoint, "__zhenxun_fast_onebot11__", False):
            _ORIGINAL_ONEBOT11_GROUP_MESSAGE = cast(
                Callable[..., Awaitable[dict[str, Any]]] | None,
                original_endpoint,
            )
            setattr(_fast_onebot11_group_message, "__zhenxun_fast_onebot11__", True)
            fetcher.endpoint[GroupMessageEvent] = _fast_onebot11_group_message

    with contextlib.suppress(Exception):
        qq_event_module = importlib.import_module("nonebot.adapters.qq.event")
        AtMessageCreateEvent = getattr(qq_event_module, "AtMessageCreateEvent")
        C2CMessageCreateEvent = getattr(qq_event_module, "C2CMessageCreateEvent")
        DirectMessageCreateEvent = getattr(qq_event_module, "DirectMessageCreateEvent")
        GroupAtMessageCreateEvent = getattr(
            qq_event_module,
            "GroupAtMessageCreateEvent",
        )
        GroupMessageCreateEvent = getattr(
            qq_event_module,
            "GroupMessageCreateEvent",
        )
        MessageCreateEvent = getattr(qq_event_module, "MessageCreateEvent")
        from nonebot_plugin_uninfo.adapters.qq.main import fetcher as qq_fetcher

        original_c2c = qq_fetcher.endpoint.get(C2CMessageCreateEvent)
        if not getattr(original_c2c, "__zhenxun_fast_qq__", False):
            _ORIGINAL_QQ_C2C_MESSAGE = cast(
                Callable[..., Awaitable[dict[str, Any]]] | None,
                original_c2c,
            )
            setattr(_fast_qq_c2c_message, "__zhenxun_fast_qq__", True)
            qq_fetcher.endpoint[C2CMessageCreateEvent] = _fast_qq_c2c_message

        for event_type in (GroupMessageCreateEvent, GroupAtMessageCreateEvent):
            original_group_at = qq_fetcher.endpoint.get(event_type)
            if getattr(original_group_at, "__zhenxun_fast_qq__", False):
                continue
            if _ORIGINAL_QQ_GROUP_AT_MESSAGE is None and original_group_at is not None:
                _ORIGINAL_QQ_GROUP_AT_MESSAGE = cast(
                    Callable[..., Awaitable[dict[str, Any]]],
                    original_group_at,
                )
            setattr(_fast_qq_group_at_message, "__zhenxun_fast_qq__", True)
            qq_fetcher.endpoint[event_type] = _fast_qq_group_at_message

        for event_type in (
            MessageCreateEvent,
            AtMessageCreateEvent,
            DirectMessageCreateEvent,
        ):
            original_guild = qq_fetcher.endpoint.get(event_type)
            if getattr(original_guild, "__zhenxun_fast_qq__", False):
                continue
            if _ORIGINAL_QQ_GUILD_MESSAGE is None and original_guild is not None:
                _ORIGINAL_QQ_GUILD_MESSAGE = cast(
                    Callable[..., Awaitable[dict[str, Any]]],
                    original_guild,
                )
            setattr(_fast_qq_guild_message, "__zhenxun_fast_qq__", True)
            qq_fetcher.endpoint[event_type] = _fast_qq_guild_message

    try:
        from nonebot_plugin_uninfo.fetch import InfoFetcher
    except Exception as e:
        logger.warning("Uninfo patch skipped", e=e)
        return

    original_fetch = getattr(InfoFetcher, "fetch", None)
    if getattr(original_fetch, "__zhenxun_singleflight__", False):
        _PATCHED = True
        return
    if original_fetch is None:
        return

    _ORIGINAL_FETCH = cast(Callable[..., Awaitable[Any]], original_fetch)
    setattr(_singleflight_fetch, "__zhenxun_singleflight__", True)
    setattr(InfoFetcher, "fetch", _singleflight_fetch)
    _PATCHED = True
    logger.debug("Uninfo fast fetch and singleflight patch applied")
