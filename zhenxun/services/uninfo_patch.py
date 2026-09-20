import asyncio
from collections.abc import Awaitable, Callable
import contextlib
from copy import deepcopy
from dataclasses import replace
import importlib
from typing import Any, cast

from nonebot.adapters import Bot, Event
from nonebot.adapters.onebot.v11.event import GroupMessageEvent
from nonebot.adapters.qq.event import (
    C2CMessageCreateEvent,
    DirectMessageCreateEvent,
    GroupMemberEvent,
    GuildMessageEvent,
    InteractionCreateEvent,
)
from nonebot.adapters.qq.event import (
    GroupMessageCreateEvent as QQGroupMessageEvent,
)
from nonebot.log import logger

_PATCHED = False
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
        "role": _sender_value(sender, "role"),
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


class _ScopedSessionCache(dict):
    """Attach bounded state to the cache already cleared by InfoFetcher.clean()."""

    def __init__(self, ttl):
        from zhenxun.services.cache.cache_containers import CacheDict

        super().__init__()
        self.values_by_scope = CacheDict("UNINFO_SCOPED_SESSIONS", expire=ttl)
        self.inflight = {}
        self.epoch = 0

    def clear(self):
        super().clear()
        self.epoch += 1
        self.values_by_scope.clear()
        for task in tuple(self.inflight.values()):
            task.cancel()
        self.inflight.clear()


async def clear_uninfo_sessions(bot: Bot | None = None) -> None:
    """Drain scoped fills at disconnect/shutdown without evicting other Bots."""
    from nonebot_plugin_uninfo.adapters import INFO_FETCHER_MAPPING

    tasks = set()
    for fetcher in set(INFO_FETCHER_MAPPING.values()):
        cache = fetcher.session_cache
        if bot is None:
            if isinstance(cache, _ScopedSessionCache):
                tasks.update(cache.inflight.values())
            fetcher.clean()
            continue
        if isinstance(cache, _ScopedSessionCache):
            adapter = type(bot.adapter)
            prefix = (
                f"{adapter.__module__}.{adapter.__qualname__}",
                str(bot.self_id),
            )
            for key in cache.values_by_scope.keys():
                if key[:2] == prefix:
                    cache.values_by_scope.pop(key, None)
            for key, task in tuple(cache.inflight.items()):
                if key[:2] == prefix:
                    cache.inflight.pop(key, None)
                    task.cancel()
                    tasks.add(task)
        if fetcher.adapter.value == bot.adapter.get_name():
            for values in (
                fetcher._user_cache,
                fetcher._scene_cache,
                fetcher._member_cache,
            ):
                values.pop(str(bot.self_id), None)
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)


def invalidate_qq_member_cache(app_id: str, group_id: str, member_id: str) -> None:
    from nonebot_plugin_uninfo.adapters.qq.main import fetcher

    cache = fetcher.session_cache
    if isinstance(cache, _ScopedSessionCache):
        for key, _ in cache.values_by_scope.items():
            if key[1] == app_id and key[-2:] == (group_id, member_id):
                cache.values_by_scope.pop(key, None)
        for key, task in tuple(cache.inflight.items()):
            if key[1] == app_id and key[-2:] == (group_id, member_id):
                cache.inflight.pop(key, None)
                task.cancel()
    fetcher._member_cache.get(app_id, {}).pop((1, group_id, member_id), None)
    # User data is shared across groups; remove only this actor's entry.
    fetcher._user_cache.get(app_id, {}).pop(member_id, None)


async def _event_session(self, bot, event):
    """Authoritative event fields must not inherit a cached member's privileges."""
    from nonebot_plugin_uninfo.model import Role

    if isinstance(event, InteractionCreateEvent) and event.type == 11:
        data = await _fast_qq_interaction(bot, event)
    elif isinstance(event, GroupMemberEvent):
        data = _qq_user_payload(bot, event.member_openid, group_id=event.group_openid)
    elif isinstance(event, QQGroupMessageEvent):
        data = await _fast_qq_group_at_message(bot, event)
    elif isinstance(event, C2CMessageCreateEvent):
        data = await _fast_qq_c2c_message(bot, event)
    elif isinstance(event, GuildMessageEvent):
        data = await _fast_qq_guild_message(bot, event)
    elif isinstance(event, GroupMessageEvent) and _has_compatible_onebot11_sender(
        event
    ):
        data = await _fast_onebot11_group_message(bot, event)
    else:
        return None
    session = self.parse({**self.supply_self(bot), **data})
    if isinstance(event, QQGroupMessageEvent) and session.member is not None:
        role = getattr(event.author, "member_role", None)
        spec = {
            "owner": ("OWNER", 100, "群主"),
            "admin": ("ADMINISTRATOR", 10, "管理员"),
            "member": ("MEMBER", 1, "成员"),
        }.get(role)
        session = replace(
            session, member=replace(session.member, roles=[Role(*spec)] if spec else [])
        )
    return session


async def _singleflight_fetch(self: Any, bot: Bot, event: Event) -> Any:
    from nonebot_plugin_uninfo.fetch import conf

    cache = self.session_cache
    if not isinstance(cache, _ScopedSessionCache):
        cache = _ScopedSessionCache(conf.uninfo_cache_expire)
        self.session_cache = cache
    local = await _event_session(self, bot, event)
    if local is not None:
        return local

    async def fetch_uncached():
        # Reuse registered suppliers and parsers without the upstream cache keyed
        # only by session ID. This also avoids stale member/role side caches.
        supplier = next(
            (self.endpoint[t] for t in type(event).__mro__ if t in self.endpoint),
            self.wildcard,
        )
        if supplier is None:
            raise NotImplementedError(f"Event {type(event)} not supported yet")
        data = await supplier(bot, event)
        return self.parse({**self.supply_self(bot), **data})

    try:
        session_id = self.get_session_id(event)
    except ValueError:
        return await fetch_uncached()
    adapter = type(bot.adapter)
    group_id = str(
        getattr(event, "group_openid", None) or getattr(event, "group_id", "")
    )
    try:
        actor = event.get_user_id()
    except ValueError:
        actor = ""
    key = (
        f"{adapter.__module__}.{adapter.__qualname__}",
        str(bot.self_id),
        type(event),
        session_id,
        group_id,
        actor,
    )
    enabled = conf.uninfo_cache and conf.uninfo_cache_expire > 0
    cache.values_by_scope.expire = conf.uninfo_cache_expire
    if enabled and (cached := cache.values_by_scope.get(key)) is not None:
        return deepcopy(cached)
    task = cache.inflight.get(key)
    if task is None:
        epoch = cache.epoch

        async def fill():
            value = await fetch_uncached()
            if (
                cache.epoch != epoch
                or cache.inflight.get(key) is not asyncio.current_task()
            ):
                raise asyncio.CancelledError("uninfo cache invalidated")
            if enabled:
                cache.values_by_scope[key] = deepcopy(value)
            return value

        task = asyncio.create_task(fill(), name="uninfo-session-fetch")
        cache.inflight[key] = task

        def completed(done):
            if cache.inflight.get(key) is done:
                cache.inflight.pop(key, None)
            if not done.cancelled():
                done.exception()

        task.add_done_callback(completed)
    return deepcopy(await asyncio.shield(task))


async def _fast_qq_interaction(
    bot: Bot, event: InteractionCreateEvent
) -> dict[str, Any]:
    from zhenxun.adapters.qq_official.context import validate_button_interaction

    validate_button_interaction(str(bot.self_id), event)
    if event.chat_type == 1:
        return _qq_user_payload(
            bot, event.group_member_openid, group_id=event.group_openid
        )
    if event.chat_type == 2:
        return _qq_user_payload(bot, event.user_openid)
    # Callbacks carry no trustworthy guild role or nickname. Do not fetch the
    # member list just to construct a session, or invent administrator roles.
    return _qq_user_payload(
        bot,
        event.data.resolved.user_id,
        avatar=None,
        guild_id=event.guild_id,
        channel_id=event.channel_id,
        guild_name="",
        guild_avatar=None,
        channel_name="",
        channel_type=0,
        roles=[],
    )


def apply_uninfo_onebot11_patch() -> None:
    global _ORIGINAL_ONEBOT11_GROUP_MESSAGE, _PATCHED
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

    setattr(_singleflight_fetch, "__zhenxun_singleflight__", True)
    setattr(InfoFetcher, "fetch", _singleflight_fetch)
    _PATCHED = True
    logger.debug("Uninfo fast fetch and singleflight patch applied")
