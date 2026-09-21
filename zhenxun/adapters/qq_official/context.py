from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from contextvars import ContextVar, Token
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from functools import wraps
import hashlib
from typing import Literal
from uuid import UUID

from nonebot.adapters import Event
from nonebot.adapters.qq.event import (
    C2CMessageCreateEvent,
    DirectMessageCreateEvent,
    FriendRobotEvent,
    GroupMemberEvent,
    GroupMessageCreateEvent,
    GroupRobotEvent,
    GuildMessageEvent,
    InteractionCreateEvent,
)
from tortoise.exceptions import IntegrityError

from zhenxun.services.message_execution import MessageExecutionDeferred
from zhenxun.services.platform_identity import CURRENT_PLATFORM_SCOPE

from .cache import (
    IDENTITY_CACHE,
    RECEIPT_CACHE,
    REPLY_STATE_CACHE,
    ReplyState,
    record_database_fallback,
)
from .models import QQOfficialIdentity, QQOfficialPrincipal, QQWebhookReceipt

OfficialScene = Literal["c2c", "group", "guild"]
_reply_state_creation_lock = asyncio.Lock()


def digest_identifier(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


@dataclass(frozen=True, slots=True)
class OfficialQQEventContext:
    app_id: str
    scene: OfficialScene
    actor_openid: str
    group_openid: str
    union_openid: str
    source_kind: Literal["msg_id", "event_id"]
    source_id: str
    principal_id: UUID
    storage_bot_id: str
    storage_user_id: str
    storage_group_id: str | None
    received_at: datetime
    reply_deadline: datetime
    max_passive_replies: int
    guild_id: str = ""
    channel_id: str = ""
    guild_direct: bool = False
    interaction_id: str = ""
    message_id: str = ""
    guild_event: GuildMessageEvent | None = field(
        default=None, repr=False, compare=False
    )
    reply_event: Event | None = field(default=None, repr=False, compare=False)

    @property
    def reply_address(self) -> dict[str, str]:
        if self.scene == "guild":
            raise ValueError("Guild replies must use the adapter's channel/DM API")
        address = {
            "scene": self.scene,
            "openid": (self.actor_openid if self.scene == "c2c" else self.group_openid),
            self.source_kind: self.source_id,
        }
        return address


CURRENT_OFFICIAL_CONTEXT: ContextVar[OfficialQQEventContext | None] = ContextVar(
    "zhenxun_qq_official_context", default=None
)
_OFFICIAL_CONTEXT_TOKEN: ContextVar[Token | None] = ContextVar(
    "zhenxun_qq_official_context_token", default=None
)
_PLATFORM_SCOPE_TOKEN: ContextVar[Token | None] = ContextVar(
    "zhenxun_qq_platform_scope_token", default=None
)


def get_current_official_context() -> OfficialQQEventContext | None:
    return CURRENT_OFFICIAL_CONTEXT.get()


def event_official_context(event: Event) -> OfficialQQEventContext | None:
    value = getattr(event, "_zhenxun_official_context", None)
    return value if isinstance(value, OfficialQQEventContext) else None


def bind_event_official_context(event: Event, context: OfficialQQEventContext) -> None:
    object.__setattr__(event, "_zhenxun_official_context", context)


def activate_event_official_context(event: Event) -> OfficialQQEventContext | None:
    context = event_official_context(event)
    if context is not None:
        _OFFICIAL_CONTEXT_TOKEN.set(CURRENT_OFFICIAL_CONTEXT.set(context))
        _PLATFORM_SCOPE_TOKEN.set(CURRENT_PLATFORM_SCOPE.set("qq_api"))
    return context


def deactivate_event_official_context() -> None:
    """Restore context values after an event finishes dispatching."""

    context_token = _OFFICIAL_CONTEXT_TOKEN.get()
    if context_token is not None:
        CURRENT_OFFICIAL_CONTEXT.reset(context_token)
        _OFFICIAL_CONTEXT_TOKEN.set(None)
    scope_token = _PLATFORM_SCOPE_TOKEN.get()
    if scope_token is not None:
        CURRENT_PLATFORM_SCOPE.reset(scope_token)
        _PLATFORM_SCOPE_TOKEN.set(None)


def with_official_event_context(
    function: Callable[..., Awaitable[object]],
) -> Callable[..., Awaitable[object]]:
    """Keep the official context in the parent event task.

    NoneBot runs event preprocessors in sibling tasks.  A ContextVar set by
    one of those preprocessors therefore cannot reach matchers.  Binding the
    scope around the parent handler also gives the fallback native handler the
    same behavior.
    """

    @wraps(function)
    async def wrapped(*args, **kwargs):
        from zhenxun.services.business_identity import business_event_scope

        event = kwargs.get("event")
        if event is None and len(args) > 1:
            event = args[1]
        context = event_official_context(event) if event is not None else None
        context_token = CURRENT_OFFICIAL_CONTEXT.set(context)
        bot = kwargs.get("bot") or (args[0] if args else None)
        from zhenxun.utils.platform import PlatformUtils

        scope_token = CURRENT_PLATFORM_SCOPE.set(PlatformUtils.get_platform_scope(bot))
        try:
            with business_event_scope():
                return await function(*args, **kwargs)
        finally:
            CURRENT_OFFICIAL_CONTEXT.reset(context_token)
            CURRENT_PLATFORM_SCOPE.reset(scope_token)

    return wrapped


def _event_address(event: Event) -> tuple[OfficialScene, str, str] | None:
    if isinstance(event, InteractionCreateEvent):
        if event.type != 11:
            return None
        if event.chat_type == 1:
            return "group", str(event.group_member_openid), str(event.group_openid)
        if event.chat_type == 2:
            return "c2c", str(event.user_openid), ""
        return "guild", str(event.data.resolved.user_id), ""
    if isinstance(event, C2CMessageCreateEvent):
        return "c2c", str(event.author.user_openid), ""
    if isinstance(event, GroupMessageCreateEvent):
        return "group", str(event.author.member_openid), str(event.group_openid)
    if isinstance(event, GroupMemberEvent):
        return "group", str(event.member_openid), str(event.group_openid)
    if isinstance(event, GuildMessageEvent):
        return "guild", str(event.author.id or ""), ""
    if isinstance(event, FriendRobotEvent):
        return "c2c", str(event.openid), ""
    if isinstance(event, GroupRobotEvent):
        actor = str(
            getattr(event, "op_member_openid", "")
            or getattr(event, "openid", "")
            or getattr(event, "user_openid", "")
        )
        return "group", actor, str(event.group_openid)
    return None


def validate_button_interaction(app_id: str, event: Event) -> None:
    """Validate addresses before either identity creation or transport receipts."""
    if not isinstance(event, InteractionCreateEvent) or event.type != 11:
        return
    if str(event.application_id) != str(app_id):
        raise ValueError("interaction application_id mismatch")
    if not event.id or not event.event_id:
        raise ValueError("interaction ID or reply event ID missing")
    if event.chat_type == 1:
        required = (event.group_openid, event.group_member_openid)
        forbidden = (event.guild_id, event.channel_id, event.user_openid)
    elif event.chat_type == 2:
        required = (event.user_openid,)
        forbidden = (
            event.group_openid,
            event.group_member_openid,
            event.guild_id,
            event.channel_id,
        )
    elif event.chat_type == 0:
        required = (event.guild_id, event.channel_id, event.data.resolved.user_id)
        forbidden = (event.group_openid, event.group_member_openid, event.user_openid)
    else:
        raise ValueError("unsupported button chat_type")
    if any(not str(value or "").strip() for value in required) or any(forbidden):
        raise ValueError("interaction actor or scene address invalid")


def validate_official_event(app_id: str, event: Event) -> None:
    validate_button_interaction(app_id, event)
    if isinstance(event, GroupMemberEvent) and (
        not event.member_openid.strip() or not event.group_openid.strip()
    ):
        raise ValueError("group member event address missing")


def _union_openid(event: Event) -> str:
    author = getattr(event, "author", None)
    return str(
        getattr(author, "union_openid", "") or getattr(event, "union_openid", "") or ""
    )


async def _resolve_principal(
    *,
    app_id: str,
    scene: OfficialScene,
    group_scope: str,
    openid: str,
    union_openid: str,
) -> UUID:
    openid_digest = digest_identifier(openid)
    key = (app_id, scene, group_scope, openid_digest)
    if cached := await IDENTITY_CACHE.get(key):
        return cached

    record_database_fallback()
    identity = await QQOfficialIdentity.get_or_none(
        app_id=app_id,
        scene=scene,
        group_scope=group_scope,
        openid_digest=openid_digest,
    )
    if identity is not None:
        principal_id = UUID(str(identity.principal_id))
        await IDENTITY_CACHE.set(key, principal_id)
        return principal_id

    principal = await QQOfficialPrincipal.create()
    try:
        identity = await QQOfficialIdentity.create(
            principal=principal,
            app_id=app_id,
            scene=scene,
            group_scope=group_scope,
            openid_digest=openid_digest,
            union_openid_digest=(
                digest_identifier(union_openid) if union_openid else ""
            ),
        )
    except IntegrityError:
        await principal.delete()
        identity = await QQOfficialIdentity.get(
            app_id=app_id,
            scene=scene,
            group_scope=group_scope,
            openid_digest=openid_digest,
        )
    principal_id = UUID(str(identity.principal_id))
    await IDENTITY_CACHE.set(key, principal_id)
    return principal_id


async def prepare_event_context(
    app_id: str, event: Event, *, received_at: datetime | None = None
) -> OfficialQQEventContext | None:
    validate_official_event(app_id, event)
    if isinstance(event, GroupMemberEvent):
        from zhenxun.services.uninfo_patch import invalidate_qq_member_cache

        invalidate_qq_member_cache(app_id, event.group_openid, event.member_openid)
    address = _event_address(event)
    if address is None:
        return None
    scene, actor_openid, group_openid = address
    if not actor_openid:
        return None
    union_openid = _union_openid(event)
    principal_id = await _resolve_principal(
        app_id=app_id,
        scene=scene,
        group_scope=group_openid,
        openid=actor_openid,
        union_openid=union_openid,
    )
    source_id = str(getattr(event, "id", "") or getattr(event, "event_id", ""))
    source_kind: Literal["msg_id", "event_id"] = (
        "msg_id" if getattr(event, "id", None) else "event_id"
    )
    interaction = isinstance(event, InteractionCreateEvent)
    if interaction:
        source_id, source_kind = str(event.event_id), "event_id"
    received_at = received_at or datetime.now(timezone.utc)
    window = timedelta(minutes=60 if scene == "c2c" else 5)
    context = OfficialQQEventContext(
        app_id=app_id,
        scene=scene,
        actor_openid=actor_openid,
        group_openid=group_openid,
        union_openid=union_openid,
        source_kind=source_kind,
        source_id=source_id,
        principal_id=principal_id,
        storage_bot_id=f"qq_api:{app_id}",
        storage_user_id=f"principal:{principal_id}",
        storage_group_id=(
            f"qq_api:{app_id}:guild:{event.guild_id}"
            if scene == "guild"
            else f"qq_api:{app_id}:group:{group_openid}"
            if group_openid
            else None
        ),
        received_at=received_at,
        reply_deadline=received_at + window,
        max_passive_replies=0 if interaction else 4 if scene == "c2c" else 5,
        interaction_id=str(event.id) if interaction else "",
        message_id=str(event.data.resolved.message_id or "") if interaction else "",
        guild_id=str(getattr(event, "guild_id", "") or ""),
        channel_id=str(getattr(event, "channel_id", "") or ""),
        guild_direct=isinstance(event, DirectMessageCreateEvent),
        guild_event=event if isinstance(event, GuildMessageEvent) else None,
        reply_event=event if interaction else None,
    )
    bind_event_official_context(event, context)
    return context


async def receipt_seen(app_id: str, event_digest: str) -> bool:
    return bool(await RECEIPT_CACHE.get((app_id, event_digest)))


async def reserve_webhook_receipt(
    *, app_id: str, event_digest: str, event_type: str
) -> bool:
    key = (app_id, event_digest)
    if await RECEIPT_CACHE.get(key):
        return False
    try:
        await QQWebhookReceipt.create(
            app_id=app_id,
            event_id_digest=event_digest,
            event_type=event_type,
            expires_at=datetime.now(timezone.utc) + timedelta(hours=24),
        )
    except IntegrityError:
        await RECEIPT_CACHE.set(key, True)
        return False
    await RECEIPT_CACHE.set(key, True)
    return True


async def release_webhook_receipt(app_id: str, event_digest: str) -> None:
    """Undo an unacknowledged reservation so the platform can safely retry."""
    await QQWebhookReceipt.filter(app_id=app_id, event_id_digest=event_digest).delete()
    await RECEIPT_CACHE.delete((app_id, event_digest))


class OfficialReplyUnavailable(RuntimeError):
    pass


class OfficialReplyExpired(OfficialReplyUnavailable, MessageExecutionDeferred):
    """The passive reply capability expired; no protocol request was sent."""


async def allocate_reply_sequence(
    context: OfficialQQEventContext,
) -> tuple[ReplyState, int]:
    if context.interaction_id:
        raise OfficialReplyUnavailable("Button replies use the adapter's native API")
    if context.scene == "guild":
        raise OfficialReplyUnavailable("Guild replies use channel/DM APIs")
    now = datetime.now(timezone.utc)
    if now >= context.reply_deadline:
        raise OfficialReplyExpired("QQ passive reply window expired")
    key = (
        context.app_id,
        context.scene,
        digest_identifier(context.source_id),
    )
    state = await REPLY_STATE_CACHE.get(key)
    if state is None:
        async with _reply_state_creation_lock:
            state = await REPLY_STATE_CACHE.get(key)
            if state is None:
                state = ReplyState(
                    deadline=context.reply_deadline,
                    max_successful=context.max_passive_replies,
                )
                await REPLY_STATE_CACHE.set(key, state)
    async with state.lock:
        if datetime.now(timezone.utc) >= state.deadline:
            raise OfficialReplyExpired("QQ passive reply window expired")
        if state.successful + state.in_flight >= state.max_successful:
            raise OfficialReplyUnavailable("QQ passive reply limit reached")
        sequence = state.next_sequence
        state.next_sequence += 1
        state.in_flight += 1
    return state, sequence


async def finish_reply(state: ReplyState, *, successful: bool) -> None:
    async with state.lock:
        state.in_flight = max(0, state.in_flight - 1)
        if successful:
            state.successful += 1


async def invalidate_principal(principal_id: UUID) -> None:
    identities = await QQOfficialIdentity.filter(principal_id=principal_id).values(
        "app_id", "scene", "group_scope", "openid_digest"
    )
    await asyncio.gather(
        *(
            IDENTITY_CACHE.delete(
                (
                    str(item["app_id"]),
                    str(item["scene"]),
                    str(item["group_scope"]),
                    str(item["openid_digest"]),
                )
            )
            for item in identities
        )
    )


__all__ = [
    "OfficialQQEventContext",
    "OfficialReplyExpired",
    "OfficialReplyUnavailable",
    "activate_event_official_context",
    "allocate_reply_sequence",
    "bind_event_official_context",
    "deactivate_event_official_context",
    "digest_identifier",
    "event_official_context",
    "finish_reply",
    "get_current_official_context",
    "invalidate_principal",
    "prepare_event_context",
    "receipt_seen",
    "release_webhook_receipt",
    "reserve_webhook_receipt",
    "with_official_event_context",
]
