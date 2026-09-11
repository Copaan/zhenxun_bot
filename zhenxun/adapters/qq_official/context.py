from __future__ import annotations

import asyncio
from contextvars import ContextVar
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import hashlib
from typing import Literal
from uuid import UUID

from nonebot.adapters import Event
from nonebot.adapters.qq.event import (
    C2CMessageCreateEvent,
    FriendRobotEvent,
    GroupMessageCreateEvent,
    GroupRobotEvent,
)
from tortoise.exceptions import IntegrityError

from zhenxun.services.platform_identity import CURRENT_PLATFORM_SCOPE

from .cache import (
    IDENTITY_CACHE,
    RECEIPT_CACHE,
    REPLY_STATE_CACHE,
    ReplyState,
    record_database_fallback,
)
from .models import QQOfficialIdentity, QQOfficialPrincipal, QQWebhookReceipt

OfficialScene = Literal["c2c", "group"]
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

    @property
    def reply_address(self) -> dict[str, str]:
        address = {
            "scene": self.scene,
            "openid": (self.actor_openid if self.scene == "c2c" else self.group_openid),
            self.source_kind: self.source_id,
        }
        return address


CURRENT_OFFICIAL_CONTEXT: ContextVar[OfficialQQEventContext | None] = ContextVar(
    "zhenxun_qq_official_context", default=None
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
        CURRENT_OFFICIAL_CONTEXT.set(context)
        CURRENT_PLATFORM_SCOPE.set("qq_api")
    return context


def _event_address(event: Event) -> tuple[OfficialScene, str, str] | None:
    if isinstance(event, C2CMessageCreateEvent):
        return "c2c", str(event.author.user_openid), ""
    if isinstance(event, GroupMessageCreateEvent):
        return "group", str(event.author.member_openid), str(event.group_openid)
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
            f"qq_api:{app_id}:group:{group_openid}" if group_openid else None
        ),
        received_at=received_at,
        reply_deadline=received_at + window,
        max_passive_replies=4 if scene == "c2c" else 5,
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


async def allocate_reply_sequence(
    context: OfficialQQEventContext,
) -> tuple[ReplyState, int]:
    now = datetime.now(timezone.utc)
    if now >= context.reply_deadline:
        raise OfficialReplyUnavailable("QQ passive reply window expired")
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
            raise OfficialReplyUnavailable("QQ passive reply window expired")
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
    "OfficialReplyUnavailable",
    "activate_event_official_context",
    "allocate_reply_sequence",
    "bind_event_official_context",
    "digest_identifier",
    "event_official_context",
    "finish_reply",
    "get_current_official_context",
    "invalidate_principal",
    "prepare_event_context",
    "receipt_seen",
    "release_webhook_receipt",
    "reserve_webhook_receipt",
]
