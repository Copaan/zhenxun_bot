"""Resolve protocol actors into business accounts without changing Uninfo IDs.

Permissions and conversation routing must continue to use the protocol session.
Only an explicit business entry point calls this module.
"""

from __future__ import annotations

import asyncio
from collections import defaultdict
from collections.abc import Callable
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from hashlib import sha256
import json

from nonebot.adapters.onebot.v11 import Bot as OneBot
from nonebot.adapters.onebot.v11 import MessageEvent as OneBotMessage
from tortoise.expressions import F
from tortoise.transactions import in_transaction

from zhenxun.models.business_identity import (
    AccountBindingChange,
    BusinessEventAccount,
    BusinessIdentityLink,
)
from zhenxun.models.user_console import UserConsole
from zhenxun.services.cache.bounded_ttl import BoundedTTLCache
from zhenxun.services.message_execution import current_execution
from zhenxun.services.platform_identity import business_write_scope


class BusinessIdentityError(ValueError):
    """A safe account could not be selected; do not fall back to a raw OpenID."""


def identity_digest(*parts: object) -> str:
    return sha256(
        json.dumps(parts, ensure_ascii=True, separators=(",", ":")).encode()
    ).hexdigest()


@dataclass(frozen=True)
class ProtocolIdentity:
    key: str
    domain: str
    app_id: str
    scene: str
    group_scope: str
    subject_digest: str
    raw_user_id: str
    original_key: str
    principal_id: str | None = None


@dataclass(frozen=True)
class BusinessIdentity:
    protocol: ProtocolIdentity
    account_id: int
    storage_key: str
    revision: int
    verified: bool
    event_snapshot: str | None = None


_current: ContextVar[BusinessIdentity | None] = ContextVar(
    "business_identity", default=None
)
_links = BoundedTTLCache("BUSINESS_IDENTITY", ttl_seconds=30, max_items=2048)
_inflight: dict[tuple[str, bool], asyncio.Task] = {}
_resolvers: dict[str, Callable] = {}


def register_business_identity_resolver(scope: str, resolver: Callable) -> None:
    """Register an adapter's explicit user/tenant domain, without guessing it."""
    if scope in {"qq_api", "qq_client"} or scope in _resolvers:
        raise ValueError("Business identity resolver already registered")
    _resolvers[scope] = resolver


@contextmanager
def business_event_scope():
    token = _current.set(None)
    try:
        yield
    finally:
        _current.reset(token)


@contextmanager
def business_account_scope(identity: BusinessIdentity | None):
    """Carry a resolved reservation across sibling tasks, never an ORM lock."""
    token = _current.set(identity)
    try:
        yield
    finally:
        _current.reset(token)


def protocol_identity(bot, event, session) -> ProtocolIdentity:
    from zhenxun.adapters.qq_official.context import event_official_context
    from zhenxun.utils.platform import PlatformUtils

    scope = PlatformUtils.get_platform_scope(bot)
    raw = str(session.user.id or "")
    if (
        not raw
        or event.get_type() != "message"
        or str(session.self_id) != str(bot.self_id)
    ):
        raise BusinessIdentityError("消息身份缺失或 Bot 不匹配")
    principal = None
    if scope == "qq_api":
        context = event_official_context(event)
        if (
            context is None
            or context.app_id != str(bot.self_id)
            or context.actor_openid != raw
            or getattr(getattr(event, "author", None), "bot", False)
        ):
            raise BusinessIdentityError("QQ 官方上下文缺失或身份不匹配")
        domain, app, scene = "qq_api", context.app_id, context.scene
        group = context.group_openid if scene == "group" else ""
        principal = str(context.principal_id)
        original = context.storage_user_id
    elif isinstance(bot, OneBot) and isinstance(event, OneBotMessage):
        if (
            getattr(event, "anonymous", None) is not None
            or event.user_id <= 0
            or str(event.user_id) != raw
            or str(event.user_id) == str(bot.self_id)
            or str(event.self_id) != str(bot.self_id)
            or getattr(event, "post_type", "message") != "message"
        ):
            raise BusinessIdentityError("只接受真实 OneBot 入站消息的发送者")
        domain, app, scene, group, original = "qq_client", "", "user", "", raw
    else:
        if resolver := _resolvers.get(scope):
            resolved = resolver(bot, event, session)
            if (
                not isinstance(resolved, ProtocolIdentity)
                or resolved.domain in {"qq_api", "qq_client"}
                or resolved.raw_user_id != raw
                or resolved.key
                != identity_digest(
                    resolved.domain,
                    resolved.app_id,
                    resolved.scene,
                    resolved.group_scope,
                    resolved.subject_digest,
                )
                or resolved.original_key != f"identity:{resolved.key}"
            ):
                raise BusinessIdentityError("适配器业务身份解析器返回无效结果")
            return resolved
        raise BusinessIdentityError("该适配器尚未注册业务身份解析器")
    subject = sha256(raw.encode()).hexdigest()
    key = identity_digest(domain, app, scene, group, subject)
    return ProtocolIdentity(
        key, domain, app, scene, group, subject, raw, original, principal
    )


async def acquire_sqlite_identity_write(db, keys) -> None:
    """Reserve SQLite's writer before reading a mapping that may be changed.

    SELECT FOR UPDATE is ignored by SQLite. A deferred read transaction cannot
    safely upgrade after another process commits; this no-op update acquires
    the writer without changing the revision or increasing the busy timeout.
    """
    if db.capabilities.dialect == "sqlite":
        await (
            BusinessIdentityLink.filter(id__in=sorted(set(keys)))
            .using_db(db)
            .update(revision=F("revision"))
        )


async def _ensure_link(protocol: ProtocolIdentity):
    cached = await _links.get(protocol.key)
    if cached is not None:
        return cached
    async with in_transaction() as db:
        await acquire_sqlite_identity_write(db, [protocol.key])
        link = (
            await BusinessIdentityLink.filter(id=protocol.key)
            .using_db(db)
            .get_or_none()
        )
        if link is None:
            with business_write_scope({protocol.original_key}):
                user, account_created = await UserConsole.get_or_create_user(
                    protocol.original_key, protocol.domain
                )
            # Base provenance on the winning row, including a concurrent
            # creator's row, not a pre-insert existence check.
            verified = (
                account_created
                or (protocol.domain == "qq_api" and user.platform != "qq_client")
                or (protocol.domain == "qq_client" and user.platform == "qq_client")
            )
            link, created = await BusinessIdentityLink.get_or_create(
                using_db=db,
                id=protocol.key,
                defaults={
                    "domain": protocol.domain,
                    "app_id": protocol.app_id,
                    "scene": protocol.scene,
                    "group_scope": protocol.group_scope,
                    "subject_digest": protocol.subject_digest,
                    "principal_id": protocol.principal_id,
                    "original_account_id": user.id,
                    "account_id": user.id,
                    "verified": verified,
                },
            )
            if created:
                await AccountBindingChange.create(
                    using_db=db,
                    id=identity_digest("provision", link.id),
                    identity_id=link.id,
                    action="provision",
                    previous_account_id=user.id,
                    account_id=user.id,
                    revision=0,
                    evidence={
                        "domain": protocol.domain,
                        "verified": verified,
                        "existing_account": not account_created,
                    },
                )
        account = await UserConsole.filter(id=link.account_id).using_db(db).get()
        result = BusinessIdentity(
            protocol, account.id, account.user_id, link.revision, link.verified
        )
    await _links.set(protocol.key, result)
    return result


def _finished(key, task):
    if _inflight.get(key) is task:
        _inflight.pop(key, None)
    if not task.cancelled():
        task.exception()


async def _load_current(protocol: ProtocolIdentity) -> BusinessIdentity:
    await _ensure_link(protocol)
    # Coalesce concurrent events too, while re-reading once for a later event
    # so another process's committed binding cannot be hidden by the TTL cache.
    link = (
        await BusinessIdentityLink.filter(id=protocol.key)
        .select_related("account")
        .get()
    )
    return BusinessIdentity(
        protocol, link.account_id, link.account.user_id, link.revision, link.verified
    )


async def resolve_business_identity(bot, event, session) -> BusinessIdentity:
    protocol = protocol_identity(bot, event, session)
    execution = current_execution.get()
    # The shared execution belongs to one durable event, unlike a ContextVar
    # copied into each matcher task. Its lock coalesces their first resolution.
    cache = (
        execution.business_identities
        if execution
        else getattr(event, "_zx_business_identities", None)
    )
    if cache is None:
        cache = {}
        object.__setattr__(event, "_zx_business_identities", cache)
    lock = cache.setdefault(("lock", protocol.key), asyncio.Lock())
    async with lock:
        resolved = cache.get(protocol.key)
        if resolved is None:
            from zhenxun.services.asset_transaction import current_asset_connection

            if current_asset_connection() is not None:
                raise BusinessIdentityError("请在资产事务开始前解析业务身份")
            flight_key = (protocol.key, bool(execution))
            task = _inflight.get(flight_key)
            if task is None:
                task = asyncio.create_task(
                    _ensure_link(protocol) if execution else _load_current(protocol)
                )
                _inflight[flight_key] = task
                task.add_done_callback(lambda done: _finished(flight_key, done))
            resolved = await asyncio.shield(task)
            if execution:
                snapshot_id = identity_digest(execution.identity, protocol.key)
                async with in_transaction() as db:
                    await acquire_sqlite_identity_write(db, [protocol.key])
                    link = (
                        await BusinessIdentityLink.filter(id=protocol.key)
                        .using_db(db)
                        .select_for_update()
                        .get()
                    )
                    snapshot, _ = await BusinessEventAccount.get_or_create(
                        using_db=db,
                        id=snapshot_id,
                        defaults={
                            "event_id": execution.identity,
                            "identity_id": protocol.key,
                            "account_id": link.account_id,
                            "revision": link.revision,
                        },
                    )
                    account = (
                        await UserConsole.filter(id=snapshot.account_id)
                        .using_db(db)
                        .get()
                    )
                    resolved = BusinessIdentity(
                        protocol,
                        account.id,
                        account.user_id,
                        snapshot.revision,
                        link.verified,
                        snapshot_id,
                    )
            cache[protocol.key] = resolved
    _current.set(resolved)
    return resolved


async def business_identity_for_session(session) -> BusinessIdentity:
    """Convenience for existing business services called by a NoneBot handler."""
    from nonebot.matcher import current_bot, current_event

    return await resolve_business_identity(
        current_bot.get(), current_event.get(), session
    )


async def business_user_id(session) -> str:
    return (await business_identity_for_session(session)).storage_key


async def official_group_business_keys(session) -> list[str]:
    """Only observed identities in this official group; never fall back to all QQ."""
    actor = await business_identity_for_session(session)
    if actor.protocol.scene != "group":
        raise BusinessIdentityError("频道成员排行暂不可用，请使用个人查询或全局排行")
    return (
        await BusinessIdentityLink.filter(
            domain="qq_api",
            app_id=actor.protocol.app_id,
            scene="group",
            group_scope=actor.protocol.group_scope,
        )
        .distinct()
        .values_list("account__user_id", flat=True)
    )


def current_business_identity() -> BusinessIdentity | None:
    identity = _current.get()
    if identity and identity.protocol.domain == "qq_api":
        from zhenxun.adapters.qq_official.context import get_current_official_context

        context = get_current_official_context()
        if (
            context is None
            or context.app_id != identity.protocol.app_id
            or str(context.principal_id) != identity.protocol.principal_id
        ):
            return None
    return identity


async def validate_business_write(keys: list[str], db) -> None:
    """Lock identity before accounts, the same order used by binding changes."""
    identity = current_business_identity()
    if identity is None:
        from zhenxun.services.platform_identity import CURRENT_PLATFORM_SCOPE

        if CURRENT_PLATFORM_SCOPE.get() == "qq_api":
            raise BusinessIdentityError("官方账号资产操作必须先解析业务身份")
        return
    if set(keys) != {identity.storage_key}:
        raise BusinessIdentityError("业务账号与当前身份不一致")
    await acquire_sqlite_identity_write(db, [identity.protocol.key])
    link = (
        await BusinessIdentityLink.filter(id=identity.protocol.key)
        .using_db(db)
        .select_for_update()
        .get()
    )
    if identity.event_snapshot:
        execution = current_execution.get()
        snapshot = (
            await BusinessEventAccount.filter(id=identity.event_snapshot)
            .using_db(db)
            .get()
        )
        if (
            not execution
            or snapshot.event_id != execution.identity
            or snapshot.account_id != identity.account_id
        ):
            raise BusinessIdentityError("消息账号快照不匹配")
    elif link.revision != identity.revision or link.account_id != identity.account_id:
        raise BusinessIdentityError("账号绑定已变更，请重新发送命令")


async def invalidate_identity(key: str) -> None:
    await _links.delete(key)


async def migration_preview() -> list[dict]:
    """Read only. NULL/qq and identifier shape are deliberately not evidence."""
    from zhenxun.adapters.qq_official.models import QQOfficialPrincipal

    principals = {
        f"principal:{value}"
        for value in await QQOfficialPrincipal.all().values_list("id", flat=True)
    }
    links = defaultdict(list)
    for row in await BusinessIdentityLink.all():
        links[row.original_account_id].append(row)
    rows = []
    for user in await UserConsole.all().order_by("id"):
        candidates = links[user.id]
        status = "ambiguous"
        if len({row.domain for row in candidates}) > 1 or (
            user.user_id in principals and user.platform == "qq_client"
        ):
            status = "conflict"
        elif candidates and all(row.verified for row in candidates):
            status = "verified_mapping"
        elif user.user_id in principals:
            status = "official_principal"
        elif user.platform == "qq_client":
            status = "explicit_onebot"
        rows.append(
            {
                "account_id": user.id,
                "storage_key": user.user_id,
                "platform": user.platform,
                "status": status,
                "identity_ids": [row.id for row in candidates],
                "automatic_rewrite": False,
            }
        )
    return rows
