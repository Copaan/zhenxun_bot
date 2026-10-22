"""Resolve protocol actors into business accounts without changing Uninfo IDs.

Permissions and conversation routing must continue to use the protocol session.
Only an explicit business entry point calls this module.
"""

from __future__ import annotations

import asyncio
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from datetime import datetime
from hashlib import sha256
import json
from zoneinfo import ZoneInfo

from nonebot.adapters.onebot.v11 import Bot as OneBot
from nonebot.adapters.onebot.v11 import MessageEvent as OneBotMessage
from tortoise.expressions import F
from tortoise.transactions import in_transaction

from zhenxun.models.business_identity import (
    AccountBindingChange,
    BusinessDailyClaim,
    BusinessIdentityLink,
)
from zhenxun.models.user_console import UserConsole
from zhenxun.services.cache.bounded_ttl import BoundedTTLCache
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


_current: ContextVar[BusinessIdentity | None] = ContextVar(
    "business_identity", default=None
)
_links = BoundedTTLCache("BUSINESS_IDENTITY", ttl_seconds=30, max_items=2048)
_inflight: dict[str, asyncio.Task] = {}


def platform_identity(bot, event, session) -> ProtocolIdentity:
    """按平台全局解析业务身份，同一平台用户 id 对应同一业务账号

    参数:
        bot: Bot
        event: Event
        session: Uninfo

    异常:
        BusinessIdentityError: 发送者与事件不一致、为 Bot 自身或以群组/频道身份发送

    返回:
        ProtocolIdentity: 协议身份
    """
    from zhenxun.utils.platform import PlatformUtils

    scope = PlatformUtils.get_platform_scope(bot)
    raw = str(session.user.id or "")
    try:
        sender = str(event.get_user_id())
    except ValueError:
        sender = ""
    if getattr(event, "sender_chat", None) is not None:
        raise BusinessIdentityError("匿名或以群组/频道身份发送的消息无法使用该功能")
    if not raw or sender != raw or raw == str(bot.self_id):
        raise BusinessIdentityError("只接受真实入站消息的发送者")
    subject = sha256(raw.encode()).hexdigest()
    key = identity_digest(scope, "", "user", "", subject)
    return ProtocolIdentity(key, scope, "", "user", "", subject, raw, f"identity:{key}")


def _platform_scope(scope: str) -> bool:
    """平台作用域是否配置为按平台全局解析业务身份

    参数:
        scope: 平台作用域

    返回:
        bool: 是否在 business_identity_scopes 中
    """
    from zhenxun.configs.config import BotConfig
    from zhenxun.services.platform_identity import is_namespaced_scope

    return is_namespaced_scope(scope) and scope in BotConfig.business_identity_scopes


def supports_business_identity(scope: str) -> bool:
    """判断平台作用域能否解析业务身份

    参数:
        scope: 平台作用域

    返回:
        bool: QQ 两个作用域或配置为按平台全局解析的作用域为 True
    """
    return scope in {"qq_api", "qq_client"} or _platform_scope(scope)


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
        if getattr(event, "anonymous", None) is not None:
            raise BusinessIdentityError("匿名消息无法使用该功能")
        if (
            event.user_id <= 0
            or str(event.user_id) != raw
            or str(event.user_id) == str(bot.self_id)
            or str(event.self_id) != str(bot.self_id)
            or getattr(event, "post_type", "message") != "message"
        ):
            raise BusinessIdentityError("只接受真实 OneBot 入站消息的发送者")
        domain, app, scene, group, original = "qq_client", "", "user", "", raw
    elif _platform_scope(scope):
        return platform_identity(bot, event, session)
    else:
        raise BusinessIdentityError("该平台未开启业务身份解析")
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


async def _verify_observed_onebot(db, link, protocol, evidence) -> None:
    """Promote legacy QQ provenance only after validating an inbound actor."""
    if link.verified or protocol.domain != "qq_client" or evidence is None:
        return
    account = (
        await UserConsole.filter(id=link.original_account_id)
        .using_db(db)
        .select_for_update()
        .get()
    )
    if account.platform not in {None, "qq", "qq_client"}:
        raise BusinessIdentityError("历史账号明确属于其他平台，需核对归属后绑定")
    if (
        account.user_id != protocol.raw_user_id
        or link.account_id != link.original_account_id
        or link.domain != protocol.domain
        or link.subject_digest != protocol.subject_digest
        or link.app_id
        or link.group_scope
        or link.scene != "user"
        or link.principal_id
    ):
        raise BusinessIdentityError("历史账号映射与当前 QQ 身份不一致，需核对归属")
    if (
        await BusinessIdentityLink.filter(original_account_id=account.id)
        .exclude(id=link.id)
        .using_db(db)
        .exists()
    ):
        raise BusinessIdentityError("历史账号存在其他原始身份映射，需核对归属")
    link.verified = True
    await link.save(using_db=db, update_fields=["verified"])
    await AccountBindingChange.create(
        using_db=db,
        id=identity_digest("event-verify", link.id),
        identity_id=link.id,
        action="verify",
        previous_account_id=link.account_id,
        account_id=link.account_id,
        revision=link.revision,
        evidence={"provenance": "onebot_inbound", **evidence},
    )


async def _ensure_link(protocol: ProtocolIdentity, evidence=None):
    cached = await _links.get(protocol.key)
    if cached is not None and cached.verified:
        return cached
    async with in_transaction() as db:
        await acquire_sqlite_identity_write(db, [protocol.key])
        link = (
            await BusinessIdentityLink.filter(id=protocol.key)
            .using_db(db)
            .select_for_update()
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
                or (
                    protocol.domain == "qq_client"
                    and user.platform == "qq_client"
                    and evidence is None
                )
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
            # A concurrent creator may have won get_or_create. Lock the winner
            # before promoting provenance, just as for an existing mapping.
            link = (
                await BusinessIdentityLink.filter(id=protocol.key)
                .using_db(db)
                .select_for_update()
                .get()
            )
        await _verify_observed_onebot(db, link, protocol, evidence)
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


async def _load_current(protocol: ProtocolIdentity, evidence=None) -> BusinessIdentity:
    await _ensure_link(protocol, evidence)
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
    evidence = (
        {
            "bot_id": str(bot.self_id),
            "event_id": str(event.message_id),
            "subject_digest": protocol.subject_digest,
        }
        if protocol.domain == "qq_client"
        else None
    )
    cache = getattr(event, "_zx_business_identities", None)
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
            task = _inflight.get(protocol.key)
            if task is None:
                task = asyncio.create_task(_load_current(protocol, evidence))
                _inflight[protocol.key] = task
                task.add_done_callback(lambda done: _finished(protocol.key, done))
            resolved = await asyncio.shield(task)
            cache[protocol.key] = resolved
    _current.set(resolved)
    return resolved


async def business_identity_for_session(session) -> BusinessIdentity:
    """在 matcher 处理流程中解析当前发送者的业务身份，无法解析时回复原因并结束 matcher

    参数:
        session: Uninfo

    异常:
        BusinessIdentityError: 不在 matcher 处理流程中且无法解析业务身份

    返回:
        BusinessIdentity: 业务身份
    """
    from nonebot.matcher import current_bot, current_event, current_matcher

    try:
        return await resolve_business_identity(
            current_bot.get(), current_event.get(), session
        )
    except BusinessIdentityError as error:
        matcher = current_matcher.get(None)
        if matcher is None:
            raise
        await matcher.finish(str(error))


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
        from zhenxun.services.platform_identity import (
            CURRENT_PLATFORM_SCOPE,
            is_isolated_scope,
        )

        if is_isolated_scope(CURRENT_PLATFORM_SCOPE.get()):
            raise BusinessIdentityError("该平台账号资产操作必须先解析业务身份")
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
    if link.revision != identity.revision or link.account_id != identity.account_id:
        raise BusinessIdentityError("账号绑定已变更，请重新发送命令")


async def invalidate_identity(key: str) -> None:
    await _links.delete(key)


async def identity_daily_claimed(db) -> bool:
    """当前业务身份今天是否已领取每日奖励，换绑账号后不能重复领取

    参数:
        db: 当前资产事务连接

    返回:
        bool: 是否已领取
    """
    actor = current_business_identity()
    if actor is None:
        return False
    today = datetime.now(ZoneInfo("Asia/Shanghai")).date()
    return (
        await BusinessDailyClaim.filter(
            id=identity_digest(actor.protocol.key, str(today))
        )
        .using_db(db)
        .exists()
    )


async def record_identity_daily_claim(db) -> None:
    """记录当前业务身份今天已领取每日奖励

    参数:
        db: 当前资产事务连接
    """
    actor = current_business_identity()
    if actor is None:
        return
    today = datetime.now(ZoneInfo("Asia/Shanghai")).date()
    await BusinessDailyClaim.get_or_create(
        using_db=db,
        id=identity_digest(actor.protocol.key, str(today)),
        defaults={
            "identity_id": actor.protocol.key,
            "day": today,
            "account_id": actor.account_id,
        },
    )
