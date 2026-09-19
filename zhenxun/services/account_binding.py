"""Persistent, target-bound QQ account linking. Never merge balances or roles."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import secrets
from uuid import uuid4
from zoneinfo import ZoneInfo

from tortoise.transactions import in_transaction

from zhenxun.models.asset_operation import AssetOperation
from zhenxun.models.business_identity import (
    AccountBindingChange,
    AccountBindingRequest,
    BusinessDailyClaim,
    BusinessIdentityLink,
)
from zhenxun.models.sign_log import SignLog
from zhenxun.models.user_console import UserConsole
from zhenxun.services.business_identity import (
    BusinessIdentity,
    BusinessIdentityError,
    acquire_sqlite_identity_write,
    current_business_identity,
    identity_digest,
    invalidate_identity,
)

_ALPHABET = "23456789ABCDEFGHJKLMNPQRSTUVWXYZ"


def _now():
    return datetime.now(timezone.utc)


def _code_digest(code: str) -> str:
    return identity_digest("binding-code", code.strip().upper())


def _qq(value: str) -> str:
    if (
        not value.isascii()
        or not value.isdigit()
        or not 5 <= len(value) <= 20
        or value.startswith("0")
    ):
        raise BusinessIdentityError("请填写有效的目标 QQ 号码")
    return value


async def _lock_links(db, *keys):
    await acquire_sqlite_identity_write(db, keys)
    result = {}
    for key in sorted(set(keys)):
        link = (
            await BusinessIdentityLink.filter(id=key)
            .using_db(db)
            .select_for_update()
            .get_or_none()
        )
        if link is None:
            raise BusinessIdentityError("关联身份不存在，请先查询绑定状态")
        result[key] = link
    return result


async def _lock_accounts(db, *ids):
    result = {}
    # Keep the same lexical storage-key lock order as asset_transactions().
    rows = await UserConsole.filter(id__in=set(ids)).using_db(db).order_by("user_id")
    for row in rows:
        result[row.id] = (
            await UserConsole.filter(id=row.id).using_db(db).select_for_update().get()
        )
    return result


async def _require_settled(db, accounts):
    if (
        await AssetOperation.filter(
            user_id__in=[row.user_id for row in accounts.values()]
        )
        .exclude(state__in=["committed", "released"])
        .using_db(db)
        .exists()
    ):
        raise BusinessIdentityError(
            "存在未完成预扣或待核对资产操作，请完成对账后再绑定/解绑"
        )


async def _carry_daily_claim(db, link, accounts):
    today = _now().astimezone(ZoneInfo("Asia/Shanghai")).date()
    start = datetime.combine(today, datetime.min.time(), ZoneInfo("Asia/Shanghai"))
    if (
        await SignLog.filter(
            user_id__in=[row.user_id for row in accounts.values()],
            create_time__gte=start,
        )
        .using_db(db)
        .exists()
    ):
        await BusinessDailyClaim.get_or_create(
            using_db=db,
            id=identity_digest(link.id, str(today)),
            defaults={
                "identity_id": link.id,
                "day": today,
                "account_id": link.account_id,
            },
        )


async def _publish(link_id: str, accounts) -> None:
    from zhenxun.services.log import logger

    targets = [(invalidate_identity, link_id)] + [
        (UserConsole.invalidate_user_cache, account.user_id)
        for account in accounts.values()
    ]
    for invalidate, key in targets:
        try:
            await invalidate(key)
        except Exception as error:
            # One failed publication must not skip other affected accounts.
            logger.error(f"绑定已提交，定向缓存失效失败 key={key}", e=error)


async def issue_binding(
    identity: BusinessIdentity, target_qq: str, *, operation_id: str | None = None
) -> str:
    if identity.protocol.domain != "qq_api":
        raise BusinessIdentityError("请先在官方 QQ 或频道发送：绑定QQ 目标QQ号码")
    return await _issue(
        identity, identity.protocol.key, _qq(target_qq), "bind", operation_id
    )


async def issue_unbind(
    identity: BusinessIdentity,
    linked_identity: str | None = None,
    *,
    operation_id: str | None = None,
) -> str:
    key = linked_identity or identity.protocol.key
    if identity.protocol.domain == "qq_client" and not linked_identity:
        raise BusinessIdentityError("请先查询绑定状态，再发送：解绑QQ 关联身份ID")
    return await _issue(identity, key, "", "unbind", operation_id)


async def _issue(actor, key, target, action, operation_id):
    code = "ZX-" + "".join(secrets.choice(_ALPHABET) for _ in range(12))
    request_id = identity_digest("binding-request", operation_id or uuid4().hex)
    async with in_transaction() as db:
        links = await _lock_links(db, actor.protocol.key, key)
        link, acting = links[key], links[actor.protocol.key]
        if not acting.verified or not link.verified:
            raise BusinessIdentityError("历史账号归属待核验，暂不能跨平台绑定")
        if action == "bind":
            if link.account_id != link.original_account_id:
                raise BusinessIdentityError("已绑定 QQ，请先解绑；可发送绑定状态查询")
        else:
            if link.account_id == link.original_account_id:
                raise BusinessIdentityError("当前身份尚未绑定 QQ")
            if actor.protocol.key != key and not (
                actor.protocol.domain == "qq_client"
                and acting.account_id == link.account_id
            ):
                raise BusinessIdentityError("不能解绑其他账号的身份")
            target = (
                await UserConsole.filter(id=link.account_id).using_db(db).get()
            ).user_id
        if await AccountBindingRequest.filter(id=request_id).using_db(db).exists():
            raise BusinessIdentityError("此操作已登记；请查询绑定状态或重新发送申请")
        await (
            AccountBindingRequest.filter(identity_id=key, state="pending")
            .using_db(db)
            .update(state="revoked")
        )
        await AccountBindingRequest.create(
            using_db=db,
            id=request_id,
            identity_id=key,
            code_digest=_code_digest(code),
            target_qq=target,
            initiator_id=actor.protocol.key,
            action=action,
            revision=link.revision,
            expires_at=_now() + timedelta(minutes=5),
        )
    return code


async def redeem_binding(actor: BusinessIdentity, code: str) -> dict:
    if actor.protocol.domain != "qq_client":
        raise BusinessIdentityError("绑定码须由目标 QQ 通过 OneBot 客户端发送")
    return await _redeem(actor, code, "bind")


async def confirm_unbind(actor: BusinessIdentity, code: str) -> dict:
    return await _redeem(actor, code, "unbind")


async def _redeem(actor, code, action):
    request = await AccountBindingRequest.get_or_none(code_digest=_code_digest(code))
    if request is None or request.action != action:
        raise BusinessIdentityError("绑定码不存在或用途不匹配")
    accounts = {}
    async with in_transaction() as db:
        links = await _lock_links(db, actor.protocol.key, request.identity_id)
        link, acting = links[request.identity_id], links[actor.protocol.key]
        request = (
            await AccountBindingRequest.filter(id=request.id)
            .using_db(db)
            .select_for_update()
            .get()
        )
        if action == "bind":
            if actor.protocol.raw_user_id != request.target_qq:
                raise BusinessIdentityError("该绑定码指定的 QQ 与消息发送者不一致")
        elif actor.protocol.key != request.initiator_id:
            raise BusinessIdentityError("解绑确认必须由申请者发送")
        if request.state == "completed":
            return {
                **request.result,
                "replayed": True,
                "superseded": link.revision != request.result["revision"],
                "current_account_id": link.account_id,
                "current_revision": link.revision,
            }
        if request.state != "pending" or request.expires_at <= _now():
            raise BusinessIdentityError("绑定码已撤销或过期，请重新申请")
        if not acting.verified or not link.verified:
            raise BusinessIdentityError("历史账号归属待核验，暂不能跨平台绑定")
        if request.revision != link.revision:
            raise BusinessIdentityError("绑定关系已变化，请重新申请")
        if action == "bind":
            if link.account_id != link.original_account_id:
                raise BusinessIdentityError("已绑定其他账号，请先解绑")
            destination = acting.original_account_id
        else:
            if actor.protocol.key != link.id and not (
                actor.protocol.domain == "qq_client"
                and acting.account_id == link.account_id
            ):
                raise BusinessIdentityError("当前 QQ 已不拥有该关联身份")
            destination = link.original_account_id
        accounts = await _lock_accounts(db, link.account_id, destination)
        await _require_settled(db, accounts)
        await _carry_daily_claim(db, link, accounts)
        previous = link.account_id
        link.account_id = destination
        link.revision += 1
        link.bound_at = _now() if action == "bind" else None
        await link.save(
            using_db=db, update_fields=["account_id", "revision", "bound_at"]
        )
        result = {
            "status": "completed",
            "action": action,
            "identity_id": link.id,
            "account_id": destination,
            "storage_key": accounts[destination].user_id,
            "revision": link.revision,
            "operation_id": request.id,
        }
        await AccountBindingChange.create(
            using_db=db,
            id=request.id,
            identity_id=link.id,
            action=action,
            previous_account_id=previous,
            account_id=destination,
            revision=link.revision,
            evidence={
                "initiator": request.initiator_id,
                "confirmer": actor.protocol.key,
                "target_qq": request.target_qq,
            },
        )
        request.state, request.result = "completed", result
        await request.save(using_db=db, update_fields=["state", "result"])
    await _publish(request.identity_id, accounts)
    return result


async def binding_status(actor: BusinessIdentity) -> list[dict]:
    own = await BusinessIdentityLink.get(id=actor.protocol.key)
    query = (
        BusinessIdentityLink.filter(account_id=own.original_account_id)
        if actor.protocol.domain == "qq_client"
        else BusinessIdentityLink.filter(id=own.id)
    )
    return [
        {
            "identity_id": row.id,
            "domain": row.domain,
            "app_id": row.app_id,
            "scene": row.scene,
            "group_scope": row.group_scope,
            "account_id": row.account_id,
            "storage_key": row.account.user_id,
            "revision": row.revision,
            "bound": row.account_id != row.original_account_id,
            "verified": row.verified,
        }
        for row in await query.select_related("account").order_by("id")
    ]


async def identity_daily_claimed(db) -> bool:
    actor = current_business_identity()
    if actor is None:
        return False
    today = _now().astimezone(ZoneInfo("Asia/Shanghai")).date()
    return (
        await BusinessDailyClaim.filter(
            id=identity_digest(actor.protocol.key, str(today))
        )
        .using_db(db)
        .exists()
    )


async def record_identity_daily_claim(db) -> None:
    actor = current_business_identity()
    if actor is None:
        return
    today = _now().astimezone(ZoneInfo("Asia/Shanghai")).date()
    await BusinessDailyClaim.get_or_create(
        using_db=db,
        id=identity_digest(actor.protocol.key, str(today)),
        defaults={
            "identity_id": actor.protocol.key,
            "day": today,
            "account_id": actor.account_id,
        },
    )


async def verify_legacy_identity(key: str, *, evidence: str, reviewer: str) -> None:
    """Administrative migration API; never exposed to user binding commands."""
    if not evidence.strip() or not reviewer.strip():
        raise ValueError("Verification requires recorded provenance and reviewer")
    async with in_transaction() as db:
        link = (await _lock_links(db, key))[key]
        if link.verified:
            return
        link.verified = True
        await link.save(using_db=db, update_fields=["verified"])
        await AccountBindingChange.create(
            using_db=db,
            id=uuid4().hex,
            identity_id=key,
            action="verify",
            previous_account_id=link.account_id,
            account_id=link.account_id,
            revision=link.revision,
            evidence={"provenance": evidence, "reviewer": reviewer},
        )
    await invalidate_identity(key)
