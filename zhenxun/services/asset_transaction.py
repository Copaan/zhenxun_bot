"""Serialize account mutations and keep their financial records in one transaction."""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from functools import wraps
from inspect import signature

from tortoise.transactions import in_transaction


@dataclass
class AssetScope:
    owner: object
    connection: object
    users: dict


_scope: ContextVar[AssetScope | None] = ContextVar("asset_transaction", default=None)


def current_asset_connection():
    """Return the connection owned by the current asset transaction.

    Asset model code must use this connection explicitly. Relying on the
    ORM's default connection can escape the transaction on pooled databases.
    """
    scope = _scope.get()
    if scope is not None and scope.owner is not asyncio.current_task():
        raise RuntimeError("asset_transaction_cross_task")
    return scope.connection if scope is not None else None


def locked_asset_keys() -> frozenset[str]:
    scope = _scope.get()
    if scope is None or scope.owner is not asyncio.current_task():
        return frozenset()
    return frozenset(scope.users)


@asynccontextmanager
async def asset_transactions(user_ids, platform: str | None = None):
    from zhenxun.models.user_console import UserConsole
    from zhenxun.services.business_identity import (
        current_business_identity,
        validate_business_write,
    )

    identities = sorted(set(map(str, user_ids)))
    active = _scope.get()
    task = asyncio.current_task()
    if active is not None:
        if active.owner is not task:
            raise RuntimeError("asset_transaction_cross_task")
        if not set(identities).issubset(active.users):
            raise RuntimeError("asset_transaction_account_not_locked")
        yield {identity: active.users[identity] for identity in identities}
        return
    business = current_business_identity()
    for identity in identities:
        if business is None or identity != business.storage_key:
            await UserConsole._get_user_for_write(identity, platform)
    async with in_transaction() as connection:
        await validate_business_write(identities, connection)
        users = {}
        for identity in identities:
            users[identity] = (
                await UserConsole.filter(user_id=identity)
                .using_db(connection)
                .select_for_update()
                .get()
            )
        token = _scope.set(AssetScope(task, connection, users))
        try:
            yield users
        finally:
            _scope.reset(token)


@asynccontextmanager
async def asset_transaction(user_id: str, platform: str | None = None):
    async with asset_transactions([user_id], platform) as users:
        yield users[str(user_id)]


def asset_db_kwargs() -> dict[str, object]:
    """Build ORM kwargs that keep an operation inside the active asset tx."""
    connection = current_asset_connection()
    return {"using_db": connection} if connection is not None else {}


def asset_queryset(query):
    """Bind an ORM queryset to the active asset transaction when present."""
    connection = current_asset_connection()
    return query.using_db(connection) if connection is not None else query


def account_write(function):
    parameters = signature(function)

    @wraps(function)
    async def wrapped(cls, user_id, *args, **kwargs):
        from .db_context import with_db_timeout

        return await with_db_timeout(
            execute(cls, user_id, *args, **kwargs),
            operation=function.__qualname__,
            source="asset_transaction",
        )

    async def execute(cls, user_id, *args, **kwargs):
        platform = parameters.bind(cls, user_id, *args, **kwargs).arguments.get(
            "platform"
        )
        async with asset_transaction(user_id, platform):
            return await function(cls, user_id, *args, **kwargs)

    return wrapped


def asset_call(function):
    """Give nonstandard asset entry points the same total database budget."""

    @wraps(function)
    async def wrapped(*args, **kwargs):
        from .db_context import with_db_timeout

        return await with_db_timeout(
            function(*args, **kwargs),
            operation=function.__qualname__,
            source="asset_transaction",
        )

    return wrapped


def require_positive_amount(amount: int) -> None:
    if isinstance(amount, bool) or not isinstance(amount, int) or amount <= 0:
        raise ValueError("amount_must_be_positive_integer")
