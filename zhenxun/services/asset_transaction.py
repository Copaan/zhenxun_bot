"""Serialize account mutations and keep their financial records in one transaction."""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from functools import wraps
import hashlib
from inspect import signature
import json

from tortoise.transactions import in_transaction


@dataclass
class AssetScope:
    owner: object
    connection: object
    users: dict


_scope: ContextVar[AssetScope | None] = ContextVar("asset_transaction", default=None)


@asynccontextmanager
async def asset_transactions(user_ids, platform: str | None = None):
    from zhenxun.models.user_console import UserConsole

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
    for identity in identities:
        await UserConsole._get_user_for_write(identity, platform)
    async with in_transaction() as connection:
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
        from zhenxun.models.asset_operation import AssetOperation
        from zhenxun.services.message_execution import current_execution, operation_key

        nested = _scope.get() is not None
        identity = (
            None if nested else operation_key(function.__qualname__, str(user_id))
        )
        fingerprint = hashlib.sha256(
            json.dumps((args, kwargs), sort_keys=True, default=str).encode()
        ).hexdigest()
        platform = parameters.bind(cls, user_id, *args, **kwargs).arguments.get(
            "platform"
        )
        async with asset_transaction(user_id, platform):
            if identity and (receipt := await AssetOperation.get_or_none(id=identity)):
                if receipt.payload["fingerprint"] != fingerprint:
                    raise RuntimeError("asset_operation_input_conflict")
                return _decode_result(receipt.payload["result"])
            result = await function(cls, user_id, *args, **kwargs)
            if identity:
                await AssetOperation.create(
                    id=identity,
                    user_id=str(user_id),
                    event_id=current_execution.get().identity,
                    kind=function.__name__,
                    state="committed",
                    payload={
                        "event": current_execution.get().identity,
                        "fingerprint": fingerprint,
                        "result": _encode_result(result),
                    },
                )
            return result

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


def _encode_result(value):
    if isinstance(value, tuple):
        return {"tuple": [_encode_result(item) for item in value]}
    if hasattr(value, "_meta"):
        return {
            "model": value.__class__.__name__,
            "fields": {
                key: json.loads(json.dumps(getattr(value, key), default=str))
                for key in value._meta.db_fields
            },
        }
    return value


def _decode_result(value):
    if isinstance(value, dict) and "tuple" in value:
        return tuple(_decode_result(item) for item in value["tuple"])
    if isinstance(value, dict) and "model" in value:
        from tortoise import Tortoise

        model = Tortoise.apps["models"][value["model"]]
        return model(
            **{
                key: model._meta.fields_map[key].to_python_value(item)
                for key, item in value["fields"].items()
            }
        )
    return value


def require_positive_amount(amount: int) -> None:
    if isinstance(amount, bool) or not isinstance(amount, int) or amount <= 0:
        raise ValueError("amount_must_be_positive_integer")
