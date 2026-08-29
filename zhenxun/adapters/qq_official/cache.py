from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import datetime
from typing import TypeAlias
from uuid import UUID

from zhenxun.services.cache import BoundedTTLCache

IdentityCacheKey: TypeAlias = tuple[str, str, str, str]
ReceiptCacheKey: TypeAlias = tuple[str, str]
ReplyCacheKey: TypeAlias = tuple[str, str, str]


@dataclass(slots=True)
class ReplyState:
    deadline: datetime
    max_successful: int
    next_sequence: int = 1
    successful: int = 0
    in_flight: int = 0
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)


IDENTITY_CACHE = BoundedTTLCache[IdentityCacheKey, UUID](
    "qq_official_identity", ttl_seconds=30 * 60, max_items=20_000
)
RECEIPT_CACHE = BoundedTTLCache[ReceiptCacheKey, bool](
    "qq_webhook_receipt", ttl_seconds=10 * 60, max_items=50_000
)
REPLY_STATE_CACHE = BoundedTTLCache[ReplyCacheKey, ReplyState](
    "qq_official_reply_state", ttl_seconds=65 * 60, max_items=20_000
)

_database_fallbacks = 0


def record_database_fallback() -> None:
    global _database_fallbacks
    _database_fallbacks += 1


async def clear_qq_official_caches() -> None:
    await asyncio.gather(
        IDENTITY_CACHE.clear(),
        RECEIPT_CACHE.clear(),
        REPLY_STATE_CACHE.clear(),
    )


async def qq_official_cache_stats() -> dict[str, object]:
    return {
        "identity": (await IDENTITY_CACHE.stats()).to_dict(),
        "receipt": (await RECEIPT_CACHE.stats()).to_dict(),
        "reply_state": (await REPLY_STATE_CACHE.stats()).to_dict(),
        "database_fallbacks": _database_fallbacks,
    }


__all__ = [
    "IDENTITY_CACHE",
    "RECEIPT_CACHE",
    "REPLY_STATE_CACHE",
    "ReplyState",
    "clear_qq_official_caches",
    "qq_official_cache_stats",
    "record_database_fallback",
]
