from __future__ import annotations

import asyncio
from hashlib import sha256
import hmac
import secrets

from fastapi import HTTPException

from zhenxun.services.cache import BoundedTTLCache

_MAX_CONNECT_FAILURES = 5


class ConsoleAccessManager:
    def __init__(self) -> None:
        self._pepper = secrets.token_bytes(32)
        self._code_digest: str | None = None
        self._boot_id: str | None = None
        self._lock = asyncio.Lock()
        self._failures = BoundedTTLCache[str, int](
            "webui_console_connect_failures",
            ttl_seconds=60,
            max_items=2048,
        )

    def _digest(self, value: str) -> str:
        return hmac.new(self._pepper, value.encode("utf-8"), sha256).hexdigest()

    async def prepare(self) -> str | None:
        """Create the process-scoped bearer code and return it exactly once."""
        async with self._lock:
            if self._code_digest is not None:
                return None
            code = secrets.token_urlsafe(32)
            self._code_digest = self._digest(code)
            self._boot_id = secrets.token_urlsafe(18)
            await self._failures.clear()
            return code

    async def claim(self, code: str, client_ip: str) -> str:
        async with self._lock:
            failures = await self._failures.get(client_ip) or 0
            if failures >= _MAX_CONNECT_FAILURES:
                raise HTTPException(
                    status_code=429,
                    detail="控制台连接尝试过于频繁，请稍后再试。",
                )
            if (
                self._code_digest is None
                or self._boot_id is None
                or not hmac.compare_digest(self._digest(code), self._code_digest)
            ):
                await self._failures.set(client_ip, failures + 1)
                raise HTTPException(
                    status_code=401,
                    detail="控制台连接链接无效，请使用本次启动输出的最新链接。",
                )
            await self._failures.delete(client_ip)
            return self._boot_id

    def accepts_boot_id(self, boot_id: object) -> bool:
        return bool(
            isinstance(boot_id, str)
            and self._boot_id
            and hmac.compare_digest(boot_id, self._boot_id)
        )

    async def reset_for_tests(self) -> None:
        async with self._lock:
            self._code_digest = None
            self._boot_id = None
            await self._failures.clear()


console_access = ConsoleAccessManager()


__all__ = ["ConsoleAccessManager", "console_access"]
