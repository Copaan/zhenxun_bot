from __future__ import annotations

import asyncio
from dataclasses import dataclass
from hashlib import sha256
import hmac
import secrets
import string
import time
from typing import Literal

from fastapi import Header, HTTPException, Request

from zhenxun.configs.config import BotConfig, Config
from zhenxun.services.cache import BoundedTTLCache
from zhenxun.services.log import logger

SetupState = Literal["unconfigured", "partial", "restart_pending", "configured"]
_CODE_TTL_SECONDS = 30 * 60
_SESSION_TTL_SECONDS = 15 * 60
_MAX_CLAIM_FAILURES = 5
_CODE_ALPHABET = string.ascii_uppercase + string.digits


@dataclass(slots=True)
class _SetupSession:
    token_digest: str
    client_ip: str
    expires_at: float
    claimed_state: SetupState
    restart_receipt_digest: str | None = None


class SetupAccessManager:
    def __init__(self) -> None:
        self._pepper = secrets.token_bytes(32)
        self._code_digest: str | None = None
        self._code_expires_at = 0.0
        self._session: _SetupSession | None = None
        self._restart_pending = False
        self._prepared = False
        self._lock = asyncio.Lock()
        self._failures = BoundedTTLCache[str, int](
            "webui_setup_claim_failures", ttl_seconds=60, max_items=2048
        )

    def _digest(self, value: str) -> str:
        return hmac.new(self._pepper, value.encode("utf-8"), sha256).hexdigest()

    def state(self) -> SetupState:
        if self._restart_pending:
            return "restart_pending"
        if self._session is not None and time.monotonic() < self._session.expires_at:
            return self._session.claimed_state
        has_password = bool(Config.get_config("web-ui", "password"))
        has_database = bool(BotConfig.db_url)
        if has_password and has_database:
            return "configured"
        if has_password or has_database:
            return "partial"
        return "unconfigured"

    async def prepare(self) -> None:
        async with self._lock:
            if self._prepared or self.state() == "configured":
                self._prepared = True
                return
            code = "".join(secrets.choice(_CODE_ALPHABET) for _ in range(8))
            self._code_digest = self._digest(code)
            self._code_expires_at = time.monotonic() + _CODE_TTL_SECONDS
            self._prepared = True
            logger.info(
                f"首次配置启动码: <y>{code}</y>（30 分钟内有效，仅可使用一次）",
                "WebUi",
            )

    async def claim(self, code: str, client_ip: str) -> tuple[str, int]:
        await self.prepare()
        async with self._lock:
            claimed_state = self.state()
            if claimed_state in {"configured", "restart_pending"}:
                raise HTTPException(status_code=409, detail="首次配置已经完成。")
            failures = await self._failures.get(client_ip) or 0
            if failures >= _MAX_CLAIM_FAILURES:
                raise HTTPException(
                    status_code=429, detail="启动码尝试过于频繁，请稍后再试。"
                )
            if self._code_digest is None or time.monotonic() >= self._code_expires_at:
                raise HTTPException(
                    status_code=401, detail="启动码已过期，请重启真寻。"
                )
            if not hmac.compare_digest(
                self._digest(code.strip().upper()), self._code_digest
            ):
                await self._failures.set(client_ip, failures + 1)
                raise HTTPException(status_code=401, detail="启动码无效。")

            token = secrets.token_urlsafe(32)
            self._session = _SetupSession(
                token_digest=self._digest(token),
                client_ip=client_ip,
                expires_at=time.monotonic() + _SESSION_TTL_SECONDS,
                claimed_state=claimed_state,
            )
            self._code_digest = None
            self._code_expires_at = 0.0
            await self._failures.delete(client_ip)
            return token, _SESSION_TTL_SECONDS

    async def authorize(
        self,
        token: str | None,
        client_ip: str,
        *,
        restart_only: bool = False,
    ) -> _SetupSession:
        async with self._lock:
            session = self._session
            if session is not None and time.monotonic() >= session.expires_at:
                raise HTTPException(
                    status_code=401, detail="首次配置会话无效或已过期。"
                )
            state = self.state()
            if state == "configured" or (
                state == "restart_pending" and not restart_only
            ):
                raise HTTPException(status_code=409, detail="首次配置接口已经关闭。")
            if (
                not token
                or session is None
                or session.client_ip != client_ip
                or not hmac.compare_digest(self._digest(token), session.token_digest)
            ):
                raise HTTPException(
                    status_code=401, detail="首次配置会话无效或已过期。"
                )
            return session

    async def mark_applied(self) -> str:
        async with self._lock:
            if self._session is None:
                raise HTTPException(status_code=401, detail="首次配置会话无效。")
            receipt = secrets.token_urlsafe(24)
            self._session.restart_receipt_digest = self._digest(receipt)
            self._restart_pending = True
            return receipt

    async def consume_restart_receipt(self, receipt: str) -> None:
        async with self._lock:
            session = self._session
            expected = session.restart_receipt_digest if session else None
            if not expected or not hmac.compare_digest(self._digest(receipt), expected):
                raise HTTPException(status_code=401, detail="重启票据无效或已使用。")
            session.restart_receipt_digest = None

    async def reset_for_tests(self) -> None:
        async with self._lock:
            self._code_digest = None
            self._code_expires_at = 0.0
            self._session = None
            self._restart_pending = False
            self._prepared = False
            await self._failures.clear()


def client_ip(request: Request) -> str:
    return request.client.host if request.client else "unknown"


setup_access = SetupAccessManager()


async def require_setup_token(
    request: Request,
    x_setup_token: str | None = Header(default=None, alias="X-Setup-Token"),
) -> _SetupSession:
    return await setup_access.authorize(x_setup_token, client_ip(request))


__all__ = [
    "SetupState",
    "client_ip",
    "require_setup_token",
    "setup_access",
]
