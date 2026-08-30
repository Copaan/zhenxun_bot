from __future__ import annotations

import asyncio
from dataclasses import dataclass
from hashlib import sha256
import hmac
import secrets
import time
from typing import Literal

from fastapi import Header, HTTPException, Request

from zhenxun.configs.config import BotConfig, Config

SetupState = Literal["unconfigured", "partial", "restart_pending", "configured"]
_SESSION_TTL_SECONDS = 15 * 60
_MAX_SESSIONS = 16


@dataclass(slots=True)
class SetupSession:
    token_digest: str
    client_ip: str
    expires_at: float
    claimed_state: SetupState
    restart_receipt_digest: str | None = None


class SetupAccessManager:
    def __init__(self) -> None:
        self._pepper = secrets.token_bytes(32)
        self._sessions: dict[str, SetupSession] = {}
        self._restart_pending = False
        self._lock = asyncio.Lock()

    def _digest(self, value: str) -> str:
        return hmac.new(self._pepper, value.encode("utf-8"), sha256).hexdigest()

    def _prune_sessions(self) -> None:
        now = time.monotonic()
        self._sessions = {
            digest: session
            for digest, session in self._sessions.items()
            if now < session.expires_at
        }

    def state(self) -> SetupState:
        if self._restart_pending:
            return "restart_pending"
        self._prune_sessions()
        if self._sessions:
            return next(iter(self._sessions.values())).claimed_state
        has_password = bool(Config.get_config("web-ui", "password"))
        has_database = bool(BotConfig.db_url)
        if has_password and has_database:
            return "configured"
        if has_password or has_database:
            return "partial"
        return "unconfigured"

    async def create_session(self, client_ip: str) -> tuple[str, int]:
        async with self._lock:
            claimed_state = self.state()
            if claimed_state in {"configured", "restart_pending"}:
                raise HTTPException(status_code=409, detail="首次配置已经完成。")
            token = secrets.token_urlsafe(32)
            token_digest = self._digest(token)
            self._prune_sessions()
            while len(self._sessions) >= _MAX_SESSIONS:
                oldest = min(
                    self._sessions,
                    key=lambda digest: self._sessions[digest].expires_at,
                )
                self._sessions.pop(oldest, None)
            self._sessions[token_digest] = SetupSession(
                token_digest=token_digest,
                client_ip=client_ip,
                expires_at=time.monotonic() + _SESSION_TTL_SECONDS,
                claimed_state=claimed_state,
            )
            return token, _SESSION_TTL_SECONDS

    async def authorize(
        self,
        token: str | None,
        client_ip: str,
        *,
        restart_only: bool = False,
    ) -> SetupSession:
        async with self._lock:
            self._prune_sessions()
            token_digest = self._digest(token) if token else ""
            session = self._sessions.get(token_digest)
            state = self.state()
            if state == "configured" or (
                state == "restart_pending" and not restart_only
            ):
                raise HTTPException(status_code=409, detail="首次配置接口已经关闭。")
            if session is None or session.client_ip != client_ip:
                raise HTTPException(
                    status_code=401,
                    detail="首次配置会话无效或已过期。",
                )
            return session

    async def mark_applied(self, session: SetupSession) -> str:
        async with self._lock:
            if session.token_digest not in self._sessions:
                raise HTTPException(status_code=401, detail="首次配置会话无效。")
            receipt = secrets.token_urlsafe(24)
            session.restart_receipt_digest = self._digest(receipt)
            self._restart_pending = True
            return receipt

    async def consume_restart_receipt(
        self,
        session: SetupSession,
        receipt: str,
    ) -> None:
        async with self._lock:
            expected = session.restart_receipt_digest
            if not expected or not hmac.compare_digest(self._digest(receipt), expected):
                raise HTTPException(status_code=401, detail="重启票据无效或已使用。")
            session.restart_receipt_digest = None

    async def reset_for_tests(self) -> None:
        async with self._lock:
            self._sessions.clear()
            self._restart_pending = False


def client_ip(request: Request) -> str:
    return request.client.host if request.client else "unknown"


setup_access = SetupAccessManager()


async def require_setup_token(
    request: Request,
    x_setup_token: str | None = Header(default=None, alias="X-Setup-Token"),
) -> SetupSession:
    return await setup_access.authorize(x_setup_token, client_ip(request))


__all__ = [
    "SetupAccessManager",
    "SetupSession",
    "SetupState",
    "client_ip",
    "require_setup_token",
    "setup_access",
]
