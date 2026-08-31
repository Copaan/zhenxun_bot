from typing import Literal

from pydantic import BaseModel, Field


class ProtocolConnection(BaseModel):
    self_id: str
    adapter: str
    platform: Literal["onebot_v11", "qq_official", "other"]
    nickname: str | None = None
    avatar_url: str | None = None


class ProtocolQQError(BaseModel):
    code: str
    message: str
    provider_code: str | None = None
    provider_explanation: str | None = None
    suggestion: str | None = None
    http_status: int | None = None
    trace_id: str | None = None
    retryable: bool = False


class ProtocolQQBotStatus(BaseModel):
    app_id: str
    bot_id: str | None = None
    username: str | None = None
    avatar_url: str | None = None
    mode: Literal["websocket", "webhook"]
    state: Literal[
        "authorizing",
        "gateway",
        "connecting",
        "connected",
        "reconnecting",
        "failed",
    ]
    connected: bool = False
    updated_at: str | None = None
    error: ProtocolQQError | None = None


class ProtocolStatus(BaseModel):
    onebot_v11_connected: bool
    qq_official_enabled: bool
    qq_official_connected: bool
    qq_webhook_mode: Literal["external", "builtin_https"]
    qq_webhook_callback_url: str | None = None
    connections: list[ProtocolConnection]
    qq_bots: list[ProtocolQQBotStatus] = Field(default_factory=list)
    onebot_v11_reverse_ws_path: str = "/onebot/v11/ws"
    qq_webhook_path: str = "/qq/webhook"
