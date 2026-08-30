from typing import Literal

from pydantic import BaseModel


class ProtocolConnection(BaseModel):
    self_id: str
    adapter: str
    platform: Literal["onebot_v11", "qq_official", "other"]


class ProtocolStatus(BaseModel):
    onebot_v11_connected: bool
    qq_official_enabled: bool
    qq_official_connected: bool
    qq_webhook_mode: Literal["external", "builtin_https"]
    connections: list[ProtocolConnection]
    onebot_v11_reverse_ws_path: str = "/onebot/v11/ws"
    qq_webhook_path: str = "/qq/webhook"
