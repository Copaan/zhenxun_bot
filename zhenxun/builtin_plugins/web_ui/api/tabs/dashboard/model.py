from datetime import datetime
from typing import Literal

from pydantic import BaseModel


class BotConnectLogInfo(BaseModel):
    bot_id: str
    """机器人ID"""
    connect_time: datetime
    """连接日期"""
    type: int
    """连接类型"""


class ChatCallMonthCount(BaseModel):
    """
    查询聊天/调用一个月记录次数
    """

    chat: list[int]
    """一个月内聊天总数"""
    call: list[int]
    """一个月内调用数据"""
    date: list[str]
    """日期"""


RuntimeLevel = Literal["ok", "warning", "critical"]


class RuntimeServiceStatus(BaseModel):
    status: RuntimeLevel
    code: str
    label: str
    detail: str
    latency_ms: int | None = None
    mode: str | None = None
    configured_mode: str | None = None
    actual_mode: str | None = None
    application_status: str | None = None
    checked_at: str | None = None


class RuntimeIssue(BaseModel):
    code: str
    severity: Literal["warning", "critical"]
    title: str
    detail: str
    action_label: str
    action_route: str


class RuntimeProcessStatus(BaseModel):
    version: str
    uptime_seconds: int
    restart_pending: bool
    listen_host: str
    listen_port: int
    log_level: str


class RuntimeBotStatus(BaseModel):
    self_id: str
    platform: str
    adapter: str
    connect_seconds: int = 0


class RuntimeProtocolStatus(BaseModel):
    onebot_v11_connected: bool
    qq_official_enabled: bool
    qq_official_connected: bool
    qq_webhook_mode: Literal["external", "builtin_https"]
    connection_count: int


class RuntimeWebSocketStatus(BaseModel):
    status: RuntimeLevel
    active_connections: int
    connection_limit: int


class RuntimeOverview(BaseModel):
    generated_at: datetime
    overall_status: RuntimeLevel
    process: RuntimeProcessStatus
    database: RuntimeServiceStatus
    cache: RuntimeServiceStatus
    websocket: RuntimeWebSocketStatus
    protocols: RuntimeProtocolStatus
    bots: list[RuntimeBotStatus]
    issues: list[RuntimeIssue]
