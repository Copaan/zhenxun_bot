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


class BotInfo(BaseModel):
    self_id: str
    """SELF ID"""
    nickname: str
    """昵称"""
    ava_url: str
    """头像url"""
    platform: str
    """平台"""
    friend_count: int = 0
    """好友数量"""
    group_count: int = 0
    """群聊数量"""
    received_messages: int = 0
    """今日消息接收"""
    day_call: int = 0
    """今日调用插件次数"""
    connect_time: int = 0
    """连接时间"""
    connect_date: str | None = None
    """连接日期"""


class QueryChatCallCount(BaseModel):
    """
    查询聊天/调用记录次数
    """

    chat_num: int
    """聊天记录总数"""
    chat_day: int
    """今日消息"""
    call_num: int
    """调用记录总数"""
    call_day: int
    """今日调用"""


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


class AllChatAndCallCount(BaseModel):
    """
    查询聊天/调用记录次数
    """

    chat_week: int
    """一周内聊天次数"""
    chat_month: int
    """一月内聊天次数"""
    chat_year: int
    """一年内聊天次数"""
    call_week: int
    """一周内调用次数"""
    call_month: int
    """一月内调用次数"""
    call_year: int
    """一年内调用次数"""


RuntimeLevel = Literal["ok", "warning", "critical"]


class RuntimeServiceStatus(BaseModel):
    status: RuntimeLevel
    code: str
    label: str
    detail: str
    latency_ms: int | None = None
    mode: str | None = None


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
