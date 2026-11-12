from tortoise import fields

from zhenxun.services.db_context import Model


class AIMemoryCursor(Model):
    id = fields.IntField(pk=True, generated=True, auto_increment=True)
    """自增id"""
    session_id = fields.CharField(255, unique=True, description="会话id")
    """聊天会话id"""
    message_id = fields.BigIntField(default=0, description="已整理到的消息")
    """已整理成长期记忆的最后一条短期记忆消息id"""
    revision = fields.IntField(default=0, description="整理修订")
    state = fields.JSONField(null=True, default=dict, description="区段进度与重试状态")
    updated_at = fields.DatetimeField(auto_now=True, description="修改时间")
    """修改时间"""

    class Meta:  # pyright: ignore [reportIncompatibleVariableOverride]
        table = "ai_memory_cursor"
        table_description = "AI 长期记忆整理进度"
