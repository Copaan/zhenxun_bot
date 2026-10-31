from typing import ClassVar

from tortoise import fields

from zhenxun.services.db_context import Model


class AIChatMessage(Model):
    id = fields.BigIntField(pk=True, generated=True, auto_increment=True)
    """自增id"""
    session_id = fields.CharField(255)
    """会话id"""
    role = fields.CharField(32)
    """角色"""
    content = fields.JSONField()
    """消息内容，媒体只存地址"""
    metadata = fields.JSONField(null=True)
    """消息元数据"""
    is_summary = fields.BooleanField(default=False)
    """是否为对话摘要"""
    created_at = fields.DatetimeField()
    """创建时间"""

    class Meta:  # pyright: ignore [reportIncompatibleVariableOverride]
        table = "ai_chat_message"
        table_description = "AI 短期对话记忆"
        indexes: ClassVar = [("session_id", "is_summary")]
