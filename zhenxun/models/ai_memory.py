from typing import ClassVar

from tortoise import fields

from zhenxun.services.db_context import Model


class AIMemory(Model):
    id = fields.IntField(pk=True, generated=True, auto_increment=True)
    """自增id"""
    group_id = fields.CharField(255, null=True, description="所属群")
    """所属群，私聊记忆为空"""
    user_id = fields.CharField(255, null=True, description="所属私聊用户")
    """私聊记忆所属的用户，群记忆为空"""
    content = fields.TextField(description="内容")
    """记忆内容"""
    topics = fields.JSONField(default=list, description="话题")
    """话题关键词"""
    facts = fields.JSONField(default=list, description="关键事实")
    """关键事实"""
    participants = fields.JSONField(default=list, description="参与者")
    """参与者，写成“昵称(id)”"""
    importance = fields.FloatField(default=0.5, description="重要度")
    """重要度 0~1，自动总结的记忆每天衰减"""
    access_count = fields.IntField(default=0, description="召回次数")
    """被召回的次数"""
    last_access_at = fields.DatetimeField(null=True, description="最近召回时间")
    """最近被召回的时间"""
    tokens = fields.TextField(default="", description="分词结果")
    """用空格连接的分词结果，供全文检索"""
    vector = fields.BinaryField(null=True, description="向量")
    """float32 向量"""
    source = fields.CharField(16, default="auto", description="来源")
    """来源：auto 自动总结，tool AI 记下，webui 手动添加"""
    archived = fields.BooleanField(default=False, description="是否归档")
    """归档的记忆不再召回"""
    created_at = fields.DatetimeField(auto_now_add=True, description="创建时间")
    """创建时间"""
    updated_at = fields.DatetimeField(auto_now=True, description="修改时间")
    """修改时间"""

    class Meta:  # pyright: ignore [reportIncompatibleVariableOverride]
        table = "ai_memory"
        table_description = "AI 长期记忆"
        indexes: ClassVar = [("group_id", "user_id")]
