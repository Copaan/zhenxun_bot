from datetime import datetime, timedelta, timezone
from typing import ClassVar

from tortoise import fields
from tortoise.timezone import localtime

from zhenxun.services.db_context import Model


class AIAgentCall(Model):
    id = fields.BigIntField(pk=True, generated=True, auto_increment=True)
    """自增id"""
    run_id = fields.CharField(32, null=True, description="运行id")
    """所属运行的id"""
    session_id = fields.CharField(255, description="会话id")
    """会话id"""
    user_id = fields.CharField(255, description="发起人")
    """发起人"""
    group_id = fields.CharField(255, null=True, description="群组id")
    """群组id"""
    tool = fields.CharField(255, description="工具名称")
    """工具名称"""
    arguments = fields.TextField(description="调用参数")
    """调用参数"""
    decision = fields.CharField(16, description="审批结果")
    """审批结果：auto 自动放行，approved 批准，rejected 拒绝，timeout 超时"""
    approver = fields.CharField(255, null=True, description="处理人")
    """处理人"""
    result = fields.TextField(default="", description="执行结果")
    """执行结果"""
    is_error = fields.BooleanField(default=False, description="是否失败")
    """是否失败"""
    duration = fields.IntField(default=0, description="耗时（毫秒）")
    """耗时（毫秒）"""
    created_at = fields.DatetimeField(auto_now_add=True, description="调用时间")
    """调用时间"""

    class Meta:  # pyright: ignore [reportIncompatibleVariableOverride]
        table = "ai_agent_call"
        table_description = "AI Agent 工具调用记录"
        indexes: ClassVar = [("created_at",)]

    @classmethod
    async def cleanup(cls, keep_days: int = 30) -> int:
        """删除过期的调用记录

        参数:
            keep_days: 保留天数

        返回:
            int: 删除条数
        """
        cutoff = localtime(datetime.now(timezone.utc) - timedelta(days=keep_days))
        return await cls.filter(created_at__lt=cutoff).delete()
