from datetime import datetime, timedelta, timezone
from typing import ClassVar

from tortoise import fields
from tortoise.exceptions import IntegrityError
from tortoise.expressions import F
from tortoise.timezone import localtime

from zhenxun.services.db_context import Model

COUNTERS = ("calls", "failures", "latency", "prompt_tokens", "completion_tokens")
"""按小时累计的计数字段"""


class AIModelStat(Model):
    id = fields.BigIntField(pk=True, generated=True, auto_increment=True)
    """自增id"""
    hour = fields.DatetimeField(description="统计小时")
    """统计小时（UTC 整点）"""
    model = fields.CharField(255, description="模型线路")
    """模型线路，提供商/模型"""
    task = fields.CharField(32, description="任务类型")
    """任务类型：chat、embedding、tts、image 等"""
    calls = fields.IntField(default=0, description="调用次数")
    """调用次数"""
    failures = fields.IntField(default=0, description="失败次数")
    """失败次数"""
    latency = fields.BigIntField(default=0, description="总耗时（毫秒）")
    """总耗时（毫秒）"""
    prompt_tokens = fields.BigIntField(default=0, description="输入 token")
    """输入 token"""
    completion_tokens = fields.BigIntField(default=0, description="输出 token")
    """输出 token"""

    class Meta:  # pyright: ignore [reportIncompatibleVariableOverride]
        table = "ai_model_stat"
        table_description = "AI 模型调用统计（按小时）"
        unique_together: ClassVar = [("hour", "model", "task")]

    @classmethod
    async def add(cls, rows: dict[tuple[datetime, str, str], list[int]]) -> None:
        """累加统计，小时按数据库时区规范化后匹配已有行

        参数:
            rows: (小时, 模型线路, 任务类型) -> 按 COUNTERS 顺序的增量
        """
        for (hour, model, task), values in rows.items():
            hour = localtime(hour)
            increments = dict(zip(COUNTERS, values))
            query = cls.filter(hour=hour, model=model, task=task)
            updates = {key: F(key) + value for key, value in increments.items()}
            if await query.update(**updates):
                continue
            try:
                await cls.create(hour=hour, model=model, task=task, **increments)
            except IntegrityError:
                await query.update(**updates)

    @classmethod
    async def cleanup(cls, keep_days: int = 90) -> int:
        """删除过期的统计

        参数:
            keep_days: 保留天数

        返回:
            int: 删除条数
        """
        cutoff = localtime(datetime.now(timezone.utc) - timedelta(days=keep_days))
        return await cls.filter(hour__lt=cutoff).delete()
