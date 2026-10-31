from datetime import date, timedelta

from tortoise import fields
from tortoise.exceptions import IntegrityError
from tortoise.expressions import F

from zhenxun.services.db_context import Model


class AITokenUsage(Model):
    id = fields.BigIntField(pk=True, generated=True, auto_increment=True)
    """自增id"""
    user_id = fields.CharField(255, description="用户id")
    """用户id"""
    day = fields.DateField(description="日期")
    """日期"""
    tokens = fields.BigIntField(default=0, description="已用 token")
    """已用 token"""

    class Meta:  # pyright: ignore [reportIncompatibleVariableOverride]
        table = "ai_token_usage"
        table_description = "AI 聊天每日 token 用量"
        unique_together = (("user_id", "day"),)

    @classmethod
    async def used_today(cls, user_id: str) -> int:
        """用户今日已用 token

        参数:
            user_id: 用户id

        返回:
            int: 已用 token
        """
        record = await cls.get_or_none(user_id=user_id, day=date.today())
        return record.tokens if record else 0

    @classmethod
    async def add_usage(cls, user_id: str, tokens: int) -> None:
        """累加用户今日 token 用量

        参数:
            user_id: 用户id
            tokens: 本次用量
        """
        if tokens <= 0:
            return
        today = date.today()
        increment = F("tokens") + tokens
        if await cls.filter(user_id=user_id, day=today).update(tokens=increment):
            return
        try:
            await cls.create(user_id=user_id, day=today, tokens=tokens)
        except IntegrityError:
            await cls.filter(user_id=user_id, day=today).update(tokens=increment)

    @classmethod
    async def cleanup(cls, keep_days: int = 30) -> int:
        """删除过期的用量记录

        参数:
            keep_days: 保留天数

        返回:
            int: 删除条数
        """
        cutoff = date.today() - timedelta(days=keep_days)
        return await cls.filter(day__lt=cutoff).delete()
