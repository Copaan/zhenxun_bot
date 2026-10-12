"""Durable evidence for account operations and fee reservations."""

from datetime import datetime, timedelta, timezone

from tortoise import fields
from tortoise.expressions import Q

from zhenxun.services.db_context import Model


class AssetOperation(Model):
    id = fields.CharField(64, pk=True)
    user_id = fields.CharField(255, index=True)
    event_id = fields.CharField(64, null=True, index=True)
    kind = fields.CharField(64)
    state = fields.CharField(32)
    payload = fields.JSONField(default=dict)
    create_time = fields.DatetimeField(auto_now_add=True)
    update_time = fields.DatetimeField(auto_now=True)

    class Meta:
        table = "asset_operations"

    @classmethod
    async def clear_expired(cls, days: int = 7) -> int:
        """清理已结束超过指定天数的金币预扣、每日签到及旧版事件回执

        参数:
            days: 保留天数

        返回:
            int: 删除数量
        """
        return await cls.filter(
            Q(kind__in=["fee", "daily_sign"]) | Q(event_id__isnull=False),
            state__in=["committed", "released"],
            update_time__lt=datetime.now(timezone.utc) - timedelta(days=days),
        ).delete()

    @classmethod
    async def _run_script(cls):
        from zhenxun.services.db_context.schema_ops import AddColumn, CreateIndex

        return [
            AddColumn("asset_operations", "event_id", "VARCHAR(64)"),
            CreateIndex("asset_operations", ["event_id"], name="zx_asset_event"),
        ]
