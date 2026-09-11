"""Durable evidence for account operations and fee reservations."""

from tortoise import fields

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
    async def _run_script(cls):
        from zhenxun.services.db_context.schema_ops import AddColumn, CreateIndex

        return [
            AddColumn("asset_operations", "event_id", "VARCHAR(64)"),
            CreateIndex("asset_operations", ["event_id"], name="zx_asset_event"),
        ]
