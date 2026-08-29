from tortoise import fields

from zhenxun.services.db_context import Model


class QQOfficialPrincipal(Model):
    id = fields.UUIDField(pk=True)
    created_at = fields.DatetimeField(auto_now_add=True)
    updated_at = fields.DatetimeField(auto_now=True)

    class Meta:  # pyright: ignore[reportIncompatibleVariableOverride]
        table = "qq_official_principals"


class QQOfficialIdentity(Model):
    id = fields.BigIntField(pk=True, generated=True)
    principal = fields.ForeignKeyField(
        "models.QQOfficialPrincipal",
        related_name="identities",
        on_delete=fields.CASCADE,
    )
    app_id = fields.CharField(max_length=64, index=True)
    scene = fields.CharField(max_length=24)
    group_scope = fields.CharField(max_length=128, default="")
    openid_digest = fields.CharField(max_length=64)
    union_openid_digest = fields.CharField(max_length=64, default="")
    created_at = fields.DatetimeField(auto_now_add=True)
    last_seen_at = fields.DatetimeField(auto_now=True)

    class Meta:  # pyright: ignore[reportIncompatibleVariableOverride]
        table = "qq_official_identities"
        unique_together = (("app_id", "scene", "group_scope", "openid_digest"),)
        indexes = (("principal_id",), ("app_id", "union_openid_digest"))


class QQWebhookReceipt(Model):
    id = fields.BigIntField(pk=True, generated=True)
    app_id = fields.CharField(max_length=64)
    event_id_digest = fields.CharField(max_length=64)
    event_type = fields.CharField(max_length=96, default="")
    received_at = fields.DatetimeField(auto_now_add=True)
    expires_at = fields.DatetimeField(index=True)

    class Meta:  # pyright: ignore[reportIncompatibleVariableOverride]
        table = "qq_webhook_receipts"
        unique_together = (("app_id", "event_id_digest"),)
        indexes = (("app_id", "expires_at"),)


__all__ = ["QQOfficialIdentity", "QQOfficialPrincipal", "QQWebhookReceipt"]
