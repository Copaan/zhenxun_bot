"""Additive identity links; existing business rows and their keys stay intact."""

from tortoise import fields

from zhenxun.services.db_context import Model


class BusinessIdentityLink(Model):
    id = fields.CharField(64, pk=True)
    domain = fields.CharField(64)
    app_id = fields.CharField(128, default="")
    scene = fields.CharField(32)
    group_scope = fields.CharField(128, default="")
    subject_digest = fields.CharField(64)
    principal_id = fields.CharField(36, null=True, index=True)
    original_account = fields.ForeignKeyField(
        "models.UserConsole",
        related_name="original_identities",
        on_delete=fields.RESTRICT,
    )
    account = fields.ForeignKeyField(
        "models.UserConsole",
        related_name="business_identities",
        on_delete=fields.RESTRICT,
    )
    verified = fields.BooleanField(default=False)
    revision = fields.IntField(default=0)
    bound_at = fields.DatetimeField(null=True)
    created_at = fields.DatetimeField(auto_now_add=True)

    class Meta:
        table = "business_identity_links"


class AccountBindingRequest(Model):
    id = fields.CharField(64, pk=True)
    identity = fields.ForeignKeyField(
        "models.BusinessIdentityLink",
        related_name="requests",
        on_delete=fields.RESTRICT,
    )
    code_digest = fields.CharField(64, unique=True)
    target_qq = fields.CharField(32)
    initiator_id = fields.CharField(64)
    action = fields.CharField(16, default="bind")
    state = fields.CharField(16, default="pending")
    revision = fields.IntField()
    expires_at = fields.DatetimeField()
    result = fields.JSONField(default=dict)
    created_at = fields.DatetimeField(auto_now_add=True)

    class Meta:
        table = "account_binding_requests"
        indexes = (("identity_id", "state"),)


class AccountBindingChange(Model):
    id = fields.CharField(64, pk=True)
    identity = fields.ForeignKeyField(
        "models.BusinessIdentityLink", related_name="changes", on_delete=fields.RESTRICT
    )
    action = fields.CharField(16)
    previous_account_id = fields.IntField()
    account_id = fields.IntField()
    revision = fields.IntField()
    evidence = fields.JSONField(default=dict)
    created_at = fields.DatetimeField(auto_now_add=True)

    class Meta:
        table = "account_binding_changes"


class BusinessEventAccount(Model):
    """Pin the account before the first business operation, including after restart."""

    id = fields.CharField(64, pk=True)
    event_id = fields.CharField(64, index=True)
    identity = fields.ForeignKeyField(
        "models.BusinessIdentityLink", related_name="events", on_delete=fields.RESTRICT
    )
    account = fields.ForeignKeyField(
        "models.UserConsole", related_name="business_events", on_delete=fields.RESTRICT
    )
    revision = fields.IntField()
    created_at = fields.DatetimeField(auto_now_add=True)

    class Meta:
        table = "business_event_accounts"


class BusinessDailyClaim(Model):
    """An identity cannot collect a second daily reward by changing accounts."""

    id = fields.CharField(64, pk=True)
    identity = fields.ForeignKeyField(
        "models.BusinessIdentityLink",
        related_name="daily_claims",
        on_delete=fields.RESTRICT,
    )
    day = fields.DateField()
    account_id = fields.IntField()

    class Meta:
        table = "business_daily_claims"
        unique_together = (("identity_id", "day"),)
