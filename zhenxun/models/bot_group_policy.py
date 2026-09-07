from typing import ClassVar

from tortoise import fields

from zhenxun.services.db_context import Model


class BotGroupMembership(Model):
    id = fields.IntField(pk=True)
    bot_id = fields.CharField(max_length=191)
    platform_scope = fields.CharField(max_length=32)
    group_id = fields.CharField(max_length=191)
    channel_id = fields.CharField(max_length=191, default="")
    group_name = fields.CharField(max_length=255, default="")
    source = fields.CharField(max_length=16, default="event")
    last_seen = fields.DatetimeField(auto_now=True)

    class Meta:
        table = "bot_group_membership"
        unique_together: ClassVar = (
            ("bot_id", "platform_scope", "group_id", "channel_id"),
        )


class BotGroupPluginPolicy(Model):
    id = fields.IntField(pk=True)
    bot_id = fields.CharField(max_length=191)
    platform_scope = fields.CharField(max_length=32)
    group_id = fields.CharField(max_length=191)
    channel_id = fields.CharField(max_length=191, default="")
    block_plugins: list[str] = fields.JSONField(default=list)
    block_tasks: list[str] = fields.JSONField(default=list)
    update_time = fields.DatetimeField(auto_now=True)

    class Meta:
        table = "bot_group_plugin_policy"
        unique_together: ClassVar = (
            ("bot_id", "platform_scope", "group_id", "channel_id"),
        )
