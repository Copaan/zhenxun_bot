from typing import ClassVar

from tortoise import fields

from zhenxun.services.db_context import Model


class PluginPolicy(Model):
    id = fields.IntField(pk=True, generated=True, auto_increment=True)
    name = fields.CharField(max_length=80, unique=True, description="策略名称")
    description = fields.CharField(max_length=255, default="", description="策略说明")
    block_plugins: list[str] = fields.JSONField(default=list)  # type: ignore
    block_tasks: list[str] = fields.JSONField(default=list)  # type: ignore
    revision = fields.IntField(default=1, description="策略修订号")
    create_time = fields.DatetimeField(auto_now_add=True)
    update_time = fields.DatetimeField(auto_now=True)

    bindings: fields.ReverseRelation["BotPluginPolicyBinding"]

    class Meta:  # pyright: ignore[reportIncompatibleVariableOverride]
        table = "plugin_policy"
        table_description = "机器人账号插件策略"
        indexes: ClassVar = [("name",)]


class BotPluginPolicyBinding(Model):
    id = fields.IntField(pk=True, generated=True, auto_increment=True)
    bot_id = fields.CharField(max_length=255, unique=True, description="Bot存储身份")
    policy = fields.ForeignKeyField(
        "models.PluginPolicy",
        related_name="bindings",
        on_delete=fields.RESTRICT,
        description="绑定的插件策略",
    )
    create_time = fields.DatetimeField(auto_now_add=True)
    update_time = fields.DatetimeField(auto_now=True)

    class Meta:  # pyright: ignore[reportIncompatibleVariableOverride]
        table = "bot_plugin_policy_binding"
        table_description = "机器人账号插件策略绑定"
        indexes: ClassVar = [("policy_id",)]
