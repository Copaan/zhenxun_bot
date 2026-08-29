from __future__ import annotations

from pydantic import BaseModel, Field, ValidationError, validator


class QQOfficialIntent(BaseModel):
    guilds: bool = False
    guild_members: bool = False
    guild_messages: bool = False
    guild_message_reactions: bool = False
    direct_message: bool = False
    open_forum_event: bool = False
    audio_live_member: bool = False
    c2c_group_at_messages: bool = False
    interaction: bool = False
    message_audit: bool = False
    forum_event: bool = False
    audio_action: bool = False
    at_messages: bool = False

    class Config:
        extra = "forbid"


class QQOfficialBotConfig(BaseModel):
    id: str
    token: str
    secret: str
    use_websocket: bool = False
    intent: QQOfficialIntent = Field(default_factory=QQOfficialIntent)

    @validator("id", "token", "secret")
    def _not_blank(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("must not be blank")
        return value

    class Config:
        extra = "forbid"


class QQOfficialConfig(BaseModel):
    qq_bots: list[QQOfficialBotConfig] = Field(default_factory=list)
    qq_verify_webhook: bool = True


class QQOfficialConfigError(RuntimeError):
    """Raised when explicitly enabled QQ official support is unsafe to start."""


def _validation_paths(error: ValidationError) -> str:
    paths = []
    for item in error.errors():
        location = ".".join(str(part) for part in item.get("loc", ()))
        if location:
            paths.append(location)
    return ", ".join(dict.fromkeys(paths)) or "QQ_BOTS"


def validate_qq_official_config() -> QQOfficialConfig:
    """Validate all QQ settings before adapter imports can create side effects."""
    import nonebot

    try:
        config = nonebot.get_plugin_config(QQOfficialConfig)
    except ValidationError as exc:
        raise QQOfficialConfigError(
            f"QQ 官方适配器配置无效，错误字段: {_validation_paths(exc)}"
        ) from None

    if not config.qq_bots:
        raise QQOfficialConfigError("QQ_ADAPTER_LOAD=True 时 QQ_BOTS 不能为空")
    if not config.qq_verify_webhook:
        raise QQOfficialConfigError("QQ_VERIFY_WEBHOOK 必须保持开启")

    app_ids = [item.id for item in config.qq_bots]
    duplicates = sorted({app_id for app_id in app_ids if app_ids.count(app_id) > 1})
    if duplicates:
        raise QQOfficialConfigError("QQ_BOTS 存在重复 AppID")

    for index, item in enumerate(config.qq_bots):
        if item.use_websocket:
            raise QQOfficialConfigError(
                f"QQ_BOTS.{index}.use_websocket 首期 Webhook 模式必须为 false"
            )
        if not item.intent.c2c_group_at_messages:
            raise QQOfficialConfigError(
                f"QQ_BOTS.{index}.intent.c2c_group_at_messages 必须为 true"
            )
        unsupported_intents = [
            name
            for name, enabled in item.intent.dict().items()
            if name != "c2c_group_at_messages" and enabled
        ]
        if unsupported_intents:
            raise QQOfficialConfigError(
                f"QQ_BOTS.{index}.intent 包含首期不支持的字段: "
                + ", ".join(unsupported_intents)
            )
    return config


__all__ = [
    "QQOfficialBotConfig",
    "QQOfficialConfig",
    "QQOfficialConfigError",
    "QQOfficialIntent",
    "validate_qq_official_config",
]
