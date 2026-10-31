from typing import Any, ClassVar
from typing_extensions import Self

from tortoise import fields

from zhenxun.services.db_context import Model


class AIChatGroup(Model):
    id = fields.IntField(pk=True, generated=True, auto_increment=True)
    """自增id"""
    group_id = fields.CharField(255, unique=True, description="群号")
    """群号"""
    active_probability = fields.FloatField(null=True, description="主动回复概率")
    """主动回复概率，为空使用全局值"""
    wake_words = fields.JSONField(null=True, description="唤醒词")
    """唤醒词，为空使用全局值"""
    model = fields.CharField(255, null=True, description="对话模型")
    """对话模型，为空使用人设或默认模型"""
    voice_reply = fields.CharField(16, null=True, description="语音回复")
    """语音回复：off、voice、always，为空使用全局值"""
    disabled = fields.BooleanField(null=True, description="停用 AI 聊天")
    """为真时该群不触发 AI 聊天"""
    knowledge_bases = fields.JSONField(null=True, description="知识库")
    """使用的知识库 id，为空使用全局设置，空列表为不使用"""
    proactive = fields.JSONField(null=True, description="主动对话设置")
    """主动插话设置（冷却、每日预算），为空使用全局值"""
    agent_runner = fields.CharField(16, null=True, description="执行器")
    """执行器：local、dsh，为空使用全局 AGENT_RUNNER"""
    updated_at = fields.DatetimeField(auto_now=True, description="更新时间")
    """更新时间"""

    _settings: ClassVar[dict[str, "AIChatGroup"] | None] = None
    OVERRIDES: ClassVar[tuple[str, ...]] = (
        "active_probability",
        "wake_words",
        "model",
        "voice_reply",
        "disabled",
        "knowledge_bases",
        "proactive",
        "agent_runner",
    )

    class Meta:  # pyright: ignore [reportIncompatibleVariableOverride]
        table = "ai_chat_group"
        table_description = "AI 聊天按群设置"

    @classmethod
    def _run_script(cls):
        return ["DROP TABLE IF EXISTS ai_group_memory"]

    @classmethod
    async def get_setting(cls, group_id: str) -> Self | None:
        """获取群的 AI 聊天设置，首次读取时整表载入内存

        参数:
            group_id: 群号

        返回:
            Self | None: 设置，没有时为空
        """
        if cls._settings is None:
            cls._settings = {row.group_id: row for row in await cls.all()}
        return cls._settings.get(group_id)  # type: ignore[return-value]

    @classmethod
    async def save_setting(cls, group_id: str, **values: Any) -> Self | None:
        """保存群的部分 AI 聊天设置，保存后各项都为空时删除该群设置

        参数:
            group_id: 群号
            **values: 覆盖项（active_probability、wake_words、model、voice_reply、
                disabled、knowledge_bases、proactive、agent_runner），为空使用全局值

        返回:
            Self | None: 保存后的设置，删除时为空
        """
        cls._settings = None
        setting, _ = await cls.update_or_create(group_id=group_id, defaults=values)
        if all(getattr(setting, name) is None for name in cls.OVERRIDES):
            await setting.delete()
            return None
        return setting
