from tortoise import fields

from zhenxun.services.db_context import Model


class AIStickerBlock(Model):
    id = fields.IntField(pk=True, generated=True, auto_increment=True)
    """自增id"""
    md5 = fields.CharField(32, unique=True)
    """不再收集的图片 md5"""
    created_at = fields.DatetimeField(auto_now_add=True)
    """加入时间"""

    class Meta:  # pyright: ignore [reportIncompatibleVariableOverride]
        table = "ai_sticker_block"
        table_description = "AI 聊天不再收集的表情"
