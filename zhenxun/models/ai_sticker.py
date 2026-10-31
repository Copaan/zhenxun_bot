from tortoise import fields

from zhenxun.services.db_context import Model


class AISticker(Model):
    id = fields.IntField(pk=True, generated=True, auto_increment=True)
    """自增id"""
    md5 = fields.CharField(32, unique=True)
    """图片 md5，用于去重"""
    file = fields.CharField(64)
    """文件名，位于 data/ai_chat/stickers/"""
    remark = fields.CharField(64, default="")
    """备注"""
    tags = fields.JSONField(default=list)
    """标签"""
    usage = fields.CharField(255, default="")
    """适用场合"""
    use_count = fields.IntField(default=0)
    """发送次数"""
    last_used_at = fields.DatetimeField(null=True)
    """最近发送时间"""
    source = fields.CharField(16, default="ai")
    """来源：ai 模型收藏，webui 手动上传"""
    group_id = fields.CharField(255, null=True)
    """收藏时所在的群"""
    user_id = fields.CharField(255, null=True)
    """原图发送者"""
    created_at = fields.DatetimeField(auto_now_add=True)
    """收藏时间"""

    class Meta:  # pyright: ignore [reportIncompatibleVariableOverride]
        table = "ai_sticker"
        table_description = "AI 聊天表情包"
