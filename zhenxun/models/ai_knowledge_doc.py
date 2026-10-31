from tortoise import fields

from zhenxun.services.db_context import Model


class AIKnowledgeDoc(Model):
    id = fields.IntField(pk=True, generated=True, auto_increment=True)
    """自增id"""
    kb_id = fields.IntField(db_index=True, description="所属知识库")
    """所属知识库"""
    filename = fields.CharField(255, description="文件名")
    """文件名"""
    kind = fields.CharField(16, description="文件类型")
    """文件类型（扩展名，图片为 image）"""
    size = fields.IntField(default=0, description="文件大小")
    """文件大小（字节）"""
    chunks = fields.IntField(default=0, description="分块数")
    """分块数"""
    status = fields.CharField(16, default="pending", description="状态")
    """状态：pending 等待，processing 处理中，ready 完成，failed 失败"""
    error = fields.TextField(null=True, description="错误信息")
    """失败时的错误信息"""
    created_at = fields.DatetimeField(auto_now_add=True, description="创建时间")
    """创建时间"""
    updated_at = fields.DatetimeField(auto_now=True, description="更新时间")
    """更新时间"""

    class Meta:  # pyright: ignore [reportIncompatibleVariableOverride]
        table = "ai_knowledge_doc"
        table_description = "AI 知识库文档"
