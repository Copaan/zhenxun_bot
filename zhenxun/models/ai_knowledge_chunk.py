from tortoise import fields

from zhenxun.services.db_context import Model


class AIKnowledgeChunk(Model):
    id = fields.IntField(pk=True, generated=True, auto_increment=True)
    """自增id"""
    kb_id = fields.IntField(db_index=True, description="所属知识库")
    """所属知识库"""
    doc_id = fields.IntField(db_index=True, description="所属文档")
    """所属文档"""
    seq = fields.IntField(default=0, description="序号")
    """在文档中的序号"""
    content = fields.TextField(description="内容")
    """分块内容，图片为描述"""
    tokens = fields.TextField(default="", description="分词结果")
    """用空格连接的分词结果，供全文检索"""
    vector = fields.BinaryField(null=True, description="文本向量")
    """float32 文本向量"""
    image_vector = fields.BinaryField(null=True, description="图片向量")
    """float32 图片向量，embedding 模型支持图片时才有"""
    image_path = fields.CharField(255, null=True, description="图片路径")
    """图片相对 data 目录的路径"""

    class Meta:  # pyright: ignore [reportIncompatibleVariableOverride]
        table = "ai_knowledge_chunk"
        table_description = "AI 知识库分块"
