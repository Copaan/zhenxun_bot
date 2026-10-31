from tortoise import fields

from zhenxun.services.db_context import Model


class AIKnowledgeBase(Model):
    id = fields.IntField(pk=True, generated=True, auto_increment=True)
    """自增id"""
    name = fields.CharField(64, unique=True, description="知识库名称")
    """知识库名称"""
    description = fields.TextField(default="", description="说明")
    """说明，会提供给模型判断何时检索"""
    embedding_model = fields.CharField(255, null=True, description="embedding 模型")
    """embedding 模型，为空使用默认模型"""
    dimensions = fields.IntField(null=True, description="向量维度")
    """请求的向量维度，为空使用模型默认维度"""
    chunk_size = fields.IntField(default=500, description="分块大小")
    """分块大小（字）"""
    chunk_overlap = fields.IntField(default=50, description="分块重叠")
    """相邻分块重叠的字数"""
    top_k = fields.IntField(default=5, description="检索条数")
    """每次检索返回的条数"""
    created_at = fields.DatetimeField(auto_now_add=True, description="创建时间")
    """创建时间"""
    updated_at = fields.DatetimeField(auto_now=True, description="更新时间")
    """更新时间"""

    class Meta:  # pyright: ignore [reportIncompatibleVariableOverride]
        table = "ai_knowledge_base"
        table_description = "AI 知识库"
