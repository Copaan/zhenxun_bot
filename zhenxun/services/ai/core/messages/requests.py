from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from zhenxun.services.ai.core.models import ToolChoice
from zhenxun.services.ai.core.options import (
    GenerationConfig,
    LLMEmbeddingConfig,
    TTSConfig,
)

from .models import LLMMessage
from .parts import EmbedBatch


class BaseRequest(BaseModel):
    """基础请求 DTO"""

    timeout: float | None = Field(default=None)
    extra: dict[str, Any] = Field(default_factory=dict)

    model_config = ConfigDict(arbitrary_types_allowed=True)


class ChatRequest(BaseRequest):
    """对话生成请求 DTO"""

    messages: list[LLMMessage]
    config: GenerationConfig | None = None
    tools: list[Any] | None = None
    tool_choice: str | dict[str, Any] | ToolChoice | None = None


class EmbeddingRequest(BaseRequest):
    """向量嵌入请求 DTO"""

    batch: EmbedBatch
    """向量嵌入的批次载体"""
    config: LLMEmbeddingConfig | None = None
    """向量嵌入配置"""


class ImageRequest(BaseRequest):
    """图像生成请求 DTO"""

    prompt: str
    """图像生成提示词"""
    images: list[Any] | None = None
    """输入参考图像列表"""
    config: GenerationConfig | None = None
    """图像生成配置"""


class SpeechRequest(BaseRequest):
    """语音合成请求 DTO"""

    input_text: str
    """待合成的文本内容"""
    voice: str | None = None
    """发音人/音色标识 (快捷覆盖参数，为空则使用模型默认音色)"""
    config: TTSConfig | None = None
    """语音合成配置"""


class RerankRequest(BaseRequest):
    """文本重排请求 DTO"""

    query: str
    """检索查询词"""
    documents: list[str | dict[str, str]]
    """待排序的候选文档列表"""
    top_n: int = 3
    """返回的最相关文档数量"""


__all__ = [
    "BaseRequest",
    "ChatRequest",
    "EmbeddingRequest",
    "ImageRequest",
    "RerankRequest",
    "SpeechRequest",
]
