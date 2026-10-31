from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator

from zhenxun.services.ai.core.models import ToolChoice
from zhenxun.services.ai.core.options import (
    GenerationConfig,
    LLMEmbeddingConfig,
    TTSConfig,
)

from .models import (
    AssistantMessage,
    LLMMessage,
    SystemMessage,
    ToolMessage,
    UserMessage,
)
from .parts import AudioPart, EmbedBatch


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

    @field_validator("messages")
    @classmethod
    def _typed_messages(cls, messages: list[LLMMessage]) -> list[LLMMessage]:
        """把按 role 构造的通用消息转为对应的消息类型，各协议按类型编码"""
        return [_typed(m) for m in messages]


_ROLE_TYPES: dict[str, Any] = {
    "system": SystemMessage,
    "user": UserMessage,
    "assistant": AssistantMessage,
    "tool": ToolMessage,
}


def _typed(message: LLMMessage) -> LLMMessage:
    """通用消息转为其 role 对应的消息类型，无法转换时原样返回"""
    target = _ROLE_TYPES.get(str(message.role))
    if target is None or isinstance(message, target):
        return message
    try:
        return target(
            content=message.content,
            created_at=message.created_at,
            metadata=message.metadata,
        )
    except ValueError:
        return message


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


class TranscriptionRequest(BaseRequest):
    """语音转文字请求 DTO"""

    audio: AudioPart
    """待识别的音频"""
    language: str | None = None
    """音频语言 (ISO-639-1，如 zh)，为空时由模型自动判断"""
    prompt: str | None = None
    """提示文本，用于提示专有名词或衔接上文"""


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
    "TranscriptionRequest",
]
