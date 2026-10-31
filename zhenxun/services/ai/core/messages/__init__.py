"""
消息与响应域类型定义 - 统一导出门面
"""

from zhenxun.utils.pydantic_compat import model_rebuild

from .models import (
    AssistantMessage,
    LLMMessage,
    SystemMessage,
    ToolMessage,
    UserMessage,
)
from .parts import (
    AudioPart,
    BaseContentPart,
    EmbedBatch,
    EmbedPayload,
    FilePart,
    ImagePart,
    LLMContentPart,
    TextPart,
    ThoughtPart,
    ToolCallPart,
    ToolReturnPart,
    VideoPart,
)
from .requests import (
    BaseRequest,
    ChatRequest,
    EmbeddingRequest,
    ImageRequest,
    RerankRequest,
    SpeechRequest,
    TranscriptionRequest,
)
from .responses import (
    AudioResponse,
    ChatResponse,
    EmbeddingResponse,
    ImageResponse,
    RerankResponse,
    TranscriptionResponse,
)
from .shared import (
    LLMGroundingAttribution,
    LLMGroundingMetadata,
    RerankDocument,
    RerankResult,
    UsageInfo,
)
from .types import (
    ContentT,
    PromptInput,
    RoleT,
    UserContentUnion,
)

model_rebuild(ChatResponse)
model_rebuild(LLMMessage)
model_rebuild(SystemMessage)
model_rebuild(UserMessage)
model_rebuild(AssistantMessage)
model_rebuild(ToolMessage)
model_rebuild(RerankResponse)


__all__ = [
    "AssistantMessage",
    "AudioPart",
    "AudioResponse",
    "BaseContentPart",
    "BaseRequest",
    "ChatRequest",
    "ChatResponse",
    "ContentT",
    "EmbedBatch",
    "EmbedPayload",
    "EmbeddingRequest",
    "EmbeddingResponse",
    "FilePart",
    "ImagePart",
    "ImageRequest",
    "ImageResponse",
    "LLMContentPart",
    "LLMGroundingAttribution",
    "LLMGroundingMetadata",
    "LLMMessage",
    "PromptInput",
    "RerankDocument",
    "RerankRequest",
    "RerankResponse",
    "RerankResult",
    "RoleT",
    "SpeechRequest",
    "SystemMessage",
    "TextPart",
    "ThoughtPart",
    "ToolCallPart",
    "ToolMessage",
    "ToolReturnPart",
    "TranscriptionRequest",
    "TranscriptionResponse",
    "UsageInfo",
    "UserContentUnion",
    "UserMessage",
    "VideoPart",
]
