"""旧版 LLM 导入路径，转发到 zhenxun.services.ai.llm"""

import sys

from zhenxun.services.ai.core import exceptions
from zhenxun.services.ai.llm import (
    AudioResponse,
    ChatResponse,
    LLMContentPart,
    LLMException,
    LLMMessage,
    TTSConfig,
    chat,
    create_image,
    create_speech,
    embed,
    generate,
    generate_structured,
    get_default_model,
    list_available_models,
    rerank,
)

sys.modules[f"{__name__}.types"] = sys.modules["zhenxun.services.ai.core"]
sys.modules[f"{__name__}.types.exceptions"] = exceptions

__all__ = [
    "AudioResponse",
    "ChatResponse",
    "LLMContentPart",
    "LLMException",
    "LLMMessage",
    "TTSConfig",
    "chat",
    "create_image",
    "create_speech",
    "embed",
    "generate",
    "generate_structured",
    "get_default_model",
    "list_available_models",
    "rerank",
]
