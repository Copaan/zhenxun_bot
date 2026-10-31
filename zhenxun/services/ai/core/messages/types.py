from __future__ import annotations

from typing import Any
from typing_extensions import TypeVar

from nonebot_plugin_alconna import UniMessage

from .parts import (
    AudioPart,
    FilePart,
    ImagePart,
    LLMContentPart,
    TextPart,
    VideoPart,
)

UserContentUnion = TextPart | ImagePart | AudioPart | VideoPart | FilePart
"""用户消息允许的多模态内容片段联合类型"""


RoleT = TypeVar("RoleT", default=str, covariant=True)
"""泛型：消息参与者角色类型变量"""

ContentT = TypeVar("ContentT", default=LLMContentPart, covariant=True)
"""泛型：多模态片段数组的元素内容类型变量"""

OutputDataT = TypeVar("OutputDataT", default=str)


from .models import (
    LLMMessage,
)

PromptInput = str | UniMessage | LLMMessage | list[LLMContentPart] | Any
"""支持作为 LLM 输入的提示词对象联合类型，包括纯文本、UniMessage、LLMMessage 消息实体"""


__all__ = [
    "ContentT",
    "LLMContentPart",
    "OutputDataT",
    "PromptInput",
    "RoleT",
    "UserContentUnion",
]
