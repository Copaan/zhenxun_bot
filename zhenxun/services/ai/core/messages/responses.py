from __future__ import annotations

import io
from pathlib import Path
from typing import TYPE_CHECKING, Any
import wave

from pydantic import BaseModel, Field

from .parts import (
    AudioPart,
    ImagePart,
    LLMContentPart,
    TextPart,
    ThoughtPart,
    ToolCallPart,
)
from .shared import RerankResult, UsageInfo

if TYPE_CHECKING:
    from nonebot_plugin_alconna import Voice


_AUDIO_MIMETYPES = {"mp3": "audio/mpeg", "opus": "audio/ogg", "aac": "audio/aac"}


class ChatResponse(BaseModel):
    """
    文本/多模态对话响应对象，确立 SSOT (单一数据源) 架构。
    """

    content_parts: list[LLMContentPart] = Field(default_factory=list)
    """经由解析、统一封装后的标准内容部件列表"""
    usage_info: dict[str, Any] | None = None
    """厂商返回的原始用量遥测字段"""
    raw_response: dict[str, Any] | None = None
    """完整的 API 层级原生响应字典 (未经框架清洗)"""
    grounding_metadata: Any | None = None
    """联网搜索、位置等基底事实归因元数据"""
    parsed_obj: Any | None = Field(default=None)
    """在结构化输出模式下，由中间件反序列化得出的强类型 Pydantic 对象实例"""

    @property
    def tool_calls(self) -> list[ToolCallPart]:
        return [p for p in self.content_parts if isinstance(p, ToolCallPart)]

    @property
    def text(self) -> str:
        """动态视图：提取并拼接所有文本块"""
        return "".join(
            p.text for p in self.content_parts if isinstance(p, TextPart)
        ).strip()

    @property
    def thought_text(self) -> str | None:
        """动态视图：提取并拼接所有思考/推理块"""
        thoughts = [
            p.thought_text for p in self.content_parts if isinstance(p, ThoughtPart)
        ]
        return "\n".join(thoughts).strip() if thoughts else None

    @property
    def images(self) -> list[bytes | Path | str]:
        """动态视图：提取响应中包含的所有图片数据"""
        imgs = []
        for p in self.content_parts:
            if isinstance(p, ImagePart):
                if p.url:
                    imgs.append(p.url)
                elif p.raw:
                    imgs.append(p.raw)
                elif p.path:
                    imgs.append(p.path)
        return imgs

    @property
    def audios(self) -> list[bytes | Path | str]:
        """动态视图：提取音频数据"""
        audios = []
        for p in self.content_parts:
            if isinstance(p, AudioPart):
                if p.url:
                    audios.append(p.url)
                elif p.raw:
                    audios.append(p.raw)
                elif p.path:
                    audios.append(p.path)
        return audios


class EmbeddingResponse(BaseModel):
    """Embedding 向量富响应对象"""

    embeddings: list[list[float]]
    """多段输入文本对应生成的高维浮点数向量数组 (二维)"""
    usage: UsageInfo = Field(default_factory=UsageInfo)
    """执行向量化任务产生的 Token 用量开销统计"""
    model_name: str
    """实际用于执行本次编码任务的模型名称"""

    @property
    def vector(self) -> list[float]:
        """便捷属性：当且仅当只需提取单一文本向量时，直接返回一维向量"""
        return self.embeddings[0] if self.embeddings else []


class AudioResponse(BaseModel):
    """统一的语音合成响应对象"""

    audio_bytes: bytes
    """生成的音频二进制裸数据，可直接用于发送或保存"""
    audio_format: str
    """实际返回的音频格式 (如 mp3, wav)"""
    usage: UsageInfo = Field(default_factory=UsageInfo)
    """Token 或 字符数消耗统计"""
    raw_response: Any | None = None
    """原生响应体，供高级调试使用"""
    model_name: str
    """实际执行任务的模型名称"""

    def to_voice(self) -> Voice:
        """转为可直接发送的语音消息段，PCM 数据先封装为 WAV

        返回:
            Voice: 语音消息段

        示例:
            res = await create_speech("你好")
            await MessageUtils.build_message(res.to_voice()).send()
        """
        from nonebot_plugin_alconna import Voice

        audio, audio_format = self.audio_bytes, self.audio_format.lower()
        if audio_format == "pcm":
            buffer = io.BytesIO()
            with wave.open(buffer, "wb") as writer:
                writer.setnchannels(1)
                writer.setsampwidth(2)
                writer.setframerate(24000)
                writer.writeframes(audio)
            audio, audio_format = buffer.getvalue(), "wav"
        mimetype = _AUDIO_MIMETYPES.get(audio_format, f"audio/{audio_format}")
        return Voice(raw=audio, mimetype=mimetype, name=f"speech.{audio_format}")


class TranscriptionResponse(BaseModel):
    """语音转文字响应对象"""

    text: str
    """识别出的文字"""
    usage: UsageInfo = Field(default_factory=UsageInfo)
    """Token 或时长消耗统计"""
    raw_response: Any | None = None
    """原生响应体，供高级调试使用"""
    model_name: str
    """实际执行任务的模型名称"""


class ImageResponse(BaseModel):
    """图像生成响应对象"""

    content_parts: list[LLMContentPart] = Field(default_factory=list)
    raw_response: dict[str, Any] | None = None

    @property
    def images(self) -> list[bytes | Path | str]:
        """动态视图：提取响应中包含的所有图片数据"""
        imgs = []
        for p in self.content_parts:
            if isinstance(p, ImagePart):
                if p.url:
                    imgs.append(p.url)
                elif p.raw:
                    imgs.append(p.raw)
                elif p.path:
                    imgs.append(p.path)
        return imgs

    @property
    def text(self) -> str:
        """动态视图：提取并拼接所有 TextPart 文本内容"""
        return "".join(
            p.text for p in self.content_parts if isinstance(p, TextPart)
        ).strip()


class RerankResponse(BaseModel):
    """文本重排响应对象"""

    results: list[RerankResult]
    """重排后的文档结果列表"""


__all__ = [
    "AudioResponse",
    "ChatResponse",
    "EmbeddingResponse",
    "ImageResponse",
    "RerankResponse",
    "TranscriptionResponse",
]
