"""
AI 模块配置数据域类型定义
"""

from __future__ import annotations

from typing import Any, Generic, Literal, TypeVar

from pydantic import BaseModel, ConfigDict, Field, field_validator

from zhenxun.utils.pydantic_compat import model_copy, model_dump, model_validate

T = TypeVar("T")


class BaseOutputDefinition(Generic[T]):
    """声明式结构化输出基类"""

    type_: type[T]


class ToolOutput(BaseOutputDefinition[T]):
    """工具输出标记：使用强制工具调用 (Tool Call) 结束任务并返回指定结构"""

    name: str | None = None
    description: str | None = None
    strict: bool | None = None

    def __init__(
        self,
        type_: type[T],
        name: str | None = None,
        description: str | None = None,
        strict: bool | None = None,
    ):
        self.type_ = type_
        self.name = name
        self.description = description
        self.strict = strict


def deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    """深度合并两个字典，override 中的值优先

    参数:
        base: 基础字典
        override: 覆盖字典

    返回:
        dict[str, Any]: 合并后的新字典
    """
    result = dict(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(result.get(key), dict):
            result[key] = deep_merge(result[key], value)
        else:
            result[key] = value
    return result


class CommonLLMConfig(BaseModel):
    """跨协议通用的生成参数"""

    temperature: float | None = Field(default=None, ge=0.0, le=2.0)
    """采样温度。较高的值会使输出更加随机，较低的值会使其更加集中和确定。"""
    max_tokens: int | None = Field(default=None, gt=0)
    """单次生成的最大 Token 数。"""
    top_p: float | None = Field(default=None, ge=0.0, le=1.0)
    """核采样 (Nucleus sampling) 概率阈值。"""
    top_k: int | None = Field(default=None, gt=0)
    """仅从概率最高的前 K 个 Token 中采样 (并非所有模型支持)。"""
    frequency_penalty: float | None = Field(default=None, ge=-2.0, le=2.0)
    """频率惩罚，降低模型逐字重复同一行的可能性。"""
    presence_penalty: float | None = Field(default=None, ge=-2.0, le=2.0)
    """存在惩罚，增加模型谈论新主题的可能性。"""
    repetition_penalty: float | None = Field(default=None, ge=0.0, le=2.0)
    """重复惩罚系数 (部分非 OpenAI 模型独有)。"""
    stop: list[str] | str | None = Field(default=None)
    """停止生成的停止词序列。"""
    reasoning_effort: (
        Literal["none", "minimal", "low", "medium", "high", "xhigh", "max"] | None
    ) = Field(default=None)
    """统一的推理强度，会映射到模型支持的最接近等级。"""

    @field_validator("reasoning_effort", mode="before")
    @classmethod
    def _lower_effort(cls, value: Any) -> Any:
        if isinstance(value, str):
            return value.strip().lower() or None
        return value


class OutputFormatConfig(BaseModel):
    """输出格式与结构化控制"""

    response_format: Literal["text", "json"] | None = Field(default=None)
    """响应格式，json 表示要求模型输出 JSON 对象。"""
    response_schema: dict[str, Any] | None = Field(default=None)
    """JSON Schema，用于约束模型返回的 JSON 结构。"""
    response_modalities: list[str] | None = Field(default=None)
    """期望的输出模态 (如 ["text", "image"])，用于可直接出图的对话模型。"""


class ToolCallConfig(BaseModel):
    """工具调用策略配置"""

    mode: Literal["AUTO", "ANY", "NONE"] = Field(default="AUTO")
    """工具调用模式 (AUTO: 自动, ANY: 强制至少调一个, NONE: 禁用)。"""
    allowed_function_names: list[str] | None = Field(default=None)
    """允许被调用的特定函数名称白名单。"""


class MediaGenerationConfig(BaseModel):
    """图像生成配置"""

    aspect_ratio: str | None = Field(default=None)
    """宽高比 (如 '16:9')"""
    resolution: str | None = Field(default=None)
    """分辨率 (如 '1K', '4K' 或 '1024x1024')"""
    quality: Literal["low", "medium", "high", "standard", "hd"] | None = Field(
        default=None
    )
    """渲染质量"""


class TTSConfig(BaseModel):
    """文本转语音 (TTS) 配置"""

    response_format: Literal["mp3", "wav", "pcm", "flac", "opus", "aac"] = Field(
        default="mp3"
    )
    """输出音频格式"""
    speed: float = Field(default=1.0, ge=0.25, le=4.0)
    """语速"""
    params: dict[str, Any] = Field(default_factory=dict)
    """原样合并进请求体的厂商参数 (如 MiniMax 的 voice_setting)"""


class GenerationConfig(BaseModel):
    """LLM 生成配置"""

    common: CommonLLMConfig = Field(default_factory=CommonLLMConfig)
    """通用生成参数"""
    output: OutputFormatConfig = Field(default_factory=OutputFormatConfig)
    """输出格式与约束"""
    tools: ToolCallConfig = Field(default_factory=ToolCallConfig)
    """工具调用策略"""
    media: MediaGenerationConfig = Field(default_factory=MediaGenerationConfig)
    """图像生成参数"""
    params: dict[str, Any] = Field(default_factory=dict)
    """原样深度合并进请求体的厂商参数"""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    def to_dict(self) -> dict[str, Any]:
        return model_dump(self, exclude_none=True)

    def merge_with(self, other: GenerationConfig | None) -> GenerationConfig:
        """深度合并两个配置，other 中显式设置的值优先"""
        if not other:
            return model_copy(self, deep=True)
        merged = deep_merge(
            model_dump(self, exclude_none=True), model_dump(other, exclude_none=True)
        )
        return model_validate(GenerationConfig, merged)


class LLMEmbeddingConfig(BaseModel):
    """Embedding 专用配置"""

    task_type: str | None = Field(default=None)
    """任务类型：RETRIEVAL_QUERY / RETRIEVAL_DOCUMENT / SEMANTIC_SIMILARITY 等"""
    output_dimensionality: int | None = Field(default=None)
    """要求输出的向量维度"""
    title: str | None = Field(default=None)
    """文档标题，仅在 task_type 为 RETRIEVAL_DOCUMENT 时有效。"""
    encoding_format: str | None = Field(default="float")
    """向量编码格式 (float 或 base64)"""
    multimodal: bool | list[str] = Field(default=False)
    """是否允许多模态向量化。False 为纯文本；True 全部放行；
    也可传入 ['image', 'text'] 细粒度控制。"""
    params: dict[str, Any] = Field(default_factory=dict)
    """原样合并进请求体的厂商参数"""


__all__ = [
    "BaseOutputDefinition",
    "CommonLLMConfig",
    "GenerationConfig",
    "LLMEmbeddingConfig",
    "MediaGenerationConfig",
    "OutputFormatConfig",
    "TTSConfig",
    "ToolCallConfig",
    "ToolOutput",
    "deep_merge",
]
