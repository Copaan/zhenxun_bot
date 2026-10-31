"""外部 AI 插件使用的旧接口

旧模块路径由 `zhenxun.services` 中的导入钩子映射到这里或现有模块，
这里只实现插件实际调用到的部分。
"""

from typing import Any
from typing_extensions import Self

from zhenxun.services.ai.context.memory.models import MemoryConfig, ShortTermConfig
from zhenxun.services.ai.core.messages.parts import BaseContentPart
from zhenxun.services.ai.core.models import ModelModality, ModelProfile
from zhenxun.services.ai.core.options import CommonLLMConfig, GenerationConfig
from zhenxun.utils.pydantic_compat import model_copy, model_dump

LLMContentPart = BaseContentPart
"""内容片段的旧名称，工厂方法见 BaseContentPart"""


class MemoryBuilder(MemoryConfig):
    """记忆配置的链式写法，结果可直接作为 Agent 的 memory 参数"""

    @classmethod
    def auto(cls) -> "MemoryBuilder":
        """开启短期记忆与默认压缩

        返回:
            MemoryBuilder: 记忆配置
        """
        return cls(short_term=ShortTermConfig(enable=True))

    def with_short_term(
        self, enable: bool = True, isolation: Any = None, backend: Any = None
    ) -> Self:
        """配置短期记忆

        参数:
            enable: 是否开启
            isolation: 不再使用，会话按 RunContext.session_id 区分
            backend: 存储后端，为空使用数据库

        返回:
            Self: 自身
        """
        self.short_term.enable = enable
        if backend is not None and not isinstance(backend, str):
            self.short_term.backend = backend
        return self

    def with_multimodal_window(self, window_size: int = 5) -> Self:
        """只保留最近若干轮消息中的媒体

        参数:
            window_size: 轮数

        返回:
            Self: 自身
        """
        self.compression.vision_window = window_size
        return self

    def with_llm_summary(self, *args: Any, **kwargs: Any) -> Self:
        """使用默认的摘要压缩

        返回:
            Self: 自身
        """
        return self

    def build(self) -> MemoryConfig:
        """返回记忆配置

        返回:
            MemoryConfig: 自身
        """
        return self


class _MemoryCleaner:
    def __init__(self):
        self._session_id: str | None = None

    def session(self, session_id: str) -> Self:
        self._session_id = session_id
        return self

    async def clear_short_term(self) -> None:
        from zhenxun.services.ai.context.memory.engine import chat_backend

        backend = chat_backend(MemoryConfig())
        if backend and self._session_id:
            await backend.clear(self._session_id)

    clear_all = clear_short_term


class _MemoryManager:
    def cleaner(self) -> _MemoryCleaner:
        """按会话清理短期记忆：`cleaner().session(id).clear_short_term()`"""
        return _MemoryCleaner()


memory_manager = _MemoryManager()


class IntentBuilder(GenerationConfig):
    """生成配置的链式写法，结果可直接作为 config 参数"""

    def config_core(
        self,
        temperature: float | None = None,
        max_tokens: int | None = None,
        top_p: float | None = None,
    ) -> Self:
        """设置采样参数

        参数:
            temperature: 温度
            max_tokens: 最大输出 token
            top_p: top_p

        返回:
            Self: 自身
        """
        values = {"temperature": temperature, "max_tokens": max_tokens, "top_p": top_p}
        self.common = CommonLLMConfig(
            **{
                **model_dump(self.common, exclude_unset=True),
                **{k: v for k, v in values.items() if v is not None},
            }
        )
        return self

    def build(self) -> GenerationConfig:
        """返回生成配置

        返回:
            GenerationConfig: 自身
        """
        return self


class _Orchestrator:
    async def invoke(
        self,
        request: Any,
        model_name: str | None = None,
        task: str = "chat",
        override_config: GenerationConfig | dict | None = None,
        cancellation_token: Any = None,
    ) -> Any:
        """按模型名或路由组执行请求

        参数:
            request: 统一请求对象
            model_name: 模型全名或路由组名，为空时使用任务默认模型
            task: 任务类型
            override_config: 替换请求中的生成配置
            cancellation_token: 取消令牌

        返回:
            Any: 统一响应对象
        """
        from zhenxun.services.ai.llm.api import invoke

        if isinstance(override_config, dict):
            override_config = GenerationConfig(**override_config)
        if override_config is not None:
            request = model_copy(request, update={"config": override_config})
        return await invoke(task, request, model_name, cancellation_token)


LLMOrchestrator = _Orchestrator()


def get_model_capabilities(model_name: str) -> ModelProfile:
    """取模型首条可用线路的能力档案，无法解析时视为支持全部输入

    参数:
        model_name: 模型全名或路由组名

    返回:
        ModelProfile: 能力档案
    """
    from zhenxun.services.ai.llm.manager import resolve_routes

    try:
        routes = resolve_routes(model_name)
    except Exception:
        routes = []
    if not routes:
        return ModelProfile(input_modalities=set(ModelModality))
    return routes[0].profile
