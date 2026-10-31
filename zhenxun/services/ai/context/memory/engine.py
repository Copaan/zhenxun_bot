import asyncio
from collections.abc import Awaitable, Callable, Sequence

from zhenxun.services.ai.core.messages import LLMMessage
from zhenxun.services.ai.core.models import ModelProfile
from zhenxun.services.ai.utils.logger import log_memory as logger

from .compression import CondenserPipeline, summary_sink
from .models import BaseChatContext, MemoryConfig
from .storage import DatabaseChatContext, after_latest_summary

_default_backend = DatabaseChatContext()
_summarizing: set[str] = set()
_summary_tasks: set[asyncio.Task] = set()


def chat_backend(config: MemoryConfig | None) -> BaseChatContext | None:
    """按配置取短期对话记忆后端，配置中未指定时使用数据库后端

    参数:
        config: 记忆配置

    返回:
        BaseChatContext | None: 后端，未开启短期记忆时为空
    """
    if not config or not config.short_term.enable:
        return None
    return config.short_term.backend or _default_backend


class SessionMemoryContext:
    """
    会话记忆门面。
    负责读取会话历史并执行上下文压缩，以及保存新产生的对话。
    """

    def __init__(self, session_id: str, memory_config: MemoryConfig):
        """初始化会话记忆门面

        参数:
            session_id: 会话 ID
            memory_config: 记忆配置
        """
        self.session_id = session_id
        self.memory_config = memory_config
        self.backend = chat_backend(memory_config)

    async def read(
        self,
        model_name: str,
        profile: ModelProfile | None = None,
        base_overhead: int = 0,
    ) -> list[LLMMessage]:
        """读取最新摘要之后的历史消息，并按压缩管线修剪

        参数:
            model_name: 本次请求的模型名称
            profile: 模型能力档案
            base_overhead: 系统提示词与工具定义等固定开销的 Token 数

        返回:
            list[LLMMessage]: 历史消息
        """
        if not self.backend:
            return []
        history = after_latest_summary(await self.backend.get_messages(self.session_id))
        pipeline = CondenserPipeline.create_from_configs(
            self.memory_config, profile, model_name
        )
        if not pipeline.reducers:
            return history
        token = summary_sink.set(self._summary_sink(self.backend))
        try:
            history, _ = await pipeline.run(
                history, model_name=model_name, base_overhead=base_overhead
            )
        finally:
            summary_sink.reset(token)
        return history

    def _summary_sink(
        self, backend: BaseChatContext
    ) -> Callable[[Callable[[], Awaitable[LLMMessage | None]]], None]:
        """生成后台摘要任务的提交函数，同一会话同一时间只运行一个摘要任务

        参数:
            backend: 保存摘要的短期记忆后端

        返回:
            Callable: 接收摘要生成函数的提交函数
        """
        key = f"{id(backend)}:{self.session_id}"
        session_id = self.session_id

        def submit(produce: Callable[[], Awaitable[LLMMessage | None]]) -> None:
            if key in _summarizing:
                return
            _summarizing.add(key)

            async def job() -> None:
                try:
                    if summary := await produce():
                        await backend.add_messages(session_id, [summary])
                        logger.info(f"💾 [SessionMemory] 已保存会话 {key} 的摘要")
                except Exception as e:
                    logger.error(f"[SessionMemory] 后台生成摘要失败: {e}", e=e)
                finally:
                    _summarizing.discard(key)

            task = asyncio.create_task(job())
            _summary_tasks.add(task)
            task.add_done_callback(_summary_tasks.discard)

        return submit

    async def write(self, new_messages: Sequence[LLMMessage]) -> None:
        """保存新产生的对话

        参数:
            new_messages: 新消息
        """
        if self.backend and new_messages:
            await self.backend.add_messages(self.session_id, list(new_messages))
