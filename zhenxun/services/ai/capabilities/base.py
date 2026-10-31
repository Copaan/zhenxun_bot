from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING, Any

from zhenxun.services.ai.core.messages import ChatRequest, ChatResponse
from zhenxun.services.ai.core.models import LLMContext

if TYPE_CHECKING:
    from zhenxun.services.ai.run.context import RunContext

WrapModelRequestHandler = Callable[
    [LLMContext[ChatRequest, ChatResponse]], Awaitable[ChatResponse]
]
"""单次大模型 API 请求包裹的处理函数类型"""

WrapToolExecuteHandler = Callable[[dict[str, Any]], Awaitable[Any]]
"""单一工具执行过程包裹的处理函数类型"""


class AbstractCapability:
    """
    Agent 能力组件基类协议。
    """

    async def get_system_prompts(self, context: RunContext) -> list[str]:
        """获取该能力提供的系统提示词列表。"""
        return []

    async def get_tools(self, context: RunContext) -> list[Any]:
        """获取该能力附带的工具列表。"""
        return []

    async def prepare_tools(
        self, context: RunContext, tool_defs: list[Any]
    ) -> list[Any]:
        """运行开始前/装配工具时触发。允许动态增删改当前将发往大模型的工具列表。
        默认实现：无操作，直接返回传入的工具列表。"""
        return tool_defs

    async def wrap_model_request(
        self,
        context: RunContext,
        llm_context: LLMContext[ChatRequest, ChatResponse],
        handler: WrapModelRequestHandler,
    ) -> ChatResponse:
        """包裹单次大模型 API 请求 (洋葱模型)。"""
        return await handler(llm_context)

    async def wrap_tool_execute(
        self,
        context: RunContext,
        tool_name: str,
        arguments: dict[str, Any],
        handler: WrapToolExecuteHandler,
    ) -> Any:
        """包裹单一工具的执行 (洋葱模型)。"""
        return await handler(arguments)


class CombinedCapability(AbstractCapability):
    """
    组合能力容器。
    将多个 Capability 按顺序融合成洋葱模型，排在前面的在最外层执行。
    """

    def __init__(self, capabilities: list[AbstractCapability]):
        """初始化组合能力，展平嵌套的组合并按实例去重"""
        self.capabilities: list[AbstractCapability] = []
        for c in capabilities:
            for cap in c.capabilities if isinstance(c, CombinedCapability) else [c]:
                if all(cap is not exist for exist in self.capabilities):
                    self.capabilities.append(cap)

    async def get_system_prompts(self, context: RunContext) -> list[str]:
        """获取组合中所有能力提供的系统提示词列表"""
        prompts = []
        for cap in self.capabilities:
            prompts.extend(await cap.get_system_prompts(context))
        return prompts

    async def get_tools(self, context: RunContext) -> list[Any]:
        """获取组合中所有能力附带注册的工具列表"""
        tools = []
        for cap in self.capabilities:
            tools.extend(await cap.get_tools(context))
        return tools

    async def prepare_tools(
        self, context: RunContext, tool_defs: list[Any]
    ) -> list[Any]:
        """依次调用组合中所有能力的准备工具钩子处理工具定义"""
        current_defs = list(tool_defs)
        for cap in self.capabilities:
            res = await cap.prepare_tools(context, current_defs)
            if res is not None:
                current_defs = res
        return current_defs

    async def wrap_model_request(
        self,
        context: RunContext,
        llm_context: LLMContext[ChatRequest, ChatResponse],
        handler: WrapModelRequestHandler,
    ) -> ChatResponse:
        """串联能力组合的 wrap_model_request 洋葱模型拦截器链"""
        chain = handler
        for cap in reversed(self.capabilities):

            def link(ctx, cap=cap, inner=chain):
                return cap.wrap_model_request(context, ctx, inner)

            chain = link
        return await chain(llm_context)

    async def wrap_tool_execute(
        self,
        context: RunContext,
        tool_name: str,
        arguments: dict[str, Any],
        handler: WrapToolExecuteHandler,
    ) -> Any:
        """串联能力组合的 wrap_tool_execute 洋葱模型拦截器链"""
        chain = handler
        for cap in reversed(self.capabilities):

            def link(args, cap=cap, inner=chain):
                return cap.wrap_tool_execute(context, tool_name, args, inner)

            chain = link
        return await chain(arguments)
