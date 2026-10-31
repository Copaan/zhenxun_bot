import asyncio
from collections.abc import AsyncIterator, Callable, Sequence
import contextlib
import copy
import inspect
from typing import Any

from nonebot.utils import is_coroutine_callable

from zhenxun.services.ai.capabilities.base import AbstractCapability, CombinedCapability
from zhenxun.services.ai.capabilities.builtin import default_capabilities
from zhenxun.services.ai.config import get_llm_config
from zhenxun.services.ai.context.memory.engine import SessionMemoryContext
from zhenxun.services.ai.context.memory.models import MemoryConfig
from zhenxun.services.ai.core.engine.token_counter import token_counter
from zhenxun.services.ai.core.exceptions import AbortException, ControlFlowExit
from zhenxun.services.ai.core.messages import (
    LLMMessage,
    PromptInput,
    TextPart,
    UsageInfo,
)
from zhenxun.services.ai.core.models import CancellationToken
from zhenxun.services.ai.core.options import GenerationConfig
from zhenxun.services.ai.core.protocols.tool import ToolResolvable
from zhenxun.services.ai.core.stream_events import EventBus
from zhenxun.services.ai.llm.manager import resolve_model_profile
from zhenxun.services.ai.llm.protocols import drop_tool_media, has_media
from zhenxun.services.ai.message_builder import MessageBuilder
from zhenxun.services.ai.run.context import RunContext
from zhenxun.services.ai.run.di import DependencyInjector
from zhenxun.services.ai.run.models import (
    AgentRunEnd,
    AgentRunError,
    AgentRunResult,
    AgentRunStart,
    StreamedRunResult,
)
from zhenxun.services.ai.run.session import session_manager
from zhenxun.services.ai.run.subscribers import DefaultUISubscriber, show_exit_notice
from zhenxun.services.ai.tools.core.tool import BaseTool
from zhenxun.services.ai.tools.core.toolkit import BaseToolkit
from zhenxun.services.ai.tools.engine.registry import ToolCollection, resolve_tools
from zhenxun.services.ai.tools.models import ResolvedToolPayload
from zhenxun.services.ai.utils.logger import log_flow as logger
from zhenxun.utils.pydantic_compat import model_construct, model_copy
from zhenxun.utils.utils import infer_plugin_namespace

from .executor import AgentExecutor
from .models import AgentConfig, AgentRunResources, AgentState

ToolSource = Callable | BaseTool | BaseToolkit | ToolResolvable
"""可以提供给大模型的工具来源（函数、工具、工具箱或可解析对象）"""

_GLOBAL_ENGINE_FIELDS = (
    "max_cycles",
    "enable_parallel_calls",
    "reflexion_retries",
    "enable_fallback_summary",
)
"""AgentConfig 中未显式设置时跟随全局 agent_settings 的字段"""


def _prompt_text(prompt: PromptInput | None) -> str:
    """提取输入中的纯文本，用于记录用户输入与追加消息

    参数:
        prompt: 输入内容

    返回:
        str: 纯文本
    """
    if prompt is None:
        return ""
    if isinstance(prompt, str):
        return prompt
    if isinstance(prompt, list):
        return " ".join(p.text for p in prompt if isinstance(p, TextPart))
    if extract := getattr(prompt, "extract_plain_text", None):
        return extract()
    return str(prompt)


async def _call_with_deps(func: Callable, context: RunContext) -> Any:
    """注入依赖后调用函数，支持同步与异步函数

    参数:
        func: 函数
        context: 运行上下文

    返回:
        Any: 函数返回值
    """
    sig = inspect.signature(func)
    kwargs = (
        await DependencyInjector.resolve_all(sig=sig, call_kwargs={}, context=context)
        if sig.parameters
        else {}
    )
    return await func(**kwargs) if is_coroutine_callable(func) else func(**kwargs)


class Agent:
    """
    Agent 运行时封装。
    负责组织模型、工具、记忆与能力组件，驱动单次运行或流式运行。
    """

    def __init__(
        self,
        name: str,
        instruction: str = "",
        model: str | Callable[[], str] | None = None,
        tools: Sequence[ToolSource] | None = None,
        memory: bool | MemoryConfig = False,
        config: AgentConfig | dict | None = None,
        capabilities: list[AbstractCapability] | None = None,
        generation_config: GenerationConfig | None = None,
    ):
        """初始化 Agent

        参数:
            name: Agent 名称，用于日志、事件和记忆隔离
            instruction: 静态系统指令
            model: 默认模型名称 (Provider/Model) 或返回模型名的回调
            tools: 工具来源列表
            memory: 是否开启短期记忆与上下文压缩，支持布尔值或 MemoryConfig
            config: 运行配置，支持字典
            capabilities: 能力组件列表
            generation_config: 生成配置 (温度、最大输出等)
        """
        self.name = name
        self.instruction = instruction
        self.model_name = model
        self.namespace = infer_plugin_namespace() or "unknown"
        self.tool_definitions = list(tools or [])
        self.capabilities = list(capabilities or [])
        self.dynamic_prompts: list[Callable] = []
        self.memory_config = MemoryConfig.resolve(memory)
        if isinstance(config, dict):
            config = AgentConfig(**config)
        self.config = config or AgentConfig()
        self.generation_config = generation_config

    @property
    def stateless(self) -> bool:
        """是否不使用短期记忆"""
        return not self.memory_config.short_term.enable

    def system_prompt(self, func: Callable) -> Callable:
        """注册动态系统提示词函数，参数按依赖注入解析

        参数:
            func: 返回提示词文本或消息的函数

        返回:
            Callable: 原函数
        """
        self.dynamic_prompts.append(func)
        return func

    async def run(
        self, prompt: PromptInput | None = None, *, context: RunContext | None = None
    ) -> AgentRunResult[Any]:
        """运行 Agent 并等待结果

        运行被中止 (ControlFlowExit) 时发送其提示语，并返回 output 为空、
        stopped 为中止原因的结果；任务被取消时照常抛出 CancelledError。

        参数:
            prompt: 用户输入
            context: 运行上下文

        返回:
            AgentRunResult[Any]: 运行结果
        """
        ctx: RunContext = context or RunContext()
        try:
            async with self.run_stream(prompt, context=ctx) as stream:
                return await stream.get_run_result()
        except ControlFlowExit as e:
            logger.info(f"[{self.name}] 运行中止: {type(e).__name__} - {e}")
            await show_exit_notice(e, ctx)
            return AgentRunResult(output="", usage=UsageInfo(), stopped=e)

    @contextlib.asynccontextmanager
    async def run_stream(
        self, prompt: PromptInput | None = None, *, context: RunContext | None = None
    ) -> AsyncIterator[StreamedRunResult[Any]]:
        """流式运行 Agent，负责会话并发控制与事件总线挂载

        参数:
            prompt: 用户输入
            context: 运行上下文

        返回:
            AsyncIterator[StreamedRunResult[Any]]: 流式结果
        """
        ctx = context or RunContext()
        bus = EventBus()
        if not ctx.state.get("__is_root_run_executed__"):
            ctx.state["__is_root_run_executed__"] = True
            if ctx.get_bot() and ctx.get_event():
                DefaultUISubscriber(ctx).attach(bus)
        text = _prompt_text(prompt)

        async def execution() -> None:
            cancel_token = ctx.run.cancellation_token or CancellationToken()
            ctx.run.cancellation_token = cancel_token
            try:
                async with session_manager.run_scope(
                    ctx.session_id or "default_session",
                    cancel_token,
                    exclusive=not self.stateless,
                    follow_up=prompt if self.config.follow_up else None,
                    sender=ctx.get_user_id(),
                ):
                    await bus.emit(AgentRunStart(agent_name=self.name))
                    result = await self._execute(prompt, text, ctx, cancel_token)
                    await bus.emit(AgentRunEnd(result=result))
            except asyncio.CancelledError:
                logger.debug(f"[{self.name}] 执行被取消。")
                await bus.emit(AgentRunError(error=AbortException("任务已被中止")))
            except (ControlFlowExit, Exception) as e:
                await bus.emit(AgentRunError(error=e))
            finally:
                await bus.end()

        ctx.run.event_bus = bus
        task = asyncio.create_task(execution())
        try:
            yield StreamedRunResult[Any](bus)
        finally:
            if not task.done():
                task.cancel()

    def _effective_config(self) -> AgentConfig:
        """未显式设置的循环相关字段取全局 agent_settings

        返回:
            AgentConfig: 本次运行生效的配置
        """
        config = model_copy(self.config)
        agent_settings = get_llm_config().agent_settings
        for field in _GLOBAL_ENGINE_FIELDS:
            if field not in self.config.model_fields_set:
                setattr(config, field, getattr(agent_settings, field))
        return config

    async def _build_prompts(
        self, context: RunContext, capability: CombinedCapability
    ) -> tuple[list[str], list[str]]:
        """生成静态系统提示词（指令与能力组件提示）与本次运行的提示

        参数:
            context: 运行上下文
            capability: 组合能力

        返回:
            tuple[list[str], list[str]]: 静态提示词与本次运行的提示
        """
        static = [self.instruction] if self.instruction else []
        static.extend(
            text
            for text in await capability.get_system_prompts(context)
            if text and text.strip()
        )
        reminders: list[str] = []
        for func in self.dynamic_prompts:
            res = await _call_with_deps(func, context)
            for item in res if isinstance(res, list) else [res]:
                text = item.extract_text if isinstance(item, LLMMessage) else item
                if text and str(text).strip():
                    reminders.append(str(text))
        return static, reminders

    async def _build_tools(
        self, context: RunContext, capability: CombinedCapability
    ) -> tuple[ToolCollection, ResolvedToolPayload]:
        """解析工具来源，经能力组件筛选后生成本次可用的工具集合

        参数:
            context: 运行上下文
            capability: 组合能力

        返回:
            tuple[ToolCollection, ResolvedToolPayload]: 工具集合与解析载荷
                （取其中的说明与提示）
        """
        payload = await resolve_tools(
            [*self.tool_definitions, *await capability.get_tools(context)], context
        )
        defs = [
            d
            for t in payload.tools
            if hasattr(t, "get_definition") and (d := await t.get_definition(context))
        ]
        defs = list(await capability.prepare_tools(context, defs) or [])
        defs_by_name = {d.name.lower(): d for d in defs if d}
        tools = []
        for t in payload.tools:
            name = getattr(t, "name", "unknown").lower()
            if name in defs_by_name:
                tool = copy.copy(t)
                setattr(tool, "_dynamic_def", defs_by_name.pop(name))
                tools.append(tool)
        return ToolCollection(tools), payload

    async def _prepare(
        self,
        prompt: PromptInput | None,
        context: RunContext,
        resources: AgentRunResources,
    ) -> AgentState:
        """装配系统提示词、工具与记忆，生成初始运行状态

        参数:
            prompt: 用户输入
            context: 运行上下文
            resources: 运行资源

        返回:
            AgentState: 初始运行状态
        """
        static, reminders = await self._build_prompts(context, resources.capability)
        tools, payload = await self._build_tools(context, resources.capability)
        static.extend(payload.injected_prompts)
        reminders.extend(payload.reminders)
        model = context.run.current_model or ""
        overhead = sum(token_counter._count_text(p) for p in [*static, *reminders])
        for t in tools:
            definition = getattr(t, "_dynamic_def", None)
            if parameters := getattr(definition, "parameters", None):
                overhead += token_counter.count_tools_schema(parameters)

        state = AgentState(
            static_system_prompt=static, reminders=reminders, tools=tools
        )
        memory = resources.memory_context
        messages = (
            await memory.read(
                model_name=model,
                profile=resources.model_profile,
                base_overhead=overhead,
            )
            if memory
            else []
        )
        messages.extend(context.run.messages)
        if prompt is not None and (
            msgs := await MessageBuilder.normalize_to_llm_messages(prompt)
        ):
            messages.append(msgs[-1])
            state._input = msgs[-1]
            if memory:
                if has_media(msgs[-1]):
                    state._pending_input = msgs[-1]
                else:
                    await memory.write([msgs[-1]])
        context.run.messages = messages
        state._origin_msg_len = len(messages)
        return state

    async def _execute(
        self,
        prompt: PromptInput | None,
        text: str,
        context: RunContext,
        cancel_token: CancellationToken,
    ) -> AgentRunResult[Any]:
        """执行一次完整运行并把新增对话写入记忆

        参数:
            prompt: 用户输入
            text: 输入的纯文本
            context: 运行上下文
            cancel_token: 取消令牌

        返回:
            AgentRunResult[Any]: 运行结果
        """
        config = self._effective_config()
        context.run.user_input = text
        context.run.agent_name = self.name
        context.run.tool_max_retries = config.reflexion_retries
        context.run.cancellation_token = cancel_token
        if not context.run.current_model:
            context.run.current_model = (
                self.model_name() if callable(self.model_name) else self.model_name
            )
        memory = None
        if not self.stateless:
            memory = SessionMemoryContext(
                session_id=context.session_id or f"{self.namespace}:{self.name}",
                memory_config=self.memory_config,
            )
        capability = CombinedCapability(
            [*default_capabilities(), *context.capabilities, *self.capabilities]
        )
        resources = AgentRunResources(
            run_context=context,
            memory_context=memory,
            capability=capability,
            config=config,
            model_profile=resolve_model_profile(context.run.current_model),
            generation_config=self.generation_config,
        )

        original_capabilities = context.capabilities
        context.capabilities = capability.capabilities
        try:
            state = await self._prepare(prompt, context, resources)
            pending = [state._pending_input] if state._pending_input else []
            try:
                result = await AgentExecutor().run(state, resources)
            except BaseException:
                if pending and memory:
                    await memory.write(pending)
                raise
        finally:
            context.capabilities = original_capabilities

        new_msgs = result.messages[state._origin_msg_len :]
        if memory:
            await memory.write([*pending, *drop_tool_media(new_msgs)])
        output = result.output or (
            new_msgs[-1].extract_text
            if new_msgs and isinstance(new_msgs[-1], LLMMessage)
            else ""
        )
        return model_construct(
            AgentRunResult,
            output=output,
            messages=new_msgs,
            usage=result.usage or UsageInfo(),
        )
