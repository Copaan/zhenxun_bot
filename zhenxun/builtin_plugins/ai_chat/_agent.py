import asyncio
import json
import time
from typing import Any
import uuid

from zhenxun.configs.config import Config
from zhenxun.models.ai_agent_call import AIAgentCall
from zhenxun.models.ai_agent_run import AIAgentRun
from zhenxun.services.ai.capabilities.base import (
    AbstractCapability,
    WrapToolExecuteHandler,
)
from zhenxun.services.ai.core.exceptions import ControlFlowExit, LLMException
from zhenxun.services.ai.llm import get_default_model
from zhenxun.services.ai.run.context import RunContext
from zhenxun.services.ai.sandbox.manager import SandboxManager
from zhenxun.services.ai.tools.core.toolkit import BaseToolkit
from zhenxun.services.ai.tools.models import ResolvedToolPayload, ToolResult
from zhenxun.services.ai.utils.runtime import PermissionUtils
from zhenxun.services.log import logger

from ._data_source import MODULE, PROACTIVE_SOURCES
from ._reply import parse_silent

AGENT_PROMPT = (
    "## Agent 模式\n"
    "当前对话者是超级用户，你可以使用 HTTP 请求、沙箱、MCP 等工具完成任务，"
    "工具调用会直接执行。有副作用的操作要先想清楚，意图不明确时先用 "
    "`ask_user_for_help` 向用户确认。"
)


RUN_ID_KEY = "_agent_run_id"
"""运行上下文 state 中的运行id"""
AGENT_ACTIVE_KEY = "_agent_active"
"""运行上下文 state 中缓存的 Agent 是否生效"""
AGENT_TOOLS_KEY = "_agent_tools"
"""运行上下文 state 中本次运行提供的 Agent 工具名"""


def agent_enabled() -> bool:
    """是否为超级用户开启 Agent

    返回:
        bool: 是否开启
    """
    return bool(Config.get_config(MODULE, "AGENT_ENABLED"))


USER_KEY = "_agent_user"
"""运行上下文 state 中的发起人，用于没有 NoneBot 事件的运行（如 WebUI 控制台）"""


def start_run(context: RunContext) -> str:
    """为一次运行分配运行id

    参数:
        context: 运行上下文

    返回:
        str: 运行id
    """
    run_id = uuid.uuid4().hex
    context.state[RUN_ID_KEY] = run_id
    return run_id


async def record_run(
    context: RunContext,
    source: str,
    persona: str | None,
    text: str,
    started: float,
    result: Any = None,
    error: BaseException | None = None,
) -> None:
    """保存一次运行的记录：聊天与控制台只在 Agent 生效时记录，主动发言总是记录，
    主动发言放弃发言时状态为 silent

    参数:
        context: 运行上下文
        source: 来源：qq、webui 或主动发言来源（interject、task）
        persona: 人设名称
        text: 输入文本
        started: 开始时间（time.monotonic）
        result: 运行结果
        error: 运行失败时的异常
    """
    run_id = context.state.get(RUN_ID_KEY)
    proactive = source in PROACTIVE_SOURCES
    if not run_id or not (proactive or context.state.get(AGENT_ACTIVE_KEY)):
        return
    status, message = "ok", ""
    if error is not None:
        status = "stopped" if isinstance(error, asyncio.CancelledError) else "error"
        message = (
            error.user_friendly_message
            if isinstance(error, LLMException)
            else f"{type(error).__name__}: {error}"
        )
    elif result is not None and result.stopped:
        status, message = "stopped", str(result.stopped)
    elif proactive and result is not None and parse_silent(str(result.output)) is None:
        status = "silent"
    try:
        await AIAgentRun.create(
            run_id=run_id,
            source=source,
            session_id=context.session_id or "",
            user_id=context.get_user_id() or context.state.get(USER_KEY) or "",
            group_id=context.get_group_id(),
            persona=persona,
            model=context.run.current_model or get_default_model("chat"),
            input=_shorten(text, 2000),
            output=_shorten(str(result.output) if result else "", 4000),
            tools=await AIAgentCall.filter(run_id=run_id).count(),
            tokens=result.usage.total_tokens if result else 0,
            status=status,
            error=_shorten(message, 2000),
            duration=int((time.monotonic() - started) * 1000),
        )
    except Exception as e:
        logger.warning(f"保存 Agent 运行记录失败: {e}", MODULE)


def _shorten(value: Any, limit: int) -> str:
    """把参数或结果转为不超过 limit 字符的文本

    参数:
        value: 参数或结果
        limit: 最大字符数

    返回:
        str: 文本
    """
    text = (
        value
        if isinstance(value, str)
        else json.dumps(value, ensure_ascii=False, default=str)
    )
    return text if len(text) <= limit else f"{text[:limit]}…"


class _Resolved:
    """已解析的工具载荷，原样提供给 Agent"""

    def __init__(self, payload: ResolvedToolPayload):
        self.payload = payload

    async def resolve(self, context: RunContext | None = None) -> ResolvedToolPayload:
        return self.payload


class AgentCapability(AbstractCapability):
    """超级用户在 WebUI 开启 Agent 后，为其提供 HTTP 请求、沙箱、MCP 与提问工具及
    提示词，这些工具调用直接执行并记录"""

    async def active(self, context: RunContext) -> bool:
        """当前对话是否启用 Agent

        参数:
            context: 运行上下文

        返回:
            bool: 已开启 Agent 且对话者是超级用户时为 True
        """
        if not agent_enabled():
            return False
        if AGENT_ACTIVE_KEY not in context.state:
            context.state[AGENT_ACTIVE_KEY] = await PermissionUtils.check_superuser(
                context
            )
        return context.state[AGENT_ACTIVE_KEY]

    async def get_system_prompts(self, context: RunContext) -> list[str]:
        return [AGENT_PROMPT] if await self.active(context) else []

    async def get_tools(self, context: RunContext) -> list[Any]:
        if not await self.active(context):
            return []
        from zhenxun.services.ai.tools.engine.registry import resolve_tools
        from zhenxun.services.ai.tools.providers.builtin.hitl import HITLToolkit
        from zhenxun.services.ai.tools.providers.builtin.rest_api import (
            RestApiToolkit,
        )
        from zhenxun.services.ai.tools.providers.mcp.provider import MCPSource

        tools: list[Any] = [HITLToolkit(), RestApiToolkit()]
        if (await SandboxManager.ready())[0]:
            from zhenxun.services.ai.tools.providers.builtin.sandbox import (
                SandboxToolkit,
            )

            tools.append(SandboxToolkit())
        tools.append(MCPSource.all_enabled())
        payload = await resolve_tools(tools, context)
        context.state[AGENT_TOOLS_KEY] = {getattr(t, "name", "") for t in payload.tools}
        return [_Resolved(payload)]

    async def wrap_tool_execute(
        self,
        context: RunContext,
        tool_name: str,
        arguments: dict[str, Any],
        handler: WrapToolExecuteHandler,
    ) -> Any:
        if tool_name not in context.state.get(AGENT_TOOLS_KEY, ()):
            return await handler(arguments)
        call = AIAgentCall(
            run_id=context.state.get(RUN_ID_KEY),
            session_id=context.session_id or "",
            user_id=context.get_user_id() or context.state.get(USER_KEY) or "",
            group_id=context.get_group_id(),
            tool=tool_name,
            arguments=_shorten(arguments, 2000),
            decision="auto",
        )
        result: Any = None
        start = time.monotonic()
        try:
            result = await handler(arguments)
            return result
        except (ControlFlowExit, Exception) as e:
            result = ToolResult(output=f"{type(e).__name__}: {e}").as_error()
            raise
        finally:
            if isinstance(result, ToolResult):
                call.is_error = result.is_error
                call.result = _shorten(result.output, 2000)
            elif result is not None:
                call.result = _shorten(result, 2000)
            call.duration = int((time.monotonic() - start) * 1000)
            try:
                await call.save()
            except Exception as e:
                logger.warning(f"保存 Agent 调用记录失败: {e}", MODULE)


SUBAGENT_DEPTH_KEY = "_subagent_depth"
"""运行上下文 state 中的子代理嵌套层数"""


class SubagentToolkit(BaseToolkit):
    """人设配置的子代理：每个子代理是另一个人设（用它自己的模型与工具），
    主 Agent 通过 transfer_to_<id> 把任务交给它并取回文字结果；子代理不能再转交"""

    def __init__(self, persona_ids: list[int]):
        super().__init__()
        self.persona_ids = persona_ids

    async def resolve(self, context: RunContext | None = None) -> ResolvedToolPayload:
        from zhenxun.models.ai_persona import AIPersona
        from zhenxun.services.ai.tools.core.tool import FunctionTool
        from zhenxun.services.ai.tools.models import ToolOptions

        if not self.persona_ids or (context and context.state.get(SUBAGENT_DEPTH_KEY)):
            return ResolvedToolPayload()
        personas = await AIPersona.filter(id__in=self.persona_ids).order_by("id")
        tools = []
        for persona in personas:
            brief = " ".join(persona.prompt.split())[:120]
            function_tool = FunctionTool(
                _transfer(persona.id),
                name=f"transfer_to_{persona.id}",
                description=f"把一项任务交给子代理「{persona.name}」完成，"
                f"返回它的文字结果。它的设定：{brief}",
                settings=ToolOptions(metadata={"no_timeout": True}),
            )
            tools.extend((await function_tool.resolve(context)).tools)
        return ResolvedToolPayload(tools=tools)


def _transfer(persona_id: int):
    """生成转交给某个子代理的工具函数

    参数:
        persona_id: 子代理人设id

    返回:
        Callable: 工具函数
    """

    async def transfer(task: str, context: RunContext) -> ToolResult:
        """把任务交给子代理

        参数:
            task: 交给子代理的任务，写清背景与要求
        """
        from zhenxun.models.ai_persona import AIPersona
        from zhenxun.models.ai_token_usage import AITokenUsage
        from zhenxun.services.ai.context.memory.engine import chat_backend

        from ._data_source import AIChatManager

        persona = await AIPersona.get_or_none(id=persona_id)
        if persona is None:
            return ToolResult(output="这个子代理已不存在。").as_error(False)
        agent = AIChatManager.get_agent(persona)
        child = RunContext(
            deps=context.deps, session_id=f"{context.session_id}:sub:{uuid.uuid4().hex}"
        )
        child.state[SUBAGENT_DEPTH_KEY] = 1
        child.run.cancellation_token = context.run.cancellation_token
        try:
            result = await agent.run(task, context=child)
        except LLMException as e:
            return ToolResult(
                output=f"子代理执行失败: {e.user_friendly_message}"
            ).as_error(False)
        finally:
            if backend := chat_backend(agent.memory_config):
                await backend.clear(child.session_id or "")
        if user_id := context.get_user_id():
            await AITokenUsage.add_usage(user_id, result.usage.total_tokens)
        return ToolResult(output=str(result.output).strip() or "子代理没有给出结果。")

    return transfer
