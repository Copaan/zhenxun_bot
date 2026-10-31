"""第三方执行器：选用非 local 执行器时由它代替本地 Agent 完成一轮对话，
唤醒、限流、安全过滤、额度与发送仍由 ai_chat 处理"""

from typing import Any

from zhenxun.configs.config import Config
from zhenxun.models.ai_chat_group import AIChatGroup
from zhenxun.models.ai_persona import AIPersona
from zhenxun.services.ai.core.models import CancellationToken
from zhenxun.services.ai.run.context import RunContext
from zhenxun.services.ai.run.dsh import (
    DshRuntime,
    DshSessions,
    DshSettings,
    content_blocks,
)
from zhenxun.services.ai.run.models import AgentRunResult
from zhenxun.services.ai.run.session import session_manager
from zhenxun.services.ai.utils.runtime import PermissionUtils
from zhenxun.services.log import logger

from ._data_source import MODULE, AIChatManager, _time_prompt

RUNNERS = {"local": "本地 Agent", "dsh": "DeepSeek Harness"}
"""可选的执行器"""


def agent_runner(setting: AIChatGroup | None) -> str:
    """会话使用的执行器：群设置优先，其次为全局 AGENT_RUNNER

    参数:
        setting: 群设置，私聊为空

    返回:
        str: 执行器名称
    """
    runner = (setting.agent_runner if setting else None) or Config.get_config(
        MODULE, "AGENT_RUNNER", "local"
    )
    return runner if runner in RUNNERS else "local"


def dsh_settings() -> DshSettings:
    """读取聊天设置中的 DSH

    返回:
        DshSettings: dsh 执行器设置
    """
    return DshSettings(**(Config.get_config(MODULE, "DSH") or {}))


async def run_dsh(
    context: RunContext, parts: list[Any], persona: AIPersona | None
) -> AgentRunResult:
    """用 dsh 完成一轮对话：新会话首轮以文本注入人设，人设变化时换新会话；
    dsh 的审批只允许超级用户的会话，提问转为向当前用户提问

    参数:
        context: 运行上下文
        parts: 本轮输入
        persona: 人设

    返回:
        AgentRunResult: 运行结果
    """
    from zhenxun.services.ai.run.hitl import HITLController

    settings = dsh_settings()
    await DshRuntime.ensure(settings)
    context.run.current_model = f"dsh:{settings.model}"
    session_id = context.session_id or ""
    persona_key = f"{persona.id}:{persona.updated_at.isoformat()}" if persona else ""
    dsh_id, new = DshSessions.get(session_id, persona_key)
    blocks = []
    if new:
        persona_text = AIChatManager.build_instruction(persona)
        blocks.append({"type": "text", "text": f"<人设>\n{persona_text}\n</人设>"})
    blocks.append({"type": "text", "text": _time_prompt(context)})
    blocks += await content_blocks(parts)

    async def approve(tool: str, reason: str) -> bool:
        allowed = await PermissionUtils.check_superuser(context)
        logger.info(
            f"dsh 请求使用 {tool}（{reason or '无说明'}）: "
            + ("允许" if allowed else "拒绝"),
            MODULE,
            session=context.get_user_id(),
        )
        return allowed

    async def ask(question: str) -> str:
        try:
            return await HITLController(context).ask_text(question, timeout=120)
        except Exception:
            return ""

    token = context.run.cancellation_token or CancellationToken()
    async with session_manager.run_scope(session_id, token, exclusive=True):
        text, usage, reason = await DshRuntime.prompt(
            dsh_id, blocks, approve, ask, settings.timeout
        )
    if reason == "aborted":
        text = ""
    return AgentRunResult(output=text, usage=usage)
