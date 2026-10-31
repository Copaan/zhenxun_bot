from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field, PrivateAttr

from zhenxun.services.ai.capabilities.base import CombinedCapability
from zhenxun.services.ai.context.memory.engine import SessionMemoryContext
from zhenxun.services.ai.core.messages import (
    ChatResponse,
    LLMMessage,
    ToolCallPart,
    UsageInfo,
)
from zhenxun.services.ai.core.models import ModelProfile
from zhenxun.services.ai.core.options import GenerationConfig
from zhenxun.services.ai.run.context import RunContext
from zhenxun.services.ai.run.models import AgentRunResult
from zhenxun.services.ai.tools.engine.registry import ToolCollection


class AgentConfig(BaseModel):
    """智能体运行配置，未显式设置的循环相关字段跟随全局 agent_settings"""

    max_cycles: int = Field(default=10)
    """工具调用最大循环次数"""
    enable_parallel_calls: bool = Field(default=True)
    """允许并行工具调用"""
    reflexion_retries: int = Field(default=1)
    """工具未单独配置 max_retries 时允许的连续出错次数"""
    enable_fallback_summary: bool = Field(default=True)
    """达到最大循环次数时，是否触发大模型兜底总结（而不是直接报错）"""
    follow_up: bool = Field(default=False)
    """同一会话正在运行时，新消息追加到当前任务结束后继续处理，而不是排队另起一轮"""


class AgentState(BaseModel):
    """大模型思考循环的流转状态"""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    static_system_prompt: list[str] = Field(default_factory=list)
    """不变的系统提示词（用于前缀缓存）"""
    reminders: list[str] = Field(default_factory=list)
    """每次运行变化的提示（时间、群背景、检索结果等），只贴在本轮用户消息末尾"""
    tools: ToolCollection | None = None
    """当前生效的、已完成鉴权和过滤的工具集合"""

    _origin_msg_len: int = PrivateAttr(default=0)
    """进入循环时的消息历史长度，用于增量保存记忆"""
    _pending_input: LLMMessage | None = PrivateAttr(default=None)
    """含媒体的本轮输入，执行后与回复一起写入记忆，以便保存媒体转换出的文字"""
    _input: LLMMessage | None = PrivateAttr(default=None)
    """本轮用户输入，提示块贴在它的末尾"""

    current_cycle: int = 0
    """当前思考循环的轮次索引"""
    current_request_messages: list[LLMMessage] = Field(default_factory=list)
    """当前即将发往大模型的实际请求消息"""
    current_response: ChatResponse | None = None
    """大模型最新返回的响应实体"""
    current_tool_calls: list[ToolCallPart] = Field(default_factory=list)
    """当前轮次准备在本地执行的工具调用"""
    current_tool_results: list[Any] = Field(default_factory=list)
    """当前轮次工具执行的结果或异常收集"""

    should_reset_cycle: bool = False
    """是否需要重置循环计数（追加了用户消息）"""
    is_finished: bool = False
    """大模型循环是否结束"""
    pending_result: AgentRunResult[Any] | None = None
    """等待返回的最终运行结果"""
    usage: UsageInfo = Field(default_factory=UsageInfo)
    """累计的 Token 消耗"""
    token_drift: int = 0
    """动态 Token 校准偏移量 (真实 - 预估)"""


class AgentRunResources(BaseModel):
    """大模型执行过程中的静态资源与配置载体"""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    run_context: RunContext
    """运行时上下文"""
    memory_context: SessionMemoryContext | None = None
    """统一处理对话历史读写与压缩的会话记忆门面，无状态运行时为空"""
    capability: CombinedCapability
    """本次运行生效的组合能力"""
    config: AgentConfig = Field(default_factory=AgentConfig)
    """本次运行生效的配置"""
    generation_config: GenerationConfig | None = None
    """大模型生成配置"""
    model_profile: ModelProfile | None = None
    """当前模型的能力档案"""
