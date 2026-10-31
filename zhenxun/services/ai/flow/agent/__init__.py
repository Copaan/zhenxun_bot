"""Agent 运行时"""

from zhenxun.services.ai import forward

__getattr__ = forward({"Agent": "zhenxun.services.ai.flow.agent.agent"})
