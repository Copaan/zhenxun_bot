"""AI 运行上下文"""

from zhenxun.services.ai import forward

__getattr__ = forward(
    {
        "NoneBotDeps": "zhenxun.services.ai.run.context",
        "RunContext": "zhenxun.services.ai.run.context",
    }
)
