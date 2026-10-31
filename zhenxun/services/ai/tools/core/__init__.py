"""工具核心"""

from zhenxun.services.ai import forward

__getattr__ = forward(
    {
        "BaseTool": "zhenxun.services.ai.tools.core.tool",
        "FunctionTool": "zhenxun.services.ai.tools.core.tool",
    }
)
