"""MCP 工具"""

from zhenxun.services.ai import forward

__getattr__ = forward({"MCPSource": "zhenxun.services.ai.tools.providers.mcp.provider"})
