"""MCP 服务管理：增删改、启用停用、导入与连接测试"""

from __future__ import annotations

import asyncio
import json
import re
from typing import Any, Literal

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from zhenxun.services.ai.tools.providers.mcp.provider import (
    MCPServerConfig,
    mcp_provider,
)
from zhenxun.services.log import logger
from zhenxun.utils.pydantic_compat import model_dump, model_validate

from ....base_model import Result
from ....utils import authentication

router = APIRouter()

NAME_PATTERN = r"^[A-Za-z0-9_-]{1,40}$"
"""服务名格式"""
STDIO_TEST_TIMEOUT = 120
"""stdio 服务测试超时秒数，首次运行 npx / uvx 需要下载"""


class ServerPayload(BaseModel):
    transport: Literal["stdio", "sse", "streamable-http"] = "stdio"
    command: str | None = Field(default=None, max_length=500)
    args: list[str] = Field(default_factory=list, max_length=100)
    env: dict[str, str] | None = None
    cwd: str | None = Field(default=None, max_length=500)
    install_command: str | None = Field(default=None, max_length=500)
    url: str | None = Field(default=None, max_length=1000)
    headers: dict[str, str] | None = None
    timeout: int = Field(default=30, ge=1, le=600)
    description: str | None = Field(default=None, max_length=500)
    enabled: bool = True
    admin_level: int = Field(default=0, ge=0, le=9)


class SavePayload(ServerPayload):
    original: str | None = Field(default=None, max_length=40)
    """改名前的服务名"""


class TogglePayload(BaseModel):
    enabled: bool


class TestPayload(BaseModel):
    name: str = Field(pattern=NAME_PATTERN)
    config: ServerPayload


class ImportPayload(BaseModel):
    text: str = Field(min_length=2, max_length=200_000)


def _config(payload: ServerPayload | dict[str, Any]) -> MCPServerConfig:
    """校验并转为服务配置：stdio 需要启动命令，远端服务需要 http(s) 地址

    参数:
        payload: 请求内容或 mcp.json 中的一项

    返回:
        MCPServerConfig: 服务配置
    """
    data = payload if isinstance(payload, dict) else model_dump(payload)
    data = {k: v for k, v in data.items() if k != "original"}
    try:
        conf = model_validate(MCPServerConfig, data)
    except Exception as e:
        raise HTTPException(422, detail=f"配置无效：{e}") from e
    if conf.transport == "stdio":
        if not (conf.command or "").strip():
            raise HTTPException(422, detail="stdio 服务需要填写启动命令")
        conf.url, conf.headers = None, None
    else:
        if not (conf.url or "").startswith(("http://", "https://")):
            raise HTTPException(422, detail="远端服务需要填写 http(s) 地址")
        conf.command, conf.args, conf.env = None, [], None
        conf.cwd, conf.install_command = None, None
    return conf


def _check_name(name: str) -> None:
    if not re.fullmatch(NAME_PATTERN, name):
        raise HTTPException(422, detail="服务名只能包含字母、数字、下划线和短横线")


def _servers() -> dict[str, Any]:
    known = mcp_provider.known_tools()
    return {
        "servers": [
            {
                "name": name,
                **model_dump(conf),
                "prefix": mcp_provider.prefix(name),
                "tools": [
                    {"name": tool, "description": desc}
                    for tool, desc in known.get(name, {}).items()
                ]
                if name in known
                else None,
            }
            for name, conf in sorted(mcp_provider.servers().items())
        ]
    }


@router.get("/mcp/servers", dependencies=[authentication()], response_model=Result)
async def list_servers() -> Result:
    await mcp_provider.initialize()
    return Result.ok(_servers())


@router.put(
    "/mcp/servers/{name}", dependencies=[authentication()], response_model=Result
)
async def save_server(name: str, payload: SavePayload) -> Result:
    _check_name(name)
    conf = _config(payload)
    await mcp_provider.initialize()
    servers = mcp_provider.servers()
    if payload.original and payload.original not in servers:
        raise HTTPException(404, detail=f"MCP 服务 {payload.original} 不存在")
    if name in servers and payload.original != name:
        raise HTTPException(409, detail=f"MCP 服务 {name} 已存在")
    if payload.original and payload.original != name:
        await mcp_provider.unregister_server(payload.original)
    await mcp_provider.save_server(name, conf)
    logger.info(f"WebUI 保存了 MCP 服务 {name}", "WebUi")
    return Result.ok(_servers(), info=f"MCP 服务 {name} 已保存。")


@router.delete(
    "/mcp/servers/{name}", dependencies=[authentication()], response_model=Result
)
async def delete_server(name: str) -> Result:
    await mcp_provider.initialize()
    if name not in mcp_provider.servers():
        raise HTTPException(404, detail=f"MCP 服务 {name} 不存在")
    await mcp_provider.unregister_server(name)
    logger.info(f"WebUI 删除了 MCP 服务 {name}", "WebUi")
    return Result.ok(_servers(), info=f"MCP 服务 {name} 已删除。")


@router.post(
    "/mcp/servers/{name}/toggle", dependencies=[authentication()], response_model=Result
)
async def toggle_server(name: str, payload: TogglePayload) -> Result:
    if not await mcp_provider.set_enabled(name, payload.enabled):
        raise HTTPException(404, detail=f"MCP 服务 {name} 不存在")
    state = "启用" if payload.enabled else "停用"
    return Result.ok(_servers(), info=f"MCP 服务 {name} 已{state}。")


@router.post("/mcp/import", dependencies=[authentication()], response_model=Result)
async def import_servers(payload: ImportPayload) -> Result:
    text = payload.text.strip()
    if text.startswith("```"):
        text = text.strip("`").partition("\n")[2]
    try:
        data = json.loads(text)
    except json.JSONDecodeError as e:
        raise HTTPException(422, detail=f"JSON 解析失败：{e}") from e
    servers = data.get("mcpServers", data) if isinstance(data, dict) else None
    if not isinstance(servers, dict) or not servers:
        raise HTTPException(422, detail='需要 {"mcpServers": {服务名: 配置}} 格式')
    configs = {}
    for name, item in servers.items():
        _check_name(str(name))
        if not isinstance(item, dict):
            raise HTTPException(422, detail=f"MCP 服务 {name} 的配置不是对象")
        if "url" in item and "type" not in item and "transport" not in item:
            item = {**item, "transport": "streamable-http"}
        configs[str(name)] = _config(item)
    for name, conf in configs.items():
        await mcp_provider.save_server(name, conf)
    logger.info(f"WebUI 导入了 MCP 服务 {'、'.join(configs)}", "WebUi")
    return Result.ok(_servers(), info=f"已导入 {len(configs)} 个 MCP 服务。")


@router.post("/mcp/test", dependencies=[authentication()], response_model=Result)
async def test_server(payload: TestPayload) -> Result:
    conf = _config(payload.config)
    timeout = STDIO_TEST_TIMEOUT if conf.transport == "stdio" else conf.timeout + 5
    try:
        tools = await mcp_provider.test(payload.name, conf, timeout)
    except asyncio.TimeoutError as e:
        raise HTTPException(422, detail=f"连接超时（{timeout} 秒）") from e
    except Exception as e:
        cause: Any = e
        while getattr(cause, "exceptions", None):
            cause = cause.exceptions[0]
        raise HTTPException(422, detail=f"连接失败：{cause}") from e
    return Result.ok(
        {
            "tools": [{"name": k, "description": v} for k, v in tools.items()],
            **_servers(),
        },
        info=f"连接成功，获取到 {len(tools)} 个工具。",
    )
