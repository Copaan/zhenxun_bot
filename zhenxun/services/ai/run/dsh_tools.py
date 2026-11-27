"""在当前 DSH 运行内解析真寻工具、复核权限并结算重复回调。"""

import asyncio
import hashlib
import json
import secrets

from nonebot import get_driver

from zhenxun.services.ai.capabilities.base import CombinedCapability
from zhenxun.services.ai.capabilities.builtin import default_capabilities
from zhenxun.services.ai.core.messages import ImagePart, TextPart, ToolCallPart
from zhenxun.services.ai.run.context import set_run_context
from zhenxun.services.ai.tools.engine.executor import ToolExecutor
from zhenxun.services.ai.utils.runtime import PermissionUtils

AGENT_TOOLS_KEY = "_agent_tools"
"""Agent 能力记录本轮敏感工具名的状态键"""
REST_API_MODULE = "zhenxun.services.ai.tools.providers.builtin.rest_api"
"""REST API 工具所在模块"""
IMAGE_LIMIT = 8 * 1024 * 1024
"""单次工具结果中图片的总大小上限"""
RESULT_LIMIT = 16 * 1024 * 1024
"""一轮内桥接工具结果的总大小上限"""
EXECUTOR_TOOL_NAMES = {
    "run_code",
    "ask_user_question",
    "zx_list",
    "zx_read",
    "zx_write",
}
"""执行端保留或自带的工具名，同名工具不桥接"""


class ChatToolBridge:
    """保存在根运行内的定义快照、执行权限及调用收据。"""

    def __init__(self, agent, context):
        self.agent = agent
        self.context = context
        self.capability = CombinedCapability(
            [*default_capabilities(), *context.capabilities, *agent.capabilities]
        )
        self.lock = asyncio.Lock()
        self.receipts = {}
        self.identities = {}
        self.definitions = []
        self.instructions = ""
        self.reminders = []
        self.reply_actions = {}
        self.result_bytes = 0

    def allowed(self, tool) -> bool:
        """判断工具能否桥接到 DSH

        聊天 Agent 自身的工具除子代理外全部桥接；Agent 能力提供的敏感工具只在
        agent 模式下桥接 MCP 与已配置地址的 REST API，沙箱和向用户确认由 DSH
        自带工具承担。

        参数:
            tool: 已解析的工具

        返回:
            bool: 是否桥接
        """
        if (
            getattr(tool, "execution_side", None) != "client"
            or (tool.metadata or {}).get("delegates")
            or tool.name in EXECUTOR_TOOL_NAMES
        ):
            return False
        if tool.name not in self.context.state.get(AGENT_TOOLS_KEY, ()):
            return True
        if self.context.state["_dsh_binding"]["mode"] != "agent":
            return False
        if tool.identity.startswith("mcp:"):
            return True
        target = tool.get_signature_target()
        return target.__module__ == REST_API_MODULE and bool(
            getattr(getattr(target, "__self__", None), "base_url", None)
        )

    async def prepare(self) -> None:
        """解析本轮可桥接的工具，记录工具身份、定义与说明

        后续调用只能使用这里记录的工具身份，权限复核只会收窄范围。
        """
        tools, payload = await self.agent.resolve_run_tools(
            self.context, self.capability, include=self.allowed
        )
        self.identities = {t.name: t.identity for t in tools}
        self.reply_actions = {
            t.name: t.metadata["reply_action"]
            for t in tools
            if (t.metadata or {}).get("reply_action")
        }
        self.definitions = [
            (await t.get_definition(self.context)).model_dump(
                include={"name", "description", "parameters"}
            )
            for t in tools
        ]
        self.instructions = "\n".join(payload.injected_prompts)
        binding = self.context.state["_dsh_binding"]
        if not (binding.get("native") and binding.get("inherited")):
            self.reminders = list(dict.fromkeys(payload.reminders))
        self.context.state["_injections"] = {
            "system": {
                "count": int(bool(self.instructions)),
                "characters": len(self.instructions),
                "temporary": True,
            },
            "reminders": {
                "count": len(self.reminders),
                "characters": sum(map(len, self.reminders)),
                "temporary": True,
            },
            "tools": sorted(self.identities),
            "sources": list(payload.injection_sources),
        }

    async def call(self, turn, call_id: str, name: str, arguments: dict) -> dict:
        """复核本轮授权后执行一次桥接工具调用，同一调用编号只执行一次

        参数:
            turn: 本轮 DSH 执行
            call_id: DSH 的工具调用编号
            name: 工具名
            arguments: 工具参数

        返回:
            dict: blocks 为文字或图片结果，error 表示工具是否失败
        """
        from .dsh import DshSessions

        context = self.context
        async with self.lock:
            PermissionUtils.ensure_current(context)
            binding = turn.binding
            record = (
                DshSessions.for_binding(binding)
                ._read()["owners"]
                .get(binding["owner"], {})
            )
            if (
                turn.revoked
                or name not in self.identities
                or record.get("local_id") != binding["local_id"]
                or record.get("remote_id") != binding["remote_id"]
                or (record.get("inflight") or {}).get("run_id") != context.run.run_id
                or await context.state["_dsh_authorization_check"]() != binding["mode"]
            ):
                raise PermissionError
            with set_run_context(context):
                context.state["_tool_recheck"] = True
                try:
                    tools, _ = await self.agent.resolve_run_tools(
                        context, self.capability, include=self.allowed
                    )
                finally:
                    context.state.pop("_tool_recheck", None)
                tool = tools.get(name)
                if turn.revoked or not tool or tool.identity != self.identities[name]:
                    raise PermissionError
                digest = hashlib.sha256(
                    json.dumps([name, arguments], sort_keys=True).encode()
                ).digest()
                if saved := self.receipts.get(call_id):
                    if saved[0] != digest:
                        raise PermissionError
                    return saved[1]
                if len(self.receipts) >= 128:
                    raise PermissionError
                self.receipts[call_id] = (
                    digest,
                    {
                        "error": True,
                        "blocks": [
                            {"type": "text", "text": "执行结果待核验，不能重试。"}
                        ],
                    },
                )
                executor = ToolExecutor()
                call = ToolCallPart(id=call_id, tool_name=name, args=arguments)
                checked = await executor.validate_tool_call(call, tools, context)
                tool_context = context.clone_for_tool_call(call_id, name)
                tool_context.capabilities = self.capability.capabilities
                _, result = await executor.execute_tool_call(
                    checked, tool_context, context.run.event_bus
                )
                out = (
                    result.output
                    if isinstance(result.output, list)
                    else [result.output]
                )
                blocks = []
                image_bytes = 0
                failed = result.is_error
                for item in out:
                    if isinstance(item, ImagePart):
                        from .dsh import content_blocks
                        from .dsh_desktop import DshUnavailable

                        try:
                            image = (await content_blocks([item], transport="desktop"))[
                                0
                            ]
                        except DshUnavailable as exc:
                            failed = True
                            blocks.append({"type": "text", "text": str(exc)})
                            continue
                        image_bytes += len(image["data"])
                        if image_bytes > IMAGE_LIMIT:
                            blocks.append(
                                {"type": "text", "text": "图片结果过大，未返回图片。"}
                            )
                            continue
                        blocks.append(image)
                    else:
                        text = (
                            item.text
                            if isinstance(item, TextPart)
                            else item
                            if isinstance(item, str)
                            else json.dumps(item, ensure_ascii=False)
                            if isinstance(item, dict | list | int | float | bool)
                            else "不支持该附件类型。"
                        )
                        blocks.append({"type": "text", "text": text[:16000]})
                response = {"blocks": blocks, "error": failed}
                size = sum(
                    len(b.get("data", "")) + len(b.get("text", "").encode())
                    for b in blocks
                )
                if self.result_bytes + size > RESULT_LIMIT:
                    response = {
                        "blocks": [
                            {
                                "type": "text",
                                "text": "工具结果过大，不要重复执行。",
                            }
                        ],
                        "error": True,
                    }
                else:
                    self.result_bytes += size
                self.receipts[call_id] = digest, response
                return response


def bridge_lease(turn) -> dict | None:
    """生成本轮工具回调的凭据与工具定义，只交给执行器，不进入提示词

    参数:
        turn: 本轮 DSH 执行

    返回:
        dict | None: 回调凭据，本轮没有工具桥接时为空
    """
    bridge = turn.context.state.get("_dsh_chat_tools")
    if not bridge:
        return None
    return {
        "port": int(get_driver().config.port),
        "sessionId": turn.binding["remote_id"],
        "requestId": turn.request_id,
        "runId": turn.context.run.run_id,
        "token": turn.token,
        "definitions": bridge.definitions,
        "searchMode": bridge.context.state.get("_dsh_search_source", "off"),
        "replyProtocol": 1,
        "replyActions": bridge.reply_actions,
    }


async def invoke_bridge(token: str, payload) -> dict:
    """核对执行器的本轮凭据后执行桥接工具调用

    参数:
        token: 执行器携带的本轮凭据
        payload: 工具调用请求，含会话、运行、请求与调用编号、工具名和参数

    返回:
        dict: 工具结果

    异常:
        PermissionError: 凭据、运行或会话不匹配，或本轮已撤销
    """
    from .dsh import DshRuntime

    turn = DshRuntime._turns.get(payload.runId)
    if (
        not turn
        or turn.revoked
        or not secrets.compare_digest(turn.token, token)
        or turn.request_id != payload.requestId
        or turn.binding["remote_id"] != payload.sessionId
    ):
        raise PermissionError
    bridge = turn.context.state.get("_dsh_chat_tools")
    if not bridge:
        raise PermissionError
    if bridge.reply_actions and getattr(payload, "replyProtocol", None) != 1:
        raise PermissionError("DSH 发送协议已变化，请重新加载真寻工具集。")
    task = asyncio.current_task()
    turn.bridge_tasks.add(task)
    try:
        return await bridge.call(turn, payload.callId, payload.name, payload.arguments)
    finally:
        turn.bridge_tasks.discard(task)
