from __future__ import annotations

import ast
import asyncio
from contextlib import asynccontextmanager
import inspect
import json
from typing import Any

import json_repair

from zhenxun.services.ai.capabilities.base import CombinedCapability
from zhenxun.services.ai.core.exceptions import (
    ControlFlowExit,
    ToolFatalError,
    ToolRetryError,
)
from zhenxun.services.ai.core.messages import (
    AudioPart,
    FilePart,
    ImagePart,
    LLMMessage,
    TextPart,
    ToolCallPart,
    ToolMessage,
    UsageInfo,
    VideoPart,
)
from zhenxun.services.ai.core.stream_events import (
    EventBus,
    ToolCallEndEvent,
    ToolCallStartEvent,
)
from zhenxun.services.ai.run.context import RunContext, set_run_context
from zhenxun.services.ai.run.di import DependencyInjector
from zhenxun.services.ai.tools.core.tool import BaseTool
from zhenxun.services.ai.tools.models import ToolResult, ValidatedToolCall
from zhenxun.services.ai.utils.logger import log_tool as logger
from zhenxun.utils.pydantic_compat import dump_json_safely

from .registry import ToolCollection


def _tool_timeout(tool: Any) -> int:
    """工具调用的超时秒数，元数据标记 no_timeout 的工具不限时

    参数:
        tool: 工具

    返回:
        int: 超时秒数，0 表示不限时
    """
    from zhenxun.services.ai.config import get_llm_config

    if (getattr(tool, "metadata", None) or {}).get("no_timeout"):
        return 0
    return get_llm_config().agent_settings.tool_timeout


class ToolExecutor:
    """
    全能工具执行器。
    负责接收工具调用请求，解析参数，触发回调，执行工具，并返回标准化的结果。
    """

    @staticmethod
    def assemble_tool_message(
        original_call: ToolCallPart,
        res_or_exc: BaseException | tuple[ToolCallPart, ToolResult],
        tool_res: ToolResult | None,
        max_chars: int = 0,
    ) -> tuple[ToolMessage, UsageInfo | None]:
        """处理异常、分离多模态片段、序列化并截断结果，装配为工具消息

        参数:
            original_call: 原始工具调用
            res_or_exc: 执行结果或异常
            tool_res: 工具结果
            max_chars: 结果文本的最大字符数，超出时保留首尾、截去中间；0 不限制

        返回:
            tuple[ToolMessage, UsageInfo | None]: 工具消息与工具产生的用量
        """
        media_parts = []
        final_content = "Success"
        usage = None

        if isinstance(res_or_exc, BaseException):
            if isinstance(res_or_exc, ControlFlowExit):
                raise res_or_exc
            final_content = json.dumps(
                {"error": str(res_or_exc), "status": "failed"},
                ensure_ascii=False,
            )
        elif tool_res is not None:
            if isinstance(tool_res.output, list):
                texts = []
                for item in tool_res.output:
                    if isinstance(item, ImagePart | AudioPart | VideoPart | FilePart):
                        media_parts.append(item)
                    elif isinstance(item, TextPart):
                        texts.append(item.text)
                    else:
                        texts.append(str(item))
                final_content = " ".join(texts) if texts else "Success"
            elif isinstance(tool_res.output, str):
                final_content = tool_res.output
            else:
                final_content = dump_json_safely(tool_res.output, ensure_ascii=False)

            usage = getattr(tool_res, "usage", None)

        if max_chars and len(final_content) > max_chars:
            head = max_chars * 2 // 3
            tail = max_chars - head
            cut = len(final_content) - max_chars
            final_content = (
                f"{final_content[:head]}\n...[结果过长，已截去中间 {cut} 字]...\n"
                f"{final_content[-tail:] if tail else ''}"
            )

        msg = LLMMessage.tool_response(
            original_call.id, original_call.tool_name, final_content
        )
        if media_parts:
            msg.content.extend(media_parts)
        return msg, usage

    def _get_combined_capability(
        self, executable: Any, context: RunContext
    ) -> CombinedCapability:
        """合并 Agent 上下文 (已包含 Global) 与 Tool 私有的 Capability"""
        tool_caps = getattr(getattr(executable, "settings", None), "capabilities", [])
        agent_caps = getattr(context, "capabilities", [])

        all_caps = list(agent_caps)
        for c in tool_caps:
            if c not in all_caps:
                all_caps.append(c)
        return CombinedCapability(all_caps)

    @asynccontextmanager
    async def _tool_stream_scope(
        self,
        event_bus: EventBus | None,
        tool_name: str,
        arguments: dict[str, Any],
    ):
        """生命周期上下文管理器：接管事件流的发送与异常包装样板代码"""
        if event_bus:
            await event_bus.emit(
                ToolCallStartEvent(tool_name=tool_name, arguments=arguments)
            )
        result_box: dict[str, Any] = {}
        try:
            yield result_box
        finally:
            if event_bus and "result" in result_box:
                res = result_box["result"]
                await event_bus.emit(
                    ToolCallEndEvent(
                        tool_name=tool_name, result=res, is_error=res.is_error
                    )
                )

    @staticmethod
    def _robust_parse_args(
        args_raw: str | dict[str, Any],
    ) -> tuple[bool, dict[str, Any], str]:
        """容错解析参数纯函数，返回 (是否成功, 解析后的字典, 原始字符串)"""
        if isinstance(args_raw, dict):
            return True, args_raw.copy(), json.dumps(args_raw, ensure_ascii=False)
        args_str = args_raw
        if not args_str.strip():
            return True, {}, args_str
        try:
            parsed = json.loads(args_str)
            if isinstance(parsed, dict):
                return True, parsed, args_str
            return False, {}, args_str
        except json.JSONDecodeError:
            pass
        try:
            parsed = ast.literal_eval(args_str)
            if isinstance(parsed, dict):
                return True, parsed, args_str
        except (ValueError, SyntaxError):
            try:
                repaired_str = str(
                    json_repair.repair_json(args_str, skip_json_loads=True)
                )
                parsed = json.loads(repaired_str)
                if isinstance(parsed, dict):
                    logger.debug(
                        f"⚒️ 成功修复损坏的工具参数: {args_str} -> {repaired_str}"
                    )
                    return True, parsed, args_str
            except Exception:
                pass
        return False, {}, args_str

    def _prepare_tool_context(
        self,
        context: RunContext | None,
        tool_call_id: str,
        tool_name: str,
        executable: Any,
        event_bus: EventBus | None,
    ) -> RunContext:
        """准备/克隆工具调用所使用的隔离 RunContext"""
        safe_context = (
            context.clone_for_tool_call(tool_call_id, tool_name)
            if context
            else RunContext()
        )
        safe_context.run.event_bus = event_bus
        safe_context.call.tool_name = tool_name
        safe_context.call.current_tool = executable
        return safe_context

    async def validate_tool_call(
        self,
        tool_call: ToolCallPart,
        available_tools: ToolCollection | dict[str, Any] | None,
        context: RunContext | None = None,
    ) -> ValidatedToolCall:
        """验证单一工具调用，完成参数解析与类型检查"""
        if not available_tools:
            available_tools = {}
        if not tool_call.tool_name:
            return ValidatedToolCall(
                call=tool_call,
                args_valid=False,
                validation_error=ToolRetryError("tool_call.tool_name 不能为空"),
            )

        tool_name = tool_call.tool_name
        parsed_successfully, arguments, arguments_str = self._robust_parse_args(
            tool_call.args
        )

        if tool_call.args and not parsed_successfully:
            if context:
                context.run.tool_retries[tool_name] = (
                    context.run.tool_retries.get(tool_name, 0) + 1
                )
            return ValidatedToolCall(
                call=tool_call,
                args_valid=False,
                validation_error=ToolRetryError(
                    f"参数JSON解析失败，请检查 JSON 语法是否合法: {arguments_str}"
                ),
            )

        executable = available_tools.get(tool_name)
        if not executable or not hasattr(executable, "execute"):
            return ValidatedToolCall(
                call=tool_call,
                args_valid=False,
                validation_error=ToolRetryError(f"Tool '{tool_name}' not found."),
            )

        try:
            if hasattr(executable, "validate_args"):
                arguments = await executable.validate_args(arguments)
            return ValidatedToolCall(
                call=tool_call,
                tool=executable,
                args_valid=True,
                validated_args=arguments,
            )
        except Exception as e:
            return ValidatedToolCall(
                call=tool_call,
                tool=executable,
                args_valid=False,
                validation_error=e,
            )

    async def execute_tool_call(
        self,
        validated: ValidatedToolCall,
        context: RunContext | None = None,
        event_bus: EventBus | None = None,
    ) -> tuple[ToolCallPart, ToolResult]:
        """核心执行阶段。只接收已经通过 Validation 阶段的 ValidatedToolCall 载体。"""
        tool_name = validated.call.tool_name
        if not validated.args_valid or validated.tool is None:
            if isinstance(validated.validation_error, ControlFlowExit):
                raise validated.validation_error

            err_msg = getattr(
                validated.validation_error, "message", str(validated.validation_error)
            )
            res = ToolResult(output=f"执行被拦截或参数错误: {err_msg}").as_error()
            if event_bus:
                await event_bus.emit(
                    ToolCallEndEvent(
                        tool_name=tool_name, result=res, is_error=res.is_error
                    )
                )
            return validated.call, res

        executable = validated.tool
        arguments = validated.validated_args or {}

        safe_context = self._prepare_tool_context(
            context, validated.call.id, tool_name, executable, event_bus
        )
        combined_cap = self._get_combined_capability(executable, safe_context)

        async def inner_handler(args_inner: dict) -> Any:
            call = executable.execute(context=safe_context, **args_inner)
            if not (timeout := _tool_timeout(executable)):
                return await call
            try:
                return await asyncio.wait_for(call, timeout)
            except asyncio.TimeoutError:
                return ToolResult(output=f"工具执行超时（{timeout} 秒）").as_error()

        async with self._tool_stream_scope(event_bus, tool_name, arguments) as box:
            with set_run_context(safe_context):
                try:
                    result = await combined_cap.wrap_tool_execute(
                        safe_context, tool_name, arguments, inner_handler
                    )
                    if not isinstance(result, ToolResult):
                        result = ToolResult(output=result)
                except BaseException as e:
                    if isinstance(e, ControlFlowExit | asyncio.CancelledError):
                        raise e
                    logger.error(f"洋葱模型异常穿透: {e}")
                    result = ToolResult(output=f"System Fatal Error: {e}").as_error()
            box["result"] = result

        return validated.call, box["result"]

    async def execute_tool_calls(
        self,
        validated_calls: list[ValidatedToolCall],
        context: RunContext | None = None,
        event_bus: EventBus | None = None,
        parallel: bool = True,
    ) -> list[tuple[ToolCallPart, ToolResult] | BaseException]:
        """执行一批工具调用，允许并行时同时执行，否则依次执行

        参数:
            validated_calls: 已校验的工具调用
            context: 运行上下文
            event_bus: 事件总线
            parallel: 是否允许并行

        返回:
            list[tuple[ToolCallPart, ToolResult] | BaseException]: 与调用顺序一致的
                结果，出错的调用对应异常对象；遇到中止或取消后不再执行后续调用
        """
        size = len(validated_calls)
        chunks = [list(range(size))] if parallel else [[i] for i in range(size)]
        results: list[tuple[ToolCallPart, ToolResult] | BaseException] = []
        for chunk in chunks:
            chunk_results = await asyncio.gather(
                *[
                    self.execute_tool_call(validated_calls[i], context, event_bus)
                    for i in chunk
                ],
                return_exceptions=True,
            )
            results.extend(chunk_results)
            stop = next(
                (
                    r
                    for r in chunk_results
                    if isinstance(r, ControlFlowExit | asyncio.CancelledError)
                ),
                None,
            )
            if stop is not None:
                results.extend([stop] * (size - len(results)))
                break
        return results

    async def execute_batch(
        self,
        tool_calls: list[ToolCallPart],
        available_tools: ToolCollection | dict[str, Any] | None,
        context: RunContext | None = None,
    ) -> list[ToolMessage]:
        """校验并并行执行一批工具调用，结果装配为工具消息（外部插件使用的旧接口）

        参数:
            tool_calls: 模型给出的工具调用
            available_tools: 可用工具
            context: 运行上下文

        返回:
            list[ToolMessage]: 与调用顺序一致的工具消息，出错的调用为错误结果
        """
        validated = await asyncio.gather(
            *[self.validate_tool_call(c, available_tools, context) for c in tool_calls]
        )
        results = await self.execute_tool_calls(list(validated), context)
        return [
            self.assemble_tool_message(
                call, result, result[1] if isinstance(result, tuple) else None
            )[0]
            for call, result in zip(tool_calls, results)
        ]


async def run_native_tool(
    tool: BaseTool, context: RunContext, **kwargs: Any
) -> ToolResult:
    """注入依赖后运行工具函数，非 ToolResult 的返回值包装为 ToolResult

    参数:
        tool: 工具
        context: 运行上下文
        **kwargs: 已校验的工具参数

    返回:
        ToolResult: 工具结果
    """
    try:
        call_kwargs = await DependencyInjector.resolve_all(
            sig=inspect.signature(tool.get_signature_target()),
            call_kwargs=dict(kwargs),
            context=context,
        )
    except ValueError as e:
        logger.error(f"工具 {tool.name} 依赖注入失败: {e}", e=e)
        raise ToolFatalError(f"框架依赖注入失败: {e}")
    res = await tool.get_execute_target()(**call_kwargs)
    return res if isinstance(res, ToolResult) else ToolResult(output=res)
