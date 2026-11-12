"""AI 控制台：超级用户在 WebUI 与 AI 聊天或 Agent 对话，流式输出、停止与内联审批"""

import asyncio
from collections.abc import AsyncIterator
import json
import time
from typing import Any

from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from zhenxun.services.log import logger

from ....base_model import Result
from ....utils import authentication

router = APIRouter()

CONSOLE_PREFIX = "webui_console:"
"""控制台会话id前缀，后接人设id（默认人设为 default）"""
_tasks: dict[str, asyncio.Task] = {}


class ConsoleRequest(BaseModel):
    message: str = Field(min_length=1, max_length=8000)
    persona_id: int | None = None
    agent: bool = False


class ConsoleStopRequest(BaseModel):
    persona_id: int | None = None


def console_session(persona_id: int | None) -> str:
    """控制台会话id

    参数:
        persona_id: 人设id，为空时为默认人设

    返回:
        str: 会话id
    """
    return f"{CONSOLE_PREFIX}{persona_id if persona_id is not None else 'default'}"


def _shorten(value: Any, limit: int = 2000) -> str:
    text = (
        value
        if isinstance(value, str)
        else json.dumps(value, ensure_ascii=False, default=str)
    )
    return text if len(text) <= limit else f"{text[:limit]}…"


def _event_item(event: Any) -> dict[str, Any] | None:
    """把 Agent 工具事件转为推送给前端的数据，回复正文只随结束事件推送

    参数:
        event: Agent 流事件

    返回:
        dict[str, Any] | None: 推送数据，不需要推送的事件为空
    """
    from zhenxun.services.ai.core.stream_events import (
        ToolCallEndEvent,
        ToolCallStartEvent,
    )

    if isinstance(event, ToolCallStartEvent):
        return {
            "type": "tool_start",
            "name": event.tool_name,
            "arguments": _shorten(event.arguments),
        }
    if isinstance(event, ToolCallEndEvent):
        result = getattr(event.result, "output", event.result)
        return {
            "type": "tool_end",
            "name": event.tool_name,
            "result": _shorten(result),
            "is_error": event.is_error,
        }
    return None


async def _run(
    payload: ConsoleRequest,
    persona: Any,
    queue: asyncio.Queue[dict | None],
    receipt: dict | None = None,
) -> None:
    """在会话锁内运行一次控制台对话：工具事件实时推送，回复正文随结束事件推送，
    等待前端收到结束事件后按实际送达的内容结算历史与运行记录，最后写入 None

    参数:
        payload: 请求
        persona: 人设
        queue: 推送队列
        receipt: 推送确认，accepted 为前端已收到结束事件
    """
    from zhenxun.builtin_plugins.ai_chat._agent import (
        AGENT_ACTIVE_KEY,
        USER_KEY,
        bind_source,
    )
    from zhenxun.builtin_plugins.ai_chat._data_source import AIChatManager
    from zhenxun.builtin_plugins.ai_chat._reply import parse_silent
    from zhenxun.builtin_plugins.ai_chat._safety import Safety
    from zhenxun.builtin_plugins.ai_chat._turn import FAILED_NOTICE, ChatTurn
    from zhenxun.services.ai.core.exceptions import ControlFlowExit
    from zhenxun.services.ai.run.context import RunContext

    context = RunContext(session_id=console_session(payload.persona_id))
    bind_source(context, "webui")
    context.state[AGENT_ACTIVE_KEY] = payload.agent
    context.state[USER_KEY] = "WebUI"
    context.state["_retrieval_query"] = payload.message
    agent = AIChatManager.get_agent(persona)
    turn = ChatTurn(context, agent, "webui", persona, payload.message, "WebUI")
    receipt = receipt if receipt is not None else {"event": asyncio.Event()}
    finalized = False

    async def before_run():
        if Safety.check_input(payload.message):
            raise ValueError("输入未通过安全检查")

    async def finalize(result, error=None):
        nonlocal finalized
        if finalized:
            return
        finalized = True
        turn.result = result
        output = parse_silent(str(result.output)) if result else None
        if not output and not isinstance(
            error, asyncio.CancelledError | ControlFlowExit
        ):
            output = (
                context.run.user_notice
                or (persona and persona.error_message)
                or FAILED_NOTICE
            )
        output = Safety.filter_output(output or "")[0]
        done = {
            "type": "done",
            "run_id": turn.run_id,
            "duration": int((time.monotonic() - turn.started) * 1000),
            "output": output,
            "tokens": turn.tokens,
        }
        if error is not None:
            done[
                "stopped"
                if isinstance(error, asyncio.CancelledError | ControlFlowExit)
                else "error"
            ] = (
                "已停止"
                if isinstance(error, asyncio.CancelledError | ControlFlowExit)
                else "本轮未完成，请查看运行记录或稍后重试。"
            )
        try:
            await queue.put(done)
            if not isinstance(error, asyncio.CancelledError):
                await asyncio.wait_for(receipt["event"].wait(), 5)
        except asyncio.TimeoutError:
            pass
        except asyncio.CancelledError as exc:
            error = exc
            raise
        finally:
            if not output:
                turn.delivery.status = "skipped"
            elif receipt.get("accepted"):
                turn.delivery.status, turn.delivery.parts = "sent", [output]
            else:
                turn.delivery.status, turn.delivery.uncertain = "unknown", True
            await turn.settle(error)

    try:
        async with agent.run_stream(
            payload.message, context=context, finalize=finalize, before_run=before_run
        ) as stream:
            async for event in stream.stream_events():
                if item := _event_item(event):
                    await queue.put(item)
            await stream.get_run_result()
    except asyncio.CancelledError as error:
        if not finalized:
            await finalize(None, error)
        raise
    except (ControlFlowExit, Exception) as error:
        if not finalized:
            await finalize(None, error)
        logger.warning(f"控制台对话结束: {type(error).__name__}", "WebUi")
    finally:
        await queue.put(None)


@router.post("/console/run", dependencies=[authentication()])
async def console_run(payload: ConsoleRequest) -> StreamingResponse:
    from zhenxun.models.ai_persona import AIPersona

    persona = None
    if payload.persona_id is not None:
        persona = await AIPersona.get_or_none(id=payload.persona_id)
        if persona is None:
            raise HTTPException(404, detail={"code": "persona_not_found"})
    session = console_session(payload.persona_id)
    if (task := _tasks.get(session)) and not task.done():
        raise HTTPException(409, detail={"code": "console_busy"})
    queue: asyncio.Queue[dict | None] = asyncio.Queue()
    receipt: dict[str, Any] = {"event": asyncio.Event()}
    task = asyncio.create_task(_run(payload, persona, queue, receipt))
    _tasks[session] = task

    async def events() -> AsyncIterator[str]:
        try:
            while (item := await queue.get()) is not None:
                yield f"data: {json.dumps(item, ensure_ascii=False)}\n\n"
                if item.get("type") == "done":
                    receipt["accepted"] = True
                    receipt["event"].set()
        finally:
            if not task.done():
                task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            if _tasks.get(session) is task:
                _tasks.pop(session, None)

    return StreamingResponse(
        events(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@router.post("/console/stop", dependencies=[authentication()], response_model=Result)
async def console_stop(payload: ConsoleStopRequest) -> Result:
    from zhenxun.builtin_plugins.ai_chat._data_source import AIChatManager

    session = console_session(payload.persona_id)
    task = _tasks.get(session)
    count = await AIChatManager.stop(session)
    if task and not task.done():
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        count += 1
    return Result.ok(info="已停止。" if count else "当前没有进行中的对话。")
