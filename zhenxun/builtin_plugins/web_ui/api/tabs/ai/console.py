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
    """把 Agent 事件转为推送给前端的数据

    参数:
        event: Agent 流事件

    返回:
        dict[str, Any] | None: 推送数据，不需要推送的事件为空
    """
    from zhenxun.services.ai.core.messages import TextPart, ThoughtPart
    from zhenxun.services.ai.core.stream_events import (
        LLMEndEvent,
        TextDeltaEvent,
        ToolCallEndEvent,
        ToolCallStartEvent,
    )

    if isinstance(event, TextDeltaEvent):
        return {"type": "delta", "kind": event.kind, "text": event.text}
    if isinstance(event, LLMEndEvent):
        parts = getattr(event.response, "content_parts", None) or []
        return {
            "type": "turn",
            "text": "".join(p.text for p in parts if isinstance(p, TextPart)),
            "thought": "".join(
                p.thought_text for p in parts if isinstance(p, ThoughtPart)
            ),
        }
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
    payload: ConsoleRequest, persona: Any, queue: asyncio.Queue[dict | None]
) -> None:
    """运行一次控制台对话，事件与结果写入队列，结束时写入 None

    参数:
        payload: 请求
        persona: 人设
        queue: 推送队列
    """
    from zhenxun.builtin_plugins.ai_chat._agent import (
        AGENT_ACTIVE_KEY,
        USER_KEY,
        record_run,
        start_run,
    )
    from zhenxun.builtin_plugins.ai_chat._data_source import AIChatManager
    from zhenxun.services.ai.core.exceptions import ControlFlowExit, LLMException
    from zhenxun.services.ai.core.stream_events import STREAM_TEXT_KEY
    from zhenxun.services.ai.run.context import RunContext

    context = RunContext(session_id=console_session(payload.persona_id))
    context.state[STREAM_TEXT_KEY] = True
    context.state[AGENT_ACTIVE_KEY] = payload.agent
    context.state[USER_KEY] = "WebUI"
    run_id = start_run(context)
    name = persona.name if persona else None
    started = time.monotonic()
    result = error = None
    try:
        agent = AIChatManager.get_agent(persona)
        async with agent.run_stream(payload.message, context=context) as stream:
            async for event in stream.stream_events():
                if item := _event_item(event):
                    await queue.put(item)
            result = await stream.get_run_result()
    except (asyncio.CancelledError, ControlFlowExit, LLMException) as e:
        error = e
    except Exception as e:
        logger.error("控制台对话失败", "WebUi", e=e)
        error = e
    finally:
        await record_run(
            context, "webui", name, payload.message, started, result, error
        )
        done: dict[str, Any] = {
            "type": "done",
            "run_id": run_id,
            "duration": int((time.monotonic() - started) * 1000),
            "output": str(result.output) if result else "",
            "tokens": result.usage.total_tokens if result else 0,
        }
        if isinstance(error, asyncio.CancelledError | ControlFlowExit):
            done["stopped"] = str(error) or "已停止"
        elif isinstance(error, LLMException):
            done["error"] = error.user_friendly_message
        elif error is not None:
            done["error"] = f"{type(error).__name__}: {error}"
        await queue.put(done)
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
    task = asyncio.create_task(_run(payload, persona, queue))
    _tasks[session] = task

    async def events() -> AsyncIterator[str]:
        try:
            while (item := await queue.get()) is not None:
                yield f"data: {json.dumps(item, ensure_ascii=False)}\n\n"
        finally:
            if not task.done():
                task.cancel()
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
        count += 1
    return Result.ok(info="已停止。" if count else "当前没有进行中的对话。")
