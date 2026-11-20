"""AI 控制台：超级用户在 WebUI 与 AI 聊天或 Agent 对话，流式输出、停止与内联审批"""

import asyncio
from collections.abc import AsyncIterator
import hashlib
import json
import time
from typing import Any
import uuid

import anyio
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from zhenxun.services.log import logger

from ....base_model import Result
from ....utils import authentication

router = APIRouter()

CONSOLE_PREFIX = "webui_console:"
"""控制台会话id前缀，后接人设id（默认人设为 default）"""
_tasks: dict[str, asyncio.Task] = {}


class ConsoleEntry(BaseModel):
    owner: str | None = Field(default=None, max_length=64)
    agent: bool = False


class ConsoleRequest(ConsoleEntry):
    message: str = Field(min_length=1, max_length=8000)
    persona_id: int | None = None
    session_id: str | None = Field(default=None, max_length=255)
    executor: str | None = Field(default=None, max_length=16)


class ConsoleStopRequest(BaseModel):
    persona_id: int | None = None
    run_id: str | None = Field(default=None, max_length=32)


class ConsoleAnswerRequest(BaseModel):
    run_id: str = Field(min_length=32, max_length=32)
    question_id: str = Field(min_length=32, max_length=32)
    answer: str = Field(min_length=1, max_length=4000)


@router.post("/console/answer", dependencies=[authentication()], response_model=Result)
async def console_answer(payload: ConsoleAnswerRequest, request: Request) -> Result:
    from zhenxun.services.ai.run.session import session_manager
    from zhenxun.services.ai.utils.runtime import PermissionUtils

    subject = management_subject(request)
    entry = session_manager.runs.get(payload.run_id)
    context = entry[0] if entry else None
    if (
        not context
        or not context.run.caller
        or context.run.caller.source != "webui"
        or context.run.caller.user_id != f"webui:{subject}"
    ):
        raise HTTPException(409, detail={"code": "question_not_current"})
    try:
        PermissionUtils.ensure_current(context)
    except (asyncio.CancelledError, asyncio.TimeoutError) as exc:
        raise HTTPException(409, detail={"code": "question_not_current"}) from exc
    pending = context.state.get("_dsh_question")
    if not pending or pending[0] != payload.question_id or pending[1].done():
        raise HTTPException(409, detail={"code": "question_not_current"})
    pending[1].set_result(payload.answer)
    return Result.ok(info="已提交本轮回答。")


def management_subject(request: Request) -> str:
    """读取已经验证的管理账号主体，不转换为 QQ 身份。"""
    from ....security import decode_access_token_status

    principal, status = decode_access_token_status(
        request.headers.get("Authorization", "")
    )
    if status != "valid" or not principal:
        raise HTTPException(401, detail={"code": "invalid_auth"})
    return str(principal["sub"])


def personal_targets():
    """列出在线 Bot 当前配置的超管，映射历史不能作为身份依据。"""
    import nonebot

    from zhenxun.services.ai.run.context import CallerIdentity
    from zhenxun.services.ai.run.dsh import DshSessions
    from zhenxun.utils.platform import PlatformUtils

    result = {}
    for bot in nonebot.get_bots().values():
        for user in PlatformUtils.get_superusers(bot):
            owner = CallerIdentity(
                "qq",
                PlatformUtils.get_platform_scope(bot),
                str(bot.self_id),
                user,
                None,
            )
            result[DshSessions.owner_key(owner, "agent")] = owner
    return result


def console_entry(subject):
    return "webui_entry:" + hashlib.sha256(subject.encode()).hexdigest()


@router.put("/console/entry", dependencies=[authentication()], response_model=Result)
async def set_console_entry(payload: ConsoleEntry, request: Request) -> Result:
    from zhenxun.builtin_plugins.ai_chat._sessions import Conversations

    owner = personal_targets().get(payload.owner)
    if owner is None:
        raise HTTPException(409, detail={"message": "请选择当前 Bot 的有效超管。"})
    await Conversations.owner_entry(
        owner, console_entry(management_subject(request)), payload.agent
    )
    return Result.ok(info="已保存本人控制台入口。")


@router.get("/console/session", dependencies=[authentication()], response_model=Result)
async def resolve_console_session(
    request: Request, persona_id: int | None = None, owner: str | None = None
) -> Result:
    from zhenxun.builtin_plugins.ai_chat._agent import AGENT_ACTIVE_KEY, bind_source
    from zhenxun.builtin_plugins.ai_chat._runner import (
        agent_runner,
        dsh_settings,
    )
    from zhenxun.builtin_plugins.ai_chat._sessions import Conversations
    from zhenxun.services.ai.run.context import RunContext, ScheduledDeps
    from zhenxun.services.ai.run.dsh import DshSessions

    from ....security import validate_access_token

    subject = management_subject(request)
    targets = personal_targets()
    if owner is None and len(targets) == 1:
        owner = next(iter(targets))
    if owner and owner not in targets:
        raise HTTPException(409, detail={"message": "所选超管已失效。"})
    selected = targets.get(owner)
    personal = (
        await Conversations.owner_entry(selected, console_entry(subject))
        if selected
        else None
    )
    choices = [
        {"id": k, "label": f"{v.user_id} · Bot {v.bot_id} ({v.platform})"}
        for k, v in targets.items()
    ]
    data = {
        "owners": choices,
        "owner": owner,
        "agent": bool(personal),
        "executor": agent_runner(),
    }
    if data["executor"] == "local":
        snapshot = await Conversations.snapshot(personal) if personal else None
        return Result.ok(
            {
                **data,
                "session_id": snapshot["session_id"]
                if snapshot
                else console_session(persona_id),
                "revision": snapshot["revision"] if snapshot else None,
            }
        )
    context = RunContext(
        deps=ScheduledDeps(user_id=f"webui:{subject}", platform="webui")
    )
    credential = request.headers.get("Authorization", "")
    bind_source(
        context, "webui", management_check=lambda: validate_access_token(credential)
    )
    context.state[AGENT_ACTIVE_KEY] = bool(personal)
    context.state["_personal_scene"] = personal
    context.state["_personal_owner"] = selected
    mode = "agent" if personal else "chat"
    records = DshSessions.for_settings(dsh_settings())
    item = records._read()["owners"].get(records.owner(context, mode))
    return Result.ok(
        {
            "session_id": item["local_id"] if item else None,
            **data,
            "mode": mode,
            "revision": item["revision"] if item else None,
        }
    )


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
            "run_id": event.run_id,
            "parent_run_id": event.parent_run_id,
            "tool_call_id": event.tool_call_id,
            "arguments": _shorten(event.arguments),
        }
    if isinstance(event, ToolCallEndEvent):
        result = getattr(event.result, "output", event.result)
        return {
            "type": "tool_end",
            "name": event.tool_name,
            "run_id": event.run_id,
            "parent_run_id": event.parent_run_id,
            "tool_call_id": event.tool_call_id,
            "result": _shorten(result),
            "is_error": event.is_error,
            "uncertain": bool(getattr(event.result, "uncertain", False)),
            "reused": bool(getattr(event.result, "reused", False)),
        }
    return None


async def _run(
    payload: ConsoleRequest,
    persona: Any,
    queue: asyncio.Queue[dict | None],
    receipt: dict | None = None,
    management_check=None,
    subject: str = "",
) -> None:
    """确保控制台生产者在初始化失败或退出时结束事件流。"""
    if receipt is None:
        receipt = {"event": asyncio.Event()}
    try:
        await _run_console(payload, persona, queue, receipt, management_check, subject)
    except Exception as error:
        logger.warning(f"控制台运行未完成: {type(error).__name__}", "WebUi")
        if not receipt.get("done_emitted"):
            queue.put_nowait(
                {
                    "type": "done",
                    "outcome": "failed",
                    "error": "控制台未能完成本轮，请检查执行器配置和运行记录。",
                }
            )
    finally:
        queue.put_nowait(None)


async def _run_console(payload, persona, queue, receipt, management_check, subject):
    """在会话锁内运行一次控制台对话：工具事件实时推送，回复正文随结束事件推送，
    等待前端收到结束事件后按实际送达的内容结算历史与运行记录

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
    from zhenxun.builtin_plugins.ai_chat._runner import (
        agent_runner,
        run_dsh,
        select_dsh_session,
    )
    from zhenxun.builtin_plugins.ai_chat._safety import Safety, SessionLimit
    from zhenxun.builtin_plugins.ai_chat._turn import FAILED_NOTICE, ChatTurn
    from zhenxun.services.ai.core.exceptions import ControlFlowExit
    from zhenxun.services.ai.run.context import RunContext, ScheduledDeps
    from zhenxun.services.ai.run.session import session_manager

    context = RunContext(
        session_id=console_session(payload.persona_id),
        deps=ScheduledDeps(user_id=f"webui:{subject}", platform="webui"),
    )
    if receipt and receipt.get("run_id"):
        context.run.run_id = receipt["run_id"]
    bind_source(context, "webui", management_check=management_check)
    context.state[AGENT_ACTIVE_KEY] = payload.agent
    from zhenxun.builtin_plugins.ai_chat._sessions import Conversations

    async def verify_entry():
        owner = personal_targets().get(payload.owner) if payload.owner else None
        personal = (
            await Conversations.owner_entry(owner, console_entry(subject))
            if owner
            else None
        )
        if bool(personal) != payload.agent or (payload.owner and not owner):
            raise ValueError("控制台入口已变化，请刷新后重新发送。")
        return owner, personal

    context.state[USER_KEY] = f"webui:{subject}"
    context.state["_dsh_event"] = queue.put_nowait

    async def ask_dsh(question, approval=False):
        question_id = uuid.uuid4().hex
        future = asyncio.get_running_loop().create_future()
        context.state["_dsh_question"] = (question_id, future)
        try:
            await queue.put(
                {
                    "type": "question",
                    "run_id": context.run.run_id,
                    "question_id": question_id,
                    "question": question,
                    "kind": "approval" if approval else "text",
                }
            )
            return await asyncio.wait_for(future, 300)
        finally:
            if context.state.get("_dsh_question", (None,))[0] == question_id:
                context.state.pop("_dsh_question", None)
            if not future.done():
                future.cancel()

    context.state["_dsh_ask"] = ask_dsh
    runner = agent_runner()
    try:
        if payload.executor and payload.executor != runner:
            raise ValueError("执行器已变化")
        owner, personal = await verify_entry()
        context.state["_personal_owner"] = owner
        context.state["_personal_scene"] = personal
        if personal:
            context.run.follow_up = False
            snapshot = await Conversations.snapshot(personal)
            context.state["_conversation"] = snapshot
            context.state["_entry_scene"] = console_entry(subject)
            context.state["_scene_configuration"] = await Conversations.resolve(
                personal, snapshot, owner=owner
            )
            if context.state["_scene_configuration"]["effective"]["disabled"]:
                raise ValueError("个人会话已停用")
            if payload.persona_id is None:
                persona = await Conversations.persona(snapshot, None, owner.user_id)
            context.session_id = context.session.session_id = snapshot["session_id"]
            context.run.management_check = lambda: bool(
                management_check
                and management_check()
                and personal_targets().get(payload.owner) == owner
            )
        if runner == "dsh":
            await select_dsh_session(context, persona)
        if payload.session_id and payload.session_id != context.session_id:
            raise ValueError("会话已变化")
    except Exception:
        await queue.put(
            {
                "type": "done",
                "error": "会话或权限已变化，请刷新后重试。",
                "outcome": "failed",
            }
        )
        return
    AIChatManager.prepare_input(context, payload.message)
    agent = AIChatManager.get_agent(persona)
    if SessionLimit.personal_full(personal):
        queue.put_nowait(
            {
                "type": "done",
                "outcome": "skipped",
                "error": "个人 Agent 队列已满，请稍后再试。",
            }
        )
        return
    turn = ChatTurn(
        context,
        agent,
        "webui",
        persona,
        payload.message,
        "WebUI",
        usage_id=f"webui:{subject}" if runner == "dsh" else None,
    )
    AIChatManager.configuration(context, None, persona, runner)
    await queue.put(
        {
            "type": "accepted",
            "run_id": turn.run_id,
            "session_id": context.session_id,
            "executor": runner,
            "phase": "queued",
        }
    )
    finalized = False

    async def prepare_run():
        nonlocal persona, agent
        from zhenxun.models.ai_persona import AIPersona
        from zhenxun.services.ai.core.exceptions import LLMException

        await verify_entry()
        if personal:
            await Conversations.check(snapshot)
        if agent_runner() != runner:
            raise LLMException("排队期间执行器已切换，请重新发送。")

        if payload.persona_id is not None:
            persona = await AIPersona.get_or_none(id=payload.persona_id)
            if persona is None or not persona.enabled:
                context.run.user_notice = "所选人设已删除或停用，请重新选择。"
                raise LLMException(context.run.user_notice)
        agent = AIChatManager.get_agent(persona)
        turn.select_agent(agent, persona)
        AIChatManager.configuration(context, None, persona, runner)
        context.run.current_model = context.state["_configuration"]["model"]
        return agent, payload.message

    async def before_run():
        if Safety.check_input(payload.message):
            raise ValueError("输入未通过安全检查")

    async def finalize(result, error=None):
        nonlocal finalized
        if finalized:
            return
        finalized = True
        output = turn.resolve_reply(result, error)
        if (
            turn.reply_kind == "failure"
            and not turn.tool_texts
            and not turn.report(error)["uncertain"]
        ):
            output = (
                context.run.user_notice
                or (persona and persona.error_message)
                or FAILED_NOTICE
            )
        output = Safety.filter_output(output or "")[0]
        from zhenxun.builtin_plugins.ai_chat._agent import (
            AGENT_TOOLS_KEY,
        )
        from zhenxun.services.ai.utils.runtime import PermissionUtils

        allowed = payload.agent and await PermissionUtils.check_superuser(context)
        actual = context.state.get("_available_tool_names", set()) & context.state.get(
            AGENT_TOOLS_KEY, set()
        )
        available = bool(allowed and actual)
        if runner == "dsh":
            available = bool(
                allowed and context.run.operations.get("dsh", {}).get("mode") == "agent"
            )
        reason = ""
        if payload.agent and not available:
            reason = context.state.get(
                "_permission_reason", "当前人设没有可用的敏感工具"
            )
        done = {
            "type": "done",
            "run_id": turn.run_id,
            "outcome": context.run.outcome,
            "reply_kind": turn.reply_kind,
            "phase": context.run.phase,
            "duration": int((time.monotonic() - turn.started) * 1000),
            "output": output,
            "tokens": turn.tokens,
            "agent_requested": payload.agent,
            "agent_available": available,
            "agent_unavailable_reason": reason,
            "diagnostics": {
                "dsh": context.run.operations.get("dsh"),
                "tool_conflicts": context.state.get("_tool_conflicts", []),
                "operation_uncertain": bool(
                    context.run.operations.get("uncertain_tools")
                ),
            },
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
            receipt["done_emitted"] = True
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
        if runner == "dsh":
            from zhenxun.services.ai.core.messages import TextPart

            async def prepare_dsh():
                await prepare_run()
                return [TextPart(text=payload.message)], persona

            await run_dsh(
                context,
                [TextPart(text=payload.message)],
                persona,
                finalize=finalize,
                before_run=before_run,
                prepare_run=prepare_dsh,
            )
            return
        async with agent.run_stream(
            payload.message,
            context=context,
            finalize=finalize,
            before_run=before_run,
            prepare_run=prepare_run,
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
        await queue.put(
            {
                "type": "settled",
                "run_id": turn.run_id,
                "outcome": context.run.outcome,
                "phase": "finished",
                "details": session_manager.details(context),
            }
        )


@router.post("/console/run", dependencies=[authentication()])
async def console_run(payload: ConsoleRequest, request: Request) -> StreamingResponse:
    from zhenxun.models.ai_persona import AIPersona

    persona = None
    if payload.persona_id is not None:
        persona = await AIPersona.get_or_none(id=payload.persona_id)
        if persona is None:
            raise HTTPException(404, detail={"code": "persona_not_found"})
        if not persona.enabled:
            raise HTTPException(409, detail={"code": "persona_disabled"})
    subject = management_subject(request)
    session = (
        console_session(payload.persona_id)
        + ":"
        + hashlib.sha256(subject.encode()).hexdigest()
    )
    if (task := _tasks.get(session)) and not task.done():
        raise HTTPException(409, detail={"code": "console_busy"})
    queue: asyncio.Queue[dict | None] = asyncio.Queue()
    receipt: dict[str, Any] = {"event": asyncio.Event(), "run_id": uuid.uuid4().hex}
    from ....security import decode_access_token_status, validate_access_token

    credential = request.headers.get("Authorization", "")
    principal, status = decode_access_token_status(credential)
    if status != "valid" or not principal:
        raise HTTPException(401, detail={"code": "invalid_auth"})
    expires_at = float(principal.get("exp", time.time()))
    task = asyncio.create_task(
        _run(
            payload,
            persona,
            queue,
            receipt,
            lambda: validate_access_token(credential),
            str(principal["sub"]),
        )
    )
    _tasks[session] = task

    async def events() -> AsyncIterator[str]:
        try:
            while validate_access_token(credential):
                remaining = expires_at - time.time()
                if remaining <= 0:
                    break
                try:
                    item = await asyncio.wait_for(queue.get(), min(remaining, 5))
                except asyncio.TimeoutError:
                    continue
                if item is None or not validate_access_token(credential):
                    break
                yield f"data: {json.dumps(item, ensure_ascii=False)}\n\n"
                if item.get("type") == "done":
                    receipt["accepted"] = True
                    receipt["event"].set()
        finally:
            with anyio.CancelScope(shield=True):
                if not task.done():
                    task.cancel()
                await asyncio.wait({task}, timeout=10)
            if _tasks.get(session) is task:
                _tasks.pop(session, None)

    return StreamingResponse(
        events(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@router.post("/console/stop", dependencies=[authentication()], response_model=Result)
async def console_stop(payload: ConsoleStopRequest, request: Request) -> Result:
    from zhenxun.builtin_plugins.ai_chat._commands import stop_message
    from zhenxun.builtin_plugins.ai_chat._runner import dsh_settings
    from zhenxun.services.ai.run.dsh import DshSessions
    from zhenxun.services.ai.run.dsh_control import DshControl
    from zhenxun.services.ai.run.session import session_manager

    subject = management_subject(request)
    session = console_session(payload.persona_id)
    captured = []
    for run_id, (context, task) in list(session_manager.runs.items()):
        if payload.run_id and run_id != payload.run_id:
            continue
        caller = context.run.caller
        owned = (
            caller is not None
            and caller.source == "webui"
            and caller.user_id == f"webui:{subject}"
        )
        if payload.run_id and not owned:
            raise HTTPException(409, detail={"message": "运行不属于当前账号"})
        if owned and (
            payload.run_id
            or context.session_id == session
            or context.state.get("_dsh_binding")
        ):
            captured.append((run_id, context))
    completed = [await session_manager.stop(run_id) for run_id, _ in captured]
    states = [
        context.run.operations.get("dsh", {}).get("stop", "unknown")
        for _, context in captured
        if context.state.get("_dsh_binding")
    ]
    if not payload.run_id:
        task = _tasks.get(session + ":" + hashlib.sha256(subject.encode()).hexdigest())
        if task and not task.done():
            task.cancel()
            _, pending = await asyncio.wait({task}, timeout=10)
            completed.append(not pending)
    settings = dsh_settings()
    records = DshSessions.for_settings(settings)
    captured_ids = {run_id for run_id, _ in captured}
    for item in records._read()["owners"].values():
        flight = item.get("inflight") or {}
        run_id = flight.get("run_id")
        if not run_id or run_id in captured_ids or payload.run_id != run_id:
            continue
        from zhenxun.models.ai_agent_run import AIAgentRun

        if not await AIAgentRun.filter(
            run_id=run_id,
            source="webui",
            user_id=f"webui:{subject}",
            session_id=item["local_id"],
        ).exists():
            continue
        result = await DshControl.stop_owned(settings, dict(item), run_id)
        states.append(result["state"])
    local_done = all(completed)
    info = (
        stop_message(states, local_done)
        if completed or states
        else "没有本账号的登记运行；未对无归属的原生活动执行停止。"
    )
    return Result.ok({"local_done": local_done, "remote_states": states}, info=info)
