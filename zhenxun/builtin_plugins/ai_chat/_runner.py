import asyncio
import hashlib
import time

"""第三方执行器：选用非 local 执行器时由它代替本地 Agent 完成一轮对话，
唤醒、限流、安全过滤、额度与发送仍由 ai_chat 处理"""

from typing import Any

from zhenxun.configs.config import Config
from zhenxun.models.ai_persona import AIPersona
from zhenxun.services.ai.core.exceptions import LLMException, ToolFatalError
from zhenxun.services.ai.core.messages import TextPart, UsageInfo
from zhenxun.services.ai.core.models import CancellationToken
from zhenxun.services.ai.run.context import RunContext, set_run_context
from zhenxun.services.ai.run.dsh import (
    DshRuntime,
    DshSessions,
    DshSettings,
    content_blocks,
)
from zhenxun.services.ai.run.models import AgentRunResult
from zhenxun.services.ai.run.session import session_manager
from zhenxun.services.ai.utils.runtime import PermissionUtils
from zhenxun.services.log import logger

from ._data_source import MODULE, AIChatManager, _time_prompt

RUNNERS = {"local": "本地 Agent", "dsh": "DeepSeek Harness"}
"""可选的执行器"""
APPROVE_ONCE = {"允许", "批准", "allow", "yes"}
"""批准本次 DSH 请求的回复"""
APPROVE_ALL = {"全部允许", "一键允许", "allow all"}
"""批准本轮后续全部 DSH 请求的回复"""
APPROVE_HINT = "允许：仅本次；全部允许：本轮；停止：结束本轮。其他回复拒绝本次。"
"""聊天中批准请求的回复说明"""


def agent_runner() -> str:
    """读取所有入口共用的执行器。"""
    runner = Config.get_config(MODULE, "AGENT_RUNNER", "local")
    if runner not in RUNNERS:
        raise LLMException("全局执行器设置无效，请在 WebUI 中重新选择。")
    return runner


def dsh_reusable(
    outcome: str, delivery: dict, text: str, raw: str, suppressed: bool
) -> bool:
    """判断 DSH 会话记录与实际送达是否一致，一致时下一轮沿用原会话

    参数:
        outcome: 本轮执行结果
        delivery: 投递结果
        text: 过滤后的最终文本
        raw: 执行端返回的原始文本
        suppressed: 答复已由工具发出，收尾文本未转发

    返回:
        bool: 是否沿用原会话
    """
    from ._reply import parse_silent

    status, attachment = delivery.get("status"), delivery.get("attachment")
    if suppressed or (status == "skipped" and not attachment):
        if delivery.get("uncertain") or delivery.get("operation_uncertain"):
            return False
        if suppressed:
            return status == "sent"
        return outcome == "completed" and parse_silent(raw) is None
    return (
        status == "sent"
        and outcome == "completed"
        and not delivery.get("uncertain")
        and (attachment.get("status") == "sent" if attachment else text == raw)
    )


def dsh_settings() -> DshSettings:
    """读取聊天设置中的 DSH

    返回:
        DshSettings: dsh 执行器设置
    """
    return DshSettings(**(Config.get_config(MODULE, "DSH") or {}))


def chat_search_hint(enabled: bool) -> str:
    """说明普通 DSH 会话本轮实际可用的搜索和回复方式。"""
    search = (
        "遇到陌生的具体事物、时效性问题或需要核实的事实，先用 web_search "
        "搜索，再依据查到的资料回答，普通闲聊不必搜索；工具列表里没有 "
        "web_search 时，无法核实的内容要说明，不要假称已查询。"
        if enabled
        else "本轮没有联网工具；无法核实的内容要说明，不要假称已查询。"
    )
    return (
        search + "只用实际提供的工具接口，不把调用写进正文。普通回复直接写文字；"
        "已用工具发出的内容不用再说一遍。"
    )


async def dsh_mode(context: RunContext, persona: AIPersona | None) -> str:
    """从当前来源、超管授权、Agent 开关和人设确定桌面权限上限。"""
    from ._agent import AgentCapability

    enabled = dsh_settings().allow_tools and (
        persona is None or persona.tools is None or "agent" in persona.tools
    )
    return "agent" if enabled and await AgentCapability().active(context) else "chat"


async def select_dsh_session(context: RunContext, persona: AIPersona | None) -> dict:
    """在排队前绑定所有者与模式，回复位置仍由本轮上下文持有。"""
    mode = await dsh_mode(context, persona)
    if context.state.get("_personal_scene") and mode != "agent":
        raise LLMException("当前 DSH 权限或人设不允许个人 Agent，未进入普通聊天。")
    key = f"{persona.id}:{persona.updated_at.isoformat()}" if persona else ""
    return DshSessions.for_settings(dsh_settings()).route(context, mode, key)


def scene_session(context: RunContext, snapshot: dict, runner: str) -> str:
    """读取命令已绑定的历史，不创建映射或改用另一个执行器的历史。"""
    if runner == "dsh":
        records = DshSessions.for_settings(dsh_settings())
        mode = "agent" if context.state.get("_personal_scene") else "chat"
        item = records._read()["owners"].get(records.owner(context, mode))
        return item["local_id"] if item else ""
    return snapshot["session_id"] or ""


async def _run_dsh(
    context: RunContext,
    parts: list[Any],
    persona: AIPersona | None,
    *,
    finalize=None,
    before_run=None,
    prepare_run=None,
) -> AgentRunResult:
    """在会话执行锁内调用桌面，等待投递和历史结算后释放归属。

    参数:
        context: 运行上下文
        parts: 本轮输入
        persona: 人设

    返回:
        AgentRunResult: 运行结果
    """
    from zhenxun.services.ai.context.memory.engine import chat_backend
    from zhenxun.services.ai.core.exceptions import RunInterrupted
    from zhenxun.services.ai.core.messages import LLMMessage
    from zhenxun.services.ai.run.hitl import HITLController

    from ._attachments import collect_export, export_instruction, export_slot
    from ._reply import clean_output, empty_reply_reason
    from ._safety import Safety

    session_id = context.session_id or ""
    timeout = float(dsh_settings().timeout)
    deadline = 0.0
    waiting = 0
    settling = False
    paused_at = 0.0
    approval_lock = asyncio.Lock()
    remote = None

    async def approve(tool: str, reason: str) -> bool:
        try:
            PermissionUtils.ensure_current(context)
        except (asyncio.CancelledError, asyncio.TimeoutError):
            return False
        allowed = await PermissionUtils.check_superuser(context)
        how = ""
        if allowed:
            async with approval_lock:
                token.raise_if_cancelled()
                if dsh_settings().permission == "full":
                    how = "（完全权限）"
                elif context.state.get("_dsh_approve_all"):
                    how = "（本轮全部允许）"
                else:
                    try:
                        reply = await ask(
                            f"DSH 请求批准：{tool}\n{reason or '未提供说明'}",
                            approval=True,
                        )
                    except (ToolFatalError, asyncio.TimeoutError):
                        reply = ""
                    reply = reply.strip().lower()
                    if reply in {"停止", "stop"}:
                        token.cancel("cancelled")
                        raise asyncio.CancelledError
                    if reply in APPROVE_ALL:
                        context.state["_dsh_approve_all"] = True
                    allowed = reply in APPROVE_ONCE | APPROVE_ALL
        decision = (f"自动允许{how}" if how else "允许") if allowed else "拒绝"
        logger.info(
            f"dsh 请求使用 {tool}: {decision}",
            MODULE,
            session=context.get_user_id(),
        )
        return allowed

    async def ask(question: str, approval: bool = False) -> str:
        nonlocal waiting, paused_at, deadline
        if not waiting:
            paused_at = time.monotonic()
        waiting += 1
        try:
            if callback := context.state.get("_dsh_ask"):
                return await callback(question, approval)
            if approval:
                question += f"\n{APPROVE_HINT}"
            return await HITLController(context).ask_text(question, timeout=300)
        finally:
            waiting -= 1
            if not waiting:
                paused = time.monotonic() - paused_at
                deadline += paused
                if remote:
                    DshRuntime.extend(remote, paused)

    token = context.run.cancellation_token or CancellationToken()
    context.run.cancellation_token = token
    revision = session_manager.revision(session_id)
    async with session_manager.run_scope(
        session_id,
        token,
        exclusive=True,
        revision=revision,
        owner_scope=context.state.get("_personal_scene"),
    ):
        await session_manager.phase(context, "running")
        deadline = time.monotonic() + timeout
        expired = False

        async def turn():
            nonlocal remote, settling
            binding = context.state.get("_dsh_binding")
            if binding is None:
                raise LLMException("DSH 会话没有在准入时绑定。")
            try:
                DshSessions.for_binding(binding).refresh(
                    binding, input_id=context.state.get("_dsh_input_id")
                )
                prepared, selected = parts, persona
                if prepare_run:
                    prepared, selected = await prepare_run()
                if before_run:
                    await before_run()
                settings = dsh_settings()
                if settings.transport != binding.get("transport", "desktop"):
                    raise LLMException("排队期间 DSH 后端已切换，请重新发送。")
                search_allowed = (
                    not selected
                    or selected.tools is None
                    or "web_search" in selected.tools
                )
                from zhenxun.services.ai.tools.providers.builtin.web import (
                    dsh_search_source,
                )

                source = dsh_search_source(search_allowed, binding["mode"])
                context.state["_dsh_search_source"] = source
                persona_key = (
                    f"{selected.id}:{selected.updated_at.isoformat()}"
                    if selected
                    else ""
                )
                if (
                    persona_key != context.state.get("_dsh_persona_at_admission")
                    or await dsh_mode(context, selected) != binding["mode"]
                ):
                    raise LLMException("排队期间人设或 Agent 权限已变化，请重新提问。")

                async def authorization_check():
                    PermissionUtils.ensure_current(context)
                    current = (
                        await AIPersona.get_or_none(id=selected.id)
                        if selected
                        else None
                    )
                    if selected and (
                        current is None
                        or not current.enabled
                        or current.updated_at != selected.updated_at
                    ):
                        return "denied"
                    snapshot = context.state.get("_conversation")
                    if snapshot:
                        from ._sessions import Conversations

                        await Conversations.check(snapshot)
                    return await dsh_mode(context, current)

                context.state["_dsh_authorization_check"] = authorization_check
                from zhenxun.services.ai.run.dsh_tools import ChatToolBridge

                bridge = ChatToolBridge(AIChatManager.get_agent(selected), context)
                context.state["_dsh_chat_tools"] = bridge
                await bridge.prepare()
                if source == "api" and "web_search" not in bridge.identities:
                    source = context.state["_dsh_search_source"] = "off"
                context.run.operations.setdefault("dsh", {})["search_source"] = source
                media_usage = UsageInfo()

                def record_media_usage(usage):
                    nonlocal media_usage
                    media_usage += usage
                    context.run.budget["tokens"] = media_usage.total_tokens

                input_blocks = await content_blocks(
                    prepared,
                    transport=settings.transport,
                    token=token,
                    record_usage=record_media_usage,
                    check_input=Safety.check_input,
                )
                token.raise_if_cancelled()
                if any(not isinstance(p, TextPart) for p in prepared) and (
                    await authorization_check() != binding["mode"]
                    or dsh_settings() != settings
                ):
                    raise LLMException("处理附件期间会话或权限已变化，请重新提问。")
                backend = chat_backend(AIChatManager.get_agent(selected).memory_config)
                history = await backend.get_messages(session_id) if backend else []
                remote_id = binding["remote_id"]
                new = (
                    not binding["created"]
                    or binding.get("quarantined")
                    or binding.get("inflight")
                )
                remote = await DshRuntime.start(
                    context, binding, settings, approve, ask
                )
                remote.usage += media_usage
                new = new or remote_id != binding["remote_id"]
                blocks = []
                if receipt := binding.get("delivery_note"):
                    blocks.append(
                        {
                            "type": "text",
                            "text": "<投递记录>上一轮最终答复已由工具发送，"
                            "随后生成的收尾文本没有转发给 QQ 用户，"
                            "不要当成已经对用户说过的话。已确认发送内容：\n"
                            + receipt
                            + "\n</投递记录>",
                        }
                    )
                persona_text = AIChatManager.build_instruction(selected)
                stamp = hashlib.sha256(
                    "\0".join(
                        (binding["remote_id"], persona_text, bridge.instructions)
                    ).encode()
                ).hexdigest()
                refresh = new or binding.get("applied_persona") != stamp
                if refresh and not (binding.get("native") and binding.get("inherited")):
                    blocks.append(
                        {
                            "type": "text",
                            "text": f"<本轮人设>\n{persona_text}\n</本轮人设>",
                        }
                    )
                if not binding.get("native"):
                    if new:
                        transcript = []
                        for message in history[-40:]:
                            text = "".join(
                                p.text
                                for p in message.content
                                if isinstance(p, TextPart)
                            )
                            if text:
                                transcript.append(f"{message.role}: {text}")
                        if transcript:
                            blocks.append(
                                {
                                    "type": "text",
                                    "text": "以下是此前已确认送达的对话历史：\n"
                                    + "\n".join(transcript),
                                }
                            )
                    blocks.append({"type": "text", "text": _time_prompt(context)})
                    if hint := context.run.dynamic_prompts.get("proactive"):
                        blocks.append({"type": "text", "text": hint})
                    if binding["mode"] == "chat":
                        guidance = chat_search_hint(source != "off")
                        if source != "off" and "web_fetch" not in bridge.identities:
                            guidance += "搜索摘要不等于网页正文，不要声称已读取原文。"
                        if context.run.source in {"qq", "webui"}:
                            guidance += "本轮是直接向你提问，不是邀请你判断是否插话。"
                        blocks.append({"type": "text", "text": guidance})
                blocks += input_blocks
                blocks.extend(
                    {"type": "text", "text": text} for text in bridge.reminders
                )
                if bridge.instructions and (refresh or not binding.get("native")):
                    blocks.insert(
                        0,
                        {
                            "type": "text",
                            "text": "本轮工具说明（以实际工具列表为准）：\n"
                            + bridge.instructions,
                        },
                    )
                if backend:
                    user_message = LLMMessage.user(prepared)
                    user_message.metadata = {
                        **context.run.history_metadata,
                        **context.state.get("_dsh_input_metadata", {}),
                    }
                    await backend.add_messages(session_id, [user_message])
                with export_slot(context, binding) as slot:
                    if slot:
                        blocks.append(
                            {"type": "text", "text": export_instruction(slot[1])}
                        )
                    raw, usage, reason = await DshRuntime.prompt(remote, blocks)
                    remote.revoked = True
                    settling = True
                    diagnostics = context.run.operations.setdefault("dsh", {})
                    diagnostics["bridge_offered"] = sorted(bridge.identities)
                    diagnostics["bridge_calls"] = len(bridge.receipts)
                    diagnostics["end_reason"] = reason
                    text, blocked = Safety.filter_output(clean_output(raw))
                    diagnostics["raw_chars"] = len(raw)
                    diagnostics["filtered_chars"] = len(text)
                    if slot and not blocked:
                        text = collect_export(context, slot, text)
                if backend:
                    assistant = LLMMessage.assistant_text_response(text)
                    assistant.metadata = {
                        **context.run.history_metadata,
                        "chat_final": True,
                    }
                    await backend.add_messages(session_id, [assistant])
                if blocked:
                    context.state["_chat_reply_issue"] = "output_blocked"
                elif not text and raw:
                    context.state["_chat_reply_issue"] = empty_reply_reason(raw)
                result = AgentRunResult(output=text, usage=usage)
            except BaseException as error:
                from zhenxun.services.ai.run.dsh_desktop import DshUnavailable

                if isinstance(error, DshUnavailable):
                    context.run.user_notice = str(error)
                context.run.outcome = (
                    "interrupted"
                    if isinstance(error, asyncio.CancelledError | RunInterrupted)
                    else "failed"
                )
                if remote:
                    cleanup = asyncio.create_task(DshRuntime.interrupt(remote))
                    try:
                        await asyncio.shield(cleanup)
                    except asyncio.CancelledError:
                        await asyncio.shield(cleanup)
                    context.run.operations["external_stop_guarantee"] = (
                        remote.stop_confirmation
                    )
                if (
                    isinstance(error, asyncio.CancelledError)
                    and expired
                    and not token.is_cancelled()
                ):
                    error = asyncio.TimeoutError("本轮执行超时")
                if isinstance(error, asyncio.TimeoutError):
                    context.run.outcome = "timeout"
                    context.run.user_notice = (
                        f"DSH 本轮执行超过 {int(timeout)} 秒，已结束本地等待。"
                        "等待确认的时间不计入，可在 DSH 连接设置中调整单轮超时。"
                    )
                elif (
                    isinstance(error, asyncio.CancelledError)
                    and token.reason == "cancelled"
                ):
                    context.run.outcome = "cancelled"
                try:
                    if finalize:
                        await finalize(None, error)
                finally:
                    if remote:
                        await DshRuntime.release(remote)
                raise
            try:
                context.run.outcome = result.outcome
                if finalize:
                    await finalize(result, None)
                delivery = context.state.get("_delivery", {})
                from ._reply import parse_silent

                suppressed = bool(
                    context.run.reply_control
                    and context.run.reply_control.closed
                    and parse_silent(text)
                )
                context.run.operations["dsh"]["final_text_suppressed"] = suppressed
                DshSessions.for_binding(binding).update(
                    binding,
                    inflight=None,
                    quarantined=None
                    if dsh_reusable(result.outcome, delivery, text, raw, suppressed)
                    else "delivery_or_filter_changed",
                    delivery_uncertain=bool(delivery.get("uncertain"))
                    or delivery.get("status") != "sent",
                    delivery_note=(delivery.get("text") or "")[:8000]
                    if suppressed
                    else None,
                    applied_persona=stamp,
                    persona=persona_key,
                )
                return result
            except BaseException:
                DshSessions.for_binding(binding).update(
                    binding, inflight=None, quarantined="settlement_unconfirmed"
                )
                raise
            finally:
                await DshRuntime.release(remote)

        task = asyncio.create_task(turn())
        try:
            while not task.done():
                remaining = deadline - time.monotonic()
                if remaining <= 0 and not waiting and not settling:
                    expired = True
                    task.cancel()
                    break
                await asyncio.wait({task}, timeout=max(remaining, 1.0))
            try:
                return await task
            except asyncio.CancelledError:
                if expired and not token.is_cancelled():
                    raise asyncio.TimeoutError("本轮执行超时") from None
                raise
        finally:
            if not task.done():
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)


async def run_dsh(
    context: RunContext,
    parts: list[Any],
    persona: AIPersona | None,
    *,
    finalize=None,
    before_run=None,
    prepare_run=None,
) -> AgentRunResult:
    """登记 DSH 本地等待范围；取消不等于已确认远端执行停止。"""
    session_manager.register(context)
    with set_run_context(context):
        try:
            await session_manager.accept(context)
            return await _run_dsh(
                context,
                parts,
                persona,
                finalize=finalize,
                before_run=before_run,
                prepare_run=prepare_run,
            )
        finally:
            await session_manager.finish(context)


async def dsh_command(context, argument: str, *, current_mode: bool = False) -> str:
    """执行 DSH 原生会话控制命令

    参数:
        context: 运行上下文
        argument: 子命令与参数
        current_mode: 普通会话命令使用群共享或私聊当前模式，否则管理个人 Agent

    返回:
        str: 执行结果
    """
    from zhenxun.services.ai.run.dsh_control import DshControl

    from ._commands import AGENT_PRIVATE, require_admin

    action, _, value = argument.strip().partition(" ")
    settings = dsh_settings()
    records = DshSessions.for_settings(settings)
    group = context.get_group_id()
    if group and not current_mode and action not in {"", "status"}:
        raise ValueError(AGENT_PRIVATE)
    mode = (
        "agent"
        if not current_mode or (not group and context.state.get("_personal_scene"))
        else "chat"
    )
    if mode == "agent":
        await require_admin(context)
    owner = records.owner(context, mode)
    current = records._read()["owners"].get(owner)
    if action in {"", "status"}:
        state = await DshRuntime.status(settings)
        return "\n".join(
            f"{key}: {state.get(key)}"
            for key in ("transport", "connected", "version", "reason", "native_ready")
        )
    if action in {"new", "ls", "reset", "archive"} and value:
        raise ValueError(
            "此 DSH 操作不接受名称、分页或额外目标；请使用完整会话 ID 切换。"
        )
    if action == "ls":
        rows = await DshControl.list(settings, owner, mode=mode)
        return (
            "\n".join(
                f"{r['remote_id']} · {r['title']} · {r.get('model') or '继承'}"
                for r in rows[:30]
            )
            or "没有可接入的会话。"
        )
    if action in {"new", "switch"}:
        if action == "switch" and not value:
            raise ValueError("请提供 /ai dsh ls 中的完整会话 ID。")
        selected = await DshControl.choose(
            context.run.caller,
            settings,
            value if action == "switch" else None,
            current["revision"] if current else 0,
            mode=mode,
        )
        if action == "new" and context.get_group_id():
            from ._buffer import ChatBuffer

            ChatBuffer.reset(context.get_bot().self_id, context.get_group_id())
        if action == "new" and group and current_mode:
            return "已新建本群共享对话。旧对话和长期记忆保留。"
        return (
            f"已选择 {selected['remote_id']}；"
            "回复到每次触发位置，原会话历史和设置保留。"
        )
    if not current:
        raise ValueError("请先新建或选择 DSH 会话。")
    if action in {"reset", "archive"}:
        await DshControl.release(settings, current, archive=action == "archive")
        if action == "reset":
            await AIChatManager.reset(current["local_id"])
        return "已解除关联；远端历史未删除。"
    if action in {"model", "thinking", "mode"}:
        return await DshControl.parameter(settings, current, action, value)
    raise ValueError("不支持的 DSH 命令；未作为聊天内容发送。")
