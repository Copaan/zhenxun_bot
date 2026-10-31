"""DeepSeek Harness（dsh）执行器：以子进程运行 `dsh --profile sdk`，
在标准输入输出上按行收发 JSON-RPC 2.0，替代本地 Agent 完成一轮对话"""

import asyncio
import base64
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from io import BytesIO
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
from typing import Any, ClassVar

from pydantic import BaseModel, Field

from zhenxun.configs.path_config import DATA_PATH
from zhenxun.services.ai.core.exceptions import LLMException
from zhenxun.services.ai.core.messages import ImagePart, TextPart, UsageInfo
from zhenxun.services.log import logger

LOG_NAME = "dsh"
DSH_PATH = DATA_PATH / "ai" / "dsh"
"""dsh 数据目录：home（会话历史）、workspace（工作目录）、patch 与会话映射"""
IMAGE_TYPES = {
    "PNG": "image/png",
    "JPEG": "image/jpeg",
    "WEBP": "image/webp",
    "GIF": "image/gif",
}
"""dsh 接受的图片格式"""
LOCAL_TOOLS = (
    "tool-bash",
    "tool-pwsh",
    "tool-jobs",
    "tool-fs",
    "tool-fs-search",
    "tool-skill",
    "tool-subagent",
    "tool-subagent-fork",
    "tool-subagent-control",
    "tool-workflow",
    "tool-schedule",
    "tool-plugin-manager",
    "tool-web",
    "present",
)
"""不允许本地工具时在 patch 中关闭的插件"""
PERSONA_PREFIX = "你在 QQ 中与用户聊天，用自然的口语回复，不使用 Markdown。"

Approver = Callable[[str, str], Awaitable[bool]]
"""审批回调：工具名、原因 → 是否允许"""
Asker = Callable[[str], Awaitable[str]]
"""提问回调：问题文本 → 回答"""


class DshSettings(BaseModel):
    """dsh 执行器设置"""

    bin: str = ""
    """dsh 可执行文件路径，为空时在 PATH 中查找 dsh"""
    provider: str = "deepseek-official"
    """dsh 的服务商路由"""
    model: str = "deepseek-v4-flash"
    """全部会话共用的模型"""
    reasoning_effort: str = ""
    """推理强度，为空使用模型默认值"""
    max_tokens: int = Field(default=0, ge=0)
    """单次输出 token 上限，0 使用模型默认值"""
    api_provider: str = ""
    """取 API Key 的服务商名称，为空时用第一个 deepseek 预设的服务商"""
    base_url: str = ""
    """DEEPSEEK_BASE_URL，为空使用 dsh 默认地址"""
    timeout: int = Field(default=300, ge=10)
    """一轮对话的超时秒数"""
    allow_tools: bool = False
    """保留 dsh 的本地工具（命令行、文件等），只有超级用户的会话能通过审批"""


@dataclass
class _Turn:
    """一个会话正在进行的一轮对话"""

    approve: Approver
    ask: Asker
    events: list[tuple[str, dict[str, Any]]] = field(default_factory=list)
    changed: asyncio.Event = field(default_factory=asyncio.Event)


def _api_key(settings: DshSettings) -> tuple[str, str]:
    """从已配置的服务商取 API Key 与地址

    参数:
        settings: dsh 设置

    返回:
        tuple[str, str]: API Key, 地址
    """
    from zhenxun.services.ai.config.manager import get_llm_config

    providers = get_llm_config().providers
    name = settings.api_provider.strip()
    provider = next(
        (
            p
            for p in providers
            if (p.name == name if name else p.preset == "deepseek") and p.api_keys
        ),
        None,
    )
    if provider is None:
        raise LLMException("dsh 执行器没有可用的 API Key，请先配置 DeepSeek 服务商。")
    return provider.api_keys[0], settings.base_url.strip()


def _patch() -> str:
    """生成关闭本地工具并设置 QQ 场景前缀的 profile patch

    返回:
        str: patch 内容
    """
    lines = [
        "- id: system-prompt",
        "  config:",
        f"    personaPrefix: {json.dumps(PERSONA_PREFIX, ensure_ascii=False)}",
    ]
    for plugin in LOCAL_TOOLS:
        lines += [f"- id: {plugin}", "  disabled: true"]
    return "\n".join(lines) + "\n"


def _image_block(part: ImagePart) -> dict[str, Any] | None:
    """把图片片段转为 dsh 的内联图片块，不支持的格式转为 PNG

    参数:
        part: 图片片段

    返回:
        dict[str, Any] | None: 图片块，读取不到时为空
    """
    from PIL import Image

    data = part.raw
    if data is None and part.path is not None:
        data = part.path.read_bytes()
    if not data:
        return None
    try:
        with Image.open(BytesIO(data)) as image:
            mime = IMAGE_TYPES.get(image.format or "")
            if mime is None:
                buffer = BytesIO()
                image.save(buffer, "PNG")
                data, mime = buffer.getvalue(), "image/png"
    except Exception:
        return None
    return {"type": "image", "data": base64.b64encode(data).decode(), "mimeType": mime}


async def content_blocks(parts: list[Any]) -> list[dict[str, Any]]:
    """把消息片段转为 dsh 的内容块：文字原样，图片内联，其他媒体换为占位文字

    参数:
        parts: 消息片段

    返回:
        list[dict[str, Any]]: 内容块
    """
    from zhenxun.services.ai.utils.utils import MediaFetchError, fetch_media

    blocks: list[dict[str, Any]] = []
    for part in parts:
        if isinstance(part, TextPart):
            if part.text:
                blocks.append({"type": "text", "text": part.text})
            continue
        if isinstance(part, ImagePart):
            if part.url and part.raw is None and part.path is None:
                try:
                    part = ImagePart(
                        raw=await fetch_media(part.url, allow_private=True)
                    )
                except MediaFetchError as e:
                    logger.debug(f"图片下载失败: {e}", LOG_NAME)
            if block := await asyncio.to_thread(_image_block, part):
                blocks.append(block)
                continue
            blocks.append({"type": "text", "text": "[图片，未能读取]"})
            continue
        kind = getattr(part, "type", "")
        blocks.append({"type": "text", "text": f"[{kind or '附件'}]"})
    return blocks


class DshRuntime:
    """dsh SDK 运行时：懒启动的单个子进程，所有会话共用一套模型；
    设置变化时重启，退出时先请求 shutdown 再结束进程树"""

    _proc: ClassVar[asyncio.subprocess.Process | None] = None
    _reader: ClassVar[asyncio.Task | None] = None
    _stderr: ClassVar[asyncio.Task | None] = None
    _serving: ClassVar[set[asyncio.Task]] = set()
    _pending: ClassVar[dict[int, asyncio.Future]] = {}
    _turns: ClassVar[dict[str, _Turn]] = {}
    _next_id: ClassVar[int] = 0
    _key: ClassVar[str] = ""
    _lock: ClassVar[asyncio.Lock | None] = None

    @classmethod
    def running(cls) -> bool:
        """运行时进程是否在运行

        返回:
            bool: 是否在运行
        """
        return cls._proc is not None and cls._proc.returncode is None

    @classmethod
    async def ensure(cls, settings: DshSettings) -> None:
        """确保运行时已按当前设置启动并完成握手

        参数:
            settings: dsh 设置

        异常:
            LLMException: 找不到 dsh、没有 API Key 或握手失败
        """
        if cls._lock is None:
            cls._lock = asyncio.Lock()
        key = settings.model_dump_json()
        async with cls._lock:
            if cls.running() and cls._key == key:
                return
            await cls.close()
            await cls._start(settings)
            cls._key = key

    @classmethod
    async def _start(cls, settings: DshSettings) -> None:
        """启动子进程并发送 initialize

        参数:
            settings: dsh 设置
        """
        binary = settings.bin.strip() or shutil.which("dsh")
        if not binary or not Path(binary).exists():
            raise LLMException("找不到 dsh 可执行文件，请在聊天设置的 DSH 中填写 bin。")
        api_key, base_url = _api_key(settings)
        home, workspace = DSH_PATH / "home", DSH_PATH / "workspace"
        home.mkdir(parents=True, exist_ok=True)
        workspace.mkdir(parents=True, exist_ok=True)
        args = [binary, "--profile", "sdk"]
        if not settings.allow_tools:
            patch = DSH_PATH / "zhenxun.patch.yml"
            patch.write_text(_patch(), encoding="utf-8")
            args += ["--patch", str(patch.resolve())]
        env = {**os.environ, "DSH_HOME": str(home.resolve())}
        env["DEEPSEEK_API_KEY"] = api_key
        if base_url:
            env["DEEPSEEK_BASE_URL"] = base_url
        flags = subprocess.CREATE_NEW_PROCESS_GROUP if sys.platform == "win32" else 0
        cls._proc = await asyncio.create_subprocess_exec(
            *args,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            cwd=str(workspace.resolve()),
            env=env,
            limit=64 * 1024 * 1024,
            creationflags=flags,
            start_new_session=sys.platform != "win32",
        )
        cls._reader = asyncio.create_task(cls._read_loop(cls._proc))
        cls._stderr = asyncio.create_task(cls._read_stderr(cls._proc))
        params: dict[str, Any] = {
            "cwd": str(workspace.resolve()),
            "provider": settings.provider,
            "model": settings.model,
            "approvalRequests": True,
            "capabilities": {"userQuestions": True},
        }
        if settings.reasoning_effort:
            params["reasoningEffort"] = settings.reasoning_effort
        if settings.max_tokens:
            params["maxTokens"] = settings.max_tokens
        try:
            info = await cls.request("initialize", params, 60)
        except LLMException:
            await cls.close()
            raise
        logger.info(f"dsh 运行时已启动: {info.get('serverInfo')}", LOG_NAME)

    @classmethod
    async def request(cls, method: str, params: Any, timeout: float = 30) -> Any:
        """发送一个请求并等待响应

        参数:
            method: 方法名
            params: 参数
            timeout: 超时秒数

        返回:
            Any: 响应结果

        异常:
            LLMException: 运行时未启动、超时或返回错误
        """
        proc = cls._proc
        if proc is None or proc.stdin is None or proc.returncode is not None:
            raise LLMException("dsh 运行时未启动。")
        cls._next_id += 1
        request_id = cls._next_id
        future = asyncio.get_running_loop().create_future()
        cls._pending[request_id] = future
        await cls._write(
            {
                "jsonrpc": "2.0",
                "id": request_id,
                "method": method,
                **({"params": params} if params is not None else {}),
            }
        )
        try:
            return await asyncio.wait_for(future, timeout)
        except asyncio.TimeoutError as e:
            raise LLMException(f"dsh 请求 {method} 超时。") from e
        finally:
            cls._pending.pop(request_id, None)

    @classmethod
    async def _write(cls, message: dict[str, Any]) -> None:
        """写入一行 JSON-RPC 消息

        参数:
            message: 消息
        """
        proc = cls._proc
        if proc is None or proc.stdin is None:
            return
        proc.stdin.write(json.dumps(message, ensure_ascii=False).encode() + b"\n")
        await proc.stdin.drain()

    @classmethod
    async def _read_loop(cls, proc: asyncio.subprocess.Process) -> None:
        """读取标准输出，分发响应、通知与服务端请求

        参数:
            proc: 子进程
        """
        assert proc.stdout is not None
        while line := await proc.stdout.readline():
            try:
                message = json.loads(line)
            except ValueError:
                continue
            if not isinstance(message, dict):
                continue
            method, request_id = message.get("method"), message.get("id")
            if method and request_id is not None:
                task = asyncio.create_task(cls._serve(request_id, method, message))
                cls._serving.add(task)
                task.add_done_callback(cls._serving.discard)
            elif request_id is not None:
                future = cls._pending.get(request_id)
                if future is None or future.done():
                    continue
                if error := message.get("error"):
                    future.set_exception(
                        LLMException(f"dsh 返回错误: {error.get('message', error)}")
                    )
                else:
                    future.set_result(message.get("result") or {})
            elif method:
                cls._notify(method, message.get("params") or {})
        for future in cls._pending.values():
            if not future.done():
                future.set_exception(LLMException("dsh 运行时已退出。"))
        for turn in cls._turns.values():
            turn.events.append(("exit", {}))
            turn.changed.set()
        logger.warning(f"dsh 运行时已退出，返回码 {await proc.wait()}", LOG_NAME)

    @classmethod
    async def _read_stderr(cls, proc: asyncio.subprocess.Process) -> None:
        """把标准错误输出写入调试日志

        参数:
            proc: 子进程
        """
        assert proc.stderr is not None
        while line := await proc.stderr.readline():
            logger.debug(line.decode(errors="replace").rstrip(), LOG_NAME)

    @classmethod
    def _notify(cls, method: str, params: dict[str, Any]) -> None:
        """把通知交给对应会话正在进行的一轮对话

        参数:
            method: 通知方法
            params: 通知参数
        """
        if turn := cls._turns.get(str(params.get("sessionId", ""))):
            turn.events.append((method, params))
            turn.changed.set()

    @classmethod
    async def _serve(cls, request_id: Any, method: str, message: dict) -> None:
        """应答服务端请求：审批交给会话的审批回调，提问交给会话的提问回调，
        找不到会话时拒绝或给空答案

        参数:
            request_id: 请求id
            method: 方法名
            message: 请求
        """
        params = message.get("params") or {}
        turn = cls._turns.get(str(params.get("sessionId", "")))
        try:
            if method == "approval/request":
                allowed = turn is not None and await turn.approve(
                    str(params.get("toolName", "")), str(params.get("reason") or "")
                )
                result: Any = {"outcome": "allowed-once" if allowed else "rejected"}
            elif method == "user-question/ask":
                result = {"answers": await cls._answer(turn, params.get("questions"))}
            else:
                await cls._write(
                    {
                        "jsonrpc": "2.0",
                        "id": request_id,
                        "error": {"code": -32601, "message": "method not found"},
                    }
                )
                return
        except Exception as e:
            logger.debug(f"应答 dsh 请求 {method} 失败: {e}", LOG_NAME)
            result = (
                {"outcome": "rejected"}
                if method == "approval/request"
                else {"answers": cls._blank(params.get("questions"))}
            )
        await cls._write({"jsonrpc": "2.0", "id": request_id, "result": result})

    @staticmethod
    def _blank(questions: Any) -> list[dict[str, Any]]:
        """不回答时每个问题的空答案

        参数:
            questions: 问题

        返回:
            list[dict[str, Any]]: 答案
        """
        items = questions if isinstance(questions, list) else []
        return [{"id": str(q.get("id", "")), "selected": []} for q in items]

    @classmethod
    async def _answer(cls, turn: _Turn | None, questions: Any) -> list[dict[str, Any]]:
        """逐个向用户提问，回复选项编号或名称时选中该选项，否则作为自定义回答

        参数:
            turn: 会话正在进行的一轮对话
            questions: 问题

        返回:
            list[dict[str, Any]]: 答案
        """
        if turn is None or not isinstance(questions, list):
            return cls._blank(questions)
        answers = []
        for question in questions:
            options = [str(o.get("label", "")) for o in question.get("options") or []]
            lines = [str(question.get("question", ""))]
            if detail := question.get("detail"):
                lines.append(str(detail))
            lines += [f"{i}. {label}" for i, label in enumerate(options, 1)]
            reply = (await turn.ask("\n".join(lines))).strip()
            if reply.isdigit() and 0 < int(reply) <= len(options):
                reply = options[int(reply) - 1]
            answer: dict[str, Any] = {"id": str(question.get("id", ""))}
            answer["selected"] = [reply] if reply in options else []
            if reply not in options:
                answer["custom"] = reply
            answers.append(answer)
        return answers

    @classmethod
    async def prompt(
        cls,
        session_id: str,
        blocks: list[dict[str, Any]],
        approve: Approver,
        ask: Asker,
        timeout: float,
    ) -> tuple[str, UsageInfo, str | None]:
        """发送一轮输入，等到会话回到空闲，取最后一条助手消息

        参数:
            session_id: dsh 会话id，未知时由 dsh 新建
            blocks: 内容块
            approve: 审批回调
            ask: 提问回调
            timeout: 超时秒数

        返回:
            tuple[str, UsageInfo, str | None]: 回复, 用量, 结束原因

        异常:
            LLMException: 运行时退出、超时或本轮出错
        """
        turn = _Turn(approve=approve, ask=ask)
        cls._turns[session_id] = turn
        try:
            receipt = await cls.request(
                "session/prompt", {"sessionId": session_id, "contentBlocks": blocks}
            )
            events = await asyncio.wait_for(
                cls._collect(turn, str(receipt.get("messageId", ""))), timeout
            )
        except asyncio.TimeoutError as e:
            raise LLMException("dsh 回复超时。") from e
        finally:
            if cls._turns.get(session_id) is turn:
                cls._turns.pop(session_id)
        return cls._result(events)

    @staticmethod
    async def _collect(turn: _Turn, message_id: str) -> list[dict[str, Any]]:
        """收集本轮输入被接收之后、会话回到空闲之前的会话事件

        参数:
            turn: 本轮对话
            message_id: 输入的消息id

        返回:
            list[dict[str, Any]]: 会话事件
        """
        index, started, events = 0, False, []
        while True:
            await turn.changed.wait()
            turn.changed.clear()
            while index < len(turn.events):
                method, params = turn.events[index]
                index += 1
                if method == "exit":
                    raise LLMException("dsh 运行时已退出。")
                event = params.get("event")
                if method == "session.event" and isinstance(event, dict):
                    if not started:
                        inserted = (event.get("data") or {}).get("inserted") or []
                        started = event.get("type") == "agent/inbox/spliced" and any(
                            isinstance(m, dict) and m.get("id") == message_id
                            for m in inserted
                        )
                    if started:
                        events.append(event)
                elif (
                    method == "session.status"
                    and started
                    and params.get("status") == "idle"
                ):
                    return events

    @staticmethod
    def _result(events: list[dict[str, Any]]) -> tuple[str, UsageInfo, str | None]:
        """从会话事件中取最后一条助手消息、累计用量与结束原因

        参数:
            events: 会话事件

        返回:
            tuple[str, UsageInfo, str | None]: 回复, 用量, 结束原因
        """
        text, usage, reason = "", UsageInfo(), None
        for event in events:
            data = event.get("data") or {}
            if event.get("type") == "assistant/message":
                if counts := data.get("usage"):
                    prompt = int(counts.get("inputTokens") or 0)
                    cached = int(counts.get("cacheReadTokens") or 0)
                    output = int(counts.get("outputTokens") or 0)
                    usage.prompt_tokens += prompt + cached
                    usage.prompt_cache_hit_tokens += cached
                    usage.completion_tokens += output
                    usage.total_tokens += int(
                        counts.get("totalTokens") or prompt + cached + output
                    )
                message = data.get("message") or data
                content = message.get("content") if isinstance(message, dict) else None
                if isinstance(content, list):
                    text = (
                        "".join(
                            str(block.get("text") or "")
                            for block in content
                            if isinstance(block, dict) and block.get("type") == "text"
                        )
                        or text
                    )
            elif event.get("type") == "turn/end":
                reason = (data.get("reason") or {}).get("kind")
        if reason == "error":
            raise LLMException("dsh 本轮对话出错。")
        return text, usage, reason

    @classmethod
    async def close(cls) -> None:
        """关闭运行时：请求 shutdown，未退出时结束整个进程树"""
        proc, cls._proc = cls._proc, None
        cls._key = ""
        if proc is None:
            return
        if proc.returncode is None:
            cls._proc = proc
            try:
                await cls.request("shutdown", None, 5)
            except LLMException:
                pass
            cls._proc = None
            if proc.stdin:
                proc.stdin.close()
            try:
                await asyncio.wait_for(proc.wait(), 5)
            except asyncio.TimeoutError:
                await cls._kill_tree(proc)
        for task in (cls._reader, cls._stderr):
            if task and not task.done():
                task.cancel()
        cls._reader = cls._stderr = None

    @staticmethod
    async def _kill_tree(proc: asyncio.subprocess.Process) -> None:
        """结束子进程及其全部后代进程

        参数:
            proc: 子进程
        """
        if sys.platform == "win32":
            killer = await asyncio.create_subprocess_exec(
                "taskkill",
                "/T",
                "/F",
                "/PID",
                str(proc.pid),
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.DEVNULL,
            )
            await killer.wait()
        else:
            import signal

            try:
                os.killpg(proc.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
        try:
            await asyncio.wait_for(proc.wait(), 5)
        except asyncio.TimeoutError:
            logger.warning(f"dsh 进程 {proc.pid} 未能结束", LOG_NAME)


class DshSessions:
    """zhenxun 会话 → dsh 会话的映射，存于 data/ai/dsh/sessions.json；
    人设变化或重置时换一个新的 dsh 会话"""

    _data: ClassVar[dict[str, dict[str, str]] | None] = None
    FILE = DSH_PATH / "sessions.json"

    @classmethod
    def _load(cls) -> dict[str, dict[str, str]]:
        if cls._data is None:
            try:
                cls._data = json.loads(cls.FILE.read_text(encoding="utf-8"))
            except (OSError, ValueError):
                cls._data = {}
        return cls._data  # type: ignore[return-value]

    @classmethod
    def _save(cls) -> None:
        cls.FILE.parent.mkdir(parents=True, exist_ok=True)
        cls.FILE.write_text(
            json.dumps(cls._load(), ensure_ascii=False, indent=1), encoding="utf-8"
        )

    @classmethod
    def get(cls, session_id: str, persona_key: str) -> tuple[str, bool]:
        """取会话对应的 dsh 会话，没有或人设已变化时新建

        参数:
            session_id: zhenxun 会话id
            persona_key: 人设标识（id 与修改时间）

        返回:
            tuple[str, bool]: dsh 会话id, 是否为新会话
        """
        import uuid

        data = cls._load()
        item = data.get(session_id)
        if item and item.get("persona") == persona_key:
            return item["id"], False
        data[session_id] = {"id": f"zx-{uuid.uuid4().hex}", "persona": persona_key}
        cls._save()
        return data[session_id]["id"], True

    @classmethod
    def reset(cls, session_id: str) -> None:
        """忘记会话对应的 dsh 会话，下一轮换新会话

        参数:
            session_id: zhenxun 会话id
        """
        if cls._load().pop(session_id, None) is not None:
            cls._save()
