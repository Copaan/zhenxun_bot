"""AI 聊天会话与用量"""

from __future__ import annotations

from datetime import date
from typing import Any

from fastapi import APIRouter, HTTPException, Query
from tortoise.functions import Count, Max, Sum

from zhenxun.services.log import logger

from ....base_model import Result
from ....utils import authentication

router = APIRouter()

from .console import CONSOLE_PREFIX

SESSION_PREFIX = "ai_chat:"


def _parse_session(session_id: str) -> dict[str, Any]:
    """拆分聊天会话id，群聊会话没有用户（旧版按群和用户区分的会话仍带用户）

    参数:
        session_id: 会话id

    返回:
        dict[str, Any]: Bot id、群号（私聊为空）、用户id
    """
    from zhenxun.builtin_plugins.ai_chat._data_source import parse_session

    bot_id, group_id, user_id = parse_session(session_id)
    return {"bot_id": bot_id, "group_id": group_id, "user_id": user_id or ""}


def _message_text(content: Any) -> str:
    """把短期记忆中的消息内容转为可读文本，媒体用占位表示，略去思考过程

    参数:
        content: 消息内容

    返回:
        str: 文本
    """
    if isinstance(content, str):
        return content
    return "".join(
        str(part.get("text", ""))
        if part.get("type") == "text"
        else f"[{part.get('type')}]"
        for part in content or ()
        if isinstance(part, dict) and part.get("type") != "thought"
    )


def _require_session(session_id: str) -> str:
    if not session_id.startswith((SESSION_PREFIX, CONSOLE_PREFIX)):
        raise HTTPException(422, detail={"code": "session_invalid"})
    return session_id


@router.get("/chat/sessions", dependencies=[authentication()], response_model=Result)
async def list_sessions(
    keyword: str = Query(default="", max_length=64),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=20, ge=1, le=100),
) -> Result:
    from zhenxun.models.ai_chat_message import AIChatMessage

    query = AIChatMessage.filter(session_id__startswith=SESSION_PREFIX)
    if keyword.strip():
        query = query.filter(session_id__contains=keyword.strip())
    total = len(await query.distinct().values_list("session_id", flat=True))
    rows = (
        await query.annotate(messages=Count("id"), last_at=Max("created_at"))
        .group_by("session_id")
        .order_by("-last_at")
        .offset((page - 1) * page_size)
        .limit(page_size)
        .values("session_id", "messages", "last_at")
    )
    items = [
        {
            "session_id": row["session_id"],
            **_parse_session(row["session_id"]),
            "messages": row["messages"],
            "last_at": row["last_at"].isoformat() if row["last_at"] else None,
        }
        for row in rows
    ]
    return Result.ok({"total": total, "items": items})


@router.get(
    "/chat/sessions/messages", dependencies=[authentication()], response_model=Result
)
async def session_messages(
    session_id: str = Query(min_length=1, max_length=255),
    raw: bool = False,
) -> Result:
    from zhenxun.models.ai_chat_message import AIChatMessage

    rows = await AIChatMessage.filter(session_id=_require_session(session_id)).order_by(
        "id"
    )
    return Result.ok(
        {
            "session_id": session_id,
            **_parse_session(session_id),
            "messages": [
                {
                    "role": row.role,
                    "text": _message_text(row.content),
                    **(
                        {"content": row.content, "metadata": row.metadata}
                        if raw
                        else {}
                    ),
                    "is_summary": row.is_summary,
                    "created_at": row.created_at.isoformat(),
                }
                for row in rows
            ],
        }
    )


@router.delete("/chat/sessions", dependencies=[authentication()], response_model=Result)
async def reset_session(
    session_id: str = Query(min_length=1, max_length=255),
) -> Result:
    from zhenxun.builtin_plugins.ai_chat._data_source import AIChatManager

    await AIChatManager.reset(_require_session(session_id))
    logger.info(f"WebUI 重置了 AI 聊天会话 {session_id}", "WebUi")
    return Result.ok(info="会话记忆已清空。")


@router.get("/chat/usage", dependencies=[authentication()], response_model=Result)
async def chat_usage(limit: int = Query(default=20, ge=1, le=100)) -> Result:
    from zhenxun.models.ai_token_usage import AITokenUsage

    today = AITokenUsage.filter(day=date.today())
    rows = await today.order_by("-tokens").limit(limit).values("user_id", "tokens")
    total = (await today.annotate(total=Sum("tokens")).values("total"))[0]["total"]
    return Result.ok(
        {
            "day": date.today().isoformat(),
            "total": int(total or 0),
            "users": await today.count(),
            "top": rows,
        }
    )
