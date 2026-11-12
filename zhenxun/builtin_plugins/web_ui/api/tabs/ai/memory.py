"""内置 AI 聊天的长期记忆管理：召回模式、浏览、编辑、召回测试与重算向量"""

from __future__ import annotations

from typing import Any, Literal

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field
from tortoise.functions import Count

from ....base_model import Result
from ....utils import authentication

router = APIRouter()

PAGE_SIZE = 50
"""每页条数"""


class ScopePayload(BaseModel):
    group_id: str | None = Field(default=None, max_length=255)
    user_id: str | None = Field(default=None, max_length=255)

    @property
    def scope(self) -> tuple[str | None, str | None]:
        """记忆范围，群号和私聊用户不是正好填一个时报 422"""
        if bool(self.group_id) == bool(self.user_id):
            raise HTTPException(422, detail="群号和私聊用户只能填一个")
        return (self.group_id, None) if self.group_id else (None, self.user_id)


class MemoryUpdate(BaseModel):
    content: str = Field(min_length=1, max_length=2000)
    topics: list[str] = Field(default_factory=list, max_length=10)
    facts: list[str] = Field(default_factory=list, max_length=20)
    importance: float = Field(default=0.6, ge=0, le=1)
    pinned: bool | None = None


class MemoryCreate(ScopePayload, MemoryUpdate):
    pass


class MemoryEdit(MemoryUpdate):
    archived: bool = False


class RecallPayload(ScopePayload):
    query: str = Field(min_length=1, max_length=500)
    top_k: int | None = Field(default=None, ge=1, le=20)


class EmbeddingPayload(BaseModel):
    enabled: bool


class ModePayload(BaseModel):
    mode: Literal["off", "inject", "tool"]


class SummarizePayload(BaseModel):
    session_id: str = Field(min_length=1, max_length=255)


def _item(memory: Any) -> dict[str, Any]:
    return {
        "id": memory.id,
        "group_id": memory.group_id,
        "user_id": memory.user_id,
        "content": memory.content,
        "topics": memory.topics or [],
        "facts": memory.facts or [],
        "participants": memory.participants or [],
        "importance": round(memory.importance, 3),
        "access_count": memory.access_count,
        "last_access_at": memory.last_access_at,
        "source": memory.source,
        "author_id": memory.author_id,
        "pinned": memory.pinned,
        "archived": memory.archived,
        "has_vector": bool(memory.vector),
        "created_at": memory.created_at,
        "updated_at": memory.updated_at,
    }


def _clean(values: list[str], size: int = 100) -> list[str]:
    return list(dict.fromkeys(v.strip()[:size] for v in values if v.strip()))


async def _get_memory(memory_id: int) -> Any:
    from zhenxun.models.ai_memory import AIMemory

    memory = await AIMemory.get_or_none(id=memory_id)
    if memory is None:
        raise HTTPException(404, detail="记忆不存在")
    return memory


@router.get("/memory/status", dependencies=[authentication()], response_model=Result)
async def memory_status() -> Result:
    from zhenxun.builtin_plugins.ai_chat._data_source import MODULE
    from zhenxun.builtin_plugins.ai_chat._memory import (
        Memory,
        embedding_model,
        memory_mode,
    )
    from zhenxun.configs.config import Config
    from zhenxun.models.ai_memory import AIMemory

    rows = (
        await AIMemory.filter(archived=False)
        .annotate(n=Count("id"))
        .group_by("group_id", "user_id")
        .values("group_id", "user_id", "n")
    )
    from zhenxun.models.ai_memory_cursor import AIMemoryCursor

    cursors = await AIMemoryCursor.all().order_by("-updated_at").limit(100)
    enabled = Config.get_config(MODULE, "MEMORY_EMBEDDING")
    return Result.ok(
        {
            "mode": memory_mode(),
            "rebuilding": Memory.rebuilding,
            "rebuild_result": Memory.rebuild_result,
            "local_agent_only": True,
            "summaries": [
                {"session_id": c.session_id, "revision": c.revision, **(c.state or {})}
                for c in cursors
                if (c.state or {}).get("status") in {"running", "retry_wait", "pending"}
            ],
            "embedding_enabled": enabled is None or bool(enabled),
            "embedding_model": embedding_model(),
            "scopes": sorted(
                (
                    {
                        "group_id": r["group_id"],
                        "user_id": r["user_id"],
                        "count": r["n"],
                    }
                    for r in rows
                ),
                key=lambda r: -r["count"],
            ),
            "archived": await AIMemory.filter(archived=True).count(),
        }
    )


@router.get("/memory/items", dependencies=[authentication()], response_model=Result)
async def list_memories(
    group_id: str = Query(default="", max_length=255),
    user_id: str = Query(default="", max_length=255),
    query: str = Query(default="", max_length=64),
    source: Literal["", "auto", "tool", "webui"] = "",
    archived: bool = False,
    page: int = Query(default=1, ge=1),
) -> Result:
    from tortoise.expressions import Q

    from zhenxun.models.ai_memory import AIMemory

    qs = AIMemory.filter(archived=archived)
    if group_id:
        qs = qs.filter(group_id=group_id)
    if user_id:
        qs = qs.filter(user_id=user_id)
    if source:
        qs = qs.filter(source=source)
    if query := query.strip():
        qs = qs.filter(Q(content__icontains=query) | Q(tokens__icontains=query))
    total = await qs.count()
    rows = await qs.order_by("-id").offset((page - 1) * PAGE_SIZE).limit(PAGE_SIZE)
    return Result.ok(
        {"items": [_item(m) for m in rows], "total": total, "page_size": PAGE_SIZE}
    )


@router.post("/memory/items", dependencies=[authentication()], response_model=Result)
async def create_memory(payload: MemoryCreate) -> Result:
    from zhenxun.builtin_plugins.ai_chat._memory import Memory
    from zhenxun.configs.config import Config

    memory, created = await Memory.add(
        payload.scope,
        payload.content,
        _clean(payload.topics, 32),
        _clean(payload.facts),
        importance=payload.importance,
        source="webui",
        author_id="webui:"
        + str(Config.get_config("web-ui", "username") or "administrator"),
        pinned=True if payload.pinned is None else payload.pinned,
    )
    info = (
        "已添加记忆。"
        if created
        else f"已有完全相同的记忆 #{memory.id}，未覆盖其他内容。"
    )
    return Result.ok(_item(memory), info=info)


@router.put(
    "/memory/items/{memory_id}",
    dependencies=[authentication()],
    response_model=Result,
)
async def update_memory(memory_id: int, payload: MemoryEdit) -> Result:
    from zhenxun.builtin_plugins.ai_chat._memory import Memory

    memory = await Memory.update(
        await _get_memory(memory_id),
        content=payload.content.strip(),
        topics=_clean(payload.topics, 32),
        facts=_clean(payload.facts),
        importance=payload.importance,
        archived=payload.archived,
        **({"pinned": payload.pinned} if payload.pinned is not None else {}),
    )
    return Result.ok(_item(memory), info="已保存。")


@router.delete(
    "/memory/items/{memory_id}",
    dependencies=[authentication()],
    response_model=Result,
)
async def delete_memory(memory_id: int) -> Result:
    from zhenxun.builtin_plugins.ai_chat._memory import Memory

    await Memory.remove(await _get_memory(memory_id))
    return Result.ok(info="已删除。")


@router.post("/memory/recall", dependencies=[authentication()], response_model=Result)
async def recall(payload: RecallPayload) -> Result:
    from zhenxun.builtin_plugins.ai_chat._memory import Memory

    hits = await Memory.recall(
        payload.scope, payload.query, payload.top_k, reinforce=False
    )
    return Result.ok(
        [{**_item(memory), "score": round(score, 4)} for memory, score in hits]
    )


@router.post(
    "/memory/summarize", dependencies=[authentication()], response_model=Result
)
async def summarize(payload: SummarizePayload) -> Result:
    from zhenxun.builtin_plugins.ai_chat._memory import (
        Memory,
        memory_mode,
        session_scope,
    )

    if session_scope(payload.session_id) is None:
        raise HTTPException(422, detail="这个会话没有长期记忆")
    if memory_mode() == "off":
        return Result.fail("长期记忆已关闭，请先开启后再整理。")
    memory = await Memory.summarize(
        payload.session_id, await Memory.start_of(payload.session_id), retry=True
    )
    if memory is None:
        from zhenxun.models.ai_memory_cursor import AIMemoryCursor

        cursor = await AIMemoryCursor.get_or_none(session_id=payload.session_id)
        state = (cursor.state or {}) if cursor else {}
        if payload.session_id in Memory._running:
            return Result.fail("该会话正在整理，请稍后查看结果。")
        if state.get("status") in {"pending", "retry_wait"}:
            return Result.fail(state.get("error") or "整理未完成，原进度已保留。")
        return Result.ok(None, info="没有新的可整理内容，或本段无需形成长期记忆。")
    return Result.ok(_item(memory), info=f"已整理为记忆 #{memory.id}。")


@router.post("/memory/rebuild", dependencies=[authentication()], response_model=Result)
async def rebuild() -> Result:
    from zhenxun.builtin_plugins.ai_chat._memory import Memory

    if not Memory.rebuild():
        return Result.fail("正在重算向量。")
    return Result.ok(info="已开始重算向量。")


@router.put("/memory/mode", dependencies=[authentication()], response_model=Result)
async def update_mode(payload: ModePayload) -> Result:
    from zhenxun.builtin_plugins.ai_chat._data_source import MODULE
    from zhenxun.builtin_plugins.ai_chat._memory import Memory
    from zhenxun.configs.config import Config
    from zhenxun.services.log import logger

    Config.set_config(MODULE, "MEMORY_MODE", payload.mode)
    Config.save(save_simple_data=True)
    if payload.mode == "off":
        await Memory.stop()
    logger.info(f"WebUI 修改了长期记忆模式: {payload.mode}", "WebUi")
    names = {"off": "已关闭长期记忆。", "inject": "已切换为自动召回。"}
    return Result.ok(info=names.get(payload.mode, "已切换为按需召回。"))


@router.put("/memory/embedding", dependencies=[authentication()], response_model=Result)
async def update_embedding(payload: EmbeddingPayload) -> Result:
    from zhenxun.builtin_plugins.ai_chat._data_source import MODULE
    from zhenxun.configs.config import Config
    from zhenxun.services.log import logger

    Config.set_config(MODULE, "MEMORY_EMBEDDING", payload.enabled)
    Config.save(save_simple_data=True)
    logger.info(f"WebUI 修改了长期记忆向量召回: 开启={payload.enabled}", "WebUi")
    return Result.ok(
        info="已开启向量召回。"
        if payload.enabled
        else "已关闭向量召回，只用关键词召回。"
    )
