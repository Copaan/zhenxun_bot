"""内置 AI 聊天的知识库管理：可选组件、知识库、文档与检索测试"""

from __future__ import annotations

import asyncio
import base64
from collections import Counter
import io
from typing import Any

from fastapi import APIRouter, File, Form, HTTPException, Query, UploadFile
from pydantic import BaseModel, Field

from ....base_model import Result
from ....utils import authentication

router = APIRouter()

THUMB_SIZE = 240
"""分块图片缩略图边长"""


class BasePayload(BaseModel):
    name: str = Field(min_length=1, max_length=64)
    description: str = Field(default="", max_length=500)
    embedding_model: str | None = Field(default=None, max_length=255)
    chunk_size: int = Field(default=500, ge=100, le=4000)
    chunk_overlap: int = Field(default=50, ge=0, le=1000)
    top_k: int = Field(default=5, ge=1, le=20)


class GlobalPayload(BaseModel):
    enabled: bool


class SearchPayload(BaseModel):
    kb_ids: list[int] = Field(min_length=1, max_length=20)
    query: str = Field(min_length=1, max_length=500)
    top_k: int | None = Field(default=None, ge=1, le=20)


def _base_item(kb: Any, docs: int = 0) -> dict[str, Any]:
    from zhenxun.builtin_plugins.ai_chat._knowledge import (
        KnowledgeManage,
        global_bases,
    )

    return {
        "id": kb.id,
        "name": kb.name,
        "description": kb.description,
        "embedding_model": kb.embedding_model,
        "dimensions": kb.dimensions,
        "chunk_size": kb.chunk_size,
        "chunk_overlap": kb.chunk_overlap,
        "top_k": kb.top_k,
        "docs": docs,
        "global": kb.id in global_bases(),
        "rebuilding": KnowledgeManage.progress(kb.id),
        "created_at": kb.created_at,
        "updated_at": kb.updated_at,
    }


def _doc_item(doc: Any) -> dict[str, Any]:
    return {
        "id": doc.id,
        "kb_id": doc.kb_id,
        "filename": doc.filename,
        "kind": doc.kind,
        "size": doc.size,
        "chunks": doc.chunks,
        "status": doc.status,
        "error": doc.error,
        "created_at": doc.created_at,
        "updated_at": doc.updated_at,
    }


def _thumb(data: bytes) -> str | None:
    """生成图片缩略图的 data URL"""
    from PIL import Image

    try:
        with Image.open(io.BytesIO(data)) as image:
            image.thumbnail((THUMB_SIZE, THUMB_SIZE))
            buffer = io.BytesIO()
            image.convert("RGB").save(buffer, "JPEG", quality=80)
    except Exception:
        return None
    return f"data:image/jpeg;base64,{base64.b64encode(buffer.getvalue()).decode()}"


async def _image_thumb(image_path: str | None) -> str | None:
    from zhenxun.builtin_plugins.ai_chat._knowledge import KnowledgeManage

    if not image_path:
        return None
    path = KnowledgeManage.image_file(image_path)
    if not path.exists():
        return None
    return await asyncio.to_thread(_thumb, await asyncio.to_thread(path.read_bytes))


async def _get_base(kb_id: int) -> Any:
    from zhenxun.models.ai_knowledge_base import AIKnowledgeBase

    kb = await AIKnowledgeBase.get_or_none(id=kb_id)
    if kb is None:
        raise HTTPException(404, detail="知识库不存在")
    return kb


@router.get("/knowledge/status", dependencies=[authentication()], response_model=Result)
async def knowledge_status() -> Result:
    from zhenxun.builtin_plugins.ai_chat._components import component_status
    from zhenxun.builtin_plugins.ai_chat._knowledge import SUPPORTED_TYPES
    from zhenxun.services.ai.llm.manager import get_default_model

    return Result.ok(
        {
            "components": component_status("kb_docs", "kb_ocr"),
            "models": {
                task: get_default_model(task)
                for task in ("embedding", "vision", "rerank")
            },
            "types": sorted(SUPPORTED_TYPES),
        }
    )


@router.get("/knowledge/bases", dependencies=[authentication()], response_model=Result)
async def list_bases() -> Result:
    from zhenxun.models.ai_knowledge_base import AIKnowledgeBase
    from zhenxun.models.ai_knowledge_doc import AIKnowledgeDoc

    bases = await AIKnowledgeBase.all().order_by("id")
    kb_ids = await AIKnowledgeDoc.all().values_list("kb_id", flat=True)
    counts: Counter[Any] = Counter(kb_ids)
    return Result.ok([_base_item(kb, counts.get(kb.id, 0)) for kb in bases])


@router.post("/knowledge/bases", dependencies=[authentication()], response_model=Result)
async def create_base(payload: BasePayload) -> Result:
    from zhenxun.builtin_plugins.ai_chat._knowledge import KnowledgeManage

    try:
        kb = await KnowledgeManage.create_base(**payload.model_dump())
    except ValueError as e:
        raise HTTPException(422, detail=str(e)) from e
    return Result.ok(_base_item(kb), info="知识库已创建。")


@router.put(
    "/knowledge/bases/{kb_id}", dependencies=[authentication()], response_model=Result
)
async def update_base(kb_id: int, payload: BasePayload) -> Result:
    from zhenxun.builtin_plugins.ai_chat._knowledge import KnowledgeManage

    kb = await _get_base(kb_id)
    try:
        kb = await KnowledgeManage.update_base(kb, **payload.model_dump())
    except ValueError as e:
        raise HTTPException(422, detail=str(e)) from e
    info = "已保存，正在后台重建。" if KnowledgeManage.progress(kb_id) else "已保存。"
    return Result.ok(_base_item(kb), info=info)


@router.put(
    "/knowledge/bases/{kb_id}/global",
    dependencies=[authentication()],
    response_model=Result,
)
async def set_global_base(kb_id: int, payload: GlobalPayload) -> Result:
    from zhenxun.builtin_plugins.ai_chat._knowledge import set_global_base

    kb = await _get_base(kb_id)
    set_global_base(kb_id, payload.enabled)
    return Result.ok(
        _base_item(kb), info="已全局启用。" if payload.enabled else "已取消全局启用。"
    )


@router.delete(
    "/knowledge/bases/{kb_id}", dependencies=[authentication()], response_model=Result
)
async def delete_base(kb_id: int) -> Result:
    from zhenxun.builtin_plugins.ai_chat._knowledge import KnowledgeManage

    await _get_base(kb_id)
    await KnowledgeManage.delete_base(kb_id)
    return Result.ok(info="知识库已删除。")


@router.post(
    "/knowledge/bases/{kb_id}/rebuild",
    dependencies=[authentication()],
    response_model=Result,
)
async def rebuild_base(kb_id: int) -> Result:
    from zhenxun.builtin_plugins.ai_chat._knowledge import KnowledgeManage

    await _get_base(kb_id)
    if not KnowledgeManage.rebuild(kb_id):
        return Result.ok(info="正在重建中。")
    return Result.ok(info="已开始在后台重建。")


@router.get(
    "/knowledge/bases/{kb_id}/docs",
    dependencies=[authentication()],
    response_model=Result,
)
async def list_docs(kb_id: int) -> Result:
    from zhenxun.builtin_plugins.ai_chat._knowledge import KnowledgeManage

    kb = await _get_base(kb_id)
    docs = await KnowledgeManage.list_docs(kb_id)
    return Result.ok(
        {
            "items": [_doc_item(doc) for doc in docs],
            "rebuilding": KnowledgeManage.progress(kb.id),
        }
    )


@router.post(
    "/knowledge/bases/{kb_id}/docs",
    dependencies=[authentication()],
    response_model=Result,
)
async def upload_doc(
    kb_id: int,
    file: UploadFile = File(...),
    extract_images: bool = Form(default=False),
) -> Result:
    from zhenxun.builtin_plugins.ai_chat._knowledge import (
        MAX_FILE_SIZE,
        KnowledgeManage,
    )

    kb = await _get_base(kb_id)
    data = await file.read(MAX_FILE_SIZE + 1)
    try:
        doc = await KnowledgeManage.add_document(
            kb, file.filename or "", data, extract_images
        )
    except ValueError as e:
        raise HTTPException(422, detail=str(e)) from e
    return Result.ok(_doc_item(doc), info="已上传，正在后台处理。")


@router.delete(
    "/knowledge/docs/{doc_id}", dependencies=[authentication()], response_model=Result
)
async def delete_doc(doc_id: int) -> Result:
    from zhenxun.builtin_plugins.ai_chat._knowledge import KnowledgeManage
    from zhenxun.models.ai_knowledge_doc import AIKnowledgeDoc

    doc = await AIKnowledgeDoc.get_or_none(id=doc_id)
    if doc is None:
        raise HTTPException(404, detail="文档不存在")
    await KnowledgeManage.delete_document(doc)
    return Result.ok(info="文档已删除。")


@router.get(
    "/knowledge/docs/{doc_id}/chunks",
    dependencies=[authentication()],
    response_model=Result,
)
async def list_chunks(
    doc_id: int,
    offset: int = Query(default=0, ge=0),
    limit: int = Query(default=20, ge=1, le=100),
) -> Result:
    from zhenxun.models.ai_knowledge_chunk import AIKnowledgeChunk

    query = AIKnowledgeChunk.filter(doc_id=doc_id)
    chunks = await query.order_by("seq", "id").offset(offset).limit(limit)
    return Result.ok(
        {
            "total": await query.count(),
            "items": [
                {
                    "id": c.id,
                    "seq": c.seq,
                    "content": c.content,
                    "has_vector": bool(c.vector),
                    "has_image_vector": bool(c.image_vector),
                    "thumb": await _image_thumb(c.image_path),
                }
                for c in chunks
            ],
        }
    )


@router.post(
    "/knowledge/search", dependencies=[authentication()], response_model=Result
)
async def search(payload: SearchPayload) -> Result:
    from zhenxun.builtin_plugins.ai_chat._knowledge import KnowledgeManage
    from zhenxun.models.ai_knowledge_doc import AIKnowledgeDoc

    hits, notices = await KnowledgeManage.search(
        payload.kb_ids, payload.query, payload.top_k
    )
    names = dict(
        await AIKnowledgeDoc.filter(id__in=[h.doc_id for h in hits]).values_list(
            "id", "filename"
        )
    )
    return Result.ok(
        {
            "notices": notices,
            "items": [
                {
                    "chunk_id": h.chunk_id,
                    "kb_id": h.kb_id,
                    "doc_id": h.doc_id,
                    "filename": names.get(h.doc_id, ""),
                    "content": h.content,
                    "score": round(h.score, 4),
                    "thumb": await _image_thumb(h.image_path),
                }
                for h in hits
            ],
        }
    )
