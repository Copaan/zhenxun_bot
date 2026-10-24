"""AI 聊天插件开关与默认人设"""

from __future__ import annotations

import json
from typing import Any

from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from zhenxun.services.log import logger

from ....base_model import Result
from ....utils import authentication
from ._store import config_revision

router = APIRouter()


class ChatPluginSwitchRequest(BaseModel):
    expected_revision: str = Field(min_length=64, max_length=64)
    module: str = Field(min_length=1, max_length=255)
    enabled: bool


class PersonaUpdate(BaseModel):
    expected_revision: str = Field(min_length=64, max_length=64)
    name: str = Field(min_length=1, max_length=80)
    prompt: str = Field(min_length=1, max_length=20000)
    style: str = Field(default="", max_length=2000)
    tone_examples: list[str] = Field(default_factory=list, max_length=12)
    preset_dialogues: list[str] = Field(default_factory=list, max_length=12)
    enabled: bool = True


def _persona_revision(payload: dict[str, Any]) -> str:
    content = json.dumps(
        payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    )
    return config_revision(content)


def _persona_state() -> tuple[Any, dict[str, Any]] | None:
    from zhenxun.services.ai.chat_switch import persona_provider

    provider = persona_provider()
    if provider is None:
        return None
    try:
        persona = provider.get_default_persona()
    except (OSError, RuntimeError, ValueError) as error:
        logger.warning(f"读取 AI 人设失败: {provider.__name__}", "WebUi", e=error)
        return None
    return (provider, persona) if isinstance(persona, dict) else None


def _persona_view(persona: dict[str, Any]) -> dict[str, Any]:
    return {
        "name": str(persona.get("name") or ""),
        "prompt": str(persona.get("prompt") or ""),
        "style": str(persona.get("style") or ""),
        "tone_examples": [str(item) for item in persona.get("tone_examples") or ()],
        "preset_dialogues": [
            str(item) for item in persona.get("preset_dialogues") or ()
        ],
        "enabled": bool(persona.get("enabled", True)),
    }


@router.get(
    "/chat-plugins/switches", dependencies=[authentication()], response_model=Result
)
async def chat_plugin_switches() -> Result:
    from zhenxun.services.ai.chat_switch import loaded_ai_chat_plugins
    from zhenxun.services.plugin_policy import plugin_policy_service

    state = await plugin_policy_service.global_state()
    chat_plugins = []
    for plugin in loaded_ai_chat_plugins():
        metadata = plugin.metadata
        extra = metadata.extra if metadata and isinstance(metadata.extra, dict) else {}
        chat_plugins.append(
            {
                "module": plugin.name,
                "module_path": plugin.module_name,
                "name": metadata.name if metadata else plugin.name,
                "description": metadata.description if metadata else "",
                "usage": metadata.usage if metadata else "",
                "author": extra.get("author"),
                "version": extra.get("version"),
            }
        )
    return Result.ok({**state, "chat_plugins": chat_plugins})


@router.put(
    "/chat-plugins/switches", dependencies=[authentication()], response_model=Result
)
async def update_chat_plugin_switch(payload: ChatPluginSwitchRequest) -> Result:
    from zhenxun.models.plugin_info import PluginInfo
    from zhenxun.services.ai.chat_switch import AI_CHAT_CAPABILITY, plugin_capabilities

    from ..plugin_manage.store import _plugin_capabilities

    plugin = await PluginInfo.get_plugin(module=payload.module)
    supported = plugin is not None and AI_CHAT_CAPABILITY in plugin_capabilities(
        plugin.module_path or plugin.module
    )
    if plugin is not None and not supported:
        from zhenxun.builtin_plugins.plugin_store.data_source import StoreManager

        catalogs = await StoreManager.get_data()
        supported = any(
            item.module == plugin.module
            and AI_CHAT_CAPABILITY in _plugin_capabilities(item)
            for catalog in catalogs
            for item in catalog
        )
    if not supported:
        raise HTTPException(422, detail={"code": "ai_chat_plugin_not_supported"})
    from zhenxun.services.plugin_policy import (
        PluginPolicyConflict,
        plugin_policy_service,
    )

    try:
        state = await plugin_policy_service.set_global(
            plugin.module, payload.enabled, expected_revision=payload.expected_revision
        )
    except PluginPolicyConflict as error:
        raise HTTPException(
            409, detail={"code": error.code, **error.details}
        ) from error
    return Result.ok(
        {**state, "operation": None},
        info="已更新数据库插件策略，进行中的请求正常收尾。",
    )


@router.get(
    "/personas/default",
    dependencies=[authentication()],
    response_model=Result,
    response_class=JSONResponse,
)
async def get_default_persona() -> Result:
    state = _persona_state()
    if state is None:
        return Result.ok(
            {
                "available": False,
                "reason": "ai_chat_persona_not_available",
            }
        )
    _, persona = state
    view = _persona_view(persona)
    return Result.ok(
        {
            "available": True,
            "revision": _persona_revision(view),
            "persona": view,
        }
    )


@router.put(
    "/personas/default",
    dependencies=[authentication()],
    response_model=Result,
    response_class=JSONResponse,
)
async def update_default_persona(payload: PersonaUpdate) -> Result:
    state = _persona_state()
    if state is None:
        raise HTTPException(
            status_code=404,
            detail={
                "code": "ai_chat_persona_not_available",
                "message": "当前未安装支持人设管理的 AI 聊天插件。",
            },
        )
    module, persona = state
    current = _persona_view(persona)
    if payload.expected_revision != _persona_revision(current):
        raise HTTPException(
            status_code=409,
            detail={
                "code": "persona_revision_conflict",
                "message": "AI 人设已被其他操作修改，请重新加载后再保存。",
            },
        )
    updated = {
        "name": payload.name.strip(),
        "prompt": payload.prompt.strip(),
        "style": payload.style.strip(),
        "tone_examples": [
            item.strip() for item in payload.tone_examples if item.strip()
        ],
        "preset_dialogues": [
            item.strip() for item in payload.preset_dialogues if item.strip()
        ],
        "enabled": payload.enabled,
    }
    invalid = HTTPException(
        status_code=422,
        detail={
            "code": "persona_invalid",
            "message": "AI 人设内容无效。",
        },
    )
    try:
        saved = module.save_default_persona(updated)
    except ValueError as error:
        raise invalid from error
    except OSError as error:
        logger.error("AI 人设保存失败", "WebUi", e=error)
        raise HTTPException(
            status_code=500,
            detail={
                "code": "persona_save_failed",
                "message": "AI 人设保存失败，请检查数据目录写入权限。",
            },
        ) from error
    if not isinstance(saved, dict):
        raise invalid
    view = _persona_view(saved)
    return Result.ok(
        {
            "available": True,
            "revision": _persona_revision(view),
            "persona": view,
            "apply_mode": "hot_reloaded",
            "restart_required": False,
            "restart_available": False,
            "reason_codes": [],
        },
        info="AI 人设已保存，将从下一条消息开始生效。",
    )
