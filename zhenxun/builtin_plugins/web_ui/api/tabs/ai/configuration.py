from __future__ import annotations

import asyncio
import contextvars
from copy import deepcopy
from functools import cache
import hashlib
from importlib import import_module
from io import StringIO
import json
from pathlib import Path
import re
import time
from typing import Any, Literal
from urllib.parse import urlparse

from fastapi import APIRouter, HTTPException
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse
import httpx
from pydantic import BaseModel, Field

from zhenxun.configs.config import Config
from zhenxun.services.ai.config import get_llm_config
from zhenxun.services.ai.config.models import LLMConfig, ProviderConfig
from zhenxun.services.ai.core.exceptions import (
    AuthenticationException,
    ConfigurationException,
    InvalidRequestException,
    LLMException,
    LocationNotSupportedException,
    QuotaExceededException,
    RateLimitException,
    ResponseParseException,
)
from zhenxun.services.ai.core.messages import (
    ChatRequest,
    EmbeddingRequest,
    ImageRequest,
    LLMMessage,
    RerankRequest,
    SpeechRequest,
)
from zhenxun.services.ai.core.messages.parts import EmbedBatch, EmbedPayload, TextPart
from zhenxun.services.ai.core.models import ModelDetail, ModelIdentity
from zhenxun.services.ai.core.options import GenerationConfig
from zhenxun.services.ai.llm.adapters.base import join_api_url
from zhenxun.services.ai.llm.adapters.factory import (
    LLMAdapterFactory,
    get_adapter_for_api_type,
)
from zhenxun.services.ai.llm.manager import get_default_api_base_for_type
from zhenxun.services.ai.llm.system.capabilities import (
    DEFAULT_PERMISSIVE_CAPABILITIES,
    get_model_capabilities,
)
from zhenxun.services.ai.llm.system.network import health_manager, http_client_manager
from zhenxun.services.log import logger
from zhenxun.services.plugin_host import RuntimeOperation
from zhenxun.services.runtime_config import reload_runtime_config
from zhenxun.utils._restart_utils import issue_restart_ticket
from zhenxun.utils.pydantic_compat import (
    model_dump,
    model_json_schema,
    parse_as,
)

from ....apply_result import (
    APPLY_CONFIG_RELOADED,
    APPLY_NEW_SESSION,
    APPLY_NO_CHANGE,
    APPLY_RESTART_PENDING,
    apply_result_data,
    update_pending_restart,
)
from ....base_model import Result
from ....config_validation import _yaml_parser
from ....restart_service import restart_status_data
from ....utils import authentication
from ...configure.persistence import _write_transaction

router = APIRouter()

_CONFIG_FILE = Path("data/config.yaml")
_SECTIONS = {
    "default_models": "default_models",
    "context": "context_settings",
    "agent": "agent_settings",
    "sandbox": "sandbox",
    "advanced": None,
}
_ADVANCED_KEYS = {"client_settings", "debug_log", "provider_settings"}
_DISCOVERY_TYPES = {"openai", "openai_responses", "deepseek", "openrouter", "gemini"}
_DISCOVERY_TIMEOUT = 12.0
_PERSIST_LOCK = asyncio.Lock()
# 在空上下文中读取，避免把 web_ui 登记为全部 AI 配置项的导入期读取方
_SANDBOX_STARTUP_VALUES = contextvars.Context().run(
    lambda: model_dump(get_llm_config().sandbox)
)
_SANDBOX_STARTUP_KEYS = {"enable_sandbox", "sandbox_type"}


class ProviderSettingsUpdate(BaseModel):
    expected_revision: str = Field(min_length=64, max_length=64)
    name: str = Field(min_length=1, max_length=80)
    api_type: str = Field(min_length=1, max_length=64)
    api_base: str | None = Field(default=None, max_length=500)
    timeout: int = Field(default=180, ge=1, le=1800)
    temperature: float | None = Field(default=None, ge=0, le=2)
    max_output_tokens: int | None = Field(default=None, ge=1)
    api_keys: list[str] | None = None
    models: list[dict[str, Any]] | None = None


class ProviderModelsUpdate(BaseModel):
    expected_revision: str = Field(min_length=64, max_length=64)
    models: list[dict[str, Any]]


class SectionUpdate(BaseModel):
    expected_revision: str = Field(min_length=64, max_length=64)
    value: Any


class RoutingValidationRequest(BaseModel):
    value: Any
    default_models: dict[str, Any] | None = None
    expected_revision: str | None = None


class RoutingUpdate(BaseModel):
    expected_revision: str = Field(min_length=64, max_length=64)
    model_groups: Any
    default_models: dict[str, Any]


class ReferenceRepairRequest(BaseModel):
    expected_revision: str = Field(min_length=64, max_length=64)


class ChatPluginSwitchRequest(ReferenceRepairRequest):
    module: str = Field(min_length=1, max_length=255)
    enabled: bool


class ProviderDiscoveryRequest(BaseModel):
    expected_revision: str | None = Field(default=None, min_length=64, max_length=64)
    provider_name: str | None = None
    saved_provider_name: str | None = Field(default=None, max_length=80)
    api_type: str | None = None
    api_base: str | None = None
    api_key: str | None = None


class ModelTestRequest(BaseModel):
    model: str
    task: Literal["chat", "embedding", "rerank", "image", "tts"] = "chat"
    confirmed_paid_request: bool = False
    expected_revision: str | None = Field(default=None, min_length=64, max_length=64)
    provider_name: str | None = Field(default=None, max_length=80)
    saved_provider_name: str | None = Field(default=None, max_length=80)
    api_type: str | None = Field(default=None, max_length=64)
    api_base: str | None = Field(default=None, max_length=500)
    api_key: str | None = None
    timeout: int | None = Field(default=None, ge=1, le=1800)
    model_settings: dict[str, Any] | None = Field(default=None, alias="model_config")


class PersonaUpdate(BaseModel):
    expected_revision: str = Field(min_length=64, max_length=64)
    name: str = Field(min_length=1, max_length=80)
    prompt: str = Field(min_length=1, max_length=20000)
    style: str = Field(default="", max_length=2000)
    tone_examples: list[str] = Field(default_factory=list, max_length=12)
    preset_dialogues: list[str] = Field(default_factory=list, max_length=12)
    enabled: bool = True


def _read() -> str:
    return _CONFIG_FILE.read_text(encoding="utf-8") if _CONFIG_FILE.exists() else ""


def _revision(content: str) -> str:
    return hashlib.sha256(content.encode("utf-8")).hexdigest()


def _persona_revision(payload: dict[str, Any]) -> str:
    content = json.dumps(
        payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    )
    return _revision(content)


def _persona_state() -> tuple[Any, Any] | None:
    try:
        module = import_module("zhenxun.plugins.chatinter.persona")
        personas = module.list_personas()
    except (ImportError, RuntimeError):
        return None
    persona = next(
        (item for item in personas if getattr(item, "persona_id", "") == "default"),
        personas[0] if personas else None,
    )
    return (module, persona) if persona is not None else None


def _persona_view(persona: Any) -> dict[str, Any]:
    return {
        "persona_id": str(persona.persona_id),
        "name": str(persona.name),
        "prompt": str(persona.prompt),
        "style": str(persona.style),
        "tone_examples": list(persona.tone_examples),
        "preset_dialogues": list(persona.preset_dialogues),
        "enabled": bool(persona.enabled),
    }


def _load(content: str) -> dict[str, Any]:
    data = _yaml_parser().load(StringIO(content)) or {}
    if not isinstance(data, dict):
        raise _configuration_error(
            "yaml_top_level_mapping_required", "config.yaml 顶层必须是映射。"
        )
    ai = data.setdefault("AI", {})
    if not isinstance(ai, dict):
        raise _configuration_error("ai_mapping_required", "AI 配置组必须是映射。", "AI")
    return data


def _dump(data: dict[str, Any]) -> str:
    stream = StringIO()
    _yaml_parser().dump(data, stream)
    return stream.getvalue()


def _configuration_error(
    code: str, message: str, path: str | None = None
) -> HTTPException:
    return HTTPException(
        status_code=422,
        detail={
            "message": "AI 配置校验失败。",
            "issues": [
                {
                    "code": code,
                    "file": "config.yaml",
                    "path": path,
                    "line": None,
                    "column": None,
                    "severity": "error",
                    "message": message,
                }
            ],
        },
    )


def _existing_key(ai: dict[str, Any], key: str) -> str | None:
    expected = key.casefold()
    return next((str(item) for item in ai if str(item).casefold() == expected), None)


def _ai_get(ai: dict[str, Any], key: str, default: Any = None) -> Any:
    existing = _existing_key(ai, key)
    return ai[existing] if existing is not None else default


def _ai_set(ai: dict[str, Any], key: str, value: Any) -> None:
    ai[_existing_key(ai, key) or key] = value


def _llm_payload(ai: dict[str, Any]) -> dict[str, Any]:
    current = get_llm_config()
    return {
        "default_models": _ai_get(
            ai, "default_models", model_dump(current.default_models)
        ),
        "client_settings": _ai_get(
            ai, "client_settings", model_dump(current.client_settings)
        ),
        "debug_log": _ai_get(ai, "debug_log", model_dump(current.debug_log)),
        "providers": _ai_get(
            ai, "PROVIDERS", [model_dump(item) for item in current.providers]
        ),
        "context_settings": _ai_get(
            ai, "context_settings", model_dump(current.context_settings)
        ),
        "model_groups": _ai_get(ai, "MODEL_GROUPS", current.model_groups),
        "agent_settings": _ai_get(
            ai, "agent_settings", model_dump(current.agent_settings)
        ),
        "sandbox": _ai_get(ai, "sandbox", model_dump(current.sandbox)),
        "provider_settings": _ai_get(
            ai, "provider_settings", model_dump(current.provider_settings)
        ),
    }


def _reference_issues(config: LLMConfig) -> list[dict[str, str]]:
    issues: list[dict[str, str]] = []
    available_models = {
        f"{provider.name}/{model.model_name}"
        for provider in config.providers
        for model in provider.models
    }
    group_names = set(config.model_groups)
    model_details = {
        f"{provider.name}/{model.model_name}": model
        for provider in config.providers
        for model in provider.models
    }

    def add(code: str, path: str, message: str) -> None:
        issue = {"code": code, "path": path, "message": message}
        if issue not in issues:
            issues.append(issue)

    def visit(group: str, stack: tuple[str, ...]) -> None:
        if group in stack:
            add(
                "model_group_cycle",
                "AI.MODEL_GROUPS",
                f"模型路由组 {' -> '.join((*stack, group))} 存在循环引用。",
            )
            return
        targets = config.model_groups.get(group, [])
        for index, target in enumerate(targets):
            if target in group_names:
                visit(target, (*stack, group))
            elif target not in available_models:
                add(
                    "model_reference_missing",
                    f"AI.MODEL_GROUPS.{group}.{index}",
                    f"模型路由组 {group} 引用了不存在的模型 {target}。",
                )

    for group_name in group_names:
        targets = config.model_groups.get(group_name, [])
        if len(targets) != len(set(targets)):
            add(
                "model_group_target_duplicate",
                f"AI.MODEL_GROUPS.{group_name}",
                f"模型路由组 {group_name} 包含重复目标。",
            )
        visit(group_name, ())

    def model_supports(target: str, task: str) -> bool:
        model = model_details.get(target)
        if model is None:
            return False
        capabilities = get_model_capabilities(model.model_name)
        declared_task = model.task_type or ""
        if task == "image" and declared_task == "image_generation":
            return True
        return declared_task == task or capabilities.supports_task(task)

    def target_supports(target: str, task: str, seen: set[str]) -> bool:
        if target in model_details:
            return model_supports(target, task)
        if target not in group_names or target in seen:
            return False
        return any(
            target_supports(child, task, {*seen, target})
            for child in config.model_groups.get(target, [])
        )

    for task, target in model_dump(config.default_models).items():
        if target and target not in available_models and target not in group_names:
            add(
                "default_model_missing",
                f"AI.default_models.{task}",
                f"{task} 默认模型 {target} 不存在。",
            )
        elif target and not target_supports(target, task, set()):
            if target in model_details:
                label = f"模型 {target}"
            else:
                label = f"路由组 {target}"
            if target in available_models or target in group_names:
                add(
                    "default_model_capability_mismatch",
                    f"AI.default_models.{task}",
                    f"{label}中没有支持 {task} 任务的可用模型。",
                )
    return issues


def _raise_reference_issue(issue: dict[str, str]) -> None:
    raise _configuration_error(issue["code"], issue["message"], issue["path"])


def _validate_full(ai: dict[str, Any], *, strict_references: bool = True) -> LLMConfig:
    try:
        config = parse_as(LLMConfig, _llm_payload(ai))
    except Exception as error:
        errors = error.errors() if callable(getattr(error, "errors", None)) else []
        path = (
            ".".join(str(item) for item in errors[0].get("loc", [])) if errors else "AI"
        )
        raise _configuration_error(
            "ai_field_invalid", "字段类型或取值无效。", path
        ) from error

    provider_names: set[str] = set()
    for provider in config.providers:
        normalized = provider.name.strip().casefold()
        if not normalized or "/" in provider.name:
            raise _configuration_error(
                "provider_name_invalid",
                "服务商名称不能为空或包含斜杠。",
                "AI.PROVIDERS",
            )
        if normalized in provider_names:
            raise _configuration_error(
                "provider_name_duplicate",
                f"服务商名称 {provider.name} 重复。",
                "AI.PROVIDERS",
            )
        provider_names.add(normalized)
        if provider.api_base:
            parsed = urlparse(provider.api_base)
            if parsed.scheme not in {"http", "https"} or not parsed.netloc:
                raise _configuration_error(
                    "provider_url_invalid",
                    f"服务商 {provider.name} 的 API 地址无效。",
                    "AI.PROVIDERS",
                )
        model_names: set[str] = set()
        for model in provider.models:
            if not model.model_name.strip():
                raise _configuration_error(
                    "model_name_empty",
                    f"服务商 {provider.name} 存在空的模型名称。",
                    "AI.PROVIDERS",
                )
            if model.model_name in model_names:
                raise _configuration_error(
                    "model_name_duplicate",
                    f"服务商 {provider.name} 存在重复模型 {model.model_name}。",
                    "AI.PROVIDERS",
                )
            model_names.add(model.model_name)
    issues = _reference_issues(config)
    if strict_references and issues:
        _raise_reference_issue(issues[0])
    return config


def _provider_index(providers: Any, name: str) -> int | None:
    """按名称查找服务商下标，名称不区分大小写

    参数:
        providers: 原始服务商列表
        name: 服务商名称

    返回:
        int | None: 下标，未找到时为 None
    """
    return next(
        (
            index
            for index, item in enumerate(providers or [])
            if isinstance(item, dict)
            and str(item.get("name", "")).casefold() == name.strip().casefold()
        ),
        None,
    )


def _find_provider(ai: dict[str, Any], name: str) -> tuple[int, dict[str, Any]]:
    providers = _ai_get(ai, "PROVIDERS", [])
    index = _provider_index(providers, name)
    if index is None:
        raise HTTPException(
            status_code=404,
            detail={"code": "provider_not_found", "message": "未找到该 AI 服务商。"},
        )
    return index, providers[index]


_PLACEHOLDER_SECRETS = {
    "change_me",
    "changeme",
    "replace_me",
    "your_api_key",
    "your_key_here",
}


def _is_placeholder_secret(value: Any) -> bool:
    normalized = str(value or "").strip()
    if not normalized:
        return True
    folded = normalized.casefold()
    return bool(
        folded in _PLACEHOLDER_SECRETS
        or folded.startswith("your_")
        or re.fullmatch(r"\$\{[^{}]+\}", normalized)
        or re.fullmatch(r"<[^<>]+>", normalized)
    )


def _keys(value: Any) -> list[str]:
    """整理 API Key 列表，去掉空值与占位符

    参数:
        value: 单个密钥或密钥列表

    返回:
        list[str]: 有效密钥
    """
    candidates = [value] if isinstance(value, str) else value or []
    return [
        str(item).strip() for item in candidates if not _is_placeholder_secret(item)
    ]


def _rename_provider_references(ai: dict[str, Any], old: str, new: str) -> None:
    if old == new:
        return
    old_prefix = f"{old}/"
    new_prefix = f"{new}/"

    defaults = _ai_get(ai, "default_models", {})
    if isinstance(defaults, dict):
        for task, target in defaults.items():
            if isinstance(target, str) and target.startswith(old_prefix):
                defaults[task] = new_prefix + target[len(old_prefix) :]

    groups = _ai_get(ai, "MODEL_GROUPS", {})
    if isinstance(groups, dict):
        for group, targets in groups.items():
            if isinstance(targets, list):
                groups[group] = [
                    new_prefix + target[len(old_prefix) :]
                    if isinstance(target, str) and target.startswith(old_prefix)
                    else target
                    for target in targets
                ]


def _merge_models(
    provider: dict[str, Any], models: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    """用提交的模型列表覆盖服务商模型，保留同名模型中未提交的字段

    参数:
        provider: 原始服务商配置
        models: 提交的模型列表

    返回:
        list[dict[str, Any]]: 合并后的模型列表
    """
    existing = {
        str(item.get("model_name")): item
        for item in provider.get("models") or []
        if isinstance(item, dict) and item.get("model_name")
    }
    return [
        {**deepcopy(existing.get(str(model.get("model_name", "")), {})), **model}
        for model in models
    ]


async def _persist(
    expected_revision: str,
    mutate: Any,
    *,
    reload_consumers: bool = True,
) -> tuple[str, LLMConfig, RuntimeOperation | None]:
    """校验修订号后修改 AI 配置并热加载，失败时恢复原文件

    参数:
        expected_revision: 前端持有的配置修订号
        mutate: 修改 AI 配置组的函数
        reload_consumers: 是否重载配置消费者

    返回:
        tuple[str, LLMConfig, RuntimeOperation | None]: 新修订号、配置与重载结果
    """
    async with _PERSIST_LOCK:
        current = _read()
        if _revision(current) != expected_revision:
            raise HTTPException(
                status_code=409,
                detail={
                    "code": "configuration_revision_conflict",
                    "message": "配置文件已被外部修改，请重新加载。",
                },
            )
        data = _load(current)
        baseline = {
            (item["code"], item["path"], item["message"])
            for item in _reference_issues(
                _validate_full(data["AI"], strict_references=False)
            )
        }
        candidate = deepcopy(data)
        mutate(candidate["AI"])
        config = _validate_full(candidate["AI"], strict_references=False)
        new_issues = [
            item
            for item in _reference_issues(config)
            if (item["code"], item["path"], item["message"]) not in baseline
        ]
        if new_issues:
            _raise_reference_issue(new_issues[0])
        content = _dump(candidate)
        if content == current:
            return _revision(content), config, None
        original = _CONFIG_FILE.read_bytes() if _CONFIG_FILE.exists() else None
        try:
            _write_transaction([(_CONFIG_FILE, content.encode("utf-8"))])
            operation = await reload_runtime_config(
                submit_restart=False, reload_consumers=reload_consumers
            )
        except Exception as error:
            if original is None:
                _CONFIG_FILE.unlink(missing_ok=True)
            else:
                _write_transaction([(_CONFIG_FILE, original)])
            try:
                await reload_runtime_config(submit_restart=False)
            except Exception:
                pass
            raise HTTPException(
                status_code=500,
                detail={
                    "code": "ai_reload_failed",
                    "message": "AI 配置保存或热加载失败，已恢复原配置。",
                },
            ) from error
        return _revision(content), config, operation


def _restore_sandbox_startup_runtime() -> None:
    group = Config.get("AI")
    config_model = group.configs.get("SANDBOX")
    current = deepcopy(config_model.value) if config_model else {}
    if not isinstance(current, dict):
        current = {}
    for key in _SANDBOX_STARTUP_KEYS:
        current[key] = deepcopy(_SANDBOX_STARTUP_VALUES.get(key))
    if config_model:
        config_model.value = current
    ai_data = Config._simple_data.get("AI")
    if isinstance(ai_data, dict):
        raw_key = next(
            (key for key in ai_data if str(key).upper() == "SANDBOX"),
            "sandbox",
        )
        ai_data[raw_key] = deepcopy(current)
    get_llm_config.cache_clear()


def _apply_response(
    config: LLMConfig,
    revision: str,
    operation: RuntimeOperation | None,
    *,
    apply_mode: str | None = None,
    changed_keys: list[str] | None = None,
    reason_codes: list[str] | None = None,
    pending_source: str = "webui.ai",
) -> dict[str, Any]:
    """生成保存结果，需要重启时登记待重启原因

    参数:
        config: 保存后的 AI 配置
        revision: 新修订号
        operation: 配置重载结果
        apply_mode: 指定的生效方式，为空时取重载结果
        changed_keys: 指定的变更键，为空时取重载结果
        reason_codes: 指定的重启原因，为空时取重载结果
        pending_source: 待重启原因的来源

    返回:
        dict[str, Any]: 生效结果与最新配置视图
    """
    mode = apply_mode or (
        operation.mode.value if operation is not None else APPLY_NO_CHANGE
    )
    required = mode == APPLY_RESTART_PENDING
    if reason_codes is None:
        reason_codes = [operation.reason] if operation and operation.reason else []
    if changed_keys is None:
        changed_keys = operation.config_keys if operation is not None else []
    if required and update_pending_restart(
        pending_source, reason_codes or ["ai_runtime_restart_required"]
    ):
        issue_restart_ticket("webui.settings", ttl_seconds=10 * 60)
    status = restart_status_data()
    return apply_result_data(
        apply_mode=mode,
        changed_keys=changed_keys,
        restart_required=required,
        reason_codes=reason_codes,
        access_urls=status["access_urls"],
        access_targets=status["access_targets"],
        **_configuration_view(config, revision),
    )


def _provider_view(
    provider: ProviderConfig, raw_provider: dict[str, Any] | None = None
) -> dict[str, Any]:
    """生成服务商视图，包含密钥、模型能力与自动发现状态

    参数:
        provider: 服务商配置
        raw_provider: 配置文件中的原始服务商数据，用于保留未声明的模型字段

    返回:
        dict[str, Any]: 服务商视图
    """
    keys = _keys(provider.api_key)
    raw_models = {
        str(item.get("model_name")): item
        for item in (raw_provider or {}).get("models") or []
        if isinstance(item, dict) and item.get("model_name")
    }
    if provider.api_type not in _DISCOVERY_TYPES:
        discovery_status = "manual_only"
    elif not (provider.api_base or get_default_api_base_for_type(provider.api_type)):
        discovery_status = "missing_base"
    elif not keys:
        discovery_status = "missing_credentials"
    else:
        discovery_status = "ready"
    return {
        **model_dump(provider),
        "api_key": keys,
        "models": [
            {
                **jsonable_encoder(raw_models.get(model.model_name, {})),
                **model_dump(model),
                "capabilities": jsonable_encoder(model_dump(capabilities)),
                "capability_source": (
                    "default"
                    if capabilities is DEFAULT_PERMISSIVE_CAPABILITIES
                    else "registry"
                ),
            }
            for model in provider.models
            for capabilities in (get_model_capabilities(model.model_name),)
        ],
        "discovery_status": discovery_status,
    }


def _normalize_model_groups(value: Any) -> dict[str, list[str]]:
    if isinstance(value, dict):
        rows = [{"name": name, "targets": targets} for name, targets in value.items()]
    elif isinstance(value, list):
        rows = value
    else:
        raise _configuration_error(
            "model_groups_invalid",
            "模型路由组必须是列表或映射。",
            "AI.MODEL_GROUPS",
        )
    groups: dict[str, list[str]] = {}
    for index, row in enumerate(rows):
        if not isinstance(row, dict):
            raise _configuration_error(
                "model_group_invalid",
                "模型路由组格式无效。",
                f"AI.MODEL_GROUPS.{index}",
            )
        name = str(row.get("name") or "").strip()
        if not name:
            raise _configuration_error(
                "model_group_name_required",
                "请填写路由组名称。",
                f"AI.MODEL_GROUPS.{index}.name",
            )
        if name in groups:
            raise _configuration_error(
                "model_group_name_duplicate",
                f"路由组名称 {name} 重复。",
                f"AI.MODEL_GROUPS.{index}.name",
            )
        targets = row.get("targets", [])
        if not isinstance(targets, list) or not all(
            isinstance(target, str) and target.strip() for target in targets
        ):
            raise _configuration_error(
                "model_group_targets_invalid",
                "模型路由目标必须是非空字符串列表。",
                f"AI.MODEL_GROUPS.{index}.targets",
            )
        normalized_targets = [target.strip() for target in targets]
        if len(normalized_targets) != len(set(normalized_targets)):
            raise _configuration_error(
                "model_group_target_duplicate",
                f"路由组 {name} 不能重复引用同一目标。",
                f"AI.MODEL_GROUPS.{index}.targets",
            )
        groups[name] = normalized_targets
    return groups


@cache
def _schema() -> dict[str, Any]:
    return model_json_schema(LLMConfig)


def _configuration_view(config: LLMConfig, revision: str) -> dict[str, Any]:
    """生成 AI 配置页面数据

    参数:
        config: AI 配置
        revision: 配置修订号

    返回:
        dict[str, Any]: 服务商、各分区配置与引用问题
    """
    try:
        raw_ai = _load(_read())["AI"]
    except Exception:
        raw_ai = {}
    raw_providers = _ai_get(raw_ai, "PROVIDERS", [])

    def raw_provider(name: str) -> dict[str, Any] | None:
        index = _provider_index(raw_providers, name)
        return None if index is None else raw_providers[index]

    def section(key: str, normalized: Any) -> Any:
        value = _ai_get(raw_ai, key)
        return jsonable_encoder(value) if value is not None else normalized

    api_types = sorted(LLMAdapterFactory.list_supported_types())
    return {
        "revision": revision,
        "schema": _schema(),
        "api_types": api_types,
        "default_api_bases": {
            api_type: default_base
            for api_type in api_types
            if (default_base := get_default_api_base_for_type(api_type))
        },
        "discovery_api_types": sorted(_DISCOVERY_TYPES),
        "providers": [
            _provider_view(provider, raw_provider(provider.name))
            for provider in config.providers
        ],
        "sections": {
            "default_models": section(
                "default_models", model_dump(config.default_models)
            ),
            "model_groups": section("MODEL_GROUPS", config.model_groups),
            "context": section("context_settings", model_dump(config.context_settings)),
            "agent": section("agent_settings", model_dump(config.agent_settings)),
            "sandbox": section("sandbox", model_dump(config.sandbox)),
            "advanced": {
                "client_settings": section(
                    "client_settings", model_dump(config.client_settings)
                ),
                "debug_log": section("debug_log", model_dump(config.debug_log)),
                "provider_settings": section(
                    "provider_settings", model_dump(config.provider_settings)
                ),
            },
        },
        "validation_issues": _reference_issues(config),
    }


@router.get(
    "/configuration",
    dependencies=[authentication()],
    response_model=Result,
    response_class=JSONResponse,
)
async def get_configuration() -> Result:
    content = _read()
    data = _load(content)
    config = _validate_full(data["AI"], strict_references=False)
    return Result.ok(_configuration_view(config, _revision(content)))


def _provider_fields(payload: ProviderSettingsUpdate, keys: Any) -> dict[str, Any]:
    """生成要写入的服务商字段，未填写密钥时保存为空列表

    参数:
        payload: 提交的服务商设置
        keys: 要保存的 API Key

    返回:
        dict[str, Any]: 服务商字段
    """
    keys = _keys(keys)
    return {
        "name": payload.name.strip(),
        "api_key": keys,
        "api_base": payload.api_base or None,
        "api_type": payload.api_type,
        "temperature": payload.temperature,
        "max_output_tokens": payload.max_output_tokens,
        "timeout": payload.timeout,
    }


def _check_provider_name(
    ai: dict[str, Any], name: str, own_index: int | None = None
) -> None:
    if _provider_index(_ai_get(ai, "PROVIDERS", []), name) not in (None, own_index):
        raise _configuration_error(
            "provider_name_duplicate", "服务商名称已存在。", "AI.PROVIDERS"
        )


@router.post(
    "/providers",
    dependencies=[authentication()],
    response_model=Result,
    response_class=JSONResponse,
)
async def create_provider(payload: ProviderSettingsUpdate) -> Result:
    def mutate(ai: dict[str, Any]) -> None:
        _check_provider_name(ai, payload.name)
        providers = _ai_get(ai, "PROVIDERS")
        if providers is None:
            providers = []
            _ai_set(ai, "PROVIDERS", providers)
        providers.append(
            {
                **_provider_fields(payload, payload.api_keys),
                "models": deepcopy(payload.models or []),
            }
        )

    revision, config, operation = await _persist(payload.expected_revision, mutate)
    return Result.ok(
        _apply_response(config, revision, operation),
        info="AI 服务商已保存并热加载。",
    )


@router.put(
    "/providers/{provider_name}",
    dependencies=[authentication()],
    response_model=Result,
    response_class=JSONResponse,
)
async def update_provider(
    provider_name: str, payload: ProviderSettingsUpdate
) -> Result:
    def mutate(ai: dict[str, Any]) -> None:
        index, provider = _find_provider(ai, provider_name)
        _check_provider_name(ai, payload.name, index)
        fields = _provider_fields(
            payload,
            provider.get("api_key") if payload.api_keys is None else payload.api_keys,
        )
        _rename_provider_references(ai, str(provider.get("name", "")), fields["name"])
        provider.update(fields)
        if payload.models is not None:
            provider["models"] = _merge_models(provider, payload.models)

    revision, config, operation = await _persist(payload.expected_revision, mutate)
    return Result.ok(
        _apply_response(config, revision, operation),
        info="AI 服务商已保存并热加载。",
    )


@router.delete(
    "/providers/{provider_name}",
    dependencies=[authentication()],
    response_model=Result,
    response_class=JSONResponse,
)
async def delete_provider(provider_name: str, expected_revision: str) -> Result:
    def mutate(ai: dict[str, Any]) -> None:
        index, _ = _find_provider(ai, provider_name)
        _ai_get(ai, "PROVIDERS", []).pop(index)

    revision, config, operation = await _persist(expected_revision, mutate)
    return Result.ok(
        _apply_response(config, revision, operation),
        info="AI 服务商已删除并热加载。",
    )


@router.put(
    "/providers/{provider_name}/models",
    dependencies=[authentication()],
    response_model=Result,
    response_class=JSONResponse,
)
async def update_models(provider_name: str, payload: ProviderModelsUpdate) -> Result:
    def mutate(ai: dict[str, Any]) -> None:
        _, provider = _find_provider(ai, provider_name)
        provider["models"] = _merge_models(provider, payload.models)

    revision, config, operation = await _persist(payload.expected_revision, mutate)
    return Result.ok(
        _apply_response(config, revision, operation),
        info="模型列表已保存并热加载。",
    )


def _sandbox_apply_mode(before: dict[str, Any], after: dict[str, Any]) -> tuple:
    """判断沙箱设置的生效方式，启动期字段的运行态保持启动时的值

    参数:
        before: 保存前的沙箱设置
        after: 保存后的沙箱设置

    返回:
        tuple: 生效方式、变更键与重启原因
    """
    changed = sorted(
        key for key in before.keys() | after.keys() if before.get(key) != after.get(key)
    )
    deferred = sorted(
        key
        for key in _SANDBOX_STARTUP_KEYS
        if after.get(key) != _SANDBOX_STARTUP_VALUES.get(key)
    )
    if deferred:
        _restore_sandbox_startup_runtime()
    if not changed:
        mode = APPLY_NO_CHANGE
    elif deferred and _SANDBOX_STARTUP_KEYS.intersection(changed):
        mode = APPLY_RESTART_PENDING
    elif set(changed) - _SANDBOX_STARTUP_KEYS:
        mode = APPLY_NEW_SESSION
    else:
        mode = APPLY_CONFIG_RELOADED
    return (
        mode,
        [f"AI.SANDBOX.{key}" for key in changed],
        [f"ai.sandbox.{key}" for key in deferred],
    )


@router.put(
    "/configuration/sections/{section}",
    dependencies=[authentication()],
    response_model=Result,
    response_class=JSONResponse,
)
async def update_section(section: str, payload: SectionUpdate) -> Result:
    if section not in _SECTIONS:
        raise HTTPException(
            status_code=404,
            detail={
                "code": "ai_section_not_found",
                "message": "不支持的 AI 配置分区。",
            },
        )

    def update_value(ai: dict[str, Any], key: str, value: Any) -> None:
        existing_key = _existing_key(ai, key)
        existing = ai.get(existing_key) if existing_key is not None else None
        if isinstance(existing, dict) and isinstance(value, dict):
            for item_key, item_value in value.items():
                current = existing.get(item_key)
                if isinstance(current, dict) and isinstance(item_value, dict):
                    current.update(item_value)
                else:
                    existing[item_key] = item_value
        else:
            _ai_set(ai, key, value)

    def mutate(ai: dict[str, Any]) -> None:
        key = _SECTIONS[section]
        if section == "advanced":
            if (
                not isinstance(payload.value, dict)
                or set(payload.value) - _ADVANCED_KEYS
            ):
                raise _configuration_error(
                    "advanced_section_invalid", "高级设置包含不支持的字段。", "AI"
                )
            for advanced_key in _ADVANCED_KEYS & set(payload.value):
                update_value(ai, advanced_key, payload.value[advanced_key])
        elif section == "default_models":
            _ai_set(ai, "default_models", _normalize_defaults(payload.value))
        elif key:
            update_value(ai, key, payload.value)

    if section != "sandbox":
        revision, config, operation = await _persist(payload.expected_revision, mutate)
        return Result.ok(
            _apply_response(config, revision, operation), info="AI 配置已保存。"
        )
    before = _validate_full(_load(_read())["AI"], strict_references=False)
    revision, config, operation = await _persist(
        payload.expected_revision, mutate, reload_consumers=False
    )
    mode, changed_keys, reason_codes = _sandbox_apply_mode(
        model_dump(before.sandbox), model_dump(config.sandbox)
    )
    update_pending_restart("webui.ai-sandbox", reason_codes)
    return Result.ok(
        _apply_response(
            config,
            revision,
            operation,
            apply_mode=mode,
            changed_keys=changed_keys,
            reason_codes=reason_codes,
            pending_source="webui.ai-sandbox",
        ),
        info=(
            "AI 配置已保存，需要重启后生效。"
            if mode == APPLY_RESTART_PENDING
            else "AI 配置已保存。"
        ),
    )


@router.post(
    "/configuration/validate-routing",
    dependencies=[authentication()],
    response_model=Result,
    response_class=JSONResponse,
)
async def validate_routing(payload: RoutingValidationRequest) -> Result:
    content = _read()
    if payload.expected_revision and payload.expected_revision != _revision(content):
        raise HTTPException(409, detail={"code": "configuration_revision_conflict"})
    data = _load(content)
    candidate = deepcopy(data)
    _ai_set(candidate["AI"], "MODEL_GROUPS", _normalize_model_groups(payload.value))
    if payload.default_models is not None:
        _ai_set(
            candidate["AI"],
            "default_models",
            _normalize_defaults(payload.default_models),
        )
    config = _validate_full(candidate["AI"], strict_references=False)
    baseline = _reference_issues(_validate_full(data["AI"], strict_references=False))
    blockers = [issue for issue in _reference_issues(config) if issue not in baseline]
    return Result.ok({"valid": not blockers, "issues": blockers})


def _normalize_defaults(value: Any) -> dict[str, str | None]:
    tasks = {"chat", "embedding", "tts", "image", "rerank"}
    if (
        not isinstance(value, dict)
        or set(value) - tasks
        or any(
            item is not None and not isinstance(item, str) for item in value.values()
        )
    ):
        raise _configuration_error(
            "default_models_invalid",
            "默认模型必须是任务与模型名称的映射。",
            "AI.default_models",
        )
    return {task: (value.get(task) or "").strip() or None for task in sorted(tasks)}


@router.put(
    "/configuration/routing", dependencies=[authentication()], response_model=Result
)
async def update_routing(payload: RoutingUpdate) -> Result:
    def mutate(ai):
        _ai_set(ai, "MODEL_GROUPS", _normalize_model_groups(payload.model_groups))
        _ai_set(ai, "default_models", _normalize_defaults(payload.default_models))

    revision, config, operation = await _persist(payload.expected_revision, mutate)
    return Result.ok(
        _apply_response(config, revision, operation), info="模型路由与默认模型已保存。"
    )


def _repair_references(
    ai: dict[str, Any],
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    config = _validate_full(ai, strict_references=False)
    groups = deepcopy(config.model_groups)
    defaults = _normalize_defaults(model_dump(config.default_models))
    models = {f"{p.name}/{m.model_name}" for p in config.providers for m in p.models}
    changes = []
    while True:
        changed = False
        for group, targets in list(groups.items()):
            kept = [
                target for target in targets if target in models or target in groups
            ]
            if kept and kept == targets:
                continue
            if kept:
                groups[group] = kept
            else:
                groups.pop(group)
            changes.append(
                {
                    "path": f"AI.MODEL_GROUPS.{group}",
                    "before": targets,
                    "after": kept or None,
                }
            )
            changed = True
        if not changed:
            break
    result = deepcopy(ai)
    _ai_set(result, "MODEL_GROUPS", groups)
    _ai_set(result, "default_models", defaults)
    for task, target in defaults.items():
        if target and target not in models and target not in groups:
            changes.append(
                {"path": f"AI.default_models.{task}", "before": target, "after": None}
            )
            defaults[task] = None
    for issue in _reference_issues(_validate_full(result, strict_references=False)):
        if issue["code"] in {
            "default_model_missing",
            "default_model_capability_mismatch",
        }:
            task = issue["path"].rsplit(".", 1)[-1]
            changes.append(
                {"path": issue["path"], "before": defaults[task], "after": None}
            )
            defaults[task] = None
    return result, changes


@router.post(
    "/configuration/repair-references/preview",
    dependencies=[authentication()],
    response_model=Result,
)
async def preview_reference_repair(payload: ReferenceRepairRequest) -> Result:
    content = _read()
    if _revision(content) != payload.expected_revision:
        raise HTTPException(409, detail={"code": "configuration_revision_conflict"})
    candidate, changes = _repair_references(_load(content)["AI"])
    config = _validate_full(candidate, strict_references=False)
    return Result.ok(
        {
            "revision": payload.expected_revision,
            "changes": changes,
            "remaining_issues": _reference_issues(config),
            "model_groups": config.model_groups,
            "default_models": model_dump(config.default_models),
        }
    )


@router.post(
    "/configuration/repair-references/apply",
    dependencies=[authentication()],
    response_model=Result,
)
async def apply_reference_repair(payload: ReferenceRepairRequest) -> Result:
    def mutate(ai):
        candidate, _ = _repair_references(ai)
        _ai_set(ai, "MODEL_GROUPS", _ai_get(candidate, "MODEL_GROUPS"))
        _ai_set(ai, "default_models", _ai_get(candidate, "default_models"))

    revision, config, operation = await _persist(payload.expected_revision, mutate)
    return Result.ok(
        _apply_response(config, revision, operation),
        info="失效引用已修复，服务商和密钥保持不变。",
    )


@router.get(
    "/chat-plugins/switches", dependencies=[authentication()], response_model=Result
)
async def chat_plugin_switches() -> Result:
    from zhenxun.services.plugin_policy import plugin_policy_service

    return Result.ok(await plugin_policy_service.global_state())


@router.put(
    "/chat-plugins/switches", dependencies=[authentication()], response_model=Result
)
async def update_chat_plugin_switch(payload: ChatPluginSwitchRequest) -> Result:
    from zhenxun.models.plugin_info import PluginInfo

    from ..plugin_manage.store import _AI_CHAT_PLUGIN_MODULES, _plugin_capabilities

    plugin = await PluginInfo.get_plugin(module=payload.module)
    supported = plugin is not None and plugin.module in _AI_CHAT_PLUGIN_MODULES
    if plugin is not None and not supported:
        from zhenxun.builtin_plugins.plugin_store.data_source import StoreManager

        catalogs = await StoreManager.get_data()
        supported = any(
            item.module == plugin.module and "ai_chat" in _plugin_capabilities(item)
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


def _probe_error(status_code: int, code: str, message: str) -> HTTPException:
    return HTTPException(
        status_code=status_code, detail={"code": code, "message": message}
    )


_PROBE_ERRORS: tuple[tuple[Any, int, str, str], ...] = (
    (
        LocationNotSupportedException,
        422,
        "provider_location_unsupported",
        "服务商不支持当前出口所在地区。",
    ),
    (
        AuthenticationException,
        422,
        "provider_credentials_invalid",
        "服务商拒绝了当前凭据。",
    ),
    (
        RateLimitException | QuotaExceededException,
        429,
        "provider_rate_limited",
        "服务商当前限流或额度不可用。",
    ),
    (
        asyncio.TimeoutError | httpx.TimeoutException,
        504,
        "provider_timeout",
        "连接服务商超时。",
    ),
    (httpx.NetworkError, 502, "provider_unreachable", "无法连接到服务商。"),
    (
        ResponseParseException,
        502,
        "provider_response_parse_failed",
        "服务商响应与所选 API 协议不匹配。",
    ),
    (
        ConfigurationException,
        422,
        "provider_protocol_mismatch",
        "所选 API 协议、地址或模型配置不匹配。",
    ),
)


def _map_probe_error(error: Exception) -> HTTPException:
    """把服务商探测异常转换为前端可读的错误

    参数:
        error: 探测异常

    返回:
        HTTPException: 带错误码与说明的异常
    """
    from zhenxun.services.network_proxy import ProxyPolicyError, ProxyRequestError

    if isinstance(error, ProxyPolicyError | ProxyRequestError):
        return _probe_error(
            502, error.code, "代理配置、连接或认证失败，请检查网络代理设置。"
        )
    if isinstance(error, InvalidRequestException):
        fields = [
            name
            for name in ("maxOutputTokens", "thinkingConfig", "safetySettings", "tools")
            if name in str(error) or name in str(getattr(error, "details", {}))
        ]
        return _probe_error(
            422,
            "provider_request_invalid",
            "服务商拒绝了请求参数，请检查模型参数及服务商限制。"
            + (f" 涉及字段：{', '.join(fields)}。" if fields else ""),
        )
    for types, status_code, code, message in _PROBE_ERRORS:
        if isinstance(error, types):
            return _probe_error(status_code, code, message)
    return _probe_error(
        502, "provider_request_failed", "服务商请求失败，请查看后台脱敏日志。"
    )


def _saved_provider(
    name: str | None, expected_revision: str | None
) -> ProviderConfig | None:
    """查找已保存的服务商，提供修订号时读取配置文件并校验修订号

    参数:
        name: 服务商名称
        expected_revision: 前端持有的配置修订号

    返回:
        ProviderConfig | None: 已保存的服务商
    """
    if expected_revision is None:
        config = get_llm_config()
    else:
        content = _read()
        if _revision(content) != expected_revision:
            raise HTTPException(
                status_code=409,
                detail={
                    "code": "configuration_revision_conflict",
                    "message": "AI 配置已变化，请重新加载后再测试。",
                },
            )
        config = _validate_full(_load(content)["AI"], strict_references=False)
    if not name:
        return None
    return next(
        (item for item in config.providers if item.name.casefold() == name.casefold()),
        None,
    )


def _normalized_api_base(api_type: str, api_base: str | None) -> str:
    return str(api_base or get_default_api_base_for_type(api_type) or "").rstrip("/")


async def _probe_target(
    *,
    provider_name: str | None,
    saved_provider_name: str | None,
    api_type: str | None,
    api_base: str | None,
    api_key: str | None,
    expected_revision: str | None,
    timeout: int | None = None,
) -> tuple[ProviderConfig, list[str], str | None]:
    """确定探测使用的服务商与候选密钥

    已保存的密钥只会发往该服务商保存的 API 类型和地址；使用已保存密钥时
    按健康状态排序全部密钥，否则只使用临时密钥。

    参数:
        provider_name: 草稿中的服务商名称
        saved_provider_name: 已保存的服务商名称
        api_type: 草稿中的 API 类型
        api_base: 草稿中的 API 地址
        api_key: 临时密钥
        expected_revision: 前端持有的配置修订号
        timeout: 请求超时

    返回:
        tuple[ProviderConfig, list[str], str | None]: 服务商、候选密钥，
            以及使用已保存密钥时用于记录健康状态的服务商名称
    """
    saved = _saved_provider(saved_provider_name or provider_name, expected_revision)
    resolved_type = str(api_type or (saved.api_type if saved else "")).strip()
    resolved_base = _normalized_api_base(
        resolved_type,
        api_base if api_base is not None else (saved.api_base if saved else None),
    )
    temporary_key = str(api_key or "").strip()
    scope_matches = bool(
        saved is not None
        and resolved_type == saved.api_type
        and resolved_base == _normalized_api_base(saved.api_type, saved.api_base)
    )
    if saved is not None and not temporary_key and not scope_matches:
        raise _probe_error(
            422,
            "provider_credentials_scope_mismatch",
            "修改 API 类型或地址后，请填写临时 API Key 再测试。",
        )
    saved_keys = _keys(saved.api_key) if saved is not None and scope_matches else []
    selected_key = temporary_key or next(iter(saved_keys), "")
    if not resolved_type or not resolved_base or not selected_key:
        raise _probe_error(
            422,
            "provider_credentials_incomplete",
            "请填写 API 类型、API 地址和有效 API Key。",
        )
    provider = ProviderConfig(
        name=str(provider_name or (saved.name if saved else "draft-provider")),
        api_key=selected_key,
        api_base=resolved_base,
        api_type=resolved_type,
        timeout=int(timeout or (saved.timeout if saved else _DISCOVERY_TIMEOUT)),
        temperature=saved.temperature if saved else None,
        max_output_tokens=saved.max_output_tokens if saved else None,
        models=[],
    )
    if saved is None or selected_key not in saved_keys:
        return provider, [selected_key], None
    preferred = await health_manager.get_next_available_key(
        saved.name, saved_keys, strict_mode=False
    )
    keys = [selected_key]
    if preferred:
        keys = [preferred, *(key for key in saved_keys if key != preferred)]
    return provider, keys, saved.name


async def _try_keys(
    keys: list[str], health_name: str | None, call: Any
) -> tuple[str, Any]:
    """依次用候选密钥发起请求，凭据被拒绝时换下一个

    参数:
        keys: 候选密钥
        health_name: 记录密钥健康状态的服务商名称，为空时不记录
        call: 接收密钥并发起请求的协程函数

    返回:
        tuple[str, Any]: 成功的密钥与请求结果
    """
    rejected: AuthenticationException | None = None
    for key in keys:
        try:
            return key, await call(key)
        except LLMException as error:
            if health_name:
                await health_manager.record_key_failure(health_name, key, error)
            if not isinstance(error, AuthenticationException):
                raise
            rejected = error
    assert rejected is not None
    raise rejected


def _minimal_model_request(task: str, *, api_type: str | None = None):
    if task == "embedding":
        return EmbeddingRequest(
            batch=EmbedBatch(payloads=[EmbedPayload(parts=[TextPart(text="test")])]),
            timeout=20,
        )
    if task == "rerank":
        return RerankRequest(query="a", documents=["a", "b"], top_n=1, timeout=20)
    if task == "image":
        return ImageRequest(prompt="one dot", timeout=20)
    if task == "tts":
        return SpeechRequest(input_text="test", timeout=20)
    config = GenerationConfig()
    config.common.max_tokens = 64 if api_type == "gemini" else 1
    return ChatRequest(
        messages=[LLMMessage.user("Reply OK")], config=config, timeout=20
    )


async def _run_exact_model_probe(
    provider: ProviderConfig,
    model_name: str,
    model_config: dict[str, Any] | None,
    task: str,
    api_key: str,
) -> float:
    detail_payload = dict(model_config or {})
    detail_payload["model_name"] = model_name
    model_detail = parse_as(ModelDetail, detail_payload)
    api_type = model_detail.api_type or provider.api_type
    adapter = get_adapter_for_api_type(api_type)
    capabilities = get_model_capabilities(model_name)
    identity = ModelIdentity(
        provider_name=provider.name,
        model_name=model_name,
        api_type=api_type,
        api_base=provider.api_base,
        path_prefix=model_detail.path_prefix,
        capabilities=capabilities,
        generation_config=None,
    )
    request = _minimal_model_request(task, api_type=api_type)
    request_data = await adapter.prepare_payload(identity, api_key, request)
    if api_type == "gemini" and task == "chat":
        # Connectivity probes use provider safety defaults, not the global
        # BLOCK_NONE override which some credentials/models cannot request.
        request_data.body.pop("safetySettings", None)
    client = await http_client_manager.get_client(provider)
    started = time.monotonic()
    kwargs: dict[str, Any] = {
        "headers": request_data.headers,
        "timeout": min(float(provider.timeout), 20.0),
    }
    if request_data.files:
        kwargs["data"] = request_data.body
        kwargs["files"] = request_data.files
    else:
        kwargs["content"] = json.dumps(request_data.body, ensure_ascii=False)
    response = await client.request(request_data.method, request_data.url, **kwargs)
    if response.status_code == 404:
        raise _probe_error(404, "model_not_found", "服务商未找到指定模型。")
    if exception := adapter.handle_http_error(response):
        raise exception
    await adapter.parse_payload(identity, request, response)
    return (time.monotonic() - started) * 1000


@router.post(
    "/providers/discover",
    dependencies=[authentication()],
    response_model=Result,
    response_class=JSONResponse,
)
async def discover_models(payload: ProviderDiscoveryRequest) -> Result:
    provider, keys, health_name = await _probe_target(
        provider_name=payload.provider_name,
        saved_provider_name=payload.saved_provider_name,
        api_type=payload.api_type,
        api_base=payload.api_base,
        api_key=payload.api_key,
        expected_revision=payload.expected_revision,
    )
    api_type = provider.api_type
    if api_type not in _DISCOVERY_TYPES:
        raise _probe_error(
            422,
            "model_discovery_unsupported",
            "该 API 类型不支持安全的模型自动发现，请手动添加模型。",
        )
    endpoint = "v1beta/models" if api_type == "gemini" else "v1/models"
    url = join_api_url(provider.api_base or "", endpoint)

    async def request(key: str) -> httpx.Response:
        client = await http_client_manager.get_client(provider)
        response = await client.request(
            "GET",
            url,
            headers=(
                {"x-goog-api-key": key}
                if api_type == "gemini"
                else {"Authorization": f"Bearer {key}"}
            ),
            timeout=min(float(provider.timeout), _DISCOVERY_TIMEOUT),
        )
        if response.status_code in {401, 403}:
            raise AuthenticationException("provider rejected credential")
        return response

    started = time.monotonic()
    try:
        key, response = await _try_keys(keys, health_name, request)
        if response.status_code == 429:
            raise _probe_error(
                429,
                "provider_rate_limited",
                "服务商暂时限制了模型查询，请稍后重试。",
            )
        if response.status_code >= 400:
            raise _probe_error(
                502,
                "provider_discovery_failed",
                f"服务商模型查询失败（HTTP {response.status_code}）。",
            )
        body = response.json()
        if not isinstance(body, dict):
            raise ValueError("model discovery response is not an object")
        raw_models = body.get("models" if api_type == "gemini" else "data", [])
        names = {
            str(raw_name).removeprefix("models/")
            for item in raw_models
            if (raw_name := item.get("name" if api_type == "gemini" else "id"))
        }
    except HTTPException:
        raise
    except (LLMException, httpx.HTTPError, ValueError) as error:
        logger.warning(
            "AI Provider 探测失败 | "
            f"api_type={api_type} | error={error.__class__.__name__}",
            "WebUi",
        )
        raise _map_probe_error(error) from error
    latency = round((time.monotonic() - started) * 1000)
    if health_name:
        await health_manager.record_key_success(health_name, key)
    logger.info(
        "AI Provider 探测成功 | "
        f"api_type={api_type} | models={len(names)} | latency={latency}ms",
        "WebUi",
    )
    return Result.ok({"models": sorted(names), "latency_ms": latency})


@router.post(
    "/models/test",
    dependencies=[authentication()],
    response_model=Result,
    response_class=JSONResponse,
)
async def test_model(payload: ModelTestRequest) -> Result:
    if not payload.confirmed_paid_request:
        raise HTTPException(
            status_code=409,
            detail={
                "code": "paid_test_confirmation_required",
                "message": "单模型测试可能产生费用，请确认后重试。",
            },
        )
    if "/" not in payload.model:
        raise HTTPException(
            status_code=422,
            detail={
                "code": "model_name_invalid",
                "message": "模型名称必须包含服务商前缀。",
            },
        )
    model_provider_name, model_name = payload.model.split("/", 1)
    provider, keys, health_name = await _probe_target(
        provider_name=payload.provider_name or model_provider_name,
        saved_provider_name=payload.saved_provider_name,
        api_type=payload.api_type,
        api_base=payload.api_base,
        api_key=payload.api_key,
        expected_revision=payload.expected_revision,
        timeout=payload.timeout,
    )
    route = f"{health_name}/{model_name}"
    try:
        key, latency_ms = await _try_keys(
            keys,
            health_name,
            lambda key: _run_exact_model_probe(
                provider, model_name, payload.model_settings, payload.task, key
            ),
        )
    except HTTPException:
        raise
    except Exception as error:
        logger.warning(
            f"AI 模型测试失败 | task={payload.task} | error={error.__class__.__name__}",
            "WebUi",
        )
        if health_name and isinstance(error, LLMException | httpx.HTTPError):
            await health_manager.record_route_failure(route, error)
        raise _map_probe_error(error) from error
    if health_name:
        await health_manager.record_key_success(health_name, key)
        await health_manager.record_route_success(route, latency_ms)
    return Result.ok(
        {"ok": True, "latency_ms": round(latency_ms), "task": payload.task}
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
    updated_payload = {
        **persona.to_payload(),
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
        "source": "file",
    }
    updated = module.Persona.from_payload(updated_payload)
    if updated is None:
        raise HTTPException(
            status_code=422,
            detail={
                "code": "persona_invalid",
                "message": "AI 人设内容无效。",
            },
        )
    try:
        saved = module.upsert_persona(updated)
    except OSError as error:
        logger.error("AI 人设保存失败", "WebUi", e=error)
        raise HTTPException(
            status_code=500,
            detail={
                "code": "persona_save_failed",
                "message": "AI 人设保存失败，请检查数据目录写入权限。",
            },
        ) from error
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


__all__ = ["router"]
