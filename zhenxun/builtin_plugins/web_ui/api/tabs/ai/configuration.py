from __future__ import annotations

import asyncio
from copy import deepcopy
import hashlib
from io import StringIO
from pathlib import Path
import time
from typing import Any, Literal
from urllib.parse import urlparse

from fastapi import APIRouter, HTTPException
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse
import httpx
from pydantic import BaseModel, Field
from ruamel.yaml import YAML

from zhenxun.services.ai.config import get_llm_config
from zhenxun.services.ai.config.models import LLMConfig, ProviderConfig
from zhenxun.services.ai.llm.adapters.factory import LLMAdapterFactory
from zhenxun.services.ai.llm.manager import get_default_api_base_for_type
from zhenxun.services.ai.llm.system.capabilities import get_model_capabilities
from zhenxun.services.log import logger
from zhenxun.services.runtime_config_reload import reload_runtime_config
from zhenxun.utils.pydantic_compat import (
    model_dump,
    model_json_schema,
    parse_as,
)

from ....base_model import Result
from ....utils import authentication
from ...configure.persistence import _write_transaction

router = APIRouter()

_CONFIG_FILE = Path("data/config.yaml")
_SECTIONS = {
    "default_models": "default_models",
    "model_groups": "MODEL_GROUPS",
    "context": "context_settings",
    "agent": "agent_settings",
    "sandbox": "sandbox",
    "advanced": None,
}
_ADVANCED_KEYS = {"client_settings", "debug_log", "provider_settings"}
_OPENAI_DISCOVERY_TYPES = {
    "openai",
    "openai_responses",
    "deepseek",
    "openrouter",
}
_DISCOVERY_TIMEOUT = 12.0
_PERSIST_LOCK = asyncio.Lock()


class SecretSlot(BaseModel):
    existing_index: int | None = Field(default=None, ge=0)
    value: str | None = None


class ProviderSettingsUpdate(BaseModel):
    expected_revision: str = Field(min_length=64, max_length=64)
    name: str = Field(min_length=1, max_length=80)
    api_type: str = Field(min_length=1, max_length=64)
    api_base: str | None = Field(default=None, max_length=500)
    timeout: int = Field(default=180, ge=1, le=1800)
    temperature: float | None = Field(default=None, ge=0, le=2)
    max_output_tokens: int | None = Field(default=None, ge=1)
    api_keys: list[SecretSlot] | None = None
    models: list[dict[str, Any]] | None = None


class ProviderModelsUpdate(BaseModel):
    expected_revision: str = Field(min_length=64, max_length=64)
    models: list[dict[str, Any]]


class SectionUpdate(BaseModel):
    expected_revision: str = Field(min_length=64, max_length=64)
    value: Any


class ProviderDiscoveryRequest(BaseModel):
    provider_name: str | None = None
    api_type: str | None = None
    api_base: str | None = None
    api_key: str | None = None


class ModelTestRequest(BaseModel):
    model: str
    task: Literal["chat", "embedding", "rerank", "image", "tts"] = "chat"
    confirmed_paid_request: bool = False


def _yaml() -> YAML:
    parser = YAML()
    parser.preserve_quotes = True
    parser.indent(mapping=2, sequence=4, offset=2)
    return parser


def _read() -> str:
    return _CONFIG_FILE.read_text(encoding="utf-8") if _CONFIG_FILE.exists() else ""


def _revision(content: str) -> str:
    return hashlib.sha256(content.encode("utf-8")).hexdigest()


def _load(content: str) -> dict[str, Any]:
    data = _yaml().load(StringIO(content)) or {}
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
    _yaml().dump(data, stream)
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
        for target in config.model_groups.get(group, []):
            if target in group_names:
                visit(target, (*stack, group))
            elif target not in available_models:
                add(
                    "model_reference_missing",
                    "AI.MODEL_GROUPS",
                    f"模型路由组 {group} 引用了不存在的模型 {target}。",
                )

    for group_name in group_names:
        visit(group_name, ())

    for task, target in model_dump(config.default_models).items():
        if target and target not in available_models and target not in group_names:
            add(
                "default_model_missing",
                f"AI.default_models.{task}",
                f"{task} 默认模型 {target} 不存在。",
            )
        elif target and target in model_details:
            model = model_details[target]
            capabilities = get_model_capabilities(model.model_name)
            declared_task = model.task_type or ""
            supported = capabilities.supports_task(task)
            if task == "image" and declared_task == "image_generation":
                supported = True
            elif declared_task == task:
                supported = True
            if not supported:
                add(
                    "default_model_capability_mismatch",
                    f"AI.default_models.{task}",
                    f"模型 {target} 不支持 {task} 任务。",
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


def _find_provider(ai: dict[str, Any], name: str) -> tuple[int, dict[str, Any]]:
    providers = _ai_get(ai, "PROVIDERS", [])
    for index, provider in enumerate(providers):
        if (
            isinstance(provider, dict)
            and str(provider.get("name", "")).casefold() == name.casefold()
        ):
            return index, provider
    raise HTTPException(
        status_code=404,
        detail={"code": "provider_not_found", "message": "未找到该 AI 服务商。"},
    )


def _keys(provider: dict[str, Any]) -> list[str]:
    value = provider.get("api_key", [])
    if isinstance(value, str):
        return [value] if value else []
    return [str(item) for item in value if str(item)] if isinstance(value, list) else []


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


def _apply_secret_slots(
    existing: list[str], slots: list[SecretSlot] | None
) -> list[str]:
    if slots is None:
        return existing
    result: list[str] = []
    used: set[int] = set()
    for slot in slots:
        replacement = (slot.value or "").strip()
        if slot.existing_index is None:
            if replacement:
                result.append(replacement)
            continue
        if slot.existing_index >= len(existing) or slot.existing_index in used:
            raise _configuration_error(
                "secret_slot_invalid",
                "密钥列表已变化，请重新加载后再保存。",
                "AI.PROVIDERS.api_key",
            )
        used.add(slot.existing_index)
        result.append(replacement or existing[slot.existing_index])
    return result


async def _persist_unlocked(
    expected_revision: str, mutate: Any
) -> tuple[str, LLMConfig]:
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
    baseline = _validate_full(data["AI"], strict_references=False)
    baseline_issues = {
        (item["code"], item["path"], item["message"])
        for item in _reference_issues(baseline)
    }
    candidate = deepcopy(data)
    mutate(candidate["AI"])
    config = _validate_full(candidate["AI"], strict_references=False)
    candidate_issues = _reference_issues(config)
    new_issues = [
        item
        for item in candidate_issues
        if (item["code"], item["path"], item["message"]) not in baseline_issues
    ]
    if new_issues:
        _raise_reference_issue(new_issues[0])
    content = _dump(candidate)
    original = _CONFIG_FILE.read_bytes() if _CONFIG_FILE.exists() else None
    try:
        _write_transaction([(_CONFIG_FILE, content.encode("utf-8"))])
        await reload_runtime_config()
    except Exception as error:
        if original is None:
            _CONFIG_FILE.unlink(missing_ok=True)
        else:
            _write_transaction([(_CONFIG_FILE, original)])
        try:
            await reload_runtime_config()
        except Exception:
            pass
        raise HTTPException(
            status_code=500,
            detail={
                "code": "ai_reload_failed",
                "message": "AI 配置保存或热加载失败，已恢复原配置。",
            },
        ) from error
    return _revision(content), config


async def _persist(expected_revision: str, mutate: Any) -> tuple[str, LLMConfig]:
    async with _PERSIST_LOCK:
        return await _persist_unlocked(expected_revision, mutate)


def _safe_model_data(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict):
        return {}

    def sensitive(key: Any) -> bool:
        normalized = str(key).strip().lower().replace("-", "_")
        return normalized in {
            "api_key",
            "apikey",
            "token",
            "access_token",
            "secret",
            "password",
            "credential",
            "credentials",
        } or normalized.endswith(("_api_key", "_token", "_secret", "_password"))

    def clean(item: Any) -> Any:
        if isinstance(item, dict):
            return {
                str(key): clean(child)
                for key, child in item.items()
                if not sensitive(key)
            }
        if isinstance(item, list):
            return [clean(child) for child in item]
        return jsonable_encoder(item)

    return clean(value)


def _provider_view(
    provider: ProviderConfig, raw_provider: dict[str, Any] | None = None
) -> dict[str, Any]:
    data = model_dump(provider, exclude={"api_key"})
    keys = (
        provider.api_key if isinstance(provider.api_key, list) else [provider.api_key]
    )
    data["api_key_slots"] = [
        {"existing_index": index, "configured": True}
        for index, value in enumerate(keys)
        if value
    ]
    raw_models = (
        raw_provider.get("models", []) if isinstance(raw_provider, dict) else []
    )
    raw_by_name = {
        str(item.get("model_name")): item
        for item in raw_models
        if isinstance(item, dict) and item.get("model_name")
    }
    data["models"] = []
    for model in provider.models:
        raw_model = raw_by_name.get(model.model_name, {})
        data["models"].append(
            {
                **_safe_model_data(raw_model),
                **model_dump(model),
                "capabilities": jsonable_encoder(
                    model_dump(get_model_capabilities(model.model_name))
                ),
            }
        )
    data["discovery_supported"] = (
        provider.api_type in _OPENAI_DISCOVERY_TYPES or provider.api_type == "gemini"
    )
    return data


def _configuration_view(config: LLMConfig, revision: str) -> dict[str, Any]:
    try:
        raw_ai = _load(_read())["AI"]
    except Exception:
        raw_ai = {}
    raw_providers = _ai_get(raw_ai, "PROVIDERS", [])

    def raw_provider(name: str) -> dict[str, Any] | None:
        return next(
            (
                item
                for item in raw_providers
                if isinstance(item, dict)
                and str(item.get("name", "")).casefold() == name.casefold()
            ),
            None,
        )

    def section(key: str, normalized: Any) -> Any:
        value = _ai_get(raw_ai, key)
        if isinstance(value, dict):
            return _safe_model_data(value)
        return jsonable_encoder(value) if value is not None else normalized

    api_types = sorted(LLMAdapterFactory.list_supported_types())
    return {
        "revision": revision,
        "schema": model_json_schema(LLMConfig),
        "api_types": api_types,
        "default_api_bases": {
            api_type: default_base
            for api_type in api_types
            if (default_base := get_default_api_base_for_type(api_type))
        },
        "discovery_api_types": sorted((*_OPENAI_DISCOVERY_TYPES, "gemini")),
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
        "effects": {
            "providers": "hot_reload",
            "models": "hot_reload",
            "default_models": "hot_reload",
            "model_groups": "hot_reload",
            "context": "hot_reload",
            "agent": "hot_reload",
            "sandbox": "new_session",
            "sandbox.enable_sandbox": "restart_required",
            "sandbox.sandbox_type": "restart_required",
            "advanced": "hot_reload",
        },
        "runtime": {"hot_reload_available": True},
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


@router.post(
    "/providers",
    dependencies=[authentication()],
    response_model=Result,
    response_class=JSONResponse,
)
async def create_provider(payload: ProviderSettingsUpdate) -> Result:
    def mutate(ai: dict[str, Any]) -> None:
        providers = _ai_get(ai, "PROVIDERS")
        if providers is None:
            providers = []
            _ai_set(ai, "PROVIDERS", providers)
        if any(
            str(item.get("name", "")).casefold() == payload.name.casefold()
            for item in providers
            if isinstance(item, dict)
        ):
            raise _configuration_error(
                "provider_name_duplicate", "服务商名称已存在。", "AI.PROVIDERS"
            )
        keys = _apply_secret_slots([], payload.api_keys)
        if not keys:
            raise _configuration_error(
                "provider_key_required",
                "新增服务商必须填写至少一个 API Key。",
                "AI.PROVIDERS.api_key",
            )
        providers.append(
            {
                "name": payload.name.strip(),
                "api_key": keys,
                "api_base": payload.api_base or None,
                "api_type": payload.api_type,
                "temperature": payload.temperature,
                "max_output_tokens": payload.max_output_tokens,
                "timeout": payload.timeout,
                "models": deepcopy(payload.models or []),
            }
        )

    revision, config = await _persist(payload.expected_revision, mutate)
    return Result.ok(
        _configuration_view(config, revision), info="AI 服务商已保存并热加载。"
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
        for other_index, other in enumerate(_ai_get(ai, "PROVIDERS", [])):
            if (
                other_index != index
                and str(other.get("name", "")).casefold() == payload.name.casefold()
            ):
                raise _configuration_error(
                    "provider_name_duplicate", "服务商名称已存在。", "AI.PROVIDERS"
                )
        keys = _apply_secret_slots(_keys(provider), payload.api_keys)
        if not keys:
            raise _configuration_error(
                "provider_key_required",
                "服务商必须保留至少一个 API Key。",
                "AI.PROVIDERS.api_key",
            )
        _rename_provider_references(ai, str(provider.get("name", "")), payload.name)
        provider.update(
            {
                "name": payload.name.strip(),
                "api_key": keys,
                "api_base": payload.api_base or None,
                "api_type": payload.api_type,
                "temperature": payload.temperature,
                "max_output_tokens": payload.max_output_tokens,
                "timeout": payload.timeout,
            }
        )

    revision, config = await _persist(payload.expected_revision, mutate)
    return Result.ok(
        _configuration_view(config, revision), info="AI 服务商已保存并热加载。"
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

    revision, config = await _persist(expected_revision, mutate)
    return Result.ok(
        _configuration_view(config, revision), info="AI 服务商已删除并热加载。"
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
        existing = {
            str(item.get("model_name")): item
            for item in provider.get("models", [])
            if isinstance(item, dict) and item.get("model_name")
        }
        provider["models"] = [
            {
                **deepcopy(existing.get(str(model.get("model_name", "")), {})),
                **model,
            }
            for model in payload.models
        ]

    revision, config = await _persist(payload.expected_revision, mutate)
    return Result.ok(
        _configuration_view(config, revision), info="模型列表已保存并热加载。"
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

    def mutate(ai: dict[str, Any]) -> None:
        def update_value(key: str, value: Any) -> None:
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

        key = _SECTIONS[section]
        if section == "advanced":
            if (
                not isinstance(payload.value, dict)
                or set(payload.value) - _ADVANCED_KEYS
            ):
                raise _configuration_error(
                    "advanced_section_invalid", "高级设置包含不支持的字段。", "AI"
                )
            for advanced_key in _ADVANCED_KEYS:
                if advanced_key in payload.value:
                    update_value(advanced_key, payload.value[advanced_key])
        elif section == "model_groups" and key:
            value = payload.value
            if isinstance(value, list):
                groups: dict[str, list[str]] = {}
                for index, row in enumerate(value):
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
                        isinstance(target, str) for target in targets
                    ):
                        raise _configuration_error(
                            "model_group_targets_invalid",
                            "模型路由目标必须是字符串列表。",
                            f"AI.MODEL_GROUPS.{index}.targets",
                        )
                    groups[name] = targets
                value = groups
            elif isinstance(value, dict):
                if any(not str(name).strip() for name in value):
                    raise _configuration_error(
                        "model_group_name_required",
                        "请填写路由组名称。",
                        "AI.MODEL_GROUPS",
                    )
            else:
                raise _configuration_error(
                    "model_groups_invalid",
                    "模型路由组必须是列表或映射。",
                    "AI.MODEL_GROUPS",
                )
            _ai_set(ai, key, deepcopy(value))
        elif key:
            update_value(key, payload.value)

    revision, config = await _persist(payload.expected_revision, mutate)
    effect = "new_session" if section == "sandbox" else "hot_reload"
    return Result.ok(
        {**_configuration_view(config, revision), "effect": effect},
        info="AI 配置已保存。",
    )


def _discovery_url(api_type: str, api_base: str) -> str:
    base = api_base.rstrip("/")
    if api_type == "gemini":
        return f"{base}/v1beta/models"
    return f"{base}/models" if base.endswith("/v1") else f"{base}/v1/models"


@router.post(
    "/providers/discover",
    dependencies=[authentication()],
    response_model=Result,
    response_class=JSONResponse,
)
async def discover_models(payload: ProviderDiscoveryRequest) -> Result:
    provider: ProviderConfig | None = None
    if payload.provider_name:
        provider = next(
            (
                item
                for item in get_llm_config().providers
                if item.name.casefold() == payload.provider_name.casefold()
            ),
            None,
        )
    temporary_key = (payload.api_key or "").strip()
    if provider is not None and not temporary_key:
        requested_type = payload.api_type or provider.api_type
        saved_base = (
            provider.api_base or get_default_api_base_for_type(provider.api_type) or ""
        ).rstrip("/")
        requested_base = (
            payload.api_base
            or get_default_api_base_for_type(requested_type)
            or provider.api_base
            or ""
        ).rstrip("/")
        if requested_type != provider.api_type or requested_base != saved_base:
            raise HTTPException(
                status_code=422,
                detail={
                    "code": "provider_credentials_scope_mismatch",
                    "message": "修改 API 类型或地址后，请填写临时 API Key 再测试。",
                },
            )
    api_type = payload.api_type or (provider.api_type if provider else "")
    api_base = (
        payload.api_base
        or (provider.api_base if provider else None)
        or get_default_api_base_for_type(api_type)
    )
    saved_keys = (
        []
        if provider is None
        else (
            provider.api_key
            if isinstance(provider.api_key, list)
            else [provider.api_key]
        )
    )
    api_key = temporary_key or next((key for key in saved_keys if key), "")
    if api_type not in _OPENAI_DISCOVERY_TYPES and api_type != "gemini":
        raise HTTPException(
            status_code=422,
            detail={
                "code": "model_discovery_unsupported",
                "message": "该 API 类型不支持安全的模型自动发现，请手动添加模型。",
            },
        )
    if not api_base or not api_key:
        raise HTTPException(
            status_code=422,
            detail={
                "code": "provider_credentials_incomplete",
                "message": "请先填写 API 地址和 API Key。",
            },
        )
    started = time.monotonic()
    try:
        headers = (
            {"x-goog-api-key": api_key}
            if api_type == "gemini"
            else {"Authorization": f"Bearer {api_key}"}
        )
        async with httpx.AsyncClient(
            timeout=_DISCOVERY_TIMEOUT, follow_redirects=False
        ) as client:
            response = await client.get(
                _discovery_url(api_type, api_base), headers=headers
            )
        if response.status_code in {401, 403}:
            raise HTTPException(
                status_code=401,
                detail={
                    "code": "provider_credentials_invalid",
                    "message": "服务商拒绝了当前凭据。",
                },
            )
        if response.status_code == 429:
            raise HTTPException(
                status_code=429,
                detail={
                    "code": "provider_rate_limited",
                    "message": "服务商暂时限制了模型查询，请稍后重试。",
                },
            )
        if response.status_code >= 400:
            raise HTTPException(
                status_code=502,
                detail={
                    "code": "provider_discovery_failed",
                    "message": f"服务商模型查询失败（HTTP {response.status_code}）。",
                },
            )
        body = response.json()
        raw_models = (
            body.get("models", []) if api_type == "gemini" else body.get("data", [])
        )
        names = []
        for item in raw_models:
            raw_name = item.get("name") if api_type == "gemini" else item.get("id")
            if raw_name:
                names.append(str(raw_name).removeprefix("models/"))
        latency = round((time.monotonic() - started) * 1000)
        logger.info(
            "AI Provider 探测成功 | "
            f"api_type={api_type} | models={len(names)} | latency={latency}ms",
            "WebUi",
        )
        return Result.ok(
            {"models": sorted(set(names)), "latency_ms": latency, "authenticated": True}
        )
    except HTTPException:
        raise
    except httpx.TimeoutException as error:
        raise HTTPException(
            status_code=504,
            detail={"code": "provider_timeout", "message": "连接服务商超时。"},
        ) from error
    except (httpx.HTTPError, ValueError) as error:
        logger.warning(
            "AI Provider 探测失败 | "
            f"api_type={api_type} | error={error.__class__.__name__}",
            "WebUi",
        )
        raise HTTPException(
            status_code=502,
            detail={
                "code": "provider_unreachable",
                "message": "无法完成服务商连接测试。",
            },
        ) from error


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
    started = time.monotonic()
    try:
        from zhenxun.services.ai.llm.api import (
            chat,
            create_image,
            create_speech,
            embed,
            rerank,
        )

        if payload.task == "embedding":
            await embed("test", model=payload.model)
        elif payload.task == "rerank":
            await rerank("a", ["a", "b"], top_n=1, model=payload.model)
        elif payload.task == "image":
            await create_image("one dot", model=payload.model)
        elif payload.task == "tts":
            await create_speech("test", model=payload.model)
        else:
            await chat("Reply OK", model=payload.model, timeout=20)
        latency = round((time.monotonic() - started) * 1000)
        return Result.ok({"ok": True, "latency_ms": latency, "task": payload.task})
    except asyncio.CancelledError:
        raise
    except Exception as error:
        logger.warning(
            f"AI 模型测试失败 | task={payload.task} | error={error.__class__.__name__}",
            "WebUi",
        )
        raise HTTPException(
            status_code=502,
            detail={
                "code": "model_test_failed",
                "message": "模型测试失败，请检查服务商配置和后台脱敏日志。",
            },
        ) from error


__all__ = ["router"]
