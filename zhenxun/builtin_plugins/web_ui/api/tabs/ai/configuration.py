from __future__ import annotations

import asyncio
from copy import deepcopy
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
from ruamel.yaml import YAML

from zhenxun.configs.config import Config
from zhenxun.services.ai.config import get_llm_config
from zhenxun.services.ai.config.models import LLMConfig, ProviderConfig
from zhenxun.services.ai.llm.adapters.factory import LLMAdapterFactory
from zhenxun.services.ai.llm.manager import get_default_api_base_for_type
from zhenxun.services.ai.llm.system.capabilities import get_model_capabilities
from zhenxun.services.log import logger
from zhenxun.services.runtime_config_reload import reload_runtime_config
from zhenxun.services.runtime_reload.models import RuntimeOperation
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
from ....restart_service import restart_status_data
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
_SANDBOX_STARTUP_VALUES = model_dump(get_llm_config().sandbox)
_SANDBOX_STARTUP_KEYS = {"enable_sandbox", "sandbox_type"}


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


class RoutingValidationRequest(BaseModel):
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


class PersonaUpdate(BaseModel):
    expected_revision: str = Field(min_length=64, max_length=64)
    name: str = Field(min_length=1, max_length=80)
    prompt: str = Field(min_length=1, max_length=20000)
    style: str = Field(default="", max_length=2000)
    tone_examples: list[str] = Field(default_factory=list, max_length=12)
    preset_dialogues: list[str] = Field(default_factory=list, max_length=12)
    enabled: bool = True


def _yaml() -> YAML:
    parser = YAML()
    parser.preserve_quotes = True
    parser.indent(mapping=2, sequence=4, offset=2)
    return parser


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
        targets = config.model_groups.get(group, [])
        for target in targets:
            if target in group_names:
                visit(target, (*stack, group))
            elif target not in available_models:
                add(
                    "model_reference_missing",
                    "AI.MODEL_GROUPS",
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
        candidates = [value]
    elif isinstance(value, list):
        candidates = [str(item) for item in value]
    else:
        candidates = []
    return [item for item in candidates if not _is_placeholder_secret(item)]


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
        if replacement and _is_placeholder_secret(replacement):
            replacement = ""
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
    expected_revision: str,
    mutate: Any,
    *,
    reload_consumers: bool = True,
) -> tuple[str, LLMConfig, RuntimeOperation | None]:
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
    if content == current:
        return _revision(content), config, None
    original = _CONFIG_FILE.read_bytes() if _CONFIG_FILE.exists() else None
    try:
        _write_transaction([(_CONFIG_FILE, content.encode("utf-8"))])
        if reload_consumers:
            operation = await reload_runtime_config(submit_restart=False)
        else:
            try:
                operation = await reload_runtime_config(
                    submit_restart=False,
                    reload_consumers=False,
                )
            except TypeError as error:
                if "reload_consumers" not in str(error):
                    raise
                operation = await reload_runtime_config(submit_restart=False)
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


async def _persist_with_operation(
    expected_revision: str,
    mutate: Any,
    *,
    reload_consumers: bool = True,
) -> tuple[str, LLMConfig, RuntimeOperation | None]:
    async with _PERSIST_LOCK:
        return await _persist_unlocked(
            expected_revision,
            mutate,
            reload_consumers=reload_consumers,
        )


async def _persist(expected_revision: str, mutate: Any) -> tuple[str, LLMConfig]:
    revision, config, _ = await _persist_with_operation(expected_revision, mutate)
    return revision, config


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
    restart_required: bool | None = None,
    reason_codes: list[str] | None = None,
    pending_source: str = "webui.ai",
) -> dict[str, Any]:
    status = restart_status_data()
    mode = apply_mode or (
        operation.mode.value if operation is not None else APPLY_NO_CHANGE
    )
    required = (
        mode == APPLY_RESTART_PENDING if restart_required is None else restart_required
    )
    reasons = reason_codes or (
        [operation.reason] if operation is not None and operation.reason else []
    )
    if required:
        launcher_managed = update_pending_restart(
            pending_source,
            reasons or ["ai_runtime_restart_required"],
            issue_ticket=False,
        )
        if launcher_managed:
            issue_restart_ticket("webui.settings", ttl_seconds=10 * 60)
    return apply_result_data(
        apply_mode=mode,
        changed_keys=(
            changed_keys
            if changed_keys is not None
            else operation.config_keys
            if operation is not None
            else []
        ),
        restart_required=required,
        reason_codes=reasons,
        access_urls=status["access_urls"],
        access_targets=status["access_targets"],
        **_configuration_view(config, revision),
    )


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
    valid_keys = [value for value in keys if not _is_placeholder_secret(value)]
    data["api_key_slots"] = [
        {"existing_index": index, "configured": True}
        for index, value in enumerate(keys)
        if not _is_placeholder_secret(value)
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
    discovery_supported = (
        provider.api_type in _OPENAI_DISCOVERY_TYPES or provider.api_type == "gemini"
    )
    effective_base = provider.api_base or get_default_api_base_for_type(
        provider.api_type
    )
    if not discovery_supported:
        discovery_status = "manual_only"
        discovery_reason_code = "model_discovery_unsupported"
    elif not effective_base:
        discovery_status = "missing_base"
        discovery_reason_code = "provider_api_base_incomplete"
    elif not valid_keys:
        discovery_status = "missing_credentials"
        discovery_reason_code = "provider_credentials_incomplete"
    else:
        discovery_status = "ready"
        discovery_reason_code = None
    data["discovery_supported"] = discovery_supported
    data["credential_status"] = "configured" if valid_keys else "missing"
    data["discovery_status"] = discovery_status
    data["discovery_reason_code"] = discovery_reason_code
    return data


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

    revision, config, operation = await _persist_with_operation(
        payload.expected_revision, mutate
    )
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

    revision, config, operation = await _persist_with_operation(
        payload.expected_revision, mutate
    )
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

    revision, config, operation = await _persist_with_operation(
        expected_revision, mutate
    )
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

    revision, config, operation = await _persist_with_operation(
        payload.expected_revision, mutate
    )
    return Result.ok(
        _apply_response(config, revision, operation),
        info="模型列表已保存并热加载。",
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

    before_config = _validate_full(_load(_read())["AI"], strict_references=False)

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
            _ai_set(ai, key, deepcopy(_normalize_model_groups(payload.value)))
        elif key:
            update_value(key, payload.value)

    revision, config, operation = await _persist_with_operation(
        payload.expected_revision,
        mutate,
        reload_consumers=section != "sandbox",
    )
    effect = "hot_reload"
    apply_mode: str | None = None
    restart_required: bool | None = None
    changed_keys: list[str] | None = None
    reason_codes: list[str] | None = None
    pending_source = "webui.ai"
    if section == "sandbox":
        before_sandbox = model_dump(before_config.sandbox)
        after_sandbox = model_dump(config.sandbox)
        changed_fields = sorted(
            key
            for key in before_sandbox.keys() | after_sandbox.keys()
            if before_sandbox.get(key) != after_sandbox.get(key)
        )
        deferred_fields = sorted(
            key
            for key in _SANDBOX_STARTUP_KEYS
            if after_sandbox.get(key) != _SANDBOX_STARTUP_VALUES.get(key)
        )
        changed_startup_fields = sorted(set(changed_fields) & _SANDBOX_STARTUP_KEYS)
        changed_new_session_fields = sorted(set(changed_fields) - _SANDBOX_STARTUP_KEYS)
        if deferred_fields:
            _restore_sandbox_startup_runtime()
        pending_source = "webui.ai-sandbox"
        reason_codes = [f"ai.sandbox.{key}" for key in deferred_fields]
        update_pending_restart(pending_source, reason_codes, issue_ticket=False)
        if changed_startup_fields and deferred_fields:
            apply_mode = APPLY_RESTART_PENDING
            restart_required = True
            changed_keys = [f"AI.SANDBOX.{key}" for key in changed_fields]
            effect = "restart_required"
        elif changed_new_session_fields:
            apply_mode = APPLY_NEW_SESSION
            restart_required = False
            changed_keys = [f"AI.SANDBOX.{key}" for key in changed_fields]
            effect = "new_session"
        elif changed_startup_fields:
            apply_mode = APPLY_CONFIG_RELOADED
            restart_required = False
            changed_keys = [f"AI.SANDBOX.{key}" for key in changed_fields]
            effect = "hot_reload"
        else:
            apply_mode = APPLY_NO_CHANGE
            restart_required = False
            changed_keys = []
    return Result.ok(
        {
            **_apply_response(
                config,
                revision,
                operation,
                apply_mode=apply_mode,
                changed_keys=changed_keys,
                restart_required=restart_required,
                reason_codes=reason_codes,
                pending_source=pending_source,
            ),
            "effect": effect,
        },
        info=(
            "AI 配置已保存，需要重启后生效。" if restart_required else "AI 配置已保存。"
        ),
    )


@router.post(
    "/configuration/validate-routing",
    dependencies=[authentication()],
    response_model=Result,
    response_class=JSONResponse,
)
async def validate_routing(payload: RoutingValidationRequest) -> Result:
    data = _load(_read())
    candidate = deepcopy(data)
    _ai_set(candidate["AI"], "MODEL_GROUPS", _normalize_model_groups(payload.value))
    config = _validate_full(candidate["AI"], strict_references=False)
    issues = _reference_issues(config)
    return Result.ok({"valid": not issues, "issues": issues})


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
    api_key = temporary_key or next(
        (key for key in saved_keys if not _is_placeholder_secret(key)), ""
    )
    if _is_placeholder_secret(api_key):
        api_key = ""
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
    provider_name = payload.model.split("/", 1)[0]
    provider = next(
        (
            item
            for item in get_llm_config().providers
            if item.name.casefold() == provider_name.casefold()
        ),
        None,
    )
    provider_keys = (
        []
        if provider is None
        else (
            provider.api_key
            if isinstance(provider.api_key, list)
            else [provider.api_key]
        )
    )
    if not any(not _is_placeholder_secret(key) for key in provider_keys):
        raise HTTPException(
            status_code=422,
            detail={
                "code": "provider_credentials_incomplete",
                "message": "该模型所属服务商尚未配置有效 API Key。",
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
