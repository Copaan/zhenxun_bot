from __future__ import annotations

import hashlib
from io import StringIO
import json
import os
from pathlib import Path
import re
from typing import Any, Literal, get_args, get_origin

from dotenv import dotenv_values
from fastapi import APIRouter, HTTPException, Response
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from ruamel.yaml import YAML

from zhenxun.configs.config import Config
from zhenxun.configs.webui_tls import (
    WebUITLSConfigError,
    settings_from_values,
    validate_webui_tls_settings,
)
from zhenxun.services.network_proxy import PROXY_KEYS, ProxyPolicy, proxy_runtime
from zhenxun.services.runtime_config_reload import reload_runtime_config
from zhenxun.services.runtime_environment import (
    KNOWN_ENV_KEYS,
    environment_effect,
    is_sensitive_env_key,
    runtime_environment_manager,
)
from zhenxun.services.runtime_mutation import managed_mutation
from zhenxun.services.runtime_reload.models import ApplyMode, RuntimeOperation
from zhenxun.utils._restart_utils import issue_restart_ticket
from zhenxun.utils.pydantic_compat import (
    _is_pydantic_type,
    model_dump,
    model_json_schema,
)

from ....apply_result import (
    APPLY_NO_CHANGE,
    apply_result_data,
    update_pending_restart,
)
from ....base_model import Result
from ....config_validation import (
    ConfigurationValidationError,
    validate_dotenv,
    validate_simple_yaml,
    validation_detail,
)
from ....restart_service import preferred_access_targets, request_webui_restart
from ....utils import authentication
from ...configure.persistence import _write_transaction

router = APIRouter(prefix="/configuration")

_ENV_FILE = Path(".env.dev")
_ENV_TEMPLATE = Path(".env.example")
_SIMPLE_FILE = Path("data/config.yaml")
_ENV_FORM_KEYS = (
    "HOST",
    "PORT",
    "WEBUI_HTTPS_ENABLED",
    "WEBUI_TLS_CERTFILE",
    "WEBUI_TLS_KEYFILE",
    "WEBUI_HTTP_MODE",
    "WEBUI_HTTP_REDIRECT_ENABLED",
    "WEBUI_HTTP_REDIRECT_PORT",
    "LOG_LEVEL",
    "SYSTEM_PROXY",
    "NETWORK_PROXY_MODE",
    "NETWORK_PROXY_PLUGINS",
    "NETWORK_PROXY_CORE_ENABLED",
    "NETWORK_PROXY_BYPASS",
    "NICKNAME",
    "SELF_NICKNAME",
    "COMMAND_START",
    "COMMAND_SEP",
    "ALCONNA_USE_COMMAND_START",
    "SUPERUSERS",
    "PLATFORM_SUPERUSERS",
    "SESSION_EXPIRE_TIMEOUT",
    "IMAGE_TO_BYTES",
    "EXT_PATH",
)
_SECRET_MARKERS = ("TOKEN", "SECRET", "PASSWORD", "COOKIE", "API_KEY", "APIKEY")
_ENV_KEY_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


class CustomEnvOperation(BaseModel):
    key: str = Field(min_length=1, max_length=128)
    operation: Literal["set", "delete"]
    value: str | None = Field(default=None, max_length=65536)


class ConfigurationFileUpdate(BaseModel):
    expected_revision: str = Field(min_length=64, max_length=64)
    content: str | None = None
    fields: dict[str, Any] | None = None
    custom_operations: list[CustomEnvOperation] | None = None


class ConfigurationValidation(BaseModel):
    file: Literal["env", "simple"]
    content: str


def _validate_env(content: str) -> list[dict[str, Any]]:
    warnings = validate_dotenv(content)
    values = dict(dotenv_values(stream=StringIO(content)))
    ProxyPolicy.from_values(values)
    qq_enabled = str(values.get("QQ_ADAPTER_LOAD") or "").casefold() == "true"
    qq_builtin = str(values.get("QQ_WEBHOOK_MODE") or "") == "builtin_https"
    try:
        qq_port = int(values.get("QQ_WEBHOOK_LISTEN_PORT") or 443)
    except (TypeError, ValueError):
        qq_port = 443
    try:
        validate_webui_tls_settings(
            settings_from_values(values),
            qq_https_port=qq_port if qq_enabled and qq_builtin else None,
            launcher_managed=bool(os.getenv("ZHENXUN_LAUNCHER_PID")),
        )
    except WebUITLSConfigError as error:
        raise ConfigurationValidationError(
            [
                {
                    "code": "webui_tls_invalid",
                    "file": ".env.dev",
                    "path": "WEBUI_HTTPS_ENABLED",
                    "line": None,
                    "column": None,
                    "severity": "error",
                    "message": str(error),
                }
            ]
        ) from error
    return warnings


def _validate_simple(content: str) -> list[dict[str, Any]]:
    return validate_simple_yaml(content)


def _path(file: str) -> Path:
    if file == "env":
        return _ENV_FILE if _ENV_FILE.exists() else _ENV_TEMPLATE
    if file == "simple":
        return _SIMPLE_FILE
    raise HTTPException(status_code=404, detail="不支持的配置文件。")


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8") if path.exists() else ""


def _revision(content: str) -> str:
    return hashlib.sha256(content.encode("utf-8")).hexdigest()


def _yaml_parser() -> YAML:
    parser = YAML()
    parser.preserve_quotes = True
    parser.indent(mapping=2, sequence=4, offset=2)
    return parser


def _type_name(value_type: Any) -> tuple[str, list[str]]:
    if value_type is None:
        return "str", []
    origin = get_origin(value_type)
    if origin is not None:
        args = [getattr(item, "__name__", str(item)) for item in get_args(value_type)]
        return getattr(origin, "__name__", str(origin)), args
    return getattr(value_type, "__name__", str(value_type)), []


def _schema_for_type(value_type: Any) -> dict[str, Any]:
    if value_type is None:
        return {"type": "string"}
    origin = get_origin(value_type)
    if origin in (list, tuple, set):
        args = get_args(value_type)
        return {
            "type": "array",
            "items": _schema_for_type(args[0]) if args else {},
        }
    if origin is dict:
        args = get_args(value_type)
        return {
            "type": "object",
            "additionalProperties": _schema_for_type(args[1]) if len(args) > 1 else {},
        }
    if _is_pydantic_type(value_type) and isinstance(value_type, type):
        return model_json_schema(value_type)
    mapping = {str: "string", int: "integer", float: "number", bool: "boolean"}
    return {"type": mapping.get(value_type, "string")}


def _registered_groups() -> list[dict[str, Any]]:
    groups = []
    for module, group in Config.get_data().items():
        fields = []
        for key, config in group.configs.items():
            type_name, type_inner = _type_name(config.type)
            ui = model_dump(config.ui, exclude_none=True) if config.ui else {}
            sensitive = bool(ui.get("secret")) or any(
                marker in key.upper() for marker in _SECRET_MARKERS
            )
            fields.append(
                {
                    "key": key,
                    "help": config.help or "",
                    "type": type_name,
                    "type_inner": type_inner,
                    "value": None if sensitive else jsonable_encoder(config.value),
                    "default_value": (
                        None if sensitive else jsonable_encoder(config.default_value)
                    ),
                    "sensitive": sensitive,
                    "has_value": bool(config.value) if sensitive else None,
                    "schema": _schema_for_type(config.type),
                    "ui": ui,
                }
            )
        fields.sort(key=lambda item: (item["ui"].get("order", 0), item["key"]))
        groups.append(
            {
                "module": module,
                "name": group.name or module,
                "fields": fields,
            }
        )
    return groups


def _env_encode(value: Any) -> str:
    if isinstance(value, bool):
        return "True" if value else "False"
    if isinstance(value, int | float):
        return str(value)
    return json.dumps(value, ensure_ascii=False)


def _update_env(content: str, fields: dict[str, Any]) -> str:
    validate_dotenv(content)
    remaining = {key.upper(): value for key, value in fields.items()}
    output: list[str] = []
    from dotenv.parser import parse_stream

    for binding in parse_stream(StringIO(content)):
        key = binding.key.upper() if binding.key else None
        if key in remaining:
            output.append(f"{key} = {_env_encode(remaining.pop(key))}\n")
        else:
            output.append(binding.original.string)
    if remaining:
        if output and not output[-1].endswith("\n"):
            output.append("\n")
        output.extend(
            f"{key} = {_env_encode(value)}\n" for key, value in remaining.items()
        )
    return "".join(output)


def _update_custom_env(content: str, operations: list[CustomEnvOperation]) -> str:
    if not operations:
        return content
    changes: dict[str, CustomEnvOperation] = {}
    for operation in operations:
        key = operation.key.strip()
        folded = key.casefold()
        if not _ENV_KEY_PATTERN.fullmatch(key):
            raise ValueError("custom_env_key_invalid")
        if key.upper() in KNOWN_ENV_KEYS:
            raise ValueError("custom_env_key_managed")
        if folded in changes:
            raise ValueError("custom_env_key_duplicate")
        if operation.operation == "set" and operation.value is None:
            raise ValueError("custom_env_value_required")
        changes[folded] = operation.model_copy(update={"key": key})

    output: list[str] = []
    from dotenv.parser import parse_stream

    for binding in parse_stream(StringIO(content)):
        folded = binding.key.casefold() if binding.key else None
        operation = changes.pop(folded, None) if folded else None
        if operation is None:
            output.append(binding.original.string)
        elif operation.operation == "set":
            output.append(f"{operation.key} = {_env_encode(operation.value)}\n")
    additions = [item for item in changes.values() if item.operation == "set"]
    if additions:
        if output and not output[-1].endswith("\n"):
            output.append("\n")
        output.extend(f"{item.key} = {_env_encode(item.value)}\n" for item in additions)
    return "".join(output)


def _update_simple(content: str, fields: dict[str, Any]) -> str:
    parser = _yaml_parser()
    data = parser.load(StringIO(content)) or {}
    if not isinstance(data, dict):
        raise ValueError("yaml_top_level_mapping_required")
    for module, values in fields.items():
        if not isinstance(values, dict):
            raise ValueError("yaml_group_mapping_required")
        group = data.setdefault(module, {})
        if not isinstance(group, dict):
            raise ValueError("yaml_group_mapping_required")
        for key, value in values.items():
            group[str(key).upper()] = value
    stream = StringIO()
    parser.dump(data, stream)
    return stream.getvalue()


def _validation_error(file: str, error: Exception) -> HTTPException:
    if isinstance(error, ConfigurationValidationError):
        return HTTPException(
            status_code=422,
            detail=validation_detail(error, message=f"{file} 配置校验失败。"),
        )
    code = str(error)
    messages = {
        "dotenv_invalid_statement": "dotenv 中存在无法解析的语句。",
        "dotenv_duplicate_key": "dotenv 中存在重复配置键。",
        "env_field_not_editable": "提交中包含不允许在此页面修改的环境配置。",
        "custom_env_key_invalid": "自定义环境变量名称不合法。",
        "custom_env_key_managed": "该变量由系统配置表单管理，不能重复添加。",
        "custom_env_key_duplicate": "自定义环境变量操作中存在重复名称。",
        "custom_env_value_required": "新增或替换环境变量时必须填写值。",
        "custom_env_raw_conflict": "高级原文与自定义变量操作不能同时提交。",
        "custom_env_file_invalid": "自定义环境变量只能写入 .env.dev。",
        "yaml_top_level_mapping_required": "config.yaml 顶层必须是映射。",
        "yaml_group_mapping_required": "config.yaml 配置组必须是映射。",
    }
    return HTTPException(
        status_code=422,
        detail=messages.get(
            code, f"{file} 配置校验失败（{error.__class__.__name__}）。"
        ),
    )


@router.get(
    "/summary",
    dependencies=[authentication()],
    response_model=Result,
    response_class=JSONResponse,
)
async def configuration_summary() -> Result:
    from ....restart_service import network_configuration_status

    env_path = _path("env")
    env_content = _read(env_path)
    values = dotenv_values(stream=StringIO(env_content))
    env_fields = {
        key: None if is_sensitive_env_key(key) else values.get(key)
        for key in _ENV_FORM_KEYS
    }
    custom_env = []
    for key, value in sorted(values.items(), key=lambda item: str(item[0]).casefold()):
        if str(key).upper() in KNOWN_ENV_KEYS:
            continue
        sensitive = is_sensitive_env_key(str(key))
        custom_env.append(
            {
                "key": str(key),
                "value": None if sensitive else value,
                "configured": value not in {None, ""},
                "sensitive": sensitive,
                "apply_effect": "restart_required",
            }
        )
    return Result.ok(
        {
            "env": {
                "fields": env_fields,
                "field_effects": {
                    key: environment_effect(key) for key in _ENV_FORM_KEYS
                },
                "custom_env": custom_env,
                "revision": _revision(env_content),
            },
            "simple": {
                "groups": _registered_groups(),
                "revision": _revision(_read(_SIMPLE_FILE)),
            },
            "launcher_managed": bool(os.getenv("ZHENXUN_LAUNCHER_PID")),
            "network": network_configuration_status(),
        }
    )


@router.get(
    "/files/{file}",
    dependencies=[authentication()],
    response_model=Result,
    response_class=JSONResponse,
)
async def get_configuration_file(file: str, response: Response) -> Result:
    content = _read(_path(file))
    response.headers["Cache-Control"] = "no-store"
    return Result.ok(
        {
            "file": file,
            "content": content,
            "revision": _revision(content),
            "sensitive": True,
        }
    )


@router.post(
    "/validate",
    dependencies=[authentication()],
    response_model=Result,
    response_class=JSONResponse,
)
async def validate_configuration(payload: ConfigurationValidation) -> Result:
    try:
        warnings = (
            _validate_env(payload.content)
            if payload.file == "env"
            else validate_simple_yaml(payload.content)
        )
    except Exception as error:
        raise _validation_error(payload.file, error) from error
    return Result.ok({"valid": True, "warnings": warnings})


@router.put(
    "/files/{file}",
    dependencies=[authentication()],
    response_model=Result,
    response_class=JSONResponse,
)
@managed_mutation("webui.configuration")
async def update_configuration_file(
    file: str, payload: ConfigurationFileUpdate
) -> Result:
    target = _ENV_FILE if file == "env" else _path(file)
    current_path = _path(file)
    current = _read(current_path)
    if _revision(current) != payload.expected_revision:
        raise HTTPException(
            status_code=409, detail="配置文件已被外部修改，请重新加载。"
        )
    if (
        payload.content is None
        and payload.fields is None
        and not payload.custom_operations
    ):
        raise HTTPException(status_code=422, detail="没有可保存的配置内容。")
    try:
        content = payload.content
        if payload.content is not None and payload.custom_operations:
            raise ValueError("custom_env_raw_conflict")
        if file != "env" and payload.custom_operations:
            raise ValueError("custom_env_file_invalid")
        if content is None:
            if file == "env" and set(payload.fields or {}) - set(_ENV_FORM_KEYS):
                raise ValueError("env_field_not_editable")
            content = (
                _update_env(current, payload.fields or {})
                if file == "env"
                else _update_simple(current, payload.fields or {})
            )
            if file == "env":
                content = _update_custom_env(content, payload.custom_operations or [])
        warnings = (
            _validate_env(content) if file == "env" else validate_simple_yaml(content)
        )
    except Exception as error:
        raise _validation_error(file, error) from error

    if file == "env":
        values = dict(dotenv_values(stream=StringIO(content)))
        previous_values = dict(dotenv_values(stream=StringIO(current)))
        if any(values.get(key) != previous_values.get(key) for key in PROXY_KEYS):
            await proxy_runtime.prepare(ProxyPolicy.from_values(values))
    original = target.read_bytes() if target.exists() else None
    operation: RuntimeOperation | None = None
    env_operation = None
    try:
        content_bytes = content.encode("utf-8")
        content_changed = original != content_bytes
        if content_changed:
            _write_transaction([(target, content_bytes)])
        if file == "simple" and content_changed:
            operation = await reload_runtime_config(submit_restart=False)
            if operation.mode is ApplyMode.FAILED:
                raise RuntimeError(operation.reason or "config_consumer_reload_failed")
        elif file == "env" and content_changed:
            env_operation = await runtime_environment_manager.apply(
                current, content, submit_restart=False
            )
    except Exception as error:
        if original is None:
            target.unlink(missing_ok=True)
        else:
            _write_transaction([(target, original)])
        if file == "simple":
            try:
                await reload_runtime_config(submit_restart=False)
            except Exception:
                pass
        raise HTTPException(
            status_code=500,
            detail=f"配置保存或重载失败（{error.__class__.__name__}）。",
        ) from error

    if file == "env":
        apply_mode = env_operation.apply_mode if env_operation else APPLY_NO_CHANGE
        restart_required = bool(env_operation and env_operation.restart_required)
        changed_keys = env_operation.changed_keys if env_operation else []
        reason_codes = env_operation.reason_codes if env_operation else []
        launcher_managed = bool(os.getenv("ZHENXUN_LAUNCHER_PID"))
    else:
        apply_mode = operation.mode.value if operation is not None else APPLY_NO_CHANGE
        restart_required = apply_mode in {
            ApplyMode.RESTART_PENDING.value,
            ApplyMode.RESTART_REQUESTED.value,
        }
        changed_keys = operation.config_keys if operation is not None else []
        reason_codes = [operation.reason] if operation and operation.reason else []
        launcher_managed = update_pending_restart(
            "webui.config",
            reason_codes if restart_required else [],
            issue_ticket=False,
        )
    if restart_required and launcher_managed:
        issue_restart_ticket("webui.settings", ttl_seconds=10 * 60)
    access_urls: list[str] = []
    access_targets: list[dict[str, str]] = []
    if file == "env":
        values = dotenv_values(stream=StringIO(content))
        host = str(values.get("HOST") or "0.0.0.0")
        try:
            port = int(values.get("PORT") or 8080)
        except (TypeError, ValueError):
            port = 8080
        tls_settings = settings_from_values(dict(values))
        local_urls = preferred_access_targets(
            host, port, settings=tls_settings, sidecar_available=launcher_managed
        )
        access_urls = [item.url for item in local_urls]
        access_targets = [
            {
                "kind": item.label.lower(),
                "url": item.url,
                "scheme": item.url.split(":", 1)[0],
            }
            for item in local_urls
        ]
    data = apply_result_data(
        apply_mode=apply_mode,
        changed_keys=changed_keys,
        restart_required=restart_required,
        hot_reloaded=(
            env_operation.hot_reloaded if env_operation is not None else None
        ),
        reason_codes=reason_codes,
        access_urls=access_urls,
        access_targets=access_targets,
        file=file,
        revision=_revision(content),
        warnings=warnings,
        reason=operation.reason if operation is not None else None,
        field_effects=env_operation.field_effects if env_operation else {},
        component_effects=env_operation.component_effects if env_operation else {},
        affected_components=(
            env_operation.affected_components if env_operation else []
        ),
        rolled_back=bool(env_operation and env_operation.rolled_back),
        affected=(
            env_operation.changed_plugins
            if env_operation is not None
            else operation.changed
            if operation is not None
            else []
        ),
    )
    if apply_mode == APPLY_NO_CHANGE:
        info = "配置已保存，没有需要应用的运行时变化。"
    elif restart_required:
        info = "配置已保存，需要重启后生效。"
    elif apply_mode == "component_restarted":
        info = "配置已保存，相关运行时组件已重建。"
    else:
        info = "配置已保存并热加载。"
    return Result.ok(data, info=info)


@router.post(
    "/restart",
    dependencies=[authentication()],
    response_model=Result,
    response_class=JSONResponse,
)
async def restart_after_configuration() -> Result:
    if not os.getenv("ZHENXUN_LAUNCHER_PID"):
        return Result.fail("当前不是 launcher 托管模式，请手动重启真寻。", code=409)
    ok, message, data = await request_webui_restart(
        "webui.settings", require_ticket="webui.settings"
    )
    return Result.ok(data, info=message) if ok else Result.fail(message, code=409)


__all__ = ["router"]
