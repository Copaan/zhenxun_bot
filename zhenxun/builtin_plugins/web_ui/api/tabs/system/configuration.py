from __future__ import annotations

import asyncio
from datetime import timedelta
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
from pydantic.errors import PydanticUserError
from ruamel.yaml import YAML

from zhenxun.configs.config import Config
from zhenxun.configs.environment import environment_file, environment_target
from zhenxun.configs.webui_tls import (
    WebUITLSConfigError,
    settings_from_values,
    validate_webui_tls_settings,
)
from zhenxun.services.log import logger
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
    model_dump,
)

from ....apply_result import (
    APPLY_NO_CHANGE,
    apply_result_data,
    update_pending_restart,
)
from ....base_model import Result
from ....config_schema import configuration_value, inferred_schema, schema_for_type
from ....config_validation import (
    ConfigurationValidationError,
    validate_dotenv,
    validate_simple_yaml,
    validation_detail,
)
from ....environment_fields import (
    CORE_FIELDS,
    comparable_value,
    environment_category,
    typed_environment_value,
    visible_environment_fields,
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
    unset_fields: list[list[str]] = Field(default_factory=list)


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


def _path(file: str) -> Path:
    if file == "env":
        return environment_file(
            template=True, preferred=_ENV_FILE, template_path=_ENV_TEMPLATE
        )
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
    return schema_for_type(value_type)


def _model_field_descriptors(model: type[BaseModel]) -> dict[str, dict[str, Any]]:
    fields = getattr(model, "model_fields", None) or getattr(model, "__fields__", {})
    result = {}
    for name, field in fields.items():
        annotation = getattr(field, "outer_type_", None) or getattr(
            field, "annotation", Any
        )
        schema = _schema_for_type(annotation)
        default = getattr(field, "default", None)
        if (
            default is not None
            and type(default).__name__ not in {"PydanticUndefinedType", "UndefinedType"}
            and "default" not in schema
        ):
            try:
                schema["default"] = configuration_value(default)
            except (TypeError, ValueError):
                pass
        result[str(name)] = schema
    return result


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
                    "value": None if sensitive else configuration_value(config.value),
                    "effective_value": None
                    if sensitive
                    else configuration_value(config.value),
                    "default_value": (
                        None if sensitive else configuration_value(config.default_value)
                    ),
                    "sensitive": sensitive,
                    "has_value": bool(config.value) if sensitive else None,
                    "schema": _schema_for_type(config.type),
                    "ui": ui,
                    "authority": (
                        "database"
                        if module.casefold() == "ai"
                        and key.upper() == "CHAT_PLUGIN_ENABLED"
                        else "file"
                    ),
                }
            )
        fields.sort(key=lambda item: (item["ui"].get("order", 0), item["key"]))
        groups.append(
            {
                "module": module,
                "name": group.name or module,
                "fields": fields,
                "registered": True,
                "source_status": "loaded",
            }
        )
    stored = _yaml_parser().load(StringIO(_read(_SIMPLE_FILE))) or {}
    if isinstance(stored, dict):
        known_groups = {item["module"]: item for item in groups}
        for module, values in stored.items():
            if not isinstance(values, dict):
                groups.append(
                    {
                        "module": str(module),
                        "name": str(module),
                        "fields": [],
                        "registered": False,
                        "source_status": "invalid_top_level",
                        "raw_value": jsonable_encoder(values),
                        "readonly_reason": "此顶层值不是配置组映射，请使用原文编辑。",
                    }
                )
                continue
            group = known_groups.get(str(module))
            if group is None:
                group = {
                    "module": str(module),
                    "name": str(module),
                    "fields": [],
                    "registered": False,
                    "source_status": "orphaned",
                }
                groups.append(group)
            existing = {item["key"].upper(): item for item in group["fields"]}
            for key, value in values.items():
                if str(key).upper() in existing:
                    descriptor = existing[str(key).upper()]
                    descriptor["configured"] = True
                    if not descriptor["sensitive"]:
                        descriptor["file_value"] = configuration_value(value)
                        descriptor["value"] = configuration_value(value)
                    continue
                group["fields"].append(
                    {
                        "key": str(key),
                        "help": "未注册配置，保存后需确认对应插件已加载。",
                        "type": type(value).__name__,
                        "value": configuration_value(value),
                        "default_value": None,
                        "schema": inferred_schema(value),
                        "ui": {},
                        "registered": False,
                        "sensitive": False,
                    }
                )
    for group in groups:
        for field in group["fields"]:
            if (
                group["module"].casefold() == "ai"
                and field["key"].upper() == "CHAT_PLUGIN_ENABLED"
            ):
                field["help"] = (
                    "历史首次导入值；当前开关以数据库插件策略为准。请在插件策略页面修改。"
                )
                field["authority"] = "database"
    return groups


def _environment_models():
    from nonebot import get_loaded_plugins
    from nonebot.adapters.onebot.v11.config import Config as OneBotConfig
    from nonebot.config import Config as NoneBotConfig

    from zhenxun.adapters.qq_official.config import QQOfficialConfig
    from zhenxun.configs.config import BotSetting
    from zhenxun.services.cache import Config as CacheConfig

    models = [NoneBotConfig, BotSetting, QQOfficialConfig, CacheConfig, OneBotConfig]
    for plugin in sorted(get_loaded_plugins(), key=lambda item: item.id_):
        model = getattr(getattr(plugin, "metadata", None), "config", None)
        if model is not None and model not in models:
            models.append(model)
    return models


def _environment_fields(values: dict[str, Any]) -> dict[str, dict[str, Any]]:
    from nonebot import get_adapters, get_driver, get_loaded_plugins

    from zhenxun.utils.pydantic_compat import model_json_schema

    values = {key.upper(): value for key, value in values.items()}
    schemas = {}
    annotations = {}
    defaults = {}
    declarations = {}
    conflicts = {}
    schema_errors = {}
    owner_names = {
        plugin.metadata.config: plugin.metadata.name or plugin.id_
        for plugin in get_loaded_plugins()
        if plugin.metadata and plugin.metadata.config
    }
    labels = {}
    attribute_names = {}
    for model in _environment_models():
        source = f"{model.__module__}.{model.__name__}"
        try:
            root = model_json_schema(model, by_alias=False)
        except Exception as error:
            root = {
                "properties": _model_field_descriptors(model),
                "x-schema-error": type(error).__name__,
            }
            schema_errors[source] = type(error).__name__
        fields = dict(
            getattr(model, "model_fields", None) or getattr(model, "__fields__", {})
        )
        properties = dict(root.get("properties", {}))
        for name, field in list(fields.items()):
            attribute_names.setdefault(name.upper(), name)
            alias = getattr(field, "alias", None)
            if (
                isinstance(alias, str)
                and alias != name
                and alias.upper() in values
                and name in properties
            ):
                fields[alias] = field
                properties[alias] = properties[name]
                attribute_names[alias.upper()] = name
        for name, schema in properties.items():
            field = fields.get(name)
            if field is not None:
                annotation = getattr(field, "outer_type_", None) or getattr(
                    field, "annotation", Any
                )
                annotations.setdefault(name.upper(), annotation)
                if "default" in schema:
                    defaults.setdefault(name.upper(), schema["default"])
                elif not getattr(field, "default_factory", None):
                    from pydantic_core import PydanticUndefined

                    default = getattr(field, "default", PydanticUndefined)
                    if (
                        default is not PydanticUndefined
                        and type(default).__name__ != "UndefinedType"
                    ):
                        defaults.setdefault(name.upper(), default)
            key = name.upper()
            declarations.setdefault(key, []).append(source)
            labels.setdefault(key, []).append(owner_names.get(model, source))
            # Ignore presentation/default differences; retain definitions when
            # comparing referenced types so equal names cannot hide conflicts.
            signature = {
                k: v
                for k, v in schema.items()
                if k not in {"title", "description", "default", "examples"}
            }
            if "$ref" in json.dumps(signature):
                signature["definitions"] = root.get(
                    "$defs", root.get("definitions", {})
                )
            if key in schemas and schemas[key]["signature"] != signature:
                conflicts[key] = True
            else:
                schemas.setdefault(
                    key,
                    {
                        "schema": {**schema, "x-root-schema": root},
                        "signature": signature,
                    },
                )
    for key, (annotation, default, _) in CORE_FIELDS.items():
        annotations.setdefault(key, annotation)
        defaults.setdefault(key, default)
        schemas.setdefault(
            key,
            {
                "schema": {
                    **schema_for_type(annotation),
                    **({"default": default} if default is not None else {}),
                }
            },
        )
    keys = set(KNOWN_ENV_KEYS) | set(_ENV_FORM_KEYS) | set(schemas)
    runtime = get_driver().config
    from zhenxun.configs.config import BotConfig, BotSetting
    from zhenxun.services.cache import Config as CacheConfig
    from zhenxun.services.cache import cache_config

    bot_keys = set(BotSetting.model_fields)
    cache_keys = set(CacheConfig.model_fields)
    onebot = next(
        (
            getattr(adapter, "onebot_config", None)
            for adapter in get_adapters().values()
            if getattr(adapter, "onebot_config", None) is not None
        ),
        None,
    )
    result = {}
    for key in sorted(keys):
        schema = {} if key in conflicts else schemas.get(key, {}).get("schema", {})
        raw = values.get(key)
        annotation = annotations.get(key, Any)
        default = defaults.get(key, schema.get("default"))
        if default is not None:
            try:
                default = typed_environment_value(default, annotation)
            except (ValueError, TypeError, PydanticUserError):
                pass
        parsed = raw
        valid = True
        if key in values:
            try:
                parsed = typed_environment_value(raw, annotation)
            except (ValueError, TypeError, PydanticUserError):
                valid = False
        secret = is_sensitive_env_key(key) or key == "DB_URL"
        attr = attribute_names.get(key, key.lower())
        provider = (
            cache_config
            if attr in cache_keys
            else BotConfig
            if attr in bot_keys
            else runtime
        )
        if attr.startswith("onebot_"):
            provider = onebot
        available = hasattr(provider, attr)
        category = environment_category(key, declarations.get(key, []))
        if provider is runtime and category == "plugins":
            # Driver extras are loader inputs, not evidence of the plugin's
            # currently consumed configuration instance.
            available = False
        effective = getattr(provider, attr, None)
        if available:
            try:
                effective = typed_environment_value(effective, annotation)
            except (ValueError, TypeError, PydanticUserError):
                available = False
        if annotation is timedelta:
            schema = {
                "type": "number",
                "minimum": 0,
                "description": "交互会话的超时时间，单位：秒。",
            }
        if default is not None:
            schema = {**schema, "default": comparable_value(default)}
        expected = parsed if key in values else default
        external_present = key in runtime_environment_manager.external_values
        overridden = False
        if available and external_present:
            try:
                external = typed_environment_value(
                    runtime_environment_manager.external_values[key], annotation
                )
                overridden = comparable_value(external) == comparable_value(
                    effective
                ) and comparable_value(expected) != comparable_value(effective)
            except (ValueError, TypeError, PydanticUserError):
                pass
        if (
            available
            and valid
            and comparable_value(expected) == comparable_value(effective)
        ):
            apply_state = "applied"
        elif key in runtime_environment_manager.pending_keys:
            apply_state = "restart_pending"
        elif runtime_environment_manager._lock.locked():
            apply_state = "applying"
        else:
            apply_state = "unknown"
        result[key] = {
            "key": key,
            "schema": {
                k: v
                for k, v in schema.items()
                if k not in {"default", "examples", "x-root-schema"}
            }
            if secret
            else schema,
            "schema_sources": declarations.get(key, []),
            "source_labels": labels.get(key, []),
            "schema_errors": [
                schema_errors[source]
                for source in declarations.get(key, [])
                if source in schema_errors
            ],
            "schema_notice": (
                "字段类型声明无法完整生成，保存时由后端校验。"
                if any(source in schema_errors for source in declarations.get(key, []))
                else None
            ),
            "readonly_reason": (
                "字段类型声明冲突：" + "、".join(declarations[key])
                if key in conflicts
                else None
            ),
            "sensitive": secret,
            "value": None if secret else comparable_value(parsed),
            "default_value": None if secret else comparable_value(default),
            "configured": key in values,
            "effective_value": None
            if secret or not available
            else comparable_value(effective),
            "runtime_available": available and not secret,
            "apply_state": apply_state,
            "category": category,
            "registered": key in annotations or key in KNOWN_ENV_KEYS,
            "overridden": overridden,
            "external_environment_present": external_present,
            "apply_effect": environment_effect(key),
        }
    return result


def _validate_environment_types(
    content: str, changed_keys: set[str] | None = None
) -> None:
    from zhenxun.utils.pydantic_compat import parse_as

    values = {
        key.upper(): value
        for key, value in dotenv_values(stream=StringIO(content)).items()
    }
    descriptors = _environment_fields(values)
    issues = [
        {
            "code": "env_schema_conflict",
            "file": str(_path("env")),
            "path": key,
            "message": field["readonly_reason"],
            "severity": "error",
        }
        for key, field in descriptors.items()
        if key in values
        and field["readonly_reason"]
        and (changed_keys is None or key in changed_keys)
    ]
    for model in _environment_models():
        fields = dict(
            getattr(model, "model_fields", None) or getattr(model, "__fields__", {})
        )
        for field in list(fields.values()):
            alias = getattr(field, "alias", None)
            if isinstance(alias, str) and alias.upper() in values:
                fields[alias] = field
        for name, field in fields.items():
            key = name.upper()
            if key not in values or (
                changed_keys is not None and key not in changed_keys
            ):
                continue
            raw = values[key]
            value = descriptors[key]["value"]
            if descriptors[key]["sensitive"]:
                value = raw
            annotation = getattr(field, "outer_type_", field.annotation)
            try:
                parse_as(annotation, value)
            except Exception as error:
                details = error.errors() if hasattr(error, "errors") else [{}]
                for detail in details:
                    issues.append(
                        {
                            "code": "env_value_invalid",
                            "file": str(_path("env")),
                            "path": ".".join([key, *map(str, detail.get("loc", ()))]),
                            "message": detail.get("msg", "值不符合声明类型"),
                            "severity": "error",
                        }
                    )
    if issues:
        raise ConfigurationValidationError(issues)


def _env_encode(value: Any) -> str:
    if isinstance(value, bool):
        return "True" if value else "False"
    if isinstance(value, int | float):
        return str(value)
    if isinstance(value, dict | list):
        # The outer quoting belongs to dotenv; one json.loads recovers the object.
        return json.dumps(json.dumps(value, ensure_ascii=False), ensure_ascii=False)
    return json.dumps(value, ensure_ascii=False)


def _update_env(content: str, fields: dict[str, Any]) -> str:
    validate_dotenv(content)
    remaining = {key.upper(): value for key, value in fields.items()}
    for model in _environment_models():
        declarations = getattr(model, "model_fields", None) or getattr(
            model, "__fields__", {}
        )
        for name, field in declarations.items():
            key = name.upper()
            annotation = getattr(field, "outer_type_", None) or getattr(
                field, "annotation", Any
            )
            if (
                annotation is timedelta
                and key in remaining
                and isinstance(remaining[key], int | float)
            ):
                seconds = typed_environment_value(
                    remaining[key], timedelta
                ).total_seconds()
                remaining[key] = f"PT{seconds:g}S"
    output: list[str] = []
    from dotenv.parser import parse_stream

    for binding in parse_stream(StringIO(content)):
        key = binding.key.upper() if binding.key else None
        if key in remaining:
            output.append(_replace_env_binding(binding, remaining.pop(key)))
        else:
            output.append(binding.original.string)
    if remaining:
        if output and not output[-1].endswith("\n"):
            output.append("\n")
        output.extend(
            f"{key} = {_env_encode(value)}\n" for key, value in remaining.items()
        )
    return "".join(output)


def _replace_env_binding(binding, value: Any) -> str:
    original = binding.original.string
    match = re.match(
        r"(\s*(?:export[ \t]+)?(?:'[^']+'|[^\s=#]+)[ \t]*=[ \t]*)([\s\S]*)", original
    )
    if match is None:
        return f"{binding.key} = {_env_encode(value)}\n"
    prefix, old = match.groups()
    quoted = re.match(r"""(?:"(?:\\[\s\S]|[^"\\])*"|'(?:\\[\s\S]|[^'\\])*')""", old)
    if quoted:
        suffix = old[quoted.end() :]
    else:
        comment = re.search(r"[ \t]+#", old)
        suffix = (
            old[comment.start() :] if comment else ("\n" if old.endswith("\n") else "")
        )
    return prefix + _env_encode(value) + suffix


def _update_custom_env(content: str, operations: list[CustomEnvOperation]) -> str:
    if not operations:
        return content
    registered = set(_environment_fields({}))
    changes: dict[str, CustomEnvOperation] = {}
    for operation in operations:
        key = operation.key.strip()
        folded = key.casefold()
        if not _ENV_KEY_PATTERN.fullmatch(key):
            raise ValueError("custom_env_key_invalid")
        if key.upper() in registered:
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


def _replace_value(previous: Any, value: Any) -> Any:
    if previous == value:
        return previous
    if isinstance(previous, dict) and isinstance(value, dict):
        for key in list(previous):
            if key not in value:
                del previous[key]
        for key, child in value.items():
            previous[key] = _replace_value(previous.get(key), child)
        return previous
    if isinstance(previous, list) and isinstance(value, list):
        for index, child in enumerate(value):
            if index < len(previous):
                previous[index] = _replace_value(previous[index], child)
            else:
                previous.append(child)
        del previous[len(value) :]
        return previous
    return value


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
            registered = Config.get_data().get(module)
            normalized = str(key).upper()
            target_key = next(
                (old for old in group if str(old).upper() == normalized), None
            )
            if target_key is None:
                target_key = (
                    normalized
                    if registered and normalized in registered.configs
                    else str(key)
                )
            group[target_key] = _replace_value(group.get(target_key), value)
    stream = StringIO()
    parser.dump(data, stream)
    return stream.getvalue()


def _unset_fields(content: str, file: str, paths: list[list[str]]) -> str:
    if not paths:
        return content
    if file == "env":
        from dotenv.parser import parse_stream

        if any(
            len(path) != 1 or not _ENV_KEY_PATTERN.fullmatch(path[0]) for path in paths
        ):
            raise ValueError("invalid_unset_path")
        keys = {path[0].upper() for path in paths}
        return "".join(
            binding.original.string
            for binding in parse_stream(StringIO(content))
            if not binding.key or binding.key.upper() not in keys
        )
    parser = _yaml_parser()
    data = parser.load(StringIO(content)) or {}
    for path in paths:
        if len(path) != 2:
            raise ValueError("invalid_unset_path")
        group = data.get(path[0], {})
        if isinstance(group, dict):
            key = next(
                (key for key in group if str(key).casefold() == path[1].casefold()),
                path[1],
            )
            group.pop(key, None)
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
    from nonebot import get_loaded_plugins

    from ....restart_service import network_configuration_status

    env_path = _path("env")
    env_content = _read(env_path)

    values = dict(dotenv_values(stream=StringIO(env_content)))
    registry = _environment_fields(values)
    library_sources = {
        f"{model.__module__}.{model.__name__}"
        for plugin in get_loaded_plugins()
        if (metadata := plugin.metadata)
        and metadata.type == "library"
        and (model := metadata.config) is not None
    }
    descriptors = visible_environment_fields(
        registry,
        library_sources=library_sources,
        core_keys=set(KNOWN_ENV_KEYS) | set(CORE_FIELDS),
        proxy_keys=PROXY_KEYS,
    )
    env_fields = {
        key: None if descriptors[key]["sensitive"] else values.get(key)
        for key in descriptors
    }
    custom_env = []
    for key, value in sorted(values.items(), key=lambda item: str(item[0]).casefold()):
        if str(key).upper() in registry:
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
                "descriptors": list(descriptors.values()),
                "source_file": str(env_path),
                "field_effects": {key: environment_effect(key) for key in descriptors},
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
        if payload.file == "env":
            _validate_environment_types(payload.content)
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
    target = environment_target(_path("env")) if file == "env" else _path(file)
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
        and not payload.unset_fields
    ):
        raise HTTPException(status_code=422, detail="没有可保存的配置内容。")
    try:
        content = payload.content
        if payload.content is not None and (
            payload.custom_operations or payload.unset_fields
        ):
            raise ValueError("custom_env_raw_conflict")
        if file != "env" and payload.custom_operations:
            raise ValueError("custom_env_file_invalid")
        if content is None:
            if file == "env" and set(payload.fields or {}) - set(
                _environment_fields(dict(dotenv_values(stream=StringIO(current))))
            ):
                raise ValueError("env_field_not_editable")
            content = (
                _update_env(current, payload.fields or {})
                if file == "env"
                else _update_simple(current, payload.fields or {})
            )
            if file == "env":
                content = _update_custom_env(content, payload.custom_operations or [])
        content = _unset_fields(content, file, payload.unset_fields)
        warnings = (
            _validate_env(content) if file == "env" else validate_simple_yaml(content)
        )
        if file == "env":
            before = {
                key.upper(): value
                for key, value in dotenv_values(stream=StringIO(current)).items()
            }
            after = {
                key.upper(): value
                for key, value in dotenv_values(stream=StringIO(content)).items()
            }
            _validate_environment_types(
                content,
                None
                if payload.content is not None
                else {key for key in after if before.get(key) != after[key]},
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
    except (Exception, asyncio.CancelledError) as error:
        if original is None:
            target.unlink(missing_ok=True)
        else:
            _write_transaction([(target, original)])
        if file == "simple":
            try:
                await reload_runtime_config(submit_restart=False)
            except Exception as rollback_error:
                logger.error("配置文件已恢复，但运行态恢复失败", e=rollback_error)
        if isinstance(error, asyncio.CancelledError):
            raise
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
