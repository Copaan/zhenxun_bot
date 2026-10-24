from __future__ import annotations

import asyncio
from datetime import timedelta
import hashlib
from io import StringIO
import json
import os
from pathlib import Path
import re
from typing import Any, Literal

from dotenv import dotenv_values
from dotenv.parser import parse_stream
from fastapi import APIRouter, HTTPException, Response
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from pydantic.errors import PydanticUserError

from zhenxun.configs.config import Config
from zhenxun.configs.environment import environment_file, environment_target
from zhenxun.configs.webui_tls import (
    WebUITLSConfigError,
    settings_from_values,
    validate_http_compat_port,
    validate_webui_tls_settings,
)
from zhenxun.services.log import logger
from zhenxun.services.network_proxy import PROXY_KEYS, ProxyPolicy, proxy_runtime
from zhenxun.services.plugin_host import ApplyMode, RuntimeOperation
from zhenxun.services.runtime_config import (
    KNOWN_ENV_KEYS,
    environment_effect,
    reload_runtime_config,
    runtime_environment_manager,
)
from zhenxun.services.runtime_mutation import managed_mutation
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
from ....config_schema import (
    config_field_schema,
    configuration_value,
    inferred_schema,
    schema_for_type,
)
from ....config_validation import (
    ConfigurationValidationError,
    _yaml_parser,
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
from ....restart_service import (
    access_targets,
    preferred_access_urls,
    request_webui_restart,
)
from ....utils import authentication
from ...configure.persistence import _write_transaction

router = APIRouter(prefix="/configuration")

_ENV_FILE = Path(".env.dev")
_ENV_TEMPLATE = Path(".env.example")
_SIMPLE_FILE = Path("data/config.yaml")
_ENV_KEY_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_DELETE = object()
_PAGE_GROUPS = {"ai", "web-ui"}
_HIDDEN_ENV_CATEGORIES = {"adapters", "legacy", "plugins", "storage"}


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


def _validate_env(content: str) -> None:
    """校验 dotenv 语法、代理策略与 WebUI TLS 设置

    参数:
        content: dotenv 内容
    """
    validate_dotenv(content)
    values = dict(dotenv_values(stream=StringIO(content)))
    ProxyPolicy.from_values(values)
    qq_enabled = str(values.get("QQ_ADAPTER_LOAD") or "").casefold() == "true"
    qq_builtin = str(values.get("QQ_WEBHOOK_MODE") or "") == "builtin_https"
    try:
        qq_port = int(values.get("QQ_WEBHOOK_LISTEN_PORT") or 443)
    except (TypeError, ValueError):
        qq_port = 443
    qq_https_port = qq_port if qq_enabled and qq_builtin else None
    path = "WEBUI_HTTPS_ENABLED"
    try:
        settings = settings_from_values(values)
        validate_webui_tls_settings(settings, qq_https_port=qq_https_port)
        path = "WEBUI_HTTP_REDIRECT_PORT"
        validate_http_compat_port(settings, qq_https_port=qq_https_port)
    except WebUITLSConfigError as error:
        raise ConfigurationValidationError(
            [
                {
                    "code": "webui_tls_invalid",
                    "file": ".env.dev",
                    "path": path,
                    "line": None,
                    "column": None,
                    "severity": "error",
                    "message": str(error),
                }
            ]
        ) from error


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


def _env_values(content: str) -> dict[str, Any]:
    """解析 dotenv 内容，键名统一为大写

    参数:
        content: dotenv 内容

    返回:
        dict[str, Any]: 键值
    """
    return {
        key.upper(): value
        for key, value in dotenv_values(stream=StringIO(content)).items()
    }


def _group_owners(names: list[str]) -> tuple[list[dict[str, str]], str]:
    """读取配置组的已加载归属插件及分类

    参数:
        names: 注册过配置组的插件名

    返回:
        tuple[list[dict[str, str]], str]: 归属插件与分类（plugin / builtin / core）
    """
    from nonebot import get_loaded_plugins

    plugins = {plugin.name: plugin for plugin in get_loaded_plugins()}
    loaded = [plugin for name in names if (plugin := plugins.get(name))]
    owners = [
        {
            "module": plugin.name,
            "name": getattr(plugin.metadata, "name", None) or plugin.name,
        }
        for plugin in loaded
    ]
    if not loaded:
        return owners, "core"
    if all(
        plugin.module_name.startswith("zhenxun.builtin_plugins.") for plugin in loaded
    ):
        return owners, "builtin"
    return owners, "plugin"


async def _plugin_page_groups() -> set[str]:
    """获取可在插件列表的插件详情中编辑的配置组

    返回:
        set[str]: 配置组名
    """
    from zhenxun.models.plugin_info import PluginInfo

    from ..plugin_manage.data_source import ApiDataSource

    plugins = await PluginInfo.get_plugins(load_status=True, include_parked=True)
    return {
        group
        for plugin in plugins
        for group in ApiDataSource.config_modules(plugin.module)
    }


def simple_config_groups(modules: set[str] | None = None) -> dict[str, Any]:
    """读取 config.yaml 的配置组与文件版本

    参数:
        modules: 只返回这些配置组，为空时返回全部

    返回:
        dict[str, Any]: 配置组列表与文件版本
    """
    content = _read(_SIMPLE_FILE)
    groups = [
        group
        for group in _registered_groups(content)
        if (group["fields"] or not group["registered"])
        and group["module"].casefold() not in _PAGE_GROUPS
        and (modules is None or group["module"] in modules)
    ]
    return {"groups": groups, "revision": _revision(content)}


def _registered_groups(content: str) -> list[dict[str, Any]]:
    """读取插件配置组，合并 config.yaml 中的值与未注册配置

    参数:
        content: config.yaml 内容

    返回:
        list[dict[str, Any]]: 配置组
    """
    groups = []
    for module, group in Config.get_data().items():
        owners, category = _group_owners(group.plugins)
        fields = []
        for key, config in group.configs.items():
            fields.append(
                {
                    "key": key,
                    "help": config.help or "",
                    "value": configuration_value(config.value),
                    "default_value": configuration_value(config.default_value),
                    "schema": config_field_schema(
                        config.type, config.value, config.default_value
                    ),
                    "ui": model_dump(config.ui, exclude_none=True) if config.ui else {},
                }
            )
        fields.sort(key=lambda item: (item["ui"].get("order", 0), item["key"]))
        groups.append(
            {
                "module": module,
                "name": module
                if len(owners) > 1
                else group.name or (owners[0]["name"] if owners else module),
                "plugins": owners,
                "category": category,
                "fields": fields,
                "registered": True,
                "source_status": "loaded",
            }
        )
    stored = _yaml_parser().load(StringIO(content)) or {}
    if not isinstance(stored, dict):
        return groups
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
            if descriptor := existing.get(str(key).upper()):
                descriptor["file_value"] = configuration_value(value)
                descriptor["value"] = descriptor["file_value"]
                continue
            group["fields"].append(
                {
                    "key": str(key),
                    "help": "未注册配置，保存后需确认对应插件已加载。",
                    "value": configuration_value(value),
                    "default_value": None,
                    "schema": inferred_schema(value),
                    "ui": {},
                }
            )
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
    """构建环境配置字段登记表

    参数:
        values: dotenv 中的键值

    返回:
        dict[str, dict[str, Any]]: 键名到字段描述的映射
    """
    from nonebot import get_adapters, get_driver, get_loaded_plugins
    from pydantic_core import PydanticUndefined

    from zhenxun.configs.config import BotConfig, BotSetting
    from zhenxun.services.cache import Config as CacheConfig
    from zhenxun.services.cache import cache_config
    from zhenxun.utils.pydantic_compat import model_json_schema

    values = {key.upper(): value for key, value in values.items()}
    schemas = {}
    annotations = {}
    defaults = {}
    declarations = {}
    conflicts = set()
    owner_names = {
        plugin.metadata.config: plugin.metadata.name or plugin.id_
        for plugin in get_loaded_plugins()
        if plugin.metadata and plugin.metadata.config
    }
    labels = {}
    attribute_names = {}
    for model in _environment_models():
        source = f"{model.__module__}.{model.__name__}"
        fields = dict(
            getattr(model, "model_fields", None) or getattr(model, "__fields__", {})
        )
        try:
            root = model_json_schema(model, by_alias=False)
        except Exception:
            root = {
                "properties": {
                    name: schema_for_type(getattr(field, "annotation", Any))
                    for name, field in fields.items()
                }
            }
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
            key = name.upper()
            field = fields.get(name)
            if field is not None:
                annotations.setdefault(
                    key,
                    getattr(field, "outer_type_", None)
                    or getattr(field, "annotation", Any),
                )
                if "default" in schema:
                    defaults.setdefault(key, schema["default"])
                elif not getattr(field, "default_factory", None):
                    default = getattr(field, "default", PydanticUndefined)
                    if (
                        default is not PydanticUndefined
                        and type(default).__name__ != "UndefinedType"
                    ):
                        defaults.setdefault(key, default)
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
                conflicts.add(key)
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
    runtime = get_driver().config
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
    for key in sorted(set(KNOWN_ENV_KEYS) | set(schemas)):
        schema = {} if key in conflicts else schemas.get(key, {}).get("schema", {})
        annotation = annotations.get(key, Any)
        default = defaults.get(key, schema.get("default"))
        if default is not None:
            try:
                default = typed_environment_value(default, annotation)
            except (ValueError, TypeError, PydanticUserError):
                pass
        parsed = values.get(key)
        valid = True
        if key in values:
            try:
                parsed = typed_environment_value(parsed, annotation)
            except (ValueError, TypeError, PydanticUserError):
                valid = False
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
            "schema": schema,
            "schema_sources": declarations.get(key, []),
            "source_labels": labels.get(key, []),
            "readonly_reason": (
                "字段类型声明冲突：" + "、".join(declarations[key])
                if key in conflicts
                else None
            ),
            "value": comparable_value(parsed),
            "default_value": comparable_value(default),
            "configured": key in values,
            "effective_value": comparable_value(effective) if available else None,
            "runtime_available": available,
            "apply_state": apply_state,
            "category": category,
            "registered": key in annotations or key in KNOWN_ENV_KEYS,
            "overridden": overridden,
            "external_environment_present": external_present,
            "apply_effect": environment_effect(key),
        }
    return result


def _validate_environment_types(
    values: dict[str, Any],
    changed_keys: set[str] | None = None,
) -> None:
    """按声明类型校验环境配置

    参数:
        values: dotenv 中的键值，键名为大写
        changed_keys: 只校验这些键，为 None 时校验全部
    """
    from zhenxun.utils.pydantic_compat import parse_as

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
            annotation = getattr(field, "outer_type_", field.annotation)
            try:
                parse_as(annotation, descriptors[key]["value"])
            except Exception as error:
                details = error.errors() if hasattr(error, "errors") else [{}]
                issues.extend(
                    {
                        "code": "env_value_invalid",
                        "file": str(_path("env")),
                        "path": ".".join([key, *map(str, detail.get("loc", ()))]),
                        "message": detail.get("msg", "值不符合声明类型"),
                        "severity": "error",
                    }
                    for detail in details
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


def _rewrite_env(content: str, changes: dict[str, Any]) -> str:
    """按键名改写 dotenv 内容，保留其余行、注释与格式

    参数:
        content: dotenv 内容
        changes: 键名到新值的映射，值为 _DELETE 时删除该键

    返回:
        str: 改写后的内容
    """
    pending = {key.casefold(): (key, value) for key, value in changes.items()}
    output: list[str] = []
    for binding in parse_stream(StringIO(content)):
        item = pending.pop(binding.key.casefold(), None) if binding.key else None
        if item is None:
            output.append(binding.original.string)
        elif item[1] is not _DELETE:
            output.append(_replace_env_binding(binding, item[1]))
    additions = [item for item in pending.values() if item[1] is not _DELETE]
    if additions:
        if output and not output[-1].endswith("\n"):
            output.append("\n")
        output.extend(f"{key} = {_env_encode(value)}\n" for key, value in additions)
    return "".join(output)


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
    return _rewrite_env(content, remaining)


def _form_env_keys(registry: dict[str, dict[str, Any]]) -> set[str]:
    """获取由配置表单或机器人接入页面管理的环境变量

    插件声明的环境变量不在表单中展示，按自定义变量维护。

    参数:
        registry: 环境配置字段登记表

    返回:
        set[str]: 键名
    """
    return {key for key, field in registry.items() if field["category"] != "plugins"}


def _custom_env_allowed(key: str, managed: set[str]) -> bool:
    """判断变量能否作为自定义变量展示和修改

    适配器变量即使未注册也可能包含凭据，统一由机器人接入页面或配置原文维护。

    参数:
        key: 变量名
        managed: 由表单管理的键名

    返回:
        bool: 是否允许
    """
    key = key.upper()
    return key not in managed and environment_category(key, []) != "adapters"


def _update_custom_env(
    content: str, operations: list[CustomEnvOperation], registered: set[str]
) -> str:
    """应用自定义环境变量的新增、替换与删除

    参数:
        content: dotenv 内容
        operations: 变量操作
        registered: 由配置表单管理的键名

    返回:
        str: 改写后的内容
    """
    changes: dict[str, Any] = {}
    for operation in operations:
        key = operation.key.strip()
        if not _ENV_KEY_PATTERN.fullmatch(key):
            raise ValueError("custom_env_key_invalid")
        if not _custom_env_allowed(key, registered):
            raise ValueError("custom_env_key_managed")
        if any(key.casefold() == old.casefold() for old in changes):
            raise ValueError("custom_env_key_duplicate")
        if operation.operation == "set" and operation.value is None:
            raise ValueError("custom_env_value_required")
        changes[key] = operation.value if operation.operation == "set" else _DELETE
    return _rewrite_env(content, changes)


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
        if str(module).casefold() in _PAGE_GROUPS:
            raise ValueError("config_group_not_editable")
        if not isinstance(values, dict):
            raise ValueError("yaml_group_mapping_required")
        group = data.setdefault(module, {})
        if not isinstance(group, dict):
            raise ValueError("yaml_group_mapping_required")
        registered = Config.get_data().get(module)
        for key, value in values.items():
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
        if any(
            len(path) != 1 or not _ENV_KEY_PATTERN.fullmatch(path[0]) for path in paths
        ):
            raise ValueError("invalid_unset_path")
        return _rewrite_env(content, {path[0]: _DELETE for path in paths})
    parser = _yaml_parser()
    data = parser.load(StringIO(content)) or {}
    for path in paths:
        if len(path) != 2:
            raise ValueError("invalid_unset_path")
        if path[0].casefold() in _PAGE_GROUPS:
            raise ValueError("config_group_not_editable")
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
        "config_group_not_editable": "该配置组请在 AI 配置或账号安全中修改。",
        "custom_env_key_invalid": "自定义环境变量名称不合法。",
        "custom_env_key_managed": "该变量由配置表单或机器人接入页面管理。",
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
    managed = _form_env_keys(registry)
    custom_env = [
        {"key": str(key), "value": value}
        for key, value in sorted(values.items(), key=lambda item: item[0].casefold())
        if _custom_env_allowed(key, managed)
    ]
    simple = simple_config_groups()
    plugin_groups = await _plugin_page_groups()
    simple["groups"] = [
        group for group in simple["groups"] if group["module"] not in plugin_groups
    ]
    return Result.ok(
        {
            "env": {
                "descriptors": [
                    field
                    for field in descriptors.values()
                    if field["category"] not in _HIDDEN_ENV_CATEGORIES
                ],
                "source_file": str(env_path),
                "custom_env": custom_env,
                "revision": _revision(env_content),
            },
            "simple": simple,
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
    return Result.ok({"content": content, "revision": _revision(content)})


@router.post(
    "/validate",
    dependencies=[authentication()],
    response_model=Result,
    response_class=JSONResponse,
)
async def validate_configuration(payload: ConfigurationValidation) -> Result:
    try:
        if payload.file == "env":
            _validate_env(payload.content)
            _validate_environment_types(_env_values(payload.content))
        else:
            validate_simple_yaml(payload.content)
    except Exception as error:
        raise _validation_error(payload.file, error) from error
    return Result.ok()


def _render_update(current: str, file: str, payload: ConfigurationFileUpdate) -> str:
    """按提交内容生成新的配置文件内容并校验

    参数:
        current: 当前文件内容
        file: 配置文件类型
        payload: 提交内容

    返回:
        str: 新的文件内容
    """
    if payload.content is not None and (
        payload.custom_operations or payload.unset_fields
    ):
        raise ValueError("custom_env_raw_conflict")
    if file != "env":
        if payload.custom_operations:
            raise ValueError("custom_env_file_invalid")
        content = payload.content
        if content is None:
            content = _update_simple(current, payload.fields or {})
        content = _unset_fields(content, file, payload.unset_fields)
        validate_simple_yaml(content)
        return content
    previous = _env_values(current)
    content = payload.content
    if content is None:
        registry = _environment_fields(previous)
        editable = {
            key
            for key, field in registry.items()
            if field["category"] not in _HIDDEN_ENV_CATEGORIES
        }
        if set(payload.fields or {}) - editable:
            raise ValueError("env_field_not_editable")
        content = _update_env(current, payload.fields or {})
        content = _update_custom_env(
            content, payload.custom_operations or [], _form_env_keys(registry)
        )
    content = _unset_fields(content, file, payload.unset_fields)
    _validate_env(content)
    values = _env_values(content)
    _validate_environment_types(
        values,
        None
        if payload.content is not None
        else {key for key in values if previous.get(key) != values[key]},
    )
    return content


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
    current_path = _path(file)
    target = environment_target(current_path) if file == "env" else current_path
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
        content = _render_update(current, file, payload)
    except Exception as error:
        raise _validation_error(file, error) from error

    values = dict(dotenv_values(stream=StringIO(content))) if file == "env" else {}
    if file == "env":
        previous = dict(dotenv_values(stream=StringIO(current)))
        if any(values.get(key) != previous.get(key) for key in PROXY_KEYS):
            await proxy_runtime.prepare(ProxyPolicy.from_values(values))
    original = target.read_bytes() if target.exists() else None
    operation: RuntimeOperation | None = None
    env_operation = None
    try:
        content_bytes = content.encode("utf-8")
        if original != content_bytes:
            _write_transaction([(target, content_bytes)])
            if file == "simple":
                operation = await reload_runtime_config(submit_restart=False)
                if operation.mode is ApplyMode.FAILED:
                    raise RuntimeError(
                        operation.reason or "config_consumer_reload_failed"
                    )
            else:
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

    access_urls: list[str] = []
    if file == "env":
        apply_mode = env_operation.apply_mode if env_operation else APPLY_NO_CHANGE
        restart_required = bool(env_operation and env_operation.restart_required)
        changed_keys = env_operation.changed_keys if env_operation else []
        reason_codes = env_operation.reason_codes if env_operation else []
        launcher_managed = bool(os.getenv("ZHENXUN_LAUNCHER_PID"))
        try:
            port = int(values.get("PORT") or 8080)
        except (TypeError, ValueError):
            port = 8080
        access_urls = preferred_access_urls(
            str(values.get("HOST") or "0.0.0.0"),
            port,
            settings=settings_from_values(values),
            http_available=True,
        )
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
        )
    if restart_required and launcher_managed:
        issue_restart_ticket("webui.settings", ttl_seconds=10 * 60)
    data = apply_result_data(
        apply_mode=apply_mode,
        changed_keys=changed_keys,
        restart_required=restart_required,
        reason_codes=reason_codes,
        access_urls=access_urls,
        access_targets=access_targets(access_urls),
        revision=_revision(content),
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
