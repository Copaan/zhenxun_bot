"""Typed environment values and presentation metadata without plugin activation."""

from datetime import timedelta
import json
from typing import Any

from .config_schema import configuration_value

CORE_FIELDS = {
    "ALCONNA_USE_COMMAND_START": (bool, False, "bot"),
    "IMAGE_TO_BYTES": (bool, False, "bot"),
    "WEBUI_HTTPS_ENABLED": (bool, False, "access"),
    "WEBUI_HTTP_REDIRECT_ENABLED": (bool, False, "access"),
    "WEBUI_HTTP_MODE": (str, None, "access"),
    "WEBUI_HTTP_REDIRECT_PORT": (int, 80, "access"),
    "WEBUI_TLS_CERTFILE": (str, "", "access"),
    "WEBUI_TLS_KEYFILE": (str, "", "access"),
    "ONEBOT_ACCESS_TOKEN": (str, "", "adapters"),
    "ONEBOT_REVERSE_WS_HOST": (str, "", "adapters"),
}

NONEBOT_USER_FIELDS = {
    "HOST",
    "PORT",
    "LOG_LEVEL",
    "API_TIMEOUT",
    "SUPERUSERS",
    "NICKNAME",
    "COMMAND_START",
    "COMMAND_SEP",
    "SESSION_EXPIRE_TIMEOUT",
}
_INTERNAL_MODELS = {
    "nonebot_plugin_alconna",
    "nonebot_plugin_orm",
    "nonebot_plugin_uninfo",
    "nonebot_plugin_apscheduler",
}


def visible_environment_fields(
    descriptors: dict[str, dict[str, Any]],
    *,
    library_sources: set[str],
    core_keys: set[str],
    proxy_keys: set[str],
) -> dict[str, dict[str, Any]]:
    """Select user settings without changing the registry used to preserve files."""
    result = {}
    for key, field in descriptors.items():
        if key in proxy_keys or key in {"DRIVER", "ENVIRONMENT"}:
            continue
        sources = field.get("schema_sources", [])
        public_sources = [
            source
            for source in sources
            if source not in library_sources
            and source.split(".", 1)[0] not in _INTERNAL_MODELS
            and not source.startswith("nonebot.config.")
        ]
        if key in NONEBOT_USER_FIELDS or key in core_keys or public_sources:
            result[key] = field
    return result


def typed_environment_value(value: Any, annotation: Any) -> Any:
    """Decode dotenv values with the declared type, preserving significant text."""
    from zhenxun.utils.pydantic_compat import parse_as

    if annotation is str:
        return parse_as(annotation, value)
    decoded = value
    if isinstance(value, str):
        try:
            decoded = json.loads(value)
        except (TypeError, ValueError):
            decoded = value
    if annotation is timedelta and isinstance(decoded, int | float):
        return timedelta(seconds=float(decoded))
    return parse_as(annotation, decoded)


def comparable_value(value: Any) -> Any:
    """Use order-independent sets and second-based durations in public values."""
    if isinstance(value, timedelta):
        return value.total_seconds()
    if isinstance(value, set | frozenset):
        return sorted(
            (comparable_value(item) for item in value),
            key=lambda item: json.dumps(item, sort_keys=True),
        )
    if isinstance(value, dict):
        return {key: comparable_value(item) for key, item in value.items()}
    if isinstance(value, list | tuple):
        return [comparable_value(item) for item in value]
    return configuration_value(value)


def environment_category(key: str, sources: list[str]) -> str:
    if key in {"DB_URL", "CACHE_MODE"} or key.startswith("REDIS_"):
        return "storage"
    if key.startswith(("QQ_", "ONEBOT_")) or key == "QBOT_ID_DATA":
        return "adapters"
    if key in CORE_FIELDS:
        return CORE_FIELDS[key][2]
    if any(
        source.startswith(("nonebot.config.", "zhenxun.configs.", "zhenxun.services."))
        for source in sources
    ):
        return "runtime"
    return "plugins"
