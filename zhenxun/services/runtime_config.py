from __future__ import annotations

import asyncio
from collections.abc import Callable
import contextlib
from copy import deepcopy
from dataclasses import dataclass
from datetime import timedelta
from io import StringIO
import json
import os
from pathlib import Path
from typing import Any

from dotenv import dotenv_values
import nonebot
from nonebot.compat import model_fields, type_validate_python
from nonebot.config import Config as NoneBotConfig

from zhenxun.configs.config import BotConfig, BotSetting, Config
from zhenxun.services.ai.config import get_llm_config
from zhenxun.services.ai.llm.manager import clear_all_cache
from zhenxun.services.log import logger
from zhenxun.services.network_proxy import PROXY_KEYS, proxy_runtime
from zhenxun.services.plugin_host import ApplyMode, RuntimeOperation
from zhenxun.utils._restart_utils import (
    clear_restart_pending,
    clear_restart_ticket_if_idle,
    mark_restart_pending,
)

ENV_APPLY_SOURCE = "webui.environment"

DIRECT_KEYS = {
    "API_TIMEOUT",
    "IMAGE_TO_BYTES",
    "LOG_LEVEL",
    "NICKNAME",
    "PLATFORM_SUPERUSERS",
    "SELF_NICKNAME",
    "SESSION_EXPIRE_TIMEOUT",
    "SUPERUSERS",
}
DIRECT_KEYS |= PROXY_KEYS
RESTART_KEYS = {
    "ALCONNA_USE_COMMAND_START",
    "CACHE_MODE",
    "COMMAND_SEP",
    "COMMAND_START",
    "DB_URL",
    "DRIVER",
    "EXT_PATH",
    "HOST",
    "ONEBOT_ACCESS_TOKEN",
    "ONEBOT_REVERSE_WS_HOST",
    "PORT",
    "QBOT_ID_DATA",
    "QQ_ADAPTER_LOAD",
    "QQ_BOTS",
    "QQ_WEBHOOK_LISTEN_HOST",
    "QQ_WEBHOOK_LISTEN_PORT",
    "QQ_WEBHOOK_MODE",
    "QQ_WEBHOOK_PUBLIC_BASE_URL",
    "QQ_WEBHOOK_TLS_CERTFILE",
    "QQ_WEBHOOK_TLS_KEYFILE",
    "REDIS_HOST",
    "REDIS_PASSWORD",
    "REDIS_PORT",
    "WEBUI_HTTPS_ENABLED",
    "WEBUI_HTTP_MODE",
    "WEBUI_HTTP_REDIRECT_ENABLED",
    "WEBUI_HTTP_REDIRECT_PORT",
    "WEBUI_TLS_CERTFILE",
    "WEBUI_TLS_KEYFILE",
}
KNOWN_ENV_KEYS = DIRECT_KEYS | RESTART_KEYS

_CORE_ATTRS = {
    "API_TIMEOUT": "api_timeout",
    "IMAGE_TO_BYTES": "image_to_bytes",
    "LOG_LEVEL": "log_level",
    "NICKNAME": "nickname",
    "SESSION_EXPIRE_TIMEOUT": "session_expire_timeout",
    "SUPERUSERS": "superusers",
}
_BOT_ATTRS = {
    "PLATFORM_SUPERUSERS": "platform_superusers",
    "SELF_NICKNAME": "self_nickname",
    "SYSTEM_PROXY": "system_proxy",
    "NETWORK_PROXY_MODE": "network_proxy_mode",
    "NETWORK_PROXY_PLUGINS": "network_proxy_plugins",
    "NETWORK_PROXY_CORE_ENABLED": "network_proxy_core_enabled",
    "NETWORK_PROXY_BYPASS": "network_proxy_bypass",
}
_CORE_EXTRA_DEFAULTS = {"image_to_bytes": False}


def normalize_env(content: str) -> dict[str, str | None]:
    return {
        str(key): None if value is None else str(value)
        for key, value in dotenv_values(stream=StringIO(content)).items()
    }


def _canonical_values(values: dict[str, str | None]) -> dict[str, str | None]:
    return {
        key.upper() if key.upper() in KNOWN_ENV_KEYS else key: value
        for key, value in values.items()
    }


def _env_source() -> Path:
    from zhenxun.configs.environment import environment_file

    return environment_file()


def _read_startup_env() -> dict[str, str | None]:
    try:
        return _canonical_values(
            normalize_env(_env_source().read_text(encoding="utf-8"))
        )
    except OSError:
        return {}


_STARTUP_VALUES = _read_startup_env()


def environment_effect(key: str) -> str:
    """环境配置项的生效方式

    参数:
        key: 配置项

    返回:
        str: in_place 直接生效，worker_restart 重启后生效
    """
    return "in_place" if key.upper() in DIRECT_KEYS else "worker_restart"


def is_known_env_key(key: str) -> bool:
    return key.upper() in KNOWN_ENV_KEYS


@dataclass(slots=True)
class EnvironmentApplyResult:
    apply_mode: str
    changed_keys: list[str]
    restart_required: bool
    reason_codes: list[str]
    field_effects: dict[str, str]
    rolled_back: bool = False
    hot_reloaded: bool = False


class RuntimeEnvironmentManager:
    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self.external_values = dict(os.environ)
        self.startup_values = dict(_STARTUP_VALUES)
        self.effective_values = dict(_STARTUP_VALUES)
        self._core_defaults = NoneBotConfig(_env_file=None)
        self._bot_defaults = BotSetting()
        self.pending_keys: set[str] = set()
        try:
            self.last_content = _env_source().read_text(encoding="utf-8")
        except OSError:
            self.last_content = ""

    @staticmethod
    def _decode(raw: str | None) -> Any:
        if raw is None:
            return None
        value = raw.strip()
        try:
            return json.loads(value)
        except (TypeError, ValueError, json.JSONDecodeError):
            lowered = value.casefold()
            if lowered in {"true", "yes", "on"}:
                return True
            if lowered in {"false", "no", "off"}:
                return False
            return raw

    def _typed_value(
        self,
        model: Any,
        defaults: Any,
        attr: str,
        raw: str | None,
    ) -> Any:
        field_info = next(
            (field for field in model_fields(type(model)) if field.name == attr), None
        )
        if raw is None:
            if field_info is not None:
                get_default = getattr(field_info, "get_default", None)
                if callable(get_default):
                    try:
                        return deepcopy(get_default(call_default_factory=True))
                    except TypeError:
                        return deepcopy(get_default())
            if attr in _CORE_EXTRA_DEFAULTS:
                return deepcopy(_CORE_EXTRA_DEFAULTS[attr])
            return deepcopy(getattr(defaults, attr, None))
        decoded = self._decode(raw)
        if field_info is None:
            return decoded
        annotation = getattr(field_info, "annotation", Any)
        if annotation is timedelta and isinstance(decoded, int | float):
            return timedelta(seconds=float(decoded))
        return type_validate_python(annotation, decoded)

    @staticmethod
    def _set_process_env(values: dict[str, str | None], keys: set[str]) -> None:
        for key in keys:
            value = values.get(key)
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value

    def _apply_runtime_values(
        self, values: dict[str, str | None], keys: set[str]
    ) -> None:
        driver_config = nonebot.get_driver().config
        for key in keys:
            if attr := _CORE_ATTRS.get(key):
                setattr(
                    driver_config,
                    attr,
                    self._typed_value(
                        driver_config,
                        self._core_defaults,
                        attr,
                        values.get(key),
                    ),
                )
            if attr := _BOT_ATTRS.get(key):
                setattr(
                    BotConfig,
                    attr,
                    self._typed_value(
                        BotConfig,
                        self._bot_defaults,
                        attr,
                        values.get(key),
                    ),
                )

        self._set_process_env(values, keys)
        if "LOG_LEVEL" in keys:
            from zhenxun.services.log import reload_log_level

            reload_log_level(getattr(driver_config, "log_level", "INFO"))

    async def apply(
        self,
        before_content: str,
        after_content: str,
        *,
        source: str = ENV_APPLY_SOURCE,
        submit_restart: bool = False,
    ) -> EnvironmentApplyResult:
        before = _canonical_values(normalize_env(before_content))
        after = _canonical_values(normalize_env(after_content))
        changed = {
            key
            for key in before.keys() | after.keys()
            if before.get(key) != after.get(key)
        }
        field_effects = {key: environment_effect(key) for key in sorted(changed)}
        if not changed:
            return EnvironmentApplyResult("no_change", [], False, [], field_effects)

        async with self._lock:
            hot_candidates = {
                key
                for key in changed
                if environment_effect(key) == "in_place"
                and self.effective_values.get(key) != after.get(key)
            }
            pending_keys = set(self.pending_keys)
            for key in changed:
                if environment_effect(key) == "in_place":
                    continue
                if self.startup_values.get(key) != after.get(key):
                    pending_keys.add(key)
                else:
                    pending_keys.discard(key)

            runtime_before = dict(self.effective_values)
            try:
                self._apply_runtime_values(after, hot_candidates)
                if hot_candidates & PROXY_KEYS:
                    await proxy_runtime.apply(after)
            except (Exception, asyncio.CancelledError):
                self._apply_runtime_values(runtime_before, hot_candidates)
                raise
            for key in hot_candidates:
                self.effective_values[key] = after.get(key)

            reasons = [f"environment:{key}" for key in sorted(pending_keys)]
            if reasons:
                mark_restart_pending(source, reasons)
            else:
                clear_restart_pending(source)
                clear_restart_ticket_if_idle()
            self.pending_keys = pending_keys

            if reasons and submit_restart:
                from zhenxun.services.plugin_host import plugin_host

                await plugin_host.request_restart(set(), "environment_changed")
            logger.info(
                f"环境配置已协调: changed={len(changed)} "
                f"hot={len(hot_candidates)} "
                f"pending={len(pending_keys)}",
                "RuntimeEnvironment",
            )
            self.last_content = after_content
            return EnvironmentApplyResult(
                apply_mode="restart_pending" if reasons else "config_reloaded",
                changed_keys=sorted(changed),
                restart_required=bool(reasons),
                reason_codes=reasons,
                field_effects=field_effects,
                hot_reloaded=bool(hot_candidates),
            )


runtime_environment_manager = RuntimeEnvironmentManager()


ConfigDependency = tuple[str, str]
_STARTUP_SIMPLE_DATA = deepcopy(Config._simple_data)


class RuntimeConfigReloadError(RuntimeError):
    """Raised when the runtime cannot commit a validated config generation."""


def _flatten_simple_config(data: Any) -> dict[ConfigDependency, Any]:
    flattened: dict[ConfigDependency, Any] = {}
    if not isinstance(data, dict):
        return flattened
    for raw_module, raw_values in data.items():
        module = str(raw_module)
        if not isinstance(raw_values, dict):
            flattened[(module, "*")] = raw_values
            continue
        for raw_key, value in raw_values.items():
            flattened[(module, str(raw_key).upper())] = value
    return flattened


def changed_config_dependencies(before: Any, after: Any) -> set[ConfigDependency]:
    previous = _flatten_simple_config(before)
    current = _flatten_simple_config(after)
    return {
        dependency
        for dependency in previous.keys() | current.keys()
        if previous.get(dependency, _MISSING) != current.get(dependency, _MISSING)
    }


_MISSING = object()


async def reload_runtime_config(
    reschedule: Callable[[], None] | None = None,
    *,
    submit_restart: bool = True,
    previous_simple_data: Any | None = None,
    reload_consumers: bool = True,
) -> RuntimeOperation:
    """Reload config.yaml and the runtime state derived from it."""
    from zhenxun.builtin_plugins.init.manager import manager
    from zhenxun.services.plugin_host import plugin_host

    async def refresh_derived_state() -> None:
        get_llm_config.cache_clear()
        clear_all_cache()
        manager.init()
        await manager.load_to_db()
        if reschedule is not None:
            with contextlib.suppress(Exception):
                reschedule()

    async def apply_new_runtime() -> None:
        Config.reload(strict=True)
        await refresh_derived_state()

    async def apply_previous_runtime() -> None:
        Config.restore_runtime_values(snapshot)
        await refresh_derived_state()

    snapshot = Config.snapshot_runtime_values()
    component_result = None
    lifecycle_keys: set[str] = set()
    before = deepcopy(
        Config._simple_data if previous_simple_data is None else previous_simple_data
    )
    try:
        Config.reload(strict=True)
        changed_dependencies = changed_config_dependencies(before, Config._simple_data)
        startup_changed_dependencies = changed_config_dependencies(
            _STARTUP_SIMPLE_DATA, Config._simple_data
        )
        changed_paths = sorted(
            f"{module}.{key}" for module, key in changed_dependencies
        )
        if changed_paths:
            logger.debug(f"运行时配置变更键: {', '.join(changed_paths)}")
        else:
            logger.debug("config.yaml 语义内容未变化，跳过配置消费者重载")
        Config.restore_runtime_values(snapshot)
        lifecycle_keys = {
            value
            for module, key in changed_dependencies
            for value in (module.upper(), f"{module}.{key}".upper())
        }
        from zhenxun.services.lifecycle import lifecycle_kernel

        component_result = await lifecycle_kernel.restart_for_config(
            lifecycle_keys,
            apply_change=apply_new_runtime,
            rollback_change=apply_previous_runtime,
        )
        if component_result.apply_effect == "no_change":
            await apply_new_runtime()
        elif component_result.apply_effect == "restart_pending":
            operation = await plugin_host.request_restart(
                set(component_result.affected_components),
                "component_rollback_failed",
                submit_launcher=submit_restart,
            )
            operation.config_keys = changed_paths
            return operation
        elif component_result.apply_effect == "rolled_back":
            raise RuntimeConfigReloadError("component_restart_rolled_back")
        operation = None
        if reload_consumers:
            operation = await plugin_host.reload_config_consumers(
                changed_dependencies,
                restart_dependencies=startup_changed_dependencies,
                submit_restart=submit_restart,
            )
        if operation is not None and operation.mode is ApplyMode.FAILED:
            raise RuntimeConfigReloadError(
                operation.reason or "config_consumer_reload_failed"
            )
        if operation is None:
            operation = RuntimeOperation(
                ApplyMode.COMPONENT_RESTARTED
                if component_result.apply_effect == "component_restarted"
                else ApplyMode.CONFIG_RELOADED,
                "completed",
                changed_paths,
                generation=plugin_host.generation,
            )
            plugin_host.last_operation = operation
        operation.config_keys = changed_paths
        return operation
    except (Exception, asyncio.CancelledError):
        try:
            if (
                component_result is not None
                and component_result.apply_effect == "component_restarted"
            ):
                from zhenxun.services.lifecycle import lifecycle_kernel

                await lifecycle_kernel.restart_for_config(
                    lifecycle_keys,
                    apply_change=apply_previous_runtime,
                    rollback_change=apply_new_runtime,
                )
            else:
                Config.restore_runtime_values(snapshot)
        except Exception as component_rollback_error:
            logger.error("配置组件回滚失败，需要重启worker", e=component_rollback_error)
        try:
            Config.save()
            await refresh_derived_state()
        except Exception as rollback_error:
            logger.error("运行时配置回滚后的派生状态刷新失败", e=rollback_error)
        raise


__all__ = [
    "ENV_APPLY_SOURCE",
    "KNOWN_ENV_KEYS",
    "EnvironmentApplyResult",
    "RuntimeConfigReloadError",
    "changed_config_dependencies",
    "environment_effect",
    "is_known_env_key",
    "normalize_env",
    "reload_runtime_config",
    "runtime_environment_manager",
]
