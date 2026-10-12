from __future__ import annotations

import asyncio
from copy import deepcopy
from dataclasses import dataclass, field
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

from zhenxun.configs.config import BotConfig, BotSetting
from zhenxun.services.log import logger
from zhenxun.services.network_proxy import PROXY_KEYS, proxy_runtime
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
CACHE_KEYS = {
    "CACHE_MODE",
    "REDIS_HOST",
    "REDIS_PASSWORD",
    "REDIS_PORT",
}
RESTART_KEYS = {
    "ALCONNA_USE_COMMAND_START",
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
    "WEBUI_HTTPS_ENABLED",
    "WEBUI_HTTP_MODE",
    "WEBUI_HTTP_REDIRECT_ENABLED",
    "WEBUI_HTTP_REDIRECT_PORT",
    "WEBUI_TLS_CERTFILE",
    "WEBUI_TLS_KEYFILE",
}
KNOWN_ENV_KEYS = DIRECT_KEYS | CACHE_KEYS | RESTART_KEYS

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
_CACHE_ATTRS = {
    "CACHE_MODE": "cache_mode",
    "REDIS_HOST": "redis_host",
    "REDIS_PASSWORD": "redis_password",
    "REDIS_PORT": "redis_port",
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
        str: in_place 直接生效，component_restart 重建缓存组件，
            worker_restart 重启后生效
    """
    normalized = key.upper()
    if normalized in DIRECT_KEYS:
        return "in_place"
    if normalized in CACHE_KEYS:
        return "component_restart"
    return "worker_restart"


def is_known_env_key(key: str) -> bool:
    return key.upper() in KNOWN_ENV_KEYS


def is_sensitive_env_key(key: str) -> bool:
    normalized = key.upper()
    return normalized == "SYSTEM_PROXY" or any(
        marker in normalized
        for marker in ("TOKEN", "SECRET", "PASSWORD", "COOKIE", "API_KEY", "APIKEY")
    )


@dataclass(slots=True)
class EnvironmentApplyResult:
    apply_mode: str
    changed_keys: list[str]
    restart_required: bool
    reason_codes: list[str]
    field_effects: dict[str, str]
    rolled_back: bool = False
    hot_reloaded: bool = False
    component_effects: dict[str, str] = field(default_factory=dict)
    affected_components: list[str] = field(default_factory=list)


class RuntimeEnvironmentError(RuntimeError):
    pass


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

    async def _reconfigure_cache(self, values: dict[str, str | None]) -> None:
        from zhenxun.services.cache import cache_config
        from zhenxun.services.cache.config import CacheMode
        from zhenxun.services.cache.runtime_sync import RuntimeCacheSync

        candidate = type(cache_config)(
            **{
                attr: self._typed_value(
                    cache_config, type(cache_config)(), attr, values.get(key)
                )
                for key, attr in _CACHE_ATTRS.items()
            }
        )
        if candidate.cache_mode == CacheMode.REDIS:
            if not candidate.redis_host:
                raise RuntimeEnvironmentError("redis_host_required")
            import redis.asyncio as redis_async
            from redis.asyncio.retry import Retry
            from redis.backoff import NoBackoff

            probe = redis_async.Redis(
                host=candidate.redis_host,
                port=candidate.redis_port or 6379,
                password=candidate.redis_password or None,
                socket_connect_timeout=2,
                socket_timeout=2,
                retry=Retry(NoBackoff(), 0),
            )
            try:
                await asyncio.wait_for(probe.ping(), 2)
            except Exception as error:
                raise RuntimeEnvironmentError("redis_probe_failed") from error
            finally:
                await asyncio.wait_for(probe.aclose(), 2)

        previous = {attr: getattr(cache_config, attr) for attr in _CACHE_ATTRS.values()}
        try:
            await RuntimeCacheSync.stop()
            for attr in _CACHE_ATTRS.values():
                setattr(cache_config, attr, getattr(candidate, attr))
            await asyncio.wait_for(RuntimeCacheSync.start(strict=True), 2)
        except BaseException:
            await RuntimeCacheSync.stop()
            for attr, value in previous.items():
                setattr(cache_config, attr, value)
            await asyncio.wait_for(RuntimeCacheSync.start(strict=True), 2)
            raise
        finally:
            import sys

            overview = sys.modules.get(
                "zhenxun.builtin_plugins.web_ui.api.tabs.dashboard.overview"
            )
            if overview is not None:
                await overview.invalidate_probes()

    async def _apply_runtime_values(
        self,
        values: dict[str, str | None],
        keys: set[str],
        *,
        rebuild_components: bool = True,
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
        if rebuild_components and keys & CACHE_KEYS:
            await self._reconfigure_cache(values)

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
                key.upper()
                for key in changed
                if environment_effect(key) != "worker_restart"
                and self.effective_values.get(key) != after.get(key)
            }
            pending_keys = set(self.pending_keys)
            for key in changed:
                if environment_effect(key) != "worker_restart":
                    continue
                if self.startup_values.get(key) != after.get(key):
                    pending_keys.add(key)
                else:
                    pending_keys.discard(key)

            runtime_before = dict(self.effective_values)
            component_operation = None
            component_candidates = hot_candidates & CACHE_KEYS
            direct_candidates = hot_candidates - component_candidates
            try:
                if direct_candidates:
                    await self._apply_runtime_values(after, direct_candidates)
                if component_candidates:
                    from zhenxun.services.lifecycle import lifecycle_kernel

                    component_operation = await lifecycle_kernel.restart_for_config(
                        component_candidates,
                        apply_change=lambda: self._apply_runtime_values(
                            after,
                            component_candidates,
                            rebuild_components=False,
                        ),
                        rollback_change=lambda: self._apply_runtime_values(
                            runtime_before,
                            component_candidates,
                            rebuild_components=False,
                        ),
                    )
                    if component_operation.apply_effect == "no_change":
                        await self._apply_runtime_values(after, component_candidates)
                    elif component_operation.apply_effect == "restart_pending":
                        pending_keys.update(component_candidates)
                        hot_candidates -= component_candidates
                        for key in component_candidates:
                            field_effects[key] = "worker_restart"
                    elif component_operation.apply_effect == "rolled_back":
                        raise RuntimeEnvironmentError("component_restart_rolled_back")
                if hot_candidates & PROXY_KEYS:
                    await proxy_runtime.apply(after)
                for key in hot_candidates - pending_keys:
                    self.effective_values[key] = after.get(key)
            except (Exception, asyncio.CancelledError):
                if (
                    component_operation
                    and component_operation.apply_effect == "component_restarted"
                ):
                    from zhenxun.services.lifecycle import lifecycle_kernel

                    await lifecycle_kernel.restart_for_config(
                        component_candidates,
                        apply_change=lambda: self._apply_runtime_values(
                            runtime_before,
                            component_candidates,
                            rebuild_components=False,
                        ),
                        rollback_change=lambda: self._apply_runtime_values(
                            after,
                            component_candidates,
                            rebuild_components=False,
                        ),
                    )
                await self._apply_runtime_values(runtime_before, direct_candidates)
                self.effective_values = runtime_before
                raise

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
            mode = (
                "restart_pending"
                if reasons
                else "component_restarted"
                if component_operation
                and component_operation.apply_effect == "component_restarted"
                else "config_reloaded"
            )
            logger.info(
                f"环境配置已协调: changed={len(changed)} "
                f"hot={len(hot_candidates - pending_keys)} "
                f"pending={len(pending_keys)}",
                "RuntimeEnvironment",
            )
            self.last_content = after_content
            return EnvironmentApplyResult(
                apply_mode=mode,
                changed_keys=sorted(changed),
                restart_required=bool(reasons),
                reason_codes=reasons,
                field_effects=field_effects,
                hot_reloaded=bool(hot_candidates - pending_keys),
                component_effects={
                    key: component_operation.apply_effect
                    for key in sorted(component_candidates)
                }
                if component_operation
                else {},
                affected_components=(
                    component_operation.affected_components
                    if component_operation
                    else []
                ),
            )


runtime_environment_manager = RuntimeEnvironmentManager()


__all__ = [
    "ENV_APPLY_SOURCE",
    "KNOWN_ENV_KEYS",
    "EnvironmentApplyResult",
    "RuntimeEnvironmentError",
    "environment_effect",
    "is_known_env_key",
    "is_sensitive_env_key",
    "normalize_env",
    "runtime_environment_manager",
]
