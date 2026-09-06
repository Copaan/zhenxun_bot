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
from zhenxun.services.runtime_reload.models import ApplyMode
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
COMPONENT_KEYS = {"RUNTIME_WATCH_MODE", "SYSTEM_PROXY"}
COMMAND_KEYS = {"COMMAND_START", "COMMAND_SEP", "ALCONNA_USE_COMMAND_START"}
CACHE_KEYS = {
    "CACHE_MODE",
    "REDIS_EXPIRE",
    "REDIS_HOST",
    "REDIS_PASSWORD",
    "REDIS_PORT",
}
EXT_PATH_KEYS = {"EXT_PATH"}
RESTART_KEYS = {
    "DB_URL",
    "DRIVER",
    "HOST",
    "ONEBOT_ACCESS_TOKEN",
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
KNOWN_ENV_KEYS = (
    DIRECT_KEYS
    | COMPONENT_KEYS
    | COMMAND_KEYS
    | CACHE_KEYS
    | EXT_PATH_KEYS
    | RESTART_KEYS
)

_CORE_ATTRS = {
    "API_TIMEOUT": "api_timeout",
    "ALCONNA_USE_COMMAND_START": "alconna_use_command_start",
    "COMMAND_SEP": "command_sep",
    "COMMAND_START": "command_start",
    "IMAGE_TO_BYTES": "image_to_bytes",
    "LOG_LEVEL": "log_level",
    "NICKNAME": "nickname",
    "SESSION_EXPIRE_TIMEOUT": "session_expire_timeout",
    "SUPERUSERS": "superusers",
}
_BOT_ATTRS = {
    "EXT_PATH": "ext_path",
    "PLATFORM_SUPERUSERS": "platform_superusers",
    "SELF_NICKNAME": "self_nickname",
    "SYSTEM_PROXY": "system_proxy",
    "RUNTIME_WATCH_MODE": "runtime_watch_mode",
}
_CACHE_ATTRS = {
    "CACHE_MODE": "cache_mode",
    "REDIS_EXPIRE": "redis_expire",
    "REDIS_HOST": "redis_host",
    "REDIS_PASSWORD": "redis_password",
    "REDIS_PORT": "redis_port",
}
_CORE_EXTRA_DEFAULTS = {
    "alconna_use_command_start": False,
    "image_to_bytes": False,
}


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
    return Path(".env.dev") if Path(".env.dev").exists() else Path(".env")


def _read_startup_env() -> dict[str, str | None]:
    try:
        return _canonical_values(
            normalize_env(_env_source().read_text(encoding="utf-8"))
        )
    except OSError:
        return {}


_STARTUP_VALUES = _read_startup_env()


def environment_effect(key: str) -> str:
    normalized = key.upper()
    if normalized in DIRECT_KEYS:
        return "in_place"
    if normalized in COMPONENT_KEYS or normalized in CACHE_KEYS:
        return "component_restart"
    if normalized in COMMAND_KEYS or normalized in EXT_PATH_KEYS:
        return "plugin_reactivate"
    return "worker_restart"


def is_known_env_key(key: str) -> bool:
    return key.upper() in KNOWN_ENV_KEYS


def is_sensitive_env_key(key: str) -> bool:
    normalized = key.upper()
    return any(
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
    changed_plugins: list[str] = field(default_factory=list)
    component_effects: dict[str, str] = field(default_factory=dict)
    affected_components: list[str] = field(default_factory=list)


class RuntimeEnvironmentError(RuntimeError):
    pass


class RuntimeEnvironmentManager:
    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self.startup_values = dict(_STARTUP_VALUES)
        self.effective_values = dict(_STARTUP_VALUES)
        self._core_defaults = NoneBotConfig(_env_file=None)
        self._bot_defaults = BotSetting()
        self.pending_keys: set[str] = set()
        try:
            self.last_content = _env_source().read_text(encoding="utf-8")
        except OSError:
            self.last_content = ""
        self.last_contents = {_env_source().resolve(): self.last_content}

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
        from zhenxun.services.cache import CacheRoot, cache_config
        from zhenxun.services.cache.config import CacheMode
        from zhenxun.services.cache.runtime_cache import RuntimeCacheSync

        for key, attr in _CACHE_ATTRS.items():
            value = self._typed_value(
                cache_config,
                type(cache_config)(),
                attr,
                values.get(key),
            )
            setattr(cache_config, attr, value)

        if cache_config.cache_mode == CacheMode.REDIS:
            if not cache_config.redis_host:
                raise RuntimeEnvironmentError("redis_host_required")
            try:
                import redis.asyncio as redis_async

                probe = redis_async.Redis(
                    host=cache_config.redis_host,
                    port=cache_config.redis_port or 6379,
                    password=cache_config.redis_password or None,
                    socket_connect_timeout=3,
                    socket_timeout=3,
                )
                try:
                    await probe.ping()
                finally:
                    await probe.aclose()
            except Exception as error:
                raise RuntimeEnvironmentError("redis_probe_failed") from error

        await RuntimeCacheSync.stop()
        await CacheRoot.close()
        CacheRoot.enabled = cache_config.cache_mode == CacheMode.REDIS
        await RuntimeCacheSync.start()

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
        if rebuild_components and "SYSTEM_PROXY" in keys:
            from zhenxun.utils.http_utils import reload_system_proxy

            await reload_system_proxy(BotConfig.system_proxy)
        if rebuild_components and keys & CACHE_KEYS:
            await self._reconfigure_cache(values)

    @staticmethod
    def _path_set(value: Any) -> set[Path]:
        if not isinstance(value, list | tuple | set):
            return set()
        return {Path(str(item)).resolve() for item in value if str(item).strip()}

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

            from zhenxun.services.runtime_reload import plugin_runtime_manager

            pending_consumer_keys = {
                key
                for key in (hot_candidates | self.pending_keys)
                if key in DIRECT_KEYS | COMPONENT_KEYS | COMMAND_KEYS
            }
            _, unsafe_consumers = plugin_runtime_manager.environment_consumers(
                pending_consumer_keys
            )
            if unsafe_consumers:
                unsafe_keys = {
                    key
                    for key in pending_consumer_keys
                    if any(
                        key in plugin_runtime_manager.units[plugin_id].env_dependencies
                        for plugin_id in unsafe_consumers
                    )
                }
                hot_candidates -= unsafe_keys
                pending_keys.update(
                    key
                    for key in unsafe_keys
                    if self.startup_values.get(key) != after.get(key)
                )
                for key in unsafe_keys:
                    field_effects[key] = "worker_restart"
                    if self.startup_values.get(key) == after.get(key):
                        pending_keys.discard(key)

            runtime_before = dict(self.effective_values)
            previous_ext_paths = self._path_set(BotConfig.ext_path)
            hot_operation = None
            ext_operation = None
            component_operation = None
            component_candidates = hot_candidates & (COMPONENT_KEYS | CACHE_KEYS)
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
                if "EXT_PATH" in hot_candidates:
                    target_paths = self._path_set(
                        self._typed_value(
                            BotConfig,
                            self._bot_defaults,
                            "ext_path",
                            after.get("EXT_PATH"),
                        )
                    )
                    ext_operation = await plugin_runtime_manager.apply_ext_paths(
                        previous_ext_paths, target_paths
                    )
                    if (
                        ext_operation
                        and ext_operation.mode is ApplyMode.RESTART_PENDING
                    ):
                        pending_keys.add("EXT_PATH")
                        field_effects["EXT_PATH"] = "worker_restart"
                        await self._apply_runtime_values(runtime_before, {"EXT_PATH"})
                        hot_candidates.discard("EXT_PATH")
                    elif ext_operation and ext_operation.mode is ApplyMode.FAILED:
                        raise RuntimeEnvironmentError(
                            ext_operation.reason or "ext_path_apply_failed"
                        )

                reload_keys = hot_candidates - CACHE_KEYS - EXT_PATH_KEYS
                if reload_keys:
                    hot_operation = await plugin_runtime_manager.reload_env_consumers(
                        reload_keys, submit_restart=False
                    )
                    if hot_operation and hot_operation.mode is ApplyMode.FAILED:
                        raise RuntimeEnvironmentError(
                            hot_operation.reason or "env_consumer_reload_failed"
                        )
                for key in hot_candidates - pending_keys:
                    self.effective_values[key] = after.get(key)
            except Exception:
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
                try:
                    await plugin_runtime_manager.request_restart(
                        set(), "environment_changed", submit_launcher=True
                    )
                except TypeError as error:
                    if "submit_launcher" not in str(error):
                        raise
                    await plugin_runtime_manager.request_restart(
                        set(), "environment_changed"
                    )
            mode = (
                "restart_pending"
                if reasons
                else (
                    "hot_reloaded"
                    if hot_operation or ext_operation
                    else "component_restarted"
                    if component_operation
                    and component_operation.apply_effect == "component_restarted"
                    else "config_reloaded"
                )
            )
            changed_plugins = sorted(
                {
                    *(hot_operation.changed if hot_operation else []),
                    *(ext_operation.changed if ext_operation else []),
                }
            )
            logger.info(
                f"环境配置已协调: changed={len(changed)} "
                f"hot={len(hot_candidates - pending_keys)} "
                f"pending={len(pending_keys)}",
                "RuntimeEnvironment",
            )
            self.last_content = after_content
            self.last_contents[_env_source().resolve()] = after_content
            return EnvironmentApplyResult(
                apply_mode=mode,
                changed_keys=sorted(changed),
                restart_required=bool(reasons),
                reason_codes=reasons,
                field_effects=field_effects,
                hot_reloaded=bool(hot_candidates - pending_keys),
                changed_plugins=changed_plugins,
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

    async def apply_current_file(
        self, path: Path | None = None, *, submit_restart: bool = True
    ) -> EnvironmentApplyResult:
        path = path or _env_source()
        path = path.resolve()
        content = path.read_text(encoding="utf-8") if path.exists() else ""
        result = await self.apply(
            self.last_contents.get(path, self.last_content),
            content,
            source=ENV_APPLY_SOURCE,
            submit_restart=submit_restart,
        )
        self.last_contents[path] = content
        return result

    def observe_file(self, path: Path) -> None:
        resolved = path.resolve()
        try:
            content = resolved.read_text(encoding="utf-8")
        except OSError:
            content = ""
        self.last_contents[resolved] = content


runtime_environment_manager = RuntimeEnvironmentManager()


__all__ = [
    "COMPONENT_KEYS",
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
