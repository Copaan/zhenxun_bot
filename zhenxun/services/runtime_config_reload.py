from __future__ import annotations

from collections.abc import Callable
import contextlib
from copy import deepcopy
from pathlib import Path
from typing import Any

from zhenxun.configs.config import Config
from zhenxun.services.ai.config import get_llm_config
from zhenxun.services.ai.llm.manager import clear_all_cache
from zhenxun.services.log import logger
from zhenxun.services.runtime_reload.models import ApplyMode, RuntimeOperation

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
    from zhenxun.builtin_plugins.hooks.auth.auth_limit import LimitManager
    from zhenxun.builtin_plugins.init.manager import manager
    from zhenxun.services.runtime_reload import plugin_runtime_manager

    async def refresh_derived_state() -> None:
        get_llm_config.cache_clear()
        clear_all_cache()
        manager.init()
        await manager.load_to_db()
        await LimitManager.update_limits()
        if reschedule is not None:
            with contextlib.suppress(Exception):
                reschedule()

    snapshot = Config.snapshot_runtime_values()
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
        await refresh_derived_state()
        operation = None
        if reload_consumers:
            try:
                operation = await plugin_runtime_manager.reload_config_consumers(
                    changed_dependencies,
                    restart_dependencies=startup_changed_dependencies,
                    submit_restart=submit_restart,
                )
            except TypeError as error:
                if "restart_dependencies" not in str(error):
                    raise
                operation = await plugin_runtime_manager.reload_config_consumers(
                    changed_dependencies,
                    submit_restart=submit_restart,
                )
        if operation is not None and operation.mode is ApplyMode.FAILED:
            raise RuntimeConfigReloadError(
                operation.reason or "config_consumer_reload_failed"
            )
        if operation is None:
            operation = RuntimeOperation(
                ApplyMode.CONFIG_RELOADED,
                "completed",
                changed_paths,
                generation=plugin_runtime_manager.generation,
            )
            plugin_runtime_manager.last_operation = operation
        operation.config_keys = changed_paths
        plugin_runtime_manager.mark_content_processed(Path("data/config.yaml"))
        return operation
    except Exception:
        Config.restore_runtime_values(snapshot)
        try:
            Config.save()
            await refresh_derived_state()
        except Exception as rollback_error:
            logger.error("运行时配置回滚后的派生状态刷新失败", e=rollback_error)
        raise


__all__ = [
    "RuntimeConfigReloadError",
    "changed_config_dependencies",
    "reload_runtime_config",
]
