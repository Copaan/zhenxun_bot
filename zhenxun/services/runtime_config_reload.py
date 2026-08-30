from __future__ import annotations

from collections.abc import Callable
import contextlib

from zhenxun.configs.config import Config
from zhenxun.services.ai.config import get_llm_config
from zhenxun.services.ai.llm.manager import clear_all_cache


async def reload_runtime_config(
    reschedule: Callable[[], None] | None = None,
) -> None:
    """Reload config.yaml and the runtime state derived from it."""
    from zhenxun.builtin_plugins.hooks.auth.auth_limit import LimitManager
    from zhenxun.builtin_plugins.init.manager import manager

    Config.reload()
    get_llm_config.cache_clear()
    clear_all_cache()
    manager.init()
    await manager.load_to_db()
    await LimitManager.update_limits()
    if reschedule is not None:
        with contextlib.suppress(Exception):
            reschedule()


__all__ = ["reload_runtime_config"]
