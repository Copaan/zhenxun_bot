from __future__ import annotations

import asyncio
from pathlib import Path
from typing import TYPE_CHECKING

from zhenxun.services.log import logger
from zhenxun.services.runtime_config_reload import reload_runtime_config

from .models import ApplyMode, RuntimeOperation

if TYPE_CHECKING:
    from .manager import PluginRuntimeManager


_CONFIG_FILE = Path("data/config.yaml").resolve()
_ENV_FILES = {Path(".env.dev").resolve(), Path(".env").resolve()}
_DEPENDENCY_NAMES = {"pyproject.toml", "uv.lock", "requirements.txt"}


class RuntimeChangeCoordinator:
    def __init__(self, manager: PluginRuntimeManager) -> None:
        self.manager = manager
        self._lock = asyncio.Lock()

    async def process(self, paths: set[Path]) -> RuntimeOperation | None:
        changed = self.manager.claim_content_changes({path.resolve() for path in paths})
        if not changed:
            return None
        async with self._lock:
            if changed & _ENV_FILES:
                return await self.manager.request_restart(set(), "environment_changed")
            if any(
                path.name in _DEPENDENCY_NAMES or path.name == "requirements.txt"
                for path in changed
            ):
                return await self.manager.request_dependency_restart(changed)
            if _CONFIG_FILE in changed:
                try:
                    await reload_runtime_config()
                except Exception as e:
                    logger.error("配置文件热加载失败，已保留上一代运行状态", e=e)
                    operation = RuntimeOperation(
                        ApplyMode.FAILED,
                        "failed",
                        ["config.yaml"],
                        "config_validation_failed",
                        self.manager.generation,
                    )
                    self.manager.last_operation = operation
                    return operation
                operation = RuntimeOperation(
                    ApplyMode.CONFIG_RELOADED,
                    "completed",
                    ["config.yaml"],
                    generation=self.manager.generation,
                )
                self.manager.last_operation = operation
                return operation
            if any("data/web_ui/public" in path.as_posix() for path in changed):
                self.manager.webui_revision = self.manager._read_webui_revision()
                operation = RuntimeOperation(
                    ApplyMode.WEBUI_REFRESH,
                    "completed",
                    ["webui"],
                    generation=self.manager.generation,
                )
                self.manager.last_operation = operation
                return operation
            return await self.manager.apply_plugin_changes(changed)
