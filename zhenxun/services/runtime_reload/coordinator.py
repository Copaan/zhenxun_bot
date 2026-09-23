from __future__ import annotations

from hashlib import sha256
from pathlib import Path
from typing import TYPE_CHECKING

from zhenxun.services.log import logger
from zhenxun.services.runtime_config_reload import reload_runtime_config
from zhenxun.services.runtime_mutation import (
    managed_mutation,
    runtime_mutation_coordinator,
)
from zhenxun.services.webui_resources import webui_resources

from .models import ApplyMode, RuntimeOperation

if TYPE_CHECKING:
    from .manager import PluginRuntimeManager


_CONFIG_FILE = Path("data/config.yaml").resolve()
_ENV_FILES = {Path(".env.dev").resolve(), Path(".env").resolve()}
_DEPENDENCY_NAMES = {"pyproject.toml", "uv.lock", "requirements.txt"}


class RuntimeChangeCoordinator:
    def __init__(self, manager: PluginRuntimeManager) -> None:
        self.manager = manager

    @managed_mutation("filesystem_watcher")
    async def process(
        self, paths: set[Path], *, submit_restart: bool = True
    ) -> RuntimeOperation | None:
        resolved = {
            path.resolve() for path in paths if not webui_resources.contains(path)
        }
        await self.manager.wait_for_content_change_holds(resolved)
        changed = self.manager.claim_content_changes(resolved)
        if not changed:
            if resolved & _ENV_FILES:
                from zhenxun.services.runtime_environment import (
                    runtime_environment_manager,
                )

                for path in resolved & _ENV_FILES:
                    runtime_environment_manager.observe_file(path)
            if _CONFIG_FILE in resolved:
                logger.debug("配置文件事件已由当前运行时操作处理，跳过重复重载")
            return None
        async with runtime_mutation_coordinator.operation("filesystem_watcher"):
            operations: list[RuntimeOperation] = []
            reloaded: set[str] = set()
            code = changed - _ENV_FILES - {_CONFIG_FILE}
            digests = dict(self.manager._content_digests)
            if any(path.name in _DEPENDENCY_NAMES for path in changed):
                operation = await self.manager.request_dependency_restart(
                    changed, submit_launcher=submit_restart
                )
                return self._finish([operation], changed)
            if code and (reason := self.manager.plugin_change_restart_reason(code)):
                operation = await self.manager.request_restart(
                    self.manager.affected_units(code),
                    reason,
                    submit_launcher=submit_restart,
                )
                return self._finish([operation], changed)

            for env_path in sorted(changed & _ENV_FILES):
                try:
                    from zhenxun.services.runtime_environment import (
                        runtime_environment_manager,
                    )

                    result = await runtime_environment_manager.apply_current_file(
                        env_path, submit_restart=False
                    )
                    if result.apply_mode == "no_change":
                        continue
                    reloaded.update(result.changed_plugins)
                    operations.append(
                        RuntimeOperation(
                            ApplyMode(result.apply_mode),
                            "pending_restart"
                            if result.restart_required
                            else "completed",
                            result.changed_keys,
                            result.reason_codes[0] if result.reason_codes else None,
                            self.manager.generation,
                            config_keys=result.changed_keys,
                            component_effects=result.component_effects,
                            affected_components=result.affected_components,
                            reason_codes=result.reason_codes,
                        )
                    )
                except Exception as error:
                    logger.error("环境配置运行时协调失败", e=error)
                    operations.append(
                        RuntimeOperation(
                            ApplyMode.FAILED,
                            "failed",
                            [str(env_path)],
                            f"environment_apply_failed:{type(error).__name__}",
                            self.manager.generation,
                        )
                    )
            if _CONFIG_FILE in changed:
                logger.debug("检测到外部配置文件内容变化，开始运行时重载")
                try:
                    operation = await reload_runtime_config(submit_restart=False)
                except Exception as e:
                    logger.error("配置文件热加载失败，已保留上一代运行状态", e=e)
                    operation = RuntimeOperation(
                        ApplyMode.FAILED,
                        "failed",
                        ["config.yaml"],
                        "config_validation_failed",
                        self.manager.generation,
                    )
                operations.append(operation)
                if operation.mode is ApplyMode.HOT_RELOADED:
                    reloaded.update(operation.changed)
            blocked = any(
                operation.mode
                in {
                    ApplyMode.FAILED,
                    ApplyMode.ROLLED_BACK,
                    ApplyMode.RESTART_PENDING,
                    ApplyMode.RESTART_REQUESTED,
                }
                for operation in operations
            )
            if code and blocked:
                operations.append(
                    await self.manager.request_restart(
                        self.manager.affected_units(code),
                        "configuration_batch_incomplete",
                        submit_launcher=False,
                    )
                )
            elif code:
                remaining = set()
                for path in code:
                    try:
                        unchanged = sha256(
                            path.read_bytes()
                        ).hexdigest() == digests.get(path)
                    except OSError:
                        unchanged = False
                    if (
                        not unchanged
                        or not self.manager.affected_units({path}) <= reloaded
                    ):
                        remaining.add(path)
                if remaining:
                    operations.append(
                        await self.manager.apply_plugin_changes(
                            remaining, submit_restart=False
                        )
                    )
            if (
                submit_restart
                and any(op.mode is ApplyMode.RESTART_PENDING for op in operations)
                and not any(
                    op.mode in {ApplyMode.FAILED, ApplyMode.ROLLED_BACK}
                    for op in operations
                )
            ):
                pending = next(
                    op for op in operations if op.mode is ApplyMode.RESTART_PENDING
                )
                operations.append(
                    await self.manager.request_restart(
                        set(), pending.reason or "files_changed", submit_launcher=True
                    )
                )
            return self._finish(operations, changed)

    def _finish(
        self, operations: list[RuntimeOperation], changed: set[Path]
    ) -> RuntimeOperation | None:
        """Keep all batch evidence, with failure taking priority over success."""
        if not operations:
            return None
        priority = {
            ApplyMode.FAILED: 8,
            ApplyMode.ROLLED_BACK: 7,
            ApplyMode.RESTART_REQUESTED: 6,
            ApplyMode.RESTART_PENDING: 5,
            ApplyMode.HOT_RELOADED: 4,
            ApplyMode.COMPONENT_RESTARTED: 3,
            ApplyMode.CONFIG_RELOADED: 2,
            ApplyMode.WEBUI_REFRESH: 1,
        }
        primary = max(operations, key=lambda op: priority[op.mode])
        result = RuntimeOperation(
            primary.mode,
            primary.status,
            sorted(
                {str(path) for path in changed}
                | {item for op in operations for item in op.changed}
            ),
            primary.reason,
            self.manager.generation,
            config_keys=sorted({key for op in operations for key in op.config_keys}),
            component_effects={
                key: value
                for op in operations
                for key, value in op.component_effects.items()
            },
            affected_components=sorted(
                {key for op in operations for key in op.affected_components}
            ),
            rollback_state=primary.rollback_state,
            steps=[op.public_dict() for op in operations],
            reason_codes=sorted(
                {
                    reason
                    for op in operations
                    for reason in op.public_dict()["reason_codes"]
                }
            ),
        )
        self.manager.last_operation = result
        self.manager._persist_index()
        return result
