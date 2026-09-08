from __future__ import annotations

import asyncio
from pathlib import Path
import time
from typing import TYPE_CHECKING

from watchfiles import Change, awatch

from zhenxun.services.log import logger
from zhenxun.services.runtime_mutation import (
    RuntimeMutationBusyError,
    runtime_mutation_coordinator,
)

if TYPE_CHECKING:
    from .manager import PluginRuntimeManager


_RETRY_DELAYS = (1, 2, 5, 15, 30)


def _watch_roots(manager: PluginRuntimeManager) -> list[Path]:
    project_root = Path.cwd().resolve()
    runtime_root = (Path("data") / "runtime").resolve()
    virtual_env_root = (Path(__file__).resolve().parents[3] / ".venv").resolve()
    roots = {Path("zhenxun"), Path("data/web_ui/public")}
    roots.update(
        unit.root
        for unit in manager.units.values()
        if unit.root
        and not unit.root.resolve().is_relative_to(runtime_root)
        and (
            unit.root.resolve().is_relative_to(project_root)
            or not unit.root.resolve().is_relative_to(virtual_env_root)
        )
    )
    roots.update(
        Path(name)
        for name in (
            "data/config.yaml",
            ".env.dev",
            ".env",
            "pyproject.toml",
            "uv.lock",
        )
        if Path(name).exists()
    )
    normalized: set[Path] = set()
    for path in sorted(path.resolve() for path in roots if path.exists()):
        if any(path.is_relative_to(parent) for parent in normalized if parent.is_dir()):
            continue
        normalized.add(path)
    return sorted(normalized)


def _interesting(change: Change, raw_path: str) -> bool:
    path = Path(raw_path)
    if path.resolve().is_relative_to((Path("data") / "runtime").resolve()):
        return False
    if any(part in {"__pycache__", ".git", ".pytest_cache"} for part in path.parts):
        return False
    if path.suffix == ".json":
        return path.name == "version.json" and "web_ui" in path.parts
    return path.suffix in {
        ".py",
        ".yaml",
        ".yml",
        ".toml",
        ".lock",
        ".js",
        ".css",
        ".html",
    } or path.name.startswith(".env")


async def watch_runtime_changes(manager: PluginRuntimeManager) -> None:
    retry_index = 0
    while True:
        if not runtime_mutation_coordinator.accepting:
            manager.watcher_state = "stopped"
            return
        try:
            roots = _watch_roots(manager)
            manager.watcher_roots = [str(path) for path in roots]
            if not roots:
                logger.warning("未找到运行时监听目录，自动热加载未启动")
                manager.watcher_state = "idle"
                return
            manager.watcher_state = "watching"
            logger.info(f"运行时自动监听已启动，共 {len(roots)} 个目录")
            manifest = _build_manifest(roots)
            last_reconcile = time.monotonic()
            async for changes in awatch(
                *roots,
                debounce=750,
                step=250,
                rust_timeout=5000,
                yield_on_timeout=True,
            ):
                retry_index = 0
                manager.watcher_retry_count = 0
                manager.watcher_last_error = None
                paths = {
                    Path(raw_path)
                    for change, raw_path in changes
                    if _interesting(change, raw_path)
                }
                if paths:
                    await manager.process_changes(
                        paths,
                        submit_restart=manager.runtime_watch_mode() == "auto_restart",
                    )
                if manager.consume_watcher_refresh():
                    break
                if time.monotonic() - last_reconcile >= 60:
                    current_manifest = _build_manifest(roots)
                    missed = {
                        path
                        for path in manifest.keys() | current_manifest.keys()
                        if manifest.get(path) != current_manifest.get(path)
                    }
                    if missed:
                        await manager.process_changes(
                            missed,
                            submit_restart=manager.runtime_watch_mode()
                            == "auto_restart",
                        )
                    manifest = current_manifest
                    last_reconcile = time.monotonic()
                await asyncio.sleep(0)
            retry_index = 0
            manager.watcher_retry_count = 0
            if manager.runtime_watch_mode() == "disabled":
                manager.watcher_state = "disabled"
                return
        except asyncio.CancelledError:
            manager.watcher_state = "stopped"
            raise
        except Exception as e:
            if (
                isinstance(e, RuntimeMutationBusyError)
                and not runtime_mutation_coordinator.accepting
            ):
                manager.watcher_state = "stopped"
                return
            delay = _RETRY_DELAYS[min(retry_index, len(_RETRY_DELAYS) - 1)]
            retry_index += 1
            manager.watcher_state = "retrying"
            manager.watcher_retry_count = retry_index
            manager.watcher_last_error = f"watcher_failed:{type(e).__name__}"
            logger.error(
                f"运行时文件监听失败，将在 {delay} 秒后重试",
                e=e,
            )
            await asyncio.sleep(delay)


def _build_manifest(roots: list[Path]) -> dict[Path, tuple[int, int]]:
    result: dict[Path, tuple[int, int]] = {}
    for root in roots:
        candidates = [root] if root.is_file() else root.rglob("*")
        for path in candidates:
            if not path.is_file() or not _interesting(Change.modified, str(path)):
                continue
            try:
                stat = path.stat()
            except OSError:
                continue
            result[path.resolve()] = (stat.st_mtime_ns, stat.st_size)
    return result
