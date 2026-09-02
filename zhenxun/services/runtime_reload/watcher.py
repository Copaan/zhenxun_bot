from __future__ import annotations

import asyncio
from pathlib import Path
import time
from typing import TYPE_CHECKING

from watchfiles import Change, awatch

from zhenxun.services.log import logger

if TYPE_CHECKING:
    from .manager import PluginRuntimeManager


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
    return path.suffix in {
        ".py",
        ".yaml",
        ".yml",
        ".toml",
        ".lock",
        ".json",
        ".js",
        ".css",
        ".html",
    } or path.name.startswith(".env")


async def watch_runtime_changes(manager: PluginRuntimeManager) -> None:
    roots = _watch_roots(manager)
    if not roots:
        logger.warning("未找到运行时监听目录，自动热加载未启动")
        return
    logger.info(f"运行时自动监听已启动，共 {len(roots)} 个目录")
    manifest = _build_manifest(roots)
    last_reconcile = time.monotonic()
    try:
        async for changes in awatch(
            *roots,
            debounce=750,
            step=250,
            rust_timeout=5000,
            yield_on_timeout=True,
        ):
            paths = {
                Path(raw_path)
                for change, raw_path in changes
                if _interesting(change, raw_path)
            }
            if paths:
                await manager.process_changes(paths)
                if manager.consume_watcher_refresh():
                    manager._watcher_task = asyncio.create_task(
                        watch_runtime_changes(manager),
                        name="zhenxun-runtime-watcher",
                    )
                    return
            if time.monotonic() - last_reconcile >= 60:
                current_manifest = _build_manifest(roots)
                missed = {
                    path
                    for path in manifest.keys() | current_manifest.keys()
                    if manifest.get(path) != current_manifest.get(path)
                }
                if missed:
                    await manager.process_changes(missed)
                manifest = current_manifest
                last_reconcile = time.monotonic()
            await asyncio.sleep(0)
    except asyncio.CancelledError:
        raise
    except Exception as e:
        manager.enabled = False
        manager.compatibility_error = "watcher_failed"
        logger.error("运行时文件监听失败，插件热加载已降级", e=e)


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
