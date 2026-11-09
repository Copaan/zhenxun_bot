import asyncio
from collections import Counter
from hashlib import sha256
import os
from pathlib import Path, PureWindowsPath
import shutil
import stat
import threading
import uuid

from nonebot_plugin_apscheduler import scheduler

from zhenxun.configs.config import Config
from zhenxun.services.log import logger

Config.add_plugin_config(
    "_backup",
    "BACKUP_FLAG",
    True,
    help="是否开启文件备份",
    default_value=True,
    type=bool,
    ui={
        "label": "启用自动备份",
        "section": "基本设置",
        "component": "switch",
        "order": 10,
    },
)

Config.add_plugin_config(
    "_backup",
    "BACKUP_DIR_OR_FILE",
    ["data"],
    help="备份的文件夹或文件",
    default_value=[],
    type=list[str],
    ui={
        "label": "备份目录或文件",
        "section": "基本设置",
        "component": "tags",
        "order": 20,
        "visible_when": {"path": "BACKUP_FLAG", "operator": "eq", "value": True},
    },
)


_BACKUP_LOCK = asyncio.Lock()


def _check_links(path: Path) -> None:
    """拒绝路径及已有父目录中的符号链接和重解析点。"""
    for item in (path, *path.parents):
        try:
            info = item.lstat()
        except FileNotFoundError:
            continue
        if stat.S_ISLNK(info.st_mode) or getattr(
            info, "st_file_attributes", 0
        ) & getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 1024):
            raise ValueError("backup_link_not_allowed")


def _backup_paths(value: str, root: Path) -> tuple[Path, Path]:
    """将内部与外部来源映射到备份根内的独立目标。"""
    raw = str(value).strip()
    windows = PureWindowsPath(raw)
    path = Path(raw)
    if (
        not raw
        or ".." in path.parts
        or ".." in windows.parts
        or (windows.drive and not windows.root)
        or (windows.root and not windows.drive and os.name == "nt")
        or (os.name == "nt" and any(":" in part for part in windows.parts[1:]))
    ):
        raise ValueError("backup_path_invalid")
    source = path if path.is_absolute() else root / path
    _check_links(source)
    source = source.resolve()
    backup = root / "backup"
    _check_links(backup)
    if source == root or source == backup or source.is_relative_to(backup):
        raise ValueError("backup_source_overlap")
    if backup.is_relative_to(source):
        raise ValueError("backup_source_overlap")
    if source.is_relative_to(root):
        relative = source.relative_to(root)
        if relative.parts[0] == "_external":
            raise ValueError("backup_reserved_target")
        target = backup / relative
    else:
        digest = sha256(os.path.normcase(str(source)).encode("utf-8")).hexdigest()
        if not source.name:
            raise ValueError("backup_source_overlap")
        target = backup / "_external" / digest / source.name
    _check_links(target)
    if target == backup or not target.resolve().is_relative_to(backup.resolve()):
        raise ValueError("backup_target_escape")
    if (
        target == source
        or target.is_relative_to(source)
        or source.is_relative_to(target)
    ):
        raise ValueError("backup_source_overlap")
    return source, target


def _stop_requested(stop: threading.Event) -> None:
    if stop.is_set():
        raise InterruptedError("backup_stopped")


def _copy_backup(source: Path, target: Path, stop: threading.Event) -> None:
    """逐文件分块复制，检查链接并响应协作停止信号。"""
    _stop_requested(stop)
    _check_links(source)
    _check_links(target)
    info = source.lstat()
    if stat.S_ISDIR(info.st_mode):
        target.mkdir()
        with os.scandir(source) as entries:
            for entry in entries:
                _copy_backup(Path(entry.path), target / entry.name, stop)
        shutil.copystat(source, target, follow_symlinks=False)
    elif stat.S_ISREG(info.st_mode):
        descriptor = os.open(source, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
        with os.fdopen(descriptor, "rb") as input_file:
            opened = os.fstat(input_file.fileno())
            if (opened.st_dev, opened.st_ino) != (info.st_dev, info.st_ino):
                raise ValueError("backup_source_changed")
            with target.open("xb") as output:
                while True:
                    _stop_requested(stop)
                    chunk = input_file.read(1024 * 1024)
                    if not chunk:
                        break
                    output.write(chunk)
                output.flush()
                os.fsync(output.fileno())
        shutil.copystat(source, target, follow_symlinks=False)
    else:
        raise ValueError("backup_special_file_not_allowed")


def _remove_backup(path: Path) -> None:
    _check_links(path)
    if path.is_dir():
        shutil.rmtree(path)
    else:
        path.unlink(missing_ok=True)


def _backup_one(source: Path, target: Path, stop: threading.Event) -> str:
    """先复制候选，再发布；旧目录保留至发布完成后清理。"""
    _check_links(source)
    _check_links(target)
    target.parent.mkdir(parents=True, exist_ok=True)
    stage = target.with_name(f".{target.name}.zx-stage-{uuid.uuid4().hex}")
    old = target.with_name(f".{target.name}.zx-old")
    _check_links(old)
    if old.exists():
        if not target.exists():
            os.replace(old, target)
        else:
            try:
                _remove_backup(old)
            except (OSError, ValueError):
                return "cleanup_retry_pending"
    try:
        _copy_backup(source, stage, stop)
        _stop_requested(stop)
        _check_links(source)
        _check_links(target)
        if target.exists():
            os.replace(target, old)
        try:
            os.replace(stage, target)
        except BaseException:
            if old.exists() and not target.exists():
                os.replace(old, target)
            raise
        try:
            if old.exists():
                _remove_backup(old)
        except (OSError, ValueError):
            return "cleanup_pending"
        return "completed"
    finally:
        if stage.exists():
            try:
                _remove_backup(stage)
            except (OSError, ValueError):
                logger.warning("备份候选清理待重试", "自动备份")


def _run_backup(values: list[str], root: Path, stop: threading.Event) -> dict:
    """顺序执行各来源并汇总成功、跳过、失败和清理状态。"""
    counts: Counter = Counter()
    selected: list[tuple[Path, Path]] = []
    for value in values:
        try:
            source, target = _backup_paths(value, root)
            if not source.exists():
                counts["missing"] += 1
                continue
            selected.append((source, target))
        except (OSError, ValueError) as error:
            counts["invalid"] += 1
            reason = (
                str(error) if isinstance(error, ValueError) else type(error).__name__
            )
            logger.warning(f"备份来源拒绝处理 | reason={reason}", "自动备份")
    selected.sort(key=lambda item: len(item[0].parts))
    accepted: list[Path] = []
    for source, target in selected:
        if stop.is_set():
            counts["stopped"] += 1
            continue
        if any(
            source == previous or source.is_relative_to(previous)
            for previous in accepted
        ):
            counts["duplicate"] += 1
            continue
        accepted.append(source)
        try:
            counts[_backup_one(source, target, stop)] += 1
        except InterruptedError:
            counts["stopped"] += 1
        except Exception as error:
            counts["failed"] += 1
            logger.error("备份项目失败，保留可恢复的旧备份", "自动备份", e=error)
    return dict(counts)


@scheduler.scheduled_job("cron", hour=3, minute=25, max_instances=1, coalesce=True)
async def _():
    from zhenxun.services.scheduler.lifecycle import core_task_run

    async with core_task_run("auto_backup") as run:
        if not Config.get_config("_backup", "BACKUP_FLAG"):
            run.update(status="skipped", reason="config_disabled")
            return
        values = list(Config.get_config("_backup", "BACKUP_DIR_OR_FILE") or [])
        stop = threading.Event()
        async with _BACKUP_LOCK:
            worker = asyncio.create_task(
                asyncio.to_thread(_run_backup, values, Path.cwd().resolve(), stop)
            )
            try:
                result = await asyncio.shield(worker)
            except asyncio.CancelledError:
                stop.set()
                while not worker.done():
                    try:
                        await asyncio.shield(worker)
                    except asyncio.CancelledError:
                        continue
                if not worker.cancelled():
                    worker.result()
                raise
        run.update(
            processed=result.get("completed", 0) + result.get("cleanup_pending", 0),
            errors=sum(
                result.get(key, 0) for key in ("invalid", "failed", "recovery_required")
            ),
            skipped=sum(
                result.get(key, 0)
                for key in ("missing", "duplicate", "stopped", "cleanup_retry_pending")
            ),
            cleanup_pending=result.get("cleanup_pending", 0)
            + result.get("cleanup_retry_pending", 0),
            counts=result,
        )
        if not values:
            run.update(status="skipped", reason="no_sources")
        elif not run["processed"] and not run["errors"]:
            run.update(
                status="skipped",
                reason="cleanup_pending"
                if run["cleanup_pending"]
                else "sources_unavailable",
            )
        elif run["cleanup_pending"]:
            run["reason"] = "cleanup_pending"
