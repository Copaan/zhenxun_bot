"""备份包：收集文件、导出数据库并打包为 .zx"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
import contextlib
from datetime import datetime
import json
import os
from pathlib import Path, PurePosixPath
import re
import secrets
import shutil
import sys
import tempfile
import threading
import time
from typing import ClassVar, Literal
import zipfile

from packaging.utils import canonicalize_name
import pyzipper

from . import data, deps

FORMAT = 1
SUFFIX = ".zx"
MANIFEST = "manifest.json"
DEPENDENCIES = "dependencies.json"
BACKUP_DIR = Path("data/backups")
UPLOAD_DIR = BACKUP_DIR / ".uploads"
CATEGORIES = ("configuration", "data", "plugins", "resources")
DEFAULT_CATEGORIES = ("configuration", "data", "plugins")
_ROOTS = ("data", "resources", "zhenxun/plugins", "certs")
_EXCLUDED_DIRS = frozenset(
    {
        ".git",
        ".venv",
        "venv",
        "__pycache__",
        ".cache",
        "cache",
        ".pytest_cache",
        ".ruff_cache",
        "node_modules",
        "log",
        "logs",
        "backup",
        "backups",
        "migration",
        "temp",
        "tmp",
    }
)
_EXCLUDED_PREFIXES = ("data/runtime/", "data/web_ui/", "data/db/", "resources/temp/")
_EXCLUDED_SUFFIXES = (".pyc", ".log", SUFFIX, ".part")
_WINDOWS_INVALID = re.compile(r'[<>:"|?*\\\x00-\x1f]')
_WINDOWS_DEVICES = frozenset(
    {"CON", "PRN", "AUX", "NUL"}
    | {f"{name}{index}" for name in ("COM", "LPT") for index in range(1, 10)}
)

Progress = Callable[[str, int, int, str], None]


class BackupError(RuntimeError):
    """备份包创建或读取失败"""


def category_for(relative: str) -> str | None:
    """文件所属的备份分类，不属于任何分类时返回 None

    参数:
        relative: 相对项目根目录的路径（/ 分隔）

    返回:
        str | None: 分类名
    """
    path = relative.casefold()
    if path.startswith(_EXCLUDED_PREFIXES) or path.endswith(_EXCLUDED_SUFFIXES):
        return None
    if path.startswith("zhenxun/plugins/"):
        return "plugins"
    if path.startswith("resources/"):
        return "resources"
    if "/" not in path and path.startswith(".env") and path != ".env.example":
        return "configuration"
    if path == "data/config.yaml" or path.startswith(("data/configs/", "certs/")):
        return "configuration"
    if path.startswith("data/"):
        return "data"
    return None


def _database_files(db_url: str, root: Path) -> set[Path]:
    from zhenxun.configs.database import is_sqlite_memory_url, sqlite_path_from_url

    if not db_url.startswith("sqlite") or is_sqlite_memory_url(db_url):
        return set()
    path = sqlite_path_from_url(db_url, root).resolve()
    return {Path(f"{path}{suffix}") for suffix in ("", "-wal", "-shm", "-journal")}


def _current_db_url() -> str:
    from zhenxun.configs.config import BotConfig

    return BotConfig.db_url


def collect(
    root: Path, categories, excluded: set[Path] | None = None
) -> list[tuple[str, int]]:
    """按分类收集需要备份的文件

    参数:
        root: 项目根目录
        categories: 选择的分类
        excluded: 额外排除的文件或目录（绝对路径）

    返回:
        list[tuple[str, int]]: (相对路径, 字节数)
    """
    excluded = {path.resolve() for path in excluded or ()}
    categories = set(categories)
    result = []

    def add(path: Path):
        relative = path.relative_to(root).as_posix()
        if category_for(relative) in categories and path.resolve() not in excluded:
            try:
                result.append((relative, path.stat().st_size))
            except OSError:
                pass

    for path in sorted(root.glob(".env*")):
        if path.is_file():
            add(path)
    for base in _ROOTS:
        top = root / base
        if not top.is_dir() or top.is_symlink():
            continue
        for directory, names, files in os.walk(top):
            current = Path(directory)
            names[:] = sorted(
                name
                for name in names
                if name.casefold() not in _EXCLUDED_DIRS
                and not (current / name).is_symlink()
                and (current / name).resolve() not in excluded
            )
            for name in sorted(files):
                path = current / name
                if not path.is_symlink():
                    add(path)
    return result


def scan(root: Path | None = None, db_url: str | None = None) -> dict:
    """导出前的预览：各分类的文件数与大小、未纳管依赖

    参数:
        root: 项目根目录
        db_url: 当前数据库地址

    返回:
        dict: 预览信息
    """
    root = (root or Path.cwd()).resolve()
    excluded = _database_files(db_url or _current_db_url(), root)
    sizes = {name: {"files": 0, "bytes": 0} for name in CATEGORIES}
    for relative, size in collect(root, CATEGORIES, excluded):
        item = sizes[category_for(relative)]  # type: ignore
        item["files"] += 1
        item["bytes"] += size
    return {
        "categories": sizes,
        "default_categories": list(DEFAULT_CATEGORIES),
        "dependencies": deps.inventory(root),
    }


def _open(path: Path, mode: Literal["r", "w"], password: str | None):
    if not password:
        return zipfile.ZipFile(
            path, mode, compression=zipfile.ZIP_DEFLATED, allowZip64=True
        )
    archive = pyzipper.AESZipFile(
        path, mode, compression=zipfile.ZIP_DEFLATED, allowZip64=True
    )
    archive.setpassword(password.encode())
    if mode == "w":
        archive.setencryption(pyzipper.WZ_AES, nbits=256)
    return archive


def _write(
    destination: Path,
    root: Path,
    database: Path,
    files: list[tuple[str, int]],
    manifest: dict,
    dependencies: dict,
    password: str | None,
    progress: Progress | None,
) -> list[str]:
    skipped = []
    total, done = sum(size for _, size in files), 0
    with _open(destination, "w", password) as archive:
        for path in sorted(database.iterdir()):
            archive.write(path, f"database/{path.name}")
        for relative, size in files:
            try:
                archive.write(root / relative, f"files/{relative}")
            except OSError:
                skipped.append(relative)
            done += size
            if progress:
                progress("files", done, total, "")
        manifest["skipped_files"] = skipped
        archive.writestr(DEPENDENCIES, json.dumps(dependencies, ensure_ascii=False))
        archive.writestr(MANIFEST, json.dumps(manifest, ensure_ascii=False, indent=1))
    return skipped


async def export_backup(
    categories=DEFAULT_CATEGORIES,
    extras: list[str] | None = None,
    password: str | None = None,
    *,
    root: Path | None = None,
    db_url: str | None = None,
    destination: Path | None = None,
    progress: Progress | None = None,
) -> tuple[Path, dict]:
    """在线导出备份包：数据库快照、所选分类的文件与依赖清单

    参数:
        categories: 备份的文件分类
        extras: 高级模式选择记录的依赖包名，为 None 时记录全部依赖
        password: 加密密码，为空时不加密
        root: 项目根目录
        db_url: 数据库地址，默认使用当前配置
        destination: 备份包路径，默认 data/backups/zhenxun-<时间>.zx
        progress: 进度回调 (阶段, 已完成, 总量, 当前表名)；数据库阶段的已完成
            为当前表已导出的行数

    返回:
        tuple[Path, dict]: 备份包路径与清单
    """
    if unknown := set(categories) - set(CATEGORIES):
        raise BackupError(f"未知的备份分类: {', '.join(sorted(unknown))}")
    root = (root or Path.cwd()).resolve()
    db_url = db_url or _current_db_url()
    now = datetime.now()
    destination = (
        destination or root / BACKUP_DIR / f"zhenxun-{now:%Y%m%d-%H%M%S}{SUFFIX}"
    ).resolve()
    destination.parent.mkdir(parents=True, exist_ok=True)
    items = deps.inventory(root)
    if extras is not None:
        selected = {canonicalize_name(name) for name in extras}
        items = [item for item in items if item["name"] in selected]
    dependencies = deps.snapshot(items)
    work = Path(tempfile.mkdtemp(prefix=".zx-export-", dir=destination.parent))
    part = destination.with_name(destination.name + ".part")
    try:
        client = await data.open_client(db_url, "zx_backup")
        try:
            tables = await data.export_data(
                work,
                client,
                progress=(lambda table, rows: progress("database", rows, 0, table))
                if progress
                else None,
            )
        finally:
            await client.close()
        files = collect(
            root, categories, _database_files(db_url, root) | {destination, part, work}
        )
        manifest = {
            "format": FORMAT,
            "created_at": now.astimezone().isoformat(timespec="seconds"),
            "bot_version": dependencies["bot_version"],
            "categories": sorted(categories),
            "files": len(files),
            "bytes": sum(size for _, size in files),
            "database": {
                "dialect": tables["dialect"],
                "tables": {k: v["rows"] for k, v in tables["tables"].items()},
            },
            "encrypted": bool(password),
        }
        await asyncio.to_thread(
            _write, part, root, work, files, manifest, dependencies, password, progress
        )
        part.replace(destination)
    finally:
        shutil.rmtree(work, ignore_errors=True)
        part.unlink(missing_ok=True)
    return destination, manifest


def is_encrypted(path: Path) -> bool:
    """备份包是否加密"""
    try:
        with zipfile.ZipFile(path) as archive:
            return any(info.flag_bits & 1 for info in archive.infolist())
    except (OSError, zipfile.BadZipFile) as error:
        raise BackupError("备份包无法读取或已损坏") from error


def _read_json(archive, name: str) -> dict:
    try:
        return json.loads(archive.read(name))
    except KeyError:
        raise BackupError(f"备份包缺少 {name}") from None
    except RuntimeError as error:
        raise BackupError("备份包密码错误") from error
    except (ValueError, zipfile.BadZipFile) as error:
        raise BackupError(f"备份包中的 {name} 已损坏") from error


def _unrestorable(names: list[str]) -> list[str]:
    """备份包中无法在当前系统上按原名写出的文件

    Windows 的文件名不能包含 <>:"|?*、反斜杠与控制字符，不能以点或空格结尾，也不能
    使用 CON、NUL 等设备名；Windows 与 macOS 不区分大小写，只有大小写不同的文件只
    保留第一个。

    参数:
        names: 备份包中的文件名

    返回:
        list[str]: 无法写出的文件名
    """
    windows = os.name == "nt"
    folded = windows or sys.platform == "darwin"
    seen: set[str] = set()
    result = []
    for name in names:
        if name.endswith("/"):
            continue
        key = name.casefold() if folded else name
        if key in seen or (
            windows
            and any(
                _WINDOWS_INVALID.search(part)
                or part.endswith((".", " "))
                or part.split(".", 1)[0].rstrip().upper() in _WINDOWS_DEVICES
                for part in PurePosixPath(name).parts
            )
        ):
            result.append(name)
        seen.add(key)
    return result


def read_manifest(path: Path, password: str | None = None) -> dict:
    """读取备份包清单与依赖清单

    参数:
        path: 备份包路径
        password: 加密备份的密码

    返回:
        dict: 清单，dependencies 字段为依赖清单，unrestorable_files 为当前系统上
            无法按原名写出、恢复时跳过的文件
    """
    if is_encrypted(path) and not password:
        raise BackupError("备份包已加密，需要密码")
    with _open(path, "r", password) as archive:
        manifest = _read_json(archive, MANIFEST)
        if manifest.get("format") != FORMAT:
            raise BackupError(f"不支持的备份格式: {manifest.get('format')}")
        manifest["dependencies"] = _read_json(archive, DEPENDENCIES)
        manifest["unrestorable_files"] = _unrestorable(archive.namelist())
    return manifest


def extract(path: Path, target: Path, password: str | None = None) -> dict:
    """解压备份包到目标目录，拒绝越出目标目录的路径

    当前系统上无法按原名写出的文件（见 unrestorable_files）跳过，类 Unix 系统上保留
    文件的可执行权限。

    参数:
        path: 备份包路径
        target: 解压目录，解压后包含 database/ 与 files/
        password: 加密备份的密码

    返回:
        dict: 备份清单
    """
    manifest = read_manifest(path, password)
    skipped = set(manifest["unrestorable_files"])
    target = target.resolve()
    with _open(path, "r", password) as archive:
        for info in archive.infolist():
            name = PurePosixPath(info.filename)
            if info.filename.startswith("/") or ".." in name.parts:
                raise BackupError(f"备份包包含非法路径: {info.filename}")
            if info.is_dir() or info.filename in skipped:
                continue
            output = target.joinpath(*name.parts)
            try:
                output.parent.mkdir(parents=True, exist_ok=True)
                with archive.open(info) as source, output.open("wb") as file:
                    shutil.copyfileobj(source, file, 1024 * 1024)
            except RuntimeError as error:
                raise BackupError("备份包密码错误") from error
            except zipfile.BadZipFile as error:
                raise BackupError(f"备份包文件已损坏: {info.filename}") from error
            except OSError as error:
                raise BackupError(
                    f"无法解压备份中的文件 {info.filename}: {error}"
                ) from error
            mode = info.external_attr >> 16 & 0o777
            if os.name != "nt" and mode & 0o111:
                output.chmod(mode)
    return manifest


def list_backups(root: Path | None = None) -> list[dict]:
    """备份目录中的备份包，按时间倒序

    参数:
        root: 项目根目录

    返回:
        list[dict]: [{name, size, created_at, encrypted}]
    """
    directory = (root or Path.cwd()) / BACKUP_DIR
    result = []
    for path in directory.glob(f"*{SUFFIX}"):
        try:
            stat = path.stat()
            encrypted = is_encrypted(path)
        except (OSError, BackupError):
            continue
        result.append(
            {
                "name": path.name,
                "size": stat.st_size,
                "created_at": datetime.fromtimestamp(stat.st_mtime)
                .astimezone()
                .isoformat(timespec="seconds"),
                "encrypted": encrypted,
            }
        )
    return sorted(result, key=lambda item: item["created_at"], reverse=True)


def backup_path(name: str, root: Path | None = None) -> Path:
    """按文件名定位备份目录中的备份包

    参数:
        name: 备份包文件名
        root: 项目根目录

    返回:
        Path: 备份包路径
    """
    path = (root or Path.cwd()) / BACKUP_DIR / name
    if (
        PurePosixPath(name).name != name
        or "\\" in name
        or not name.endswith(SUFFIX)
        or not path.is_file()
    ):
        raise BackupError("备份包不存在")
    return path


class ExportTask:
    """后台导出任务，同一时间只运行一个"""

    status: ClassVar[dict] = {"state": "idle"}
    _task: ClassVar[asyncio.Task | None] = None

    @classmethod
    def start(
        cls, categories, extras: list[str] | None, password: str | None = None
    ) -> dict:
        """开始导出

        参数:
            categories: 备份的文件分类
            extras: 高级模式选择记录的依赖包名，为 None 时记录全部依赖
            password: 加密密码

        返回:
            dict: 任务状态
        """
        if cls._task and not cls._task.done():
            raise BackupError("已有导出任务正在进行")
        if unknown := set(categories) - set(CATEGORIES):
            raise BackupError(f"未知的备份分类: {', '.join(sorted(unknown))}")
        cls.status = {
            "state": "running",
            "stage": "database",
            "done": 0,
            "total": 0,
            "table": "",
        }

        def progress(stage: str, done: int, total: int, table: str):
            cls.status.update(stage=stage, done=done, total=total, table=table)

        async def run():
            from zhenxun.services.log import logger

            try:
                path, manifest = await export_backup(
                    categories, extras, password, progress=progress
                )
                cls.status = {"state": "done", "name": path.name, "manifest": manifest}
                logger.info(f"备份导出完成: {path.name}", "备份")
            except Exception as error:
                cls.status = {"state": "failed", "error": str(error)}
                logger.error("备份导出失败", "备份", e=error)

        cls._task = asyncio.create_task(run())
        return cls.status


class UploadOffsetError(BackupError):
    """分片偏移与已接收的大小不一致"""

    def __init__(self, offset: int):
        super().__init__("分片位置与已上传进度不一致")
        self.offset = offset


class ChunkUpload:
    """分片上传备份包：每片单独请求，可从已接收的位置续传"""

    CHUNK_SIZE = 32 * 1024 * 1024
    """建议的分片大小，需低于反向代理的单次请求体上限"""
    CHUNK_LIMIT = 64 * 1024 * 1024
    """单个分片允许的最大字节数"""
    TTL = 24 * 3600
    """未完成上传的保留时间（秒）"""
    _lock = threading.Lock()

    @staticmethod
    def _files(upload_id: str, root: Path | None) -> tuple[Path, Path]:
        if not re.fullmatch(r"[\w-]{16,64}", upload_id):
            raise BackupError("上传任务不存在或已过期")
        directory = (root or Path.cwd()) / UPLOAD_DIR
        return directory / f"{upload_id}.part", directory / f"{upload_id}.json"

    @classmethod
    def _meta(cls, upload_id: str, root: Path | None) -> tuple[Path, Path, dict]:
        part, meta = cls._files(upload_id, root)
        try:
            return part, meta, json.loads(meta.read_text("utf-8"))
        except (OSError, ValueError):
            raise BackupError("上传任务不存在或已过期") from None

    @classmethod
    def start(cls, name: str, size: int, root: Path | None = None) -> str:
        """创建上传任务，并清理过期的未完成上传

        参数:
            name: 备份包文件名
            size: 备份包总字节数
            root: 项目根目录

        返回:
            str: 上传任务 ID
        """
        stem = PurePosixPath(name.replace("\\", "/")).name
        if not stem.endswith(SUFFIX) or stem.startswith("."):
            raise BackupError("只能上传 .zx 备份包")
        if size <= 0:
            raise BackupError("备份包为空")
        directory = (root or Path.cwd()) / UPLOAD_DIR
        directory.mkdir(parents=True, exist_ok=True)
        expired = time.time() - cls.TTL
        for item in directory.iterdir():
            with contextlib.suppress(OSError):
                if item.stat().st_mtime < expired:
                    item.unlink()
        upload_id = secrets.token_urlsafe(16)
        part, meta = cls._files(upload_id, root)
        part.touch()
        meta.write_text(json.dumps({"name": stem, "size": size}), "utf-8")
        return upload_id

    @classmethod
    def offset(cls, upload_id: str, root: Path | None = None) -> int:
        """已接收的字节数，用于断点续传

        参数:
            upload_id: 上传任务 ID
            root: 项目根目录

        返回:
            int: 已接收的字节数
        """
        part, _, _ = cls._meta(upload_id, root)
        return part.stat().st_size

    @classmethod
    def write(
        cls, upload_id: str, offset: int, chunk: bytes, root: Path | None = None
    ) -> int:
        """在指定位置追加一个分片

        参数:
            upload_id: 上传任务 ID
            offset: 分片在文件中的起始位置，须等于已接收的字节数
            chunk: 分片内容
            root: 项目根目录

        返回:
            int: 写入后已接收的字节数
        """
        with cls._lock:
            part, _, info = cls._meta(upload_id, root)
            received = part.stat().st_size
            if offset != received:
                raise UploadOffsetError(received)
            if received + len(chunk) > info["size"]:
                raise BackupError("上传内容超出备份包大小")
            with part.open("ab") as file:
                file.write(chunk)
            return received + len(chunk)

    @classmethod
    def finish(cls, upload_id: str, root: Path | None = None) -> Path:
        """校验已接收的备份包并移入备份目录，重名时自动编号

        参数:
            upload_id: 上传任务 ID
            root: 项目根目录

        返回:
            Path: 保存后的路径
        """
        with cls._lock:
            part, meta, info = cls._meta(upload_id, root)
            if part.stat().st_size != info["size"]:
                raise BackupError("备份包尚未上传完整")
            try:
                is_encrypted(part)
            except BackupError:
                cls.cancel(upload_id, root)
                raise
            directory = (root or Path.cwd()) / BACKUP_DIR
            stem: str = info["name"]
            path, index = directory / stem, 1
            while path.exists():
                path = directory / f"{stem[: -len(SUFFIX)]}-{index}{SUFFIX}"
                index += 1
            part.replace(path)
            meta.unlink(missing_ok=True)
            return path

    @classmethod
    def cancel(cls, upload_id: str, root: Path | None = None):
        """放弃上传任务并删除已接收的内容

        参数:
            upload_id: 上传任务 ID
            root: 项目根目录
        """
        for item in cls._files(upload_id, root):
            item.unlink(missing_ok=True)
