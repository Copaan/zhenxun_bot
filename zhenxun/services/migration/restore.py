"""从备份包恢复与切换数据库

在线阶段解压备份包、导出当前数据库作为回滚点并请求重启；启动器在启动 worker
前应用依赖与文件；worker 建表后导入数据。导入失败时启动器还原文件，worker 再
导入回滚点。
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from datetime import datetime
import json
from pathlib import Path
import shutil
from typing import Any
from urllib.parse import urlsplit, urlunsplit

STATE_DIR = Path("data/runtime/restore")
REQUEST = STATE_DIR / "request.json"
PACKAGE = STATE_DIR / "package"
ROLLBACK = STATE_DIR / "rollback"
RUNNING = frozenset({"pending", "importing", "failed_import", "rollback_import"})

Restart = Callable[[], Awaitable[tuple[bool, str]]]


class RestoreError(RuntimeError):
    """恢复或切换数据库失败"""


def _now() -> str:
    return datetime.now().astimezone().isoformat(timespec="seconds")


def _load() -> dict | None:
    try:
        return json.loads(REQUEST.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None


def _save(request: dict, **changes) -> dict:
    request.update(changes, updated_at=_now())
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    temporary = REQUEST.with_suffix(".tmp")
    temporary.write_text(json.dumps(request, ensure_ascii=False), encoding="utf-8")
    temporary.replace(REQUEST)
    return request


def _mask(url: str | None) -> str | None:
    if not url:
        return url
    parts = urlsplit(url)
    if not parts.password:
        return url
    host = parts.netloc.rsplit("@", 1)[1]
    return urlunsplit(parts._replace(netloc=f"{parts.username}:***@{host}"))


def _current_db_url() -> str:
    from zhenxun.configs.config import BotConfig

    return BotConfig.db_url


def _message(error: BaseException) -> str:
    code = getattr(error, "code", None)
    return f"{code}: {error}" if code and str(error) != code else str(error)


def status() -> dict:
    """当前或最近一次恢复 / 切换的状态，数据库密码已隐藏

    返回:
        dict: 状态，没有记录时 state 为 idle
    """
    request = _load()
    if request is None:
        return {"state": "idle"}
    return {
        **request,
        "db_url": _mask(request.get("db_url")),
        "previous_db_url": _mask(request.get("previous_db_url")),
    }


def dismiss() -> None:
    """清除已结束的恢复记录"""
    request = _load()
    if request and request["state"] in RUNNING:
        raise RestoreError("恢复仍在进行中")
    shutil.rmtree(STATE_DIR, ignore_errors=True)


def needs_apply() -> bool:
    """启动器是否需要在启动 worker 前执行恢复步骤"""
    request = _load()
    return bool(request and request["state"] in {"pending", "failed_import"})


def importing() -> bool:
    """worker 是否正在导入恢复数据"""
    request = _load()
    return bool(request and request["state"] in {"importing", "rollback_import"})


def inspect(path: Path, password: str | None = None) -> dict:
    """读取备份包摘要，并给出与当前环境的差异提示

    参数:
        path: 备份包路径
        password: 加密备份的密码

    返回:
        dict: 摘要
    """
    import platform
    import sys

    from . import backup

    manifest = backup.read_manifest(path, password)
    saved = manifest.pop("dependencies")
    warnings = []
    python = platform.python_version()
    if saved["python"].rsplit(".", 1)[0] != python.rsplit(".", 1)[0]:
        warnings.append(f"备份来自 Python {saved['python']}，当前为 {python}")
    current = f"{sys.platform}-{platform.machine()}".lower()
    if saved["platform"] != current:
        warnings.append(f"备份来自 {saved['platform']}，部分依赖可能需要重新解析")
    return {
        **manifest,
        "plugins": sorted(saved["plugins"]),
        "manual_requirements": sorted(saved["manual_requirements"]),
        "extra_requirements": sorted(saved["extra_requirements"]),
        "python": saved["python"],
        "platform": saved["platform"],
        "warnings": warnings,
        "configuration_preview": (
            configuration_preview(path, password)
            if "configuration" in manifest["categories"]
            else None
        ),
    }


def configuration_preview(
    path: Path | None = None, password: str | None = None
) -> dict:
    """读取备份中恢复后会生效的 .env 配置与 WebUI 管理员账号

    参数:
        path: 备份包路径，为空时读取已解压的待恢复备份
        password: 加密备份的密码

    返回:
        dict: environment 为 .env 配置项，username 为管理员账号，缺失时为空
    """
    import contextlib
    from io import StringIO

    from dotenv import dotenv_values
    from ruamel.yaml import YAML

    from . import backup

    names = (".env.dev", ".env", "data/config.yaml", "data/configs/plugins2config.yaml")
    contents: dict[str, str] = {}
    if path is None:
        for name in names:
            with contextlib.suppress(OSError, UnicodeDecodeError):
                contents[name] = (PACKAGE / "files" / name).read_text("utf-8")
    else:
        with backup._open(path, "r", password) as archive:
            for name in names:
                with contextlib.suppress(KeyError, UnicodeDecodeError):
                    contents[name] = archive.read(f"files/{name}").decode("utf-8")

    def webui(name: str) -> dict:
        try:
            data = YAML(typ="safe").load(contents.get(name, "")) or {}
            return data.get("web-ui") or {}
        except Exception:
            return {}

    text = contents.get(".env.dev") or contents.get(".env")
    environment = dict(dotenv_values(stream=StringIO(text))) if text else {}
    username = webui("data/config.yaml").get("USERNAME") or (
        webui("data/configs/plugins2config.yaml").get("USERNAME") or {}
    ).get("value")
    return {"environment": environment, "username": username}


def _idle() -> None:
    request = _load()
    if request and request["state"] in RUNNING:
        raise RestoreError("已有恢复或切换数据库的任务正在进行")
    shutil.rmtree(STATE_DIR, ignore_errors=True)
    STATE_DIR.mkdir(parents=True)


async def _export(db_url: str, directory: Path) -> None:
    from . import data

    client = await data.open_client(db_url, "zx_restore_export")
    try:
        await data.export_data(directory, client)
    finally:
        await client.close()


async def _restart() -> tuple[bool, str]:
    from zhenxun.utils._restart_utils import request_restart

    return await request_restart("backup-restore")


async def _submit(request: dict, restart: Restart | None) -> dict:
    _save(request)
    ok, message = await (restart or _restart)()
    if not ok:
        shutil.rmtree(STATE_DIR, ignore_errors=True)
        raise RestoreError(message)
    return status()


async def request_restore(
    path: Path,
    password: str | None,
    categories: list[str],
    skip_failed: bool = False,
    restart: Restart | None = None,
) -> dict:
    """准备从备份包恢复并请求重启

    参数:
        path: 备份包路径
        password: 加密备份的密码
        categories: 恢复的文件分类
        skip_failed: 依赖无法安装时是否跳过对应插件继续
        restart: 请求重启的函数，默认直接请求启动器重启

    返回:
        dict: 恢复状态
    """
    from . import backup

    _idle()
    try:
        manifest = await asyncio.to_thread(backup.extract, path, PACKAGE, password)
        if unknown := set(categories) - set(manifest["categories"]):
            raise RestoreError(f"备份中没有这些分类: {', '.join(sorted(unknown))}")
        db_url = _current_db_url()
        await _export(db_url, ROLLBACK / "database")
    except BaseException:
        shutil.rmtree(STATE_DIR, ignore_errors=True)
        raise
    return await _submit(
        {
            "kind": "restore",
            "state": "pending",
            "name": path.name,
            "categories": sorted(categories),
            "skip_failed": skip_failed,
            "db_url": db_url,
            "previous_db_url": db_url,
            "created_at": _now(),
        },
        restart,
    )


async def _require_empty(db_url: str) -> None:
    from . import data

    try:
        client = await data.open_client(db_url, "zx_switch_check")
    except Exception as error:
        raise RestoreError(f"无法连接目标数据库: {error}") from error
    try:
        for model in data._models():
            table = model._meta.db_table
            query = client.query_class.from_(table).select(1).limit(1).get_sql()
            try:
                rows = (await client.execute_query(query))[1]
            except Exception:
                continue
            if rows:
                raise RestoreError(f"目标数据库已有数据（表 {table}），请使用空数据库")
    finally:
        await client.close()


async def request_switch(db_url: str, restart: Restart | None = None) -> dict:
    """把当前数据复制到新数据库，重启后切换 DB_URL

    参数:
        db_url: 新数据库地址，须为空库
        restart: 请求重启的函数，默认直接请求启动器重启

    返回:
        dict: 切换状态
    """
    current = _current_db_url()
    if db_url == current:
        raise RestoreError("新数据库与当前数据库相同")
    await _require_empty(db_url)
    _idle()
    try:
        await _export(current, PACKAGE / "database")
    except BaseException:
        shutil.rmtree(STATE_DIR, ignore_errors=True)
        raise
    return await _submit(
        {
            "kind": "switch",
            "state": "pending",
            "categories": [],
            "db_url": db_url,
            "previous_db_url": current,
            "created_at": _now(),
        },
        restart,
    )


class _Files:
    """恢复文件前记录被覆盖与新建的文件，用于回滚"""

    def __init__(self, root: Path):
        self.root = root
        self.record = ROLLBACK / "files.json"
        self.saved: list[str] = []
        self.created: list[str] = []

    def protect(self, relative: str) -> None:
        if relative in self.saved or relative in self.created:
            return
        target = self.root / relative
        if target.is_file():
            backup = ROLLBACK / "files" / relative
            backup.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(target, backup)
            self.saved.append(relative)
        else:
            self.created.append(relative)
        self.record.write_text(
            json.dumps({"saved": self.saved, "created": self.created}),
            encoding="utf-8",
        )

    def write(self, relative: str, source: Path) -> None:
        self.protect(relative)
        target = self.root / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, target)

    def rollback(self) -> None:
        try:
            record = json.loads(self.record.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            return
        for relative in record["saved"]:
            target = self.root / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(ROLLBACK / "files" / relative, target)
        for relative in record["created"]:
            (self.root / relative).unlink(missing_ok=True)


def _environment_file(root: Path) -> str:
    from zhenxun.configs.environment import environment_file

    return environment_file(root=root).relative_to(root).as_posix()


def _write_files(request: dict, root: Path, files: _Files) -> None:
    from dotenv import set_key

    from zhenxun.services.nonebot_store.storage import read_json, write_json

    from . import backup
    from .deps import RECEIPTS_FILE

    files.protect(_environment_file(root))
    source = PACKAGE / "files"
    if source.is_dir():
        for path in sorted(source.rglob("*")):
            relative = path.relative_to(source).as_posix()
            if (
                path.is_file()
                and backup.category_for(relative) in request["categories"]
            ):
                files.write(relative, path)
    if "plugins" in request["categories"]:
        saved = json.loads((PACKAGE / backup.DEPENDENCIES).read_text("utf-8"))
        files.protect(RECEIPTS_FILE.as_posix())
        receipts = read_json(root / RECEIPTS_FILE, {})
        write_json(root / RECEIPTS_FILE, {**receipts, **saved["store_receipts"]})
    environment = _environment_file(root)
    files.protect(environment)
    set_key(root / environment, "DB_URL", request["db_url"], quote_mode="always")


def _apply(request: dict, root: Path) -> None:
    from zhenxun.services.nonebot_store.storage import clear_pending_transaction

    from . import backup, deps

    try:
        if request["kind"] == "restore":
            saved = json.loads((PACKAGE / backup.DEPENDENCIES).read_text("utf-8"))
            report = asyncio.run(deps.stage_restore(saved, request["skip_failed"]))
            _save(request, dependencies=report)
    except Exception as error:
        _save(request, state="failed", error=f"依赖无法安装：{_message(error)}")
        return
    files = _Files(root)
    try:
        _write_files(request, root, files)
    except Exception as error:
        files.rollback()
        if request.get("dependencies", {}).get("staged"):
            clear_pending_transaction()
        _save(request, state="failed", error=f"文件恢复失败：{_message(error)}")
        return
    _save(request, state="importing")


def apply_pending(root: Path | None = None) -> bool:
    """启动器在启动 worker 前执行的恢复步骤：应用恢复或回滚导入失败的恢复

    参数:
        root: 项目根目录

    返回:
        bool: 是否执行了步骤
    """
    root = root or Path.cwd()
    request = _load()
    if request is None:
        return False
    if request["state"] == "pending":
        _apply(request, root)
    elif request["state"] == "failed_import":
        _Files(root).rollback()
        _save(
            request,
            state="rollback_import" if request["kind"] == "restore" else "rolled_back",
        )
        if request["kind"] == "switch":
            shutil.rmtree(PACKAGE, ignore_errors=True)
    else:
        return False
    return True


async def import_pending() -> None:
    """worker 建表后导入待恢复的数据，失败时请求重启以回滚"""
    request = _load()
    if request is None or request["state"] not in {"importing", "rollback_import"}:
        return
    from tortoise import Tortoise

    from zhenxun.services.log import logger
    from zhenxun.utils._restart_utils import request_restart

    from . import data

    rollback = request["state"] == "rollback_import"
    source = (ROLLBACK if rollback else PACKAGE) / "database"
    progress: dict[str, Any] = {}

    def update(table: str, rows: int):
        progress.update(table=table, rows=rows)
        _save(request, progress=progress)

    logger.info("正在导入回滚数据" if rollback else "正在导入恢复数据", "恢复")
    try:
        report = await data.import_data(
            source, Tortoise.get_connection("default"), replace=True, progress=update
        )
    except Exception as error:
        logger.error("恢复数据导入失败", "恢复", e=error)
        if rollback:
            _save(request, state="rollback_failed", rollback_error=_message(error))
            return
        _save(request, state="failed_import", error=_message(error))
        await request_restart("backup-restore-rollback")
        return
    request.pop("progress", None)
    if rollback:
        _save(request, state="rolled_back")
    else:
        _save(request, state="done", report=report)
    shutil.rmtree(PACKAGE, ignore_errors=True)
    shutil.rmtree(ROLLBACK, ignore_errors=True)
    logger.info("恢复数据导入完成", "恢复")
