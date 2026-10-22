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
import time
from typing import Any
from urllib.parse import urlsplit, urlunsplit

STATE_DIR = Path("data/runtime/restore")
REQUEST = STATE_DIR / "request.json"
PACKAGE = STATE_DIR / "package"
ROLLBACK = STATE_DIR / "rollback"
RUNNING = frozenset({"pending", "importing", "failed_import", "rollback_import"})
LOCAL_KEYS = frozenset(
    {
        "ENVIRONMENT",
        "HOST",
        "PORT",
        "DRIVER",
        "CACHE_MODE",
        "REDIS_HOST",
        "REDIS_PORT",
        "REDIS_PASSWORD",
        "WEBUI_HTTPS_ENABLED",
        "WEBUI_HTTP_MODE",
        "WEBUI_HTTP_REDIRECT_ENABLED",
        "WEBUI_HTTP_REDIRECT_PORT",
        "WEBUI_TLS_CERTFILE",
        "WEBUI_TLS_KEYFILE",
        "QQ_WEBHOOK_MODE",
        "QQ_WEBHOOK_LISTEN_HOST",
        "QQ_WEBHOOK_LISTEN_PORT",
        "QQ_WEBHOOK_PUBLIC_BASE_URL",
        "QQ_WEBHOOK_TLS_CERTFILE",
        "QQ_WEBHOOK_TLS_KEYFILE",
    }
)
"""恢复 .env 时保留本机值的配置项（监听地址、证书、缓存等与部署机器相关的配置）"""

Restart = Callable[[], Awaitable[tuple[bool, str]]]


class RestoreError(RuntimeError):
    """恢复或切换数据库失败"""


def _now() -> str:
    return datetime.now().astimezone().isoformat(timespec="seconds")


def _load() -> dict | None:
    from zhenxun.utils.atomic_json import read_json_locked

    try:
        request = read_json_locked(REQUEST, None)
    except OSError:
        return None
    return request if isinstance(request, dict) else None


def _save(request: dict, **changes) -> dict:
    from zhenxun.utils.atomic_json import write_json_locked

    request.update(changes, updated_at=_now())
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    write_json_locked(REQUEST, request)
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
    text = str(error)
    return f"{code}: {text}" if code and not text.startswith(code) else text


def status() -> dict:
    """当前或最近一次恢复 / 切换的状态，数据库密码已隐藏

    恢复写入了依赖层事务时，dependencies.layer 为该事务的应用结果。

    返回:
        dict: 状态，没有记录时 state 为 idle
    """
    from . import restore_deps

    request = _load()
    if request is None:
        return {"state": "idle"}
    report = request.get("dependencies") or {}
    if report.get("revision"):
        request["dependencies"] = {
            **report,
            "layer": restore_deps.layer_status(report["revision"]),
        }
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
    if skipped := manifest["unrestorable_files"]:
        names = "、".join(name.removeprefix("files/") for name in skipped[:5])
        warnings.append(
            f"{len(skipped)} 个文件的文件名在当前系统上无效，恢复时将跳过：{names}"
            + ("等" if len(skipped) > 5 else "")
        )
    return {
        **manifest,
        "plugins": sorted(saved["plugins"]),
        "manual_requirements": sorted(saved["manual_requirements"]),
        "extra_requirements": sorted(saved["extra_requirements"]),
        "unsupported_requirements": sorted(saved.get("unsupported_requirements") or {}),
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
    """读取恢复后会生效的 .env 配置与 WebUI 管理员账号，LOCAL_KEYS 取本机值

    参数:
        path: 备份包路径，为空时读取已解压的待恢复备份
        password: 加密备份的密码

    返回:
        dict: environment 为 .env 配置项，username 为管理员账号，缺失时为空，
            password_set 为是否设置了管理员密码
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
    if environment:
        local = dotenv_values(_environment_path(Path()))
        environment = {
            key: value
            for key, value in environment.items()
            if key.upper() not in LOCAL_KEYS
        }
        environment.update(
            (key, value) for key, value in local.items() if key.upper() in LOCAL_KEYS
        )

    def value(key: str):
        return webui("data/config.yaml").get(key) or (
            webui("data/configs/plugins2config.yaml").get(key) or {}
        ).get("value")

    return {
        "environment": environment,
        "username": value("USERNAME"),
        "password_set": bool(value("PASSWORD")),
    }


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
    db_url: str | None = None,
) -> dict:
    """准备从备份包恢复并请求重启

    所选分类中文件名在当前系统上无效的文件不恢复，记录在 unrestorable_files。

    参数:
        path: 备份包路径
        password: 加密备份的密码
        categories: 恢复的文件分类
        skip_failed: 依赖无法安装时是否跳过对应插件继续
        restart: 请求重启的函数，默认直接请求启动器重启
        db_url: 恢复到的新数据库（首次部署时使用），须为空库；
            指定时不导出当前数据库，导入失败只回滚文件

    返回:
        dict: 恢复状态
    """
    from zhenxun.services.nonebot_store.storage import active_transaction

    from . import backup

    transaction = active_transaction()
    if transaction and transaction.get("state") not in {"pending_restart", "failed"}:
        raise RestoreError(
            f"插件依赖有未完成的变更（{transaction.get('state')}），"
            "请等待完成或在插件商店处理后再恢复"
        )
    current = _current_db_url()
    if db_url:
        await _require_empty(db_url)
    _idle()
    try:
        manifest = await asyncio.to_thread(backup.extract, path, PACKAGE, password)
        if unknown := set(categories) - set(manifest["categories"]):
            raise RestoreError(f"备份中没有这些分类: {', '.join(sorted(unknown))}")
        if not db_url:
            await _export(current, ROLLBACK / "database")
    except BaseException:
        shutil.rmtree(STATE_DIR, ignore_errors=True)
        raise
    unrestorable = [
        name.removeprefix("files/")
        for name in manifest["unrestorable_files"]
        if backup.category_for(name.removeprefix("files/")) in categories
    ]
    return await _submit(
        {
            "kind": "restore",
            "state": "pending",
            "name": path.name,
            "categories": sorted(categories),
            "unrestorable_files": unrestorable,
            "skip_failed": skip_failed,
            "db_url": db_url or current,
            "previous_db_url": current,
            "created_at": _now(),
        },
        restart,
    )


TABLES_SQL = {
    "sqlite": "SELECT name FROM sqlite_master"
    " WHERE type = 'table' AND name NOT LIKE 'sqlite_%'",
    "mysql": "SELECT table_name FROM information_schema.tables"
    " WHERE table_schema = DATABASE() AND table_type = 'BASE TABLE'",
    "postgres": "SELECT tablename FROM pg_tables WHERE schemaname = current_schema()",
}


async def _require_empty(db_url: str) -> None:
    """检查目标数据库中已有的表都没有数据，不依赖已注册的模型

    参数:
        db_url: 目标数据库地址
    """
    from . import data

    try:
        client = await data.open_client(db_url, "zx_switch_check")
    except Exception as error:
        raise RestoreError(f"无法连接目标数据库: {error}") from error
    try:
        sql = TABLES_SQL[client.capabilities.dialect]
        tables = [
            next(iter(row.values())) for row in await client.execute_query_dict(sql)
        ]
        for table in sorted(tables):
            query = client.query_class.from_(table).select(1).limit(1).get_sql()
            if (await client.execute_query(query))[1]:
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


def _environment_path(root: Path) -> Path:
    from zhenxun.configs.environment import environment_file

    return environment_file(root=root)


def _environment_file(root: Path) -> str:
    return _environment_path(root).relative_to(root).as_posix()


def _keep_local_keys(target: Path, local: dict[str, str | None]) -> None:
    """把恢复后的 .env 文件中 LOCAL_KEYS 的值改回本机原值，本机未设置的项删除

    参数:
        target: 恢复后的 .env 文件
        local: 恢复前本机该文件的配置项
    """
    from dotenv import dotenv_values, set_key, unset_key

    kept = {
        key.upper(): (key, value)
        for key, value in local.items()
        if key.upper() in LOCAL_KEYS and value is not None
    }
    for key in dotenv_values(target):
        upper = key.upper()
        if upper not in LOCAL_KEYS:
            continue
        if upper in kept and kept[upper][0] == key:
            set_key(target, key, kept.pop(upper)[1], quote_mode="always")
        else:
            unset_key(target, key)
    for key, value in kept.values():
        set_key(target, key, value, quote_mode="always")


def _write_files(request: dict, root: Path, files: _Files) -> None:
    from dotenv import dotenv_values, set_key

    from zhenxun.services.nonebot_store.storage import read_json, write_json
    from zhenxun.services.plugin_store.plugin_store_receipts import RECEIPT_FILE

    from . import backup

    files.protect(_environment_file(root))
    local = {
        path.name: dotenv_values(path) for path in root.glob(".env*") if path.is_file()
    }
    source = PACKAGE / "files"
    if source.is_dir():
        for path in sorted(source.rglob("*")):
            relative = path.relative_to(source).as_posix()
            if (
                path.is_file()
                and backup.category_for(relative) in request["categories"]
            ):
                files.write(relative, path)
                if relative in local:
                    _keep_local_keys(root / relative, local[relative])
    if "plugins" in request["categories"]:
        saved = json.loads((PACKAGE / backup.DEPENDENCIES).read_text("utf-8"))
        files.protect(RECEIPT_FILE.as_posix())
        receipts = read_json(root / RECEIPT_FILE, {})
        write_json(root / RECEIPT_FILE, {**receipts, **saved["store_receipts"]})
    environment = _environment_file(root)
    files.protect(environment)
    set_key(root / environment, "DB_URL", request["db_url"], quote_mode="always")


def _apply(request: dict, root: Path) -> None:
    from zhenxun.services.nonebot_store.storage import clear_pending_transaction

    from . import backup, restore_deps

    try:
        if request["kind"] == "restore":
            saved = json.loads((PACKAGE / backup.DEPENDENCIES).read_text("utf-8"))
            files = PACKAGE / "files" if "plugins" in request["categories"] else None
            report = asyncio.run(
                restore_deps.stage_restore(saved, request["skip_failed"], files)
            )
            _save(request, dependencies=report)
    except Exception as error:
        _save(
            request,
            state="failed",
            error=f"依赖无法安装：{_message(error)}",
            error_code=getattr(error, "code", None),
            error_detail=restore_deps.describe(getattr(error, "details", None)),
        )
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
        rollback = (ROLLBACK / "database").is_dir()
        _save(request, state="rollback_import" if rollback else "rolled_back")
        if not rollback:
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
    saved_at = 0.0

    def update(table: str, rows: int):
        nonlocal saved_at
        changed = progress.get("table") != table
        progress.update(table=table, rows=rows)
        now = time.monotonic()
        if not changed and now - saved_at < 1:
            return
        saved_at = now
        try:
            _save(request, progress=progress)
        except OSError as error:
            logger.warning("恢复进度写入失败", "恢复", e=error)

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
