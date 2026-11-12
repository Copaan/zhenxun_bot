"""按持久化父任务协调依赖、文件、数据导入、启动验证及回滚"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from hashlib import sha256
import json
import os
from pathlib import Path
import shutil
import time
from urllib.parse import urlsplit, urlunsplit
import zipfile

from . import policy
from . import state as tasks

STATE_DIR = tasks.ROOT
REQUEST = tasks.REQUEST
RUNNING = frozenset(
    {
        "preparing",
        "pending",
        "snapshot_ready",
        "applying",
        "awaiting_orm",
        "importing",
        "verifying",
        "committing",
        "failed_import",
        "rollback_import",
        "rollback_target",
        "rollback_verifying",
        "recovery_required",
    }
)
_tasks: dict[str, asyncio.Task] = {}


def _paths(request: dict) -> tuple[Path, Path]:
    root = tasks.task_path(request["operation_id"])
    return root / "package", root / "rollback"


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


def _load() -> dict | None:
    return tasks.load()


def _save(request: dict, **changes) -> dict:
    global _gate_checked

    try:
        return tasks.save(request, **changes)
    finally:
        _gate_checked = 0.0


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
    from zhenxun.services.nonebot_store.dependencies import safe_process_error

    text = safe_process_error(str(error))
    return f"{code}: {text}" if code and not text.startswith(code) else text


def status(operation_id: str | None = None) -> dict:
    """当前或最近一次恢复 / 切换的状态，数据库密码已隐藏

    恢复写入了依赖层事务时，dependencies.layer 为该事务的应用结果。

    返回:
        dict: 状态，没有记录时 state 为 idle
    """
    from . import restore_deps

    try:
        request = tasks.load(operation_id)
    except (OSError, RuntimeError) as error:
        return {"state": "recovery_required", "error": _message(error)}
    if request is None:
        return {"state": "idle"}
    report = request.get("dependencies") or {}
    if report.get("revision"):
        request["dependencies"] = {
            **report,
            "layer": restore_deps.layer_status(report["revision"]),
        }
    return {
        **{
            key: value
            for key, value in request.items()
            if key
            in {
                "operation_id",
                "record_version",
                "revision",
                "state",
                "kind",
                "name",
                "categories",
                "created_at",
                "updated_at",
                "dependencies",
                "report",
                "unrestorable_files",
                "error",
                "error_code",
                "error_detail",
                "rollback_error",
                "cleanup_error",
                "verification_confirmed",
                "rollback_confirmed",
                "progress",
                "commit_decision",
                "cancel_requested",
                "rollback_components",
                "recovery_reason",
                "preparation",
                "original_modified",
                "restart_info",
                "skip_failed",
                "restore_mode",
                "backup_before_replace",
                "included_tables",
                "source_binding",
                "target_preview",
                "target_backup_status",
                "target_mutation_state",
            }
        },
        "historical_result": request.get("record_version") != 2,
        "cancel_requested": tasks.cancellation_requested(request)
        if request.get("record_version") == 2
        else False,
        "db_url": _mask(request.get("db_url")),
        "previous_db_url": _mask(request.get("previous_db_url")),
        "backup_available": bool(request.get("target_native_backup")),
        "backup_engine": (request.get("target_native_backup") or {}).get("engine"),
    }


def dismiss() -> None:
    """清除已结束的恢复记录"""
    request = _load()
    if request and request["state"] in RUNNING:
        raise RestoreError("恢复仍在进行中")
    if request:
        from zhenxun.services.runtime_mutation import mutation_guard
        from zhenxun.utils.atomic_json import write_json_locked

        with mutation_guard():
            write_json_locked(REQUEST, None)


def needs_apply() -> bool:
    """启动器是否需要在启动 worker 前执行恢复步骤"""
    request = _load()
    return bool(
        request
        and request.get("record_version") == 2
        and request["state"]
        in {
            "pending",
            "snapshot_ready",
            "applying",
            "failed_import",
            "committing",
            "rollback_target",
        }
    )


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
    from .preflight import archive_check

    try:
        archive_check(path, password)
    except (RuntimeError, OSError, zipfile.BadZipFile) as error:
        raise backup.BackupError(str(error)) from error
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
                contents[name] = (_paths(_load())[0] / "files" / name).read_text(
                    "utf-8"
                )
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
        local = dotenv_values(_environment_path(tasks.mutation.INSTANCE_ROOT))
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


async def _restart() -> tuple[bool, str]:
    from zhenxun.utils._restart_utils import request_restart

    return await request_restart("backup-restore")


def _worker_identity() -> dict:
    import psutil

    return {
        "pid": os.getpid(),
        "created_at": psutil.Process().create_time(),
        "startup_id": os.environ.get("ZHENXUN_WORKER_STARTUP_ID"),
    }


async def _submit(request: dict, restart: Restart | None) -> dict:
    from . import preparation

    if callable(getattr(restart, "prepare", None)):
        information = await restart.prepare()
        if isinstance(information, dict):
            _save(request, restart_info=information)
            await asyncio.sleep(2.5)
    if tasks.cancellation_requested(request):
        await preparation.cancel(request)
        return status(request["operation_id"])
    _save(request, state="pending")
    ok, message = await (restart or _restart)()
    if not ok:
        _save(request, state="preparing", error=message)
        raise RestoreError(message)
    if isinstance(getattr(restart, "data", None), dict):
        _save(request, restart_info=restart.data)
    return status(request["operation_id"])


async def _accepted(request: dict, callback) -> dict:
    """立即返回已占位任务，浏览器请求结束不取消在线准备。"""
    identity = request["operation_id"]
    if identity not in _tasks:

        async def execute():
            try:
                with tasks.owned(request):
                    await callback()
            finally:
                _tasks.pop(identity, None)

        task = asyncio.create_task(execute(), name=f"restore-prepare:{identity}")
        _tasks[identity] = task
        task.add_done_callback(
            lambda done: done.exception() if not done.cancelled() else None
        )
    return status(identity)


async def retry_preparation(
    operation_id: str,
    *,
    password: str | None = None,
    confirmation: str | None = None,
    restart: Restart | None = None,
    inputs: dict | None = None,
) -> dict:
    """显式续接原任务，不改变已绑定输入或复用其他执行者的候选。"""
    from . import preparation

    request = tasks.load(operation_id)
    if not request or request["state"] != "preparing":
        raise RestoreError("migration_preparation_not_retryable")
    for key in (
        "name",
        "categories",
        "skip_failed",
        "db_url",
        "restore_mode",
        "backup_before_replace",
        "included_tables",
        "source_binding",
    ):
        if key in (inputs or {}):
            value = inputs[key]
            if key in {"categories", "included_tables"} and value is not None:
                value = sorted(set(value))
            if value != request.get(key):
                raise RestoreError("migration_operation_input_changed")
    if account := (inputs or {}).get("setup_account"):
        from zhenxun.utils.passwords import verify_password

        saved_account = request.get("setup_account") or {}
        if (
            str(account.get("username") or "").strip() != saved_account.get("username")
            or account.get("password") != account.get("confirm_password")
            or not saved_account.get("password_hash")
            or not await asyncio.to_thread(
                verify_password,
                str(account.get("password") or ""),
                saved_account["password_hash"],
            )
        ):
            raise RestoreError("migration_operation_input_changed")
    if operation_id in _tasks:
        return status(operation_id)
    if password is not None and request.get("archive_password_digest") not in {
        None,
        sha256(password.encode()).hexdigest(),
    }:
        raise RestoreError("migration_operation_input_changed")
    if request.get("preparation", {}).get(
        "confirmation_required"
    ) and confirmation != request["preparation"].get("impact_digest"):
        raise RestoreError("migration_impact_confirmation_required")
    return await _accepted(
        request, lambda: preparation.run(request, password, restart, confirmation)
    )


async def stop_preparation() -> None:
    """关闭 worker 时先收尾准备子进程和文件操作，再交还任务执行权。"""
    running = list(_tasks.values())
    for task in running:
        task.cancel()
    if running:
        await asyncio.gather(*running, return_exceptions=True)


async def cancel_preparation(operation_id: str) -> None:
    """取消在线准备，不取消浏览器请求之外的无关任务。"""
    from . import preparation

    task = _tasks.get(operation_id)
    if task:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        return
    request = tasks.load(operation_id)
    if request and request["state"] == "preparing":
        with tasks.owned(request):
            preparation.claim(request)
            await preparation.cancel(request)


async def request_restore(
    path: Path,
    password: str | None,
    categories: list[str],
    skip_failed: bool = False,
    restart: Restart | None = None,
    db_url: str | None = None,
    *,
    operation_id: str | None = None,
    restore_mode: str | None = None,
    backup_before_replace: bool = True,
    included_tables: list[str] | None = None,
    source_binding: str | None = None,
    setup_account: dict | None = None,
) -> dict:
    """占位并准备恢复，完成停写后的最终快照才允许 launcher 应用"""
    from . import preparation

    current = _current_db_url()
    policy.mode({"restore_mode": restore_mode})
    if type(backup_before_replace) is not bool:
        raise RestoreError("migration_backup_option_invalid")
    if included_tables is not None:
        if (
            not isinstance(included_tables, list)
            or not included_tables
            or any(not isinstance(name, str) or not name for name in included_tables)
        ):
            raise RestoreError("migration_included_tables_invalid")
        included_tables = sorted(set(included_tables))
    account, account_binding = None, None
    if setup_account is not None:
        from zhenxun.utils.passwords import hash_password, validate_new_password

        username = str(setup_account.get("username") or "").strip()
        password_value = str(setup_account.get("password") or "")
        if (
            not username
            or len(username) > 128
            or any(character in username for character in "\r\n\x00")
            or password_value != setup_account.get("confirm_password")
        ):
            raise RestoreError("migration_setup_account_invalid")
        if error := validate_new_password(password_value):
            raise RestoreError(error)
        account_binding = {
            "username": username,
            "password_digest": sha256(password_value.encode()).hexdigest(),
        }
        account = {
            "username": username,
            "password_hash": await asyncio.to_thread(hash_password, password_value),
        }
    options = {
        "restore_mode": restore_mode,
        "backup_before_replace": backup_before_replace,
        "included_tables": included_tables,
        "source_binding": source_binding,
    }
    stat = path.stat()
    request, reused = tasks.reserve(
        str(operation_id or ""),
        {
            "kind": "restore",
            "path": str(path.resolve()),
            "size": stat.st_size,
            "mtime": stat.st_mtime_ns,
            "password": sha256((password or "").encode()).hexdigest(),
            "categories": sorted(set(categories)),
            "skip_failed": skip_failed,
            "db_url": db_url or current,
            **options,
            "setup_account": account_binding,
        },
        {
            "kind": "restore",
            "name": path.name,
            "archive_path": str(path.resolve()),
            "archive_stat": [stat.st_size, stat.st_mtime_ns],
            "archive_password_digest": (
                sha256(password.encode()).hexdigest() if password else None
            ),
            "original_modified": False,
            "categories": sorted(set(categories)),
            "skip_failed": skip_failed,
            "db_url": db_url or current,
            "previous_db_url": current,
            "snapshot_required": not bool(db_url),
            "original_snapshot_required": (
                bool(current)
                and not bool(db_url)
                and not (
                    restore_mode == "replace_database" and not backup_before_replace
                )
            ),
            **options,
            "setup_account": account,
            "previous_worker": _worker_identity(),
        },
    )
    if reused:
        return status(request["operation_id"])

    return await _accepted(request, lambda: preparation.run(request, password, restart))


TABLES_SQL = {
    "sqlite": "SELECT name FROM sqlite_master"
    " WHERE type = 'table' AND name NOT LIKE 'sqlite_%'",
    "mysql": "SELECT table_name FROM information_schema.tables"
    " WHERE table_schema = DATABASE() AND table_type = 'BASE TABLE'",
    "postgres": "SELECT tablename FROM pg_tables WHERE schemaname = current_schema()",
}


async def _require_empty(db_url: str, request: dict | None = None) -> dict:
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
        from . import data_guard

        async with data_guard.import_transaction(client) as db:
            tables = await data_guard.table_names(db)
            for schema, table in sorted(tables, key=str):
                if await data_guard.has_business_rows(db, schema, table):
                    raise RestoreError(
                        f"目标数据库已有数据（表 {table}），请使用空数据库"
                    )
            identity = await data_guard.identity(db)
            if request is not None:
                snapshot = _native_snapshot(
                    identity, _paths(request)[1] / "target.sqlite3"
                )
                _save(request, target_sqlite_snapshot=snapshot)
            return identity
    finally:
        await client.close()


async def request_switch(
    db_url: str,
    restart: Restart | None = None,
    *,
    operation_id: str | None = None,
) -> dict:
    """停写后复制最终数据到空库，并通过相同的验证及提交流程切换"""
    current = _current_db_url()
    if db_url == current:
        raise RestoreError("新数据库与当前数据库相同")
    request, reused = tasks.reserve(
        str(operation_id or ""),
        {
            "kind": "switch",
            "db_url": db_url,
        },
        {
            "kind": "switch",
            "categories": [],
            "original_modified": False,
            "db_url": db_url,
            "previous_db_url": current,
            "snapshot_required": True,
            "previous_worker": _worker_identity(),
        },
    )
    if reused:
        return status(request["operation_id"])

    from . import preparation

    return await _accepted(request, lambda: preparation.run(request, None, restart))


def snapshot_pending() -> bool:
    """当前任务是否等待 worker 关闭时生成最终快照

    返回:
        bool: 是否等待最终快照，记录无法读取时为 False
    """
    try:
        request = _load()
    except Exception:
        return False
    return bool(
        request and request["state"] == "pending" and policy.original_snapshot(request)
    )


async def final_snapshot() -> None:
    """所有写入组件关闭后、数据库连接关闭前生成最终回滚点"""
    request = _load()
    if (
        not request
        or request["state"] != "pending"
        or not policy.original_snapshot(request)
    ):
        return
    from tortoise import Tortoise

    from zhenxun.services.lifecycle import lifecycle_kernel

    from . import data, data_guard

    with tasks.owned(request):
        try:
            lifecycle = lifecycle_kernel.status()
            if not lifecycle.get("shutdown_requested"):
                raise RestoreError("migration_shutdown_not_started")
            components = lifecycle["components"]
            writers = {"management:database"}
            while True:
                expanded = writers | {
                    item["component_id"]
                    for item in components
                    if set(item.get("depends_on", [])) & writers
                }
                if expanded == writers:
                    break
                writers = expanded
            unsafe = [
                item["component_id"]
                for item in components
                if item["component_id"] in writers - {"management:database"}
                and item.get("state") not in {"stopped", "declared"}
            ]
            if unsafe:
                raise RestoreError("migration_shutdown_unconfirmed:" + ",".join(unsafe))
            client = Tortoise.get_connection("default")
            package, rollback = _paths(request)
            snapshot = rollback / "database"
            async with data_guard.import_transaction(client) as db:
                await data_guard.lock_tables(
                    db, data._models(), await data_guard.foreign_keys(db)
                )
                manifest = await data.export_data(snapshot, client, transaction=db)
                identity = await data_guard.identity(db)
                sqlite_snapshot = _native_snapshot(identity, rollback / "main.sqlite3")
                revision = await data.database_revision(db, manifest["tables"])
            if request["kind"] == "switch":
                await asyncio.to_thread(
                    shutil.copytree, snapshot, package / "database", dirs_exist_ok=True
                )
            _save(
                request,
                state="snapshot_ready",
                snapshot_identity=identity,
                snapshot_revision=revision,
                snapshot_worker=_worker_identity(),
                sqlite_snapshot=sqlite_snapshot,
            )
        except BaseException as error:
            _save(
                request,
                state="preparing",
                preflight_ready=False,
                preparation={
                    **request.get("preparation", {}),
                    "phase": "blocked",
                    "retryable": True,
                    "error": "最终快照未确认，未应用迁移：" + _message(error),
                },
                error="最终快照未确认：" + _message(error),
            )
            raise


def _native_snapshot(identity: dict, destination: Path) -> dict | None:
    """在父连接持有写入保护期间备份 SQLite，快照不与逻辑导出错位"""
    if identity["engine"] != "sqlite" or identity["file"] == ":memory:":
        return None
    from contextlib import closing
    import sqlite3

    destination.parent.mkdir(parents=True, exist_ok=True)
    with (
        closing(sqlite3.connect(identity["file"], timeout=3)) as source,
        closing(sqlite3.connect(destination)) as target,
    ):
        source.backup(target)
        fingerprint = tasks.sqlite_fingerprint(target)
    return {
        "file": identity["file"],
        "backup": str(destination),
        "fingerprint": fingerprint,
    }


def _target_snapshot(request: dict) -> dict | None:
    return request.get("target_sqlite_snapshot") or (
        request.get("sqlite_snapshot") if request.get("kind") == "restore" else None
    )


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


def _write_files(request: dict, root: Path, files: tasks.Files) -> None:
    from dotenv import dotenv_values, set_key

    from zhenxun.services.plugin_store.plugin_store_receipts import RECEIPT_FILE
    from zhenxun.utils.atomic_json import read_json_locked, write_json_locked

    from . import backup

    package, rollback = _paths(request)
    staged = rollback / "staged"
    staged.mkdir(parents=True, exist_ok=True)
    local = {
        path.name: dotenv_values(path) for path in root.glob(".env*") if path.is_file()
    }
    source = package / "files"
    if source.is_dir():
        for path in sorted(source.rglob("*")):
            relative = path.relative_to(source).as_posix()
            if (
                not path.is_file()
                or backup.category_for(relative) not in request["categories"]
            ):
                continue
            candidate = path
            if relative in local:
                candidate = staged / relative
                shutil.copy2(path, candidate)
                _keep_local_keys(candidate, local[relative])
            files.write(relative, candidate)
    if "plugins" in request["categories"]:
        saved = json.loads((package / backup.DEPENDENCIES).read_text("utf-8"))
        receipts = read_json_locked(root / RECEIPT_FILE, {}, strict=True)
        candidate = staged / "receipts.json"
        write_json_locked(candidate, {**receipts, **saved["store_receipts"]})
        files.write(RECEIPT_FILE.as_posix(), candidate)
    environment = _environment_file(root)
    candidate = staged / "environment"
    target = root / environment
    if target.exists():
        shutil.copy2(target, candidate)
    else:
        candidate.touch()
    set_key(candidate, "DB_URL", request["db_url"], quote_mode="always")
    files.write(environment, candidate)
    if account := request.get("setup_account"):
        from zhenxun.utils.webui_session import credential_documents

        simple, plugins = credential_documents(
            account["username"], account["password_hash"], root=root
        )
        for relative, content in (
            ("data/configs/plugins2config.yaml", plugins),
            ("data/config.yaml", simple),
        ):
            credential = staged / Path(relative).name
            credential.write_bytes(content)
            files.write(relative, credential)


def _old_worker_exited(request: dict) -> None:
    import psutil

    old = request.get("previous_worker") or {}
    try:
        process = psutil.Process(int(old["pid"]))
        if process.create_time() == old["created_at"]:
            raise RestoreError("migration_previous_worker_still_active")
    except psutil.NoSuchProcess:
        return
    except (psutil.Error, KeyError, ValueError) as error:
        raise RestoreError("migration_previous_worker_unconfirmed") from error


def _apply(request: dict, root: Path) -> None:
    _old_worker_exited(request)
    if tasks.cancellation_requested(request):
        from . import preparation

        asyncio.run(preparation.cancel(request))
        return
    if policy.original_snapshot(request) and request["state"] != "snapshot_ready":
        _save(
            request,
            state="preparing",
            preflight_ready=False,
            preparation={
                "phase": "blocked",
                "retryable": True,
                "error": "migration_final_snapshot_missing",
            },
        )
        return
    from . import preflight, preparation

    package, rollback = _paths(request)
    files = tasks.Files(root, rollback)
    try:
        if not request.get("preflight_ready"):
            raise RestoreError("migration_online_preparation_required")
        if preflight.environment() != request.get("preparation_environment"):
            raise RestoreError("migration_preflight_environment_changed")
        if preflight.files_check(request, package) != request.get(
            "prepared_target_files"
        ):
            raise RestoreError("migration_preflight_files_changed")
        if request["kind"] == "restore" and preflight.tree_digest(
            package
        ) != request.get("package_digest"):
            raise RestoreError("migration_prepared_package_changed")
        checked = asyncio.run(
            preflight.database_check(
                request, package if request["kind"] == "restore" else None
            )
        )
        previous = request.get("preflight_database") or {}
        if checked["identity"] != previous.get("identity") or checked[
            "structure"
        ] != previous.get("structure"):
            raise RestoreError("migration_preflight_database_changed")
        if policy.requires_empty(request):
            identity = asyncio.run(_require_empty(request["db_url"], request))
            _save(request, target_identity=identity)
        files.initialize()
        preparation.authorize(request)
    except Exception as error:
        _save(
            request,
            state="preparing",
            preflight_ready=False,
            preparation={
                **request.get("preparation", {}),
                "phase": "blocked",
                "retryable": True,
                "error": _message(error),
            },
        )
        return
    _save(request, state="applying", original_modified=True)
    try:
        _write_files(request, root, files)
        _save(request, state="importing")
    except Exception as error:
        _save(request, error=_message(error), error_code=getattr(error, "code", None))
        try:
            if not files.changed():
                preparation.hold(request)
                _save(
                    request,
                    state="preparing",
                    original_modified=False,
                    preflight_ready=False,
                    preparation={
                        **request.get("preparation", {}),
                        "phase": "blocked",
                        "retryable": True,
                        "error": _message(error),
                    },
                )
                return
        except (OSError, RuntimeError, KeyError, TypeError):
            pass
        rollback_after_stop(request)


def _orm_scope():
    """为 launcher 中独立连接的事务提供 ORM 上下文

    返回:
        已有上下文或旧版 ORM 时为空上下文，否则为临时的 TortoiseContext
    """
    from contextlib import nullcontext

    try:
        from tortoise.context import TortoiseContext, get_current_context
    except ImportError:
        return nullcontext()
    return nullcontext() if get_current_context() else TortoiseContext()


def apply_pending(root: Path | None = None) -> bool:
    request = _load()
    if request is None or request["state"] not in {
        "pending",
        "snapshot_ready",
        "applying",
        "failed_import",
        "committing",
        "rollback_target",
    }:
        return False
    with tasks.owned(request), _orm_scope():
        if request["state"] == "rollback_target":
            asyncio.run(_rollback_target(request))
        elif request["state"] == "committing":
            settle_commit(request)
        elif request["state"] in {"failed_import", "applying"}:
            rollback_after_stop(request)
        else:
            _apply(request, (root or tasks.mutation.INSTANCE_ROOT).resolve())
    return True


def dependency_ready(request: dict) -> int | None:
    """数据导入只要求本任务依赖修订已应用，不等待依赖最终启动验证"""
    from zhenxun.services.nonebot_store import storage

    report = request.get("dependencies") or {}
    if not report.get("staged"):
        return storage.load_manifest().get("active_generation")
    child = storage.pending_transaction()
    manifest = storage.load_manifest()
    if (
        not child
        or child.get("revision") != report.get("revision")
        or child.get("parent_operation_id") != request["operation_id"]
        or child.get("state") != "verification_pending"
        or manifest.get("active_generation") != child.get("generation")
    ):
        raise RestoreError("migration_dependency_not_applied")
    return child["generation"]


def defer_for_manual_orm(request: dict) -> bool:
    """仅将尚未写库、代际未切换的本任务 ORM 阻断保留为人工处理等待"""
    import os
    import shlex
    import subprocess
    import sys

    from zhenxun.services.nonebot_store import storage

    child = storage.pending_transaction() or {}
    revision = (request.get("dependencies") or {}).get("revision")
    if child.get("state") != "migration_blocked":
        return False
    if (
        not revision
        or child.get("revision") != revision
        or child.get("parent_operation_id") != request["operation_id"]
        or child.get("orm_write_started")
        or request.get("import_started")
        or storage.load_manifest().get("active_generation")
        != (child.get("base_manifest") or {}).get("active_generation")
    ):
        raise RestoreError("migration_dependency_wait_unconfirmed")
    command = [
        sys.executable,
        "-m",
        "zhenxun.services.nonebot_store.orm_migration",
        "manual-upgrade",
        "--revision",
        revision,
        "--backup-confirmed",
    ]
    display = (
        subprocess.list2cmdline(command) if os.name == "nt" else shlex.join(command)
    )
    message = (
        "插件 ORM 需要人工升级，数据尚未导入。停止当前实例并备份插件数据库后，"
        f"在项目目录运行：{display}；完成后重新运行 zx run，原任务将自动续查。"
    )
    if request["state"] != "awaiting_orm" or request.get("error") != message:
        _save(
            request,
            state="awaiting_orm",
            error_code="external_database_manual_migration_required",
            error=message,
            error_detail={"command": command, "requires_stop": True},
        )
    return True


def bind_worker(startup_id: str) -> None:
    request = _load()
    if request and request["state"] == "preparing":
        _old_worker_exited(request)
        canceled = tasks.cancellation_requested(request)
        preparation = request.get("preparation", {})
        if canceled or preparation.get("phase") != "blocked":
            preparation = {
                **preparation,
                "phase": "cleanup_pending" if canceled else "interrupted",
                "retryable": not canceled,
                "error": "原实例未变更，请重试本任务候选清理"
                if canceled
                else "准备未影响原实例，确认后可续接原任务",
            }
        with tasks.owned(request):
            _save(
                request,
                preparation_worker=None,
                preparation_startup_id=startup_id,
                preparation=preparation,
            )
        return
    if (
        request
        and request["state"] == "recovery_required"
        and _original_isolated(request)
        and request.get("rollback_components", {}).get("original_instance")
        == "confirmed"
    ):
        with tasks.owned(request):
            _save(request, state="rollback_verifying", original_recheck=True)
    if request and request["state"] in {
        "importing",
        "rollback_import",
        "verifying",
        "rollback_verifying",
    }:
        with tasks.owned(request):
            _save(request, expected_worker_startup_id=startup_id)


_gate_cache: tuple | None = None
_gate_blocked = False
_gate_checked = 0.0


def _original_isolated(request: dict) -> bool:
    """确认切库前实例与待核验目标隔离，且原实例文件及依赖已回退。"""
    components = request.get("rollback_components") or {}
    return bool(
        request.get("kind") == "switch"
        and policy.original_snapshot(request)
        and request.get("snapshot_identity")
        and request.get("target_identity")
        and request["snapshot_identity"] != request["target_identity"]
        and components.get("target_database") == "unconfirmed"
        and all(
            components.get(name) in {"confirmed", "not_applicable"}
            for name in ("dependencies", "orm", "files")
        )
    )


def business_blocked(max_age: float = 0.0) -> bool:
    """迁移任务是否阻止业务处理

    参数:
        max_age: 可复用上次检查结果的秒数，0 表示每次都检查任务文件

    返回:
        bool: 是否阻止业务处理，任务记录无法读取时为 True
    """
    global _gate_cache, _gate_blocked, _gate_checked

    now = time.monotonic()
    if max_age and _gate_cache is not None and now - _gate_checked < max_age:
        return _gate_blocked
    try:
        signature = []
        for path in (tasks.REQUEST, tasks.mutation.OWNER_FILE):
            try:
                info = path.stat()
                signature.append(
                    (str(path), info.st_ino, info.st_mtime_ns, info.st_size)
                )
            except FileNotFoundError:
                signature.append((str(path), None))
        key = tuple(signature)
        if _gate_cache != key:
            request = _load()
            _gate_blocked = bool(
                request and request["state"] in RUNNING - {"preparing"}
            )
            if (
                request
                and request["state"] == "recovery_required"
                and _original_isolated(request)
                and request["rollback_components"].get("original_instance")
                == "confirmed"
                and (request.get("verification") or {}).get("startup_id")
                == os.environ.get("ZHENXUN_WORKER_STARTUP_ID")
                and os.environ.get("ZHENXUN_WORKER_STARTUP_ID")
            ):
                _gate_blocked = False
            _gate_cache = key
        _gate_checked = now
        return _gate_blocked
    except (RuntimeError, OSError):
        return True


def verification_pending() -> bool:
    request = _load()
    return bool(request and request["state"] in {"verifying", "rollback_verifying"})


def rollback_after_stop(request: dict | None = None) -> None:
    """worker 已确认退出后仅回退本任务的子事务和文件"""
    from zhenxun.services.nonebot_store import runtime, storage

    request = request or _load()
    if not request:
        return
    with tasks.owned(request):
        if request.get("commit_decision"):
            raise RestoreError("migration_commit_already_decided")
        components = {
            "dependencies": "pending",
            "orm": "pending",
            "files": "pending",
            "target_database": "pending",
            "original_instance": "pending",
        }
        _save(request, rollback_components=components)
        try:
            child = storage.pending_transaction()
            database_started = bool(
                request.get("schema_started")
                or request.get("import_started")
                or request.get("report")
                or request.get("target_rollback")
                or (child or {}).get("orm_write_started")
            )
            revision = (request.get("dependencies") or {}).get("revision")
            if revision and (
                not child or child.get("parent_operation_id") != request["operation_id"]
            ):
                confirmed = any(
                    item.get("revision") == revision
                    and item.get("parent_operation_id") == request["operation_id"]
                    and item.get("rollback_confirmed")
                    for item in storage.settled_transactions()
                )
                if not confirmed:
                    raise RestoreError("migration_dependency_rollback_unconfirmed")
            if child and child.get("parent_operation_id") == request["operation_id"]:
                if revision and revision != child["revision"]:
                    raise RestoreError("migration_dependency_revision_changed")
                if not child.get("rollback_confirmed"):
                    if child.get("state") in {"pending_restart", "migration_blocked"}:
                        if child.get(
                            "orm_write_started"
                        ) or storage.load_manifest().get("active_generation") != (
                            child.get("base_manifest") or {}
                        ).get("active_generation"):
                            raise RestoreError(
                                "migration_dependency_rollback_unconfirmed"
                            )
                        child.update(state="failed", rollback_confirmed=True)
                        storage.save_pending_transaction(child)
                    elif not runtime.rollback_for_launcher():
                        raise RestoreError("migration_dependency_rollback_unconfirmed")
                storage.archive_settled()
            components.update(
                dependencies="confirmed" if revision else "not_applicable",
                orm="confirmed" if revision else "not_applicable",
            )
            _save(request, rollback_components=components)
            _, rollback = _paths(request)
            tasks.Files(tasks.mutation.INSTANCE_ROOT, rollback).rollback()
            components["files"] = "confirmed"
            _save(request, rollback_components=components)
            _require_rollback_evidence(request)
            if (
                request.get("import_started")
                and not request.get("report")
                and not request.get("import_rollback_confirmed")
            ):
                if (request.get("target_rollback") or {}).get("import_report"):
                    _save(request, state="rollback_target")
                    return
                raise RestoreError("migration_import_outcome_unconfirmed")
            if not database_started:
                components["target_database"] = "not_applicable"
                _save(request, rollback_components=components)
            if (
                database_started
                and request.get("target_native_backup")
                and request["target_native_backup"]["engine"] != "sqlite"
            ):
                asyncio.run(_restore_native_target(request))
                components["target_database"] = "confirmed"
                _save(request, rollback_components=components)
                if policy.original_snapshot(request):
                    _save(
                        request,
                        state="rollback_verifying",
                        import_started=None,
                        rollback_report={
                            "database_identity": request["snapshot_identity"],
                            "content_revision": request["snapshot_revision"],
                        },
                    )
                else:
                    components["original_instance"] = "not_applicable"
                    cleanup(request)
                    tasks.finish(
                        request,
                        state="rolled_back",
                        rollback_confirmed=True,
                        rollback_components=components,
                    )
                return
            if (
                database_started
                and policy.mode(request) == "replace_database"
                and not request.get("target_native_backup")
            ):
                raise RestoreError(
                    "migration_original_data_unrecoverable_without_backup"
                )
            if database_started and (snapshot := _target_snapshot(request)):
                report = request.get("report") or {}
                expected = (
                    report.get("sqlite_fingerprint")
                    or request.get("preimport_sqlite_fingerprint")
                    or request.get("orm_sqlite_fingerprint")
                    or request.get("target_last_fingerprint")
                    or snapshot["fingerprint"]
                )
                restored_fingerprint = tasks.restore_sqlite(snapshot, expected)
                components["target_database"] = "confirmed"
                _save(request, rollback_components=components)
                if not policy.original_snapshot(request):
                    components["original_instance"] = "not_applicable"
                    cleanup(request)
                    tasks.finish(
                        request,
                        state="rolled_back",
                        rollback_confirmed=True,
                        rollback_components=components,
                    )
                    return
                _save(
                    request,
                    state="rollback_verifying",
                    import_started=None,
                    rollback_report={
                        "database_identity": request["snapshot_identity"],
                        "content_revision": request["snapshot_revision"],
                        "sqlite_fingerprint": restored_fingerprint
                        if request["kind"] == "restore"
                        else (request.get("sqlite_snapshot") or {}).get("fingerprint"),
                    },
                )
                return
            if request.get("target_rollback"):
                _save(request, state="rollback_target")
                return
            if request.get("schema_started"):
                raise RestoreError("migration_schema_rollback_requires_review")
            if request.get("report"):
                if policy.requires_empty(request):
                    raise RestoreError("migration_target_rollback_requires_review")
                _save(request, state="rollback_import", import_started=None)
            elif policy.original_snapshot(request):
                _save(
                    request,
                    state="rollback_verifying",
                    import_started=None,
                    rollback_report={
                        "database_identity": request["snapshot_identity"],
                        "content_revision": request["snapshot_revision"],
                    },
                )
            else:
                tasks.finish(request, state="rolled_back", rollback_confirmed=True)
        except Exception as error:
            _save(
                request,
                state="recovery_required",
                rollback_error=_message(error),
                recovery_reason=_message(error),
            )


def _require_rollback_evidence(request: dict) -> None:
    """整库清理后没有原生回滚点时，拒绝把空目标补偿解释为原数据恢复。"""
    if (
        policy.mode(request) == "replace_database"
        and (
            request.get("target_cleared")
            or request.get("target_mutation_state") in {"clearing", "cleared"}
        )
        and not request.get("target_native_backup")
    ):
        raise RestoreError("migration_original_data_unrecoverable_without_backup")


async def _restore_native_target(request: dict) -> None:
    """在 launcher 维护进程中用原生回滚点恢复目标，并核对结构与内容

    参数:
        request: 恢复任务记录

    异常:
        RestoreError: 恢复后的结构或内容与备份时不一致
    """
    from . import data, data_guard, native_backup, target

    _require_rollback_evidence(request)
    client = await data.open_client(request["db_url"], "zx_target_native_restore")
    try:
        await native_backup.restore(
            request["db_url"],
            request["target_native_backup"],
            root=tasks.mutation.INSTANCE_ROOT,
            client=client,
        )
        if await target.database_objects(client) != request.get(
            "target_backup_objects"
        ) or await target.database_content(client) != request.get(
            "target_backup_content"
        ):
            raise RestoreError("migration_native_restore_unverified")
        identity = await data_guard.identity(client)
    finally:
        await client.close()
    changes = {"target_cleared": False, "target_mutation_state": "restored"}
    for name in ("target_identity", "snapshot_identity"):
        previous = request.get(name)
        if previous and {**previous, "schema_id": None} == {
            **identity,
            "schema_id": None,
        }:
            changes[name] = identity
    _save(request, **changes)


async def _rollback_target(request: dict) -> None:
    """在 launcher 维护进程中补偿目标，再验证原实例或恢复原库数据。"""
    from . import data, target

    client = None
    try:
        _require_rollback_evidence(request)
        native = (request.get("target_native_backup") or {}).get("engine")
        if native in {"mysql", "postgres"}:
            await _restore_native_target(request)
        else:
            client = await data.open_client(request["db_url"], "zx_target_rollback")
            if (
                request.get("import_started")
                and not request.get("report")
                and not request.get("import_rollback_confirmed")
            ):
                _save(request, report=await target.reconcile(client, request))
            if request.get("target_table_snapshot"):
                journal = request.get("target_rollback") or {}
                expected = journal.get("applied") or {
                    **journal.get("before", {}),
                    **{
                        key: item["initial"]
                        for key, item in journal.get("created", {}).items()
                        if item.get("initial")
                    },
                }
                await target.restore_tables(
                    client, request["target_table_snapshot"], expected=expected
                )
                _save(request, import_rollback_confirmed=True)
                await target.rollback(client, request, _save)
            elif native:
                await client.close()
                client = None
                await _restore_native_target(request)
            else:
                await target.rollback(client, request, _save)
        components = {
            **request.get("rollback_components", {}),
            "target_database": "confirmed",
        }
        _save(request, rollback_components=components, schema_started=False)
        if not policy.original_snapshot(request):
            components["original_instance"] = "not_applicable"
            cleanup(request)
            tasks.finish(
                request,
                state="rolled_back",
                rollback_confirmed=True,
                rollback_components=components,
            )
        elif (
            request["kind"] == "restore"
            and request.get("report")
            and not request.get("target_native_backup")
            and not request.get("target_table_snapshot")
        ):
            components["target_database"] = "pending"
            _save(request, rollback_components=components)
            _save(request, state="rollback_import", import_started=None)
        else:
            _save(
                request,
                state="rollback_verifying",
                import_started=None,
                rollback_report={
                    "database_identity": request["snapshot_identity"],
                    "content_revision": request["snapshot_revision"],
                },
            )
    except Exception as error:
        _save(
            request,
            state="recovery_required",
            rollback_error=_message(error),
            recovery_reason=_message(error),
            rollback_components={
                **request.get("rollback_components", {}),
                "target_database": "unconfirmed",
            },
        )
        if _original_isolated(request):
            _save(
                request,
                state="rollback_verifying",
                import_started=None,
                original_recheck=False,
                rollback_report={
                    "database_identity": request["snapshot_identity"],
                    "content_revision": request["snapshot_revision"],
                    "sqlite_fingerprint": (request.get("sqlite_snapshot") or {}).get(
                        "fingerprint"
                    ),
                },
            )
    finally:
        if client is not None:
            await client.close()


async def _settle_target(request: dict, save=_save) -> None:
    from . import data, target

    client = await data.open_client(request["db_url"], "zx_target_settle")
    try:
        await target.settle(client, request, save)
    finally:
        await client.close()


def settle_commit(request: dict) -> None:
    """提交决定落盘后可重放结算，不重新导入也不自动反向回滚"""
    from zhenxun.services.nonebot_store import runtime, storage

    if not request.get("commit_decision"):
        raise RestoreError("migration_commit_decision_missing")
    if request.get("target_rollback"):
        try:
            asyncio.run(_settle_target(request))
            _save(request, target_cleanup_error=None)
        except Exception as error:
            _save(
                request,
                target_cleanup_error=_message(error),
                cleanup_error=_message(error),
            )
    child = storage.pending_transaction()
    revision = (request.get("dependencies") or {}).get("revision")
    if revision:
        if child and child.get("revision") == revision:
            if child.get("parent_operation_id") != request["operation_id"]:
                raise RestoreError("migration_dependency_owner_changed")
            runtime.finalize_pending_transaction()
        elif not any(
            item["revision"] == revision
            and item.get("parent_operation_id") == request["operation_id"]
            and item.get("verification_confirmed")
            for item in storage.settled_transactions()
        ):
            raise RestoreError("migration_dependency_commit_unconfirmed")
    partial = bool((request.get("report") or {}).get("skipped_tables"))
    cleanup(request)
    tasks.finish(
        request,
        state="done_partial" if partial else "done",
        verification_confirmed=True,
    )


def commit_verified() -> bool:
    request = _load()
    if not request or request["state"] not in {"verifying", "rollback_verifying"}:
        raise RestoreError("migration_verification_not_pending")
    with tasks.owned(request):
        if (
            request["verification"]["startup_id"]
            != request["expected_worker_startup_id"]
        ):
            raise RestoreError("migration_worker_identity_changed")
        if request["state"] == "rollback_verifying":
            if _original_isolated(request):
                from zhenxun.services.nonebot_store import storage

                _save(
                    request,
                    state="recovery_required",
                    rollback_components={
                        **request["rollback_components"],
                        "original_instance": "confirmed",
                    },
                    original_generation=storage.load_manifest().get(
                        "active_generation"
                    ),
                )
                return True
            _save(
                request,
                rollback_components={
                    **request.get("rollback_components", {}),
                    "original_instance": "confirmed",
                    "target_database": "confirmed",
                },
            )
            cleanup(request)
            tasks.finish(request, state="rolled_back", rollback_confirmed=True)
            return True
        dependency_ready(request)
        with tasks.mutation.mutation_guard(request["operation_id"]):
            if tasks.cancellation_requested(request):
                return False
            _save(
                request,
                state="committing",
                commit_decision={
                    "revision": request["revision"],
                    "verified_at": tasks.now(),
                    "worker": request["verification"],
                },
            )
        settle_commit(request)
        return True


def cleanup(request: dict) -> None:
    """清理已确认终态的本任务材料，清理失败保留独立诊断"""
    if request.get("target_cleanup_error"):
        return
    try:
        for path in _paths(request):
            if path.exists():
                if path.name == "rollback" and request.get("target_native_backup"):
                    for child in path.iterdir():
                        if child.name == "native":
                            continue
                        if child.is_dir():
                            shutil.rmtree(child)
                        else:
                            child.unlink()
                    continue
                shutil.rmtree(path)
    except OSError as error:
        _save(request, cleanup_error=_message(error))
    else:
        if request.get("cleanup_error"):
            _save(request, cleanup_error=None)


def retry_cleanup() -> None:
    """仅重试已确认终态任务的私有材料清理，不改变任务结论"""
    from zhenxun.utils.atomic_json import write_json_locked

    if tasks.mutation.mutation_owner():
        return
    for record_path in (tasks.ROOT / "tasks").glob("*/request.json"):
        request = tasks.load(record_path.parent.name)
        if (
            not request
            or request["state"] not in tasks.TERMINAL
            or not request.get("cleanup_error")
        ):
            continue
        error = None
        with tasks.mutation.mutation_guard() as owner:
            if owner:
                return
            write_json_locked(
                tasks.mutation.OWNER_FILE,
                {
                    "operation_id": request["operation_id"],
                    "kind": "restore",
                    "phase": "terminal_cleanup",
                    "executor": request.get("executor"),
                },
            )
        with tasks.mutation.mutation_scope(request["operation_id"]):
            try:
                if request.get("target_cleanup_error"):
                    asyncio.run(_settle_target(request, tasks.save_terminal))
                    tasks.save_terminal(request, target_cleanup_error=None)
                for path in _paths(request):
                    if path.exists():
                        shutil.rmtree(path)
            except (OSError, RuntimeError) as failure:
                error = _message(failure)
            finally:
                try:
                    tasks.save_terminal(request, cleanup_error=error)
                finally:
                    tasks.mutation.release_mutation(request["operation_id"])


async def import_pending() -> None:
    """按父任务指定的依赖、数据库及 worker 身份导入，导入后等待 launcher 验证"""
    request = _load()
    if request is None or request["state"] not in {
        "importing",
        "rollback_import",
        "verifying",
        "rollback_verifying",
    }:
        return
    from tortoise import Tortoise

    from zhenxun.services.log import logger
    from zhenxun.utils._restart_utils import request_restart

    from . import data, target

    rollback = request["state"] in {"rollback_import", "rollback_verifying"}
    outcome = {"state": "not_started"}
    with tasks.owned(request):
        try:
            startup_id = os.environ.get("ZHENXUN_WORKER_STARTUP_ID")
            if not startup_id or startup_id != request.get(
                "expected_worker_startup_id"
            ):
                raise RestoreError("migration_worker_identity_changed")
            generation = None if rollback else dependency_ready(request)
            if request["state"] in {"verifying", "rollback_verifying"}:
                from . import data_guard

                previous = (
                    request.get("rollback_report") if rollback else request["report"]
                )
                client = Tortoise.get_connection("default")
                async with data_guard.import_transaction(client) as db:
                    await data_guard.require_same(db, previous["database_identity"])
                    recheck = bool(rollback and request.get("original_recheck"))
                    if recheck:
                        from zhenxun.services.nonebot_store import storage

                        if storage.load_manifest().get(
                            "active_generation"
                        ) != request.get("original_generation"):
                            raise RestoreError("migration_original_generation_changed")
                    await data_guard.lock_tables(
                        db,
                        [
                            model
                            for model in data._models()
                            if data._table_key(model) in previous["content_revision"]
                        ],
                        await data_guard.foreign_keys(db),
                    )
                    if not recheck and (
                        await data.database_revision(db, previous["content_revision"])
                        != previous["content_revision"]
                    ):
                        raise RestoreError("migration_verification_content_changed")
                    if (
                        not recheck
                        and previous.get("sqlite_fingerprint")
                        and previous["sqlite_fingerprint"]
                        not in {
                            await data.sqlite_revision(db),
                            await data.sqlite_revision(db, legacy=True),
                        }
                    ):
                        raise RestoreError("migration_verification_content_changed")
                _save(
                    request,
                    verification={
                        "startup_id": startup_id,
                        "generation": generation,
                        "database_identity": previous["database_identity"],
                    },
                )
                return
            if (
                request.get("import_started")
                and request["import_started"] != startup_id
            ):
                raise RestoreError("migration_import_outcome_unconfirmed")
            _save(request, import_started=startup_id)
            package, saved = _paths(request)
            source = (saved if rollback else package) / "database"
            revision = None
            if rollback:
                revision = request["report"]["content_revision"]
            elif (
                policy.original_snapshot(request)
                and request["kind"] == "restore"
                and policy.mode(request) != "replace_database"
            ):
                revision = request["snapshot_revision"]
            included = set(request["report"]["tables"]) if rollback else None
            if revision is not None:
                table_manifest = json.loads(
                    (source / data.MANIFEST).read_text("utf-8")
                )["tables"]
                selected = (
                    included & set(table_manifest)
                    if included is not None
                    else set(table_manifest) - set(request.get("excluded_tables") or [])
                )
                revision = {
                    name: value for name, value in revision.items() if name in selected
                }
            report = await data.import_data(
                source,
                Tortoise.get_connection("default"),
                replace=bool(
                    policy.original_snapshot(request) and request["kind"] == "restore"
                ),
                restore_mode=(
                    "replace_tables"
                    if rollback
                    else policy.mode(request)
                    if request.get("restore_mode")
                    else None
                ),
                source_binding=None if rollback else request.get("source_binding"),
                archive_sha256=None if rollback else request.get("archive_digest"),
                expected_identity=request.get("snapshot_identity")
                if rollback
                else request["target_identity"],
                expected_revision=revision,
                strict_tables=True,
                excluded_tables=set(request.get("excluded_tables") or [])
                if not rollback
                else set(),
                included_tables=(
                    included if rollback else request.get("included_tables")
                ),
                expected_sqlite_fingerprint=(
                    request.get("preimport_sqlite_fingerprint")
                    or request.get("orm_sqlite_fingerprint")
                    or (_target_snapshot(request) or {}).get("fingerprint")
                )
                if not rollback
                else None,
                outcome=outcome,
                before_commit=(
                    lambda db, result: target.record_applied(db, request, result, _save)
                )
                if not rollback and request.get("target_rollback")
                else None,
            )
            _save(
                request,
                state="rollback_verifying" if rollback else "verifying",
                **({"rollback_report": report} if rollback else {"report": report}),
                verification={
                    "startup_id": startup_id,
                    "generation": generation,
                    "database_identity": report["database_identity"],
                },
            )
            logger.info("数据导入完成，等待启动验证", "恢复")
        except Exception as error:
            logger.error("恢复数据导入失败", "恢复", e=error)
            _save(
                request,
                state="recovery_required"
                if rollback
                or str(error) == "migration_import_outcome_unconfirmed"
                or outcome["state"] not in {"not_started", "rolled_back"}
                else "failed_import",
                import_rollback_confirmed=(
                    outcome["state"] in {"not_started", "rolled_back"}
                ),
                **(
                    {"rollback_error": _message(error)}
                    if rollback
                    else {"error": _message(error)}
                ),
            )
            if not rollback and request["state"] == "failed_import":
                await request_restart("backup-restore-rollback")
            raise RestoreError("migration_import_startup_blocked") from error


async def _prepare_target(client, request: dict) -> None:
    """核验全部待导入模型后生成目标回滚点，再进入整库清理。"""
    from . import data, data_guard, native_backup, preflight, target

    if not request.get("restore_mode") or policy.requires_empty(request):
        return
    if request.get("target_mutation_state") == "clearing":
        raise RestoreError("migration_target_clear_outcome_unconfirmed")
    if request.get("target_cleared") or request.get("target_prepared"):
        return
    package, rollback = _paths(request)
    validated = preflight.validate_data(package / "database", client)
    selected = policy.selected_tables(request, validated["manifest"]["tables"])
    models = {data._table_key(model): model for model in data._models()}
    unavailable = selected - models.keys()
    excluded = selected & set(request.get("excluded_tables") or [])
    if unavailable or excluded:
        raise RestoreError(
            "migration_restore_models_unavailable:"
            + ",".join(sorted(unavailable | excluded))
        )
    if policy.mode(request).startswith("merge_"):
        from . import merge

        merge.validate_package(package / "database", included_tables=sorted(selected))
    identity = await data_guard.require_same(client, request["target_identity"])
    objects = (
        await target.database_objects(client)
        if policy.mode(request) == "replace_database"
        or client.capabilities.dialect == "sqlite"
        else None
    )
    if client.capabilities.dialect == "sqlite" and any(
        "CREATE VIRTUAL TABLE" in (item.get("sql") or "").upper()
        for item in objects["objects"]
    ):
        raise RestoreError("migration_sqlite_virtual_rollback_requires_review")
    if policy.target_backup(request) and client.capabilities.dialect == "mysql":
        if any(
            item["type"] == "BASE TABLE" and str(item.get("engine")).lower() != "innodb"
            for item in objects["objects"]
        ):
            raise RestoreError("migration_nontransactional_backup_requires_review")
    if policy.mode(request) == "replace_database":
        await target.database_permissions(client, backup=policy.target_backup(request))
    previous = (request.get("preflight_database") or {}).get("preview")
    if previous is not None and previous != objects:
        raise RestoreError("migration_target_structure_changed")
    if policy.target_backup(request) or (
        policy.mode(request) != "replace_database"
        and client.capabilities.dialect == "sqlite"
    ):
        revision = await target.database_content(client)
        _save(request, target_backup_status="creating")
        receipt = await native_backup.create(
            request["db_url"],
            rollback / "native",
            root=tasks.mutation.INSTANCE_ROOT,
            identity=identity,
            objects=objects,
        )
        if revision != await target.database_content(client):
            raise RestoreError("migration_target_changed_during_backup")
        if objects != await target.database_objects(client):
            raise RestoreError("migration_target_structure_changed")
        changes = {
            "target_native_backup": receipt,
            "target_backup_status": "ready",
            "target_backup_content": revision,
            "target_backup_objects": objects,
        }
        if receipt["engine"] == "sqlite":
            from contextlib import closing
            import sqlite3

            with closing(sqlite3.connect(receipt["path"])) as connection:
                fingerprint = tasks.sqlite_fingerprint(connection)
            changes["target_sqlite_snapshot"] = {
                "file": identity["file"],
                "backup": receipt["path"],
                "fingerprint": fingerprint,
            }
        _save(request, **changes)
    elif policy.mode(request) == "replace_database":
        _save(request, target_backup_status="disabled")
    else:
        receipt = await target.snapshot_tables(client, rollback / "tables", selected)
        _save(request, target_table_snapshot=receipt, target_backup_status="ready")
    if policy.mode(request) == "replace_database":
        _save(request, target_mutation_state="clearing", schema_started=True)
        await target.clear_database(client, expected=objects)
        identity = await data_guard.identity(client)
        changes = {
            "target_cleared": True,
            "target_identity": identity,
            "target_mutation_state": "cleared",
        }
        if client.capabilities.dialect == "sqlite":
            changes["target_last_fingerprint"] = await data.sqlite_revision(client)
        _save(request, **changes)
        await target.checkpoint(client, request, _save, "cleared")
    _save(request, target_prepared=True)


async def prepare_schema(client) -> bool:
    """恢复窗口只创建缺失表，已有结构须先通过独立升级处理"""
    from contextlib import nullcontext

    request = _load()
    if (
        not request
        or request["state"] not in RUNNING
        or request["state"] == "preparing"
    ):
        return False
    if request["state"] not in {
        "importing",
        "rollback_import",
        "verifying",
        "rollback_verifying",
    }:
        raise RestoreError("migration_recovery_required")
    from . import data, data_guard, target

    with tasks.owned(request):
        if request["state"] == "importing" and not request.get("import_started"):
            await _prepare_target(client, request)
        if (
            request.get("import_started")
            and request["state"] == "importing"
            and not request.get("report")
        ):
            report = await target.reconcile(client, request)
            _save(request, report=report, state="verifying")
        rollback = request["state"] in {"rollback_import", "rollback_verifying"}
        expected = (
            request.get("snapshot_identity") if rollback else request["target_identity"]
        )
        sqlite = client.capabilities.dialect == "sqlite"
        context = (
            data_guard.import_transaction(client) if sqlite else nullcontext(client)
        )
        async with context as db:
            identity = await data_guard.require_same(db, expected)
            if not sqlite and request["state"] == "importing":
                package, _ = _paths(request)
                imported_tables = policy.selected_tables(
                    request,
                    json.loads(
                        (package / "database" / data.MANIFEST).read_text("utf-8")
                    )["tables"],
                )
                if request.get("restore_mode"):
                    imported_tables.add("migration_import_records")
                async with data_guard.import_transaction(client) as protected:
                    present = await data_guard.table_names(protected)
                    models = [
                        model
                        for model in data._models()
                        if data_guard.model_key(model, identity.get("schema"))
                        in present
                        and data._table_key(model) in imported_tables
                    ]
                    await data_guard.lock_tables(
                        protected, models, await data_guard.foreign_keys(protected)
                    )
                    await target.prepare(
                        protected, request, data._models(), _save, imported_tables
                    )
                await target.create_missing(client, request, _save)
            if sqlite and request["state"] == "importing":
                fingerprint = (
                    request.get("target_last_fingerprint")
                    or request.get("orm_sqlite_fingerprint")
                    or (_target_snapshot(request) or {}).get("fingerprint")
                )
                if fingerprint and fingerprint not in {
                    await data.sqlite_revision(db),
                    await data.sqlite_revision(db, legacy=True),
                }:
                    raise RestoreError("migration_target_content_changed")
            names = await data_guard.table_names(db)
            schema = identity.get("schema")
            missing = []
            for model in data._models():
                if data_guard.model_key(model, schema) not in names:
                    missing.append(model)
                    continue
                columns = ",".join(
                    data_guard.quote(db, column)
                    for column in model._meta.fields_db_projection.values()
                )
                try:
                    await db.execute_query(
                        f"SELECT {columns} FROM {data_guard.table_sql(db, model)}"
                        " WHERE 1=0"
                    )
                except Exception as error:
                    raise RestoreError(
                        "migration_schema_upgrade_required:" + data._table_key(model)
                    ) from error
            if missing:
                if request["state"] != "importing":
                    raise RestoreError("migration_rollback_schema_changed")
                _save(
                    request,
                    created_tables=[data._table_key(model) for model in missing],
                    schema_started=True,
                )
                await data.create_schema(db, missing)
            if sqlite and request["state"] == "importing":
                _save(
                    request,
                    preimport_sqlite_fingerprint=await data.sqlite_revision(db),
                    schema_completed=True,
                )
        await target.checkpoint(client, request, _save, "schema_ready")
        return True
