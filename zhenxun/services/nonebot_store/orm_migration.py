from __future__ import annotations

from argparse import ArgumentParser, Namespace
import asyncio
from contextlib import closing, contextmanager, suppress
import json
import os
from pathlib import Path
import sqlite3
import sys
from typing import Any

import click
import nonebot

from .storage import (
    PENDING_FILE,
    STORE_ROOT,
    generation_path,
    pending_transaction,
    remove_orm_backups,
    write_json,
)


def _backup_root(revision: str) -> Path:
    return STORE_ROOT / "orm-backups" / revision


def _activate_staged_generation(transaction: dict[str, Any]) -> Path:
    generation = transaction.get("generation")
    if not isinstance(generation, int):
        raise RuntimeError("orm_migration_generation_missing")
    root = generation_path(generation).resolve()
    if not root.is_dir():
        raise RuntimeError("orm_migration_generation_missing")
    sys.path.insert(0, str(root))
    return root


def _load_target_plugins(transaction: dict[str, Any]) -> None:
    """加载需要执行迁移的插件，恢复备份时加载全部托管插件与带迁移脚本的源码插件

    参数:
        transaction: 依赖层事务
    """
    from .runtime import _load_managed_plugin

    operations = [
        item for item in transaction.get("operations", []) if isinstance(item, dict)
    ]
    migration_modules = {
        str(item.get("module_name") or "")
        for item in operations
        if item.get("database_migration_possible")
    }
    # Transactions created before migration ownership was recorded require the
    # conservative legacy behavior.
    restore = bool(transaction.get("restore"))
    restrict_modules = (
        not restore
        and bool(operations)
        and any("database_migration_possible" in item for item in operations)
    )
    plugins = transaction.get("target_manifest", {}).get("plugins", {})
    for plugin in plugins.values():
        if not isinstance(plugin, dict) or plugin.get("state") != "managed":
            continue
        module_name = str(plugin.get("module_name") or "")
        if module_name and (not restrict_modules or module_name in migration_modules):
            _load_managed_plugin(module_name)
    if restore:
        _load_source_migration_plugins()


def _load_source_migration_plugins() -> None:
    """加载带 migrations 目录的源码插件，加载失败的插件跳过，由 worker 启动时隔离"""
    from nonebot.plugin import _managers
    from nonebot.plugin.manager import PluginManager

    from zhenxun.configs.config import BotConfig

    from .dependencies import safe_process_error

    roots = [Path("zhenxun/plugins")]
    roots += [Path(path.strip()) for path in BotConfig.ext_path if path.strip()]
    for root in roots:
        if not root.is_dir():
            continue
        manager = PluginManager(search_path=[str(root.resolve())])
        _managers.append(manager)
        for plugin_id, module_name in sorted(manager.controlled_modules.items()):
            if not (root / module_name.rsplit(".", 1)[-1] / "migrations").is_dir():
                continue
            try:
                manager.load_plugin(plugin_id)
            except Exception as error:
                sys.stderr.write(
                    f"skip source plugin {plugin_id}: {type(error).__name__}: "
                    f"{safe_process_error(str(error))}\n"
                )


def _sqlite_paths(orm_module: Any) -> tuple[list[Path], bool]:
    paths: list[Path] = []
    external = False
    for engine in orm_module._engines.values():
        url = engine.url
        if str(url.drivername).startswith("sqlite"):
            database = str(url.database or "")
            if database and database != ":memory:":
                paths.append(Path(database).resolve())
        else:
            external = True
    return list(dict.fromkeys(paths)), external


def _backup_sqlite(paths: list[Path], revision: str) -> Path:
    from zhenxun.services.migration.state import sqlite_fingerprint

    root = _backup_root(revision)
    root.mkdir(parents=True, exist_ok=True)
    manifest = root / "manifest.json"
    if manifest.is_file():
        existing = json.loads(manifest.read_text(encoding="utf-8"))
        if {item["path"] for item in existing.get("databases", [])} != {
            str(path) for path in paths
        }:
            raise RuntimeError("orm_backup_target_changed")
        return root
    entries = []
    for index, path in enumerate(paths):
        backup = root / f"database-{index}.sqlite3"
        transaction = pending_transaction() or {}
        if parent_id := transaction.get("parent_operation_id"):
            from zhenxun.services.migration import state as restore_tasks

            parent = restore_tasks.load(parent_id)
            from zhenxun.services.migration.restore import _target_snapshot

            snapshot = _target_snapshot(parent or {})
            if snapshot and path.exists() and path.samefile(snapshot["file"]):
                entries.append(
                    {
                        "path": str(path),
                        "backup": snapshot["backup"],
                        "existed": True,
                        "parent_operation_id": parent_id,
                    }
                )
                continue
        existed = path.exists()
        if existed:
            path.parent.mkdir(parents=True, exist_ok=True)
            with (
                closing(sqlite3.connect(path)) as source,
                closing(sqlite3.connect(backup)) as target,
            ):
                source.backup(target)
        with closing(sqlite3.connect(backup)) as database:
            fingerprint = sqlite_fingerprint(database)
        entries.append(
            {
                "path": str(path),
                "backup": str(backup),
                "existed": existed,
                "fingerprint": fingerprint,
            }
        )
    write_json(root / "manifest.json", {"version": 2, "databases": entries})
    return root


def _record_sqlite_applied(revision: str) -> None:
    """记录 ORM 执行完成后的文件身份和内容，供独立文件回退时复核。"""
    from zhenxun.services.migration.state import sqlite_fingerprint

    manifest = _backup_root(revision) / "manifest.json"
    record = json.loads(manifest.read_text("utf-8"))
    for item in record["databases"]:
        if item.get("parent_operation_id"):
            continue
        path = Path(item["path"])
        if not path.exists():
            if item["existed"]:
                raise RuntimeError("orm_sqlite_target_missing")
            continue
        with closing(sqlite3.connect(path, timeout=3, isolation_level=None)) as db:
            db.execute("BEGIN IMMEDIATE")
            try:
                item["applied_fingerprint"] = sqlite_fingerprint(db)
                stat = path.stat()
                item["file_identity"] = [stat.st_dev, stat.st_ino]
            finally:
                db.rollback()
    write_json(manifest, record)


def restore() -> int:
    transaction = pending_transaction() or {}
    revision = str(transaction.get("revision") or "")
    if not revision:
        return 0
    manifest_path = _backup_root(revision) / "manifest.json"
    if not manifest_path.is_file():
        if transaction.get("orm_write_started"):
            raise RuntimeError("orm_rollback_manifest_missing")
        return 0
    data = json.loads(manifest_path.read_text(encoding="utf-8"))
    for item in data.get("databases", []):
        if item.get("parent_operation_id"):
            if item["parent_operation_id"] != transaction.get("parent_operation_id"):
                raise RuntimeError("orm_parent_snapshot_owner_changed")
            continue
        path = Path(item["path"])
        backup = Path(item["backup"])
        if not path.exists() and not item.get("existed"):
            continue
        if not item.get("fingerprint"):
            raise RuntimeError("orm_rollback_evidence_missing")
        if not path.exists():
            raise RuntimeError("orm_sqlite_target_missing")
        stat = path.stat()
        if (
            item.get("file_identity")
            and [stat.st_dev, stat.st_ino] != item["file_identity"]
        ):
            raise RuntimeError("orm_sqlite_target_replaced")
        from zhenxun.services.migration.state import restore_sqlite

        restore_sqlite(
            {
                "file": str(path),
                "backup": str(backup),
                "fingerprint": item["fingerprint"],
            },
            item.get("applied_fingerprint") or item["fingerprint"],
            backup_root=_backup_root(revision),
        )
        if not item["existed"]:
            path.unlink()
    return 0


def finalize() -> int:
    transaction = pending_transaction() or {}
    revision = str(transaction.get("revision") or "")
    if not revision:
        return 0
    remove_orm_backups()
    return 0


async def apply(*, manual_external: bool = False) -> int:
    transaction = pending_transaction()
    if not transaction or not transaction.get("database_migration_possible"):
        return 0
    if not transaction.get("database_migration_confirmed"):
        return 4
    revision = str(transaction.get("revision") or "")
    external_write_started = False
    try:
        _activate_staged_generation(transaction)
        from sqlalchemy.util import greenlet_spawn

        from zhenxun.configs.environment import (
            environment_file,
            migrate_orm_data_dir,
            sync_env_missing_items,
        )

        sync_env_missing_items(Path.cwd())
        nonebot.init(_env_file=environment_file())
        migrate_orm_data_dir()
        _load_target_plugins(transaction)
        orm = nonebot.require("nonebot_plugin_orm")
        from nonebot_plugin_orm import migrate

        orm._init_orm()
        sqlite_paths, external = _sqlite_paths(orm)
        if not external:
            _backup_sqlite(sqlite_paths, revision)
        cmd_opts = Namespace()
        migration_required = False
        with migrate.AlembicConfig(cmd_opts=cmd_opts) as config:
            cmd_opts.cmd = (migrate.check, [], [])
            try:
                await greenlet_spawn(migrate.check, config)
            except click.ClickException:
                migration_required = True
            if not migration_required:
                if not external:
                    _record_sqlite_applied(revision)
                return 0
            if external and not manual_external:
                return 3
            cmd_opts.cmd = (migrate.upgrade, [], [])
            transaction["orm_write_started"] = True
            write_json(PENDING_FILE, transaction)
            external_write_started = external
            await greenlet_spawn(migrate.upgrade, config)
        if not external:
            _record_sqlite_applied(revision)
        if parent_id := transaction.get("parent_operation_id"):
            from zhenxun.services.migration import state as restore_tasks

            parent = restore_tasks.load(parent_id)
            from zhenxun.services.migration.restore import _target_snapshot

            if snapshot := _target_snapshot(parent or {}):
                with closing(sqlite3.connect(snapshot["file"])) as database:
                    fingerprint = restore_tasks.sqlite_fingerprint(database)
                with restore_tasks.owned(parent):
                    restore_tasks.save(parent, orm_sqlite_fingerprint=fingerprint)
        return 0
    except Exception as error:
        from .dependencies import safe_process_error

        rollback_error = (
            {"code": "external_database_state_unconfirmed"}
            if external_write_started
            else None
        )
        try:
            for engine in getattr(locals().get("orm"), "_engines", {}).values():
                await engine.dispose()
            if not external_write_started:
                restore()
        except Exception as failure:
            rollback_error = {
                "code": type(failure).__name__,
                "diagnostic": safe_process_error(str(failure)),
            }
        sys.stderr.write(
            f"{type(error).__name__}: {safe_process_error(str(error))}\n"
            + (f"rollback: {rollback_error}\n" if rollback_error else "")
        )
        if external_write_started:
            write_json(
                PENDING_FILE,
                {
                    **transaction,
                    "state": "recovery_required",
                    "error_code": "external_database_state_unconfirmed",
                },
            )
        return 5 if rollback_error else 2
    finally:
        with suppress(Exception):
            for engine in getattr(locals().get("orm"), "_engines", {}).values():
                await engine.dispose()


@contextmanager
def _parent_scope(transaction: dict):
    """核对依赖修订后在父迁移任务归属内执行 ORM 操作"""
    if parent_id := transaction.get("parent_operation_id"):
        from zhenxun.services.migration import state as restore_tasks

        parent = restore_tasks.load(parent_id)
        if not parent or (parent.get("dependencies") or {}).get(
            "revision"
        ) != transaction.get("revision"):
            raise RuntimeError("orm_parent_transaction_changed")
        with restore_tasks.owned(parent):
            yield
    else:
        yield


def _main() -> int:
    if len(sys.argv) > 1 and sys.argv[1] == "manual-upgrade":
        parser = ArgumentParser(
            description=(
                "Upgrade the pending plugin schema in its candidate environment. "
                "Dependencies are never installed by this command."
            )
        )
        parser.add_argument("action", choices=["manual-upgrade"])
        parser.add_argument("--revision", required=True)
        parser.add_argument("--backup-confirmed", action="store_true", required=True)
        arguments = parser.parse_args()
        transaction = pending_transaction() or {}
        if (
            transaction.get("revision") != arguments.revision
            or transaction.get("state") != "migration_blocked"
        ):
            raise RuntimeError("orm_manual_transaction_changed")
        from zhenxun.services.launcher.lease import InstanceLease

        with InstanceLease(Path.cwd(), role="migration") as lease:
            from .runtime import _staged_build

            current = pending_transaction() or {}
            if current != transaction:
                raise RuntimeError("orm_manual_transaction_changed")
            if _staged_build(current) is None:
                raise RuntimeError("dependency_layer_state_mismatch")
            if parent_id := current.get("parent_operation_id"):
                from zhenxun.services.migration import state as restore_tasks

                parent = restore_tasks.load(parent_id) or {}
                if parent.get("state") != "awaiting_orm":
                    raise RuntimeError("orm_parent_not_waiting")
            keys = ("ZHENXUN_INSTANCE_LEASE_ID", "ZHENXUN_LAUNCHER_PID")
            previous = {key: os.environ.get(key) for key in keys}
            os.environ.update(
                ZHENXUN_INSTANCE_LEASE_ID=lease.identity,
                ZHENXUN_LAUNCHER_PID=str(os.getpid()),
            )
            try:
                with _parent_scope(current):
                    return asyncio.run(apply(manual_external=True))
            finally:
                for key, value in previous.items():
                    if value is None:
                        os.environ.pop(key, None)
                    else:
                        os.environ[key] = value
    action = sys.argv[1] if len(sys.argv) > 1 else "apply"
    if action == "restore":
        return restore()
    if action == "finalize":
        return finalize()
    return asyncio.run(apply())


def main() -> int:
    """在已绑定迁移任务的授权范围内执行 ORM 子阶段"""
    transaction = pending_transaction() or {}
    expected = os.environ.get("ZHENXUN_ORM_TRANSACTION_REVISION")
    if expected is not None and expected != str(transaction.get("revision") or ""):
        raise RuntimeError("orm_transaction_changed")
    if len(sys.argv) > 1 and sys.argv[1] == "manual-upgrade":
        return _main()
    with _parent_scope(transaction):
        return _main()


if __name__ == "__main__":
    raise SystemExit(main())
