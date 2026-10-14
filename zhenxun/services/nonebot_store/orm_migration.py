from __future__ import annotations

from argparse import ArgumentParser, Namespace
import asyncio
from contextlib import closing, suppress
import json
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
    restrict_modules = bool(operations) and any(
        "database_migration_possible" in item for item in operations
    )
    plugins = transaction.get("target_manifest", {}).get("plugins", {})
    for plugin in plugins.values():
        if not isinstance(plugin, dict) or plugin.get("state") != "managed":
            continue
        module_name = str(plugin.get("module_name") or "")
        if module_name and (not restrict_modules or module_name in migration_modules):
            _load_managed_plugin(module_name)


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
        existed = path.exists()
        if existed:
            path.parent.mkdir(parents=True, exist_ok=True)
            with (
                closing(sqlite3.connect(path)) as source,
                closing(sqlite3.connect(backup)) as target,
            ):
                source.backup(target)
        entries.append({"path": str(path), "backup": str(backup), "existed": existed})
    write_json(root / "manifest.json", {"databases": entries})
    return root


def restore() -> int:
    transaction = pending_transaction() or {}
    revision = str(transaction.get("revision") or "")
    if not revision:
        return 0
    manifest_path = _backup_root(revision) / "manifest.json"
    if not manifest_path.is_file():
        return 0
    data = json.loads(manifest_path.read_text(encoding="utf-8"))
    for item in data.get("databases", []):
        path = Path(item["path"])
        backup = Path(item["backup"])
        if item.get("existed"):
            if not backup.is_file():
                raise RuntimeError("orm_rollback_backup_missing")
            path.parent.mkdir(parents=True, exist_ok=True)
            with (
                closing(sqlite3.connect(backup)) as source,
                closing(sqlite3.connect(path)) as target,
            ):
                source.backup(target)
        elif not item.get("existed"):
            path.unlink(missing_ok=True)
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

        from zhenxun.configs.environment import environment_file

        nonebot.init(_env_file=environment_file())
        _load_target_plugins(transaction)
        import nonebot_plugin_orm as orm
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
                return 0
            if external and not manual_external:
                return 3
            cmd_opts.cmd = (migrate.upgrade, [], [])
            external_write_started = external
            await greenlet_spawn(migrate.upgrade, config)
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


def main() -> int:
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

        with InstanceLease(Path.cwd(), role="migration"):
            from .runtime import _staged_build

            current = pending_transaction() or {}
            if current != transaction:
                raise RuntimeError("orm_manual_transaction_changed")
            if _staged_build(current) is None:
                raise RuntimeError("dependency_layer_state_mismatch")
            return asyncio.run(apply(manual_external=True))
    action = sys.argv[1] if len(sys.argv) > 1 else "apply"
    if action == "restore":
        return restore()
    if action == "finalize":
        return finalize()
    return asyncio.run(apply())


if __name__ == "__main__":
    raise SystemExit(main())
