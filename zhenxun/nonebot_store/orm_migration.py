from __future__ import annotations

from argparse import Namespace
import asyncio
import json
from pathlib import Path
import shutil
import sqlite3
import sys
from typing import Any

import click
import nonebot
from sqlalchemy.util import greenlet_spawn

from .storage import (
    ORM_MIGRATION_STATUS_FILE,
    STORE_ROOT,
    generation_path,
    pending_transaction,
    utc_now,
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

    plugins = transaction.get("target_manifest", {}).get("plugins", {})
    for plugin in plugins.values():
        if not isinstance(plugin, dict) or plugin.get("state") != "managed":
            continue
        module_name = str(plugin.get("module_name") or "")
        if module_name:
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
    entries = []
    for index, path in enumerate(paths):
        backup = root / f"database-{index}.sqlite3"
        existed = path.exists()
        if existed:
            path.parent.mkdir(parents=True, exist_ok=True)
            with sqlite3.connect(path) as source, sqlite3.connect(backup) as target:
                source.backup(target)
        entries.append({"path": str(path), "backup": str(backup), "existed": existed})
    (root / "manifest.json").write_text(
        json.dumps({"databases": entries}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return root


def restore() -> int:
    transaction = pending_transaction() or {}
    revision = str(transaction.get("revision") or "")
    manifest_path = _backup_root(revision) / "manifest.json"
    if not manifest_path.is_file():
        return 0
    data = json.loads(manifest_path.read_text(encoding="utf-8"))
    for item in data.get("databases", []):
        path = Path(item["path"])
        backup = Path(item["backup"])
        if item.get("existed") and backup.is_file():
            path.parent.mkdir(parents=True, exist_ok=True)
            with sqlite3.connect(backup) as source, sqlite3.connect(path) as target:
                source.backup(target)
        elif not item.get("existed"):
            path.unlink(missing_ok=True)
    write_json(
        ORM_MIGRATION_STATUS_FILE,
        {"status": "restored", "revision": revision, "updated_at": utc_now()},
    )
    return 0


def finalize() -> int:
    transaction = pending_transaction() or {}
    revision = str(transaction.get("revision") or "")
    root = _backup_root(revision)
    if root.is_dir():
        shutil.rmtree(root, ignore_errors=True)
    ORM_MIGRATION_STATUS_FILE.unlink(missing_ok=True)
    return 0


async def apply() -> int:
    transaction = pending_transaction()
    if not transaction or not transaction.get("database_migration_possible"):
        return 0
    if not transaction.get("database_migration_confirmed"):
        return 4
    revision = str(transaction.get("revision") or "")
    try:
        _activate_staged_generation(transaction)
        nonebot.init()
        _load_target_plugins(transaction)
        import nonebot_plugin_orm as orm
        from nonebot_plugin_orm import migrate

        orm._init_orm()
        sqlite_paths, external = _sqlite_paths(orm)
        cmd_opts = Namespace()
        migration_required = False
        with migrate.AlembicConfig(cmd_opts=cmd_opts) as config:
            cmd_opts.cmd = (migrate.check, [], [])
            try:
                await greenlet_spawn(migrate.check, config)
            except click.ClickException:
                migration_required = True
            if not migration_required:
                write_json(
                    ORM_MIGRATION_STATUS_FILE,
                    {
                        "status": "current",
                        "revision": revision,
                        "updated_at": utc_now(),
                    },
                )
                return 0
            if external:
                write_json(
                    ORM_MIGRATION_STATUS_FILE,
                    {
                        "status": "external_manual_required",
                        "revision": revision,
                        "updated_at": utc_now(),
                    },
                )
                return 3
            _backup_sqlite(sqlite_paths, revision)
            cmd_opts.cmd = (migrate.upgrade, [], [])
            await greenlet_spawn(migrate.upgrade, config)
        write_json(
            ORM_MIGRATION_STATUS_FILE,
            {"status": "migrated", "revision": revision, "updated_at": utc_now()},
        )
        return 0
    except Exception as error:
        write_json(
            ORM_MIGRATION_STATUS_FILE,
            {
                "status": "failed",
                "revision": revision,
                "error_code": type(error).__name__,
                "updated_at": utc_now(),
            },
        )
        restore()
        return 2
    finally:
        with __import__("contextlib").suppress(Exception):
            for engine in getattr(locals().get("orm"), "_engines", {}).values():
                await engine.dispose()


def main() -> int:
    action = sys.argv[1] if len(sys.argv) > 1 else "apply"
    if action == "restore":
        return restore()
    if action == "finalize":
        return finalize()
    return asyncio.run(apply())


if __name__ == "__main__":
    raise SystemExit(main())
