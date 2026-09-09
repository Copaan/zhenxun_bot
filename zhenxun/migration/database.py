from __future__ import annotations

from collections.abc import Callable
from contextlib import closing
from pathlib import Path
import shutil
import sqlite3
import time

from .archive import require_space
from .errors import MigrationError
from .paths import contained_path

# Business keys are deliberately explicit; surrogate IDs are never identities.
BUSINESS_KEYS: dict[str, tuple[str, ...]] = {
    "user_console": ("user_id",),
    "bot_console": ("bot_id",),
    "plugin_info": ("module_path",),
    "group_console": ("group_id", "channel_id"),
    "group_tags": ("name",),
    "group_tag_links": ("tag_id", "group_id"),
    "plugin_policy": ("name",),
    "bot_plugin_policy_binding": ("bot_id",),
    "bot_group_membership": ("bot_id", "platform_scope", "group_id", "channel_id"),
    "bot_group_plugin_policy": ("bot_id", "platform_scope", "group_id", "channel_id"),
    "friend_users": ("user_id",),
    "sign_users": ("user_id",),
    "user_props": ("user_id",),
    "level_users": ("user_id", "group_id"),
    "group_info_users": ("user_id", "group_id"),
    "group_plugin_settings": ("group_id", "plugin_name"),
    "goods_info": ("goods_name",),
    "ban_console": ("user_id", "group_id"),
}


def database_capabilities() -> dict:
    return {
        "sqlite": {
            "snapshot": True,
            "candidate_incremental": True,
            "engine_version": sqlite3.sqlite_version,
        },
        "postgres": {
            "dump_tool": bool(shutil.which("pg_dump")),
            "restore_tool": bool(shutil.which("pg_restore")),
        },
        "mysql": {
            "dump_tool": bool(shutil.which("mysqldump")),
            "restore_tool": bool(shutil.which("mysql")),
        },
    }


def _check_deadline(deadline: float, cancel: Callable[[], bool] | None = None) -> None:
    if cancel and cancel():
        raise MigrationError("migration_cancelled")
    if time.monotonic() >= deadline:
        raise MigrationError("migration_database_timeout")


def _readonly(path: Path, *, immutable: bool = False) -> sqlite3.Connection:
    contained_path(path.parent, path.name, regular=True)
    connection = sqlite3.connect(
        path.absolute().as_uri() + "?mode=ro" + ("&immutable=1" if immutable else ""),
        uri=True,
        timeout=1,
    )
    connection.execute("PRAGMA trusted_schema=OFF")
    connection.execute("PRAGMA query_only=ON")
    return connection


def snapshot_sqlite(
    source: Path,
    destination: Path,
    *,
    deadline: float,
    cancel: Callable[[], bool] | None = None,
    immutable: bool = False,
) -> dict:
    """Use SQLite's backup API, including committed pages still present in the WAL."""
    _check_deadline(deadline, cancel)
    contained_path(destination.parent, destination.name)
    if destination.exists():
        raise MigrationError("migration_database_candidate_exists", status=409)
    require_space(destination.parent, source.stat().st_size * 2)
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.touch(mode=0o600, exist_ok=False)
    try:
        with (
            closing(_readonly(source, immutable=immutable)) as original,
            closing(sqlite3.connect(destination)) as backup,
        ):

            def progress(_status, _remaining, _total):
                _check_deadline(deadline, cancel)

            original.backup(backup, pages=128, progress=progress, sleep=0.05)
            backup.set_progress_handler(lambda: int(time.monotonic() >= deadline), 1000)
            if backup.execute("PRAGMA quick_check").fetchone() != ("ok",):
                raise MigrationError("migration_database_integrity_failed")
            version = backup.execute("PRAGMA user_version").fetchone()[0]
        return {
            "engine": "sqlite",
            "engine_version": sqlite3.sqlite_version,
            "user_version": version,
            "size": destination.stat().st_size,
        }
    except BaseException:
        destination.unlink(missing_ok=True)
        raise


def _quote(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _tables(
    connection: sqlite3.Connection, *, max_tables: int = 2048
) -> dict[str, str]:
    if connection.execute(
        "SELECT 1 FROM sqlite_master WHERE length(sql) > 1048576 LIMIT 1"
    ).fetchone():
        raise MigrationError("migration_database_schema_limit")
    records = connection.execute(
        "SELECT name, sql FROM sqlite_master "
        "WHERE type='table' AND name NOT LIKE 'sqlite_%'"
    ).fetchmany(max_tables + 1)
    if len(records) > max_tables:
        raise MigrationError("migration_database_table_limit")
    return dict(records)


def _columns(connection: sqlite3.Connection, table: str) -> list[tuple]:
    return list(connection.execute(f"PRAGMA table_info({_quote(table)})"))


def _foreign_keys(connection: sqlite3.Connection, table: str) -> list[tuple]:
    return list(connection.execute(f"PRAGMA foreign_key_list({_quote(table)})"))


def _unique_keys(connection: sqlite3.Connection, table: str) -> list[tuple]:
    result = []
    for index in connection.execute(f"PRAGMA index_list({_quote(table)})"):
        if index[2]:
            columns = tuple(
                row[2]
                for row in connection.execute(f"PRAGMA index_info({_quote(index[1])})")
            )
            result.append((columns, index[4]))
    return sorted(result, key=str)


def prepare_sqlite_incremental(
    source: Path,
    target: Path,
    candidate: Path,
    *,
    deadline: float,
    max_rows: int = 5_000_000,
    max_mapping_rows: int = 250_000,
    max_row_bytes: int = 16 * 1024 * 1024,
    cancel: Callable[[], bool] | None = None,
) -> dict:
    """Merge into a consistent *copy* of the target; conflicts never modify it."""
    if min(max_rows, max_mapping_rows, max_row_bytes) <= 0:
        raise MigrationError("migration_database_limits_invalid")
    snapshot_sqlite(target, candidate, deadline=deadline, cancel=cancel)
    conflicts, summaries = [], []
    result = {
        "engine": "sqlite",
        "state": "candidate_unverified",
        "tables": summaries,
        "conflicts": conflicts,
    }
    with (
        closing(_readonly(source)) as incoming,
        closing(sqlite3.connect(candidate)) as current,
    ):
        incoming.row_factory = sqlite3.Row
        current.row_factory = sqlite3.Row
        current.execute("PRAGMA foreign_keys=ON")
        current.execute("PRAGMA trusted_schema=OFF")
        for connection in (incoming, current):
            connection.set_progress_handler(
                lambda: int(time.monotonic() >= deadline or bool(cancel and cancel())),
                1000,
            )
        src_tables, dst_tables = _tables(incoming), _tables(current)
        mapping: dict[str, dict[object, object]] = {}
        pending = set(src_tables)
        seen = 0
        mapped = 0
        current.execute("BEGIN IMMEDIATE")
        try:
            while pending:
                progress = False
                for table in sorted(pending):
                    _check_deadline(deadline, cancel)
                    foreign = _foreign_keys(incoming, table)
                    if any(fk[2] in pending for fk in foreign):
                        continue
                    progress = True
                    pending.remove(table)
                    keys = BUSINESS_KEYS.get(table)
                    columns = _columns(incoming, table)
                    primary = [column for column in columns if column[5]]
                    stable_unknown = (
                        not keys
                        and bool(primary)
                        and all(
                            column[2].upper() == "TEXT" and column[3]
                            for column in primary
                        )
                        and not foreign
                    )
                    if stable_unknown:
                        keys = tuple(
                            column[1] for column in sorted(primary, key=lambda c: c[5])
                        )
                    if not keys:
                        conflicts.append(
                            {"table": table, "code": "migration_business_key_unknown"}
                        )
                        continue
                    if len(columns) > 512:
                        raise MigrationError("migration_database_column_limit")
                    if (
                        table not in dst_tables
                        or columns != _columns(current, table)
                        or foreign != _foreign_keys(current, table)
                        or _unique_keys(incoming, table) != _unique_keys(current, table)
                        or "virtual" in (src_tables[table] or "").lower()
                        or (
                            stable_unknown
                            and src_tables[table] != dst_tables.get(table)
                        )
                    ):
                        conflicts.append(
                            {"table": table, "code": "migration_schema_incompatible"}
                        )
                        continue
                    if current.execute(
                        "SELECT 1 FROM sqlite_master "
                        "WHERE type='trigger' AND tbl_name=?",
                        (table,),
                    ).fetchone():
                        conflicts.append(
                            {
                                "table": table,
                                "code": "migration_trigger_review_required",
                            }
                        )
                        continue
                    names = [column[1] for column in columns]
                    if not set(keys) <= set(names) or (
                        not stable_unknown
                        and (len(primary) != 1 or primary[0][2].upper() != "INTEGER")
                    ):
                        conflicts.append(
                            {
                                "table": table,
                                "code": "migration_primary_key_unsupported",
                            }
                        )
                        continue
                    pk = primary[0][1]
                    if any(fk[1] != 0 or fk[4] != "id" for fk in foreign):
                        conflicts.append(
                            {"table": table, "code": "migration_reference_unsupported"}
                        )
                        continue
                    qtable = _quote(table)
                    row_size = "+".join(
                        f"coalesce(length(CAST({_quote(name)} AS BLOB)),0)"
                        for name in names
                    )
                    if any(
                        connection.execute(
                            f"SELECT 1 FROM {qtable} WHERE ({row_size}) > ? LIMIT 1",
                            (max_row_bytes,),
                        ).fetchone()
                        for connection in (incoming, current)
                    ):
                        raise MigrationError("migration_database_row_size_limit")
                    grouped = ",".join(map(_quote, keys))
                    duplicate = incoming.execute(
                        f"SELECT 1 FROM {qtable} GROUP BY {grouped} "
                        "HAVING COUNT(*) > 1 LIMIT 1"
                    ).fetchone()
                    if duplicate:
                        conflicts.append(
                            {"table": table, "code": "migration_business_key_ambiguous"}
                        )
                        continue
                    mapping[table] = {}
                    added = existing = rejected = 0
                    for original in incoming.execute(f"SELECT * FROM {qtable}"):
                        _check_deadline(deadline, cancel)
                        seen += 1
                        if seen > max_rows:
                            raise MigrationError("migration_database_row_limit")
                        row = dict(original)
                        valid = True
                        for fk in foreign:
                            if row[fk[3]] is None:
                                continue
                            translated = mapping.get(fk[2], {}).get(row[fk[3]])
                            if translated is None:
                                valid = False
                                break
                            row[fk[3]] = translated
                        if not valid:
                            rejected += 1
                            continue
                        where = " AND ".join(f"{_quote(key)} IS ?" for key in keys)
                        matches = current.execute(
                            f"SELECT {_quote(pk)} FROM {qtable} WHERE {where} LIMIT 2",
                            tuple(row[key] for key in keys),
                        ).fetchall()
                        if len(matches) > 1:
                            rejected += 1
                            continue
                        if matches:
                            if stable_unknown:
                                same = current.execute(
                                    f"SELECT * FROM {qtable} WHERE {where}",
                                    tuple(row[key] for key in keys),
                                ).fetchone()
                                if dict(same) != row:
                                    rejected += 1
                                    continue
                            else:
                                mapped += 1
                                if mapped > max_mapping_rows:
                                    raise MigrationError(
                                        "migration_database_mapping_limit"
                                    )
                                mapping[table][original[pk]] = matches[0][0]
                            existing += 1
                            continue
                        values = {
                            key: value
                            for key, value in row.items()
                            if stable_unknown or key != pk
                        }
                        sql = (
                            f"INSERT INTO {qtable} ({','.join(map(_quote, values))}) "
                            f"VALUES ({','.join('?' for _ in values)})"
                        )
                        try:
                            cursor = current.execute(sql, tuple(values.values()))
                        except sqlite3.IntegrityError:
                            rejected += 1
                            continue
                        if not stable_unknown:
                            mapped += 1
                            if mapped > max_mapping_rows:
                                raise MigrationError("migration_database_mapping_limit")
                            mapping[table][original[pk]] = cursor.lastrowid
                        added += 1
                    summaries.append(
                        {
                            "table": table,
                            "added": added,
                            "existing": existing,
                            "conflicting": rejected,
                        }
                    )
                    if rejected:
                        conflicts.append(
                            {
                                "table": table,
                                "code": "migration_key_or_reference_conflict",
                                "count": rejected,
                            }
                        )
                if not progress:
                    conflicts.extend(
                        {"table": table, "code": "migration_reference_cycle"}
                        for table in sorted(pending)
                    )
                    break
            if current.execute("PRAGMA foreign_key_check").fetchone():
                raise MigrationError("migration_database_reference_invalid")
            current.commit()
        except BaseException:
            current.rollback()
            _check_deadline(deadline, cancel)
            raise
    if conflicts:
        result["state"] = "conflicts_require_confirmation"
    return result
