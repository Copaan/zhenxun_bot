from __future__ import annotations

from contextlib import suppress
import os
from typing import Any

from tortoise import Tortoise, fields

from zhenxun.services.db_context import Model
from zhenxun.services.db_context.schema_ops import AddColumn


class GroupPluginSetting(Model):
    id = fields.IntField(pk=True, generated=True, auto_increment=True)
    """自增ID"""
    group_id = fields.CharField(max_length=255, indexed=True, description="群组ID")
    """群组ID"""
    bot_id = fields.CharField(
        max_length=255, null=True, description="作用域 Bot ID，空值表示历史全局配置"
    )
    platform_scope = fields.CharField(
        max_length=64, null=True, description="作用域平台"
    )
    channel_id = fields.CharField(
        max_length=255, null=True, description="频道或子频道作用域"
    )
    plugin_name = fields.CharField(
        max_length=255, indexed=True, description="插件模块名"
    )
    """插件模块名"""
    settings = fields.JSONField(description="插件的完整配置 (JSON)")
    """插件的完整配置 (JSON)"""
    updated_at = fields.DatetimeField(auto_now=True, description="最后更新时间")
    """最后更新时间"""

    class Meta:  # pyright: ignore [reportIncompatibleVariableOverride]
        table = "group_plugin_settings"
        table_description = "插件分群通用配置表"
        unique_together = (
            "bot_id",
            "platform_scope",
            "group_id",
            "channel_id",
            "plugin_name",
        )
        indexes = (("group_id", "plugin_name"),)

    @classmethod
    async def _run_script(cls):
        return [
            AddColumn("group_plugin_settings", "bot_id", "VARCHAR(255)"),
            AddColumn("group_plugin_settings", "platform_scope", "VARCHAR(64)"),
            AddColumn("group_plugin_settings", "channel_id", "VARCHAR(255)"),
        ]


_GROUP_PLUGIN_TABLE = "group_plugin_settings"
_LEGACY_SCOPE_COLUMNS = ("group_id", "plugin_name")
_SCOPED_COLUMNS = (
    "bot_id",
    "platform_scope",
    "group_id",
    "channel_id",
    "plugin_name",
)
_SCOPED_CONSTRAINT = "uid_group_plugi_bot_id_eefbc7"


def _quote(identifier: str, dialect: str) -> str:
    if dialect == "mysql":
        return f"`{identifier.replace('`', '``')}`"
    return f'"{identifier.replace(chr(34), chr(34) + chr(34))}"'


async def _unique_definitions(
    connection: Any, dialect: str
) -> list[tuple[str, tuple[str, ...]]]:
    if dialect == "sqlite":
        rows = await connection.execute_query_dict(
            f"PRAGMA index_list({_quote(_GROUP_PLUGIN_TABLE, dialect)})"
        )
        definitions = []
        for row in rows:
            if not bool(row.get("unique")) or not row.get("name"):
                continue
            index_name = str(row["name"])
            columns = await connection.execute_query_dict(
                f"PRAGMA index_info({_quote(index_name, dialect)})"
            )
            definitions.append(
                (
                    index_name,
                    tuple(
                        str(item["name"])
                        for item in sorted(
                            columns, key=lambda item: int(item.get("seqno") or 0)
                        )
                        if item.get("name")
                    ),
                )
            )
        return definitions
    if dialect == "postgres":
        rows = await connection.execute_query_dict(
            "SELECT tc.constraint_name, kcu.column_name, kcu.ordinal_position "
            "FROM information_schema.table_constraints tc "
            "JOIN information_schema.key_column_usage kcu "
            "ON tc.constraint_name = kcu.constraint_name "
            "AND tc.table_schema = kcu.table_schema "
            "AND tc.table_name = kcu.table_name "
            "WHERE tc.table_schema = current_schema() "
            "AND tc.table_name = $1 AND tc.constraint_type = 'UNIQUE' "
            "ORDER BY tc.constraint_name, kcu.ordinal_position",
            [_GROUP_PLUGIN_TABLE],
        )
    else:
        rows = await connection.execute_query_dict(
            "SELECT INDEX_NAME AS constraint_name, COLUMN_NAME AS column_name, "
            "SEQ_IN_INDEX AS ordinal_position "
            "FROM information_schema.statistics "
            "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = %s "
            "AND NON_UNIQUE = 0 AND INDEX_NAME <> 'PRIMARY' "
            "ORDER BY INDEX_NAME, SEQ_IN_INDEX",
            [_GROUP_PLUGIN_TABLE],
        )
    grouped: dict[str, list[tuple[int, str]]] = {}
    for row in rows:
        name = str(row.get("constraint_name") or "")
        column = str(row.get("column_name") or "")
        if name and column:
            grouped.setdefault(name, []).append(
                (int(row.get("ordinal_position") or 0), column)
            )
    return [
        (name, tuple(column for _, column in sorted(items)))
        for name, items in grouped.items()
    ]


async def _rebuild_sqlite_scope_constraint(connection: Any) -> None:
    """Rebuild SQLite's table because its inline UNIQUE autoindex is not droppable."""
    columns = await connection.execute_query_dict(
        f"PRAGMA table_info({_quote(_GROUP_PLUGIN_TABLE, 'sqlite')})"
    )
    if not columns:
        return
    column_names = [str(row["name"]) for row in columns if row.get("name")]
    if not set(_SCOPED_COLUMNS).issubset(column_names):
        raise RuntimeError("group_plugin_settings_scope_columns_missing")

    definitions = []
    for row in columns:
        name = str(row["name"])
        sql_type = str(row.get("type") or "TEXT")
        definition = f"{_quote(name, 'sqlite')} {sql_type}"
        if int(row.get("pk") or 0) == 1:
            definition += " PRIMARY KEY"
            if name == "id" and sql_type.upper() == "INTEGER":
                definition += " AUTOINCREMENT"
        elif int(row.get("notnull") or 0):
            definition += " NOT NULL"
        default = row.get("dflt_value")
        if default is not None:
            definition += f" DEFAULT {default}"
        definitions.append(definition)

    indexes = await connection.execute_query_dict(
        "SELECT name FROM sqlite_master WHERE type = 'index' "
        f"AND tbl_name = '{_GROUP_PLUGIN_TABLE}'"
    )
    index_definitions: list[tuple[str, tuple[str, ...]]] = []
    for index in indexes:
        name = str(index.get("name") or "")
        if not name or name.startswith("sqlite_autoindex"):
            continue
        info = await connection.execute_query_dict(
            f"PRAGMA index_info({_quote(name, 'sqlite')})"
        )
        index_columns = tuple(
            str(item["name"])
            for item in sorted(info, key=lambda item: int(item.get("seqno") or 0))
            if item.get("name")
        )
        if index_columns:
            index_definitions.append((name, index_columns))

    temporary_table = f"{_GROUP_PLUGIN_TABLE}__scope_{os.getpid()}"
    table_sql = _quote(_GROUP_PLUGIN_TABLE, "sqlite")
    temporary_sql = _quote(temporary_table, "sqlite")
    columns_sql = ", ".join(_quote(name, "sqlite") for name in column_names)
    copy_columns = ", ".join(_quote(name, "sqlite") for name in column_names)
    unique_sql = ", ".join(_quote(name, "sqlite") for name in _SCOPED_COLUMNS)
    statements = [
        "PRAGMA foreign_keys=OFF",
        f"DROP TABLE IF EXISTS {temporary_sql}",
        f"CREATE TABLE {temporary_sql} ({', '.join(definitions)}, "
        f"CONSTRAINT {_quote(_SCOPED_CONSTRAINT, 'sqlite')} UNIQUE ({unique_sql}))",
        f"INSERT INTO {temporary_sql} ({copy_columns}) "
        f"SELECT {columns_sql} FROM {table_sql}",
        f"DROP TABLE {table_sql}",
        f"ALTER TABLE {temporary_sql} RENAME TO {table_sql}",
    ]
    statements.extend(
        "CREATE INDEX "
        f"{_quote(name, 'sqlite')} ON {table_sql}"
        f"({', '.join(_quote(column, 'sqlite') for column in index_columns)})"
        for name, index_columns in index_definitions
    )
    statements.append("PRAGMA foreign_keys=ON")
    script = ";\n".join(statements) + ";"
    try:
        await connection.execute_script(script)
    except Exception:
        with suppress(Exception):
            await connection.execute_query("PRAGMA foreign_keys=ON")
        raise


async def ensure_group_plugin_scope_constraint() -> None:
    """Upgrade the legacy two-column uniqueness on existing databases."""
    connection = Tortoise.get_connection("default")
    raw_dialect = str(
        getattr(getattr(connection, "capabilities", None), "dialect", "") or ""
    ).lower()
    dialect = (
        "sqlite"
        if raw_dialect.startswith("sqlite")
        else "postgres"
        if raw_dialect.startswith("postgres")
        else "mysql"
        if raw_dialect.startswith("mysql")
        else "unknown"
    )
    if dialect == "unknown":
        return

    definitions = await _unique_definitions(connection, dialect)
    scoped = [columns for _, columns in definitions if columns == _SCOPED_COLUMNS]
    legacy = [name for name, columns in definitions if columns == _LEGACY_SCOPE_COLUMNS]
    if scoped and not legacy:
        return
    if dialect == "sqlite":
        if legacy:
            await _rebuild_sqlite_scope_constraint(connection)
        return

    table = _quote(_GROUP_PLUGIN_TABLE, dialect)
    for name in legacy:
        if dialect == "postgres":
            await connection.execute_query(
                f"ALTER TABLE {table} DROP CONSTRAINT IF EXISTS {_quote(name, dialect)}"
            )
        else:
            await connection.execute_query(
                f"ALTER TABLE {table} DROP INDEX {_quote(name, dialect)}"
            )
    if not scoped:
        columns_sql = ", ".join(_quote(column, dialect) for column in _SCOPED_COLUMNS)
        if dialect == "postgres":
            await connection.execute_query(
                f"ALTER TABLE {table} ADD CONSTRAINT "
                f"{_quote(_SCOPED_CONSTRAINT, dialect)} "
                f"UNIQUE ({columns_sql})"
            )
        else:
            await connection.execute_query(
                f"ALTER TABLE {table} ADD UNIQUE INDEX "
                f"{_quote(_SCOPED_CONSTRAINT, dialect)} ({columns_sql})"
            )
