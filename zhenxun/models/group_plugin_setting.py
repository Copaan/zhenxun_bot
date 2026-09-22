from __future__ import annotations

from contextlib import suppress
import hashlib
import json
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
    scope_key = fields.CharField(
        max_length=64,
        null=True,
        indexed=True,
        description="完整作用域摘要，用于跨数据库唯一约束",
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
        unique_together = ("scope_key",)
        indexes = (("group_id", "plugin_name"),)

    async def save(self, *args, **kwargs):
        self.scope_key = build_scope_key(
            self.group_id,
            self.plugin_name,
            self.bot_id,
            self.platform_scope,
            self.channel_id,
        )
        update_fields = kwargs.get("update_fields")
        if update_fields is not None and "scope_key" not in update_fields:
            kwargs["update_fields"] = [*update_fields, "scope_key"]
        await super().save(*args, **kwargs)

    @classmethod
    async def _run_script(cls):
        return [
            AddColumn("group_plugin_settings", "bot_id", "VARCHAR(255)"),
            AddColumn("group_plugin_settings", "platform_scope", "VARCHAR(64)"),
            AddColumn("group_plugin_settings", "channel_id", "VARCHAR(255)"),
            AddColumn("group_plugin_settings", "scope_key", "VARCHAR(64)"),
        ]


_GROUP_PLUGIN_TABLE = "group_plugin_settings"
_LEGACY_SCOPE_COLUMNS = ("group_id", "plugin_name")
_LEGACY_SCOPED_COLUMNS = (
    "bot_id",
    "platform_scope",
    "group_id",
    "channel_id",
    "plugin_name",
)
_SCOPE_KEY_COLUMNS = ("scope_key",)
_SCOPED_CONSTRAINT = "uid_group_plugi_scope_key"


def build_scope_key(
    group_id: object | None,
    plugin_name: object | None,
    bot_id: object | None = None,
    platform_scope: object | None = None,
    channel_id: object | None = None,
) -> str:
    """Build a bounded, collision-resistant key for the full setting scope."""
    values = [
        str(value or "")
        for value in (bot_id, platform_scope, group_id, channel_id, plugin_name)
    ]
    payload = json.dumps(["v1", *values], ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


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


async def _populate_scope_keys(connection: Any, dialect: str) -> None:
    table = _quote(_GROUP_PLUGIN_TABLE, dialect)
    try:
        rows = await connection.execute_query_dict(
            f"SELECT id, group_id, plugin_name, bot_id, platform_scope, "
            f"channel_id, scope_key FROM {table}"
        )
    except Exception as error:
        # The table is created by generate_schemas before this helper runs.
        # Keep a clear error if a partially initialized database reaches here.
        error_text = str(error).lower()
        if "does not exist" in error_text or "no such table" in error_text:
            return
        raise

    placeholder = "?" if dialect == "sqlite" else "%s" if dialect == "mysql" else "$1"
    id_placeholder = (
        "?" if dialect == "sqlite" else "%s" if dialect == "mysql" else "$2"
    )
    for row in rows:
        scope_key = build_scope_key(
            row.get("group_id"),
            row.get("plugin_name"),
            row.get("bot_id"),
            row.get("platform_scope"),
            row.get("channel_id"),
        )
        if row.get("scope_key") == scope_key:
            continue
        await connection.execute_query(
            f"UPDATE {table} SET {_quote('scope_key', dialect)} = "
            f"{placeholder} WHERE {_quote('id', dialect)} = {id_placeholder}",
            [scope_key, row["id"]],
        )

    duplicates = await connection.execute_query_dict(
        f"SELECT {_quote('scope_key', dialect)} AS scope_key, COUNT(*) AS count "
        f"FROM {table} WHERE {_quote('scope_key', dialect)} IS NOT NULL "
        f"GROUP BY {_quote('scope_key', dialect)} HAVING COUNT(*) > 1"
    )
    if duplicates:
        keys = ", ".join(str(row.get("scope_key")) for row in duplicates[:5])
        raise RuntimeError(f"group_plugin_settings_scope_duplicate:{keys}")


async def _rebuild_sqlite_scope_constraint(connection: Any) -> None:
    """Rebuild SQLite's table because its inline UNIQUE autoindex is not droppable."""
    columns = await connection.execute_query_dict(
        f"PRAGMA table_info({_quote(_GROUP_PLUGIN_TABLE, 'sqlite')})"
    )
    if not columns:
        return
    column_names = [str(row["name"]) for row in columns if row.get("name")]
    if not set(_SCOPE_KEY_COLUMNS).issubset(column_names):
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
        f"PRAGMA index_list({_quote(_GROUP_PLUGIN_TABLE, 'sqlite')})"
    )
    index_definitions: list[tuple[str, tuple[str, ...], bool]] = []
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
        if index_columns in {
            _LEGACY_SCOPE_COLUMNS,
            _LEGACY_SCOPED_COLUMNS,
            _SCOPE_KEY_COLUMNS,
        }:
            continue
        if index_columns:
            index_definitions.append((name, index_columns, bool(index.get("unique"))))

    temporary_table = f"{_GROUP_PLUGIN_TABLE}__scope_{os.getpid()}"
    table_sql = _quote(_GROUP_PLUGIN_TABLE, "sqlite")
    temporary_sql = _quote(temporary_table, "sqlite")
    columns_sql = ", ".join(_quote(name, "sqlite") for name in column_names)
    copy_columns = ", ".join(_quote(name, "sqlite") for name in column_names)
    unique_sql = ", ".join(_quote(name, "sqlite") for name in _SCOPE_KEY_COLUMNS)
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
        "CREATE "
        f"{'UNIQUE ' if unique else ''}INDEX "
        f"{_quote(name, 'sqlite')} ON {table_sql}"
        f"({', '.join(_quote(column, 'sqlite') for column in index_columns)})"
        for name, index_columns, unique in index_definitions
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
    """Upgrade setting uniqueness without exceeding MySQL key length limits."""
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

    await _populate_scope_keys(connection, dialect)
    definitions = await _unique_definitions(connection, dialect)
    desired = any(columns == _SCOPE_KEY_COLUMNS for _, columns in definitions)
    obsolete = [
        name
        for name, columns in definitions
        if columns in {_LEGACY_SCOPE_COLUMNS, _LEGACY_SCOPED_COLUMNS}
    ]
    if dialect == "sqlite":
        if obsolete:
            await _rebuild_sqlite_scope_constraint(connection)
        elif not desired:
            table = _quote(_GROUP_PLUGIN_TABLE, dialect)
            index = _quote(_SCOPED_CONSTRAINT, dialect)
            columns_sql = ", ".join(
                _quote(column, dialect) for column in _SCOPE_KEY_COLUMNS
            )
            await connection.execute_query(
                f"CREATE UNIQUE INDEX {index} ON {table} ({columns_sql})"
            )
        return

    table = _quote(_GROUP_PLUGIN_TABLE, dialect)
    for name in obsolete:
        if dialect == "postgres":
            await connection.execute_query(
                f"ALTER TABLE {table} DROP CONSTRAINT IF EXISTS {_quote(name, dialect)}"
            )
        else:
            await connection.execute_query(
                f"ALTER TABLE {table} DROP INDEX {_quote(name, dialect)}"
            )
    if not desired:
        columns_sql = ", ".join(
            _quote(column, dialect) for column in _SCOPE_KEY_COLUMNS
        )
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
