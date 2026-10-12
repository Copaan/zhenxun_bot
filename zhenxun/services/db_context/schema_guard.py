from __future__ import annotations

import asyncio
import contextlib
from dataclasses import dataclass, field
import hashlib
import json
import os
from pathlib import Path
import re
from typing import Any
from urllib.parse import urlparse

from nonebot.utils import is_coroutine_callable
from tortoise import Tortoise
from tortoise.exceptions import OperationalError

from zhenxun.configs.config import BotConfig
from zhenxun.configs.database import is_sqlite_memory_url, sqlite_path_from_url
from zhenxun.services.log import logger

from .config import DB_SCHEMA_GUARD_TIMEOUT_SECONDS, LOG_COMMAND, db_model
from .schema_ops import (
    AddColumn,
    CreateIndex,
    SchemaOpRisk,
    normalize_schema_ops,
    quote_identifier,
)
from .utils import Dialect, db_dialect

_SCRIPT_HASH_DIR = Path() / "data" / ".db_script_hashes"
_TRUE_VALUES = {"1", "true", "yes", "on"}


@dataclass(slots=True)
class SchemaGuardResult:
    checked_tables: int = 0
    repaired_columns: int = 0
    repaired_indexes: int = 0
    skipped_columns: int = 0
    skipped_indexes: int = 0
    warnings: int = 0


@dataclass(slots=True)
class ColumnInfo:
    name: str
    data_type: str
    nullable: bool | None = None
    default: str | None = None


def _is_safe_missing_field(field: Any) -> bool:
    if getattr(field, "pk", False):
        return False
    if getattr(field, "generated", False):
        return False
    if bool(getattr(field, "null", False)):
        return True
    default = getattr(field, "default", None)
    return default is not None


def _field_sql_type(field: Any, dialect: Dialect) -> str | None:
    if dialect != "unknown" and hasattr(field, "get_for_dialect"):
        with contextlib.suppress(Exception):
            value = field.get_for_dialect(dialect, "SQL_TYPE")
            if value:
                return str(value)
    value = getattr(field, "SQL_TYPE", None)
    return str(value) if value else None


def _is_db_field(field: Any, dialect: Dialect) -> bool:
    if getattr(field, "virtual", False):
        return False
    return _field_sql_type(field, dialect) is not None


def _missing_column_sql(
    table: str,
    column: str,
    field: Any,
    dialect: Dialect,
) -> str | None:
    sql_type = _field_sql_type(field, dialect)
    if not sql_type:
        return None
    default = getattr(field, "default", None)
    if callable(default) or not isinstance(default, str | int | float):
        default = None
    sql = AddColumn(
        table, column, sql_type, bool(getattr(field, "null", False)), default
    ).to_sql(dialect)
    return sql[0] if sql else None


async def _table_columns(
    connection: Any, table: str, dialect: Dialect
) -> dict[str, ColumnInfo] | None:
    if dialect == "sqlite":
        rows = await connection.execute_query_dict(
            f"PRAGMA table_xinfo({quote_identifier(table, dialect)})"
        )
        if not rows:
            return None
        return {
            str(row.get("name")): ColumnInfo(
                name=str(row.get("name")),
                data_type=str(row.get("type") or ""),
                nullable=not bool(row.get("notnull")),
                default=str(row.get("dflt_value"))
                if row.get("dflt_value") is not None
                else None,
            )
            for row in rows
            if row.get("name")
        }
    if dialect == "postgres":
        rows = await connection.execute_query_dict(
            "SELECT column_name, data_type, is_nullable, column_default "
            "FROM information_schema.columns "
            "WHERE table_schema = current_schema() AND table_name = $1",
            [table],
        )
        if not rows:
            return None
        return {
            str(row.get("column_name")): ColumnInfo(
                name=str(row.get("column_name")),
                data_type=str(row.get("data_type") or ""),
                nullable=str(row.get("is_nullable") or "").upper() == "YES",
                default=str(row.get("column_default"))
                if row.get("column_default") is not None
                else None,
            )
            for row in rows
            if row.get("column_name")
        }
    if dialect == "mysql":
        rows = await connection.execute_query_dict(
            "SELECT COLUMN_NAME AS column_name, COLUMN_TYPE AS column_type, "
            "IS_NULLABLE AS is_nullable, COLUMN_DEFAULT AS column_default "
            "FROM INFORMATION_SCHEMA.COLUMNS "
            "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = %s",
            [table],
        )
        if not rows:
            return None
        return {
            str(row.get("column_name")): ColumnInfo(
                name=str(row.get("column_name")),
                data_type=str(row.get("column_type") or ""),
                nullable=str(row.get("is_nullable") or "").upper() == "YES",
                default=str(row.get("column_default"))
                if row.get("column_default") is not None
                else None,
            )
            for row in rows
            if row.get("column_name")
        }
    return None


async def _table_indexes(
    connection: Any, table: str, dialect: Dialect
) -> set[tuple[str, ...]]:
    indexes: set[tuple[str, ...]] = set()
    if dialect == "sqlite":
        rows = await connection.execute_query_dict(
            f"PRAGMA index_list({quote_identifier(table, dialect)})"
        )
        for row in rows:
            if bool(row.get("unique")):
                continue
            index_name = row.get("name")
            if not index_name:
                continue
            info = await connection.execute_query_dict(
                f"PRAGMA index_info({quote_identifier(str(index_name), dialect)})"
            )
            columns = tuple(
                str(item.get("name"))
                for item in sorted(info, key=lambda item: int(item.get("seqno") or 0))
                if item.get("name")
            )
            if columns:
                indexes.add(columns)
        return indexes
    if dialect == "postgres":
        rows = await connection.execute_query_dict(
            "SELECT indexname, indexdef FROM pg_indexes "
            "WHERE schemaname = current_schema() AND tablename = $1",
            [table],
        )
        for row in rows:
            indexdef = str(row.get("indexdef") or "")
            if " UNIQUE INDEX " in indexdef.upper():
                continue
            start = indexdef.rfind("(")
            end = indexdef.rfind(")")
            if start < 0 or end <= start:
                continue
            columns = tuple(
                part.strip().strip('"')
                for part in indexdef[start + 1 : end].split(",")
                if part.strip()
            )
            if columns:
                indexes.add(columns)
        return indexes
    if dialect == "mysql":
        rows = await connection.execute_query_dict(
            "SELECT INDEX_NAME, COLUMN_NAME, SEQ_IN_INDEX, NON_UNIQUE "
            "FROM INFORMATION_SCHEMA.STATISTICS "
            "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = %s",
            [table],
        )
        grouped: dict[str, list[tuple[int, str]]] = {}
        unique_names: set[str] = set()
        for row in rows:
            name = str(row.get("INDEX_NAME") or "")
            column = str(row.get("COLUMN_NAME") or "")
            if not name or not column:
                continue
            if int(row.get("NON_UNIQUE") or 0) == 0:
                unique_names.add(name)
                continue
            grouped.setdefault(name, []).append(
                (int(row.get("SEQ_IN_INDEX") or 0), column)
            )
        for name, items in grouped.items():
            if name in unique_names:
                continue
            columns = tuple(column for _, column in sorted(items))
            if columns:
                indexes.add(columns)
        return indexes
    return indexes


def _meta_index_columns(index: Any) -> tuple[str, ...]:
    if isinstance(index, str):
        return (index,)
    if isinstance(index, list | tuple):
        return tuple(str(column) for column in index if column)
    fields = getattr(index, "fields", None)
    if fields:
        return tuple(str(column) for column in fields if column)
    return ()


async def repair_safe_schema_drift() -> SchemaGuardResult:
    """补齐模型与表结构之间的低风险差异

    只补可空或带默认值的缺失字段和普通索引，不改类型、约束、唯一索引、外键，
    也不重建 SQLite 表。

    返回:
        SchemaGuardResult: 检查与修复统计
    """
    result = SchemaGuardResult()
    connection = Tortoise.get_connection("default")
    dialect = db_dialect(connection)
    if dialect == "unknown":
        logger.debug("SchemaGuard 跳过未知数据库方言", LOG_COMMAND)
        return result

    app = Tortoise.apps.get("models", {})
    for model in app.values():
        meta = getattr(model, "_meta", None)
        table = getattr(meta, "db_table", None) or getattr(meta, "table", None)
        if not table:
            continue
        try:
            columns = await _table_columns(connection, table, dialect)
        except Exception as exc:
            result.warnings += 1
            logger.debug(f"SchemaGuard 检查表 {table} 失败", LOG_COMMAND, e=exc)
            continue
        if columns is None:
            continue
        result.checked_tables += 1
        fields_map = getattr(meta, "fields_map", {}) or {}
        for field_name, model_field in fields_map.items():
            if not _is_db_field(model_field, dialect):
                continue
            source = str(getattr(model_field, "source_field", None) or field_name)
            if source in columns:
                continue
            sql = (
                _missing_column_sql(table, source, model_field, dialect)
                if _is_safe_missing_field(model_field)
                else None
            )
            if not sql:
                result.skipped_columns += 1
                logger.debug(
                    f"SchemaGuard 跳过非低风险缺字段: {table}.{source}",
                    LOG_COMMAND,
                )
                continue
            try:
                await asyncio.wait_for(
                    connection.execute_query_dict(sql),
                    timeout=DB_SCHEMA_GUARD_TIMEOUT_SECONDS,
                )
                result.repaired_columns += 1
                logger.info(f"SchemaGuard 已补齐字段: {table}.{source}", LOG_COMMAND)
            except Exception as exc:
                if not _is_already_exists(exc):
                    result.warnings += 1
                    logger.warning(
                        f"SchemaGuard 补齐字段失败: {table}.{source}",
                        LOG_COMMAND,
                        e=exc,
                    )
                    continue
            columns[source] = ColumnInfo(name=source, data_type="")
        try:
            existing_indexes = await _table_indexes(connection, table, dialect)
        except Exception as exc:
            result.warnings += 1
            logger.debug(f"SchemaGuard 检查索引 {table} 失败", LOG_COMMAND, e=exc)
            existing_indexes = set()
        for index in getattr(meta, "indexes", ()) or ():
            index_columns = _meta_index_columns(index)
            if not index_columns or index_columns in existing_indexes:
                continue
            sql = (
                CreateIndex(table, index_columns).to_sql(dialect)
                if all(column in columns for column in index_columns)
                else None
            )
            if not sql:
                result.skipped_indexes += 1
                continue
            try:
                await asyncio.wait_for(
                    connection.execute_query_dict(sql[0]),
                    timeout=DB_SCHEMA_GUARD_TIMEOUT_SECONDS,
                )
                result.repaired_indexes += 1
                logger.debug(
                    f"SchemaGuard 已补齐索引: {table}.{index_columns}",
                    LOG_COMMAND,
                )
            except Exception as exc:
                if not _is_already_exists(exc):
                    result.warnings += 1
                    result.skipped_indexes += 1
                    logger.warning(
                        f"SchemaGuard 补齐索引失败: {table}.{index_columns}",
                        LOG_COMMAND,
                        e=exc,
                    )
                    continue
            existing_indexes.add(index_columns)

    logger.info(
        "SchemaGuard 完成: "
        f"checked={result.checked_tables}, "
        f"repaired_columns={result.repaired_columns}, "
        f"repaired_indexes={result.repaired_indexes}, "
        f"skipped_columns={result.skipped_columns}, "
        f"skipped_indexes={result.skipped_indexes}, "
        f"warnings={result.warnings}",
        LOG_COMMAND,
    )
    return result


def _is_already_exists(exc: Exception) -> bool:
    if not isinstance(exc, OperationalError):
        return False
    err = str(exc).lower()
    return any(
        text in err
        for text in ("duplicate column", "duplicate", "already exists", "已存在")
    )


_ID = r'(?:"\w+"|`\w+`|\w+)'


def _script_name(value: str) -> str:
    return value.strip('"`')


async def script_action(connection: Any, sql: str, dialect: Dialect) -> str:
    """预检旧式迁移语句

    参数:
        connection: 数据库连接
        sql: 迁移语句
        dialect: 数据库方言

    返回:
        str: execute 需要执行，satisfied 已满足，missing_table 表尚未创建，
            unsupported 当前方言不支持
    """
    table_match = re.match(
        rf"\s*(?:ALTER\s+TABLE|UPDATE|INSERT\s+INTO|DELETE\s+FROM)\s+({_ID})(?=\s|;|$)",
        sql,
        re.I,
    )
    index = re.fullmatch(
        rf"\s*CREATE\s+(?:UNIQUE\s+)?INDEX\s+(?:IF\s+NOT\s+EXISTS\s+)?"
        rf"({_ID})\s+ON\s+({_ID})\s*\(.*",
        sql,
        re.I | re.S,
    )
    if not table_match and not index:
        return "execute"
    table = _script_name(table_match[1] if table_match else index[2])
    columns = await _table_columns(connection, table, dialect)
    if columns is None:
        return "missing_table"
    if index:
        name = _script_name(index[1])
        if dialect == "sqlite":
            rows = await connection.execute_query_dict(
                "SELECT name FROM sqlite_master "
                "WHERE type='index' AND tbl_name=? AND name=?",
                [table, name],
            )
        elif dialect == "postgres":
            rows = await connection.execute_query_dict(
                "SELECT indexname FROM pg_indexes WHERE schemaname=current_schema() "
                "AND tablename=$1 AND indexname=$2",
                [table, name],
            )
        else:
            rows = await connection.execute_query_dict(
                "SELECT INDEX_NAME FROM information_schema.statistics "
                "WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME=%s AND INDEX_NAME=%s",
                [table, name],
            )
        return "satisfied" if rows else "execute"

    if not sql.lstrip().upper().startswith("ALTER TABLE"):
        return "execute"
    tail = sql[table_match.end() :].strip().rstrip(";").strip()
    add = re.match(rf"ADD\s+(?:COLUMN\s+)?({_ID})\s+", tail, re.I)
    if add and _script_name(add[1]) in columns:
        return "satisfied"
    rename = re.fullmatch(rf"RENAME\s+COLUMN\s+({_ID})\s+TO\s+({_ID})", tail, re.I)
    if (
        rename
        and _script_name(rename[1]) not in columns
        and _script_name(rename[2]) in columns
    ):
        return "satisfied"
    drop = re.fullmatch(rf"DROP\s+COLUMN\s+(?:IF\s+EXISTS\s+)?({_ID})", tail, re.I)
    if drop and _script_name(drop[1]) not in columns:
        return "satisfied"
    if dialect == "sqlite" and re.match(r"ALTER\s+COLUMN\s", tail, re.I):
        return "unsupported"
    return "execute"


@dataclass(slots=True)
class ScriptMigrations:
    """模型 _run_script 声明的迁移语句及执行进度"""

    statements: list[tuple[str, str]]
    receipt: Path | None = None
    complete: bool = True
    done: set[int] = field(default_factory=set)
    failed: set[int] = field(default_factory=set)


def _receipt_file(fingerprint: str) -> Path:
    parsed = urlparse(BotConfig.db_url or "")
    dialect = parsed.scheme or "unknown"
    if dialect == "sqlite":
        db_identity = (
            ":memory:"
            if is_sqlite_memory_url(BotConfig.db_url)
            else str(sqlite_path_from_url(BotConfig.db_url))
        )
    else:
        db_identity = f"{parsed.hostname or ''}:{parsed.port or ''}{parsed.path}"
    db_hash = hashlib.md5(
        json.dumps(
            {"dialect": dialect, "db": db_identity, "script": fingerprint},
            ensure_ascii=False,
            sort_keys=True,
        ).encode()
    ).hexdigest()
    return _SCRIPT_HASH_DIR / f"{db_hash}.json"


async def collect_script_migrations() -> ScriptMigrations:
    """收集模型声明的迁移语句，当前数据库已完成过的同一批语句不再执行

    返回:
        ScriptMigrations: 待执行的迁移
    """
    dialect = db_dialect()
    allow_guarded = (
        os.getenv("DB_SCHEMA_RUN_GUARDED_OPS", "").strip().lower() in _TRUE_VALUES
    )
    statements: list[tuple[str, str]] = []
    complete = True
    for module, func in db_model.script_method:
        owner = f"{module}:{getattr(func, '__qualname__', 'script')}"
        try:
            items = await func() if is_coroutine_callable(func) else func()
            for item in items or ():
                if not isinstance(item, str) and (
                    item.risk == SchemaOpRisk.MANUAL
                    or (item.risk != SchemaOpRisk.SAFE and not allow_guarded)
                ):
                    logger.debug(f"{owner} 跳过 {item.risk.value} 迁移动作: {item}")
                    continue
                statements.extend(
                    (owner, sql) for sql in normalize_schema_ops([item], dialect)
                )
        except Exception as error:
            complete = False
            logger.error(f"迁移脚本生成失败: {owner}", LOG_COMMAND, e=error)
    if not statements:
        return ScriptMigrations([], complete=complete)
    fingerprint = hashlib.md5(
        json.dumps(sorted(sql for _, sql in statements), ensure_ascii=False).encode()
    ).hexdigest()
    receipt = _receipt_file(fingerprint)
    with contextlib.suppress(OSError, ValueError):
        saved = json.loads(receipt.read_text(encoding="utf-8"))
        if saved.get("status") in {None, "completed", "legacy_deferred"}:
            logger.debug("迁移脚本无变化，跳过执行")
            return ScriptMigrations([], complete=complete)
    return ScriptMigrations(statements, receipt, complete)


async def run_script_migrations(migrations: ScriptMigrations) -> None:
    """逐条执行尚未完成的迁移语句，表尚未创建的语句留到下次调用

    失败的语句只记录日志，本次启动不再重试

    参数:
        migrations: collect_script_migrations 收集的迁移
    """
    connection = Tortoise.get_connection("default")
    dialect = db_dialect(connection)
    for index, (owner, sql) in enumerate(migrations.statements):
        if index in migrations.done or index in migrations.failed:
            continue
        try:
            action = await asyncio.wait_for(
                script_action(connection, sql, dialect),
                DB_SCHEMA_GUARD_TIMEOUT_SECONDS,
            )
            if action == "missing_table":
                continue
            if action == "unsupported":
                logger.debug(f"SQLite 不支持，跳过迁移语句: {owner}: {sql}")
            elif action == "execute":
                logger.debug(f"执行迁移SQL: {owner}: {sql}")
                await asyncio.wait_for(
                    connection.execute_query_dict(sql),
                    DB_SCHEMA_GUARD_TIMEOUT_SECONDS,
                )
            migrations.done.add(index)
        except Exception as error:
            migrations.failed.add(index)
            logger.error(f"迁移语句执行失败: {owner}: {sql}", LOG_COMMAND, e=error)


def finish_script_migrations(migrations: ScriptMigrations) -> bool:
    """全部迁移语句完成时写入完成记录，否则列出未完成的来源，下次启动整批重试

    参数:
        migrations: 已执行过的迁移

    返回:
        bool: 是否全部完成
    """
    pending = sorted(
        {
            owner
            for index, (owner, _) in enumerate(migrations.statements)
            if index not in migrations.done
        }
    )
    if pending or not migrations.complete:
        logger.warning(
            f"数据库迁移未全部完成，下次启动重试: {', '.join(pending)}", LOG_COMMAND
        )
        return False
    if migrations.receipt is None:
        return True
    migrations.receipt.parent.mkdir(parents=True, exist_ok=True)
    temporary = migrations.receipt.with_suffix(f".tmp.{os.getpid()}")
    temporary.write_text(
        json.dumps(
            {
                "dialect": urlparse(BotConfig.db_url or "").scheme,
                "status": "completed",
                "statements": [sql for _, sql in migrations.statements],
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    os.replace(temporary, migrations.receipt)
    return True
