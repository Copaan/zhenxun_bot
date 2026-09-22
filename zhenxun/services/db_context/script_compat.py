"""Preflight legacy scripts without poisoning a database transaction."""

import re

from .schema_guard import _table_columns

_ID = r'(?:"\w+"|`\w+`|\w+)'


def _name(value: str) -> str:
    return value.strip('"`')


async def script_action(connection, sql: str, dialect: str) -> str:
    """Return execute, satisfied, missing_table, or legacy_deferred."""
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
    table = _name(table_match[1] if table_match else index[2])
    columns = await _table_columns(connection, table, dialect)
    if columns is None:
        if dialect not in {"sqlite", "postgres", "mysql"}:
            raise RuntimeError(f"unsupported migration dialect: {dialect}")
        return "missing_table"
    if index:
        name = _name(index[1])
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

    tail = sql[table_match.end() :].strip().rstrip(";").strip()
    if not sql.lstrip().upper().startswith("ALTER TABLE"):
        return "execute"
    add = re.match(rf"ADD\s+(?:COLUMN\s+)?({_ID})\s+", tail, re.I)
    if add and _name(add[1]) in columns:
        return "satisfied"
    rename = re.fullmatch(rf"RENAME\s+COLUMN\s+({_ID})\s+TO\s+({_ID})", tail, re.I)
    if rename and _name(rename[1]) not in columns and _name(rename[2]) in columns:
        return "satisfied"
    drop = re.fullmatch(rf"DROP\s+COLUMN\s+(?:IF\s+EXISTS\s+)?({_ID})", tail, re.I)
    if drop and _name(drop[1]) not in columns:
        return "satisfied"

    if dialect == "sqlite":
        declaration = re.fullmatch(
            rf"ALTER\s+COLUMN\s+({_ID})\s+TYPE\s+"
            r"(?:character\s+varying|varchar)\s*\(\s*\d+\s*\)",
            tail,
            re.I,
        )
        if declaration and _name(declaration[1]) in columns:
            return "legacy_deferred"
        # Old plugins emitted this PostgreSQL-only timestamp declaration even
        # for existing SQLite datetime fields. Keep the legacy deferral visible;
        # do not execute/rewrite USING expressions or claim data was converted.
        timestamp = re.fullmatch(
            rf"ALTER\s+COLUMN\s+({_ID})\s+TYPE\s+timestamp\s+with\s+time\s+zone"
            rf"(?:\s+USING\s+({_ID})::timestamp\s+with\s+time\s+zone)?",
            tail,
            re.I,
        )
        if timestamp:
            name = _name(timestamp[1])
            column = columns.get(name)
            if (
                column
                and column.data_type.lower() in {"datetime", "timestamp"}
                and (timestamp[2] is None or _name(timestamp[2]) == name)
            ):
                return "legacy_deferred"
    return "execute"
