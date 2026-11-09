"""逻辑导入的实际数据库身份、写入锁与外键边界"""

from __future__ import annotations

from contextlib import asynccontextmanager
import json
import os
from pathlib import Path

from tortoise.connection import connections


def quote(db, name: str) -> str:
    delimiter = "`" if db.capabilities.dialect == "mysql" else '"'
    return delimiter + name.replace(delimiter, delimiter * 2) + delimiter


def table_sql(db, model) -> str:
    meta = model._meta
    schema = getattr(meta, "schema", None)
    table = quote(db, meta.db_table)
    return f"{quote(db, schema)}.{table}" if schema else table


async def identity(db) -> dict:
    """从实际连接读取身份，不使用含凭据或进程随机密钥的配置指纹"""
    dialect = db.capabilities.dialect
    if dialect == "sqlite":
        rows = await db.execute_query_dict("PRAGMA database_list")
        filename = next(row["file"] for row in rows if row["name"] == "main")
        if not filename:
            return {"engine": dialect, "file": ":memory:"}
        path = Path(filename).resolve()
        stat = path.stat()
        return {
            "engine": dialect,
            "file": os.path.normcase(str(path)),
            "device": stat.st_dev,
            "inode": stat.st_ino,
        }
    if dialect == "postgres":
        rows = await db.execute_query_dict(
            "SELECT inet_server_addr()::text AS host, inet_server_port() AS port,"
            " current_database() AS database, current_schema() AS schema,"
            " (SELECT oid FROM pg_database WHERE"
            " datname=current_database()) AS"
            " database_id,"
            " (SELECT oid FROM pg_namespace"
            " WHERE nspname=current_schema()) AS schema_id"
        )
    else:
        rows = await db.execute_query_dict(
            "SELECT @@server_uuid AS server_id, DATABASE() AS db_name,"
            " @@lower_case_table_names AS name_case"
        )
    return {"engine": dialect, **rows[0]}


async def require_same(db, expected: dict | None) -> dict:
    actual = await identity(db)
    if expected and actual != expected:
        raise RuntimeError("migration_database_identity_changed")
    return actual


@asynccontextmanager
async def import_transaction(client, *, outcome: dict | None = None):
    """在检查目标之前开始有界写事务，不在已开启的事务中再次 BEGIN"""
    dialect = client.capabilities.dialect
    context = client._in_transaction()
    wrapper = context.connection
    original = wrapper.start
    sqlite_timeout = None
    if outcome is not None:
        outcome["state"] = "not_started"

    async def start():
        nonlocal sqlite_timeout
        if dialect == "sqlite":
            cursor = await wrapper._connection.execute("PRAGMA busy_timeout")
            sqlite_timeout = (await cursor.fetchone())[0]
            await cursor.close()
            await wrapper._connection.execute("PRAGMA busy_timeout=3000")
            await wrapper._connection.execute("BEGIN IMMEDIATE")
        else:
            if dialect == "mysql":
                await wrapper.execute_query(
                    "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"
                )
            await original()

    wrapper.start = start
    entered = False
    mysql_timeout = None
    try:
        try:
            db = await context.__aenter__()
            entered = True
            if outcome is not None:
                outcome["state"] = "in_transaction"
        except BaseException:
            if (
                hasattr(context, "token")
                and connections.get(context.connection_name) is wrapper
            ):
                try:
                    await wrapper.rollback()
                finally:
                    connections.reset(context.token)
                    if dialect == "sqlite" and context.lock.locked():
                        context.lock.release()
                    elif getattr(wrapper._parent, "_pool", None):
                        await wrapper._parent._pool.release(wrapper._connection)
            raise
        try:
            if dialect == "postgres":
                await db.execute_query("SET LOCAL lock_timeout = '3s'")
            if dialect == "mysql":
                mysql_timeout = (
                    await db.execute_query_dict(
                        "SELECT @@innodb_lock_wait_timeout AS value"
                    )
                )[0]["value"]
                await db.execute_query("SET SESSION innodb_lock_wait_timeout = 3")
            try:
                yield db
            finally:
                if mysql_timeout is not None:
                    await db.execute_query(
                        f"SET SESSION innodb_lock_wait_timeout = {int(mysql_timeout)}"
                    )
        except BaseException as error:
            if outcome is not None:
                outcome["state"] = "rollback_unknown"
            await context.__aexit__(type(error), error, error.__traceback__)
            if outcome is not None:
                outcome["state"] = "rolled_back"
            raise
        else:
            if outcome is not None:
                outcome["state"] = "commit_unknown"
            await context.__aexit__(None, None, None)
            if outcome is not None:
                outcome["state"] = "committed"
    finally:
        wrapper.start = original
        if entered and dialect == "sqlite" and not wrapper._finalized:
            await wrapper.rollback()
        if sqlite_timeout is not None:
            await wrapper._connection.execute(
                f"PRAGMA busy_timeout={int(sqlite_timeout)}"
            )


async def lock_tables(db, models: list, keys: list[dict] | None = None) -> None:
    """以固定顺序锁住受影响的模型表，MySQL 拒绝不可事务回滚的目标表"""
    dialect = db.capabilities.dialect
    schema = (await identity(db)).get("schema") if dialect == "postgres" else None
    imported = {model_key(model, schema) for model in models}
    relations = set(imported)
    for key in keys or []:
        if (
            relation_key(key, "parent") in imported
            or relation_key(key, "child") in imported
        ):
            relations.update((relation_key(key, "parent"), relation_key(key, "child")))
    for namespace, name in sorted(
        relations, key=lambda value: (value[0] or "", value[1])
    ):
        table = relation_sql(db, name, namespace)
        if dialect == "postgres":
            await db.execute_query(f"LOCK TABLE {table} IN SHARE ROW EXCLUSIVE MODE")
        elif dialect == "mysql":
            rows = await db.execute_query_dict(
                "SELECT ENGINE FROM information_schema.tables"
                " WHERE table_schema=DATABASE() AND table_name=%s",
                [name],
            )
            if not rows or str(rows[0]["ENGINE"]).lower() != "innodb":
                raise RuntimeError(f"migration_nontransactional_table:{name}")
            indexes = await db.execute_query_dict(
                "SELECT COLUMN_NAME FROM information_schema.statistics"
                " WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME=%s"
                " AND INDEX_NAME='PRIMARY' ORDER BY SEQ_IN_INDEX",
                [name],
            )
            order = ",".join(quote(db, row["COLUMN_NAME"]) for row in indexes)
            ordered = f" ORDER BY {order}" if order else ""
            await db.execute_query(f"SELECT * FROM {table}{ordered} FOR UPDATE")


def model_key(model, default_schema: str | None = None) -> tuple[str | None, str]:
    return getattr(model._meta, "schema", None) or default_schema, model._meta.db_table


def relation_key(key: dict, side: str) -> tuple[str | None, str]:
    return key.get(side + "_schema"), key[side]


async def table_names(db) -> set[tuple[str | None, str]]:
    """返回实际用户表，不局限于当前 ORM 已注册模型"""
    dialect = db.capabilities.dialect
    if dialect == "postgres":
        rows = await db.execute_query_dict(
            "SELECT schemaname, tablename FROM pg_tables"
            " WHERE schemaname NOT IN ('pg_catalog','information_schema')"
            " AND schemaname NOT LIKE 'pg_toast%'"
        )
        return {(row["schemaname"], row["tablename"]) for row in rows}
    query = (
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
        if dialect == "sqlite"
        else "SELECT TABLE_NAME AS name FROM information_schema.tables"
        " WHERE TABLE_SCHEMA=DATABASE() AND TABLE_TYPE='BASE TABLE'"
    )
    return {(None, row["name"]) for row in await db.execute_query_dict(query)}


async def foreign_keys(db) -> list[dict]:
    """读取当前数据库命名空间内的外键列，用于保护未参与恢复的表"""
    dialect = db.capabilities.dialect
    if dialect == "sqlite":
        result = []
        tables = await db.execute_query_dict(
            "SELECT name FROM sqlite_master WHERE type='table'"
            " AND name NOT LIKE 'sqlite_%'"
        )
        for table in tables:
            for key in await db.execute_query_dict(
                f"PRAGMA foreign_key_list({quote(db, table['name'])})"
            ):
                result.append(
                    {
                        "child": table["name"],
                        "column": key["from"],
                        "parent": key["table"],
                        "target": key["to"],
                        "group": f"{table['name']}:{key['id']}",
                        "position": key["seq"],
                    }
                )
        return result
    if dialect == "mysql":
        return await db.execute_query_dict(
            "SELECT TABLE_NAME AS child, COLUMN_NAME AS `column`,"
            " REFERENCED_TABLE_NAME AS parent, REFERENCED_COLUMN_NAME AS target,"
            " NULLIF(TABLE_SCHEMA,DATABASE()) AS child_schema,"
            " NULLIF(REFERENCED_TABLE_SCHEMA,DATABASE()) AS parent_schema,"
            " CONCAT(TABLE_SCHEMA,':',TABLE_NAME,':',CONSTRAINT_NAME) AS `group`,"
            " ORDINAL_POSITION AS position"
            " FROM information_schema.key_column_usage"
            " WHERE (TABLE_SCHEMA=DATABASE() OR"
            " REFERENCED_TABLE_SCHEMA=DATABASE())"
            " AND REFERENCED_TABLE_NAME IS NOT NULL"
        )
    return await db.execute_query_dict(
        'SELECT child.relname AS child, ca.attname AS "column",'
        " parent.relname AS parent,"
        " ns.nspname AS child_schema, pn.nspname AS parent_schema,"
        ' pa.attname AS target, c.oid::text AS "group", cols.position'
        " FROM pg_constraint c JOIN pg_class child ON child.oid=c.conrelid"
        " JOIN pg_class parent ON parent.oid=c.confrelid"
        " JOIN pg_namespace ns ON ns.oid=child.relnamespace"
        " JOIN pg_namespace pn ON pn.oid=parent.relnamespace"
        " CROSS JOIN LATERAL unnest(c.conkey,c.confkey) WITH ORDINALITY"
        " AS cols(child_id,parent_id,position)"
        " JOIN pg_attribute ca ON ca.attrelid=child.oid AND ca.attnum=cols.child_id"
        " JOIN pg_attribute pa ON pa.attrelid=parent.oid AND pa.attnum=cols.parent_id"
        " WHERE c.contype='f'"
    )


def relation_sql(db, name: str, schema: str | None = None) -> str:
    return f"{quote(db, schema)}.{quote(db, name)}" if schema else quote(db, name)


def groups(keys: list[dict]) -> list[list[dict]]:
    result = {}
    for key in keys:
        result.setdefault(key["group"], []).append(key)
    return [
        sorted(items, key=lambda item: item["position"]) for items in result.values()
    ]


async def invalid_relations(db, keys: list[dict], imported: set[tuple]) -> set[str]:
    """返回受影响关系的违约键，仅对本次新增的违约拒绝提交"""
    failures = set()
    for group in groups(keys):
        key = group[0]
        if (
            relation_key(key, "parent") not in imported
            and relation_key(key, "child") not in imported
        ):
            continue
        child = relation_sql(db, key["child"], key.get("child_schema"))
        parent = relation_sql(db, key["parent"], key.get("parent_schema"))
        if any(not item["target"] for item in group):
            if db.capabilities.dialect != "sqlite":
                raise RuntimeError("migration_foreign_key_target_unknown")
            info = await db.execute_query_dict(f"PRAGMA table_info({parent})")
            primary = sorted(
                (row for row in info if row["pk"]), key=lambda row: row["pk"]
            )
            if len(primary) != len(group):
                raise RuntimeError("migration_foreign_key_target_unknown")
            for item, column in zip(group, primary):
                item["target"] = column["name"]
        fields = ", ".join(f'c.{quote(db, item["column"])}' for item in group)
        present = " AND ".join(
            f'c.{quote(db, item["column"])} IS NOT NULL' for item in group
        )
        joined = " AND ".join(
            f'p.{quote(db, item["target"])}=c.{quote(db, item["column"])}'
            for item in group
        )
        rows = await db.execute_query_dict(
            f"SELECT DISTINCT {fields} FROM {child} c WHERE {present}"
            f" AND NOT EXISTS (SELECT 1 FROM {parent} p WHERE {joined})"
        )
        failures.update(
            json.dumps([key["group"], row], sort_keys=True, default=str) for row in rows
        )
    return failures
