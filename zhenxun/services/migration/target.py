"""迁移目标的结构归属、写入证据与外部数据库补偿。"""

from __future__ import annotations

from contextlib import asynccontextmanager
from copy import deepcopy
from hashlib import sha256
import json
from pathlib import Path
import re

from . import data_guard as guard
from . import policy


def key(schema, name) -> str:
    return json.dumps([schema, name], ensure_ascii=False)


def relation(db, item) -> str:
    return guard.relation_sql(db, item["name"], item.get("schema"))


def digest(value) -> str:
    return sha256(json.dumps(value, sort_keys=True, default=str).encode()).hexdigest()


async def describe(db, schema, name) -> dict:
    """读取表身份及结构，不把自增计数或序列当前值作为结构摘要。"""
    table = guard.relation_sql(db, name, schema)
    if db.capabilities.dialect == "postgres":
        identity = (
            await db.execute_query_dict(
                "SELECT oid, COALESCE(obj_description(oid,'pg_class'),'') AS comment"
                " FROM pg_class WHERE oid=$1::regclass",
                [table],
            )
        )[0]
        columns = await db.execute_query_dict(
            "SELECT attname,format_type(atttypid,atttypmod) AS type,attnotnull,"
            "attidentity,attgenerated,pg_get_expr(d.adbin,d.adrelid) AS expression"
            " FROM pg_attribute a LEFT JOIN pg_attrdef d"
            " ON d.adrelid=a.attrelid AND d.adnum=a.attnum"
            " WHERE a.attrelid=$1::regclass AND a.attnum>0 AND NOT a.attisdropped"
            " ORDER BY a.attnum",
            [table],
        )
        constraints = await db.execute_query_dict(
            "SELECT conname,pg_get_constraintdef(oid) AS definition"
            " FROM pg_constraint WHERE conrelid=$1::regclass ORDER BY conname",
            [table],
        )
        indexes = await db.execute_query_dict(
            "SELECT pg_get_indexdef(indexrelid) AS definition FROM pg_index"
            " WHERE indrelid=$1::regclass ORDER BY indexrelid",
            [table],
        )
        triggers = await db.execute_query_dict(
            "SELECT tgname,pg_get_triggerdef(oid) AS definition FROM pg_trigger"
            " WHERE tgrelid=$1::regclass AND NOT tgisinternal ORDER BY tgname",
            [table],
        )
        definition = [columns, constraints, indexes, triggers, identity["comment"]]
    else:
        rows = await db.execute_query_dict(f"SHOW CREATE TABLE {table}")
        definition = re.sub(r"\bAUTO_INCREMENT=\d+\s*", "", rows[0]["Create Table"])
        identity = (
            await db.execute_query_dict(
                "SELECT TABLE_COMMENT AS comment, ENGINE AS engine"
                " FROM information_schema.tables WHERE TABLE_SCHEMA=DATABASE()"
                " AND TABLE_NAME=%s",
                [name],
            )
        )[0]
        if str(identity["engine"]).lower() != "innodb":
            raise RuntimeError(f"migration_nontransactional_table:{name}")
    return {"name": name, "schema": schema, **identity, "structure": digest(definition)}


async def content(db, item) -> str:
    """按行摘要核对本任务表内容，保留重复行并避免依赖数据库返回顺序。"""
    hashes = []
    sql = f"SELECT * FROM {relation(db, item)}"
    async with db.acquire_connection() as connection:
        dialect = db.capabilities.dialect
        if dialect == "postgres":
            async with connection.transaction():
                async for row in connection.cursor(sql, prefetch=1000):
                    hashes.append(digest(dict(row)))
        elif dialect == "mysql":
            import aiomysql

            async with connection.cursor(aiomysql.SSDictCursor) as cursor:
                await cursor.execute(sql)
                while rows := await cursor.fetchmany(1000):
                    hashes.extend(digest(row) for row in rows)
        else:
            async with connection.execute(sql) as cursor:
                while rows := await cursor.fetchmany(1000):
                    hashes.extend(digest(dict(row)) for row in rows)
    return digest(sorted(hashes))


def _transaction_wrapper(client):
    """取得可绑定到指定物理连接的事务连接包装

    参数:
        client: 数据库客户端

    返回:
        事务连接包装
    """
    context = client._in_transaction()
    return getattr(context, "client", None) or context.connection


async def database_objects(db) -> dict:
    """列出整库覆盖涉及的用户对象，不将数据库容器和授权列入删除。"""
    dialect = db.capabilities.dialect
    if dialect == "sqlite":
        rows = await db.execute_query_dict(
            "SELECT type,name,tbl_name,sql FROM sqlite_master"
            " WHERE name NOT LIKE 'sqlite_%' ORDER BY type,name"
        )
        return {"engine": dialect, "objects": rows}
    if dialect == "mysql":
        if any(
            item.get("child_schema") or item.get("parent_schema")
            for item in await guard.foreign_keys(db)
        ):
            raise RuntimeError("migration_database_external_reference")
        tables = await db.execute_query_dict(
            "SELECT TABLE_NAME AS name,TABLE_TYPE AS type,ENGINE AS engine"
            " FROM information_schema.tables"
            " WHERE TABLE_SCHEMA=DATABASE() ORDER BY TABLE_NAME"
        )
        routines = await db.execute_query_dict(
            "SELECT ROUTINE_NAME AS name,ROUTINE_TYPE AS type"
            " FROM information_schema.routines"
            " WHERE ROUTINE_SCHEMA=DATABASE() ORDER BY ROUTINE_NAME"
        )
        events = await db.execute_query_dict(
            "SELECT EVENT_NAME AS name FROM information_schema.events"
            " WHERE EVENT_SCHEMA=DATABASE() ORDER BY EVENT_NAME"
        )
        triggers = await db.execute_query_dict(
            "SELECT TRIGGER_NAME AS name FROM information_schema.triggers"
            " WHERE TRIGGER_SCHEMA=DATABASE() ORDER BY TRIGGER_NAME"
        )
        return {
            "engine": dialect,
            "objects": tables,
            "routines": routines,
            "events": events,
            "triggers": triggers,
        }
    schemas = await db.execute_query_dict(
        "SELECT nspname AS name FROM pg_namespace WHERE nspname<>'information_schema'"
        " AND nspname NOT LIKE 'pg_%' ORDER BY nspname"
    )
    objects = await db.execute_query_dict(
        "SELECT n.nspname AS schema,c.relname AS name,c.relkind AS type"
        " FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace"
        " WHERE n.nspname<>'information_schema' AND n.nspname NOT LIKE 'pg_%'"
        " ORDER BY n.nspname,c.relname"
    )
    routines = await db.execute_query_dict(
        "SELECT n.nspname AS schema,p.proname AS name,"
        "pg_get_function_identity_arguments(p.oid) AS arguments"
        " FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace"
        " WHERE n.nspname<>'information_schema' AND n.nspname NOT LIKE 'pg_%'"
        " ORDER BY n.nspname,p.proname,arguments"
    )
    unsupported = await db.execute_query_dict(
        "SELECT 'event_trigger' AS type,evtname AS name FROM pg_event_trigger"
        " UNION ALL SELECT 'publication',pubname FROM pg_publication"
        " UNION ALL SELECT 'subscription',subname FROM pg_subscription"
        " UNION ALL SELECT 'foreign_server',srvname FROM pg_foreign_server"
        " UNION ALL SELECT 'type',t.typname FROM pg_type t"
        " JOIN pg_namespace n ON n.oid=t.typnamespace"
        " WHERE n.nspname<>'information_schema' AND n.nspname NOT LIKE 'pg_%'"
        " AND t.typtype<>'c' AND t.typelem=0"
        " UNION ALL SELECT 'operator',o.oprname FROM pg_operator o"
        " JOIN pg_namespace n ON n.oid=o.oprnamespace"
        " WHERE n.nspname<>'information_schema' AND n.nspname NOT LIKE 'pg_%'"
        " UNION ALL SELECT 'cast',oid::text FROM pg_cast WHERE oid>=16384"
        " UNION ALL SELECT 'large_object',oid::text FROM pg_largeobject_metadata"
    )
    if unsupported:
        raise RuntimeError(
            "migration_database_object_unsupported:" + unsupported[0]["type"]
        )
    extensions = await db.execute_query_dict(
        "SELECT extname AS name,extversion AS version"
        " FROM pg_extension ORDER BY extname"
    )
    if any(item["name"] != "plpgsql" for item in extensions):
        raise RuntimeError("migration_database_extension_requires_review")
    return {
        "engine": dialect,
        "schemas": schemas,
        "objects": objects,
        "routines": routines,
        "extensions": extensions,
    }


async def database_content(db) -> dict:
    """核对原生回滚点覆盖的全部用户表，不依赖插件模型是否注册。"""
    if db.capabilities.dialect == "sqlite":
        from . import data

        return {"sqlite": await data.sqlite_revision(db)}
    return {
        key(schema, name): await content(db, {"schema": schema, "name": name})
        for schema, name in sorted(await guard.table_names(db), key=str)
    }


def _encode_cell(value):
    import base64
    from datetime import date, datetime, time, timedelta
    from decimal import Decimal
    from uuid import UUID

    if value is None or isinstance(value, bool | int | float | str | dict | list):
        return ["value", value]
    if isinstance(value, bytes | bytearray | memoryview):
        return ["bytes", base64.b64encode(value).decode()]
    if isinstance(value, datetime | date | time):
        return [type(value).__name__, value.isoformat()]
    if isinstance(value, timedelta):
        return ["timedelta", [value.days, value.seconds, value.microseconds]]
    if isinstance(value, Decimal | UUID):
        return [type(value).__name__, str(value)]
    raise RuntimeError("migration_snapshot_value_unsupported:" + type(value).__name__)


def _decode_cell(cell):
    import base64
    from datetime import date, datetime, time, timedelta
    from decimal import Decimal
    from uuid import UUID

    kind, value = cell
    if kind == "value":
        return value
    if kind == "bytes":
        return base64.b64decode(value, validate=True)
    if kind in {"datetime", "date", "time"}:
        return {"datetime": datetime, "date": date, "time": time}[kind].fromisoformat(
            value
        )
    if kind == "timedelta":
        return timedelta(days=value[0], seconds=value[1], microseconds=value[2])
    if kind in {"Decimal", "UUID"}:
        return {"Decimal": Decimal, "UUID": UUID}[kind](value)
    raise RuntimeError("migration_snapshot_value_invalid")


async def snapshot_tables(client, directory: Path, tables: set[str]) -> dict:
    """保存受影响表和来源映射的驱动值，launcher 回滚不依赖插件模型。"""
    from . import data, native_backup

    models = {data._table_key(model): model for model in data._models()}
    selected = set(tables)
    selected.update(name for name in models if name == "migration_import_records")
    directory.mkdir(parents=True, exist_ok=True)
    manifest = {}
    async with guard.import_transaction(client) as db:
        identity = await guard.identity(db)
        present = await guard.table_names(db)
        existing = [
            models[name]
            for name in selected
            if guard.model_key(models[name], identity.get("schema")) in present
        ]
        await guard.lock_tables(db, existing, await guard.foreign_keys(db))
        for name in (name for name in models if name in selected):
            model = models[name]
            schema, table = guard.model_key(model, identity.get("schema"))
            columns = list(model._meta.fields_db_projection.values())
            primary = model._meta.db_pk_column
            path = directory / (name + ".jsonl")
            count, last = 0, None
            relation_name = guard.relation_sql(db, table, schema)
            projection = ",".join(guard.quote(db, column) for column in columns)
            with path.open("w", encoding="utf-8", newline="\n") as stream:
                while model in existing:
                    where = (
                        ""
                        if last is None
                        else " WHERE "
                        + guard.quote(db, primary)
                        + ">"
                        + data._parameters(db, 1)[0]
                    )
                    rows = await db.execute_query_dict(
                        f"SELECT {projection} FROM {relation_name}"
                        + where
                        + " ORDER BY "
                        + guard.quote(db, primary)
                        + " LIMIT 1000",
                        None if last is None else [last],
                    )
                    for row in rows:
                        stream.write(
                            json.dumps(
                                [_encode_cell(row[column]) for column in columns],
                                ensure_ascii=False,
                            )
                            + "\n"
                        )
                    count += len(rows)
                    if len(rows) < 1000:
                        break
                    last = rows[-1][primary]
            manifest[name] = {
                "schema": schema,
                "name": table,
                "columns": columns,
                "primary": primary,
                "rows": count,
                "sha256": native_backup.file_digest(path),
                "unique": [
                    [model._meta.fields_db_projection[field]]
                    for field, definition in model._meta.fields_map.items()
                    if field in model._meta.fields_db_projection
                    and definition.unique
                    and field != model._meta.pk_attr
                ]
                + [
                    [
                        model._meta.fields_map[field].source_field
                        if field in model._meta.fk_fields | model._meta.o2o_fields
                        else model._meta.fields_db_projection[field]
                        for field in constraint
                    ]
                    for constraint in model._meta.unique_together or ()
                ],
            }
    return {"identity": identity, "directory": str(directory), "tables": manifest}


async def _advance_sequence(db, table: str, column: str) -> None:
    """PostgreSQL 显式写回主键后，把自增序列推进到表内最大值之后

    参数:
        db: 事务连接
        table: 已转义的表名
        column: 主键列名
    """
    sequence = (
        await db.execute_query_dict(
            "SELECT pg_get_serial_sequence($1, $2) AS name", [table, column]
        )
    )[0]["name"]
    if not sequence:
        return
    state = (
        await db.execute_query_dict(f"SELECT last_value, is_called FROM {sequence}")
    )[0]
    maximum = (
        await db.execute_query_dict(
            f"SELECT MAX({guard.quote(db, column)}) AS value FROM {table}"
        )
    )[0]["value"]
    following = int(state["last_value"]) + (1 if state["is_called"] else 0)
    if maximum is not None and following <= maximum:
        await db.execute_query(
            "SELECT setval($1::regclass, $2, true)", [sequence, maximum]
        )


async def restore_tables(
    client, receipt: dict, *, expected: dict | None = None
) -> None:
    """同事务恢复受影响表和来源映射，不重新执行普通导入来源处理。"""
    from . import data, native_backup

    directory = Path(receipt["directory"])
    for name, item in receipt["tables"].items():
        if native_backup.file_digest(directory / (name + ".jsonl")) != item["sha256"]:
            raise RuntimeError("migration_target_snapshot_changed")
    async with guard.import_transaction(client) as db:
        await guard.require_same(db, receipt["identity"])
        selected = {
            (item["schema"], item["name"]) for item in receipt["tables"].values()
        }
        keys = await guard.foreign_keys(db)
        locked = set(selected) | {
            (item["schema"], item["name"]) for item in (expected or {}).values()
        }
        for foreign in keys:
            if (
                guard.relation_key(foreign, "parent") in selected
                or guard.relation_key(foreign, "child") in selected
            ):
                locked.update(
                    (
                        guard.relation_key(foreign, "parent"),
                        guard.relation_key(foreign, "child"),
                    )
                )
        for schema, name in sorted(locked, key=str):
            table = guard.relation_sql(db, name, schema)
            if db.capabilities.dialect == "postgres":
                await db.execute_query(
                    f"LOCK TABLE {table} IN SHARE ROW EXCLUSIVE MODE"
                )
            elif db.capabilities.dialect == "mysql":
                await db.execute_query(f"SELECT 1 FROM {table} FOR UPDATE")
        for item in (expected or {}).values():
            await _check(db, item)
        if db.capabilities.dialect == "sqlite":
            await db.execute_query("PRAGMA defer_foreign_keys=ON")
        checks = None
        if db.capabilities.dialect == "mysql":
            checks = (
                await db.execute_query_dict("SELECT @@FOREIGN_KEY_CHECKS AS value")
            )[0]["value"]
        excess, desired = {}, {}
        for name, item in receipt["tables"].items():
            table = guard.relation_sql(db, item["name"], item["schema"])
            columns, pk = item["columns"], item["primary"]
            pk_index = columns.index(pk)
            needed = {pk} | {
                column for constraint in item.get("unique", []) for column in constraint
            }
            positions = {column: columns.index(column) for column in needed}
            desired[name] = {}
            with (directory / (name + ".jsonl")).open(encoding="utf-8") as stream:
                for line in stream:
                    row = [_decode_cell(value) for value in json.loads(line)]
                    if row[pk_index] in desired[name]:
                        raise RuntimeError("migration_target_snapshot_changed")
                    desired[name][row[pk_index]] = {
                        column: row[index] for column, index in positions.items()
                    }
            if len(desired[name]) != item["rows"]:
                raise RuntimeError("migration_target_snapshot_changed")
            current = await db.execute_query_dict(
                "SELECT "
                + ",".join(guard.quote(db, col) for col in sorted(needed))
                + f" FROM {table}"
            )
            excess[name] = {row[pk] for row in current} - desired[name].keys()
            for constraint in item.get("unique", []):
                occupied = {
                    tuple(row[column] for column in constraint): row[pk]
                    for row in current
                    if all(row[column] is not None for column in constraint)
                }
                for primary, row in desired[name].items():
                    value = tuple(row[column] for column in constraint)
                    owner = (
                        occupied.get(value)
                        if all(v is not None for v in value)
                        else None
                    )
                    if (
                        owner is not None
                        and owner != primary
                        and owner in desired[name]
                    ):
                        raise RuntimeError(
                            "migration_rollback_unique_permutation_requires_review:"
                            + name
                        )
        for group in guard.groups(keys):
            foreign = group[0]
            parent = guard.relation_key(foreign, "parent")
            if (
                parent not in selected
                or guard.relation_key(foreign, "child") in selected
            ):
                continue
            name = next(
                name
                for name, item in receipt["tables"].items()
                if (item["schema"], item["name"]) == parent
            )
            extras = list(excess[name])
            child = guard.relation_sql(
                db, foreign["child"], foreign.get("child_schema")
            )
            relation_name = guard.relation_sql(db, parent[1], parent[0])
            joined = " AND ".join(
                "c."
                + guard.quote(db, item["column"])
                + "=p."
                + guard.quote(db, item["target"])
                for item in group
            )
            for offset in range(0, len(extras), 1000):
                batch = extras[offset : offset + 1000]
                found = await db.execute_query(
                    f"SELECT 1 FROM {child} c JOIN {relation_name} p ON {joined}"
                    + " WHERE p."
                    + guard.quote(db, receipt["tables"][name]["primary"])
                    + " IN ("
                    + ",".join(data._parameters(db, len(batch)))
                    + ") LIMIT 1",
                    batch,
                )
                if found[1]:
                    raise RuntimeError(
                        "migration_rollback_external_reference:" + foreign["child"]
                    )
        try:
            if checks is not None:
                await db.execute_query("SET FOREIGN_KEY_CHECKS=0")
            for name, item in reversed(list(receipt["tables"].items())):
                table = guard.relation_sql(db, item["name"], item["schema"])
                extras = list(excess[name])
                for offset in range(0, len(extras), 1000):
                    batch = extras[offset : offset + 1000]
                    await db.execute_query(
                        f"DELETE FROM {table} WHERE "
                        + guard.quote(db, item["primary"])
                        + " IN ("
                        + ",".join(data._parameters(db, len(batch)))
                        + ")",
                        batch,
                    )
            for name, item in receipt["tables"].items():
                table = guard.relation_sql(db, item["name"], item["schema"])
                pk = guard.quote(db, item["primary"])
                rows = await db.execute_query_dict(f"SELECT {pk} FROM {table}")
                existing = {row[item["primary"]] for row in rows}
                seen = set()
                columns = item["columns"]
                pk_index = columns.index(item["primary"])
                others = [i for i in range(len(columns)) if i != pk_index]
                parameters = data._parameters(db, len(others) + 1)
                update = (
                    f"UPDATE {table} SET "
                    + ",".join(
                        guard.quote(db, columns[i]) + "=" + parameters[offset]
                        for offset, i in enumerate(others)
                    )
                    + f" WHERE {pk}="
                    + parameters[-1]
                )
                insert = (
                    f"INSERT INTO {table} ("
                    + ",".join(guard.quote(db, col) for col in columns)
                    + ") VALUES ("
                    + ",".join(data._parameters(db, len(columns)))
                    + ")"
                )
                with (directory / (name + ".jsonl")).open(encoding="utf-8") as stream:
                    for line in stream:
                        row = [_decode_cell(value) for value in json.loads(line)]
                        primary = row[pk_index]
                        if primary in seen:
                            raise RuntimeError("migration_target_snapshot_changed")
                        seen.add(primary)
                        if primary in existing:
                            if others:
                                await db.execute_query(
                                    update, [row[i] for i in others] + [primary]
                                )
                        else:
                            await db.execute_query(insert, row)
                if len(seen) != item["rows"]:
                    raise RuntimeError("migration_target_snapshot_changed")
                excess[name] = existing - seen
            for item in receipt["tables"].values():
                table = guard.relation_sql(db, item["name"], item["schema"])
                count = (
                    await db.execute_query_dict(
                        f"SELECT COUNT(*) AS value FROM {table}"
                    )
                )[0]["value"]
                if count != item["rows"]:
                    raise RuntimeError("migration_target_rollback_row_count_mismatch")
                if db.capabilities.dialect == "postgres":
                    await _advance_sequence(db, table, item["primary"])
            if await guard.invalid_relations(db, keys, selected):
                raise RuntimeError("migration_rollback_foreign_key_conflict")
        finally:
            if checks is not None:
                await db.execute_query(f"SET FOREIGN_KEY_CHECKS={int(checks)}")


async def clear_database(
    client, *, expected: dict | None = None, recreate=True
) -> None:
    """核对用户对象清单后清空目标，保留数据库及数据库级权限。"""
    actual = await database_objects(client)
    if expected is not None and actual != expected:
        raise RuntimeError("migration_target_structure_changed")
    dialect = client.capabilities.dialect
    if dialect == "sqlite":
        async with guard.import_transaction(client) as db:
            await db.execute_query("PRAGMA defer_foreign_keys=ON")
            for kind in ("view", "trigger", "table"):
                for item in actual["objects"]:
                    if item["type"] == kind:
                        await db.execute_query(
                            f"DROP {kind.upper()} IF EXISTS "
                            + guard.quote(db, item["name"])
                        )
        return
    if dialect == "postgres":
        identity = await guard.identity(client)
        async with guard.import_transaction(client) as db:
            for schema in actual["schemas"]:
                await db.execute_query(
                    f"DROP SCHEMA {guard.quote(db, schema['name'])} CASCADE"
                )
            if recreate:
                schema = identity.get("schema") or "public"
                await db.execute_query(f"CREATE SCHEMA {guard.quote(db, schema)}")
        return
    async with client.acquire_connection() as connection:
        db = _transaction_wrapper(client)
        db._connection = connection
        checks = (await db.execute_query_dict("SELECT @@FOREIGN_KEY_CHECKS AS value"))[
            0
        ]["value"]
        try:
            await db.execute_query("SET FOREIGN_KEY_CHECKS=0")
            for item in actual["events"]:
                await db.execute_query(f"DROP EVENT {guard.quote(db, item['name'])}")
            for item in actual["routines"]:
                if item["type"] not in {"PROCEDURE", "FUNCTION"}:
                    raise RuntimeError("migration_database_object_unsupported")
                await db.execute_query(
                    f"DROP {item['type']} {guard.quote(db, item['name'])}"
                )
            for kind in ("VIEW", "BASE TABLE"):
                for item in actual["objects"]:
                    if item["type"] == kind:
                        action = "VIEW" if kind == "VIEW" else "TABLE"
                        await db.execute_query(
                            f"DROP {action} {guard.quote(db, item['name'])}"
                        )
        finally:
            await db.execute_query(f"SET FOREIGN_KEY_CHECKS={int(checks)}")


def schema_plan(client, models: list, operation_id: str) -> list[dict]:
    """以表为单位生成 ORM 建表计划，包含自动生成的关联表。"""
    client = getattr(client, "_parent", client)
    dialect = client.capabilities.dialect
    marker = f"[zx-migration:{operation_id}]"
    comments = {}
    names = {model._meta.db_table for model in models}
    for model in models:
        comments[model._meta.db_table] = model._meta.table_description or ""
        for name in model._meta.m2m_fields:
            field = model._meta.fields_map[name]
            if not field._generated and field.through not in names:
                comments[field.through] = field.description or ""
    base = client.schema_generator

    class Generator(base):
        def _get_models_to_create(self, result):
            result.extend(models)

        def _table_generate_extra(self, table):
            extra = super()._table_generate_extra(table)
            if dialect == "mysql":
                comment = comments[table] + marker
                if len(comment.encode()) > 2048:
                    raise RuntimeError("migration_table_comment_too_long")
                extra += (
                    " ENGINE=InnoDB COMMENT='" + self._escape_comment(comment) + "'"
                )
            return extra

        def _table_comment_generator(self, table, comment):
            if dialect == "mysql":
                return ""
            return super()._table_comment_generator(table, comment)

    generator = Generator(client)
    plans, through = [], []
    for model in models:
        generated = generator._get_table_sql(model, safe=False)
        namespace = getattr(model._meta, "schema", None)
        plans.append(
            {
                "name": generated["table"],
                "schema": namespace,
                "sql": generated["table_creation_string"],
                "references": generated["references"],
                "comment": comments[generated["table"]],
                "marker": marker,
            }
        )
        fields = [
            model._meta.fields_map[name]
            for name in model._meta.m2m_fields
            if not model._meta.fields_map[name]._generated
            and model._meta.fields_map[name].through not in names
        ]
        for field, sql in zip(fields, generated["m2m_tables"], strict=True):
            through.append(
                {
                    "name": field.through,
                    "schema": namespace,
                    "sql": sql,
                    "comment": comments[field.through],
                    "marker": marker,
                    "references": [
                        model._meta.db_table,
                        field.related_model._meta.db_table,
                    ],
                }
            )
    ordered, done = [], set()
    while plans:
        selected = next(
            (p for p in plans if p["references"] <= done | {p["name"]}), None
        )
        if selected is None:
            raise RuntimeError("migration_schema_dependency_cycle")
        plans.remove(selected)
        done.add(selected["name"])
        ordered.append({**selected, "references": sorted(selected["references"])})
    return ordered + through


async def _mysql_grants(db) -> list[str]:
    """展开当前连接已启用角色的授权。"""
    role = next(
        iter((await db.execute_query_dict("SELECT CURRENT_ROLE()"))[0].values())
    )
    sql = "SHOW GRANTS FOR CURRENT_USER"
    if role != "NONE":
        if not re.fullmatch(
            r"\s*\x60(?:[^\x60]|\x60\x60)+\x60@\x60(?:[^\x60]|\x60\x60)+\x60(?:\s*,\s*\x60(?:[^\x60]|\x60\x60)+\x60@\x60(?:[^\x60]|\x60\x60)+\x60)*\s*",
            role,
        ):
            raise RuntimeError("migration_role_privileges_unconfirmed")
        sql += " USING " + role
    return [next(iter(row.values())) for row in await db.execute_query_dict(sql)]


async def database_permissions(db, *, backup: bool) -> None:
    """预检整库用户对象的删除、重建及原生恢复权限。"""
    dialect = db.capabilities.dialect
    if dialect == "sqlite":
        return
    if dialect == "mysql":
        database = (await guard.identity(db))["db_name"]
        allowed, global_allowed = set(), set()
        for grant in await _mysql_grants(db):
            match = re.match(r"GRANT (.+?) ON (.+?) TO ", grant)
            if not match:
                continue
            scope = match[2].replace(chr(96), "").split(".", 1)
            if scope == ["*", "*"]:
                global_allowed.update(re.split(r",\s*", match[1]))
            if scope in (["*", "*"], [database, "*"]):
                allowed.update(re.split(r",\s*", match[1]))
        required = {
            "SELECT",
            "INSERT",
            "UPDATE",
            "DELETE",
            "CREATE",
            "DROP",
            "ALTER",
            "INDEX",
            "REFERENCES",
            "SHOW VIEW",
            "CREATE VIEW",
            "TRIGGER",
            "EVENT",
            "CREATE ROUTINE",
            "ALTER ROUTINE",
            "EXECUTE",
        }
        if "ALL PRIVILEGES" not in allowed and required - allowed:
            raise RuntimeError(
                "migration_database_privilege_required:"
                + ",".join(sorted(required - allowed))
            )
        if backup and not {"ALL PRIVILEGES", "SHOW_ROUTINE"} & global_allowed:
            raise RuntimeError("migration_database_privilege_required:SHOW_ROUTINE")
        if backup:
            definers = await db.execute_query_dict(
                "SELECT DEFINER AS name FROM information_schema.views"
                " WHERE TABLE_SCHEMA=DATABASE()"
                " UNION SELECT DEFINER FROM information_schema.routines"
                " WHERE ROUTINE_SCHEMA=DATABASE()"
                " UNION SELECT DEFINER FROM information_schema.triggers"
                " WHERE TRIGGER_SCHEMA=DATABASE()"
                " UNION SELECT DEFINER FROM information_schema.events"
                " WHERE EVENT_SCHEMA=DATABASE()"
            )
            current = (await db.execute_query_dict("SELECT CURRENT_USER() AS name"))[0][
                "name"
            ]
            if (
                any(item["name"] != current for item in definers)
                and not {
                    "ALL PRIVILEGES",
                    "SUPER",
                    "SET_USER_ID",
                    "SET_ANY_DEFINER",
                }
                & global_allowed
            ):
                raise RuntimeError("migration_database_privilege_required:SET_USER_ID")
        return
    schemas = await db.execute_query_dict(
        "SELECT nspname FROM pg_namespace WHERE nspname<>'information_schema'"
        " AND nspname NOT LIKE 'pg_%'"
        " AND NOT pg_has_role(current_user,nspowner,'USAGE')"
    )
    if schemas:
        raise RuntimeError(
            "migration_database_schema_owner_required:" + schemas[0]["nspname"]
        )
    ownership = await db.execute_query_dict(
        "SELECT c.relname AS name FROM pg_class c"
        " JOIN pg_namespace n ON n.oid=c.relnamespace"
        " WHERE n.nspname<>'information_schema' AND n.nspname NOT LIKE 'pg_%'"
        " AND NOT pg_has_role(current_user,c.relowner,'USAGE')"
        " UNION ALL SELECT p.proname FROM pg_proc p"
        " JOIN pg_namespace n ON n.oid=p.pronamespace"
        " WHERE n.nspname<>'information_schema' AND n.nspname NOT LIKE 'pg_%'"
        " AND NOT pg_has_role(current_user,p.proowner,'USAGE')"
    )
    if ownership:
        raise RuntimeError(
            "migration_database_object_owner_required:" + ownership[0]["name"]
        )
    allowed = await db.execute_query_dict(
        "SELECT has_database_privilege(current_user,current_database(),'CREATE')"
        " AS allowed"
    )
    if not allowed[0]["allowed"]:
        raise RuntimeError("migration_database_privilege_required:CREATE")


async def _mysql_permissions(
    db, names: set[str], created: set[str], references: set[str]
) -> None:
    """核验目标范围内的实际授权，展开当前已启用的角色。"""
    database = (await guard.identity(db))["db_name"]
    grants = await _mysql_grants(db)
    privileges = {name: set() for name in names | created}
    for grant in grants:
        match = re.match(r"GRANT (.+?) ON (.+?) TO ", grant)
        if not match:
            continue
        scope = match[2].replace(chr(96), "").split(".", 1)
        if len(scope) != 2 or scope[0] not in {"*", database}:
            continue
        for name in privileges:
            if scope[1] in {"*", name}:
                privileges[name].update(re.split(r",\s*", match[1]))
    for name, allowed in privileges.items():
        required = {"SELECT", "INSERT", "UPDATE", "DELETE"}
        if created:
            required.add("LOCK TABLES")
        if name in created:
            required |= {"CREATE", "DROP", "ALTER"}
        if name in references:
            required.add("REFERENCES")
        if "ALL PRIVILEGES" not in allowed and (missing := required - allowed):
            raise RuntimeError(
                f"migration_target_privilege_required:{name}:"
                + ",".join(sorted(missing))
            )


async def prepare(db, request: dict, models: list, save, tables=None) -> dict:
    """在首次目标写入前保存基线并核验回退所需权限。"""
    identity = await guard.require_same(db, request["target_identity"])
    namespace = identity.get("schema")
    plans = schema_plan(db, models, request["operation_id"])
    for plan in plans:
        plan["schema"] = plan["schema"] or namespace
    existing = await guard.table_names(db)
    if request.get("target_rollback"):
        for item in request["target_rollback"]["before"].values():
            await _check(db, item)
        return request["target_rollback"]
    if tables is not None:
        plans = [
            plan
            for plan in plans
            if (plan["schema"], plan["name"]) not in existing
            or plan["name"] in tables
            or f"{plan['schema']}.{plan['name']}" in tables
        ]
    selected = {(plan["schema"], plan["name"]) for plan in plans}
    missing = selected - existing
    if db.capabilities.dialect == "mysql":
        await _mysql_permissions(
            db,
            {name for _, name in selected},
            {name for _, name in missing},
            {
                name
                for plan in plans
                if (plan["schema"], plan["name"]) in missing
                for name in plan["references"]
            },
        )
    elif missing:
        for schema in {schema for schema, _ in missing}:
            if schema != namespace:
                raise RuntimeError(f"migration_schema_create_context_required:{schema}")
            allowed = (
                await db.execute_query_dict(
                    "SELECT has_schema_privilege(current_user,$1,'CREATE') AS allowed",
                    [schema],
                )
            )[0]["allowed"]
            if not allowed:
                raise RuntimeError(f"migration_schema_create_required:{schema}")
    before = {}
    for schema, name in sorted(selected & existing, key=str):
        item = await describe(db, schema, name)
        item["content"] = await content(db, item)
        before[key(schema, name)] = item
    empty_target = policy.requires_empty(request) or bool(request.get("target_cleared"))
    if empty_target:
        for schema, name in existing:
            if await guard.has_business_rows(db, schema, name):
                raise RuntimeError(f"migration_target_content_changed:{name}")
    journal = {
        "version": 1,
        "identity": identity,
        "before": before,
        "existing": [list(item) for item in sorted(existing, key=str)],
        "plans": plans,
        "created": {},
        "empty_target": empty_target,
        "state": "prepared",
    }
    save(request, target_rollback=journal)
    return journal


async def create_missing(client, request: dict, save) -> None:
    """逐表创建并记录成功证据，未确认完成的对象不会自动删除。"""
    journal = request["target_rollback"]
    originals = {tuple(item) for item in journal["existing"]}
    for plan in journal["plans"]:
        item_key = key(plan["schema"], plan["name"])
        if (plan["schema"], plan["name"]) in originals:
            continue
        if item_key in journal["created"]:
            if journal["created"][item_key]["state"] != "created":
                raise RuntimeError(
                    "migration_schema_outcome_unconfirmed:" + plan["name"]
                )
            continue
        if (plan["schema"], plan["name"]) in await guard.table_names(client):
            raise RuntimeError("migration_target_structure_changed:" + plan["name"])
        item = {**plan, "state": "creating"}
        journal["created"][item_key] = item
        save(request, target_rollback=journal, schema_started=True)
        if client.capabilities.dialect == "postgres":
            async with guard.import_transaction(client) as db:
                await db.execute_script(plan["sql"])
                text = (plan["comment"] + plan["marker"]).replace("'", "''")
                await db.execute_query(
                    f"COMMENT ON TABLE {relation(db, plan)} IS '{text}'"
                )
                result = await describe(db, plan["schema"], plan["name"])
                result["content"] = await content(db, result)
        else:
            await client.execute_script(plan["sql"])
            result = await describe(client, plan["schema"], plan["name"])
            result["content"] = await content(client, result)
        item.update(state="created", initial=result)
        save(request, target_rollback=journal)


async def record_applied(db, request: dict, report: dict, save) -> None:
    """保存数据提交前的目标证据，重启时只核对结果而不重复导入。"""
    journal = request.get("target_rollback")
    if not journal:
        return
    journal["applied"] = {}
    for plan in journal["plans"]:
        item_key = key(plan["schema"], plan["name"])
        item = await describe(db, plan["schema"], plan["name"])
        item["content"] = await content(db, item)
        journal["applied"][item_key] = item
    journal.update(state="commit_pending", import_report=report)
    save(request, target_rollback=journal)
    await checkpoint(db, request, save, "import_commit_pending")


async def checkpoint(db, request: dict, save, phase: str) -> None:
    """记录服务端整库覆盖最后确认的对象和数据，仅作为核验依据。"""
    if (request.get("target_native_backup") or {}).get("engine") not in {
        "mysql",
        "postgres",
    }:
        return
    save(
        request,
        target_checkpoint={
            "phase": phase,
            "objects": await database_objects(db),
            "content": await database_content(db),
        },
    )


async def _check(db, expected: dict) -> None:
    actual = await describe(db, expected["schema"], expected["name"])
    if any(actual.get(k) != expected.get(k) for k in ("oid", "structure", "comment")):
        raise RuntimeError("migration_target_structure_changed:" + expected["name"])
    if await content(db, actual) != expected["content"]:
        raise RuntimeError("migration_target_content_changed:" + expected["name"])


async def _matches(db, expected: dict) -> bool:
    try:
        for item in expected.values():
            await _check(db, item)
    except RuntimeError as error:
        if not str(error).startswith(
            ("migration_target_structure_changed:", "migration_target_content_changed:")
        ):
            raise
        return False
    return True


async def reconcile(client, request: dict) -> dict:
    """旧执行者退出后核对提交前收据，不重新执行数据导入。"""
    journal = request.get("target_rollback") or {}
    if not journal.get("import_report") or not journal.get("applied"):
        raise RuntimeError("migration_import_outcome_unconfirmed")
    async with guard.import_transaction(client) as db:
        await guard.require_same(db, journal["identity"])
        for item in sorted(
            journal["applied"].values(), key=lambda v: key(v["schema"], v["name"])
        ):
            if db.capabilities.dialect == "postgres":
                await db.execute_query(
                    f"LOCK TABLE {relation(db, item)} IN SHARE ROW EXCLUSIVE MODE"
                )
            else:
                await db.execute_query(f"SELECT * FROM {relation(db, item)} FOR UPDATE")
        for item in journal["applied"].values():
            await _check(db, item)
    return journal["import_report"]


@asynccontextmanager
async def _mysql_window(client, items: list[dict]):
    """在单一物理连接上持有 DDL 回退锁，退出时恢复会话状态。"""
    async with client.acquire_connection() as connection:
        db = _transaction_wrapper(client)
        db._connection = connection
        timeout = (await db.execute_query_dict("SELECT @@lock_wait_timeout AS value"))[
            0
        ]["value"]
        foreign_keys = (
            await db.execute_query_dict("SELECT @@foreign_key_checks AS value")
        )[0]["value"]
        try:
            await db.execute_query("SET SESSION lock_wait_timeout=3")
            await db.execute_query("SET SESSION foreign_key_checks=1")
            await db.execute_query(
                "LOCK TABLES "
                + ",".join(relation(db, item) + " WRITE" for item in items)
            )
            try:
                yield db
            finally:
                await db.execute_query("UNLOCK TABLES")
        finally:
            await db.execute_query(
                f"SET SESSION foreign_key_checks={int(foreign_keys)}"
            )
            await db.execute_query(f"SET SESSION lock_wait_timeout={int(timeout)}")


async def rollback(client, request: dict, save) -> None:
    """只补偿空目标及本任务新建表；既有实例数据由原快照回退。"""
    if request.get("commit_decision"):
        raise RuntimeError("migration_commit_already_decided")
    journal = request.get("target_rollback")
    if not journal:
        raise RuntimeError("migration_target_rollback_evidence_missing")
    await guard.require_same(client, journal["identity"])
    created = journal["created"]
    existing = await guard.table_names(client)
    for item in created.values():
        present = (item["schema"], item["name"]) in existing
        if item["state"] == "creating":
            if present:
                raise RuntimeError("migration_schema_outcome_unconfirmed")
            item["state"] = "dropped"
        elif item["state"] == "dropping" and not present:
            item["state"] = "dropped"
    save(request, target_rollback=journal)
    if journal.get("state") == "rolled_back":
        return
    expected = (
        journal.get("applied") if not request.get("import_rollback_confirmed") else None
    )
    expected = expected or {
        **journal["before"],
        **{k: v["initial"] for k, v in created.items() if v.get("initial")},
    }
    if journal.get("state") == "data_reverted":
        expected = journal["reverted"]
    pending = deepcopy(
        {
            k: v
            for k, v in expected.items()
            if created.get(k, {}).get("state") != "dropped"
        }
    )
    ddl_pending = journal.get("state") == "ddl_commit_pending"
    absent = {
        k
        for k, item in pending.items()
        if (item["schema"], item["name"]) not in existing
    }
    if absent and (not ddl_pending or absent != set(journal.get("drop_keys", created))):
        raise RuntimeError("migration_rollback_outcome_unconfirmed")
    if ddl_pending and absent:
        pending = {k: v for k, v in journal["reverted"].items() if k not in created}
    tables = {(item["schema"], item["name"]) for item in pending.values()}
    owned = {(item["schema"], item["name"]) for item in created.values()}
    keys = await guard.foreign_keys(client)
    for fk in keys:
        if (
            guard.relation_key(fk, "parent") in owned
            and guard.relation_key(fk, "child") not in owned
        ):
            raise RuntimeError("migration_target_external_reference:" + fk["parent"])
    dialect = client.capabilities.dialect
    async with guard.import_transaction(client) as db:
        if dialect == "postgres":
            for item in sorted(
                pending.values(),
                key=lambda value: (value["schema"] or "", value["name"]),
            ):
                await db.execute_query(
                    f"LOCK TABLE {relation(db, item)} IN ACCESS EXCLUSIVE MODE"
                )
        else:
            for item in pending.values():
                await db.execute_query(f"SELECT * FROM {relation(db, item)} FOR UPDATE")
        await guard.require_same(db, journal["identity"])
        if ddl_pending or journal.get("state") == "data_commit_pending":
            reverted = {k: v for k, v in journal["reverted"].items() if k in pending}
            if await _matches(db, reverted) and (
                not ddl_pending or absent or not created
            ):
                pending = deepcopy(reverted)
                if ddl_pending:
                    for item in created.values():
                        item["state"] = "dropped"
                    journal["state"] = "rolled_back"
                    save(request, target_rollback=journal)
                    return
                journal["state"] = "data_reverted"
                save(request, target_rollback=journal)
            elif absent or not await _matches(db, pending):
                raise RuntimeError("migration_rollback_outcome_unconfirmed")
        for fk in await guard.foreign_keys(db):
            if (
                guard.relation_key(fk, "parent") in owned
                and guard.relation_key(fk, "child") not in owned
            ):
                raise RuntimeError(
                    "migration_target_external_reference:" + fk["parent"]
                )
        for item in pending.values():
            await _check(db, item)
        if journal["empty_target"] and journal.get("state") != "data_reverted":
            affected = set(
                (request.get("report") or journal.get("import_report") or {}).get(
                    "tables", {}
                )
            )
            for plan in reversed(journal["plans"]):
                name = plan["name"]
                if (
                    name not in affected
                    and key(plan["schema"], name) in journal["before"]
                ):
                    continue
                if (plan["schema"], name) in tables:
                    await db.execute_query(f"DELETE FROM {relation(db, plan)}")
            if await guard.invalid_relations(db, keys, tables):
                raise RuntimeError("migration_rollback_foreign_key_conflict")
        if dialect == "postgres":
            reverted = {}
            for item_key, item in pending.items():
                if item_key not in created:
                    reverted[item_key] = {**item, "content": await content(db, item)}
            for plan in reversed(journal["plans"]):
                item_key = key(plan["schema"], plan["name"])
                if item_key in created and created[item_key]["state"] != "dropped":
                    await db.execute_query(f"DROP TABLE {relation(db, plan)}")
            journal.update(
                state="ddl_commit_pending",
                reverted=reverted,
                drop_keys=[k for k, v in created.items() if v["state"] != "dropped"],
            )
            save(request, target_rollback=journal)
        else:
            for item in pending.values():
                item["content"] = await content(db, item)
            journal.update(state="data_commit_pending", reverted=pending)
            save(request, target_rollback=journal)
    if dialect == "mysql":
        journal["state"] = "data_reverted"
        save(request, target_rollback=journal)
        items = list(pending.values())
        if items and any(item["state"] != "dropped" for item in created.values()):
            async with _mysql_window(client, items) as db:
                for item in items:
                    await _check(db, item)
                for fk in await guard.foreign_keys(db):
                    if (
                        guard.relation_key(fk, "parent") in owned
                        and guard.relation_key(fk, "child") not in owned
                    ):
                        raise RuntimeError(
                            "migration_target_external_reference:" + fk["parent"]
                        )
                for plan in reversed(journal["plans"]):
                    item_key = key(plan["schema"], plan["name"])
                    if item_key in created and created[item_key]["state"] != "dropped":
                        created[item_key]["state"] = "dropping"
                        save(request, target_rollback=journal)
                        await db.execute_query(f"DROP TABLE {relation(db, plan)}")
                        created[item_key]["state"] = "dropped"
                        save(request, target_rollback=journal)
    for item in created.values():
        item["state"] = "dropped"
    journal["state"] = "rolled_back"
    save(request, target_rollback=journal)


async def settle(client, request: dict, save) -> None:
    """提交决定后清除本任务的建表标记，失败保留可重试的结算记录。"""
    journal = request.get("target_rollback")
    if not journal or not request.get("commit_decision"):
        return
    await guard.require_same(client, journal["identity"])
    for item in journal["created"].values():
        if item.get("marker_removed"):
            continue
        postgres = client.capabilities.dialect == "postgres"
        window = (
            guard.import_transaction(client)
            if postgres
            else _mysql_window(client, [item])
        )
        async with window as db:
            if postgres:
                await db.execute_query(
                    f"LOCK TABLE {relation(db, item)} IN ACCESS EXCLUSIVE MODE"
                )
            await guard.require_same(db, journal["identity"])
            actual = await describe(db, item["schema"], item["name"])
            expected = item["initial"]
            if actual.get("oid") != expected.get("oid"):
                raise RuntimeError("migration_target_structure_changed:" + item["name"])
            if actual["comment"] != item["comment"]:
                if actual["structure"] != expected["structure"]:
                    raise RuntimeError(
                        "migration_target_marker_changed:" + item["name"]
                    )
                comment = item["comment"].replace("'", "''")
                table = relation(db, item)
                await db.execute_query(
                    f"COMMENT ON TABLE {table} IS '{comment}'"
                    if postgres
                    else f"ALTER TABLE {table} COMMENT=%s",
                    None if postgres else [item["comment"]],
                )
        item["marker_removed"] = True
        save(request, target_rollback=journal)
