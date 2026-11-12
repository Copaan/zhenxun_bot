"""按 ORM 模型导出 / 导入数据库数据，格式与数据库类型无关"""

from __future__ import annotations

import base64
from collections.abc import Awaitable, Callable
from contextlib import nullcontext
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from enum import Enum
from hashlib import sha256
import importlib
import json
import os
from pathlib import Path
from typing import Any
from uuid import UUID

from tortoise import Tortoise, timezone
from tortoise.backends.base.client import BaseDBAsyncClient
from tortoise.fields import BinaryField, JSONField
from tortoise.models import Model

from . import data_guard as guard

FORMAT = 1
MANIFEST = "manifest.json"

Progress = Callable[[str, int], None]


class DataError(RuntimeError):
    """数据导出 / 导入失败"""


async def open_client(
    db_url: str, name: str, *, read_only: bool = False
) -> BaseDBAsyncClient:
    """按数据库地址创建一个独立连接，不注册到 Tortoise，MySQL 会话固定为可重复读

    参数:
        db_url: 数据库地址
        name: 连接名称

    返回:
        BaseDBAsyncClient: 已建立连接的客户端
    """
    from zhenxun.configs.database import DatabaseConnection

    endpoint = DatabaseConnection.parse(
        db_url, root=Path(os.environ.get("ZHENXUN_PROJECT_ROOT") or Path.cwd())
    )
    defaults = {
        "postgres": {"minsize": 1, "maxsize": 2, "timeout": 30},
        "mysql": {"minsize": 1, "maxsize": 2, "connect_timeout": 30},
        "sqlite": {"journal_mode": "WAL", "busy_timeout": 3000, "foreign_keys": "ON"},
    }[endpoint.engine]
    config = endpoint.orm_config(defaults)
    if endpoint.engine == "mysql":
        config["credentials"]["init_command"] = (
            "SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ"
        )
    if endpoint.engine == "sqlite" and not read_only:
        if endpoint.database != ":memory:":
            Path(endpoint.database).parent.mkdir(parents=True, exist_ok=True)
    module = importlib.import_module(config["engine"])
    client = module.client_class(connection_name=name, **config["credentials"])
    if read_only and endpoint.engine == "sqlite":
        import sqlite3

        import aiosqlite

        path = Path(endpoint.database).resolve()
        if endpoint.database == ":memory:" or not path.is_file():
            raise DataError("migration_readonly_database_missing")
        client.pragmas.clear()
        connection = await aiosqlite.connect(
            path.as_uri() + "?mode=ro", uri=True, isolation_level=None
        )
        connection.row_factory = sqlite3.Row
        client._connection = connection
        await connection.execute("PRAGMA busy_timeout=3000")
        return client
    await client.create_connection(with_db=True)
    return client


def _table_key(model) -> str:
    schema = getattr(model._meta, "schema", None)
    return f"{schema}.{model._meta.db_table}" if schema else model._meta.db_table


def column_converters(executor) -> dict:
    """各列把 Python 值转换为数据库值的函数

    参数:
        executor: 模型的执行器

    返回:
        dict: 列名 -> 转换函数，参数为值与模型
    """
    converters = getattr(executor, "column_map", None)
    if converters is None:
        fields = executor.model._meta.fields_map
        converters = {
            name: fields[name].to_db_value for name in executor.regular_columns_all
        }
    return converters


def _models() -> list[type[Model]]:
    """所有已注册模型，按外键依赖排序，被引用的表在前；数据库未初始化时为空"""
    tables: dict[str, type[Model]] = {}
    for app in (Tortoise.apps or {}).values():
        for model in app.values():
            if not model._meta.abstract:
                tables.setdefault(_table_key(model), model)
    ordered: list[type[Model]] = []
    visiting: set[str] = set()

    def visit(model: type[Model]):
        table = _table_key(model)
        if model in ordered or table in visiting:
            return
        visiting.add(table)
        for name in (*model._meta.fk_fields, *model._meta.o2o_fields):
            related = model._meta.fields_map[name].related_model  # type: ignore
            if related is not model:
                visit(related)
        visiting.discard(table)
        ordered.append(model)

    for table in sorted(tables):
        visit(tables[table])
    return ordered


async def create_schema(client: BaseDBAsyncClient, models: list | None = None) -> None:
    """在指定连接上为所有已注册模型建表（已存在的表跳过）

    参数:
        client: 目标数据库连接
    """
    base = client.schema_generator

    class Generator(base):  # type: ignore
        def _get_models_to_create(self, models_to_create=None):
            result = [] if models_to_create is None else models_to_create
            for model in models if models is not None else _models():
                model._check()
                result.append(model)
            return result

    sql = Generator(client).get_create_schema_sql(safe=True)
    if client.capabilities.dialect == "sqlite":
        import sqlite3

        statement = ""
        for character in sql:
            statement += character
            if character == ";" and sqlite3.complete_statement(statement):
                await client.execute_query(statement)
                statement = ""
        if statement.strip():
            await client.execute_query(statement)
    else:
        await client.execute_script(sql)


async def sqlite_revision(db, *, legacy: bool = False) -> str:
    """在调用者的数据库保护窗口中计算 SQLite 逻辑指纹"""
    digest = sha256()
    async for statement in db._connection.iterdump():
        if not legacy and statement == 'DELETE FROM "sqlite_sequence";':
            continue
        digest.update((statement + "\n").encode())
    return digest.hexdigest()


def _encode(value: Any) -> Any:
    if isinstance(value, datetime | date | time):
        return value.isoformat()
    if isinstance(value, Decimal | UUID):
        return str(value)
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, timedelta):
        return (value.days * 86400 + value.seconds) * 1_000_000 + value.microseconds
    if isinstance(value, bytes):
        return base64.b64encode(value).decode()
    raise TypeError(f"unsupported value: {type(value).__name__}")


async def export_data(
    directory: Path,
    client: BaseDBAsyncClient,
    *,
    batch_size: int = 1000,
    progress: Progress | None = None,
    transaction=None,
) -> dict:
    """在一个快照读事务内导出所有模型数据，每张表一份 JSONL

    参数:
        directory: 输出目录
        client: 源数据库连接，须与当前模型使用的数据库类型一致
        batch_size: 每批读取行数
        progress: 进度回调 (表名, 已导出行数)

    返回:
        dict: 清单，包含每张表的字段与行数
    """
    from .merge import INTERNAL_TABLE, export_origins

    models = [m for m in _models() if m._meta.db_table != INTERNAL_TABLE]
    if (
        models
        and models[0]._meta.db.capabilities.dialect != client.capabilities.dialect
    ):
        raise DataError("源数据库类型与当前模型使用的数据库不一致")
    directory.mkdir(parents=True, exist_ok=True)
    tables = {}
    context = (
        nullcontext(transaction)
        if transaction is not None
        else client._in_transaction()
    )
    async with context as db:
        if transaction is None and client.capabilities.dialect == "postgres":
            await db.execute_query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        source_identity = await export_origins(db, directory)
        for model in models:
            meta = model._meta
            fields = list(meta.fields_db_projection)
            pk = meta.pk_attr
            count, last = 0, None
            with (directory / f"{_table_key(model)}.jsonl").open(
                "w", encoding="utf-8", newline="\n"
            ) as file:
                while True:
                    query = model.all().using_db(db).order_by(pk).limit(batch_size)
                    if last is not None:
                        query = query.filter(**{f"{pk}__gt": last})
                    rows = await query
                    for row in rows:
                        values = [getattr(row, name) for name in fields]
                        file.write(
                            json.dumps(values, ensure_ascii=False, default=_encode)
                        )
                        file.write("\n")
                    count += len(rows)
                    if progress:
                        progress(meta.db_table, count)
                    if len(rows) < batch_size:
                        break
                    last = rows[-1].pk
            tables[_table_key(model)] = {
                "model": f"{model.__module__}.{model.__name__}",
                "fields": fields,
                "rows": count,
            }
    manifest = {"format": FORMAT, "dialect": client.capabilities.dialect}
    manifest["tables"] = tables
    manifest["source_identity"] = source_identity
    manifest["origins_file"] = "origins.jsonl"
    (directory / MANIFEST).write_text(
        json.dumps(manifest, ensure_ascii=False, indent=1), encoding="utf-8"
    )
    return manifest


def _decoder(field) -> Callable[[Any], Any]:
    if isinstance(field, JSONField):
        return lambda value: value
    if isinstance(field, BinaryField):
        return lambda value: None if value is None else base64.b64decode(value)
    return field.to_python_value


def _strip_nul(value: Any) -> Any:
    """去掉字符串（含 JSON 内嵌字符串）中 PostgreSQL 无法存储的 NUL 字符

    参数:
        value: 解码后的字段值

    返回:
        Any: 处理后的值
    """
    if isinstance(value, str):
        return value.replace("\x00", "")
    if isinstance(value, list):
        return [_strip_nul(item) for item in value]
    if isinstance(value, dict):
        return {_strip_nul(key): _strip_nul(item) for key, item in value.items()}
    return value


def _default(model: type[Model], name: str) -> Callable[[], Any]:
    field = model._meta.fields_map[name]
    if getattr(field, "auto_now", False) or getattr(field, "auto_now_add", False):
        return timezone.now
    if field.default is not None:
        default = field.default
        return default if callable(default) else lambda: default
    if field.null:
        return lambda: None
    raise DataError(f"{model._meta.db_table}.{name} 缺少数据且没有默认值")


async def _reset_sequence(
    db, model: type[Model], maximum=None, *, read_only: bool = False
) -> dict | None:
    """按待导入主键提前核验序列权限，只在需要推进时取得序列写锁"""
    meta = model._meta
    if db.capabilities.dialect != "postgres" or not meta.pk.generated:
        return None
    rows = await db.execute_query_dict(
        "SELECT pg_get_serial_sequence($1, $2) AS name",
        [guard.table_sql(db, model), meta.db_pk_column],
    )
    name = rows[0]["name"]
    if not name:
        return None
    info = (
        await db.execute_query_dict(
            "SELECT n.nspname, c.relname, s.seqincrement, s.seqcache, s.seqcycle,"
            " pg_has_role(current_user,c.relowner,'USAGE') AS can_alter"
            " FROM pg_sequence s JOIN pg_class c ON c.oid=s.seqrelid"
            " JOIN pg_namespace n ON n.oid=c.relnamespace"
            " WHERE s.seqrelid=$1::regclass",
            [name],
        )
    )[0]
    sequence = guard.relation_sql(db, info["relname"], info["nspname"])
    increment = int(info["seqincrement"])
    if increment != 1 or info["seqcycle"] or int(info["seqcache"]) != 1:
        raise DataError(f"序列 {name} 使用非标准步长、循环或缓存规则，须独立核验")
    try:
        before = (
            await db.execute_query_dict(f"SELECT last_value, is_called FROM {sequence}")
        )[0]
    except Exception as error:
        raise DataError(f"migration_sequence_read_required:{name}") from error
    current_maximum = (
        await db.execute_query_dict(
            f"SELECT MAX({guard.quote(db, meta.db_pk_column)}) AS value"
            f" FROM {guard.table_sql(db, model)}"
        )
    )[0]["value"]
    maximum = max(
        (value for value in (maximum, current_maximum) if value is not None),
        default=None,
    )
    next_value = int(before["last_value"]) + (increment if before["is_called"] else 0)
    if maximum is not None and next_value <= maximum:
        if not info["can_alter"]:
            raise DataError(f"migration_sequence_alter_required:{name}")
        if read_only:
            return {"name": name, "advance_required": True}
        await db.execute_query(
            f"ALTER SEQUENCE {sequence} CACHE {int(info['seqcache'])}"
        )
        before = (
            await db.execute_query_dict(f"SELECT last_value, is_called FROM {sequence}")
        )[0]
        next_value = int(before["last_value"]) + (
            increment if before["is_called"] else 0
        )
        if next_value <= maximum:
            next_value = int(maximum) + increment
            await db.execute_query(
                f"ALTER SEQUENCE {sequence} RESTART WITH {next_value}"
            )
    return {"name": name, "before": dict(before), "next": next_value}


def _parameters(db, count: int) -> list[str]:
    if db.capabilities.dialect == "postgres":
        return ["$" + str(index + 1) for index in range(count)]
    return ["%s" if db.capabilities.dialect == "mysql" else "?"] * count


async def database_revision(db, tables: dict | None = None) -> dict:
    """按主键分批计算内容修订，跨进程一致且不包含连接凭据"""
    result = {}
    models = {_table_key(model): model for model in _models()}
    for name in sorted(tables if tables is not None else models):
        if name not in models:
            raise DataError(f"无法核对原表 {name} 的模型")
        model = models[name]
        fields = (tables[name].get("fields") if tables else None) or list(
            model._meta.fields_db_projection
        )
        digest, count, last = sha256(), 0, None
        while True:
            query = model.all().using_db(db).order_by(model._meta.pk_attr).limit(1000)
            if last is not None:
                query = query.filter(**{f"{model._meta.pk_attr}__gt": last})
            rows = await query
            for row in rows:
                payload = json.dumps(
                    [getattr(row, key) for key in fields],
                    ensure_ascii=False,
                    default=_encode,
                )
                digest.update((payload + "\n").encode())
            count += len(rows)
            if len(rows) < 1000:
                break
            last = rows[-1].pk
        result[name] = {"fields": fields, "rows": count, "sha256": digest.hexdigest()}
    return result


async def _retained_references(db, keys, models, exported, directory, schema):
    """在写入前拒绝会使保留表失去引用目标的数据替换"""
    names = {guard.model_key(model, schema): model for model in models}
    for group in guard.groups(keys):
        key = group[0]
        if (
            guard.relation_key(key, "parent") not in names
            or guard.relation_key(key, "child") in names
        ):
            continue
        model = names[guard.relation_key(key, "parent")]
        table = _table_key(model)
        projection = model._meta.fields_db_projection
        fields = exported[table]["fields"]
        columns = [
            next(
                (
                    field
                    for field, column in projection.items()
                    if column == item["target"]
                ),
                None,
            )
            for item in group
        ]
        if any(column not in fields for column in columns):
            raise DataError(f"保留表 {key['child']} 引用的字段不在备份中")
        indexes = [fields.index(column) for column in columns]
        field_types = [model._meta.fields_map[column] for column in columns]
        desired = set()
        with (directory / f"{table}.jsonl").open(encoding="utf-8") as file:
            for line in file:
                row = json.loads(line)
                desired.add(
                    tuple(
                        _decoder(field)(row[index])
                        for field, index in zip(field_types, indexes)
                    )
                )
        child = guard.relation_sql(db, key["child"], key.get("child_schema"))
        parent = guard.relation_sql(db, key["parent"], key.get("parent_schema"))
        selected = ",".join("c." + guard.quote(db, item["column"]) for item in group)
        joined = " AND ".join(
            f'c.{guard.quote(db, item["column"])}=p.{guard.quote(db, item["target"])}'
            for item in group
        )
        for row in await db.execute_query_dict(
            f"SELECT DISTINCT {selected} FROM {child} c JOIN {parent} p ON {joined}"
        ):
            values = [row[item["column"]] for item in group]
            if (
                all(value is not None for value in values)
                and tuple(
                    field.to_python_value(value)
                    for field, value in zip(field_types, values)
                )
                not in desired
            ):
                raise DataError(
                    f"保留表 {key['child']} 引用 {table} 的记录不在恢复包中，未写入目标"
                )


async def _write_table(
    db,
    path: Path,
    model: type[Model],
    source: list[str],
    batch_size,
    progress,
    *,
    merge=False,
) -> tuple[int, list[str], list[str], set]:
    meta = model._meta
    executor = db.executor_class(model=model, db=db)
    columns = executor.regular_columns_all
    index = {name: position for position, name in enumerate(source)}
    defaults = {name: _default(model, name) for name in columns if name not in index}
    decode = {name: _decoder(meta.fields_map[name]) for name in columns}
    if db.capabilities.dialect == "postgres":
        decode = {
            name: lambda value, decoder=decoder: _strip_nul(decoder(value))
            for name, decoder in decode.items()
        }
    to_db = column_converters(executor)

    def convert(row: list) -> list:
        return [
            to_db[name](
                defaults[name]()
                if name in defaults
                else decode[name](row[index[name]]),
                model,
            )
            for name in columns
        ]

    table = guard.table_sql(db, model)
    pk_column = guard.quote(db, meta.db_pk_column)
    existing = {
        row[meta.db_pk_column]
        for row in await db.execute_query_dict(f"SELECT {pk_column} FROM {table}")
    }
    if not merge and existing:
        raise DataError(f"目标表 {_table_key(model)} 不为空")
    pk_index = columns.index(meta.pk_attr)
    seen = set()
    update_columns = [index for index in range(len(columns)) if index != pk_index]
    parameters = _parameters(db, len(update_columns) + 1)
    assignments = ",".join(
        f"{guard.quote(db, meta.fields_db_projection[columns[index]])}"
        f"={parameters[offset]}"
        for offset, index in enumerate(update_columns)
    )
    update_sql = f"UPDATE {table} SET {assignments} WHERE {pk_column}={parameters[-1]}"

    async def write_batch(rows):
        inserts, updates = [], []
        for row in rows:
            key = row[pk_index]
            if key in seen:
                raise DataError(f"{_table_key(model)} 的备份包含重复主键")
            seen.add(key)
            if key in existing:
                if update_columns:
                    updates.append([row[index] for index in update_columns] + [key])
            else:
                inserts.append(row)
        if updates:
            await db.execute_many(update_sql, updates)
        if inserts:
            await db.execute_many(executor.insert_query_all, inserts)

    count, batch = 0, []
    with path.open(encoding="utf-8") as file:
        for line in file:
            batch.append(convert(json.loads(line)))
            if len(batch) >= batch_size:
                await write_batch(batch)
                count += len(batch)
                batch.clear()
                if progress:
                    progress(meta.db_table, count)
    if batch:
        await write_batch(batch)
        count += len(batch)
    if progress:
        progress(meta.db_table, count)
    return count, sorted(set(source) - set(columns)), list(defaults), existing - seen


async def import_data(
    directory: Path,
    client: BaseDBAsyncClient,
    *,
    replace: bool = False,
    batch_size: int = 1000,
    progress: Progress | None = None,
    expected_identity: dict | None = None,
    expected_revision: dict | None = None,
    strict_tables: bool = False,
    excluded_tables: set[str] | None = None,
    included_tables: set[str] | None = None,
    expected_sqlite_fingerprint: str | None = None,
    outcome: dict | None = None,
    before_commit: Callable[..., Awaitable[None]] | None = None,
    restore_mode: str | None = None,
    source_binding: str | None = None,
    archive_sha256: str | None = None,
) -> dict:
    """在受保护写事务内替换包内表，保留目标独有表及其外键引用

    参数:
        directory: 已校验的导出目录
        client: 目标数据库连接
        replace: 是否允许替换包内表的已有记录
        expected_identity: 提交时绑定的实际数据库身份
        expected_revision: 停写快照对应的内容修订
        strict_tables: 缺少目标模型时是否拒绝导入
        excluded_tables: 已确认失败插件所属且允许排除的表
        outcome: 事务退出结果，区分已提交、已回滚和结果未知
    """
    try:
        manifest = json.loads((directory / MANIFEST).read_text(encoding="utf-8"))
    except (OSError, ValueError) as error:
        raise DataError("导出清单缺失或已损坏") from error
    if manifest.get("format") != FORMAT or not isinstance(manifest.get("tables"), dict):
        raise DataError("不支持或损坏的数据清单")
    from .merge import validate_origins

    validate_origins(directory)
    if included_tables is not None:
        included_tables = set(included_tables)
    excluded_tables = set(excluded_tables or ())
    if restore_mode in {"merge_upsert", "merge_skip"}:
        from .merge import import_merge

        return await import_merge(
            directory,
            client,
            restore_mode=restore_mode,
            source_binding=source_binding,
            archive_sha256=archive_sha256,
            included_tables=included_tables,
            excluded_tables=excluded_tables,
            expected_identity=expected_identity,
            expected_revision=expected_revision,
            expected_sqlite_fingerprint=expected_sqlite_fingerprint,
            outcome=outcome,
            before_commit=before_commit,
            progress=progress,
        )
    if restore_mode is not None:
        if restore_mode not in {"empty", "replace_tables", "replace_database"}:
            raise DataError("migration_invalid_restore_mode")
        replace = restore_mode in {"replace_tables", "replace_database"}
    exported = manifest["tables"]
    all_models = _models()
    models = [
        model
        for model in all_models
        if _table_key(model) in exported
        and _table_key(model) not in (excluded_tables or set())
        and (included_tables is None or _table_key(model) in included_tables)
    ]
    skipped = {
        name: value["rows"]
        for name, value in exported.items()
        if name not in {_table_key(model) for model in models}
    }
    unexpected = set(skipped) - (excluded_tables or set())
    if included_tables is not None:
        unexpected &= included_tables
    if strict_tables and unexpected:
        raise DataError("目标模型缺失，未导入：" + ", ".join(sorted(skipped)))
    report = {
        "tables": {},
        "skipped_tables": skipped,
        "preserved_tables": sorted(
            _table_key(model)
            for model in all_models
            if _table_key(model) not in exported
        ),
        "dropped_fields": {},
        "defaulted_fields": {},
        "sequences": [],
    }
    dialect = client.capabilities.dialect
    async with guard.import_transaction(client, outcome=outcome) as db:
        actual_identity = await guard.require_same(db, expected_identity)
        schema = actual_identity.get("schema") if dialect == "postgres" else None
        names = {guard.model_key(model, schema) for model in models}
        keys = await guard.foreign_keys(db)
        await guard.lock_tables(db, models, keys)
        report["database_identity"] = await guard.require_same(db, expected_identity)
        report["preserved_tables"] = sorted(
            f"{namespace}.{name}" if namespace and namespace != schema else name
            for namespace, name in await guard.table_names(db) - names
        )
        if expected_sqlite_fingerprint:
            if expected_sqlite_fingerprint not in {
                await sqlite_revision(db),
                await sqlite_revision(db, legacy=True),
            }:
                raise DataError("migration_target_content_changed")
        if (
            expected_revision is not None
            and await database_revision(db, expected_revision) != expected_revision
        ):
            raise DataError("migration_target_content_changed")
        before = await guard.invalid_relations(db, keys, names)
        await _retained_references(db, keys, models, exported, directory, schema)
        if dialect == "postgres":
            for model in models:
                table = _table_key(model)
                columns = exported[table]["fields"]
                pk = model._meta.pk_attr
                maximum = None
                if model._meta.pk.generated and pk in columns:
                    position = columns.index(pk)
                    with (directory / f"{table}.jsonl").open(encoding="utf-8") as rows:
                        for line in rows:
                            value = json.loads(line)[position]
                            if value is not None:
                                maximum = (
                                    max(maximum, value)
                                    if maximum is not None
                                    else value
                                )
                if sequence := await _reset_sequence(db, model, maximum):
                    report["sequences"].append(sequence)
        foreign_checks = None
        if dialect == "sqlite":
            await db.execute_query("PRAGMA defer_foreign_keys = ON")
        elif dialect == "mysql":
            foreign_checks = (
                await db.execute_query_dict("SELECT @@FOREIGN_KEY_CHECKS AS value")
            )[0]["value"]
            await db.execute_query("SET FOREIGN_KEY_CHECKS = 0")
        surplus = {}
        try:
            for model in models:
                table = _table_key(model)
                count, dropped, defaulted, excess = await _write_table(
                    db,
                    directory / f"{table}.jsonl",
                    model,
                    exported[table]["fields"],
                    batch_size,
                    progress,
                    merge=replace,
                )
                if count != exported[table]["rows"]:
                    raise DataError(
                        f"{table} 行数不一致: {count}/{exported[table]['rows']}"
                    )
                surplus[model] = excess
                report["tables"][table] = count
                if dropped:
                    report["dropped_fields"][table] = dropped
                if defaulted:
                    report["defaulted_fields"][table] = defaulted
            for model in reversed(models):
                extras = list(surplus[model])
                for offset in range(0, len(extras), batch_size):
                    batch = extras[offset : offset + batch_size]
                    placeholders = ",".join(_parameters(db, len(batch)))
                    await db.execute_query(
                        f"DELETE FROM {guard.table_sql(db, model)}"
                        f" WHERE {guard.quote(db, model._meta.db_pk_column)}"
                        f" IN ({placeholders})",
                        batch,
                    )
            violations = await guard.invalid_relations(db, keys, names) - before
            if violations:
                raise DataError(
                    "migration_foreign_key_conflict:" + next(iter(violations))
                )
            for model in models:
                count = (
                    await db.execute_query_dict(
                        f"SELECT COUNT(*) AS count FROM {guard.table_sql(db, model)}"
                    )
                )[0]["count"]
                if count != report["tables"][_table_key(model)]:
                    raise DataError(
                        "migration_final_row_count_changed:" + _table_key(model)
                    )
            from .merge import ensure_identity_in_transaction, record_replacement

            await ensure_identity_in_transaction(db)
            await record_replacement(
                db,
                directory,
                models,
                source_binding=source_binding,
                archive_sha256=archive_sha256,
            )
            if dialect == "sqlite":
                report["sqlite_fingerprint"] = await sqlite_revision(db)
            report["content_revision"] = await database_revision(
                db, {name: {} for name in report["tables"]}
            )
            if before_commit is not None:
                await before_commit(db, report)
        finally:
            if foreign_checks is not None:
                await db.execute_query(
                    f"SET FOREIGN_KEY_CHECKS = {int(foreign_checks)}"
                )
    return report
