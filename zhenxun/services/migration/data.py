"""按 ORM 模型导出 / 导入数据库数据，格式与数据库类型无关"""

from __future__ import annotations

import base64
from collections.abc import Callable
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from enum import Enum
import importlib
import json
from pathlib import Path
from typing import Any
from uuid import UUID

from tortoise import Tortoise, timezone
from tortoise.backends.base.client import BaseDBAsyncClient
from tortoise.fields import BinaryField, JSONField
from tortoise.models import Model

FORMAT = 1
MANIFEST = "manifest.json"

Progress = Callable[[str, int], None]


class DataError(RuntimeError):
    """数据导出 / 导入失败"""


async def open_client(db_url: str, name: str) -> BaseDBAsyncClient:
    """按数据库地址创建一个独立连接，不注册到 Tortoise，MySQL 会话固定为可重复读

    参数:
        db_url: 数据库地址
        name: 连接名称

    返回:
        BaseDBAsyncClient: 已建立连接的客户端
    """
    from zhenxun.configs.database import DatabaseConnection
    from zhenxun.services.db_context.config import (
        MYSQL_CONFIG,
        POSTGRESQL_CONFIG,
        SQLITE_CONFIG,
    )

    endpoint = DatabaseConnection.parse(db_url)
    defaults = {
        "postgres": {**POSTGRESQL_CONFIG, "minsize": 1, "maxsize": 2},
        "mysql": {**MYSQL_CONFIG, "minsize": 1, "maxsize": 2},
        "sqlite": SQLITE_CONFIG,
    }[endpoint.engine]
    config = endpoint.orm_config(defaults)
    if endpoint.engine == "mysql":
        config["credentials"]["init_command"] = (
            "SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ"
        )
    module = importlib.import_module(config["engine"])
    client = module.client_class(connection_name=name, **config["credentials"])
    await client.create_connection(with_db=True)
    return client


def _models() -> list[type[Model]]:
    """所有已注册模型，按外键依赖排序，被引用的表在前"""
    tables: dict[str, type[Model]] = {}
    for app in Tortoise.apps.values():
        for model in app.values():
            if not model._meta.abstract:
                tables.setdefault(model._meta.db_table, model)
    ordered: list[type[Model]] = []
    visiting: set[str] = set()

    def visit(model: type[Model]):
        table = model._meta.db_table
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


async def create_schema(client: BaseDBAsyncClient) -> None:
    """在指定连接上为所有已注册模型建表（已存在的表跳过）

    参数:
        client: 目标数据库连接
    """
    base = client.schema_generator

    class Generator(base):  # type: ignore
        def _get_models_to_create(self, models_to_create):
            for model in _models():
                model._check()
                models_to_create.append(model)

    await client.execute_script(Generator(client).get_create_schema_sql(safe=True))


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
    models = _models()
    if (
        models
        and models[0]._meta.db.capabilities.dialect != client.capabilities.dialect
    ):
        raise DataError("源数据库类型与当前模型使用的数据库不一致")
    directory.mkdir(parents=True, exist_ok=True)
    tables = {}
    async with client._in_transaction() as db:
        if client.capabilities.dialect == "postgres":
            await db.execute_query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        for model in models:
            meta = model._meta
            fields = list(meta.fields_db_projection)
            pk = meta.pk_attr
            count, last = 0, None
            with (directory / f"{meta.db_table}.jsonl").open(
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
            tables[meta.db_table] = {
                "model": f"{model.__module__}.{model.__name__}",
                "fields": fields,
                "rows": count,
            }
    manifest = {"format": FORMAT, "dialect": client.capabilities.dialect}
    manifest["tables"] = tables
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


async def _reset_sequence(db, model: type[Model]) -> None:
    meta = model._meta
    if db.capabilities.dialect != "postgres" or not meta.pk.generated:
        return
    table, column = meta.db_table, meta.db_pk_column
    await db.execute_query(
        f"SELECT setval(pg_get_serial_sequence('\"{table}\"', '{column}'), "
        f'COALESCE(MAX("{column}"), 1), MAX("{column}") IS NOT NULL) '
        f'FROM "{table}"'
    )


async def _clear(db, models: list[type[Model]], replace: bool) -> None:
    for model in reversed(models):
        table = db.query_class.from_(model._meta.db_table)
        if replace:
            await db.execute_query(table.delete().get_sql())
        elif (await db.execute_query(table.select(1).limit(1).get_sql()))[1]:
            raise DataError(f"目标表 {model._meta.db_table} 不为空")


async def _write_table(
    db, path: Path, model: type[Model], source: list[str], batch_size, progress
) -> tuple[int, list[str], list[str]]:
    meta = model._meta
    executor = db.executor_class(model=model, db=db)
    columns = executor.regular_columns_all
    index = {name: position for position, name in enumerate(source)}
    defaults = {name: _default(model, name) for name in columns if name not in index}
    decode = {name: _decoder(meta.fields_map[name]) for name in columns}
    to_db = executor.column_map

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

    count, batch = 0, []
    with path.open(encoding="utf-8") as file:
        for line in file:
            batch.append(convert(json.loads(line)))
            if len(batch) >= batch_size:
                await db.execute_many(executor.insert_query_all, batch)
                count += len(batch)
                batch.clear()
                if progress:
                    progress(meta.db_table, count)
    if batch:
        await db.execute_many(executor.insert_query_all, batch)
        count += len(batch)
    if progress:
        progress(meta.db_table, count)
    await _reset_sequence(db, model)
    return count, sorted(set(source) - set(columns)), list(defaults)


async def import_data(
    directory: Path,
    client: BaseDBAsyncClient,
    *,
    replace: bool = False,
    batch_size: int = 1000,
    progress: Progress | None = None,
) -> dict:
    """在一个事务内把导出数据写入目标数据库，按目标模型兼容字段差异

    目标库须已建表。导出中有而目标模型没有的表和字段被跳过，目标模型新增的
    字段使用默认值。

    参数:
        directory: 导出目录
        client: 目标数据库连接
        replace: 是否先清空目标表；为 False 时目标表必须为空
        batch_size: 每批写入行数
        progress: 进度回调 (表名, 已导入行数)

    返回:
        dict: 导入报告
    """
    try:
        manifest = json.loads((directory / MANIFEST).read_text(encoding="utf-8"))
    except (OSError, ValueError) as error:
        raise DataError("导出清单缺失或已损坏") from error
    if manifest.get("format") != FORMAT:
        raise DataError(f"不支持的数据格式: {manifest.get('format')}")
    exported: dict = manifest["tables"]
    models = _models()
    report = {
        "tables": {},
        "skipped_tables": sorted(set(exported) - {m._meta.db_table for m in models}),
        "dropped_fields": {},
        "defaulted_fields": {},
    }
    dialect = client.capabilities.dialect
    async with client._in_transaction() as db:
        if dialect == "sqlite":
            await db.execute_query("PRAGMA defer_foreign_keys = ON")
        elif dialect == "mysql":
            await db.execute_query("SET FOREIGN_KEY_CHECKS = 0")
        try:
            await _clear(db, models, replace)
            for model in models:
                table = model._meta.db_table
                if table not in exported:
                    continue
                count, dropped, defaulted = await _write_table(
                    db,
                    directory / f"{table}.jsonl",
                    model,
                    exported[table]["fields"],
                    batch_size,
                    progress,
                )
                if count != exported[table]["rows"]:
                    raise DataError(
                        f"{table} 行数不一致: {count}/{exported[table]['rows']}"
                    )
                report["tables"][table] = count
                if dropped:
                    report["dropped_fields"][table] = dropped
                if defaulted:
                    report["defaulted_fields"][table] = defaulted
        finally:
            if dialect == "mysql":
                await db.execute_query("SET FOREIGN_KEY_CHECKS = 1")
    return report
