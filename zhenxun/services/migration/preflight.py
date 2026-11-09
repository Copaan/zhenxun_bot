"""迁移在线准备的文件、数据格式及只读目标核验。"""

from __future__ import annotations

from hashlib import sha256
import json
import os
from pathlib import Path, PurePosixPath
import re
import shutil
import sys

from . import state as tasks


def digest(value) -> str:
    return sha256(json.dumps(value, sort_keys=True, default=str).encode()).hexdigest()


def file_digest(path: Path) -> str:
    value = tasks.Files.digest(path)
    if value is None:
        raise RuntimeError("migration_prepared_file_missing:" + path.name)
    return value


def tree_digest(path: Path) -> str:
    """绑定任务私有制品内容，不依赖文件时间或工作目录。"""
    return digest(
        {
            item.relative_to(path).as_posix(): file_digest(item)
            for item in sorted(path.rglob("*"))
            if item.is_file()
        }
    )


def environment() -> str:
    """记录解释器、活动依赖和相关配置，不包含在线业务数据。"""
    from zhenxun.services.nonebot_store import runtime, storage

    root = tasks.mutation.INSTANCE_ROOT
    names = (
        ".env",
        ".env.dev",
        "data/config.yaml",
        "data/configs/plugins2config.yaml",
        "pyproject.toml",
        "uv.lock",
    )
    return digest(
        {
            "interpreter": os.path.normcase(str(Path(sys.executable).resolve())),
            "environment": runtime.environment_signature(),
            "manifest": {
                key: value
                for key, value in storage.load_manifest().items()
                if key != "updated_at"
            },
            "configuration": {name: tasks.Files.digest(root / name) for name in names},
        }
    )


def archive_check(path: Path, password: str | None) -> None:
    """只读核验归档路径、重复成员及暂存空间，不解压或执行包内代码。"""
    from . import backup

    seen = set()
    required = 0
    with backup._open(path, "r", password) as archive:
        for info in archive.infolist():
            relative = PurePosixPath(info.filename)
            if (
                relative.is_absolute()
                or ".." in relative.parts
                or "\\" in info.filename
                or re.match(r"^[A-Za-z]:", info.filename)
                or (info.external_attr >> 16) & 0o170000 == 0o120000
            ):
                raise RuntimeError("migration_archive_path_invalid:" + info.filename)
            normalized = os.path.normcase(relative.as_posix())
            if normalized in seen:
                raise RuntimeError("migration_archive_duplicate_path:" + info.filename)
            seen.add(normalized)
            required += info.file_size
    directory = tasks.ROOT
    while not directory.exists():
        directory = directory.parent
    if shutil.disk_usage(directory).free < required:
        raise RuntimeError("migration_staging_space_insufficient")


def validate_data(directory: Path, db=None) -> dict:
    """逐行检查备份结构和已知字段转换，未知插件模型留到受控验证。"""
    from . import data

    manifest = json.loads((directory / data.MANIFEST).read_text("utf-8"))
    if manifest.get("format") != data.FORMAT or not isinstance(
        manifest.get("tables"), dict
    ):
        raise RuntimeError("migration_database_manifest_invalid")
    models = {data._table_key(model): model for model in data._models()}
    deferred, maxima = [], {}
    for name, table in manifest["tables"].items():
        if (
            not isinstance(name, str)
            or Path(name).name != name
            or any(character in name for character in ("/", "\\", ":"))
        ):
            raise RuntimeError("migration_table_path_invalid")
        fields = table.get("fields")
        if (
            not isinstance(fields, list)
            or not fields
            or not all(isinstance(item, str) for item in fields)
            or len(set(fields)) != len(fields)
            or type(table.get("rows")) is not int
            or table["rows"] < 0
        ):
            raise RuntimeError("migration_table_manifest_invalid:" + name)
        model = models.get(name)
        executor = db.executor_class(model=model, db=db) if db and model else None
        converters = {}
        if executor:
            for field in executor.regular_columns_all:
                definition = model._meta.fields_map[field]
                if not type(definition).__module__.startswith("tortoise."):
                    deferred.append(name + "." + field)
                    continue
                if field in fields:
                    converters[field] = data._decoder(definition)
                else:
                    data._default(model, field)
        else:
            deferred.append(name)
        count, seen, maximum = 0, set(), None
        with (directory / (name + ".jsonl")).open(encoding="utf-8") as stream:
            for line in stream:
                count += 1
                row = json.loads(line)
                if not isinstance(row, list) or len(row) != len(fields):
                    raise RuntimeError(f"migration_row_shape_invalid:{name}:{count}")
                if executor:
                    for field, decode in converters.items():
                        try:
                            value = decode(row[fields.index(field)])
                            if db.capabilities.dialect == "postgres":
                                value = data._strip_nul(value)
                            value = executor.column_map[field](value, model)
                            if field == model._meta.pk_attr:
                                token = digest(value)
                                if token in seen:
                                    raise ValueError("duplicate_primary_key")
                                seen.add(token)
                                if isinstance(value, int):
                                    maximum = (
                                        max(maximum, value)
                                        if maximum is not None
                                        else value
                                    )
                        except Exception as error:
                            raise RuntimeError(
                                f"migration_row_conversion_invalid:{name}:{count}:{field}"
                            ) from error
        if count != table["rows"]:
            raise RuntimeError(f"migration_row_count_mismatch:{name}")
        maxima[name] = maximum
    return {"manifest": manifest, "deferred": deferred, "maxima": maxima}


def files_check(request: dict, package: Path) -> dict:
    """检查计划覆盖的目标、祖先目录及空间；不试写正式目录。"""
    from . import backup, restore

    root = tasks.mutation.INSTANCE_ROOT
    journal = tasks.Files(root, package.parent / "rollback")
    targets = {restore._environment_file(root): 0}
    for item in (package / "files").rglob("*"):
        if item.is_file():
            relative = item.relative_to(package / "files").as_posix()
            if backup.category_for(relative) in request["categories"]:
                targets[relative] = item.stat().st_size
    result, required = {}, 0
    for relative, size in targets.items():
        target = journal.target(relative)
        if target.exists() and not target.is_file():
            raise RuntimeError("migration_target_not_file:" + relative)
        parent = target.parent
        while not parent.exists():
            parent = parent.parent
        if not parent.is_dir() or not os.access(parent, os.W_OK):
            raise RuntimeError("migration_target_directory_unwritable:" + relative)
        if target.exists() and not os.access(target, os.W_OK):
            raise RuntimeError("migration_target_file_unwritable:" + relative)
        result[relative] = tasks.Files.digest(target)
        required += size + (target.stat().st_size if target.exists() else 0)
    if shutil.disk_usage(root).free < required:
        raise RuntimeError("migration_target_space_insufficient")
    return result


async def database_check(request: dict, package: Path | None = None) -> dict:
    """只读核验身份、结构及可判定权限，不创建目标库或保存最终回滚点。"""
    from zhenxun.configs.database import DatabaseConnection

    from . import data, target
    from . import data_guard as guard

    endpoint = DatabaseConnection.parse(
        request["db_url"], root=tasks.mutation.INSTANCE_ROOT
    )
    if endpoint.engine == "sqlite" and not Path(endpoint.database).exists():
        return {
            "identity": {
                "engine": "sqlite",
                "file": os.path.normcase(str(Path(endpoint.database).resolve())),
                "missing": True,
            },
            "structure": digest([]),
            "warnings": [],
            "deferred": ["目标 SQLite 尚不存在，切换时建立并复核身份与权限"],
        }
    client = await data.open_client(request["db_url"], "zx_preflight", read_only=True)
    try:
        identity = await guard.identity(client)
        existing = await guard.table_names(client)
        models = data._models()
        model_map = {data._table_key(model): model for model in models}
        validated = validate_data(package / "database", client) if package else None
        tables = (
            set(validated["manifest"]["tables"])
            if validated
            else set((request.get("preflight_database") or {}).get("tables", model_map))
        )
        selected = {
            guard.model_key(model_map[name], identity.get("schema"))
            if name in model_map
            else (
                tuple(name.rsplit(".", 1))
                if client.capabilities.dialect == "postgres" and "." in name
                else (identity.get("schema"), name)
            )
            for name in tables
        }
        empty = request["kind"] == "switch" or not request.get("snapshot_required")
        if empty:
            for schema, name in sorted(existing, key=str):
                if (
                    await client.execute_query(
                        "SELECT 1 FROM "
                        + guard.relation_sql(client, name, schema)
                        + " LIMIT 1"
                    )
                )[1]:
                    raise RuntimeError("migration_target_not_empty:" + name)
        structure, warnings = {}, []
        for schema, name in sorted(existing & selected, key=str):
            if client.capabilities.dialect == "sqlite":
                definition = await client.execute_query_dict(
                    "SELECT type,name,sql FROM sqlite_master WHERE tbl_name=?"
                    " ORDER BY type,name",
                    [name],
                )
            else:
                definition = await target.describe(client, schema, name)
            structure[target.key(schema, name)] = target.digest(definition)
            if client.capabilities.dialect == "postgres":
                for permission in ("SELECT", "INSERT", "UPDATE", "DELETE"):
                    allowed = await client.execute_query_dict(
                        "SELECT has_table_privilege(current_user,$1,$2) AS allowed",
                        [guard.relation_sql(client, name, schema), permission],
                    )
                    if not allowed[0]["allowed"]:
                        raise RuntimeError(
                            f"migration_target_privilege_required:{name}:{permission}"
                        )
        missing = selected - existing
        if client.capabilities.dialect == "mysql":
            await target._mysql_permissions(
                client,
                {name for _, name in selected},
                {name for _, name in missing},
                set(),
            )
        elif client.capabilities.dialect == "postgres":
            for schema in {schema for schema, _ in missing}:
                rows = await client.execute_query_dict(
                    "SELECT has_schema_privilege(current_user,$1,'CREATE') AS allowed",
                    [schema],
                )
                if not rows[0]["allowed"]:
                    raise RuntimeError(f"migration_schema_create_required:{schema}")
        for name in tables & model_map.keys():
            model = model_map[name]
            if guard.model_key(model, identity.get("schema")) not in existing:
                continue
            columns = ",".join(
                guard.quote(client, col)
                for col in model._meta.fields_db_projection.values()
            )
            await client.execute_query(
                f"SELECT {columns} FROM {guard.table_sql(client, model)} WHERE 1=0"
            )
            if validated:
                await data._reset_sequence(
                    client, model, validated["maxima"].get(name), read_only=True
                )
        keys = await guard.foreign_keys(client)
        if validated:
            await data._retained_references(
                client,
                keys,
                [model_map[name] for name in tables & model_map.keys()],
                validated["manifest"]["tables"],
                package / "database",
                identity.get("schema"),
            )
        return {
            "identity": identity,
            "structure": digest([structure, sorted(existing, key=str), keys]),
            "tables": sorted(tables),
            "warnings": warnings,
            "deferred": [
                "停写后复核内容、外键影响、实际写入和锁权限",
                "插件动态模型与 ORM 升级在受控启动中核验",
            ]
            + (
                ["待加载模型：" + name for name in validated["deferred"]]
                if validated
                else []
            ),
        }
    finally:
        await client.close()


async def orm_check(request: dict, package: Path) -> dict:
    """只读比对静态 ORM 迁移头与外部库版本，不加载插件或执行迁移脚本。"""
    import ast

    from dotenv import dotenv_values
    from sqlalchemy.engine import make_url

    from zhenxun.services.nonebot_store import storage

    from . import data, restore
    from . import data_guard as guard

    child = storage.pending_transaction() or {}
    if not child.get("database_migration_possible"):
        return {"state": "not_required"}
    if child.get("parent_operation_id") != request["operation_id"]:
        raise RuntimeError("migration_dependency_owner_changed")
    root = tasks.mutation.INSTANCE_ROOT
    config = restore._environment_file(root)
    candidate = package / "files" / config
    if "configuration" not in request["categories"] or not candidate.is_file():
        candidate = root / config
    values = {
        str(key).lower(): value for key, value in dotenv_values(candidate).items()
    }
    values.update({key.lower(): value for key, value in os.environ.items()})
    unknown = {"state": "deferred", "reason": "ORM 动态模型、绑定或脚本需受控启动核验"}
    if any(
        values.get(key)
        for key in (
            "sqlalchemy_binds",
            "alembic_config",
            "alembic_context",
            "alembic_script_location",
            "alembic_version_locations",
        )
    ):
        return unknown
    raw = values.get("sqlalchemy_database_url")
    if not raw:
        return unknown
    url = make_url(raw)
    dialect = url.get_backend_name()
    if dialect not in {"postgresql", "mysql"}:
        return {
            "state": "deferred",
            "reason": "SQLite ORM 使用父任务或独立附属库回滚点",
        }
    directories = []
    generation = storage.generation_path(child["generation"])
    for plugin in child.get("target_manifest", {}).get("plugins", {}).values():
        module = str(plugin.get("module_name") or "")
        if module and all(part.isidentifier() for part in module.split(".")):
            directories.append(generation.joinpath(*module.split(".")) / "migrations")
    sources = {
        item.name: item / "migrations"
        for item in (root / "zhenxun/plugins").glob("*")
        if item.is_dir()
    }
    if "plugins" in request["categories"]:
        sources.update(
            {
                item.name: item / "migrations"
                for item in (package / "files/zhenxun/plugins").glob("*")
                if item.is_dir()
            }
        )
    directories.extend(sources.values())
    revisions, parents = set(), set()
    for directory in directories:
        for script in directory.rglob("*.py"):
            fields = {}
            tree = ast.parse(script.read_text("utf-8-sig"))
            for node in tree.body:
                targets = (
                    node.targets
                    if isinstance(node, ast.Assign)
                    else [node.target]
                    if isinstance(node, ast.AnnAssign)
                    else []
                )
                for target in targets:
                    if isinstance(target, ast.Name) and target.id in {
                        "revision",
                        "down_revision",
                        "depends_on",
                    }:
                        try:
                            fields[target.id] = ast.literal_eval(node.value)
                        except (ValueError, TypeError):
                            return unknown
            if not isinstance(fields.get("revision"), str):
                continue
            revisions.add(fields["revision"])
            for field in ("down_revision", "depends_on"):
                value = fields.get(field)
                if isinstance(value, str):
                    parents.add(value)
                elif isinstance(value, list | tuple):
                    parents.update(value)
    heads = revisions - parents
    if not heads:
        return unknown
    connection = url.set(
        drivername="postgres" if dialect == "postgresql" else "mysql"
    ).render_as_string(hide_password=False)
    client = await data.open_client(connection, "zx_orm_preflight", read_only=True)
    try:
        identity = await guard.identity(client)
        existing = await guard.table_names(client)
        table = (identity.get("schema"), "alembic_version")
        current = set()
        if table in existing:
            current = {
                row["version_num"]
                for row in await client.execute_query_dict(
                    "SELECT version_num FROM "
                    + guard.relation_sql(client, table[1], table[0])
                )
            }
        if current - revisions:
            return unknown
        if not heads <= current:
            raise RuntimeError(
                "external_database_manual_migration_required:插件 ORM 需独立维护升级；"
                "原实例尚未被迁移修改。待应用版本："
                + ",".join(sorted(heads - current))
                + "；备份并完成控制台维护后重试原任务"
            )
        return {
            "state": "revisions_ready",
            "heads": sorted(heads),
            "reason": "静态迁移版本已就绪，动态模型差异仍须受控验证",
        }
    finally:
        await client.close()
