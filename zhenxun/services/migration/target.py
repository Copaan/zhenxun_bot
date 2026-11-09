"""迁移目标的结构归属、写入证据与外部数据库补偿。"""

from __future__ import annotations

from contextlib import asynccontextmanager
from copy import deepcopy
from hashlib import sha256
import json
import re

from . import data_guard as guard


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
    rows = await db.execute_query_dict(f"SELECT * FROM {relation(db, item)}")
    return digest(sorted(digest(row) for row in rows))


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


async def _mysql_permissions(
    db, names: set[str], created: set[str], references: set[str]
) -> None:
    """核验目标范围内的实际授权，展开当前已启用的角色。"""
    database = (await guard.identity(db))["db_name"]
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
    grants = [next(iter(row.values())) for row in await db.execute_query_dict(sql)]
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
                privileges[name].update(match[1].split(", "))
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
    empty_target = request["kind"] == "switch" or not request.get("snapshot_required")
    if empty_target:
        for schema, name in existing:
            if (
                await db.execute_query(
                    f"SELECT 1 FROM {guard.relation_sql(db, name, schema)} LIMIT 1"
                )
            )[1]:
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
        db = client._in_transaction().connection
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
