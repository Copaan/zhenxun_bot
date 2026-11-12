"""核心业务数据合并与持久来源映射"""

from hashlib import sha256
import json
from pathlib import Path
import re
from uuid import uuid4

from . import data
from . import data_guard as guard
from .merge_rules import (
    HISTORY,
    KEYS,
    REFERENCES,
    _rows,
    validate_package,
)
from .merge_rules import (
    validate_rules as validate_rules,
)

INTERNAL_TABLE = "migration_import_records"
LOCAL_ID = sha256(b"zhenxun:migration:local-identity").hexdigest()


def _json(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, default=data._encode)


def validate_origins(directory: Path):
    """逐行校验来源元数据，不访问数据库或修改迁移包"""
    try:
        manifest = json.loads((directory / data.MANIFEST).read_text(encoding="utf-8"))
        if not isinstance(manifest, dict) or not isinstance(
            manifest.get("tables"), dict
        ):
            raise ValueError("manifest_shape")
        source = manifest.get("source_identity")
        if source is not None and (
            not isinstance(source, str)
            or not 1 <= len(source) <= 255
            or re.fullmatch(r"[A-Za-z0-9_.:-]+", source) is None
        ):
            raise ValueError("source_identity")
        filename = manifest.get("origins_file")
        if filename not in {None, "origins.jsonl"}:
            raise ValueError("origins_file")
        path = directory / "origins.jsonl"
        if not path.exists():
            if filename:
                raise ValueError("origins_file_missing")
            return {"records": 0}
        seen = {}
        count = 0
        for row in _rows(path):
            if not isinstance(row, dict) or set(row) != {
                "source_identity",
                "table_name",
                "source_pk",
                "target_pk",
            }:
                raise ValueError("origin_shape")
            identity = row["source_identity"]
            table = row["table_name"]
            if (
                not isinstance(identity, str)
                or not 1 <= len(identity) <= 64
                or re.fullmatch(r"[A-Za-z0-9_.:-]+", identity) is None
                or not isinstance(table, str)
                or len(table) > 255
                or table not in manifest["tables"]
                or table == INTERNAL_TABLE
            ):
                raise ValueError("origin_identity_or_table")
            decoded = []
            for field in ("source_pk", "target_pk"):
                value = row[field]
                if not isinstance(value, str) or len(value) > 4096:
                    raise ValueError("origin_pk_encoding")
                value = json.loads(value)
                if type(value) not in {str, int}:
                    raise ValueError("origin_pk_type")
                if isinstance(value, str) and (not value or len(value) > 1024):
                    raise ValueError("origin_pk_length")
                if isinstance(value, int) and not -(2**63) <= value < 2**63:
                    raise ValueError("origin_pk_range")
                decoded.append(_json(value))
            key = (identity, table, decoded[0])
            if key in seen and seen[key] != decoded[1]:
                raise ValueError("origin_mapping_conflict")
            seen[key] = decoded[1]
            count += 1
        return {"records": count}
    except (OSError, ValueError, TypeError, KeyError) as error:
        raise data.DataError("migration_invalid_origin_metadata") from error


async def _mapping_rows(db):
    return await db.execute_query_dict(
        f"SELECT * FROM {guard.quote(db, INTERNAL_TABLE)}"
    )


async def _save_mapping(db, source, table, old_pk, new_pk):
    identifier = (
        LOCAL_ID
        if table == "__local__"
        else sha256(_json([source, table, old_pk]).encode()).hexdigest()
    )
    values = [identifier, source, table, _json(old_pk), _json(new_pk)]
    await db.execute_query(
        f"INSERT INTO {guard.quote(db, INTERNAL_TABLE)} "
        "(id,source_identity,table_name,source_pk,target_pk) "
        f"VALUES ({','.join(data._parameters(db, 5))})",
        values,
    )


async def export_origins(db, directory):
    """输出持久来源和当前实例身份"""
    tables = await guard.table_names(db)
    exists = any(name == INTERNAL_TABLE for _, name in tables)
    exported = {
        data._table_key(model)
        for model in data._models()
        if model._meta.db_table != INTERNAL_TABLE
    }
    local = None
    if exists:
        local_rows = await db.execute_query_dict(
            f"SELECT source_identity FROM {guard.quote(db, INTERNAL_TABLE)} "
            f"WHERE id={data._parameters(db, 1)[0]}",
            [LOCAL_ID],
        )
        if len(local_rows) > 1:
            raise data.DataError("migration_multiple_local_source_identities")
        local = local_rows[0]["source_identity"] if local_rows else None
    with (directory / "origins.jsonl").open("w", encoding="utf-8") as file:
        last = ""
        while exists:
            records = await db.execute_query_dict(
                f"SELECT * FROM {guard.quote(db, INTERNAL_TABLE)} "
                f"WHERE id>{data._parameters(db, 1)[0]} ORDER BY id LIMIT 1000",
                [last],
            )
            for row in records:
                if row["table_name"] not in exported:
                    continue
                file.write(
                    _json(
                        {
                            k: row[k]
                            for k in (
                                "source_identity",
                                "table_name",
                                "source_pk",
                                "target_pk",
                            )
                        }
                    )
                    + "\n"
                )
            if len(records) < 1000:
                break
            last = records[-1]["id"]
    return local


async def ensure_local_identity(client):
    """在表结构就绪后，为本数据库初始化稳定的来源身份"""
    async with guard.import_transaction(client) as db:
        await ensure_identity_in_transaction(db)


async def ensure_identity_in_transaction(db):
    """在调用者写事务内初始化数据库来源身份"""
    if not any(name == INTERNAL_TABLE for _, name in await guard.table_names(db)):
        return
    if db.capabilities.dialect == "mysql":
        engines = await db.execute_query_dict(
            "SELECT ENGINE FROM information_schema.tables "
            "WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME=%s",
            [INTERNAL_TABLE],
        )
        if not engines or str(engines[0]["ENGINE"]).lower() != "innodb":
            raise data.DataError("migration_nontransactional_table:" + INTERNAL_TABLE)
    rows = await db.execute_query_dict(
        f"SELECT id FROM {guard.quote(db, INTERNAL_TABLE)} "
        f"WHERE id={data._parameters(db, 1)[0]}",
        [LOCAL_ID],
    )
    if not rows:
        values = [
            LOCAL_ID,
            uuid4().hex,
            "__local__",
            _json("identity"),
            _json("identity"),
        ]
        dialect = db.capabilities.dialect
        prefix = "INSERT OR IGNORE" if dialect == "sqlite" else "INSERT"
        suffix = (
            " ON DUPLICATE KEY UPDATE id=id"
            if dialect == "mysql"
            else " ON CONFLICT(id) DO NOTHING"
            if dialect == "postgres"
            else ""
        )
        await db.execute_query(
            f"{prefix} INTO {guard.quote(db, INTERNAL_TABLE)} "
            "(id,source_identity,table_name,source_pk,target_pk) "
            f"VALUES ({','.join(data._parameters(db, 5))}){suffix}",
            values,
        )


async def record_replacement(
    db, directory, tables, *, source_binding=None, archive_sha256=None
):
    """为精确导入的记录保存原始来源，保留后续跨实例去重能力"""
    manifest = json.loads((directory / data.MANIFEST).read_text(encoding="utf-8"))
    source = source_binding or manifest.get("source_identity") or archive_sha256
    if not source:
        return
    source = sha256(source.encode()).hexdigest()
    aliases = {}
    if (directory / "origins.jsonl").exists():
        for row in _rows(directory / "origins.jsonl"):
            aliases.setdefault(
                (row["table_name"], _json(json.loads(row["target_pk"]))), []
            ).append((row["source_identity"], json.loads(row["source_pk"])))
    known = set()
    for model in tables:
        table = data._table_key(model)
        await db.execute_query(
            f"DELETE FROM {guard.quote(db, INTERNAL_TABLE)} "
            f"WHERE table_name={data._parameters(db, 1)[0]}",
            [table],
        )
        position = manifest["tables"][table]["fields"].index(model._meta.pk_attr)
        for row in _rows(directory / f"{table}.jsonl"):
            pk = row[position]
            for origin, old in [(source, pk), *aliases.get((table, _json(pk)), [])]:
                key = (origin, table, _json(old))
                if key not in known:
                    await _save_mapping(db, origin, table, old, pk)
                    known.add(key)


async def import_merge(
    directory,
    client,
    *,
    restore_mode,
    source_binding=None,
    archive_sha256=None,
    included_tables=None,
    excluded_tables=None,
    expected_identity=None,
    expected_revision=None,
    expected_sqlite_fingerprint=None,
    outcome=None,
    before_commit=None,
    progress=None,
    **unused,
):
    """按业务键合并，在业务事务中写入来源映射"""
    validate_package(directory, included_tables, excluded_tables)
    manifest = json.loads((directory / data.MANIFEST).read_text(encoding="utf-8"))
    source = source_binding or manifest.get("source_identity") or archive_sha256
    if not source:
        digest = sha256((directory / data.MANIFEST).read_bytes())
        for table in sorted(manifest["tables"]):
            with (directory / f"{table}.jsonl").open("rb") as file:
                while chunk := file.read(1024 * 1024):
                    digest.update(chunk)
        source = digest.hexdigest()
    if not isinstance(source, str) or not source or len(source) > 255:
        raise data.DataError("migration_invalid_source_identity")
    source = sha256(source.encode()).hexdigest()
    models = {
        m.__name__: m
        for m in data._models()
        if data._table_key(m) in manifest["tables"]
        and data._table_key(m) not in (excluded_tables or ())
        and (included_tables is None or data._table_key(m) in included_tables)
    }
    report = {
        "tables": {},
        "changes": {},
        "skipped_tables": {},
        "sequences": [],
        "dropped_fields": {},
        "defaulted_fields": {},
        "restore_mode": restore_mode,
    }
    aliases = {}
    origins_path = directory / "origins.jsonl"
    if origins_path.exists():
        for row in _rows(origins_path):
            aliases.setdefault(
                (row["table_name"], _json(json.loads(row["target_pk"]))), []
            ).append((row["source_identity"], json.loads(row["source_pk"])))
    async with guard.import_transaction(client, outcome=outcome) as db:
        report["database_identity"] = await guard.require_same(db, expected_identity)
        foreign_keys = await guard.foreign_keys(db)
        await guard.lock_tables(db, list(models.values()), foreign_keys)
        schema = report["database_identity"].get("schema")
        names = {guard.model_key(model, schema) for model in models.values()}
        invalid_before = await guard.invalid_relations(db, foreign_keys, names)
        if (
            expected_revision is not None
            and await data.database_revision(db, expected_revision) != expected_revision
        ):
            raise data.DataError("migration_target_content_changed")
        if expected_sqlite_fingerprint and expected_sqlite_fingerprint not in {
            await data.sqlite_revision(db),
            await data.sqlite_revision(db, legacy=True),
        }:
            raise data.DataError("migration_target_content_changed")
        await ensure_identity_in_transaction(db)
        known = {
            (r["source_identity"], r["table_name"], r["source_pk"]): json.loads(
                r["target_pk"]
            )
            for r in await _mapping_rows(db)
        }
        mapping, completed, processing = {}, set(), set()
        imported_tags = set()
        imported_runs = set()

        async def process(name):
            if name in completed:
                return
            if name in processing:
                raise data.DataError("migration_merge_reference_cycle:" + name)
            if name not in models:
                raise data.DataError("migration_merge_required_parent:" + name)
            processing.add(name)
            model = models[name]
            meta, table = model._meta, data._table_key(model)
            if not manifest["tables"][table]["rows"]:
                report["tables"][table] = (
                    await db.execute_query_dict(
                        f"SELECT COUNT(*) AS n FROM {guard.table_sql(db, model)}"
                    )
                )[0]["n"]
                report["changes"][table] = {"inserted": 0, "updated": 0, "retained": 0}
                completed.add(name)
                processing.remove(name)
                return
            references = dict(REFERENCES.get(name, {}))
            for field in (*meta.fk_fields, *meta.o2o_fields):
                relation = meta.fields_map[field]
                references[relation.source_field] = relation.related_model.__name__
            if name in {"AIPersona", "AIChatGroup"} and "AIKnowledgeBase" in models:
                await process("AIKnowledgeBase")
            if name == "ScheduledJob" and "GroupTag" in models:
                await process("GroupTag")
            if name in {"UserConsole", "UserPropsLog"} and "GoodsInfo" in models:
                await process("GoodsInfo")
            if name == "AIAgentCall" and "AIAgentRun" in models:
                await process("AIAgentRun")
            for parent in set(references.values()):
                await process(parent)
            executor = db.executor_class(model=model, db=db)
            to_db = data.column_converters(executor)
            columns = executor.regular_columns_all
            pk = meta.pk_attr
            history = name in HISTORY
            selected = guard.quote(db, meta.db_pk_column) if history else "*"
            target_rows = await db.execute_query_dict(
                f"SELECT {selected} FROM {guard.table_sql(db, model)}"
            )
            targets = [
                {
                    field: meta.fields_map[field].to_python_value(row[column])
                    for field, column in meta.fields_db_projection.items()
                    if not history or field == pk
                }
                for row in target_rows
            ]
            maximum = (
                max((r[pk] for r in targets), default=0) if meta.pk.generated else None
            )
            maximum_uid = (
                max((r["uid"] for r in targets), default=0)
                if name == "UserConsole"
                else None
            )
            unique = [
                (f,)
                for f in columns
                if meta.fields_map[f].unique and f not in {pk, "uid"}
            ]
            if name == "GoodsInfo":
                unique.append(("uuid",))
            unique += [
                tuple(
                    meta.fields_map[f].source_field
                    if f in meta.fk_fields | meta.o2o_fields
                    else f
                    for f in constraint
                )
                for constraint in meta.unique_together or ()
            ]
            keys = KEYS.get(name)
            constraints = ([keys] if keys else []) + unique
            indexes = [{} for _ in constraints]
            by_pk = {row[pk]: row for row in targets}

            def index_row(row, remove=False):
                for constraint, index in zip(constraints, indexes):
                    value = _json([row.get(k) for k in constraint])
                    bucket = index.setdefault(value, {})
                    if remove:
                        bucket.pop(row[pk], None)
                    else:
                        bucket[row[pk]] = row

            for target in targets:
                index_row(target)
            counts = {"inserted": 0, "updated": 0, "retained": 0}
            seen = set()
            seen_business = set()
            fields = manifest["tables"][table]["fields"]
            with (directory / f"{table}.jsonl").open(encoding="utf-8") as file:
                for line in file:
                    raw = dict(zip(fields, json.loads(line), strict=True))
                    old = raw[pk]
                    if _json(old) in seen:
                        raise data.DataError("migration_duplicate_source_pk:" + table)
                    seen.add(_json(old))
                    row = {
                        c: data._decoder(meta.fields_map[c])(raw[c])
                        if c in raw
                        else data._default(model, c)()
                        for c in columns
                    }
                    source_goods_uuid = row.get("uuid") if name == "GoodsInfo" else None
                    source_principal = (
                        row.get("principal_id")
                        if name == "BusinessIdentityLink"
                        else None
                    )
                    if source_principal:
                        row["principal_id"] = mapping.get(
                            ("Principal", _json(source_principal)), source_principal
                        )
                    if name == "UserConsole" and row["props"]:
                        props = {}
                        for goods_uuid, quantity in row["props"].items():
                            translated = mapping.get(
                                ("GoodsUUID", _json(goods_uuid)), goods_uuid
                            )
                            if translated in props:
                                raise data.DataError(
                                    "migration_merge_inventory_identity_conflict"
                                )
                            props[translated] = quantity
                        row["props"] = props
                    if name == "UserPropsLog":
                        row["uuid"] = mapping.get(
                            ("GoodsUUID", _json(row["uuid"])), row["uuid"]
                        )
                    if name == "GroupTag":
                        imported_tags.add(row["name"])
                    if name == "AIAgentRun":
                        imported_runs.add(row["run_id"])
                    if (
                        name == "AIAgentCall"
                        and row["run_id"]
                        and row["run_id"] not in imported_runs
                    ):
                        raise data.DataError("migration_merge_unresolved_agent_run")
                    if name == "ScheduledJob" and row["target_type"] == "TAG":
                        if row["target_identifier"] not in imported_tags:
                            raise data.DataError(
                                "migration_merge_unresolved_tag_target"
                            )
                    for field, parent in references.items():
                        value = row[field]
                        if value is None or (name == "AIMemoryCursor" and value == 0):
                            continue
                        if (parent, _json(value)) not in mapping:
                            raise data.DataError(
                                f"migration_merge_unresolved_reference:{table}.{field}:{value}"
                            )
                        row[field] = mapping[parent, _json(value)]
                    if name in {"AIPersona", "AIChatGroup"} and row.get(
                        "knowledge_bases"
                    ):
                        try:
                            row["knowledge_bases"] = [
                                mapping["AIKnowledgeBase", _json(k)]
                                for k in row["knowledge_bases"]
                            ]
                        except KeyError as error:
                            raise data.DataError(
                                "migration_merge_unresolved_knowledge_base"
                            ) from error
                    if name == "AIPersona" and row.get("subagents"):

                        def rewrite_config(value):
                            if isinstance(value, list):
                                return [rewrite_config(item) for item in value]
                            if not isinstance(value, dict):
                                return value
                            result = {}
                            for key, item in value.items():
                                if key == "knowledge_bases" and isinstance(item, list):
                                    try:
                                        item = [
                                            mapping["AIKnowledgeBase", _json(k)]
                                            for k in item
                                        ]
                                    except KeyError as error:
                                        raise data.DataError(
                                            "migration_merge_unresolved_knowledge_base"
                                        ) from error
                                else:
                                    item = rewrite_config(item)
                                result[key] = item
                            return result

                        row["subagents"] = rewrite_config(row["subagents"])
                    if keys:
                        identity_key = _json([row[k] for k in keys])
                        if identity_key in seen_business:
                            raise data.DataError(
                                "migration_merge_ambiguous_source:" + table
                            )
                        seen_business.add(identity_key)
                    identities = [(source, old), *aliases.get((table, _json(old)), [])]
                    origin_ids = {
                        known[s, table, _json(k)]
                        for s, k in identities
                        if (s, table, _json(k)) in known
                    }
                    candidates = [by_pk[key] for key in origin_ids if key in by_pk]
                    for constraint, index in zip(constraints, indexes):
                        if constraint == keys or all(
                            row.get(k) is not None for k in constraint
                        ):
                            candidates.extend(
                                index.get(
                                    _json([row.get(k) for k in constraint]), {}
                                ).values()
                            )
                    matches = {r[pk]: r for r in candidates}
                    if len(matches) > 1 or (
                        origin_ids and not origin_ids <= set(matches)
                    ):
                        raise data.DataError(
                            "migration_merge_identity_conflict:" + table
                        )
                    existing = next(iter(matches.values()), None)
                    if existing:
                        if keys and any(existing[k] != row[k] for k in keys):
                            raise data.DataError(
                                (
                                    "migration_merge_source_identity_changed:"
                                    if origin_ids
                                    else "migration_merge_unique_identity_conflict:"
                                )
                                + table
                            )
                        for field in ("platform", "owner_id", "bot_id"):
                            if (
                                not history
                                and field in row
                                and existing.get(field) != row[field]
                            ):
                                raise data.DataError(
                                    f"migration_merge_scope_conflict:{table}.{field}"
                                )
                        if name == "BusinessIdentityLink" and any(
                            existing[f] != row[f]
                            for f in ("account_id", "original_account_id")
                        ):
                            raise data.DataError("migration_merge_binding_conflict")
                        if name == "AIAgentRun" and any(
                            existing[field] != row[field]
                            for field in ("source", "session_id", "user_id")
                        ):
                            raise data.DataError(
                                "migration_merge_run_identity_conflict"
                            )
                        if name == "BusinessIdentityLink" and existing["principal_id"]:
                            principal_key = ("Principal", _json(source_principal))
                            if (
                                source_principal
                                and principal_key in mapping
                                and mapping[principal_key] != existing["principal_id"]
                            ):
                                raise data.DataError(
                                    "migration_merge_principal_binding_conflict"
                                )
                            row["principal_id"] = existing["principal_id"]
                        row[pk] = existing[pk]
                        if name == "UserConsole":
                            row["uid"] = existing["uid"]
                        if name == "GoodsInfo" and existing["uuid"]:
                            row["uuid"] = existing["uuid"]
                        if restore_mode == "merge_skip":
                            counts["retained"] += 1
                        else:
                            update = [c for c in columns if c != pk]
                            params = data._parameters(db, len(update) + 1)
                            sql = ",".join(
                                f"{guard.quote(db, meta.fields_db_projection[c])}"
                                f"={params[i]}"
                                for i, c in enumerate(update)
                            )
                            await db.execute_query(
                                f"UPDATE {guard.table_sql(db, model)} SET {sql} "
                                f"WHERE {guard.quote(db, meta.db_pk_column)}"
                                f"={params[-1]}",
                                [to_db[c](row[c], model) for c in update] + [row[pk]],
                            )
                            index_row(existing, remove=True)
                            if not history:
                                existing.update(row)
                            index_row(existing)
                            counts["updated"] += 1
                    else:
                        if meta.pk.generated:
                            maximum += 1
                            row[pk] = maximum
                        elif row[pk] in by_pk:
                            raise data.DataError(
                                "migration_merge_primary_key_collision:" + table
                            )
                        if name == "UserConsole":
                            maximum_uid += 1
                            row["uid"] = maximum_uid
                        await db.execute_query(
                            executor.insert_query_all,
                            [to_db[c](row[c], model) for c in columns],
                        )
                        target_row = {pk: row[pk]} if history else row
                        targets.append(target_row)
                        by_pk[row[pk]] = target_row
                        index_row(target_row)
                        counts["inserted"] += 1
                    mapping[name, _json(old)] = row[pk]
                    if source_principal:
                        mapping["Principal", _json(source_principal)] = row[
                            "principal_id"
                        ]
                    if source_goods_uuid:
                        goods_key = ("GoodsUUID", _json(source_goods_uuid))
                        if goods_key in mapping and mapping[goods_key] != row["uuid"]:
                            raise data.DataError("migration_merge_goods_uuid_conflict")
                        mapping[goods_key] = row["uuid"]
                    for origin, origin_pk in identities:
                        key = (origin, table, _json(origin_pk))
                        if key not in known:
                            await _save_mapping(db, origin, table, origin_pk, row[pk])
                            known[key] = row[pk]
            if len(seen) != manifest["tables"][table]["rows"]:
                raise data.DataError("migration_row_count_mismatch:" + table)
            report["tables"][table] = len(targets)
            report["changes"][table] = counts
            if sequence := await data._reset_sequence(db, model):
                report["sequences"].append(sequence)
            if progress:
                progress(table, len(seen))
            completed.add(name)
            processing.remove(name)

        for name in models:
            await process(name)
        if await guard.invalid_relations(db, foreign_keys, names) - invalid_before:
            raise data.DataError("migration_foreign_key_conflict")
        report["content_revision"] = await data.database_revision(
            db, {t: {} for t in report["tables"]}
        )
        if db.capabilities.dialect == "sqlite":
            report["sqlite_fingerprint"] = await data.sqlite_revision(db)
        if before_commit:
            await before_commit(db, report)
    return report
