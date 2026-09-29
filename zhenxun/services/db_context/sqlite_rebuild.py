"""Transactional SQLite unique-constraint replacement preserving original DDL."""

from __future__ import annotations

import uuid

from zhenxun.migration.sql_tokens import replace_sqlite_unique as replace_unique_sql


def quote(value):
    return '"' + value.replace('"', '""') + '"'


async def replace_unique_constraint(
    connection, table, *, obsolete, desired, constraint
):
    """Change one inline UNIQUE while retaining indexes, triggers and sequence."""
    async with connection.acquire_connection() as raw:
        foreign_keys = (await raw.execute_fetchall("PRAGMA foreign_keys"))[0][0]
        legacy_alter = (await raw.execute_fetchall("PRAGMA legacy_alter_table"))[0][0]
        await raw.execute("PRAGMA foreign_keys=OFF")
        await raw.execute("PRAGMA legacy_alter_table=ON")
        try:
            await raw.execute("BEGIN IMMEDIATE")
            rows = await raw.execute_fetchall(
                "SELECT sql FROM sqlite_schema WHERE type='table' AND name=?", (table,)
            )
            if not rows:
                await raw.rollback()
                return
            columns = await raw.execute_fetchall(f"PRAGMA table_xinfo({quote(table)})")
            names = [row[1] for row in columns if row[6] == 0]
            if not set(desired) <= set(names):
                raise RuntimeError("group_plugin_settings_scope_columns_missing")
            objects = await raw.execute_fetchall(
                "SELECT type,name,sql FROM sqlite_schema WHERE "
                "tbl_name=? AND type IN ('index','trigger') AND "
                "sql IS NOT NULL ORDER BY type,name",
                (table,),
            )
            obsolete_indexes = set()
            for index in await raw.execute_fetchall(
                f"PRAGMA index_list({quote(table)})"
            ):
                if not index[2] or index[4]:
                    continue
                fields = await raw.execute_fetchall(
                    f"PRAGMA index_info({quote(index[1])})"
                )
                if tuple(field[2] for field in fields) in {*obsolete, desired}:
                    obsolete_indexes.add(index[1])
            sequence = []
            if await raw.execute_fetchall(
                "SELECT 1 FROM sqlite_schema WHERE name='sqlite_sequence'"
            ):
                sequence = await raw.execute_fetchall(
                    "SELECT seq FROM sqlite_sequence WHERE name=?", (table,)
                )
            temporary = table + "__scope_" + uuid.uuid4().hex
            await raw.execute(
                replace_unique_sql(rows[0][0], temporary, obsolete, desired, constraint)
            )
            selected = ",".join(map(quote, names))
            await raw.execute(
                f"INSERT INTO {quote(temporary)} ({selected}) SELECT "
                f"{selected} FROM {quote(table)}"
            )
            await raw.execute(f"DROP TABLE {quote(table)}")
            await raw.execute(
                f"ALTER TABLE {quote(temporary)} RENAME TO {quote(table)}"
            )
            for object_type, name, sql in objects:
                if object_type == "index" and name in obsolete_indexes:
                    continue
                await raw.execute(sql)
            if sequence:
                await raw.execute(
                    "UPDATE sqlite_sequence SET seq=? WHERE name=?",
                    (sequence[0][0], table),
                )
            if await raw.execute_fetchall("PRAGMA foreign_key_check"):
                raise RuntimeError("sqlite_schema_reference_invalid")
            await raw.commit()
        except BaseException:
            await raw.rollback()
            raise
        finally:
            await raw.execute(f"PRAGMA legacy_alter_table={int(legacy_alter)}")
            await raw.execute(f"PRAGMA foreign_keys={int(foreign_keys)}")
