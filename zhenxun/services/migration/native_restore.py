"""Engine-specific native restore scripts and transaction boundaries."""

from __future__ import annotations

from itertools import pairwise

from .archive import CHUNK, Limits
from .errors import MigrationError
from .sql_tokens import mysql_sql, tokens


def mysql_restore_script(source, destination, *, database, source_database, checkpoint):
    """Rewrite schema statements while streaming large INSERT payloads unchanged."""
    required = {"SELECT", "DROP"}
    delimiter = ";"
    pending = []
    with (
        source.open("r", encoding="utf-8", newline="") as reader,
        destination.open("w", encoding="utf-8", newline="\n") as writer,
    ):
        while line := reader.readline(CHUNK):
            checkpoint()
            if not pending and line.lstrip().upper().startswith(
                ("INSERT INTO ", "REPLACE INTO ")
            ):
                required.add("INSERT")
                writer.write(line)
                while not line.endswith("\n"):
                    line = reader.readline(CHUNK)
                    if not line:
                        break
                    checkpoint()
                    writer.write(line)
                continue
            if not pending and line.strip().upper().startswith("DELIMITER "):
                delimiter = line.strip().split(None, 1)[1]
                writer.write(line)
                continue
            if not pending and (not line.strip() or line.lstrip().startswith("--")):
                writer.write(line)
                continue
            pending.append(line)
            if (
                len(line) == CHUNK and not line.endswith("\n")
            ) or not line.rstrip().endswith(delimiter):
                continue
            statement = "".join(pending)
            try:
                parsed = list(tokens(statement))
            except MigrationError:
                continue
            rewritten = mysql_sql(
                statement,
                source_database=source_database,
                target_database=database,
                disable_events=True,
            )
            words = [t.text.upper() for t in parsed if t.kind == "word"]
            if words:
                command = words[0]
                if command == "LOCK":
                    required.add("LOCK TABLES")
                if command == "ALTER":
                    required.add("ALTER")
                if command == "CREATE":
                    kinds = {
                        "TABLE": "CREATE",
                        "VIEW": "CREATE VIEW",
                        "TRIGGER": "TRIGGER",
                        "EVENT": "EVENT",
                        "PROCEDURE": "CREATE ROUTINE",
                        "FUNCTION": "CREATE ROUTINE",
                    }
                    kind = next((word for word in words[1:] if word in kinds), None)
                    if kind:
                        required.add(kinds[kind])
                    if kind == "TABLE" and "REFERENCES" in words:
                        required.add("REFERENCES")
            writer.write(rewritten)
            pending.clear()
        if pending:
            writer.write(
                mysql_sql(
                    "".join(pending),
                    source_database=source_database,
                    target_database=database,
                    disable_events=True,
                )
            )
    return required


async def import_mysql(client, payload, *, source_database):
    """Import an already checked phase without deleting existing objects."""
    rewritten = client.directory / (payload.stem + "-import.sql")
    mysql_restore_script(
        payload,
        rewritten,
        database=client.endpoint.database,
        source_database=source_database,
        checkpoint=client.check,
    )
    await client.run(
        "mysql",
        [
            *client.connection,
            "--binary-mode",
            "--batch",
            f"--database={client.endpoint.database}",
        ],
        input_path=rewritten,
        maximum=Limits().expanded,
    )


async def prepare_mysql_phases(candidate, directory, expected_revision):
    """Independently verify resumable object/data boundaries on the candidate."""
    from .archive import file_hash

    phases = []
    for name, flags in (
        (
            "structure",
            ["--no-data", "--skip-triggers", "--skip-routines", "--skip-events"],
        ),
        (
            "data",
            ["--no-create-info", "--skip-triggers", "--skip-routines", "--skip-events"],
        ),
        (
            "objects",
            ["--no-create-info", "--no-data", "--triggers", "--routines", "--events"],
        ),
    ):
        path = directory / f"mysql-{name}.sql"
        await candidate.run(
            "mysqldump",
            [
                *candidate.connection,
                "--single-transaction",
                "--no-tablespaces",
                "--set-gtid-purged=OFF",
                "--hex-blob",
                "--skip-add-locks",
                *flags,
                candidate.endpoint.database,
            ],
            output_path=path,
            maximum=Limits().expanded,
        )
        phases.append(
            {
                "name": name,
                "payload": path.name,
                "sha256": file_hash(path, candidate.check),
            }
        )
    for item in phases:
        path = directory / item["payload"]
        if item["name"] == "structure":
            await candidate.restore(
                path, confirmed_tables=[], source_database=candidate.endpoint.database
            )
        else:
            await import_mysql(
                candidate, path, source_database=candidate.endpoint.database
            )
        observed = await candidate.inspect()
        item.update(
            revision=observed["revision"],
            objects=observed.get("objects", []),
            events=observed.get("events", []),
        )
    if phases[-1]["revision"] != expected_revision:
        raise MigrationError("migration_database_candidate_mismatch")
    return phases


async def apply_mysql_phases(target, candidate, directory, plan, journal, before):
    """Journal only states independently reproduced on the isolated candidate."""
    from zhenxun.utils.atomic_json import write_json_locked

    from .archive import file_hash
    from .native_replacement import revision_matches, verify_prepared_identity
    from .paths import contained_path

    receipt = {
        "state": "applying",
        "target": plan["target"],
        "before_revision": before["revision"],
        "operations": [],
    }
    expected_events = plan.get("events_before")
    for item in plan["mysql_phases"]:
        path = contained_path(directory, item["payload"], regular=True)
        if file_hash(path, target.check) != item["sha256"]:
            raise MigrationError("migration_database_payload_changed")
        await verify_prepared_identity(target, candidate, plan)
        await target.check_restore_privileges(
            tables=set(plan["tables_before"]) | set(plan["tables_after"])
        )
        observed = await target.inspect()
        expected = receipt.get("confirmed_revision", before["revision"])
        if not revision_matches(observed, expected, expected_events):
            raise MigrationError("migration_database_target_changed")
        operation = {
            "phase": item["name"],
            "state": "started",
            "expected_revision": item["revision"],
            "objects": item["objects"],
        }
        receipt["operations"].append(operation)
        write_json_locked(journal, receipt)
        if item["name"] == "structure":
            await target.restore(
                path,
                confirmed_tables=before["tables"],
                restore_tables=plan["tables_after"],
                source_database=candidate.endpoint.database,
            )
        else:
            await import_mysql(
                target, path, source_database=candidate.endpoint.database
            )
        observed = await target.inspect()
        if not revision_matches(observed, item["revision"], item.get("events")):
            raise MigrationError("migration_database_application_mismatch")
        operation["state"] = "confirmed"
        receipt["confirmed_revision"] = item["revision"]
        expected_events = item.get("events")
        write_json_locked(journal, receipt)


async def restore_native(
    client,
    payload,
    *,
    confirmed_tables,
    restore_tables=None,
    source_database=None,
    source_schemas=None,
):
    client.check()
    if client.endpoint.engine == "postgres":
        await client.check_restore_privileges(tables=restore_tables)
        sql_path = client.directory / "restore.sql"
        await client.run(
            "pg_restore",
            ["--no-owner", "--no-acl", "--no-tablespaces", "--file=-", payload],
            output_path=sql_path,
            maximum=Limits().expanded,
        )
        parsed = list(tokens(sql_path.read_text("utf-8"), dialect="postgres"))
        if any(
            first.text.upper() == "CREATE" and second.text.upper() == "SUBSCRIPTION"
            for first, second in pairwise(parsed)
        ):
            raise MigrationError(
                "migration_database_external_dependency",
                details={"object_kind": "subscription", "phase": client.phase},
            )
        from .native_catalog import PG_USER_SCHEMA

        cleanup = client.directory / "cleanup.sql"
        statements = []
        subscriptions = await client.rows(
            "SELECT subname AS name FROM pg_subscription WHERE subdbid="
            "(SELECT oid FROM pg_database WHERE datname=current_database())"
        )
        if subscriptions:
            raise MigrationError(
                "migration_database_external_dependency",
                details={
                    "objects": [
                        {"kind": "subscription", "name": row["name"]}
                        for row in subscriptions
                    ],
                    "phase": client.phase,
                },
            )
        for relation, field, kind in (
            ("pg_event_trigger", "evtname", "EVENT TRIGGER"),
            ("pg_publication", "pubname", "PUBLICATION"),
            ("pg_foreign_server", "srvname", "SERVER"),
        ):
            for row in await client.rows(
                f"SELECT {field} AS name FROM {relation} ORDER BY {field}"
            ):
                statements.append(
                    f"DROP {kind} {client.identifier(row['name'])} CASCADE;"
                )
        for extension in await client.rows(
            "SELECT extname AS name FROM pg_extension WHERE "
            "extname<>'plpgsql' ORDER BY extname"
        ):
            statements.append(
                "DROP EXTENSION " + client.identifier(extension["name"]) + " CASCADE;"
            )
        for schema in await client.rows(
            f"SELECT n.nspname AS name FROM pg_namespace n "
            f"WHERE {PG_USER_SCHEMA} ORDER BY n.nspname"
        ):
            statements.append(
                "DROP SCHEMA IF EXISTS "
                + client.identifier(schema["name"])
                + " CASCADE;"
            )
        # pg_dump assumes the initial public schema already exists.
        if source_schemas is None or any(
            row["name"] == "public" for row in source_schemas
        ):
            statements.append("CREATE SCHEMA public AUTHORIZATION CURRENT_USER;")
            statements.append("COMMENT ON SCHEMA public IS 'standard public schema';")
        for row in await client.rows(
            "SELECT oid FROM pg_largeobject_metadata ORDER BY oid"
        ):
            statements.append(f"SELECT pg_catalog.lo_unlink({int(row['oid'])});")
        cleanup.write_text("\n".join(statements) + "\n", encoding="utf-8")
        await client.run(
            "psql",
            [
                *client.connection,
                "-X",
                "--single-transaction",
                "-v",
                "ON_ERROR_STOP=1",
                "-f",
                cleanup,
                "-f",
                sql_path,
            ],
            maximum=Limits().expanded,
        )
        return
    rewritten = client.directory / "restore.sql"
    client.restore_privileges = set(
        getattr(client, "restore_privileges", ())
    ) | mysql_restore_script(
        payload,
        rewritten,
        database=client.endpoint.database,
        source_database=source_database,
        checkpoint=client.check,
    )
    await client.check_restore_privileges(tables=restore_tables)
    cleanup = client.directory / "cleanup.sql"
    statements = ["SET FOREIGN_KEY_CHECKS=0;"]
    for kind, source, scope, name in (
        ("EVENT", "EVENTS", "EVENT_SCHEMA", "EVENT_NAME"),
        ("PROCEDURE", "ROUTINES", "ROUTINE_SCHEMA", "ROUTINE_NAME"),
    ):
        extra = ", 'kind', ROUTINE_TYPE" if kind == "PROCEDURE" else ""
        for row in await client.rows(
            f"SELECT JSON_OBJECT('name',{name}{extra}) FROM "
            f"information_schema.{source} WHERE {scope}=DATABASE()"
        ):
            statements.append(
                f"DROP {row.get('kind', kind)} IF EXISTS "
                f"{client.identifier(row['name'])};"
            )
    views = await client.rows(
        "SELECT JSON_OBJECT('name',TABLE_NAME,'kind',TABLE_TYPE) "
        "FROM information_schema.TABLES WHERE TABLE_SCHEMA=DATABASE() "
        "ORDER BY TABLE_NAME"
    )
    for row in sorted(views, key=lambda row: row["kind"] != "VIEW"):
        kind = "VIEW" if row["kind"] == "VIEW" else "TABLE"
        statements.append(f"DROP {kind} IF EXISTS {client.identifier(row['name'])};")
    statements.append("SET FOREIGN_KEY_CHECKS=1;")
    with cleanup.open("wb") as output:
        output.write(("\n".join(statements) + "\n").encode())
        with rewritten.open("rb") as source:
            while chunk := source.read(CHUNK):
                client.check()
                output.write(chunk)
    await client.run(
        "mysql",
        [
            *client.connection,
            "--binary-mode",
            "--batch",
            f"--database={client.endpoint.database}",
        ],
        input_path=cleanup,
        maximum=Limits().expanded,
    )


async def set_event_states(client, events):
    """Publish recorded event states only for the final target, never candidates."""
    if client.endpoint.engine != "mysql" or not events:
        return
    current = {
        row["name"]: row["status"]
        for row in await client.rows(
            "SELECT JSON_OBJECT('name',EVENT_NAME,'status',STATUS) "
            "FROM information_schema.EVENTS WHERE EVENT_SCHEMA=DATABASE()"
        )
    }
    for event in events:
        status = event["status"]
        if current.get(event["name"]) == status:
            continue
        clause = {
            "ENABLED": "ENABLE",
            "DISABLED": "DISABLE",
            "SLAVESIDE_DISABLED": "DISABLE ON SLAVE",
        }.get(status)
        if clause is None or event["name"] not in current:
            raise MigrationError("migration_database_event_state_unconfirmed")
        await client.query(f"ALTER EVENT {client.identifier(event['name'])} {clause};")
