"""SQLite resource classification and bounded, checkpoint-independent revisions."""

from __future__ import annotations

from contextlib import closing
import hashlib
from pathlib import Path
import sqlite3
import tempfile

from .archive import require_space
from .errors import MigrationError
from .paths import is_link

REVISION_ALGORITHM = "sqlite-content-v2"
_SUFFIXES = {".db", ".sqlite", ".sqlite3"}
_CHUNK = 64 * 1024


def classify_sqlite(path: Path, *, configured: bool = False) -> str:
    """Identify SQLite by its header; filenames alone do not establish format."""
    if not path.exists():
        return "missing"
    if is_link(path) or not path.is_file():
        raise MigrationError("migration_path_type_conflict")
    with path.open("rb") as stream:
        header = stream.read(16)
    if header == b"SQLite format 3\0":
        return "sqlite"
    if not header:
        return "sqlite_empty" if configured else "empty_placeholder"
    return "invalid_primary" if configured else "ordinary_file"


def sqlite_companion(path: Path) -> bool:
    for suffix in ("-wal", "-shm", "-journal"):
        if path.name.endswith(suffix) and path.name != suffix:
            base = path.with_name(path.name[: -len(suffix)])
            return classify_sqlite(base) == "sqlite"
    return False


def file_database_summary(path: Path, *, configured: bool = False) -> dict | None:
    kind = classify_sqlite(path, configured=configured)
    if (
        kind in {"ordinary_file", "empty_placeholder"}
        and path.suffix.lower() not in _SUFFIXES
    ):
        return None
    if kind == "missing":
        return None
    return {"kind": kind, "configured": configured, "bytes": path.stat().st_size}


def _quote(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def content_revision(path: Path, *, checkpoint=lambda: None) -> str:
    failures = []

    def check():
        try:
            checkpoint()
        except BaseException as error:
            failures.append(error)
            raise

    try:
        return _content_revision(path, checkpoint=check)
    except sqlite3.Error:
        if failures:
            raise failures[0]
        raise


def _content_revision(path: Path, *, checkpoint) -> str:
    """Hash typed cells in bounded chunks and sort row digests on disk."""
    digest = hashlib.sha256(REVISION_ALGORITHM.encode())
    require_space(path.parent, 0)
    with (
        closing(
            sqlite3.connect(path.as_uri() + "?mode=ro&immutable=1", uri=True)
        ) as db,
        tempfile.TemporaryDirectory(prefix="sqlite-revision-", dir=path.parent) as tmp,
        closing(sqlite3.connect(str(Path(tmp) / "rows.db"))) as sort,
    ):
        db.execute("PRAGMA trusted_schema=OFF")
        db.execute("PRAGMA query_only=ON")
        sort.execute("PRAGMA cache_size=-2048")
        sort.execute("PRAGMA journal_mode=OFF")
        sort.execute("PRAGMA temp_store=FILE")
        sort.execute(
            "CREATE TABLE hashes (value BLOB PRIMARY KEY, copies INTEGER) WITHOUT ROWID"
        )

        def progress():
            checkpoint()
            return 0

        db.set_progress_handler(progress, 1000)
        sort.set_progress_handler(progress, 1000)
        schema = db.execute(
            "SELECT type,name,tbl_name,sql FROM sqlite_schema "
            "WHERE name NOT LIKE 'sqlite_stat%' ORDER BY type,name"
        ).fetchall()
        for row in schema:
            checkpoint()
            for value in row:
                encoded = (value or "").encode("utf-8")
                digest.update(len(encoded).to_bytes(8, "big") + encoded)
        table_info = {r[1]: r for r in db.execute("PRAGMA table_list")}
        kinds = {name: row[2] for name, row in table_info.items()}
        for object_type, table, _owner, sql in schema:
            if object_type != "table":
                continue
            # Virtual data is stored in its shadow tables. Querying a virtual
            # table can invoke extension code and duplicates the stored content.
            if kinds.get(table) == "virtual" and any(
                name.startswith(table + "_") and kind == "shadow"
                for name, kind in kinds.items()
            ):
                continue
            checkpoint()
            columns = [
                r
                for r in db.execute(f"PRAGMA table_xinfo({_quote(table)})")
                if r[6] != 1
            ]
            names = {r[1].lower() for r in columns}
            rowid = next(
                (n for n in ("_rowid_", "rowid", "oid") if n not in names), None
            )
            if table_info.get(table, (None,) * 5)[4]:
                rowid = None
            if kinds.get(table) == "virtual":
                rowid = None
            fields = ([rowid] if rowid else []) + [r[1] for r in columns]
            expressions = []
            for field in fields:
                column = _quote(field)
                value = (
                    f"CASE WHEN typeof({column})='real' THEN printf('%!.26g',{column}) "
                    f"ELSE {column} END"
                )
                blob = f"CAST(({value}) AS BLOB)"
                expressions.append(
                    f"CAST(typeof({column})||':'||coalesce(length({blob}),0)||':' "
                    f"AS BLOB)"
                    f" || coalesce(substr({blob},1,1024),x'')"
                )
            expressions = [f"CAST(({expr}) AS BLOB)" for expr in expressions]
            if not expressions:
                continue
            quoted = _quote(table)
            digest.update(table.encode("utf-8") + b"\0")
            count = 0
            cursors = [
                db.execute(
                    f"SELECT {','.join(expressions[start:start + 256])} "
                    f"FROM {quoted} NOT INDEXED"
                )
                for start in range(0, len(expressions), 256)
            ]
            for position, chunks in enumerate(zip(*cursors)):
                values = [value for chunk in chunks for value in chunk]
                checkpoint()
                row_digest = hashlib.sha256()
                for field, encoded in zip(fields, values):
                    kind, length, prefix = encoded.split(b":", 2)
                    size = int(length)
                    row_digest.update(kind + b":" + size.to_bytes(8, "big"))
                    row_digest.update(prefix)
                    offset = len(prefix)
                    while offset < size:
                        checkpoint()
                        selector, parameters = (
                            (
                                f"WHERE {_quote(rowid)}=?",
                                (int(values[0].split(b":", 2)[2]),),
                            )
                            if rowid
                            else ("LIMIT 1 OFFSET ?", (position,))
                        )
                        chunk = db.execute(
                            f"SELECT substr(CAST({_quote(field)} AS BLOB),?,?) "
                            f"FROM {quoted} NOT INDEXED {selector}",
                            (offset + 1, _CHUNK, *parameters),
                        ).fetchone()[0]
                        if not chunk:
                            raise MigrationError(
                                "migration_database_revision_incomplete"
                            )
                        row_digest.update(chunk)
                        offset += len(chunk)
                sort.execute(
                    "INSERT INTO hashes VALUES (?,1) ON CONFLICT(value) "
                    "DO UPDATE SET copies=copies+1",
                    (row_digest.digest(),),
                )
                count += 1
                if count % 4096 == 0:
                    require_space(Path(tmp), 0)
            sort.commit()
            digest.update(count.to_bytes(8, "big"))
            for value, copies in sort.execute(
                "SELECT value,copies FROM hashes ORDER BY value"
            ):
                checkpoint()
                digest.update(value + copies.to_bytes(8, "big"))
            sort.execute("DELETE FROM hashes")
        for pragma in ("user_version", "application_id"):
            digest.update(str(db.execute(f"PRAGMA {pragma}").fetchone()).encode())
        checkpoint()
    return digest.hexdigest()
