"""Private durable inbox. All methods run on the inbox's single disk worker."""

from __future__ import annotations

from contextlib import contextmanager
import json
from pathlib import Path
import sqlite3
import threading
import time


class InboxConflict(RuntimeError):
    pass


def _message_summary(raw: str | None) -> tuple[str, str]:
    """Return a safe preview for the management UI without exposing raw payloads."""
    if not raw:
        return "", "unknown"
    try:
        payload = json.loads(raw)
    except (TypeError, ValueError):
        return "消息内容解析失败", "invalid"
    if not isinstance(payload, dict):
        return "消息内容不可用", "unknown"
    message_type = str(payload.get("post_type") or payload.get("type") or "message")
    content = payload.get("message", payload.get("content", ""))
    if isinstance(content, str):
        preview = content
    elif isinstance(content, list):
        parts = []
        for segment in content:
            if isinstance(segment, dict):
                data = segment.get("data") or {}
                value = data.get("text") if isinstance(data, dict) else None
                parts.append(
                    str(value) if value else f"[{segment.get('type', '消息段')}]"
                )
        preview = "".join(parts)
    else:
        preview = ""
    preview = " ".join(preview.split())[:200]
    return preview or "[无文本内容]", message_type


def assert_replaceable_inbox(root: Path):
    path = root / "data/runtime/message-inbox/inbox.sqlite3"
    if not path.exists():
        return
    from zhenxun.migration.errors import MigrationError

    db = None
    try:
        db = sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True, timeout=1)
        pending = db.execute(
            "SELECT COUNT(*) FROM inbox WHERE state NOT IN ('completed','dismissed')"
        ).fetchone()[0]
        pending += db.execute(
            "SELECT COUNT(*) FROM deliveries WHERE state!='completed'"
        ).fetchone()[0]
    except sqlite3.Error:
        raise MigrationError("migration_message_inbox_unverifiable") from None
    finally:
        if db is not None:
            db.close()
    if pending:
        raise MigrationError("migration_message_inbox_unresolved", status=409)


class MessageStore:
    def __init__(self, path: Path, max_records=1_000_000, max_bytes=2 * 1024**3):
        self.path = path
        self.max_records = max_records
        self.max_bytes = max_bytes
        self._local = threading.local()

    def close(self):
        connection = getattr(self._local, "connection", None)
        if connection is not None:
            connection.close()
            self._local.connection = None

    @contextmanager
    def connect(self):
        connection = getattr(self._local, "connection", None)
        if connection is None:
            connection = sqlite3.connect(self.path, timeout=1)
            connection.row_factory = sqlite3.Row
            connection.execute("PRAGMA synchronous=FULL")
            self._local.connection = connection
        # Keep WAL open for the lifetime of the disk worker; reconnecting after
        # every operation forces repeated checkpoint and file setup work.
        if getattr(self._local, "batch", False):
            yield connection
        else:
            with connection:
                yield connection

    def apply_mutations(self, mutations):
        if getattr(self._local, "batch", False):
            raise RuntimeError("nested_inbox_mutation_batch")
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            self._local.batch = True
            try:
                return [method(*args) for method, args in mutations]
            finally:
                self._local.batch = False

    def initialize(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.connect() as db:
            db.execute("PRAGMA journal_mode=WAL")
            db.executescript("""
                CREATE TABLE IF NOT EXISTS inbox (
                    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                    id TEXT NOT NULL UNIQUE, bot TEXT NOT NULL,
                    conversation TEXT NOT NULL, adapter TEXT NOT NULL,
                    payload TEXT, revision INTEGER NOT NULL DEFAULT 1,
                    received REAL NOT NULL, updated REAL NOT NULL,
                    state TEXT NOT NULL, reason TEXT NOT NULL DEFAULT '',
                    generation TEXT NOT NULL, result TEXT NOT NULL DEFAULT '{}'
                );
                CREATE INDEX IF NOT EXISTS inbox_dispatch
                    ON inbox(state, sequence);
                CREATE INDEX IF NOT EXISTS inbox_pending_age
                    ON inbox(state, received);
                CREATE INDEX IF NOT EXISTS inbox_conversation
                    ON inbox(conversation, state);
                CREATE TABLE IF NOT EXISTS counters (
                    name TEXT PRIMARY KEY, value INTEGER NOT NULL DEFAULT 0
                );
                CREATE TABLE IF NOT EXISTS deliveries (
                    id TEXT PRIMARY KEY, event_id TEXT NOT NULL, writer TEXT NOT NULL,
                    payload TEXT NOT NULL, state TEXT NOT NULL DEFAULT 'pending',
                    created REAL NOT NULL, reason TEXT NOT NULL DEFAULT ''
                );
                CREATE TABLE IF NOT EXISTS receipt_gc (id TEXT PRIMARY KEY);
                CREATE INDEX IF NOT EXISTS delivery_dispatch
                    ON deliveries(state, created, id);
            """)
            columns = {row[1] for row in db.execute("PRAGMA table_info(inbox)")}
            delivery_columns = {
                row[1] for row in db.execute("PRAGMA table_info(deliveries)")
            }
            if "updated" not in delivery_columns:
                db.execute(
                    "ALTER TABLE deliveries ADD COLUMN updated REAL NOT NULL DEFAULT 0"
                )
                # Old receipts have no completion timestamp. Retain a full
                # window from upgrade rather than guessing when they committed.
                db.execute("UPDATE deliveries SET updated=?", (time.time(),))
            db.execute(
                "CREATE INDEX IF NOT EXISTS delivery_retention "
                "ON deliveries(state, updated)"
            )
            db.execute(
                "CREATE INDEX IF NOT EXISTS delivery_event "
                "ON deliveries(event_id, state)"
            )
            if "history_state" not in columns:
                db.execute(
                    "ALTER TABLE inbox ADD COLUMN history_state TEXT "
                    "NOT NULL DEFAULT 'not_required'"
                )
            db.execute(
                "UPDATE inbox SET history_state='pending' WHERE state='held' "
                "AND (history_state='executing' OR "
                "(history_state='not_required' AND result='{}'))"
            )
            db.execute(
                "UPDATE inbox SET state='unresolved', reason='worker_interrupted', "
                "revision=revision+1, updated=? WHERE state='executing'",
                (time.time(),),
            )
            db.execute(
                "UPDATE inbox SET state='unresolved', "
                "reason='interaction_interrupted', revision=revision+1,updated=? "
                "WHERE state='waiting_input'",
                (time.time(),),
            )
            self._initialize_state_counts(db)
        return self.snapshot()

    @staticmethod
    def _initialize_state_counts(db):
        # Reconcile once at startup, then maintain occupancy in the SAME SQLite
        # transaction as each row. Admission/status no longer scan retained rows.
        db.execute(
            "CREATE TABLE IF NOT EXISTS state_counts ("
            "scope TEXT NOT NULL, state TEXT NOT NULL, count INTEGER NOT NULL, "
            "PRIMARY KEY(scope,state))"
        )
        for table in ("inbox", "deliveries"):
            db.execute("DELETE FROM state_counts WHERE scope=?", (table,))
            db.execute(
                f"INSERT INTO state_counts SELECT ?,state,COUNT(*) FROM {table} "
                "GROUP BY state",
                (table,),
            )
            increment = (
                "INSERT INTO state_counts(scope,state,count) "
                f"VALUES('{table}',NEW.state,1) ON CONFLICT(scope,state) "
                "DO UPDATE SET count=count+1;"
            )
            decrement = (
                "UPDATE state_counts SET count=count-1 "
                f"WHERE scope='{table}' AND state=OLD.state;"
            )
            for action, operation in (
                ("INSERT", increment),
                ("DELETE", decrement),
                ("UPDATE OF state", decrement + increment),
            ):
                trigger = f"{table}_count_{action.split()[0].lower()}"
                condition = (
                    " WHEN OLD.state != NEW.state"
                    if action.startswith("UPDATE")
                    else ""
                )
                db.execute(
                    f"CREATE TRIGGER IF NOT EXISTS {trigger} AFTER {action} ON {table}"
                    f"{condition} BEGIN {operation} END"
                )

    @staticmethod
    def _state_counts(db, table):
        return dict(
            db.execute(
                "SELECT state,count FROM state_counts WHERE scope=? AND count>0",
                (table,),
            ).fetchall()
        )

    def enqueue_delivery(self, identity, event_id, writer, payload):
        with self.connect() as db:
            if not db.in_transaction:
                db.execute("BEGIN IMMEDIATE")
            if db.execute(
                "SELECT 1 FROM deliveries WHERE id=?", (identity,)
            ).fetchone():
                return
            if self.disk_bytes() + len(payload.encode()) + 4096 > self.max_bytes:
                raise RuntimeError("inbox_delivery_capacity")
            db.execute(
                "INSERT OR IGNORE INTO "
                "deliveries(id,event_id,writer,payload,created,updated) "
                "VALUES(?,?,?,?,?,?)",
                (identity, event_id, writer, payload, time.time(), time.time()),
            )

    def pending_deliveries(self, limit=100):
        with self.connect() as db:
            return [
                dict(row)
                for row in db.execute(
                    "SELECT * FROM deliveries WHERE state='pending' ORDER BY created "
                    "LIMIT ?",
                    (limit,),
                )
            ]

    def retired_receipts(self, limit=100):
        with self.connect() as db:
            return [
                row[0]
                for row in db.execute("SELECT id FROM receipt_gc LIMIT ?", (limit,))
            ]

    def cleaned_receipts(self, identities):
        with self.connect() as db:
            db.executemany(
                "DELETE FROM receipt_gc WHERE id=?",
                [(identity,) for identity in identities],
            )

    def delivered(self, identities):
        with self.connect() as db:
            db.executemany(
                "UPDATE deliveries SET state='completed',updated=? "
                "WHERE id=? AND state='pending'",
                [(time.time(), identity) for identity in identities],
            )

    def block_delivery(self, identity, reason):
        with self.connect() as db:
            db.execute(
                "UPDATE deliveries SET state='blocked',reason=? "
                "WHERE id=? AND state='pending'",
                (reason, identity),
            )

    def record_held_result(self, identity, result):
        with self.connect() as db:
            db.execute(
                "UPDATE inbox SET result=?,history_state=?,"
                "revision=revision+1,updated=? "
                "WHERE id=? AND state='held'",
                (
                    json.dumps(result),
                    "blocked" if result.get("history") == "unresolved" else "completed",
                    time.time(),
                    identity,
                ),
            )

    def claim_history(self, bots, limit):
        if not bots or limit <= 0:
            return []
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            marks = ",".join("?" for _ in bots)
            rows = db.execute(
                "SELECT * FROM inbox WHERE state='held' "
                "AND history_state='pending' "
                f"AND bot IN ({marks}) ORDER BY sequence LIMIT ?",
                (*bots, limit),
            ).fetchall()
            db.executemany(
                "UPDATE inbox SET history_state='executing' WHERE id=?",
                [(row["id"],) for row in rows],
            )
            return [
                {**dict(row), "payload": json.loads(row["payload"])} for row in rows
            ]

    def disk_bytes(self):
        return sum(
            p.stat().st_size
            for p in (
                self.path,
                Path(str(self.path) + "-wal"),
                Path(str(self.path) + "-shm"),
            )
            if p.exists()
        )

    @staticmethod
    def count(db, name, amount=1):
        db.execute(
            "INSERT INTO counters(name,value) VALUES(?,?) "
            "ON CONFLICT(name) DO UPDATE SET value=value+excluded.value",
            (name, amount),
        )

    def accept(self, envelopes):
        now = time.time()
        results = []
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            count = sum(self._state_counts(db, "inbox").values())
            disk_bytes = self.disk_bytes()
            for item in envelopes:
                existing = db.execute(
                    "SELECT state FROM inbox WHERE id=?", (item["id"],)
                ).fetchone()
                if existing:
                    self.count(db, "duplicates")
                    results.append(
                        {"id": item["id"], "accepted": True, "duplicate": True}
                    )
                    continue
                payload = json.dumps(item["payload"], ensure_ascii=False)
                size = len(payload.encode())
                if (
                    count >= self.max_records
                    or disk_bytes + size + 4096 > self.max_bytes
                ):
                    self.count(db, "capacity_rejected")
                    results.append(
                        {
                            "id": item["id"],
                            "accepted": False,
                            "reason": "inbox_capacity",
                        }
                    )
                    continue
                db.execute(
                    "INSERT INTO "
                    "inbox(id,bot,conversation,adapter,payload,received,"
                    "updated,state,generation) "
                    "VALUES(?,?,?,?,?,?,?,'pending',?)",
                    (
                        item["id"],
                        item["bot"],
                        item["conversation"],
                        item["adapter"],
                        payload,
                        item["received"],
                        now,
                        item["generation"],
                    ),
                )
                count += 1
                disk_bytes += size + 4096
                self.count(db, "accepted")
                results.append({"id": item["id"], "accepted": True, "duplicate": False})
        return results

    def claim(self, bots, generation, limit=64, ttl=300):
        if not bots or limit <= 0:
            return []
        now = time.time()
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            marks = ",".join("?" for _ in bots)
            rows = db.execute(
                f"SELECT i.* FROM inbox i WHERE state='pending' AND bot IN ({marks}) "
                "AND NOT EXISTS(SELECT 1 FROM inbox running WHERE "
                "running.conversation=i.conversation AND running.state='executing') "
                "AND NOT EXISTS(SELECT 1 FROM inbox earlier WHERE "
                "earlier.conversation=i.conversation AND earlier.state='pending' "
                "AND earlier.sequence<i.sequence) "
                "ORDER BY sequence LIMIT ?",
                (*bots, limit),
            ).fetchall()
            claimed = []
            for row in rows:
                reason = ""
                if row["generation"] != generation:
                    reason = "plugin_generation_changed"
                elif now - row["received"] >= ttl:
                    reason = "waiting_expired"
                state = "held" if reason else "executing"
                db.execute(
                    "UPDATE inbox SET state=?,reason=?,updated=?,revision=revision+1 "
                    ",history_state=? "
                    "WHERE id=?",
                    (
                        state,
                        reason,
                        now,
                        "executing" if reason else "not_required",
                        row["id"],
                    ),
                )
                item = dict(row)
                item.update(state=state, reason=reason, revision=row["revision"] + 1)
                item["payload"] = json.loads(item["payload"])
                claimed.append(item)
            return claimed

    def finish(self, identity, state, reason="", result=None):
        if state not in {"completed", "failed", "unresolved", "held"}:
            raise ValueError("invalid_inbox_terminal_state")
        with self.connect() as db:
            changed = db.execute(
                "UPDATE inbox SET "
                "state=?,reason=?,result=?,updated=?,revision=revision+1 "
                "WHERE id=? AND state IN ('executing','waiting_input')",
                (state, reason, json.dumps(result or {}), time.time(), identity),
            ).rowcount
            if changed:
                self.count(db, "terminal_" + state, changed)

    def waiting_input(self, identity, waiting):
        with self.connect() as db:
            db.execute(
                "UPDATE inbox SET state=?,revision=revision+1,updated=? "
                "WHERE id=? AND state=?",
                (
                    "waiting_input" if waiting else "executing",
                    time.time(),
                    identity,
                    "executing" if waiting else "waiting_input",
                ),
            )

    def page(self, after=0, limit=50, state=None):
        limit = min(max(int(limit), 1), 100)
        with self.connect() as db:
            rows = db.execute(
                "SELECT "
                "sequence,id,bot,adapter,revision,received,updated,state,reason,"
                "result,history_state,payload "
                "FROM inbox WHERE sequence>? "
                + ("AND state=? " if state else "")
                + "ORDER BY sequence LIMIT ?",
                (after, state, limit) if state else (after, limit),
            ).fetchall()
            result = []
            for row in rows:
                item = dict(row)
                preview, message_type = _message_summary(item.pop("payload", None))
                item["content_preview"] = preview
                item["message_type"] = message_type
                result.append(item)
            return result

    def resolve(self, identity, revision, action):
        if action != "dismiss":
            raise ValueError("unknown_side_effects_cannot_be_replayed")
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("SELECT * FROM inbox WHERE id=?", (identity,)).fetchone()
            if row is None:
                raise KeyError(identity)
            if row["state"] == "dismissed":
                return {
                    "id": identity,
                    "state": "dismissed",
                    "revision": row["revision"],
                }
            if row["revision"] != revision or row["state"] not in {
                "held",
                "unresolved",
                "failed",
            }:
                raise InboxConflict("inbox_revision_conflict")
            db.execute(
                "UPDATE inbox SET state='dismissed',reason='administrator_dismissed', "
                "updated=?,revision=revision+1 WHERE id=?",
                (time.time(), identity),
            )
        return {"id": identity, "state": "dismissed", "revision": revision + 1}

    def snapshot(self):
        with self.connect() as db:
            states = self._state_counts(db, "inbox")
            oldest = db.execute(
                "SELECT MIN(received) FROM inbox WHERE state='pending'"
            ).fetchone()[0]
            counters = dict(db.execute("SELECT name,value FROM counters").fetchall())
            deliveries = self._state_counts(db, "deliveries")
            history = dict(
                db.execute(
                    "SELECT history_state,COUNT(*) FROM inbox WHERE state='held' "
                    "GROUP BY history_state"
                ).fetchall()
            )
        return {
            "states": states,
            "counters": counters,
            "bytes": self.disk_bytes(),
            "deliveries": deliveries,
            "history": history,
            "oldest_wait_seconds": max(0, time.time() - oldest) if oldest else 0,
        }

    def cleanup(self):
        now = time.time()
        with self.connect() as db:
            db.execute(
                "UPDATE inbox SET payload=NULL WHERE state IN "
                "('completed','dismissed') "
                "AND updated<? AND NOT EXISTS(SELECT 1 FROM deliveries d "
                "WHERE d.event_id=inbox.id AND d.state!='completed')",
                (now - 86400,),
            )
            db.execute(
                "DELETE FROM inbox WHERE state IN ('completed','dismissed') AND "
                "updated<? AND NOT EXISTS(SELECT 1 FROM deliveries d "
                "WHERE d.event_id=inbox.id AND d.state!='completed')",
                (now - 7 * 86400,),
            )
            db.execute(
                "INSERT OR IGNORE INTO receipt_gc SELECT id FROM deliveries "
                "WHERE state='completed' AND updated<? "
                "AND NOT EXISTS(SELECT 1 FROM inbox i WHERE i.id=deliveries.event_id "
                "AND i.state NOT IN ('completed','dismissed'))",
                (now - 7 * 86400,),
            )
            db.execute(
                "DELETE FROM deliveries WHERE id IN (SELECT id FROM receipt_gc) "
                "AND state='completed' AND updated<?",
                (now - 7 * 86400,),
            )
            db.execute(
                "UPDATE deliveries SET payload='{}' WHERE state='completed' "
                "AND updated<? AND NOT EXISTS(SELECT 1 FROM inbox i "
                "WHERE i.id=deliveries.event_id "
                "AND i.state NOT IN ('completed','dismissed'))",
                (now - 86400,),
            )
        with self.connect() as db:
            db.execute("PRAGMA wal_checkpoint(PASSIVE)")
