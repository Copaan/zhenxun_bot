from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import re

from zhenxun.utils.atomic_json import read_json_locked, write_json_locked

from .errors import MigrationError
from .paths import contained_path, logical_path

MAX_RECORD = 64 * 1024
MAX_RECORDS = 1_600_000


def _encode(value: dict) -> bytes:
    return json.dumps(
        value, sort_keys=True, ensure_ascii=True, separators=(",", ":")
    ).encode()


class FileJournal:
    """An authoritative append log with small, replaceable stage checkpoints.

    The caller owns the instance lease. Replaying once per process keeps per-file
    write cost independent of the number of preceding file actions.
    """

    def __init__(self, path: Path, initial: dict | None = None):
        self.path = contained_path(path.parent, path.name)
        self.events = contained_path(path.parent, path.name + ".events")
        self.sequence = 0
        self.head = "0" * 64
        self.offset = 0
        self.state: dict = {"schema": 2, "actions": [], "directories": []}
        self._actions: dict[int, dict] = {}
        self._directories: dict[str, dict] = {}
        self._removed_directories: dict[str, dict] = {}
        if initial is not None:
            if self.path.exists() or self.events.exists():
                raise MigrationError("migration_journal_exists", status=409)
            self.path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
            with self.events.open("xb") as stream:
                stream.flush()
                os.fsync(stream.fileno())
            self.append("begin", initial)
            self.checkpoint()
        elif self.events.exists():
            self._replay()
        else:
            legacy = read_json_locked(self.path, None)
            if not isinstance(legacy, dict) or legacy.get("schema") != 1:
                raise MigrationError("migration_journal_invalid")
            self.state = legacy

    def _apply(self, kind: str, payload: dict) -> None:
        if kind == "begin":
            if self.sequence != 1:
                raise MigrationError("migration_journal_order_invalid")
            self.state.update(payload, schema=2)
        elif kind == "stage":
            self.state.update(payload)
        elif kind == "intent":
            index = payload["index"]
            if index != len(self._actions):
                raise MigrationError("migration_journal_order_invalid")
            receipt = {**payload, "state": "intent"}
            self._actions[index] = receipt
            self.state["actions"].append(receipt)
        elif kind == "materialized":
            self._actions[payload["index"]]["after_identity"] = payload["identity"]
        elif kind in {"applied", "rolled_back"}:
            self._actions[payload["index"]]["state"] = kind
        elif kind == "directory_intent":
            path = payload["path"]
            if path in self._directories:
                raise MigrationError("migration_journal_order_invalid")
            receipt = {**payload, "state": "intent"}
            self._directories[path] = receipt
            self.state["directories"].append(receipt)
        elif kind in {"directory_created", "directory_rolled_back"}:
            self._directories[payload["path"]].update(
                payload,
                state="created" if kind == "directory_created" else "rolled_back",
            )
        elif kind == "directory_remove_intent":
            receipt = {**payload, "state": "intent"}
            self._removed_directories[payload["path"]] = receipt
            self.state.setdefault("removed_directories", []).append(receipt)
        elif kind in {"directory_removed", "directory_restored"}:
            self._removed_directories[payload["path"]]["state"] = (
                "removed" if kind == "directory_removed" else "restored"
            )
        else:
            raise MigrationError("migration_journal_event_invalid")

    def _validate(self, kind: str, payload: dict, sequence: int) -> None:
        if not isinstance(payload, dict):
            raise MigrationError("migration_journal_event_invalid")
        if kind == "begin":
            if (
                sequence != 1
                or {"actions", "directories", "removed_directories", "schema"}
                & payload.keys()
            ):
                raise MigrationError("migration_journal_order_invalid")
        elif sequence == 1:
            raise MigrationError("migration_journal_order_invalid")
        elif kind == "stage":
            if (
                {"actions", "directories", "removed_directories", "schema"}
                & payload.keys()
                or not isinstance(payload.get("stage"), str)
                or not re.fullmatch(r"[a-z_]{1,80}", payload["stage"])
            ):
                raise MigrationError("migration_journal_event_invalid")
        elif kind in {"intent", "applied", "rolled_back", "materialized"}:
            index = payload.get("index")
            if type(index) is not int or index < 0:
                raise MigrationError("migration_journal_order_invalid")
            if kind == "intent":
                logical_path(payload.get("path"))
                if index != len(self._actions):
                    raise MigrationError("migration_journal_order_invalid")
            elif index not in self._actions:
                raise MigrationError("migration_journal_order_invalid")
            elif self._actions[index]["state"] == "rolled_back":
                raise MigrationError("migration_journal_order_invalid")
        elif kind in {
            "directory_remove_intent",
            "directory_removed",
            "directory_restored",
        }:
            path = logical_path(payload.get("path"))
            if kind == "directory_remove_intent":
                logical_path(payload.get("backup"))
                identity = payload.get("identity")
                if (
                    path in self._removed_directories
                    or not isinstance(identity, list)
                    or len(identity) != 2
                    or any(type(value) is not int or value < 0 for value in identity)
                ):
                    raise MigrationError("migration_journal_event_invalid")
            elif path not in self._removed_directories:
                raise MigrationError("migration_journal_order_invalid")
            elif self._removed_directories[path]["state"] == "restored":
                raise MigrationError("migration_journal_order_invalid")
        elif kind.startswith("directory_"):
            path = logical_path(payload.get("path"))
            if kind == "directory_intent":
                if path in self._directories:
                    raise MigrationError("migration_journal_order_invalid")
            elif kind in {"directory_created", "directory_rolled_back"}:
                if path not in self._directories:
                    raise MigrationError("migration_journal_order_invalid")
            else:
                raise MigrationError("migration_journal_event_invalid")
        else:
            raise MigrationError("migration_journal_event_invalid")
        if kind in {"materialized", "directory_created"}:
            identity = payload.get("identity")
            if (
                not isinstance(identity, list)
                or len(identity) != 2
                or any(type(value) is not int or value < 0 for value in identity)
            ):
                raise MigrationError("migration_journal_event_invalid")

    def _replay(self) -> None:
        try:
            with self.events.open("rb") as stream:
                while line := stream.readline(MAX_RECORD + 1):
                    if len(line) > MAX_RECORD:
                        raise MigrationError("migration_journal_record_limit")
                    if not line.endswith(b"\n"):
                        break
                    record = json.loads(line)
                    digest = record.pop("digest")
                    if (
                        record["sequence"] != self.sequence + 1
                        or record["previous"] != self.head
                        or hashlib.sha256(_encode(record)).hexdigest() != digest
                    ):
                        raise MigrationError("migration_journal_corrupt")
                    self._validate(record["kind"], record["payload"], self.sequence + 1)
                    self.sequence += 1
                    if self.sequence > MAX_RECORDS:
                        raise MigrationError("migration_journal_record_limit")
                    self.head = digest
                    self._apply(record["kind"], record["payload"])
                    self.offset = stream.tell()
            if not self.sequence:
                raise MigrationError("migration_journal_incomplete")
        except (ValueError, KeyError, TypeError, AttributeError) as error:
            raise MigrationError("migration_journal_corrupt") from error

    def append(self, kind: str, payload: dict) -> None:
        if self.state["schema"] == 1:
            raise MigrationError("migration_legacy_journal_append_forbidden")
        self._validate(kind, payload, self.sequence + 1)
        record = {
            "sequence": self.sequence + 1,
            "previous": self.head,
            "kind": kind,
            "payload": payload,
        }
        digest = hashlib.sha256(_encode(record)).hexdigest()
        data = _encode({**record, "digest": digest}) + b"\n"
        if len(data) > MAX_RECORD or self.sequence >= MAX_RECORDS:
            raise MigrationError("migration_journal_record_limit")
        with self.events.open("r+b") as stream:
            stream.seek(self.offset)
            # An incomplete final record has not authorized a target mutation.
            stream.truncate()
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
            self.offset = stream.tell()
        self.sequence += 1
        self.head = digest
        self._apply(kind, payload)

    def checkpoint(self) -> None:
        metadata = {
            k: v
            for k, v in self.state.items()
            if k not in {"actions", "directories", "removed_directories"}
        }
        write_json_locked(
            self.path,
            {
                **metadata,
                "sequence": self.sequence,
                "head": self.head,
                "action_count": len(self.state["actions"]),
            },
        )

    def stage(self, name: str, **details) -> None:
        self.append("stage", {"stage": name, **details})
        self.checkpoint()
