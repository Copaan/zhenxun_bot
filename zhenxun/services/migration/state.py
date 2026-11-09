"""恢复任务的持久化归属、版本比较与工作目录"""

from __future__ import annotations

from contextlib import ExitStack, contextmanager
from copy import deepcopy
from datetime import datetime, timezone
from hashlib import sha256
import json
import os
from pathlib import Path
import re
import shutil
import tempfile
from typing import Any

import psutil

from zhenxun.services import runtime_mutation as mutation
from zhenxun.utils.atomic_json import (
    AtomicJsonRecordError,
    read_json_locked,
    write_json_locked,
)

ROOT = mutation.INSTANCE_ROOT / "data/runtime/restore"
REQUEST = ROOT / "request.json"
TERMINAL = frozenset({"done", "done_partial", "rolled_back", "failed", "canceled"})


def now() -> str:
    return datetime.now(timezone.utc).isoformat()


def task_path(operation_id: str) -> Path:
    """返回已验证标识对应的隔离任务目录"""
    if not re.fullmatch(r"[a-zA-Z0-9_-]{16,80}", operation_id):
        raise RuntimeError("migration_operation_id_required")
    return ROOT / "tasks" / operation_id


def load(operation_id: str | None = None) -> dict | None:
    path = task_path(operation_id) / "request.json" if operation_id else REQUEST
    value = read_json_locked(path, None, strict=True)
    owner = mutation.mutation_owner() if not operation_id else None
    if (
        owner
        and owner.get("kind") == "restore"
        and (not value or value.get("operation_id") != owner["operation_id"])
    ):
        value = read_json_locked(
            task_path(owner["operation_id"]) / "request.json", None, strict=True
        )
        if value is None:
            raise AtomicJsonRecordError("migration_owner_record_missing")
    if value is None:
        if owner and owner.get("kind") == "restore":
            value = read_json_locked(
                task_path(owner["operation_id"]) / "request.json", None, strict=True
            )
            if value is None:
                raise AtomicJsonRecordError("migration_owner_record_missing")
        else:
            return None
    if not isinstance(value, dict) or not value.get("state"):
        raise AtomicJsonRecordError("migration_record_invalid")
    if value.get("record_version") == 2:
        task_path(str(value.get("operation_id") or ""))
        if not isinstance(value.get("revision"), int):
            raise AtomicJsonRecordError("migration_revision_invalid")
        if not operation_id:
            canonical = read_json_locked(
                task_path(value["operation_id"]) / "request.json", None, strict=True
            )
            if (
                not isinstance(canonical, dict)
                or canonical.get("operation_id") != value["operation_id"]
            ):
                raise AtomicJsonRecordError("migration_task_record_missing")
            if canonical.get("revision", 0) < value["revision"]:
                raise AtomicJsonRecordError("migration_task_record_inconsistent")
            value = canonical
    return value


def _check_changes() -> None:
    from zhenxun.services.nonebot_store import storage
    from zhenxun.services.plugin_store import plugin_store_transaction as source
    from zhenxun.services.update.update_service import pending_job

    if storage.active_transaction():
        raise RuntimeError("dependency_transaction_pending")
    current = source.pending_transaction()
    if current and not (
        current.get("rollback_confirmed")
        or current.get("state") in {"committed", "cleanup_pending"}
    ):
        raise RuntimeError("source_transaction_pending")
    if pending_job():
        raise RuntimeError("update_transaction_pending")
    sync = read_json_locked(storage.DEPENDENCY_SYNC_STATUS_FILE, {}, strict=True)
    if not isinstance(sync, dict):
        raise AtomicJsonRecordError("dependency_sync_record_invalid")
    if sync.get("status") in {"pending", "running", "failed", "recovery_required"}:
        raise RuntimeError("dependency_sync_verification_pending")
    from zhenxun.utils import restart_state

    restart = read_json_locked(restart_state._RESTART_STATE_FILE, {}, strict=True)
    if not isinstance(restart, dict):
        raise AtomicJsonRecordError("restart_record_invalid")
    pending = restart.get(restart_state._PENDING_RESTARTS_KEY, {})
    if not isinstance(pending, dict):
        raise AtomicJsonRecordError("restart_record_invalid")
    if any(
        key != "webui.nonebot-store" and not key.startswith("webui.plugin:")
        for key in pending
    ):
        raise RuntimeError("configuration_verification_pending")
    if restart.get(restart_state._LAUNCHER_ACTION_KEY):
        raise RuntimeError("launcher_action_pending")


def reserve(operation_id: str, inputs: dict, values: dict) -> tuple[dict, bool]:
    """在耗时准备前占位，同一输入复用原任务且不覆盖未完成事务"""
    directory = task_path(operation_id)
    binding = sha256(
        json.dumps(inputs, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    with mutation.mutation_guard(operation_id) as owner:
        current = load()
        historical = (
            current
            if current and current.get("operation_id") == operation_id
            else load(operation_id)
        )
        if historical:
            if historical.get("input_digest") != binding:
                raise RuntimeError("migration_operation_input_changed")
            return historical, True
        if current and current.get("state") not in TERMINAL:
            raise RuntimeError("migration_operation_in_progress")
        if owner:
            raise AtomicJsonRecordError("migration_owner_unsettled")
        if mutation.runtime_mutation_coordinator.status():
            raise RuntimeError("runtime_mutation_in_progress")
        from zhenxun.services.nonebot_store import storage
        from zhenxun.services.plugin_store import plugin_store_transaction as source

        with ExitStack() as locks:
            locks.enter_context(storage.transaction_lock(timeout=0))
            locks.enter_context(source._locked_transaction(timeout=0))
            _check_changes()
        directory.mkdir(parents=True, exist_ok=True)
        launcher_pid = int(os.environ.get("ZHENXUN_LAUNCHER_PID") or os.getpid())
        executor = {
            "pid": launcher_pid,
            "created_at": psutil.Process(launcher_pid).create_time(),
            "lease_id": os.environ.get("ZHENXUN_INSTANCE_LEASE_ID"),
        }
        record = {
            **deepcopy(values),
            "record_version": 2,
            "operation_id": operation_id,
            "input_digest": binding,
            "revision": 1,
            "state": "preparing",
            "executor": executor,
            "created_at": now(),
            "updated_at": now(),
        }
        write_json_locked(
            mutation.OWNER_FILE,
            {
                "operation_id": operation_id,
                "executor": executor,
                "kind": "restore",
            },
        )
        write_json_locked(directory / "request.json", record)
        write_json_locked(REQUEST, record)
        return record, False


def save(record: dict, **changes: Any) -> dict:
    """比较任务修订后保存，旧执行者和迟到写入不得覆盖当前记录"""
    identity = str(record.get("operation_id") or "")
    with mutation.mutation_guard(identity):
        current = load()
        if (
            not current
            or current.get("operation_id") != identity
            or current.get("revision") != record.get("revision")
        ):
            raise RuntimeError("migration_record_changed")
        updated = {
            **current,
            **changes,
            "updated_at": now(),
            "revision": current["revision"] + 1,
        }
        write_json_locked(task_path(identity) / "request.json", updated)
        write_json_locked(REQUEST, updated)
        record.clear()
        record.update(updated)
        return record


@contextmanager
def owned(record: dict):
    """校验执行进程归属后进入内部阶段，重启接管须证明旧 launcher 已退出"""
    identity = str(record.get("operation_id") or "")
    if record.get("record_version") != 2:
        raise RuntimeError("migration_legacy_task_requires_review")
    executor = record.get("executor") or {}
    lease = os.environ.get("ZHENXUN_INSTANCE_LEASE_ID")
    if executor.get("lease_id") != lease:
        try:
            process = psutil.Process(int(executor["pid"]))
            alive = process.create_time() == executor.get("created_at")
        except psutil.NoSuchProcess:
            alive = False
        except (psutil.Error, KeyError, TypeError, ValueError) as error:
            raise RuntimeError("migration_executor_unconfirmed") from error
        if alive:
            raise RuntimeError("migration_executor_still_active")
        if not lease:
            raise RuntimeError("migration_launcher_identity_required")
        launcher_pid = int(os.environ.get("ZHENXUN_LAUNCHER_PID") or os.getpid())
        from zhenxun.services.launcher.lease import validate_delegation

        if launcher_pid != os.getpid():
            validate_delegation(mutation.INSTANCE_ROOT, launcher_pid, lease)
        else:
            owner = read_json_locked(
                mutation.INSTANCE_ROOT / "data/runtime/instance/owner.json",
                None,
                strict=True,
            )
            if (
                not owner
                or owner.get("identity") != lease
                or owner.get("pid") != launcher_pid
            ):
                raise RuntimeError("migration_launcher_identity_required")
        executor = {
            "pid": launcher_pid,
            "created_at": psutil.Process(launcher_pid).create_time(),
            "lease_id": lease,
        }
        save(record, executor=executor)
        with mutation.mutation_guard(identity):
            write_json_locked(
                mutation.OWNER_FILE,
                {
                    "operation_id": identity,
                    "executor": executor,
                    "kind": "restore",
                },
            )
    with mutation.mutation_scope(identity):
        with mutation.mutation_guard(identity):
            pass
        yield


def finish(record: dict, **changes: Any) -> dict:
    """终态落盘后释放归属，清理故障不覆盖业务结果"""
    save(record, **changes)
    if record["state"] not in TERMINAL:
        raise RuntimeError("migration_not_terminal")
    mutation.release_mutation(record["operation_id"])
    return record


def save_terminal(record: dict, **changes: Any) -> dict:
    """只补写已结束任务的结算字段，不覆盖当前或其他任务"""
    with mutation.mutation_guard():
        current = load(record["operation_id"])
        if not current or current.get("revision") != record.get("revision"):
            raise RuntimeError("migration_record_changed")
        if current.get("state") not in TERMINAL:
            raise RuntimeError("migration_not_terminal")
        updated = {
            **current,
            **changes,
            "revision": current["revision"] + 1,
            "updated_at": now(),
        }
        write_json_locked(task_path(record["operation_id"]) / "request.json", updated)
        index = read_json_locked(REQUEST, None, strict=True)
        if index and index.get("operation_id") == record["operation_id"]:
            write_json_locked(REQUEST, updated)
        record.clear()
        record.update(updated)
    return record


def cancellation_requested(record: dict) -> bool:
    value = read_json_locked(
        task_path(record["operation_id"]) / "cancel.json", None, strict=True
    )
    if value is None:
        return False
    if (
        not isinstance(value, dict)
        or value.get("operation_id") != record["operation_id"]
    ):
        raise AtomicJsonRecordError("migration_cancel_record_invalid")
    return True


def request_cancellation(operation_id: str) -> None:
    """登记显式取消请求，执行者只在完整阶段结束后处理"""
    with mutation.mutation_guard(operation_id):
        record = load(operation_id)
        if not record:
            raise RuntimeError("migration_operation_not_found")
        if record["state"] in TERMINAL:
            return
        if record.get("commit_decision"):
            raise RuntimeError("migration_commit_already_decided")
        write_json_locked(
            task_path(operation_id) / "cancel.json",
            {
                "operation_id": operation_id,
                "revision": record["revision"],
                "requested_at": now(),
            },
        )


def write_startup_configuration(path: Path, content: str) -> bool:
    """将已绑定 worker 的受控配置写入纳入父任务文件回滚日志"""
    root = mutation.INSTANCE_ROOT
    path = path.resolve()
    if path not in {
        root / "data/config.yaml",
        root / "data/configs/plugins2config.yaml",
    }:
        return False
    record = load()
    if not record or record["state"] not in {"importing", "verifying"}:
        return False
    with owned(record), mutation.mutation_guard(record["operation_id"]):
        current = load()
        if (
            not current
            or current["revision"] != record["revision"]
            or current.get("commit_decision")
        ):
            raise RuntimeError("migration_record_changed")
        startup_id = os.environ.get("ZHENXUN_WORKER_STARTUP_ID")
        if not startup_id or startup_id != current.get("expected_worker_startup_id"):
            raise RuntimeError("migration_worker_identity_changed")
        directory = task_path(record["operation_id"]) / "rollback"
        files = Files(root, directory)
        with tempfile.TemporaryDirectory(prefix="config-", dir=directory) as staging:
            source = Path(staging) / "config.yaml"
            source.write_bytes(content.encode("utf-8"))
            files.write(path.relative_to(root).as_posix(), source)
    return True


class Files:
    """按任务记录文件覆盖证据，只有目标仍属于本事务时才允许回退"""

    def __init__(self, root: Path, directory: Path):
        self.root = root.resolve()
        self.directory = directory
        self.record = directory / "files.json"

    def target(self, relative: str) -> Path:
        path = (self.root / relative).resolve()
        if not path.is_relative_to(self.root) or path == self.root:
            raise RuntimeError("migration_file_outside_instance")
        return path

    @staticmethod
    def digest(path: Path) -> str | None:
        try:
            with path.open("rb") as stream:
                digest = sha256()
                while chunk := stream.read(1024 * 1024):
                    digest.update(chunk)
                return digest.hexdigest()
        except FileNotFoundError:
            return None

    def initialize(self) -> None:
        if not self.record.exists():
            write_json_locked(self.record, {"version": 2, "files": {}})

    def write(self, relative: str, source: Path) -> None:
        target = self.target(relative)
        record = read_json_locked(self.record, None, strict=True)
        if not isinstance(record, dict) or record.get("version") != 2:
            raise AtomicJsonRecordError("migration_file_journal_missing")
        entry = record["files"].get(relative)
        before = self.digest(target)
        if entry and before != entry["after"]:
            raise RuntimeError("migration_file_target_changed:" + relative)
        if not entry:
            entry = {"before": before, "after": before}
            if before is not None:
                backup = self.directory / "files" / relative
                backup.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(target, backup)
                if self.digest(backup) != before or self.digest(target) != before:
                    raise RuntimeError("migration_file_target_changed:" + relative)
        entry["planned"] = self.digest(source)
        record["files"][relative] = entry
        write_json_locked(self.record, record)
        self._replace(source, target, before)
        entry["after"] = entry.pop("planned")
        write_json_locked(self.record, record)

    def _replace(self, source: Path | None, target: Path, expected: str | None) -> None:
        target.parent.mkdir(parents=True, exist_ok=True)
        if self.digest(target) != expected:
            raise RuntimeError("migration_file_target_changed:" + target.name)
        if source is None:
            target.unlink(missing_ok=True)
            return
        fd, temporary = tempfile.mkstemp(prefix=".zx-restore-", dir=target.parent)
        try:
            with os.fdopen(fd, "wb") as output, source.open("rb") as input_file:
                shutil.copyfileobj(input_file, output)
                output.flush()
                os.fsync(output.fileno())
            if self.digest(target) != expected:
                raise RuntimeError("migration_file_target_changed:" + target.name)
            os.replace(temporary, target)
        finally:
            Path(temporary).unlink(missing_ok=True)

    def changed(self) -> bool:
        """按持久化写入记录核对目标文件是否仍与原始内容一致。"""
        record = read_json_locked(self.record, None, strict=True)
        if (
            not isinstance(record, dict)
            or record.get("version") != 2
            or not isinstance(record.get("files"), dict)
        ):
            raise AtomicJsonRecordError("migration_file_journal_invalid")
        return any(
            self.digest(self.target(relative)) != entry["before"]
            for relative, entry in record["files"].items()
        )

    def rollback(self) -> None:
        record = read_json_locked(self.record, None, strict=True)
        if not isinstance(record, dict) or record.get("version") != 2:
            raise AtomicJsonRecordError("migration_file_journal_missing")
        entries = record.get("files")
        if not isinstance(entries, dict):
            raise AtomicJsonRecordError("migration_file_journal_invalid")
        for relative, entry in entries.items():
            current = self.digest(self.target(relative))
            if current not in {
                entry["before"],
                entry["after"],
                entry.get("planned", entry["after"]),
            }:
                raise RuntimeError("migration_file_rollback_target_changed:" + relative)
            if entry["before"] is not None:
                backup = self.directory / "files" / relative
                if self.digest(backup) != entry["before"]:
                    raise RuntimeError(
                        "migration_file_rollback_backup_changed:" + relative
                    )
        for relative, entry in reversed(list(entries.items())):
            target = self.target(relative)
            current = self.digest(target)
            if current == entry["before"]:
                continue
            backup = (
                self.directory / "files" / relative
                if entry["before"] is not None
                else None
            )
            self._replace(backup, target, current)
        record["rollback_confirmed"] = True
        write_json_locked(self.record, record)


def sqlite_fingerprint(connection, *, legacy: bool = False) -> str:
    """计算 SQLite 逻辑内容摘要，不依赖文件页布局、WAL 或路径大小写"""
    digest = sha256()
    for statement in connection.iterdump():
        if not legacy and statement == 'DELETE FROM "sqlite_sequence";':
            continue
        digest.update((statement + "\n").encode())
    return digest.hexdigest()


def restore_sqlite(
    snapshot: dict, expected: str, *, backup_root: Path | None = None
) -> str:
    """在同一写事务内复核并恢复父任务快照，拒绝未知外部写入"""
    from contextlib import closing
    import sqlite3

    path = Path(snapshot["file"]).resolve()
    backup = Path(snapshot["backup"]).resolve()
    if (
        not backup.is_relative_to((backup_root or ROOT).resolve())
        or not backup.is_file()
    ):
        raise RuntimeError("migration_sqlite_snapshot_missing")
    with closing(sqlite3.connect(backup)) as source:
        restored_fingerprint = sqlite_fingerprint(source)
        if snapshot["fingerprint"] not in {
            restored_fingerprint,
            sqlite_fingerprint(source, legacy=True),
        }:
            raise RuntimeError("migration_sqlite_snapshot_changed")
        statements = list(source.iterdump())
    with closing(sqlite3.connect(path, timeout=3, isolation_level=None)) as target:
        original = target.execute("PRAGMA foreign_keys").fetchone()[0]
        target.execute("PRAGMA foreign_keys=OFF")
        target.execute("BEGIN IMMEDIATE")
        try:
            actual = sqlite_fingerprint(target)
            if actual == restored_fingerprint:
                target.commit()
                return restored_fingerprint
            if expected not in {actual, sqlite_fingerprint(target, legacy=True)}:
                raise RuntimeError("migration_sqlite_rollback_target_changed")
            objects = target.execute(
                "SELECT type,name,sql FROM sqlite_master WHERE name NOT LIKE 'sqlite_%'"
                " AND type IN ('table','view','trigger') ORDER BY type DESC,name"
            ).fetchall()
            if any(
                "CREATE VIRTUAL TABLE" in (sql or "").upper() for _, _, sql in objects
            ):
                raise RuntimeError("migration_sqlite_virtual_rollback_requires_review")
            for kind, name, _ in objects:
                quoted = '"' + name.replace('"', '""') + '"'
                target.execute(f"DROP {kind.upper()} IF EXISTS {quoted}")
            for statement in statements:
                if statement not in {"BEGIN TRANSACTION;", "COMMIT;"}:
                    target.execute(statement)
            if sqlite_fingerprint(target) != restored_fingerprint:
                raise RuntimeError("migration_sqlite_rollback_verification_failed")
            target.commit()
            return restored_fingerprint
        except BaseException:
            target.rollback()
            raise
        finally:
            target.execute(f"PRAGMA foreign_keys={original}")


def reconcile_terminal() -> None:
    """终态已落盘但归属尚未释放时补齐任务索引并释放占位"""
    owner = mutation.mutation_owner()
    if not owner or owner.get("kind") != "restore":
        return
    record = load()
    if (
        not record
        or record.get("record_version") != 2
        or record.get("operation_id") != owner.get("operation_id")
        or record.get("state") not in TERMINAL
    ):
        return
    with mutation.mutation_guard(record["operation_id"]):
        write_json_locked(task_path(record["operation_id"]) / "request.json", record)
        mutation.release_mutation(record["operation_id"])
