from __future__ import annotations

from contextlib import AbstractContextManager
from contextvars import ContextVar
import os
from pathlib import Path
from typing_extensions import Self
import uuid

from filelock import FileLock, Timeout
import psutil

from zhenxun.utils.atomic_json import read_json_locked, write_json_locked

from .access import private_directory
from .errors import MigrationError
from .paths import contained_path

_CURRENT_LEASE: ContextVar[InstanceLease | None] = ContextVar(
    "migration_instance_lease", default=None
)


class InstanceLease(AbstractContextManager):
    def __init__(self, project: Path, *, role: str):
        if role not in {"launcher", "worker", "migration"}:
            raise MigrationError("migration_lease_role_invalid")
        self.project = project.absolute()
        self.root = contained_path(self.project, "migration/control")
        self.role = role
        self.lock: FileLock | None = None
        self.identity = uuid.uuid4().hex

    def __enter__(self) -> Self:
        if self.lock is not None:
            raise MigrationError("migration_instance_lease_reentry")
        private_directory(self.root)
        lock = FileLock(str(self.root / "instance.lock"), timeout=0, thread_local=False)
        try:
            lock.acquire()
        except Timeout:
            raise MigrationError("migration_instance_running", status=409) from None
        try:
            write_json_locked(
                self.root / "owner.json",
                {
                    "schema": 1,
                    "pid": os.getpid(),
                    "created_at": psutil.Process().create_time(),
                    "role": self.role,
                    "identity": self.identity,
                },
            )
            self.lock = lock
            self._token = _CURRENT_LEASE.set(self)
            return self
        except BaseException:
            lock.release()
            raise

    def require_held(self) -> None:
        if self.lock is None or not self.lock.is_locked:
            raise MigrationError("migration_instance_lease_required")
        owner = read_json_locked(self.root / "owner.json", None)
        if (
            not owner
            or owner.get("identity") != self.identity
            or owner.get("pid") != os.getpid()
        ):
            raise MigrationError("migration_instance_lease_lost")

    def __exit__(self, *args) -> None:
        if self.lock is not None:
            try:
                owner = read_json_locked(self.root / "owner.json", None)
                if owner and owner.get("identity") == self.identity:
                    write_json_locked(self.root / "owner.json", None)
            finally:
                self.lock.release()
                self.lock = None
                _CURRENT_LEASE.reset(self._token)


def current_instance_lease() -> InstanceLease:
    lease = _CURRENT_LEASE.get()
    if lease is None:
        raise MigrationError("migration_instance_lease_required")
    lease.require_held()
    return lease


class DelegatedLease:
    """A maintenance child borrows, but never acquires/releases, launcher ownership."""

    role = "migration"

    def __init__(self, project: Path, launcher_pid: int, identity: str):
        self.project = project.absolute()
        self.launcher_pid, self.identity = launcher_pid, identity
        self._owner = validate_delegation(project, launcher_pid, identity=identity)
        self._process_id = os.getpid()
        self._launcher = psutil.Process(launcher_pid)

    def require_held(self) -> None:
        # Ancestry was proved at construction and cannot grant a different child.
        # Check the live process identity and lease on every checkpoint without
        # enumerating the entire Windows ancestor tree for every payload block.
        if os.getpid() != self._process_id:
            raise MigrationError("migration_launcher_identity_mismatch")
        try:
            if (
                not self._launcher.is_running()
                or self._launcher.create_time() != self._owner["created_at"]
            ):
                raise MigrationError("migration_launcher_identity_mismatch")
        except psutil.Error:
            raise MigrationError("migration_launcher_identity_unconfirmed") from None
        owner = _delegated_owner(self.project, self.launcher_pid, self.identity)
        if owner != self._owner:
            raise MigrationError("migration_launcher_identity_mismatch")
        _require_delegated_lock(self.project, owner)


def _delegated_owner(project, launcher_pid, identity):
    root = contained_path(project, "migration/control")
    owner = read_json_locked(root / "owner.json", None) if root.exists() else None
    if not owner or owner.get("pid") != launcher_pid or owner.get("role") != "launcher":
        raise MigrationError("migration_launcher_identity_mismatch")
    if identity is not None and owner.get("identity") != identity:
        raise MigrationError("migration_launcher_identity_mismatch")
    return owner


def _require_delegated_lock(project, owner):
    root = contained_path(project, "migration/control")
    lock = FileLock(str(root / "instance.lock"), timeout=0)
    try:
        lock.acquire()
    except Timeout:
        return owner
    else:
        lock.release()
        raise MigrationError("migration_launcher_lease_missing")


def validate_delegation(
    project: Path, launcher_pid: int, *, identity: str | None = None
) -> dict:
    owner = _delegated_owner(project, launcher_pid, identity)
    try:
        ancestors = {
            parent.pid: parent.create_time() for parent in psutil.Process().parents()
        }
        if ancestors.get(launcher_pid) != owner["created_at"]:
            raise MigrationError("migration_launcher_identity_mismatch")
    except psutil.Error:
        raise MigrationError("migration_launcher_identity_unconfirmed") from None
    return _require_delegated_lock(project, owner)


def require_resolved_restore(project: Path) -> None:
    from .tasks import TaskStore

    active = TaskStore(project).active()
    if active is not None and (
        active.get("action") == "restore" or active.get("stage") == "recovery_required"
    ):
        # Until the maintenance validation entry takes ownership, a normal run
        # must never activate business against a possibly half-restored instance.
        raise MigrationError("migration_startup_recovery_required", status=409)
