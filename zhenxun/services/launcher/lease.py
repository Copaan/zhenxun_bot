"""实例锁：同一项目目录只允许一个 launcher 运行，子进程须由持锁的 launcher 启动"""

from __future__ import annotations

from contextlib import AbstractContextManager
import os
from pathlib import Path
from typing_extensions import Self
import uuid

from filelock import BaseFileLock, FileLock, Timeout
import psutil

from zhenxun.utils.atomic_json import read_json_locked, write_json_locked

LEASE_DIR = Path("data/runtime/instance")


class LeaseError(RuntimeError):
    """实例锁校验失败"""


class InstanceLease(AbstractContextManager):
    """持有项目目录的实例锁，并记录持有者进程"""

    def __init__(self, project: Path, *, role: str):
        self.root = project.absolute() / LEASE_DIR
        self.role = role
        self.lock: BaseFileLock | None = None
        self.identity = uuid.uuid4().hex

    def __enter__(self) -> Self:
        self.root.mkdir(parents=True, exist_ok=True)
        lock = FileLock(str(self.root / "instance.lock"), timeout=0, thread_local=False)
        try:
            lock.acquire()
        except Timeout:
            raise LeaseError("该目录下已有真寻实例在运行") from None
        try:
            write_json_locked(
                self.root / "owner.json",
                {
                    "pid": os.getpid(),
                    "created_at": psutil.Process().create_time(),
                    "role": self.role,
                    "identity": self.identity,
                },
            )
        except BaseException:
            lock.release()
            raise
        self.lock = lock
        return self

    def __exit__(self, *args) -> None:
        if self.lock is None:
            return
        try:
            owner = read_json_locked(self.root / "owner.json", None)
            if owner and owner.get("identity") == self.identity:
                write_json_locked(self.root / "owner.json", None)
        finally:
            self.lock.release()
            self.lock = None


def validate_delegation(project: Path, launcher_pid: int, identity: str) -> None:
    """校验当前进程由持有实例锁的 launcher 启动

    参数:
        project: 项目目录
        launcher_pid: launcher 进程号
        identity: launcher 持锁时生成的标识
    """
    root = project.absolute() / LEASE_DIR
    owner = read_json_locked(root / "owner.json", None) if root.exists() else None
    if (
        not owner
        or owner.get("pid") != launcher_pid
        or owner.get("role") != "launcher"
        or owner.get("identity") != identity
    ):
        raise LeaseError("launcher_identity_mismatch")
    try:
        ancestors = {
            parent.pid: parent.create_time() for parent in psutil.Process().parents()
        }
    except psutil.Error:
        raise LeaseError("launcher_identity_unconfirmed") from None
    if ancestors.get(launcher_pid) != owner["created_at"]:
        raise LeaseError("launcher_identity_mismatch")
    lock = FileLock(str(root / "instance.lock"), timeout=0)
    try:
        lock.acquire()
    except Timeout:
        return
    lock.release()
    raise LeaseError("launcher_lease_missing")
