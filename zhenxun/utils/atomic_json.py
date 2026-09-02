from __future__ import annotations

from collections.abc import Callable
from copy import deepcopy
import json
import os
from pathlib import Path
import tempfile
from typing import Any, TypeVar
import uuid

from filelock import FileLock, Timeout

T = TypeVar("T")


class AtomicJsonLockTimeout(TimeoutError):
    pass


def _lock_path(path: Path) -> Path:
    return path.with_name(f".{path.name}.lock")


def _read_unlocked(path: Path, default: T, *, quarantine_corrupt: bool) -> T:
    if not path.exists():
        return deepcopy(default)
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError, TypeError):
        if quarantine_corrupt:
            corrupt = path.with_name(
                f".{path.name}.corrupt-{os.getpid()}-{uuid.uuid4().hex}"
            )
            try:
                os.replace(path, corrupt)
            except OSError:
                pass
        return deepcopy(default)


def _write_unlocked(path: Path, value: Any) -> None:
    if value in ({}, None):
        path.unlink(missing_ok=True)
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True)
    handle, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".tmp", dir=path.parent
    )
    temporary = Path(temporary_name)
    try:
        with os.fdopen(handle, "w", encoding="utf-8", newline="\n") as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def read_json_locked(
    path: Path,
    default: T,
    *,
    timeout: float = 5.0,
    quarantine_corrupt: bool = False,
) -> T:
    try:
        with FileLock(_lock_path(path), timeout=timeout):
            return _read_unlocked(path, default, quarantine_corrupt=quarantine_corrupt)
    except Timeout as error:
        raise AtomicJsonLockTimeout(str(path)) from error


def write_json_locked(path: Path, value: Any, *, timeout: float = 5.0) -> None:
    try:
        with FileLock(_lock_path(path), timeout=timeout):
            _write_unlocked(path, value)
    except Timeout as error:
        raise AtomicJsonLockTimeout(str(path)) from error


def mutate_json_locked(
    path: Path,
    default: T,
    mutator: Callable[[T], Any],
    *,
    timeout: float = 5.0,
    quarantine_corrupt: bool = False,
) -> Any:
    try:
        with FileLock(_lock_path(path), timeout=timeout):
            value = _read_unlocked(path, default, quarantine_corrupt=quarantine_corrupt)
            result = mutator(value)
            _write_unlocked(path, value)
            return result
    except Timeout as error:
        raise AtomicJsonLockTimeout(str(path)) from error


__all__ = [
    "AtomicJsonLockTimeout",
    "mutate_json_locked",
    "read_json_locked",
    "write_json_locked",
]
