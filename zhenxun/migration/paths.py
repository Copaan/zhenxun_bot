from __future__ import annotations

import os
from pathlib import Path, PurePosixPath
import re
import stat
import unicodedata

from .errors import MigrationError

_DEVICE = re.compile(r"^(con|prn|aux|nul|com[0-9¹²³]|lpt[0-9¹²³])(?:\.|$)", re.I)


def logical_path(value: str) -> str:
    if not isinstance(value, str) or not value or len(value) > 2048:
        raise MigrationError("migration_path_invalid")
    parts = value.split("/")
    if len(parts) > 64 or any(
        part in {"", ".", ".."}
        or len(part) > 255
        or part.endswith((".", " "))
        or _DEVICE.match(part)
        or any(ord(c) < 32 or ord(c) == 127 or c in '\\:<>"|?*' for c in part)
        or unicodedata.normalize("NFC", part) != part
        for part in parts
    ):
        raise MigrationError("migration_path_invalid")
    return PurePosixPath(*parts).as_posix()


def is_link(path: Path) -> bool:
    info = path.lstat()
    return stat.S_ISLNK(info.st_mode) or bool(
        getattr(info, "st_file_attributes", 0) & 0x400
    )


def contained_path(root: Path, relative: str, *, regular: bool = False) -> Path:
    relative = logical_path(relative)
    root = root.absolute()
    # Resolve neither payload symlinks nor Windows junctions before checking them.
    for ancestor in (*reversed(root.parents), root):
        if ancestor.exists() and is_link(ancestor):
            raise MigrationError("migration_link_forbidden")
    current = root
    parts = relative.split("/")
    for index, part in enumerate(parts):
        current /= part
        if os.path.lexists(current):
            if is_link(current):
                raise MigrationError("migration_link_forbidden", path=relative)
            info = current.lstat()
            if index < len(parts) - 1 and not stat.S_ISDIR(info.st_mode):
                raise MigrationError("migration_path_type_conflict", path=relative)
            if not (stat.S_ISDIR(info.st_mode) or stat.S_ISREG(info.st_mode)):
                raise MigrationError("migration_special_file", path=relative)
    if regular and (not current.exists() or not current.is_file()):
        raise MigrationError("migration_file_missing", path=relative)
    return current


class PathIndex:
    def __init__(self) -> None:
        self.files: set[str] = set()
        self.directories: set[str] = set()
        self.spellings: dict[str, str] = {}

    def add(self, value: str) -> str:
        value = logical_path(value)
        folded = value.casefold()
        if folded in self.files or folded in self.directories:
            raise MigrationError("migration_duplicate_path", path=value)
        parts = value.split("/")
        for index in range(1, len(parts) + 1):
            prefix = "/".join(parts[:index])
            key = prefix.casefold()
            previous = self.spellings.setdefault(key, prefix)
            if previous != prefix:
                raise MigrationError("migration_case_conflict", path=value)
            if index < len(parts):
                if key in self.files:
                    raise MigrationError("migration_path_type_conflict", path=value)
                self.directories.add(key)
        self.files.add(folded)
        return value
