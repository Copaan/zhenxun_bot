from __future__ import annotations

from collections.abc import Callable
from dataclasses import asdict, dataclass, field
import hashlib
import json
import os
from pathlib import Path
import stat
import subprocess

from .errors import MigrationError
from .paths import PathIndex, contained_path, is_link, logical_path

CATEGORIES = frozenset({"configuration", "data", "plugins", "resources"})
_EXCLUDED_DIRS = frozenset(
    {
        ".git",
        ".hg",
        ".svn",
        ".venv",
        "venv",
        "env",
        "virtualenv",
        "__pycache__",
        ".cache",
        "cache",
        ".pytest_cache",
        ".ruff_cache",
        ".mypy_cache",
        "node_modules",
        "log",
        "logs",
        "backup",
        "backups",
        "migration",
        "temp",
        "tmp",
        ".tox",
        ".nox",
    }
)
_CORE_DIRS = frozenset(
    {"tests", "test", "scripts", "docs", "envs", ".github", "dist", "build"}
)
_RUNTIME_PREFIXES = ("data/runtime", "data/web_ui", "resources/temp")
_SESSION_NAMES = frozenset(
    {".restart_state.json", "restart-state.json", "session.json", "sessions.json"}
)


@dataclass(frozen=True)
class SourceBoundary:
    tracked: frozenset[str]
    source: str
    revision: str | None

    @classmethod
    def read(cls, root: Path) -> SourceBoundary:
        try:
            top = (
                subprocess.run(
                    ["git", "-C", str(root), "rev-parse", "--show-toplevel"],
                    capture_output=True,
                    check=True,
                    timeout=10,
                )
                .stdout.decode("utf-8")
                .strip()
            )
            if Path(top).resolve() != root.resolve():
                raise MigrationError("migration_git_root_mismatch")
            paths = (
                subprocess.run(
                    ["git", "-C", str(root), "ls-files", "--cached", "-z"],
                    capture_output=True,
                    check=True,
                    timeout=30,
                )
                .stdout.decode("utf-8")
                .split("\0")
            )
            revision = subprocess.run(
                ["git", "-C", str(root), "rev-parse", "HEAD"],
                capture_output=True,
                timeout=10,
            )
            return cls(
                frozenset(logical_path(p).casefold() for p in paths if p),
                "git_index",
                revision.stdout.decode("ascii").strip()
                if revision.returncode == 0
                else None,
            )
        except (OSError, subprocess.SubprocessError, UnicodeError):
            if (root / ".git").exists():
                raise MigrationError("migration_git_index_unavailable") from None
        inventory = contained_path(root, "zhenxun/migration/core-files-v1.json")
        try:
            if inventory.stat().st_size > 16 * 1024 * 1024:
                raise ValueError
            value = json.loads(inventory.read_text("utf-8"))
            if value["schema"] != 1 or not isinstance(value["files"], list):
                raise ValueError
            paths = frozenset(logical_path(p).casefold() for p in value["files"])
            if not {"pyproject.toml", "zhenxun/cli.py"} <= paths:
                raise ValueError
            return cls(paths, "release_inventory", value.get("revision"))
        except (OSError, ValueError, KeyError, TypeError):
            raise MigrationError("migration_source_boundary_unknown") from None

    def protected(self, relative: str) -> bool:
        path = logical_path(relative).casefold()
        parts = path.split("/")
        return (
            path in self.tracked
            or any(part in _EXCLUDED_DIRS for part in parts)
            or parts[0] in _CORE_DIRS
            or (
                parts[0] == "zhenxun"
                and path != "zhenxun/plugins"
                and not path.startswith("zhenxun/plugins/")
            )
            or any(path == p or path.startswith(p + "/") for p in _RUNTIME_PREFIXES)
            or parts[-1] in _SESSION_NAMES
            or parts[-1].endswith((".zx", ".pyc", ".pyo", ".lock", ".log"))
        )


@dataclass(frozen=True)
class FileEntry:
    root: str
    path: str
    category: str
    size: int
    mtime_ns: int


@dataclass
class Scan:
    boundary: SourceBoundary
    files: list[FileEntry] = field(default_factory=list)
    excluded: list[dict] = field(default_factory=list)

    def public(self) -> dict:
        categories = {}
        for category in sorted(CATEGORIES):
            files = [f for f in self.files if f.category == category]
            categories[category] = {
                "count": len(files),
                "bytes": sum(f.size for f in files),
            }
        entries = [asdict(f) for f in self.files]
        revision = hashlib.sha256(
            json.dumps(entries, sort_keys=True, ensure_ascii=True).encode()
        ).hexdigest()
        return {
            "source_boundary": self.boundary.source,
            "source_revision": self.boundary.revision,
            "selection_revision": revision,
            "categories": categories,
            "files": entries,
            "excluded": self.excluded,
            "contains_credentials": True,
        }


def category_for(relative: str) -> str | None:
    path = relative.casefold()
    name = path.rsplit("/", 1)[-1]
    if path.startswith("zhenxun/plugins/") or path.startswith("plugins/"):
        return "plugins"
    if path.startswith("resources/"):
        return "resources"
    if name.startswith(".env") or name in {"config.yaml", "config.yml"}:
        return "configuration"
    if path.startswith(("data/config/", "data/configs/", "certs/", "certificates/")):
        return "configuration"
    if path.startswith("data/"):
        return "data"
    return None


def bounded_walk(root: Path, *, max_inspected: int = 600_000, checkpoint=lambda: None):
    """Like top-down os.walk, but bound allocation while reading each directory."""
    inspected = 0

    def visit(directory: Path):
        nonlocal inspected
        names, directories = [], []
        with os.scandir(directory) as iterator:
            for entry in iterator:
                checkpoint()
                inspected += 1
                if inspected > max_inspected:
                    raise MigrationError("migration_scan_work_limit")
                logical_path((Path(entry.path).relative_to(root)).as_posix())
                if entry.is_dir(follow_symlinks=False):
                    directories.append(entry.name)
                else:
                    names.append(entry.name)
        directories.sort()
        names.sort()
        yield directory, directories, names
        for name in directories:
            relative = (directory / name).relative_to(root).as_posix()
            yield from visit(contained_path(root, relative))

    contained_path(root, "__migration_walk_check__")
    yield from visit(root)


def scan_project(
    root: Path,
    *,
    max_entries: int = 200_000,
    max_inspected: int = 600_000,
    checkpoint: Callable[[], None] = lambda: None,
) -> Scan:
    if max_entries <= 0 or max_inspected <= 0:
        raise MigrationError("migration_scan_limit_invalid")
    boundary = SourceBoundary.read(root)
    result = Scan(boundary)
    index = PathIndex()
    inspected = 0

    def visit(relative: str = "") -> None:
        nonlocal inspected
        directory = contained_path(root, relative) if relative else root
        with os.scandir(directory) as iterator:
            entries = []
            for entry in iterator:
                checkpoint()
                inspected += 1
                if inspected > max_inspected:
                    raise MigrationError("migration_scan_work_limit")
                entries.append(entry)
            for entry in sorted(entries, key=lambda e: e.name.casefold()):
                checkpoint()
                path = f"{relative}/{entry.name}" if relative else entry.name
                logical_path(path)
                source = Path(entry.path)
                if is_link(source):
                    result.excluded.append({"path": path, "reason": "link"})
                    continue
                directory_entry = entry.is_dir(follow_symlinks=False)
                parts = path.casefold().split("/")
                excluded = (
                    boundary.protected(path)
                    or (directory_entry and parts[-1] in _EXCLUDED_DIRS)
                    or (directory_entry and (source / "pyvenv.cfg").is_file())
                )
                if path == "zhenxun":
                    excluded = False
                if excluded:
                    result.excluded.append({"path": path, "reason": "protected"})
                    continue
                if directory_entry:
                    visit(path)
                    continue
                if not stat.S_ISREG(entry.stat(follow_symlinks=False).st_mode):
                    result.excluded.append({"path": path, "reason": "special_file"})
                    continue
                category = category_for(path)
                if not category:
                    result.excluded.append({"path": path, "reason": "unclassified"})
                    continue
                index.add(path)
                info = source.stat()
                if len(result.files) >= max_entries:
                    raise MigrationError("migration_entry_limit")
                result.files.append(
                    FileEntry("project", path, category, info.st_size, info.st_mtime_ns)
                )

    contained_path(root, "__migration_boundary_check__")
    visit()
    return result


def discover_inbox(root: Path) -> list[dict]:
    result = []
    for relative in (None, "migration/inbox"):
        directory = root if relative is None else contained_path(root, relative)
        if not directory.is_dir():
            continue
        with os.scandir(directory) as entries:
            for entry in entries:
                if (
                    entry.name.lower().endswith(".zx")
                    and entry.is_file(follow_symlinks=False)
                    and not is_link(Path(entry.path))
                ):
                    result.append(
                        {
                            "path": (f"{relative}/" if relative else "") + entry.name,
                            "size": entry.stat(follow_symlinks=False).st_size,
                        }
                    )
                if len(result) >= 100:
                    return result
    return sorted(result, key=lambda value: value["path"])
