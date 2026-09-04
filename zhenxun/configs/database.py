from __future__ import annotations

from pathlib import Path
import re
from urllib.parse import unquote


def is_sqlite_memory_url(value: str) -> bool:
    scheme, separator, raw_path = value.partition(":")
    if not separator or scheme.casefold() != "sqlite":
        return False
    return raw_path.lstrip("/").partition("?")[0].casefold() == ":memory:"


def sqlite_path_from_url(value: str, root: Path | None = None) -> Path:
    """Resolve supported SQLite URLs without treating a relative path as a host."""
    scheme, separator, raw_path = value.partition(":")
    if not separator or scheme.casefold() != "sqlite":
        raise ValueError("database_url_not_sqlite")

    # ``sqlite://data/file.db`` was emitted by the first-setup UI. URL parsers
    # interpret ``data`` as a host, but it was always intended as a relative path.
    if raw_path.startswith("//"):
        raw_path = raw_path[2:]
    raw_path = unquote(raw_path.partition("?")[0])
    if re.match(r"^/[A-Za-z]:[/\\]", raw_path):
        raw_path = raw_path[1:]

    path = Path(raw_path)
    if not path.is_absolute():
        path = (root or Path.cwd()) / path
    return path.resolve()


__all__ = ["is_sqlite_memory_url", "sqlite_path_from_url"]
