from __future__ import annotations

from hashlib import sha256
from pathlib import Path
from typing import Any

from zhenxun.utils.atomic_json import (
    mutate_json_locked,
    read_json_locked,
    write_json_locked,
)

_RECEIPT_FILE = Path("data/runtime/plugin-store-receipts-v1.json")


def source_digest(path: Path) -> str | None:
    if not path.exists():
        return None
    files = (
        [path]
        if path.is_file()
        else sorted(item for item in path.rglob("*") if item.is_file())
    )
    digest = sha256()
    root = path.parent if path.is_file() else path
    try:
        for item in files:
            if "__pycache__" in item.parts or item.suffix in {".pyc", ".pyo"}:
                continue
            digest.update(item.relative_to(root).as_posix().encode("utf-8"))
            digest.update(b"\0")
            digest.update(item.read_bytes())
            digest.update(b"\0")
    except OSError:
        return None
    return digest.hexdigest()


class StoreReceiptStore:
    @staticmethod
    def load() -> dict[str, dict[str, Any]]:
        data = read_json_locked(_RECEIPT_FILE, {})
        return data if isinstance(data, dict) else {}

    @staticmethod
    def save(data: dict[str, dict[str, Any]]) -> None:
        write_json_locked(_RECEIPT_FILE, data)

    @classmethod
    def set(cls, store_key: str, receipt: dict[str, Any]) -> None:
        def update(data: dict[str, dict[str, Any]]) -> None:
            data[store_key] = receipt

        mutate_json_locked(_RECEIPT_FILE, {}, update)

    @classmethod
    def delete(cls, store_key: str) -> None:
        mutate_json_locked(
            _RECEIPT_FILE,
            {},
            lambda data: data.pop(store_key, None),
        )


__all__ = ["StoreReceiptStore", "source_digest"]
