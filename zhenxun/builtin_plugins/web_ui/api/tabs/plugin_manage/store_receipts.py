from __future__ import annotations

from hashlib import sha256
import json
import os
from pathlib import Path
import tempfile
from typing import Any

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
        try:
            data = json.loads(_RECEIPT_FILE.read_text(encoding="utf-8"))
        except (OSError, ValueError, TypeError):
            return {}
        return data if isinstance(data, dict) else {}

    @staticmethod
    def save(data: dict[str, dict[str, Any]]) -> None:
        _RECEIPT_FILE.parent.mkdir(parents=True, exist_ok=True)
        payload = json.dumps(data, ensure_ascii=False, indent=2, sort_keys=True)
        fd, temp_name = tempfile.mkstemp(
            prefix=f".{_RECEIPT_FILE.name}.", dir=_RECEIPT_FILE.parent
        )
        try:
            with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as stream:
                stream.write(payload)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temp_name, _RECEIPT_FILE)
        finally:
            Path(temp_name).unlink(missing_ok=True)

    @classmethod
    def set(cls, store_key: str, receipt: dict[str, Any]) -> None:
        data = cls.load()
        data[store_key] = receipt
        cls.save(data)

    @classmethod
    def delete(cls, store_key: str) -> None:
        data = cls.load()
        if data.pop(store_key, None) is not None:
            cls.save(data)


__all__ = ["StoreReceiptStore", "source_digest"]
