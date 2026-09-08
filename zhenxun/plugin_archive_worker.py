"""Private subprocess entrypoint for bounded static inspection, never plugin import."""

from __future__ import annotations

import json
from pathlib import Path
import sys

from zhenxun.plugin_archive import (
    ArchiveError,
    extract_archive,
    identify_plugin,
    payload_digest,
    static_requirements,
)
from zhenxun.plugin_store_receipts import source_digest


def inspect(path: Path, kind: str) -> dict:
    summary = extract_archive(path / "upload", path / "extracted", kind)
    candidate, scopes, metadata = identify_plugin(path / "extracted")
    requirements = static_requirements(scopes, metadata)
    metadata["identification"] = "static_python_source"
    return {
        "summary": summary,
        "candidate": candidate.relative_to(path).as_posix(),
        "metadata": metadata,
        "requirements": requirements,
        "candidate_digest": payload_digest(candidate),
        "source_digest": source_digest(candidate),
    }


def main() -> None:
    path = Path(sys.argv[1])
    try:
        result = inspect(path, sys.argv[2])
    except ArchiveError as error:
        result = {"error": error.code, "status": error.status}
    except Exception:
        result = {"error": "archive_inspection_failed", "status": 400}
    (path / "inspection.json").write_text(json.dumps(result), encoding="utf-8")


if __name__ == "__main__":
    main()
