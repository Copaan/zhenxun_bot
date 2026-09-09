"""Bounded read/prepare work; private inputs arrive exclusively over stdin."""

from __future__ import annotations

import json
from pathlib import Path
import sys

from zhenxun.utils.atomic_json import write_json_locked

from .analysis import execute_analysis
from .errors import MigrationError
from .paths import contained_path


def main():
    raw = sys.stdin.buffer.read(64 * 1024 + 1)
    if len(raw) > 64 * 1024:
        return 2
    request = json.loads(raw)
    project = Path.cwd()
    identity = request["invocation"]
    if len(identity) != 32 or any(c not in "0123456789abcdef" for c in identity):
        return 2
    destination = contained_path(project, f"migration/analysis/{identity}/result.json")
    try:
        value = {"result": execute_analysis(project, request)}
    except MigrationError as error:
        value = {"error": error.code, "status": error.status}
    except Exception:
        value = {"error": "migration_analysis_failed", "status": 500}
    write_json_locked(destination, {"invocation": identity, **value})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
