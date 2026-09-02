from __future__ import annotations

import compileall
from pathlib import Path


def precompile_path(path: Path) -> bool:
    """Best-effort bytecode warmup for the current interpreter."""
    if path.is_dir():
        return bool(compileall.compile_dir(path, quiet=2, force=False))
    if path.suffix == ".py" and path.is_file():
        return bool(compileall.compile_file(path, quiet=2, force=False))
    return True


__all__ = ["precompile_path"]
