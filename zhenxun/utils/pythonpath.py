import os
from pathlib import Path

import zhenxun


def subprocess_pythonpath() -> str:
    """Keep child processes on the currently loaded source installation."""
    source_root = str(Path(zhenxun.__file__).resolve().parent.parent)
    return os.pathsep.join(filter(None, (source_root, os.environ.get("PYTHONPATH"))))
