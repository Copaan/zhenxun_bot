import contextlib
import importlib
import json
import os
from pathlib import Path
import site
import sys

import zhenxun

LAYER_ROOT = Path("data") / "runtime" / "nonebot-site-packages"
STORE_ROOT = Path("data") / "runtime" / "nonebot-store"


def subprocess_pythonpath() -> str:
    """Keep child processes on the currently loaded source installation."""
    source_root = str(Path(zhenxun.__file__).resolve().parent.parent)
    return os.pathsep.join(filter(None, (source_root, os.environ.get("PYTHONPATH"))))


def startup_dependency_layer() -> Path | None:
    """启动时应使用的依赖层目录，只使用标准库读取依赖层记录

    依赖层事务等待人工恢复时不使用依赖层。路径相对项目根目录。

    返回:
        Path | None: 依赖层目录，没有可用的依赖层时为 None
    """
    try:
        manifest = json.loads((STORE_ROOT / "manifest-v1.json").read_text("utf-8"))
        generation = manifest.get("active_generation")
    except (OSError, ValueError, AttributeError):
        return None
    with contextlib.suppress(OSError, ValueError, AttributeError):
        pending = json.loads(
            (STORE_ROOT / "pending-transaction-v1.json").read_text("utf-8")
        )
        if pending.get("state") == "recovery_required":
            return None
    if not isinstance(generation, int) or isinstance(generation, bool):
        return None
    path = (LAYER_ROOT / f"generation-{generation}").resolve()
    return path if path.is_dir() else None


def activate_dependency_layer(path: Path | None) -> Path | None:
    """把依赖层目录放到 sys.path 最前面并处理其中的 .pth 文件，同时移除其他依赖层

    只使用标准库，可以在导入任何第三方包之前调用，使依赖层中的版本先于核心虚拟环境
    生效。指定的依赖层已在最前面时不重复处理。

    参数:
        path: 依赖层目录，为 None 时只移除依赖层

    返回:
        Path | None: 已激活的依赖层目录
    """
    root = LAYER_ROOT.resolve()

    def inside(value: str) -> bool:
        try:
            Path(value).resolve().relative_to(root)
        except (OSError, ValueError):
            return False
        return True

    if path is not None and sys.path and sys.path[0] == str(path):
        return path
    sys.path[:] = [item for item in sys.path if not inside(item)]
    if path is not None:
        sys.path.insert(0, str(path))
        # The layer is already first on sys.path, so addsitedir only processes
        # its .pth files (e.g. pywin32, namespace packages).
        site.addsitedir(str(path))
        importlib.invalidate_caches()
    return path
