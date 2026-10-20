"""Environment file selection shared by runtime and configuration editors."""

from contextlib import closing
from pathlib import Path
import shutil
import sqlite3
import sys


def environment_file(
    *,
    template: bool = False,
    preferred: Path = Path(".env.dev"),
    template_path: Path = Path(".env.example"),
    root: Path | None = None,
) -> Path:
    if root is not None:
        preferred = root / preferred
        template_path = root / template_path
    # Launcher and editors prefer .env.dev and share the same .env fallback.
    target = (
        preferred
        if preferred.exists() or preferred != (root or Path()) / ".env.dev"
        else (root or Path()) / ".env"
    )
    if template and not target.exists():
        return template_path
    return target


def environment_target(source: Path) -> Path:
    return Path(".env.dev") if source.name == ".env.example" else source


def _log(message: str) -> None:
    sys.stderr.write(f"[zx] {message}\n")
    sys.stderr.flush()


def sync_env_missing_items(project_root: Path) -> None:
    """从 .env.example 补齐 .env.dev 缺失的配置项，不改动已有值

    参数:
        project_root: 项目目录
    """
    example_path = project_root / ".env.example"
    env_path = project_root / ".env.dev"
    if not example_path.exists():
        return
    if not env_path.exists():
        env_path.write_text(example_path.read_text(encoding="utf-8"), encoding="utf-8")
        _log("已根据 .env.example 生成 .env.dev")
        return

    from io import StringIO

    from dotenv.parser import parse_stream

    content = env_path.read_text(encoding="utf-8")
    existing_values = {
        item.key: item.value for item in parse_stream(StringIO(content)) if item.key
    }
    additions: list[str] = []
    added_keys: list[str] = []
    comments: list[str] = []
    for item in parse_stream(StringIO(example_path.read_text(encoding="utf-8"))):
        if item.error:
            raise ValueError("env_example_parse_failed")
        if item.key is None:
            comments.append(item.original.string)
            continue
        if item.key not in existing_values:
            additions.append("".join(comments) + item.original.string)
            existing_values[item.key] = item.value
            added_keys.append(item.key)
        comments.clear()
    if not additions:
        return
    env_path.write_text(
        content.rstrip("\r\n") + "\n\n" + "".join(additions), encoding="utf-8"
    )
    _log(f"已补齐 .env.dev 缺失配置: {', '.join(added_keys)}")


def migrate_orm_data_dir(root: Path | None = None) -> None:
    """首次使用 .env 指定的 nonebot-plugin-orm 数据目录时复制旧数据

    目录由 LOCALSTORE_PLUGIN_DATA_DIR 中的 nonebot_plugin_orm 指定；未指定、目录已存在
    或与 localstore 默认目录相同时不做处理。旧数据只复制不移动，复制失败时本进程继续
    使用默认目录。需在 nonebot.init 之后、localstore 导入之前调用。

    参数:
        root: 项目根目录
    """
    import nonebot

    try:
        from nonestorage import user_data_dir
    except ImportError:
        return
    root = root or Path.cwd()
    config = nonebot.get_driver().config
    plugin_dirs = dict(getattr(config, "localstore_plugin_data_dir", None) or {})
    if "nonebot_plugin_orm" not in plugin_dirs:
        return
    target = Path(plugin_dirs["nonebot_plugin_orm"])
    target = (target if target.is_absolute() else root / target).resolve()
    if custom := getattr(config, "localstore_data_dir", None):
        base = Path(custom)
    elif getattr(config, "localstore_use_cwd", False):
        base = root / "data"
    else:
        base = user_data_dir("nonebot2")
    legacy = (base if base.is_absolute() else root / base).resolve()
    legacy /= "nonebot_plugin_orm"
    if target.exists() or target == legacy or not legacy.is_dir():
        return
    staging = target.with_name(f"{target.name}.part")
    try:
        shutil.rmtree(staging, ignore_errors=True)
        shutil.copytree(
            legacy,
            staging,
            ignore=shutil.ignore_patterns("__pycache__", "db.sqlite3*"),
        )
        if (legacy / "db.sqlite3").is_file():
            with (
                closing(sqlite3.connect(legacy / "db.sqlite3")) as source,
                closing(sqlite3.connect(staging / "db.sqlite3")) as copy,
            ):
                source.backup(copy)
        staging.rename(target)
    except Exception as error:
        shutil.rmtree(staging, ignore_errors=True)
        plugin_dirs["nonebot_plugin_orm"] = legacy
        config.localstore_plugin_data_dir = plugin_dirs
        _log(f"复制 nonebot-plugin-orm 旧数据失败，本次继续使用 {legacy}: {error}")
        return
    _log(f"已将 nonebot-plugin-orm 数据从 {legacy} 复制到 {target}")
