"""Environment file selection shared by runtime and configuration editors."""

from pathlib import Path
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
