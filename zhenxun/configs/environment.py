"""Environment file selection shared by runtime and configuration editors."""

from pathlib import Path


def environment_file(
    *,
    template: bool = False,
    preferred: Path = Path(".env.dev"),
    template_path: Path = Path(".env.example"),
) -> Path:
    # The supported launcher explicitly initializes NoneBot with .env.dev.
    target = (
        preferred
        if preferred.exists() or preferred != Path(".env.dev")
        else Path(".env")
    )
    if template and not target.exists():
        return template_path
    return target


def environment_target(source: Path) -> Path:
    return Path(".env.dev") if source.name == ".env.example" else source
