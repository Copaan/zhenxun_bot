"""Environment file selection shared by runtime and configuration editors."""

from pathlib import Path


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
