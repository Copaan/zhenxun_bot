from __future__ import annotations

import importlib.metadata
import json
from pathlib import Path
import platform
import re
import sys

from packaging.utils import canonicalize_name

from .errors import MigrationError
from .paths import contained_path

_MAX_ITEMS = 20_000


def _read_record(root: Path, relative: str) -> dict:
    path = contained_path(root, relative)
    if not path.exists():
        return {}
    if path.stat().st_size > 16 * 1024 * 1024:
        raise MigrationError("migration_plugin_inventory_limit")
    with path.open("rb") as stream:
        data = stream.read(16 * 1024 * 1024 + 1)
    if len(data) > 16 * 1024 * 1024:
        raise MigrationError("migration_plugin_inventory_limit")
    try:
        value = json.loads(data)
        if not isinstance(value, dict) or len(value) > _MAX_ITEMS:
            raise ValueError
        return value
    except (ValueError, TypeError, RecursionError):
        raise MigrationError("migration_plugin_inventory_invalid") from None


def _safe_text(value) -> str | None:
    if (
        isinstance(value, str)
        and len(value) <= 512
        and not any(c in value for c in (":", "/", "\\", "@", "\n", "\r", "\x00"))
    ):
        return value
    return None


def distributions(
    paths: list[str] | None = None,
    *,
    priority: int = 0,
    layer: str = "environment",
    checkpoint=lambda: None,
) -> list[dict]:
    result = []
    installed = (
        importlib.metadata.distributions(path=paths)
        if paths is not None
        else importlib.metadata.distributions()
    )
    for item in installed:
        checkpoint()
        if len(result) >= _MAX_ITEMS:
            raise MigrationError("migration_dependency_inventory_limit")
        name = _safe_text(item.metadata.get("Name"))
        if not name:
            raise MigrationError("migration_distribution_metadata_invalid")
        origin = "index_or_unknown"
        try:
            source = json.loads(item.read_text("direct_url.json") or "{}")
            if source.get("dir_info", {}).get("editable"):
                origin = "editable"
            elif source.get("dir_info") is not None:
                origin = "local_directory"
            elif source.get("vcs_info"):
                origin = "vcs"
            elif source.get("archive_info"):
                origin = "archive"
        except (ValueError, TypeError, OSError, AttributeError):
            origin = "unobserved"
        result.append(
            {
                "name": name,
                "version": _safe_text(item.version),
                "version_evidence": "distribution_metadata",
                "source_type": origin,
                "priority": priority,
                "layer": layer,
                "requires_python": _safe_text(item.metadata.get("Requires-Python")),
                "source_mapping_required": origin in {"editable", "local_directory"},
            }
        )
    return sorted(
        result, key=lambda item: (item["name"].casefold(), item["version"] or "")
    )


def environment_inventory(
    *,
    revision: str | None = None,
    project: Path | None = None,
    paths: list[str] | None = None,
    checkpoint=lambda: None,
) -> dict:
    try:
        version = importlib.metadata.version("zhenxun-bot")
    except importlib.metadata.PackageNotFoundError:
        version = "uninstalled"
    search = list(sys.path if paths is None else paths)
    layer = None
    issues = []
    if project is not None:
        manifest = _read_record(project, "data/runtime/nonebot-store/manifest-v1.json")
        generation = manifest.get("active_generation")
        if generation is not None:
            if type(generation) is not int or generation < 0:
                raise MigrationError("migration_dependency_layer_invalid")
            layer = contained_path(
                project, f"data/runtime/nonebot-site-packages/generation-{generation}"
            )
            if not layer.is_dir():
                issues.append(
                    {"scope": "managed_plugins", "code": "active_layer_missing"}
                )
                layer = None
        layer_root = (project / "data/runtime/nonebot-site-packages").absolute()
        search = [
            p for p in search if not Path(p).absolute().is_relative_to(layer_root)
        ]
    candidates = ([str(layer)] if layer is not None else []) + search
    records = []
    visited = set()
    effective = set()
    for priority, path in enumerate(candidates):
        checkpoint()
        resolved = Path(path).resolve()
        if resolved in visited:
            continue
        visited.add(resolved)
        for item in distributions(
            [str(resolved)],
            priority=priority,
            layer="managed_plugins"
            if layer is not None and resolved == layer
            else "environment",
            checkpoint=checkpoint,
        ):
            key = canonicalize_name(item["name"])
            item["effective"] = key not in effective
            effective.add(key)
            records.append(item)
            if len(records) > _MAX_ITEMS:
                raise MigrationError("migration_dependency_inventory_limit")
    return {
        "core_version": version,
        "git_revision": revision,
        "python": platform.python_version(),
        "implementation": sys.implementation.name,
        "system": platform.system(),
        "architecture": platform.machine(),
        "distributions": records,
        "dependency_inventory_scope": "environment_and_active_layer",
        "dependency_inventory_issues": issues,
    }


def plugin_descriptors(root: Path) -> list[dict]:
    manifest = _read_record(root, "data/runtime/nonebot-store/manifest-v1.json")
    receipts = _read_record(root, "data/runtime/plugin-store-receipts-v1.json")
    try:
        plugins = manifest.get("plugins", {})
        if not isinstance(plugins, dict) or len(plugins) + len(receipts) > _MAX_ITEMS:
            raise ValueError
        # Never carry old generation identifiers, paths, pending operations or URLs.
        result = []
        for source, entries in (("distribution", plugins), ("source", receipts)):
            for key, item in entries.items():
                if (
                    not isinstance(item, dict)
                    or not isinstance(key, str)
                    or not re.fullmatch(r"[\w.-]+(?::[\w.-]+)?", key)
                    or len(key) > 512
                ):
                    raise ValueError
                descriptor = {"key": key, "installation_type": source}
                for field in (
                    "module",
                    "distribution",
                    "runtime_module",
                    "version",
                    "installed_version",
                    "version_source",
                    "source_type",
                    "source",
                ):
                    if (value := _safe_text(item.get(field))) is not None:
                        descriptor[field] = value
                if type(item.get("enabled")) is bool:
                    descriptor["enabled"] = item["enabled"]
                digest = item.get("source_digest")
                if isinstance(digest, str) and re.fullmatch("[a-f0-9]{64}", digest):
                    descriptor["recorded_source_sha256"] = digest
                if source == "source":
                    module = item.get("runtime_module", "")
                    if re.fullmatch(r"zhenxun\.plugins\.[A-Za-z_]\w*", module):
                        relative = module.replace(".", "/")
                        package = contained_path(root, relative)
                        single = contained_path(root, relative + ".py")
                        descriptor["source_state"] = (
                            "present"
                            if package.is_dir() or single.is_file()
                            else "missing"
                        )
                    else:
                        descriptor["source_state"] = "mapping_required"
                    dependencies = item.get("dependency_packages", {})
                    if (
                        not isinstance(dependencies, dict)
                        or len(dependencies) > _MAX_ITEMS
                    ):
                        raise ValueError
                    descriptor["dependency_packages"] = {
                        name: version
                        for name, version in dependencies.items()
                        if _safe_text(name) and _safe_text(version)
                    }
                result.append(descriptor)
        return result
    except (ValueError, TypeError):
        raise MigrationError("migration_plugin_inventory_invalid") from None
