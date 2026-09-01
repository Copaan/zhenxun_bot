from __future__ import annotations

import asyncio
from collections import deque
from hashlib import sha256
import importlib.metadata
import json
import os
from pathlib import Path
import platform
import re
import sys
import tempfile
from typing import Any
from urllib.parse import quote

import httpx
from packaging.markers import InvalidMarker, Marker, default_environment
from packaging.requirements import InvalidRequirement, Requirement
from packaging.specifiers import SpecifierSet
from packaging.utils import canonicalize_name
from packaging.version import InvalidVersion, Version

try:
    import tomllib
except ModuleNotFoundError:  # pragma: no cover - Python 3.10
    import tomli as tomllib

from .storage import load_manifest

LOCK_FILE = Path("uv.lock")
_REQ_LINE = re.compile(r"^([A-Za-z0-9][A-Za-z0-9._-]*)==([^\s;]+)")
_SENSITIVE = re.compile(r"(?i)(authorization|token|password|secret)=?[^\s]*")


class DependencyAnalysisError(RuntimeError):
    def __init__(self, code: str, message: str | None = None):
        super().__init__(f"{code}: {message}" if message else code)
        self.code = code


def safe_process_error(value: str) -> str:
    value = _SENSITIVE.sub(r"\1=<redacted>", value)
    return " ".join(value.split())[-1200:]


def installed_inventory() -> dict[str, str]:
    result: dict[str, str] = {}
    for distribution in importlib.metadata.distributions():
        name = distribution.metadata.get("Name")
        if name:
            result[canonicalize_name(name)] = distribution.version
    for name, info in load_manifest().get("packages", {}).items():
        if isinstance(info, dict) and info.get("version"):
            result[canonicalize_name(name)] = str(info["version"])
    return result


def _lock_packages() -> tuple[dict[str, set[str]], dict[str, str], set[str]]:
    if not LOCK_FILE.is_file():
        raise DependencyAnalysisError("project_lock_missing")
    data = tomllib.loads(LOCK_FILE.read_text(encoding="utf-8"))
    graph: dict[str, set[str]] = {}
    versions: dict[str, str] = {}
    roots: set[str] = set()
    for package in data.get("package", []):
        if not isinstance(package, dict) or not package.get("name"):
            continue
        name = canonicalize_name(str(package["name"]))
        if package.get("version"):
            versions.setdefault(name, str(package["version"]))
        dependencies = {
            canonicalize_name(str(item["name"]))
            for item in package.get("dependencies", [])
            if isinstance(item, dict)
            and item.get("name")
            and _lock_dependency_enabled(item)
        }
        for optional_dependencies in package.get("optional-dependencies", {}).values():
            dependencies.update(
                canonicalize_name(str(item["name"]))
                for item in optional_dependencies
                if isinstance(item, dict)
                and item.get("name")
                and _lock_dependency_enabled(item)
            )
        graph.setdefault(name, set()).update(dependencies)
        if name == "zhenxun-bot":
            roots.update(dependencies)
    return graph, versions, roots


def _lock_dependency_enabled(item: dict[str, Any]) -> bool:
    marker = str(item.get("marker") or "").strip()
    if not marker:
        return True
    try:
        return Marker(marker).evaluate(default_environment())
    except InvalidMarker:
        return True


def protected_core() -> dict[str, str]:
    graph, lock_versions, roots = _lock_packages()
    protected: set[str] = set()
    queue = deque(roots)
    while queue:
        name = queue.popleft()
        if name in protected:
            continue
        protected.add(name)
        queue.extend(graph.get(name, set()) - protected)
    return {name: lock_versions[name] for name in protected if lock_versions.get(name)}


def environment_drift() -> list[dict[str, str | None]]:
    expected = protected_core()
    current = installed_inventory()
    return [
        {
            "name": name,
            "expected": version,
            "actual": current.get(name),
        }
        for name, version in sorted(expected.items())
        if current.get(name) != version
    ]


def environment_fingerprint(registry_plugin: dict[str, Any]) -> str:
    lock_digest = (
        sha256(LOCK_FILE.read_bytes()).hexdigest() if LOCK_FILE.exists() else "missing"
    )
    inventory = installed_inventory()
    payload = {
        "python": platform.python_version(),
        "implementation": platform.python_implementation(),
        "platform": sys.platform,
        "machine": platform.machine(),
        "lock": lock_digest,
        "inventory": sorted(inventory.items()),
        "project": registry_plugin.get("project_link"),
        "version": registry_plugin.get("version"),
    }
    return sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()


async def fetch_pypi_metadata(project: str, version: str) -> dict[str, Any]:
    safe_project = quote(project, safe="-._")
    safe_version = quote(version, safe="-._+")
    url = f"https://pypi.org/pypi/{safe_project}/{safe_version}/json"
    async with httpx.AsyncClient(timeout=15, follow_redirects=True) as client:
        response = await client.get(url, headers={"Accept": "application/json"})
        if response.status_code == 404:
            raise DependencyAnalysisError("registry_version_not_on_pypi")
        try:
            response.raise_for_status()
            payload = response.json()
        except (httpx.HTTPError, ValueError) as error:
            raise DependencyAnalysisError("pypi_metadata_unavailable") from error
    if not isinstance(payload, dict) or not isinstance(payload.get("info"), dict):
        raise DependencyAnalysisError("pypi_metadata_invalid")
    return payload


def metadata_compatibility(metadata: dict[str, Any]) -> list[dict[str, str]]:
    info = metadata["info"]
    reasons: list[dict[str, str]] = []
    requires_python = str(info.get("requires_python") or "").strip()
    if requires_python:
        try:
            if not SpecifierSet(requires_python).contains(
                Version(platform.python_version()), prereleases=True
            ):
                reasons.append(
                    {"code": "python_version_incompatible", "message": requires_python}
                )
        except (InvalidVersion, ValueError):
            reasons.append(
                {"code": "python_requirement_invalid", "message": requires_python}
            )

    installed = installed_inventory()
    environment = default_environment()
    for raw in info.get("requires_dist") or []:
        try:
            requirement = Requirement(str(raw))
        except InvalidRequirement:
            reasons.append(
                {"code": "dependency_metadata_invalid", "message": str(raw)[:160]}
            )
            continue
        if requirement.marker and not requirement.marker.evaluate(environment):
            continue
        name = canonicalize_name(requirement.name)
        if name == "nonebot":
            reasons.append({"code": "nonebot1_plugin", "message": str(requirement)})
            continue
        if name not in {"nonebot2", "pydantic"} or not requirement.specifier:
            continue
        current = installed.get(name)
        if current and not requirement.specifier.contains(current, prereleases=True):
            reasons.append(
                {
                    "code": f"{name}_version_incompatible",
                    "message": f"{requirement}; current={current}",
                }
            )
    return reasons


def _write_constraints(path: Path, values: dict[str, str]) -> None:
    path.write_text(
        "".join(f"{name}=={version}\n" for name, version in sorted(values.items())),
        encoding="utf-8",
    )


def _parse_compiled(path: Path) -> dict[str, str]:
    result: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        match = _REQ_LINE.match(line.strip())
        if match:
            result[canonicalize_name(match.group(1))] = match.group(2)
    if not result:
        raise DependencyAnalysisError("dependency_plan_empty")
    return result


async def _compile(
    requirements: list[str], constraints: dict[str, str], *, wheels_only: bool
) -> tuple[dict[str, str] | None, str]:
    with tempfile.TemporaryDirectory(prefix="zhenxun_nb_analyze_") as root:
        directory = Path(root)
        source = directory / "requirements.in"
        constraint_file = directory / "constraints.txt"
        output = directory / "requirements.txt"
        source.write_text("\n".join(requirements) + "\n", encoding="utf-8")
        _write_constraints(constraint_file, constraints)
        command = [
            "uv",
            "pip",
            "compile",
            str(source),
            "--output-file",
            str(output),
            "--constraints",
            str(constraint_file),
            "--no-header",
            "--no-annotate",
            "--python",
            sys.executable,
        ]
        if wheels_only:
            command.append("--only-binary=:all:")
        process = await asyncio.create_subprocess_exec(
            *command,
            cwd=str(Path.cwd()),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env={**os.environ, "UV_NO_PROGRESS": "1"},
        )
        stdout, stderr = await process.communicate()
        details = safe_process_error((stderr or stdout).decode(errors="replace"))
        if process.returncode != 0 or not output.exists():
            return None, details
        return _parse_compiled(output), details


def _package_changes(
    resolved: dict[str, str], current: dict[str, str]
) -> dict[str, list[dict[str, str]]]:
    added = []
    changed = []
    for name, version in sorted(resolved.items()):
        existing = current.get(name)
        if existing is None:
            added.append({"name": name, "version": version})
        elif existing != version:
            changed.append({"name": name, "from": existing, "to": version})
    return {"added": added, "changed": changed, "removed": []}


async def solve_install(
    plugin: dict[str, Any], metadata: dict[str, Any]
) -> dict[str, Any]:
    drift = environment_drift()
    if drift:
        details = ", ".join(
            f"{item['name']}={item['actual'] or 'missing'}"
            f" (lock={item['expected']})"
            for item in drift[:8]
        )
        raise DependencyAnalysisError("environment_drift", details)
    manifest = load_manifest()
    active_requirements = [
        f"{item['project_link']}=={item['version']}"
        for item in manifest.get("plugins", {}).values()
        if isinstance(item, dict)
        and item.get("state") == "managed"
        and item.get("project_link") != plugin["project_link"]
    ]
    requirements = [
        *active_requirements,
        f"{plugin['project_link']}=={plugin['version']}",
    ]
    current = installed_inventory()
    core = protected_core()

    resolved, strict_error = await _compile(requirements, current, wheels_only=True)
    source_required = False
    relaxed = False
    if resolved is None:
        relaxed = True
        resolved, wheel_error = await _compile(requirements, core, wheels_only=True)
        if resolved is None:
            resolved, source_error = await _compile(
                requirements, core, wheels_only=False
            )
            if resolved is None:
                candidate = [f"{plugin['project_link']}=={plugin['version']}"]
                candidate_core, candidate_core_error = await _compile(
                    candidate, core, wheels_only=False
                )
                if candidate_core is not None:
                    raise DependencyAnalysisError(
                        "third_party_dependency_conflict",
                        source_error or wheel_error or strict_error,
                    )
                candidate_free, candidate_free_error = await _compile(
                    candidate, {}, wheels_only=False
                )
                if candidate_free is not None:
                    raise DependencyAnalysisError(
                        "core_dependency_conflict",
                        candidate_core_error or source_error or wheel_error,
                    )
                raise DependencyAnalysisError(
                    "plugin_dependency_invalid",
                    candidate_free_error
                    or candidate_core_error
                    or source_error
                    or wheel_error
                    or strict_error,
                )
            source_required = True

    core_changes = [
        {"name": name, "from": core[name], "to": version}
        for name, version in resolved.items()
        if name in core and core[name] != version
    ]
    if core_changes:
        raise DependencyAnalysisError("core_dependency_conflict")
    changes = _package_changes(resolved, current)
    root_files = metadata.get("urls") or []
    pure_root_wheel = any(
        str(item.get("filename", "")).endswith("-py3-none-any.whl")
        or str(item.get("filename", "")).endswith("-py2.py3-none-any.whl")
        for item in root_files
        if isinstance(item, dict)
    )
    return {
        "requirements": requirements,
        "resolved_packages": resolved,
        "package_changes": changes,
        "core_changes": core_changes,
        "non_core_changes": changes["changed"],
        "source_build_required": source_required,
        "pure_python_candidate": pure_root_wheel and not source_required,
        "used_relaxed_resolution": relaxed,
        "resolver_note": strict_error if relaxed else None,
    }


def uninstall_plan(plugin: dict[str, Any]) -> dict[str, Any]:
    manifest = load_manifest()
    packages = {
        canonicalize_name(name): str(info["version"])
        for name, info in manifest.get("packages", {}).items()
        if isinstance(info, dict) and info.get("version")
    }
    packages.pop(canonicalize_name(str(plugin["project_link"])), None)
    return {
        "requirements": [],
        "resolved_packages": packages,
        "package_changes": {
            "added": [],
            "changed": [],
            "removed": [
                {
                    "name": canonicalize_name(str(plugin["project_link"])),
                    "version": str(plugin["version"]),
                }
            ],
        },
        "core_changes": [],
        "non_core_changes": [],
        "source_build_required": False,
        "pure_python_candidate": True,
        "used_relaxed_resolution": False,
        "resolver_note": None,
    }


async def preflight_source_requirements(files: list[Path]) -> dict[str, Any]:
    """Validate source-store requirements before the legacy installer mutates .venv."""
    requirements: list[str] = []
    for path in files:
        for raw_line in path.read_text(encoding="utf-8-sig").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue
            try:
                requirement = Requirement(line)
            except InvalidRequirement as error:
                raise DependencyAnalysisError(
                    "source_plugin_requirement_unsupported", line[:160]
                ) from error
            if requirement.url:
                raise DependencyAnalysisError(
                    "source_plugin_direct_url_forbidden", requirement.name
                )
            if canonicalize_name(requirement.name) == "nonebot":
                raise DependencyAnalysisError("nonebot1_plugin")
            requirements.append(line)
    if not requirements:
        return {
            "resolved_packages": {},
            "package_changes": {"added": [], "changed": [], "removed": []},
        }
    drift = environment_drift()
    if drift:
        raise DependencyAnalysisError("environment_drift")
    current = installed_inventory()
    core = protected_core()
    resolved, error = await _compile(requirements, current, wheels_only=False)
    if resolved is None:
        resolved, error = await _compile(requirements, core, wheels_only=False)
    if resolved is None:
        raise DependencyAnalysisError("core_dependency_conflict", error)
    for name, version in resolved.items():
        if name in core and core[name] != version:
            raise DependencyAnalysisError("core_dependency_conflict")
    return {
        "resolved_packages": resolved,
        "package_changes": _package_changes(resolved, current),
    }
