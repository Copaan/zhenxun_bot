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
import shutil
import subprocess
import sys
import tempfile
from typing import Any, Literal
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

from .storage import LAYER_ROOT, generation_path, load_manifest

LOCK_FILE = Path("uv.lock")
_REQ_LINE = re.compile(r"^([A-Za-z0-9][A-Za-z0-9._-]*)==([^\s;]+)")
_SENSITIVE = re.compile(r"(?i)(authorization|token|password|secret)=?[^\s]*")
PROJECT_FILE = Path("pyproject.toml")
PROJECT_NAME = "zhenxun-bot"
IMMUTABLE_ANCHORS = frozenset(
    {
        "nonebot2",
        "pydantic",
        "pydantic-core",
        "pydantic-settings",
        "fastapi",
        "uvicorn",
        "starlette",
        "nonebot-adapter-onebot",
        "nonebot-adapter-qq",
        "nonebot-plugin-alconna",
        "nonebot-plugin-apscheduler",
        "nonebot-plugin-session",
        "nonebot-plugin-uninfo",
        "nonebot-plugin-waiter",
        "nonebot-plugin-htmlrender",
        "tortoise-orm",
        "asyncpg",
        "aiomysql",
        "pymysql",
        "redis",
        "aiocache",
    }
)
FORBIDDEN_LAYER_PACKAGES = frozenset({PROJECT_NAME, "pip", "setuptools", "wheel", "uv"})
DependencyTier = Literal[
    "immutable_core", "shared_compatible", "plugin_private", "external_extra"
]


class DependencyAnalysisError(RuntimeError):
    def __init__(self, code: str, message: str | None = None, *, details: Any = None):
        super().__init__(f"{code}: {message}" if message else code)
        self.code = code
        self.details = details


def safe_process_error(value: str) -> str:
    value = _SENSITIVE.sub(r"\1=<redacted>", value)
    return " ".join(value.split())[-1200:]


def _distribution_inventory(*, include_layer: bool) -> dict[str, str]:
    result: dict[str, str] = {}
    layer_root = LAYER_ROOT.resolve()
    for distribution in importlib.metadata.distributions():
        name = distribution.metadata.get("Name")
        if not name:
            continue
        if not include_layer:
            try:
                location = Path(distribution.locate_file("")).resolve()
                if location.is_relative_to(layer_root):
                    continue
            except (OSError, ValueError):
                pass
        result[canonicalize_name(name)] = distribution.version
    return result


def base_installed_inventory() -> dict[str, str]:
    return _distribution_inventory(include_layer=False)


def layer_inventory() -> dict[str, str]:
    manifest = load_manifest()
    active_generation = manifest.get("active_generation")
    if not isinstance(active_generation, int):
        return {}
    active_path = generation_path(active_generation)
    if not active_path.is_dir():
        return {}
    result: dict[str, str] = {}
    for distribution in importlib.metadata.distributions(path=[str(active_path)]):
        name = distribution.metadata.get("Name")
        if name:
            result[canonicalize_name(name)] = distribution.version
    return result


def installed_inventory() -> dict[str, str]:
    return {**base_installed_inventory(), **layer_inventory()}


def _marker_enabled(marker: Any) -> bool:
    text = str(marker or "").strip()
    if not text:
        return True
    try:
        return Marker(text).evaluate(default_environment())
    except InvalidMarker:
        return True


def _lock_package_enabled(package: dict[str, Any]) -> bool:
    markers = package.get("resolution-markers") or []
    if not markers:
        return True
    return any(_marker_enabled(marker) for marker in markers)


def _lock_packages() -> tuple[dict[str, set[str]], dict[str, str], set[str]]:
    if not LOCK_FILE.is_file():
        raise DependencyAnalysisError("project_lock_missing")
    data = tomllib.loads(LOCK_FILE.read_text(encoding="utf-8"))
    packages: dict[str, dict[str, Any]] = {}
    for package in data.get("package", []):
        if (
            not isinstance(package, dict)
            or not package.get("name")
            or not _lock_package_enabled(package)
        ):
            continue
        name = canonicalize_name(str(package["name"]))
        packages[name] = package

    graph: dict[str, set[str]] = {}
    versions: dict[str, str] = {
        name: str(package["version"])
        for name, package in packages.items()
        if package.get("version")
    }
    selected_extras: dict[str, set[str]] = {}
    visited: set[str] = set()
    queue: deque[tuple[str, frozenset[str]]] = deque([(PROJECT_NAME, frozenset())])
    while queue:
        name, extras = queue.popleft()
        previous_extras = selected_extras.setdefault(name, set())
        new_extras = set(extras) - previous_extras
        first_visit = name not in visited
        if not first_visit and not new_extras:
            continue
        visited.add(name)
        previous_extras.update(extras)
        package = packages.get(name, {})
        dependencies = list(package.get("dependencies", [])) if first_visit else []
        optional = package.get("optional-dependencies", {})
        for extra in extras if first_visit else new_extras:
            dependencies.extend(optional.get(extra, []))
        for item in dependencies:
            if (
                not isinstance(item, dict)
                or not item.get("name")
                or not _lock_dependency_enabled(item)
            ):
                continue
            dependency = canonicalize_name(str(item["name"]))
            graph.setdefault(name, set()).add(dependency)
            queue.append(
                (
                    dependency,
                    frozenset(str(extra) for extra in item.get("extra", [])),
                )
            )
    roots = set(graph.get(PROJECT_NAME, set()))
    return graph, versions, roots


def _lock_dependency_enabled(item: dict[str, Any]) -> bool:
    return _marker_enabled(item.get("marker"))


def project_closure() -> dict[str, str]:
    graph, versions, roots = _lock_packages()
    selected: set[str] = set()
    queue = deque(roots)
    while queue:
        name = queue.popleft()
        if name in selected:
            continue
        selected.add(name)
        queue.extend(graph.get(name, set()) - selected)
    return {name: versions[name] for name in selected if versions.get(name)}


def protected_core() -> dict[str, str]:
    graph, lock_versions, _ = _lock_packages()
    closure = project_closure()
    protected: set[str] = set()
    queue = deque(
        name for name in IMMUTABLE_ANCHORS | FORBIDDEN_LAYER_PACKAGES if name in closure
    )
    while queue:
        name = queue.popleft()
        if name in protected:
            continue
        protected.add(name)
        queue.extend(graph.get(name, set()) - protected)
    return {name: lock_versions[name] for name in protected if lock_versions.get(name)}


def shared_dependencies() -> dict[str, str]:
    closure = project_closure()
    immutable = protected_core()
    return {name: version for name, version in closure.items() if name not in immutable}


def project_requirements() -> list[str]:
    if not PROJECT_FILE.is_file():
        raise DependencyAnalysisError("project_metadata_missing")
    data = tomllib.loads(PROJECT_FILE.read_text(encoding="utf-8"))
    values = data.get("project", {}).get("dependencies", [])
    result: list[str] = []
    for raw in values:
        try:
            requirement = Requirement(str(raw))
        except InvalidRequirement as error:
            raise DependencyAnalysisError(
                "project_dependency_invalid", str(raw)[:160]
            ) from error
        if requirement.marker and not requirement.marker.evaluate(
            default_environment()
        ):
            continue
        result.append(str(requirement))
    return result


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


def _version_relation(actual: str | None, expected: str) -> str:
    if actual is None:
        return "missing"
    try:
        actual_version = Version(actual)
        expected_version = Version(expected)
    except InvalidVersion:
        return "version_mismatch"
    if actual_version > expected_version:
        return "newer"
    if actual_version < expected_version:
        return "older"
    return "matched"


def _project_requirement_map() -> dict[str, Requirement]:
    result: dict[str, Requirement] = {}
    for raw in project_requirements():
        requirement = Requirement(raw)
        result[canonicalize_name(requirement.name)] = requirement
    return result


def _project_venv_active() -> bool:
    expected = (Path.cwd() / ".venv").resolve()
    try:
        return Path(sys.prefix).resolve() == expected
    except OSError:
        return False


def environment_report(*, check_lock: bool = False) -> dict[str, Any]:
    immutable = protected_core()
    shared = shared_dependencies()
    base = base_installed_inventory()
    layer = layer_inventory()
    requirements = _project_requirement_map()
    immutable_drift: list[dict[str, Any]] = []
    compatible_shared_drift: list[dict[str, Any]] = []
    incompatible_shared_drift: list[dict[str, Any]] = []

    for name, expected in sorted(immutable.items()):
        actual = base.get(name)
        if actual == expected:
            continue
        immutable_drift.append(
            {
                "name": name,
                "tier": "immutable_core",
                "expected": expected,
                "actual": actual,
                "kind": _version_relation(actual, expected),
            }
        )

    for name, expected in sorted(shared.items()):
        actual = base.get(name)
        if actual == expected:
            continue
        requirement = requirements.get(name)
        compatible = bool(actual)
        if compatible and requirement and requirement.specifier:
            try:
                compatible = requirement.specifier.contains(
                    Version(str(actual)), prereleases=True
                )
            except InvalidVersion:
                compatible = False
        item = {
            "name": name,
            "tier": "shared_compatible",
            "expected": expected,
            "actual": actual,
            "kind": _version_relation(actual, expected),
            "requirement": str(requirement) if requirement else None,
        }
        target = compatible_shared_drift if compatible else incompatible_shared_drift
        target.append(item)

    project_names = set(immutable) | set(shared) | {PROJECT_NAME}
    external = [
        {
            "name": name,
            "tier": "external_extra",
            "actual": version,
            "kind": "extra",
        }
        for name, version in sorted(base.items())
        if name not in project_names and name not in layer
    ]
    interpreter_mismatch = not _project_venv_active()
    lock_stale = False
    if check_lock:
        uv = shutil.which("uv")
        if uv is None:
            lock_stale = True
        else:
            checked = subprocess.run(
                [uv, "lock", "--check"],
                cwd=Path.cwd(),
                capture_output=True,
                timeout=30,
                check=False,
            )
            lock_stale = checked.returncode != 0
    if lock_stale:
        status = "project_lock_stale"
    elif interpreter_mismatch:
        status = "interpreter_mismatch"
    elif immutable_drift:
        status = "immutable_drift"
    elif incompatible_shared_drift:
        status = "incompatible_shared_drift"
    elif compatible_shared_drift:
        status = "compatible_shared_drift"
    elif external:
        status = "extra_packages"
    else:
        status = "healthy"
    lock_digest = (
        sha256(LOCK_FILE.read_bytes()).hexdigest() if LOCK_FILE.exists() else "missing"
    )
    payload = {
        "status": status,
        "python": platform.python_version(),
        "platform": sys.platform,
        "immutable_count": len(immutable),
        "shared_count": len(shared),
        "immutable_drift": immutable_drift,
        "compatible_shared_drift": compatible_shared_drift,
        "incompatible_shared_drift": incompatible_shared_drift,
        "extra_packages": external,
        "extra_count": len(external),
        "interpreter_mismatch": interpreter_mismatch,
        "project_lock_stale": lock_stale,
        "repairable": bool(
            not interpreter_mismatch
            and not lock_stale
            and (immutable_drift or incompatible_shared_drift)
            and LOCK_FILE.is_file()
        ),
        "repair_command": "uv sync --locked --inexact",
        "lock_digest": lock_digest,
        "layer_packages": len(layer),
    }
    payload["fingerprint"] = sha256(
        json.dumps(
            {
                "lock": lock_digest,
                "base": sorted(base.items()),
                "layer": sorted(layer.items()),
                "python": payload["python"],
                "platform": payload["platform"],
                "lock_stale": lock_stale,
            },
            sort_keys=True,
        ).encode()
    ).hexdigest()
    return payload


async def preflight_environment_repair() -> tuple[bool, str]:
    process = await asyncio.create_subprocess_exec(
        "uv",
        "sync",
        "--locked",
        "--inexact",
        "--dry-run",
        cwd=str(Path.cwd()),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        env={**os.environ, "UV_NO_PROGRESS": "1"},
    )
    stdout, stderr = await process.communicate()
    detail = safe_process_error((stderr or stdout).decode(errors="replace"))
    return process.returncode == 0, detail


def environment_fingerprint(registry_plugin: dict[str, Any]) -> str:
    lock_digest = (
        sha256(LOCK_FILE.read_bytes()).hexdigest() if LOCK_FILE.exists() else "missing"
    )
    report = environment_report()
    payload = {
        "python": platform.python_version(),
        "implementation": platform.python_implementation(),
        "platform": sys.platform,
        "machine": platform.machine(),
        "lock": lock_digest,
        "environment": report["fingerprint"],
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


def _active_metadata_requirements(metadata: dict[str, Any]) -> list[Requirement]:
    result: list[Requirement] = []
    for raw in metadata.get("info", {}).get("requires_dist") or []:
        try:
            requirement = Requirement(str(raw))
        except InvalidRequirement as error:
            raise DependencyAnalysisError(
                "dependency_metadata_invalid", str(raw)[:160]
            ) from error
        if requirement.marker and not requirement.marker.evaluate(
            default_environment()
        ):
            continue
        result.append(requirement)
    return result


def _soft_upper_mismatch(specifier: SpecifierSet, current: Version) -> bool:
    if specifier.contains(current, prereleases=True):
        return False
    upper_mismatch = False
    for item in specifier:
        operator = item.operator
        if operator in {"!=", "==="}:
            return False
        raw = item.version.rstrip(".*")
        try:
            boundary = Version(raw)
        except InvalidVersion:
            return False
        if operator in {">", ">="} and not item.contains(current, prereleases=True):
            return False
        if operator in {"<", "<="} and not item.contains(current, prereleases=True):
            upper_mismatch = current >= boundary
        elif operator in {"==", "~="} and not item.contains(current, prereleases=True):
            if current <= boundary:
                return False
            upper_mismatch = True
    return upper_mismatch


def _compatibility_overrides(
    metadata: dict[str, Any],
    *,
    immutable: dict[str, str],
    shared: dict[str, str],
    current: dict[str, str],
) -> tuple[list[dict[str, str]], list[str]]:
    overrides: list[dict[str, str]] = []
    rewritten: list[str] = []
    for requirement in _active_metadata_requirements(metadata):
        name = canonicalize_name(requirement.name)
        actual = current.get(name)
        if not requirement.specifier or not actual:
            rewritten.append(str(requirement))
            continue
        try:
            actual_version = Version(actual)
        except InvalidVersion:
            rewritten.append(str(requirement))
            continue
        if requirement.specifier.contains(actual_version, prereleases=True):
            rewritten.append(str(requirement))
            continue
        detail = {
            "name": name,
            "declared_requirement": str(requirement),
            "effective_version": actual,
        }
        if name in immutable:
            raise DependencyAnalysisError(
                "core_dependency_conflict",
                f"{requirement}; current={actual}",
                details=[{**detail, "tier": "immutable_core"}],
            )
        if (
            name not in shared
            or requirement.url
            or not _soft_upper_mismatch(requirement.specifier, actual_version)
        ):
            rewritten.append(str(requirement))
            continue
        overrides.append(
            {
                **detail,
                "tier": "shared_compatible",
                "risk": "unverified_runtime_compatibility",
            }
        )
        rewritten.append(f"{name}=={actual}")
    return overrides, rewritten


async def solve_install(
    plugin: dict[str, Any], metadata: dict[str, Any]
) -> dict[str, Any]:
    report = environment_report()
    blocking_drift = [
        *(
            {
                **item,
                "tier": "immutable_core",
                "kind": _version_relation(item.get("actual"), item["expected"]),
            }
            for item in environment_drift()
        ),
        *report["incompatible_shared_drift"],
    ]
    if report["interpreter_mismatch"]:
        raise DependencyAnalysisError(
            "interpreter_mismatch", details={"command": "uv run zx"}
        )
    if blocking_drift:
        raise DependencyAnalysisError(
            "environment_drift",
            ", ".join(
                f"{item['name']}={item['actual'] or 'missing'}"
                f" (lock={item['expected']})"
                for item in blocking_drift[:8]
            ),
            details=blocking_drift,
        )
    manifest = load_manifest()
    active_requirements = [
        f"{item['project_link']}=={item['version']}"
        for item in manifest.get("plugins", {}).values()
        if isinstance(item, dict)
        and item.get("state") == "managed"
        and item.get("project_link") != plugin["project_link"]
    ]
    candidate = f"{plugin['project_link']}=={plugin['version']}"
    current = installed_inventory()
    base = base_installed_inventory()
    immutable = protected_core()
    shared = shared_dependencies()
    overrides, candidate_dependencies = _compatibility_overrides(
        metadata,
        immutable=immutable,
        shared=shared,
        current=current,
    )
    candidate_inputs = candidate_dependencies if overrides else [candidate]
    requirements = [*project_requirements(), *active_requirements, *candidate_inputs]

    project_locked = project_closure()
    resolved, strict_error = await _compile(
        requirements, project_locked, wheels_only=False
    )
    relaxed = False
    if resolved is None:
        relaxed = True
        resolved, relaxed_error = await _compile(
            requirements, immutable, wheels_only=False
        )
        if resolved is None:
            candidate_requirements = [*project_requirements(), *candidate_inputs]
            candidate_core, candidate_core_error = await _compile(
                candidate_requirements, immutable, wheels_only=False
            )
            if candidate_core is not None:
                raise DependencyAnalysisError(
                    "third_party_dependency_conflict",
                    relaxed_error or strict_error,
                )
            candidate_free, candidate_free_error = await _compile(
                candidate_inputs, {}, wheels_only=False
            )
            if candidate_free is not None:
                raise DependencyAnalysisError(
                    "project_dependency_conflict",
                    candidate_core_error or relaxed_error or strict_error,
                )
            raise DependencyAnalysisError(
                "plugin_dependency_invalid",
                candidate_free_error
                or candidate_core_error
                or relaxed_error
                or strict_error,
            )

    core_changes = [
        {"name": name, "from": immutable[name], "to": version}
        for name, version in resolved.items()
        if name in immutable and immutable[name] != version
    ]
    if core_changes:
        raise DependencyAnalysisError("core_dependency_conflict", details=core_changes)

    plugin_inputs = [*active_requirements, *candidate_inputs]
    plugin_resolved, plugin_error = await _compile(
        plugin_inputs, resolved, wheels_only=True
    )
    if plugin_resolved is None:
        plugin_resolved, source_error = await _compile(
            plugin_inputs, resolved, wheels_only=False
        )
        if plugin_resolved is None:
            raise DependencyAnalysisError(
                "dependency_resolution_failed", source_error or plugin_error
            )
        source_required = True
    else:
        source_required = False
    if overrides:
        plugin_resolved[canonicalize_name(str(plugin["project_link"]))] = str(
            plugin["version"]
        )
    shared_changes = [
        {"name": name, "from": base.get(name), "to": version}
        for name, version in sorted(resolved.items())
        if name in shared and base.get(name) != version
    ]
    layer_packages = {
        name: version
        for name, version in plugin_resolved.items()
        if name not in immutable
        and name not in FORBIDDEN_LAYER_PACKAGES
        and (name not in shared or base.get(name) != version)
    }
    private_packages = {
        name: version for name, version in layer_packages.items() if name not in shared
    }
    changes = _package_changes(layer_packages, current)
    private_changes = [
        item
        for item in [*changes["added"], *changes["changed"]]
        if item["name"] in private_packages
    ]
    root_files = metadata.get("urls") or []
    root_wheel_available = any(
        str(item.get("filename", "")).endswith(".whl")
        for item in root_files
        if isinstance(item, dict)
    )
    pure_root_wheel = any(
        str(item.get("filename", "")).endswith("-py3-none-any.whl")
        or str(item.get("filename", "")).endswith("-py2.py3-none-any.whl")
        for item in root_files
        if isinstance(item, dict)
    )
    source_required = source_required or not root_wheel_available
    return {
        "requirements": requirements,
        "resolved_packages": layer_packages,
        "package_changes": changes,
        "core_changes": core_changes,
        "immutable_conflicts": core_changes,
        "shared_changes": shared_changes,
        "private_changes": private_changes,
        "compatibility_overrides": overrides,
        "native_changes": [],
        "dependency_tiers": {
            "immutable_core": sorted(immutable),
            "shared_compatible": sorted(shared),
            "plugin_private": sorted(private_packages),
        },
        "environment_warnings": [
            *report["compatible_shared_drift"],
            *report["extra_packages"],
        ],
        "non_core_changes": [*shared_changes, *changes["changed"]],
        "source_build_required": source_required,
        "pure_python_candidate": (
            pure_root_wheel and not source_required and not overrides
        ),
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
        "immutable_conflicts": [],
        "shared_changes": [],
        "private_changes": [],
        "compatibility_overrides": [],
        "native_changes": [],
        "dependency_tiers": {
            "immutable_core": sorted(protected_core()),
            "shared_compatible": sorted(shared_dependencies()),
            "plugin_private": sorted(packages),
        },
        "environment_warnings": [],
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
