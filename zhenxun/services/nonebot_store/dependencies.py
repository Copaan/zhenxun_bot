from __future__ import annotations

import asyncio
from collections import deque
import contextlib
from hashlib import sha256
import importlib.metadata
import json
import os
from pathlib import Path
import platform
import re
import subprocess
import sys
import tempfile
from typing import Any, Literal
from urllib.parse import quote

from dotenv import dotenv_values
import httpx
from packaging.markers import InvalidMarker, Marker, default_environment
from packaging.requirements import InvalidRequirement, Requirement
from packaging.specifiers import SpecifierSet
from packaging.tags import sys_tags
from packaging.utils import (
    InvalidWheelFilename,
    canonicalize_name,
    parse_wheel_filename,
)
from packaging.version import InvalidVersion, Version

try:
    import tomllib
except ModuleNotFoundError:  # pragma: no cover - Python 3.10
    import tomli as tomllib

from .storage import LAYER_ROOT, load_manifest

SOLVER_POLICY_VERSION = 11
COORDINATED_TRANSITIVE_PACKAGES = frozenset({"aiofiles", "idna"})
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
    value = re.sub(r"https?://[^\s<>]+", "<package-source>", value)
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
        result.setdefault(canonicalize_name(name), distribution.version)
    return result


def base_installed_inventory() -> dict[str, str]:
    return _distribution_inventory(include_layer=False)


def installed_inventory() -> dict[str, str]:
    from .environment import capture_environment

    return capture_environment().versions


def _marker_enabled(marker: Any) -> bool:
    text = str(marker or "").strip()
    if not text:
        return True
    try:
        return Marker(text).evaluate(default_environment())
    except InvalidMarker as error:
        raise DependencyAnalysisError(
            "dependency_marker_invalid", text[:200]
        ) from error


def _lock_package_enabled(package: dict[str, Any]) -> bool:
    markers = package.get("resolution-markers") or []
    if not markers:
        return True
    return any(_marker_enabled(marker) for marker in markers)


_lock_cache: tuple[tuple[str, int, int], Any] | None = None


def _lock_packages() -> tuple[dict[str, set[str]], dict[str, str], set[str]]:
    """解析 uv.lock 中项目可达的依赖图，按文件修改时间和大小缓存

    返回:
        tuple: 依赖图、锁定版本、项目直接依赖
    """
    global _lock_cache
    if not LOCK_FILE.is_file():
        raise DependencyAnalysisError("project_lock_missing")
    stat = LOCK_FILE.stat()
    key = (str(LOCK_FILE.resolve()), stat.st_mtime_ns, stat.st_size)
    if _lock_cache is None or _lock_cache[0] != key:
        _lock_cache = (key, _parse_lock_packages())
    graph, versions, roots = _lock_cache[1]
    return (
        {name: set(items) for name, items in graph.items()},
        dict(versions),
        set(roots),
    )


def _parse_lock_packages() -> tuple[dict[str, set[str]], dict[str, str], set[str]]:
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
        if name in packages and (
            packages[name].get("version") != package.get("version")
            or packages[name].get("source") != package.get("source")
        ):
            raise DependencyAnalysisError("project_lock_branch_ambiguous", name)
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
            selected = packages.get(dependency)
            if (
                selected is None
                or (item.get("version") and item["version"] != selected.get("version"))
                or (item.get("source") and item["source"] != selected.get("source"))
            ):
                raise DependencyAnalysisError(
                    "project_lock_branch_ambiguous", dependency
                )
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
    return {
        name: lock_versions[name]
        for name in protected
        if lock_versions.get(name) and name not in COORDINATED_TRANSITIVE_PACKAGES
    }


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
    expected = os.environ.get("UV_PROJECT_ENVIRONMENT") or os.environ.get("VIRTUAL_ENV")
    try:
        actual = Path(sys.prefix).resolve()
        requested = (
            Path(expected).resolve() if expected else (Path.cwd() / ".venv").resolve()
        )
        return actual == requested and actual != Path(sys.base_prefix).resolve()
    except OSError:
        return False


def environment_report(*, check_lock: bool = False, snapshot=None) -> dict[str, Any]:
    immutable = protected_core()
    shared = shared_dependencies()
    from .environment import capture_environment, core_environment_summary

    snapshot = snapshot or capture_environment()
    base = {name: dist.version for name, dist in snapshot.base.items()}
    layer = {name: dist.version for name, dist in snapshot.layer.items()}
    effective = snapshot.versions
    manifest = snapshot.manifest
    recorded = {
        canonicalize_name(name): str(info.get("version", ""))
        for name, info in manifest.get("packages", {}).items()
        if isinstance(info, dict)
    }
    layer_mismatch = [
        {
            "name": name,
            "recorded": recorded.get(name),
            "actual": layer.get(name),
            "layer": "active_generation",
            "generation": manifest.get("active_generation"),
        }
        for name in sorted(set(recorded) | set(layer))
        if recorded.get(name) != layer.get(name)
    ]
    requirements = _project_requirement_map()
    requirement_conflicts = snapshot.conflicts
    immutable_drift: list[dict[str, Any]] = []
    incompatible_shared_drift: list[dict[str, Any]] = []

    for name, expected in sorted(immutable.items()):
        actual = effective.get(name)
        if actual == expected:
            continue
        immutable_drift.append(
            {
                "name": name,
                "tier": "immutable_core",
                "expected": expected,
                "actual": actual,
                "layer": "active_generation" if name in layer else "base",
                "requirement": f"{name}=={expected}",
                "kind": _version_relation(actual, expected),
            }
        )

    for name, expected in sorted(shared.items()):
        actual = effective.get(name)
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
        if compatible:
            continue
        incompatible_shared_drift.append(
            {
                "name": name,
                "tier": "shared_compatible",
                "expected": expected,
                "actual": actual,
                "kind": _version_relation(actual, expected),
                "requirement": str(requirement) if requirement else None,
                "layer": "active_generation" if name in layer else "base",
            }
        )

    interpreter_mismatch = not _project_venv_active()
    lock_stale = False
    if check_lock:
        from zhenxun.services.installer_network import (
            project_environment,
            uv_executable,
        )

        uv = uv_executable()
        if uv is None:
            lock_stale = True
        else:
            try:
                checked = subprocess.run(
                    [uv, "lock", "--check"],
                    cwd=Path.cwd(),
                    capture_output=True,
                    timeout=30,
                    check=False,
                    env=project_environment(),
                )
            except (OSError, subprocess.TimeoutExpired):
                lock_stale = True
            else:
                lock_stale = checked.returncode != 0
    lock_digest = (
        sha256(LOCK_FILE.read_bytes()).hexdigest() if LOCK_FILE.exists() else "missing"
    )
    payload = {
        "immutable_drift": immutable_drift,
        "layer_mismatch": layer_mismatch,
        "requirement_conflicts": requirement_conflicts,
        "incompatible_shared_drift": incompatible_shared_drift,
        "interpreter_mismatch": interpreter_mismatch,
        "project_lock_stale": lock_stale,
        "repairable": bool(
            not interpreter_mismatch
            and not lock_stale
            and not layer_mismatch
            and (immutable_drift or incompatible_shared_drift or requirement_conflicts)
            and LOCK_FILE.is_file()
        ),
        "repair_mode": "base" if immutable_drift else "layer",
    }
    payload["fingerprint"] = sha256(
        json.dumps(
            {
                "lock": lock_digest,
                "project": sha256(PROJECT_FILE.read_bytes()).hexdigest(),
                "versions": sorted(snapshot.versions.items()),
                "policy": SOLVER_POLICY_VERSION,
                "base": sorted(base.items()),
                "layer": sorted(layer.items()),
                "manifest": {
                    key: manifest.get(key)
                    for key in (
                        "active_generation",
                        "packages",
                        "plugins",
                        "manual_requirements",
                        "generation_digest",
                        "pending_verification",
                    )
                },
                "python": platform.python_version(),
                "platform": sys.platform,
                "lock_stale": lock_stale,
            },
            sort_keys=True,
        ).encode()
    ).hexdigest()
    payload["core_summary"] = core_environment_summary(snapshot, immutable, payload)
    return payload


async def environment_report_async(
    *, check_lock: bool = False, snapshot=None
) -> dict[str, Any]:
    """在线程中生成依赖环境报告，避免扫描元数据和运行 uv 时阻塞事件循环

    参数:
        check_lock: 是否检查 uv.lock 是否过期
        snapshot: 已采集的环境快照

    返回:
        dict[str, Any]: 依赖环境报告
    """
    return await asyncio.to_thread(
        environment_report, check_lock=check_lock, snapshot=snapshot
    )


async def preflight_environment_repair() -> tuple[bool, str]:
    guard_orm_sync()
    from zhenxun.services.installer_network import project_environment, uv_command

    process = await asyncio.create_subprocess_exec(
        *uv_command("sync", "--locked", "--inexact", "--dry-run"),
        cwd=str(Path.cwd()),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        env=project_environment(),
    )
    stdout, stderr = await process.communicate()
    detail = safe_process_error((stderr or stdout).decode(errors="replace"))
    return process.returncode == 0, detail


def guard_orm_sync() -> None:
    """Reject lock synchronization that would change any ORM distribution."""
    from .policy import guard_orm_changes

    current = base_installed_inventory()
    guard_orm_changes(current, {**current, **project_closure()})


def environment_fingerprint(
    registry_plugin: dict[str, Any], *, pending_revision: str = ""
) -> str:
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
        "solver_policy": SOLVER_POLICY_VERSION,
        "pending_revision": pending_revision,
    }
    return sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()


async def fetch_pypi_metadata(project: str, version: str) -> dict[str, Any]:
    # 延迟导入：nonebot_store 要保证"被 import 时不拉起服务层"。
    # 模块级 import 会经 runtime.py 把 zhenxun.services 带进 sys.modules，
    # 破坏 launcher 的导入隔离（见 tests/test_launcher_import_safety.py）。
    from zhenxun.services.network_proxy import ManagedAsyncClient

    safe_project = quote(project, safe="-._")
    safe_version = quote(version, safe="-._+")
    url = f"https://pypi.org/pypi/{safe_project}/{safe_version}/json"
    async with ManagedAsyncClient(timeout=15, follow_redirects=True) as client:
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
    requirements: list[str],
    constraints: dict[str, str],
    *,
    wheels_only: bool,
    restricted_packages: tuple[str, ...] = (),
    preferences: dict[str, str] | None = None,
    no_dependencies: bool = False,
    _metadata_links: tuple[Path, ...] = (),
) -> tuple[dict[str, str] | None, str]:
    from zhenxun.services.installer_network import (
        UV_BINARY_POLICY_VERSION,
        installer_environment,
        resolver_configuration_error,
        uv_binary_options,
        uv_compile_command,
        uv_version,
    )

    with tempfile.TemporaryDirectory(prefix="zhenxun_nb_analyze_") as root:
        directory = Path(root)
        source = directory / "requirements.in"
        constraint_file = directory / "constraints.txt"
        output = directory / "requirements.txt"
        source.write_text("\n".join(requirements) + "\n", encoding="utf-8")
        _write_constraints(constraint_file, constraints)
        if preferences:
            # uv reuses existing output pins as preferences, not hard constraints.
            _write_constraints(output, preferences)
        command = uv_compile_command(source, output, constraint_file)
        command.extend(
            uv_binary_options(wheels_only=True) if wheels_only else ["--no-build"]
        )
        if no_dependencies:
            command.append("--no-deps")
        environment = installer_environment()
        if _metadata_links:
            # Command-line --find-links replaces UV_FIND_LINKS, so carry the
            # configured local sources along with the projected metadata.
            configured = re.split(r"[,\s]+", environment.get("UV_FIND_LINKS", ""))
            for link in [*filter(None, configured), *map(str, _metadata_links)]:
                command.extend(["--find-links", link])
            command.append("--no-cache")
        process = await asyncio.create_subprocess_exec(
            *command,
            cwd=str(Path.cwd()),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env=environment,
        )
        import psutil

        from zhenxun.utils.process_tree import kill_tree, verified_descendants

        try:
            owned = psutil.Process(process.pid)
        except psutil.NoSuchProcess:
            owned = None
        try:
            stdout, stderr = await asyncio.wait_for(process.communicate(), 180)
        except (asyncio.CancelledError, asyncio.TimeoutError):
            tree = [owned] if owned is not None else []
            if owned is not None:
                with contextlib.suppress(psutil.NoSuchProcess):
                    tree += verified_descendants(owned)
            alive = await asyncio.to_thread(kill_tree, tree)
            if process.returncode is None:
                with contextlib.suppress(ProcessLookupError):
                    process.kill()
            await asyncio.wait_for(process.wait(), 10)
            if alive:
                raise DependencyAnalysisError("dependency_tool_cleanup_unconfirmed")
            raise
        details = safe_process_error((stderr or stdout).decode(errors="replace"))
        if resolver_configuration_error(details):
            raise DependencyAnalysisError(
                "dependency_resolver_configuration",
                details,
                details={
                    "uv_version": uv_version(),
                    "binary_policy_version": UV_BINARY_POLICY_VERSION,
                    "wheels_only": wheels_only,
                    "diagnostic": details,
                },
            )
        if process.returncode != 0 or not output.exists():
            _check_resolution_failure(details)
            missing = re.search(r"Wheels are required for `([A-Za-z0-9_.-]+)`", details)
            if not wheels_only and missing and len(_metadata_links) < 32:
                name = canonicalize_name(missing.group(1))
                projected = any(
                    list(link.glob(name.replace("-", "_") + "-*.whl"))
                    for link in _metadata_links
                )
                if name not in restricted_packages and not projected:
                    from .source_metadata import project_static_sources

                    projection = directory / "source-metadata"
                    specifiers = [
                        str(requirement.specifier)
                        for raw in requirements
                        if canonicalize_name((requirement := Requirement(raw)).name)
                        == name
                    ]
                    if name in constraints:
                        specifiers.append("==" + constraints[name])
                    if await project_static_sources(
                        name, projection, installer_environment(), specifiers
                    ):
                        return await _compile(
                            requirements,
                            constraints,
                            wheels_only=wheels_only,
                            restricted_packages=restricted_packages,
                            preferences=preferences,
                            no_dependencies=no_dependencies,
                            _metadata_links=(*_metadata_links, projection),
                        )
            return None, details
        return _parse_compiled(output), details


def _check_resolution_failure(details: str) -> None:
    from zhenxun.services.installer_network import resolver_failure

    failure = resolver_failure(details)
    if failure["code"] not in {
        "dependency_conflict",
        "dependency_source_required",
    }:
        raise DependencyAnalysisError(failure["code"], details)


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


def _wheel_availability(metadata: dict[str, Any]) -> dict[str, Any]:
    supported_tags = set(sys_tags())
    wheels: list[str] = []
    compatible: list[str] = []
    for item in metadata.get("urls") or []:
        if not isinstance(item, dict):
            continue
        filename = str(item.get("filename") or "")
        if not filename.endswith(".whl"):
            continue
        wheels.append(filename)
        try:
            _, _, _, tags = parse_wheel_filename(filename)
        except InvalidWheelFilename:
            continue
        if supported_tags.intersection(tags):
            compatible.append(filename)
    return {
        "compatible": bool(compatible),
        "compatible_wheel_files": compatible,
    }


def _compatibility_overrides(
    metadata: dict[str, Any],
    *,
    immutable: dict[str, str],
    shared: dict[str, str],
    current: dict[str, str],
) -> tuple[list[dict[str, str]], list[str]]:
    from .policy import ORM_PACKAGES, guard_orm_request

    overrides, inputs = [], []
    for requirement in _active_metadata_requirements(metadata):
        name = canonicalize_name(requirement.name)
        version = current.get(name)
        if (
            not version
            or requirement.url
            or requirement.specifier.contains(version, prereleases=True)
        ):
            inputs.append(str(requirement))
            continue
        if name in ORM_PACKAGES:
            guard_orm_request({name})
        if name in immutable:
            raise DependencyAnalysisError(
                "core_dependency_conflict",
                details=[
                    {
                        "name": name,
                        "tier": "immutable_core",
                        "kind": "version_mismatch",
                        "declared_requirement": str(requirement),
                        "expected": immutable[name],
                        "actual": version,
                    }
                ],
            )
        overrides.append(
            {
                "name": name,
                "declared_requirement": str(requirement),
                "effective_version": version,
                "tier": "shared_compatible" if name in shared else "plugin_private",
                "risk": "unverified_runtime_compatibility",
            }
        )
        extras = (
            "[" + ",".join(sorted(requirement.extras)) + "]"
            if requirement.extras
            else ""
        )
        inputs.append(f"{name}{extras}=={version}")
    return overrides, inputs


async def solve_install(
    plugin: dict[str, Any],
    metadata: dict[str, Any],
    *,
    manifest: dict[str, Any] | None = None,
    prefer_existing: bool = True,
) -> dict[str, Any]:
    from .environment import capture_environment

    snapshot = capture_environment()
    report = environment_report(snapshot=snapshot)
    if report.get("layer_mismatch"):
        raise DependencyAnalysisError(
            "dependency_layer_state_mismatch", details=report["layer_mismatch"]
        )
    blocking_drift = [
        *(
            {
                **item,
                "tier": "immutable_core",
                "kind": _version_relation(item.get("actual"), item["expected"]),
            }
            for item in report["immutable_drift"]
        ),
        *report["incompatible_shared_drift"],
    ]
    if report["interpreter_mismatch"]:
        raise DependencyAnalysisError(
            "interpreter_mismatch", details={"command": "uv run zx"}
        )
    if blocking_drift:
        raise DependencyAnalysisError(
            "current_environment_dependency_conflict",
            ", ".join(
                f"{item['name']}={item['actual'] or 'missing'}"
                f" (lock={item['expected']})"
                for item in blocking_drift[:8]
            ),
            details=blocking_drift,
        )
    manifest = manifest or load_manifest()
    from .environment import dependency_roots

    excluded_owners = {"pyproject.toml", canonicalize_name(plugin["project_link"])}
    managed_requirements = [
        raw for owner, raw in dependency_roots(manifest) if owner not in excluded_owners
    ]
    candidate = f"{plugin['project_link']}=={plugin['version']}"
    current = snapshot.versions
    base = {name: dist.version for name, dist in snapshot.base.items()}
    immutable = protected_core()
    shared = shared_dependencies()
    from .policy import (
        ORM_PACKAGES,
        external_requirements,
        guard_orm_changes,
        guard_orm_request,
        retained_conflicts,
        scoped_roots,
    )

    guard_orm_request({canonicalize_name(str(plugin["project_link"]))})
    frozen = {
        **immutable,
        **{name: current[name] for name in ORM_PACKAGES if name in current},
    }
    for requirement in _active_metadata_requirements(metadata):
        name = canonicalize_name(requirement.name)
        if name in ORM_PACKAGES and name not in current:
            guard_orm_request({name})
    overrides, candidate_dependencies = _compatibility_overrides(
        metadata,
        immutable=immutable,
        shared=shared,
        current=current,
    )
    if not prefer_existing:
        overrides = []
    candidate_inputs = candidate_dependencies if overrides else [candidate]
    candidate_name = canonicalize_name(str(plugin["project_link"]))
    baseline = {
        name: version for name, version in current.items() if name != candidate_name
    }
    # Pending transactions are not yet active; only their explicit plan may
    # replace the installed view. Historical active metadata is checked above.
    if manifest.get("packages") != load_manifest().get("packages"):
        baseline.update(
            {
                name: str(info["version"])
                for name, info in manifest.get("packages", {}).items()
                if name != candidate_name
            }
        )
    baseline.update(frozen)

    # Discover only the requested plugin's closure. Other installed plugins
    # participate in compatibility checks, never in the scope allowed to move.
    candidate_probe, candidate_error = await _compile(
        candidate_inputs, baseline, wheels_only=False, preferences=baseline
    )
    if candidate_probe is None:
        candidate_probe, candidate_error = await _compile(
            candidate_inputs, frozen, wheels_only=False, preferences=baseline
        )
    if candidate_probe is None:
        raise DependencyAnalysisError("plugin_dependency_invalid", candidate_error)
    candidate_scope = set(candidate_probe) | {candidate_name}
    # Keep requirements imposed by external owners on the candidate closure.
    active_requirements = [
        *scoped_roots(managed_requirements, candidate_scope),
        *external_requirements(snapshot, candidate_scope),
    ]
    requirements = [
        *scoped_roots(project_requirements(), candidate_scope),
        *active_requirements,
        *candidate_inputs,
    ]
    strict_constraints = {**baseline, **frozen}
    scoped_constraints = {
        **{
            name: version
            for name, version in baseline.items()
            if name not in candidate_scope
        },
        **frozen,
    }
    for name, version in current.items():
        if name.startswith("nonebot-plugin-") and name != candidate_name:
            scoped_constraints[name] = version

    resolved, strict_error = await _compile(
        requirements, strict_constraints, wheels_only=False, preferences=baseline
    )
    if resolved is None:
        resolved, relaxed_error = await _compile(
            requirements, scoped_constraints, wheels_only=False, preferences=baseline
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
        {
            "name": name,
            "tier": "immutable_core",
            "kind": "version_mismatch",
            "expected": immutable[name],
            "actual": version,
            "from": immutable[name],
            "to": version,
        }
        for name, version in resolved.items()
        if name in immutable and immutable[name] != version
    ]
    if core_changes:
        raise DependencyAnalysisError("core_dependency_conflict", details=core_changes)
    unrelated_changes = [
        {"name": name, "from": baseline[name], "to": version}
        for name, version in resolved.items()
        if name in baseline
        and name not in candidate_scope
        and version != baseline[name]
    ]
    if unrelated_changes:
        raise DependencyAnalysisError(
            "third_party_dependency_conflict", details=unrelated_changes
        )

    final_candidate, final_error = await _compile(
        candidate_inputs, resolved, wheels_only=False
    )
    if final_candidate is None:
        raise DependencyAnalysisError("dependency_resolution_failed", final_error)
    candidate_scope = set(final_candidate) | {candidate_name}

    plugin_inputs = [*active_requirements, *candidate_inputs]
    plugin_resolved, plugin_error = await _compile(
        plugin_inputs,
        resolved,
        wheels_only=False,
        restricted_packages=tuple(sorted(resolved)),
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
    guard_orm_changes(current, {**current, **resolved, **plugin_resolved})
    for managed_plugin in manifest.get("plugins", {}).values():
        if (
            isinstance(managed_plugin, dict)
            and managed_plugin.get("state") == "managed"
            and managed_plugin.get("resolution_inputs")
            and canonicalize_name(str(managed_plugin["project_link"])) != candidate_name
        ):
            plugin_resolved[canonicalize_name(str(managed_plugin["project_link"]))] = (
                str(managed_plugin["version"])
            )
    shared_changes = [
        {"name": name, "from": current.get(name), "to": version}
        for name, version in sorted(resolved.items())
        if name in shared and current.get(name) != version
    ]
    layer_packages = {
        name: version
        for name, version in plugin_resolved.items()
        if name not in immutable
        and name not in FORBIDDEN_LAYER_PACKAGES
        and base.get(name) != version
    }
    # A new install must not silently remove packages retained in the active layer.
    for name, info in manifest.get("packages", {}).items():
        if (
            name not in layer_packages
            and name not in immutable
            and name not in FORBIDDEN_LAYER_PACKAGES
        ):
            layer_packages[name] = resolved.get(name, str(info["version"]))
    changes = _package_changes(layer_packages, current)
    affected = {
        candidate_name,
        *(item["name"] for item in [*changes["added"], *changes["changed"]]),
    }
    change_sources = {
        item["name"]: {
            "kind": (
                "requested_plugin"
                if item["name"] == candidate_name
                else "candidate_dependency"
                if item["name"] in candidate_scope
                else "pending_transaction"
            ),
            "required_by": candidate_name if item["name"] in candidate_scope else None,
        }
        for item in [*changes["added"], *changes["changed"], *shared_changes]
    }
    wheel_status = _wheel_availability(metadata)
    pure_root_wheel = any(
        filename.endswith(("-py3-none-any.whl", "-py2.py3-none-any.whl"))
        for filename in wheel_status["compatible_wheel_files"]
    )
    source_required = source_required or not wheel_status["compatible"]
    candidate_requirement_names = {
        canonicalize_name(requirement.name)
        for requirement in _active_metadata_requirements(metadata)
    }
    database_migration_possible = (
        canonicalize_name(str(plugin["project_link"])) == "nonebot-plugin-orm"
        or "nonebot-plugin-orm" in candidate_requirement_names
    )
    database_type = "none"
    if database_migration_possible:
        env_file = Path(".env.dev") if Path(".env.dev").exists() else Path(".env")
        raw_url = str(dotenv_values(env_file).get("SQLALCHEMY_DATABASE_URL") or "")
        database_type = (
            "sqlite"
            if not raw_url or raw_url.casefold().startswith("sqlite")
            else "external"
        )
    return {
        "requirements": requirements,
        "resolved_packages": layer_packages,
        "package_changes": changes,
        "change_sources": change_sources,
        "candidate_packages": sorted(candidate_scope),
        "shared_changes": shared_changes,
        "compatibility_overrides": overrides,
        "remaining_conflicts": retained_conflicts(snapshot, affected),
        "non_core_changes": [*shared_changes, *changes["changed"]],
        "source_build_required": source_required,
        "pure_python_candidate": (
            pure_root_wheel and not source_required and not overrides
        ),
        "solver_policy_version": SOLVER_POLICY_VERSION,
        "candidate_inputs": candidate_inputs,
        "database_migration_possible": database_migration_possible,
        "database_type": database_type,
    }


def _plugin_released_packages(
    manifest: dict[str, Any], project: str
) -> tuple[list[str], list[dict[str, Any]]]:
    """计算卸载 NoneBot 插件后可释放的依赖，分析失败时不释放

    参数:
        manifest: 依赖层清单
        project: 规范化的插件包名

    返回:
        tuple[list[str], list[dict[str, Any]]]: 可释放的包，因仍被导入而保留的包
    """
    from copy import deepcopy

    from zhenxun.services.plugin_store.plugin_store_receipts import (
        receipt_dependency_inputs,
    )

    from .environment import released_packages

    after = deepcopy(manifest)
    after["plugins"] = {
        key: item
        for key, item in manifest.get("plugins", {}).items()
        if canonicalize_name(str(item.get("project_link") or "")) != project
    }
    try:
        sources = receipt_dependency_inputs()
        released, imported = released_packages(
            manifest, after, sources_before=sources, sources_after=sources
        )
    except (DependencyAnalysisError, OSError, KeyError, TypeError):
        return [], []
    return [name for name in released if name != project], imported


def uninstall_plan(
    plugin: dict[str, Any], *, manifest: dict[str, Any] | None = None
) -> dict[str, Any]:
    manifest = manifest or load_manifest()
    packages = {
        canonicalize_name(name): str(info["version"])
        for name, info in manifest.get("packages", {}).items()
        if isinstance(info, dict) and info.get("version")
    }
    project = canonicalize_name(str(plugin["project_link"]))
    from .environment import capture_environment
    from .policy import guard_orm_request, retained_conflicts

    guard_orm_request({project})
    snapshot = capture_environment()
    owners = sorted(
        {
            item["owner"]
            for item in snapshot.declarations
            if item["name"] == project and item["owner"] != project
        }
    )
    if not owners:
        packages.pop(project, None)
    released, imported = _plugin_released_packages(manifest, project)
    for name in released:
        packages.pop(canonicalize_name(name), None)
    return {
        "requirements": [],
        "resolved_packages": packages,
        "package_changes": {
            "added": [],
            "changed": [],
            "removed": (
                []
                if owners
                else [
                    {
                        "name": project,
                        "version": str(plugin["version"]),
                        "reason": "plugin",
                    }
                ]
            )
            + [
                {
                    "name": name,
                    "version": snapshot.versions.get(name),
                    "reason": "released",
                }
                for name in released
                if name not in packages
            ],
        },
        "shared_changes": [],
        "compatibility_overrides": [],
        "retained": (
            [
                {
                    "name": project,
                    "owners": owners,
                    "version": snapshot.versions.get(project),
                }
            ]
            if owners
            else []
        )
        + imported,
        "remaining_conflicts": retained_conflicts(
            snapshot, set() if owners else {project}
        ),
        "non_core_changes": [],
        "source_build_required": False,
        "pure_python_candidate": True,
        "solver_policy_version": SOLVER_POLICY_VERSION,
        "candidate_inputs": [],
        "database_migration_possible": False,
        "database_type": "none",
    }


def read_requirement_files(files: list[Path]) -> list[str]:
    """读取插件 requirements 文件中的依赖声明

    只接受包依赖声明，安装器参数（如 -r、--index-url）和直链依赖会被拒绝。

    参数:
        files: requirements 文件列表

    返回:
        list[str]: 依赖声明
    """
    requirements: list[str] = []
    for path in files:
        for raw_line in path.read_text(encoding="utf-8-sig").splitlines():
            line = re.sub(r"\s+#.*$", "", raw_line).strip()
            if line and not line.startswith("#"):
                requirements.append(str(parse_requirement(line)))
    return requirements


def parse_requirement(value: str) -> Requirement:
    """解析单条依赖声明，拒绝安装器参数、直链依赖和 NoneBot 1

    参数:
        value: 依赖声明

    返回:
        Requirement: 包名已规范化的依赖

    异常:
        DependencyAnalysisError: 依赖声明无效或不受支持
    """
    line = str(value).strip()
    if line.startswith("-"):
        raise DependencyAnalysisError(
            "dependency_requirement_option_forbidden", line[:160]
        )
    try:
        requirement = Requirement(line)
    except InvalidRequirement as error:
        raise DependencyAnalysisError(
            "dependency_requirement_invalid", line[:160]
        ) from error
    requirement.name = canonicalize_name(requirement.name)
    if requirement.url:
        raise DependencyAnalysisError(
            "dependency_requirement_direct_url", requirement.name
        )
    if requirement.name == "nonebot":
        raise DependencyAnalysisError("nonebot1_plugin")
    return requirement
