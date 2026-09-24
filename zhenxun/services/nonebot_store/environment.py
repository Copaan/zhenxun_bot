"""Distribution views shared by dependency analysis and generation verification."""

from __future__ import annotations

from dataclasses import dataclass
from hashlib import sha256
import importlib.metadata as metadata
import json
from pathlib import Path
import sys
from typing import Any
from urllib.parse import urldefrag

from packaging.markers import default_environment
from packaging.requirements import InvalidRequirement, Requirement
from packaging.specifiers import InvalidSpecifier, SpecifierSet
from packaging.utils import canonicalize_name
from packaging.version import InvalidVersion, Version

from .storage import LAYER_ROOT, generation_path, load_manifest


def _invalid(owner: str, value: str):
    from .dependencies import DependencyAnalysisError

    raise DependencyAnalysisError(
        "dependency_metadata_invalid",
        details={"owner": owner, "declaration": value[:200]},
    )


def distributions(paths=None) -> tuple[dict, dict]:
    """Select the first distribution on each search path, retaining duplicates."""
    selected, duplicates = {}, {}
    for distribution in metadata.distributions(
        path=paths if paths is not None else sys.path
    ):
        raw_name = distribution.metadata.get("Name")
        if not raw_name:
            continue
        try:
            name = canonicalize_name(raw_name, validate=True)
            Version(distribution.version)
        except (ValueError, TypeError):
            _invalid(str(raw_name), str(distribution.version))
        if name in selected:
            if distribution.version == selected[name].version and str(
                getattr(distribution, "_path", distribution.locate_file(""))
            ) == str(getattr(selected[name], "_path", selected[name].locate_file(""))):
                continue
            duplicates.setdefault(name, []).append(str(distribution.locate_file("")))
        else:
            selected[name] = distribution
    return selected, duplicates


@dataclass
class EnvironmentSnapshot:
    manifest: dict
    base: dict
    layer: dict
    selected: dict
    declarations: list[dict]
    conflicts: list[dict]
    sources: dict
    duplicates: dict
    loaded_mismatches: list[dict]
    digest: str

    @property
    def versions(self) -> dict[str, str]:
        return {
            name: distribution.version for name, distribution in self.selected.items()
        }


def capture_environment(
    *, manifest=None, candidate_path: Path | None = None
) -> EnvironmentSnapshot:
    """Read base and selected generation metadata without importing plugins."""
    from .dependencies import project_requirements

    manifest = load_manifest() if manifest is None else manifest
    root = LAYER_ROOT.resolve()
    paths = []
    for value in sys.path:
        if not Path(value).resolve().is_relative_to(root):
            paths.append(value)
    base, duplicates = distributions(paths)
    generation = manifest.get("active_generation")
    layer_path = candidate_path
    if layer_path is None and isinstance(generation, int):
        layer_path = generation_path(generation)
    layer, layer_duplicates = (
        distributions([str(layer_path)]) if layer_path is not None else ({}, {})
    )
    selected = {**base, **layer}
    roots = [("pyproject.toml", raw) for raw in project_requirements()]
    for plugin in manifest.get("plugins", {}).values():
        if plugin.get("state") == "managed":
            roots.extend(
                (canonicalize_name(plugin["project_link"]), raw)
                for raw in (
                    plugin.get("resolution_inputs")
                    or [f"{plugin['project_link']}=={plugin['version']}"]
                )
            )
    requirements = {}
    for name, distribution in selected.items():
        requirements[name] = list(distribution.requires or [])
    extras: dict[str, set[str]] = {}
    declarations: dict[tuple[str, str], dict] = {}
    pending = roots + [
        (owner, raw) for owner, values in requirements.items() for raw in values
    ]
    while True:
        expanded = False
        for owner, raw in pending:
            try:
                requirement = Requirement(raw)
                contexts = {"", *extras.get(owner, set())}
                if requirement.marker and not any(
                    requirement.marker.evaluate(
                        {**default_environment(), "extra": extra}
                    )
                    for extra in contexts
                ):
                    continue
            except (InvalidRequirement, ValueError):
                _invalid(owner, raw)
            name = canonicalize_name(requirement.name)
            new_extras = {
                canonicalize_name(extra) for extra in requirement.extras
            } - extras.setdefault(name, set())
            if new_extras:
                extras[name].update(new_extras)
                expanded = True
            declarations[owner, str(requirement)] = {
                "owner": owner,
                "name": name,
                "requirement": str(requirement),
                "owner_version": selected[owner].version if owner in selected else None,
            }
        if not expanded:
            break
    sources = {
        name: {
            "version": dist.version,
            "layer": "active_generation" if name in layer else "base",
            "path": str(dist.locate_file("")),
        }
        for name, dist in selected.items()
    }
    conflicts = []
    for declaration in declarations.values():
        name = declaration["name"]
        requirement = Requirement(declaration["requirement"])
        actual = selected[name].version if name in selected else None
        try:
            compatible = actual is not None and requirement.specifier.contains(
                actual, prereleases=True
            )
        except InvalidVersion:
            _invalid(name, str(actual))
        if requirement.url and name in selected:
            try:
                direct = json.loads(selected[name].read_text("direct_url.json") or "{}")
            except (ValueError, TypeError):
                _invalid(name, "direct_url.json")
            if urldefrag(requirement.url).url != urldefrag(direct.get("url", "")).url:
                conflicts.append(
                    {
                        **declaration,
                        "actual": actual,
                        "kind": "source_unverified",
                        "layer": sources[name]["layer"],
                    }
                )
        if not compatible:
            conflicts.append(
                {
                    **declaration,
                    "actual": actual,
                    "layer": "active_generation" if name in layer else "base",
                }
            )
    loaded = []
    for name, dist in selected.items():
        python = dist.metadata.get("Requires-Python")
        try:
            if python and not SpecifierSet(python).contains(
                default_environment()["python_full_version"], prereleases=True
            ):
                conflicts.append(
                    {
                        "owner": name,
                        "name": name,
                        "actual": dist.version,
                        "requirement": f"Python{python}",
                        "kind": "requires_python",
                        "layer": sources[name]["layer"],
                    }
                )
        except InvalidSpecifier:
            _invalid(name, python)
        for file in dist.files or ():
            parts = file.parts
            if not (
                (len(parts) == 2 and parts[1] == "__init__.py")
                or (len(parts) == 1 and parts[0].endswith(".py"))
            ):
                continue
            module_name = parts[0].removesuffix(".py")
            module = sys.modules.get(module_name)
            actual_path = getattr(module, "__file__", None)
            expected_path = Path(dist.locate_file(file)).resolve()
            if actual_path and Path(actual_path).resolve() != expected_path:
                loaded.append(
                    {
                        "name": name,
                        "module": module_name,
                        "loaded_path": str(actual_path),
                        "expected_path": str(expected_path),
                    }
                )
    evidence = {
        "sources": sources,
        "declarations": [declarations[key] for key in sorted(declarations)],
        "requires_python": {
            name: dist.metadata.get("Requires-Python")
            for name, dist in selected.items()
        },
        "generation": manifest.get("active_generation"),
        "duplicates": {**duplicates, **layer_duplicates},
        "environment": default_environment(),
        "executable": sys.executable,
    }
    return EnvironmentSnapshot(
        manifest,
        base,
        layer,
        selected,
        list(declarations.values()),
        conflicts,
        sources,
        {**duplicates, **layer_duplicates},
        loaded,
        sha256(json.dumps(evidence, sort_keys=True).encode()).hexdigest(),
    )


def conflict_key(issue: dict) -> tuple:
    return tuple(
        issue.get(key)
        for key in (
            "owner",
            "owner_version",
            "name",
            "requirement",
            "actual",
            "kind",
            "layer",
        )
    )


def verify_effective_environment() -> dict:
    """Verify the base and active layer after a dependency synchronization."""
    from .dependencies import DependencyAnalysisError, environment_report

    report = environment_report()
    failures = [
        *report["layer_mismatch"],
        *report["immutable_drift"],
        *report["incompatible_shared_drift"],
        *report["requirement_conflicts"],
    ]
    if failures:
        raise DependencyAnalysisError(
            "current_environment_dependency_conflict", details=failures
        )
    return report


def validate_candidate(path: Path, transaction: dict[str, Any]) -> dict:
    """Reject new conflicts and verify every planned version before publication."""
    from .dependencies import DependencyAnalysisError, protected_core

    snapshot = capture_environment(
        manifest=transaction["target_manifest"], candidate_path=path
    )
    expected = {
        canonicalize_name(name): item["version"]
        for name, item in transaction["target_manifest"].get("packages", {}).items()
    }
    if {name: dist.version for name, dist in snapshot.layer.items()} != expected:
        raise DependencyAnalysisError("dependency_layer_state_mismatch")
    _, duplicate_layer = distributions([str(path)])
    if duplicate_layer:
        raise DependencyAnalysisError(
            "dependency_layer_state_mismatch", details=duplicate_layer
        )
    permitted = (
        transaction.get("remaining_conflicts", [])
        if transaction.get("action") == "environment_repair"
        else []
    )
    remaining_core = []
    for name, version in protected_core().items():
        if snapshot.versions.get(name) != version:
            unchanged = next(
                (
                    item
                    for item in permitted
                    if item.get("tier") == "immutable_core"
                    and item["name"] == name
                    and item.get("expected") == version
                    and item.get("actual") == snapshot.versions.get(name)
                    and name not in snapshot.layer
                ),
                None,
            )
            if unchanged:
                remaining_core.append(unchanged)
                continue
            raise DependencyAnalysisError(
                "core_dependency_conflict",
                details=[
                    {
                        "name": name,
                        "actual": snapshot.versions.get(name),
                        "expected": version,
                    }
                ],
            )
    allowed = {conflict_key(item) for item in permitted}
    errors = [item for item in snapshot.conflicts if conflict_key(item) not in allowed]
    if errors:
        raise DependencyAnalysisError(
            "current_environment_dependency_conflict", details=errors
        )
    return {
        "state": "partial" if snapshot.conflicts or remaining_core else "passed",
        "remaining_conflicts": [*snapshot.conflicts, *remaining_core],
        "snapshot_digest": snapshot.digest,
    }
