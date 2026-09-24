"""Preview independent dependency repairs without changing installed plugins."""

from copy import deepcopy
from hashlib import sha256
from typing import Any

from packaging.requirements import Requirement
from packaging.utils import canonicalize_name

from .dependencies import (
    FORBIDDEN_LAYER_PACKAGES,
    DependencyAnalysisError,
    _compile,
    environment_report,
    protected_core,
)
from .environment import capture_environment


def select_repair_groups(preview: dict, selected: list[str] | None) -> dict:
    """Combine only explicitly selectable components of a bound preview."""
    groups = preview["groups"]
    chosen = (
        set(selected)
        if selected is not None
        else {group["id"] for group in groups if group["status"] == "repairable"}
    )
    if not chosen or chosen - {g["id"] for g in groups if g["status"] == "repairable"}:
        raise DependencyAnalysisError("dependency_repair_selection_invalid")
    target = deepcopy(preview["base_manifest"])
    changes, repaired, updates = [], set(), {}
    for group in groups:
        if group["id"] not in chosen:
            continue
        repaired.update(group["roots"])
        changes.extend(group["changes"])
        for name, value in group["package_updates"].items():
            if name in updates and updates[name] != value:
                raise DependencyAnalysisError("dependency_repair_scope_changed")
            updates[name] = value
            if value is None:
                target["packages"].pop(name, None)
            else:
                target["packages"][name] = deepcopy(value)
    remaining = [item for item in preview["issues"] if item["name"] not in repaired]
    return {
        "target_manifest": target,
        "changes": changes,
        "remaining_conflicts": remaining,
    }


async def preview_layer_repair(report: dict[str, Any]) -> dict[str, Any]:
    """Solve connected repair components, keeping unrelated versions fixed."""
    from zhenxun.services.plugin_store.plugin_archive_dependencies import (
        ArchiveDependencyConflict,
        archive_dependency_contract,
    )
    from zhenxun.services.plugin_store.plugin_store_transaction import (
        ArchiveSourceBuildConflict,
        preflight_archive_dependency_policy,
    )

    snapshot = capture_environment()
    if environment_report(snapshot=snapshot)["fingerprint"] != report["fingerprint"]:
        raise DependencyAnalysisError("environment_analysis_stale")
    manifest, current = snapshot.manifest, snapshot.versions
    base = {name: dist.version for name, dist in snapshot.base.items()}
    core = protected_core()
    plugins = {name for name in current if name.startswith("nonebot-plugin-")}
    plugins.update(
        canonicalize_name(p["project_link"]) for p in manifest["plugins"].values()
    )
    fixed = {**{name: current[name] for name in plugins if name in current}, **core}
    issues = (
        report["requirement_conflicts"]
        + report["immutable_drift"]
        + report["incompatible_shared_drift"]
    )
    roots = {item["name"] for item in issues}
    edges: dict[str, set[str]] = {}
    for item in snapshot.declarations:
        edges.setdefault(item["owner"], set()).add(item["name"])
    components = []
    for root in sorted(roots):
        scope, queue = {root}, [root]
        while queue:
            for child in edges.get(queue.pop(), set()) - scope - set(fixed):
                scope.add(child)
                queue.append(child)
        components.append(({root}, scope))
    changed = True
    while changed:
        changed = False
        for index, (left_roots, left) in enumerate(components):
            for other in range(index + 1, len(components)):
                right_roots, right = components[other]
                if left & right:
                    components[index] = (left_roots | right_roots, left | right)
                    components.pop(other)
                    changed = True
                    break
            if changed:
                break
    archive = archive_dependency_contract()
    groups = []
    for group_roots, scope in components:
        group = {
            "id": sha256("\n".join(sorted(group_roots)).encode()).hexdigest()[:16],
            "roots": sorted(group_roots),
            "status": "blocked",
            "changes": [],
            "package_updates": {},
            "issues": [item for item in issues if item["name"] in group_roots],
        }
        groups.append(group)
        try:
            if any(
                name in core and base.get(name) != core[name] for name in group_roots
            ):
                raise DependencyAnalysisError(
                    "dependency_base_repair_required", details=group["issues"]
                )
            if any(item.get("kind") == "requires_python" for item in group["issues"]):
                raise DependencyAnalysisError(
                    "dependency_python_incompatible", details=group["issues"]
                )
            inputs = []
            for item in snapshot.declarations:
                if item["name"] in scope and item["owner"] not in scope:
                    req = Requirement(item["requirement"])
                    req.marker = None
                    inputs.append(str(req))
            inputs.extend(
                f"{name}=={core[name]}" if name in core else name
                for name in group_roots
            )
            inputs.extend(
                raw
                for raw in archive.get("requirements", [])
                if canonicalize_name(Requirement(raw).name) in scope
            )
            constraints = {
                name: version for name, version in current.items() if name not in scope
            }
            constraints.update(archive.get("packages", {}))
            constraints.update(fixed)
            resolved, detail = await _compile(
                inputs, constraints, wheels_only=True, preferences=current
            )
            if resolved is None:
                raise DependencyAnalysisError(
                    "current_environment_dependency_conflict",
                    detail,
                    details=group["issues"],
                )
            if (set(resolved) - scope) & (roots - group_roots):
                raise DependencyAnalysisError("dependency_repair_scope_changed")
            for name, version in resolved.items():
                if name in FORBIDDEN_LAYER_PACKAGES or current.get(name) == version:
                    continue
                if name in plugins or (name in current and name not in scope):
                    raise DependencyAnalysisError("dependency_repair_scope_changed")
                group["changes"].append(
                    {
                        "name": name,
                        "from": current.get(name),
                        "to": version,
                        "source": snapshot.sources.get(name, {}).get("layer"),
                        "requirements": [
                            item
                            for item in snapshot.declarations
                            if item["name"] == name
                        ],
                    }
                )
                group["package_updates"][name] = (
                    None
                    if base.get(name) == version
                    else {**manifest["packages"].get(name, {}), "version": version}
                )
            if not group["changes"]:
                raise DependencyAnalysisError("dependency_environment_not_repairable")
            target = deepcopy(manifest)
            for name, value in group["package_updates"].items():
                if value is None:
                    target["packages"].pop(name, None)
                else:
                    target["packages"][name] = value
            preflight_archive_dependency_policy(
                {
                    "base_manifest": manifest,
                    "target_manifest": target,
                    "source_build_confirmed": False,
                    "action": "environment_repair",
                    "affected_dependency_packages": sorted(scope),
                }
            )
            group["status"] = "repairable"
        except (
            DependencyAnalysisError,
            ArchiveDependencyConflict,
            ArchiveSourceBuildConflict,
        ) as error:
            group.update(code=error.code, diagnostic=str(error), details=error.details)
            group["changes"], group["package_updates"] = [], {}
    return {
        "base_manifest": manifest,
        "groups": groups,
        "issues": issues,
        "changes": [item for group in groups for item in group["changes"]],
        "fingerprint": report["fingerprint"],
        "mode": "layer",
    }
