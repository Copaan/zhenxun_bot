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
    shared_dependencies,
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
    target["dependency_warnings"] = [
        item
        for item in preview.get("bound_conflicts", [])
        if item["name"] not in repaired and item["owner"] not in repaired
    ]
    return {
        "target_manifest": target,
        "changes": changes,
        "remaining_conflicts": remaining,
        "source_build_required": any(
            group.get("source_build_required")
            for group in groups
            if group["id"] in chosen
        ),
    }


def _layer_rebuild_preview(report: dict[str, Any], snapshot) -> dict[str, Any]:
    """依赖层内容与清单不一致时，按清单重建依赖层的修复预览

    与清单一致的包沿用当前依赖层，其余包按清单记录的版本重新安装，清单以外的
    包不再保留。

    参数:
        report: 依赖环境报告
        snapshot: 当前环境快照

    返回:
        dict[str, Any]: 只含一个重建分组的修复预览
    """
    from .policy import retained_conflicts

    mismatch = report["layer_mismatch"]
    names = sorted(item["name"] for item in mismatch)
    changes = [
        {
            "name": item["name"],
            "from": item["actual"],
            "to": item["recorded"],
            "source": "active_generation",
            "requirements": [],
        }
        for item in mismatch
    ]
    group = {
        "id": sha256("\n".join(names).encode()).hexdigest()[:16],
        "roots": names,
        "status": "repairable",
        "changes": changes,
        "package_updates": {},
        "issues": mismatch,
        "source_build_required": True,
    }
    return {
        "base_manifest": snapshot.manifest,
        "bound_conflicts": retained_conflicts(snapshot, set()),
        "groups": [group],
        "issues": [*mismatch, *report["requirement_conflicts"]],
        "changes": changes,
        "fingerprint": report["fingerprint"],
        "mode": "layer",
    }


async def preview_layer_repair(report: dict[str, Any]) -> dict[str, Any]:
    """按冲突分组生成依赖层修复预览，分组外的包版本保持不变

    依赖层内容与清单不一致时，只给出按清单重建依赖层的分组。

    参数:
        report: 依赖环境报告

    返回:
        dict[str, Any]: 修复预览
    """
    snapshot = capture_environment()
    from .policy import ORM_PACKAGES, guard_orm_changes, retained_conflicts

    if environment_report(snapshot=snapshot)["fingerprint"] != report["fingerprint"]:
        raise DependencyAnalysisError("environment_analysis_stale")
    if report["layer_mismatch"]:
        return _layer_rebuild_preview(report, snapshot)
    manifest, current = snapshot.manifest, snapshot.versions
    base = {name: dist.version for name, dist in snapshot.base.items()}
    core = protected_core()
    floors = shared_dependencies()
    plugins = {name for name in current if name.startswith("nonebot-plugin-")}
    plugins.update(
        canonicalize_name(p["project_link"]) for p in manifest["plugins"].values()
    )
    fixed = {**{name: current[name] for name in plugins if name in current}, **core}
    fixed.update({name: current[name] for name in ORM_PACKAGES if name in current})
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
            constraints = {
                name: version for name, version in current.items() if name not in scope
            }
            constraints.update(fixed)
            resolved, detail = await _compile(
                inputs,
                constraints,
                wheels_only=True,
                preferences=current,
                floors=floors,
            )
            group["source_build_required"] = False
            if resolved is None:
                resolved, source_detail = await _compile(
                    inputs,
                    constraints,
                    wheels_only=False,
                    preferences=current,
                    floors=floors,
                )
                if resolved is not None:
                    group["source_build_required"] = True
                    group["diagnostic"] = detail
                else:
                    detail = source_detail or detail
            if resolved is None:
                raise DependencyAnalysisError(
                    "dependency_resolution_failed",
                    detail,
                    details=group["issues"],
                )
            guard_orm_changes(current, {**current, **resolved})
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
            group["status"] = "repairable"
        except DependencyAnalysisError as error:
            group.update(code=error.code, diagnostic=str(error), details=error.details)
            group["changes"], group["package_updates"] = [], {}
    return {
        "base_manifest": manifest,
        "bound_conflicts": retained_conflicts(snapshot, set()),
        "groups": groups,
        "issues": issues,
        "changes": [item for group in groups for item in group["changes"]],
        "fingerprint": report["fingerprint"],
        "mode": "layer",
    }
