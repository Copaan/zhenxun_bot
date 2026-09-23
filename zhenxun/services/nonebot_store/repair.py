"""Preview targeted repairs without modifying the active dependency generation."""

from copy import deepcopy
from typing import Any

from .dependencies import (
    FORBIDDEN_LAYER_PACKAGES,
    DependencyAnalysisError,
    _compile,
    base_installed_inventory,
    installed_inventory,
    project_requirements,
    protected_core,
)
from .storage import load_manifest


async def preview_layer_repair(report: dict[str, Any]) -> dict[str, Any]:
    """Solve conflicting roots together while pinning unrelated installed packages."""
    from zhenxun.services.plugin_store.plugin_archive_dependencies import (
        archive_dependency_contract,
    )
    from zhenxun.services.plugin_store.plugin_store_transaction import (
        preflight_archive_dependency_policy,
    )

    manifest = load_manifest()
    current = installed_inventory()
    base = base_installed_inventory()
    core = protected_core()
    issues = [
        *report.get("immutable_drift", []),
        *report.get("incompatible_shared_drift", []),
        *report.get("requirement_conflicts", []),
    ]
    roots = {item["name"] for item in issues}
    declarations = project_requirements()
    for plugin in manifest.get("plugins", {}).values():
        if plugin.get("state") == "managed":
            declarations.extend(
                plugin.get("resolution_inputs")
                or [f"{plugin['project_link']}=={plugin['version']}"]
            )
    repair_inputs = [
        str(item["requirement"]) for item in issues if item.get("requirement")
    ]
    if not repair_inputs:
        raise DependencyAnalysisError("dependency_environment_not_repairable")
    closure, detail = await _compile(
        repair_inputs, core, wheels_only=True, preferences=current
    )
    if closure is None:
        raise DependencyAnalysisError(
            "current_environment_dependency_conflict", detail, details=issues
        )
    allowed = roots | set(closure)
    declarations.extend(
        f"{name}=={version}"
        for name, version in current.items()
        if name not in allowed and name not in FORBIDDEN_LAYER_PACKAGES
    )
    archive = archive_dependency_contract()
    constraints = {
        name: version for name, version in current.items() if name not in allowed
    }
    constraints.update(archive.get("packages", {}))
    constraints.update(core)
    resolved, detail = await _compile(
        [*declarations, *repair_inputs, *archive.get("requirements", [])],
        constraints,
        wheels_only=True,
        preferences=current,
    )
    if resolved is None:
        raise DependencyAnalysisError(
            "current_environment_dependency_conflict", detail, details=issues
        )
    target = deepcopy(manifest)
    packages = target["packages"]
    for name, version in resolved.items():
        if name in core or name in FORBIDDEN_LAYER_PACKAGES:
            if name in packages and base.get(name) == version:
                packages.pop(name)
            continue
        if name in packages or current.get(name) != version:
            if base.get(name) == version:
                packages.pop(name, None)
            else:
                packages[name] = {**packages.get(name, {}), "version": version}
    changes = [
        {
            "name": name,
            "from": current.get(name),
            "to": version,
            "source": "active_generation" if name in manifest["packages"] else "base",
            "requirements": [item for item in issues if item["name"] == name],
        }
        for name, version in sorted(resolved.items())
        if current.get(name) != version
    ]
    if any(item["name"] not in allowed for item in changes):
        raise DependencyAnalysisError(
            "dependency_repair_scope_changed", details=changes
        )
    if any(
        name in core and base.get(name) != version for name, version in resolved.items()
    ):
        raise DependencyAnalysisError("dependency_base_repair_required", details=issues)
    transaction = {
        "base_manifest": manifest,
        "target_manifest": target,
        "source_build_confirmed": False,
        "action": "environment_repair",
        "affected_dependency_packages": sorted(allowed),
    }
    preflight_archive_dependency_policy(transaction)
    return {
        "target_manifest": target,
        "changes": changes,
        "issues": issues,
        "fingerprint": report["fingerprint"],
        "mode": "layer",
    }
