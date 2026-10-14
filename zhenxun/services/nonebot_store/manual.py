"""Resolve explicit package requests into the shared managed generation."""

from __future__ import annotations

from copy import deepcopy
from hashlib import sha256
import json
from pathlib import Path
from typing import Any

from packaging.requirements import Requirement


def normalize_requirements(values: list[str], action: str) -> dict[str, list[str]]:
    """Accept package requirements, never installer arguments or direct URLs."""
    from .dependencies import (
        FORBIDDEN_LAYER_PACKAGES,
        DependencyAnalysisError,
        parse_requirement,
    )

    if action not in {"install", "uninstall"} or not values:
        raise DependencyAnalysisError("dependency_request_invalid")
    result: dict[str, list[str]] = {}
    for value in values:
        requirement = parse_requirement(value)
        name = requirement.name
        if name in FORBIDDEN_LAYER_PACKAGES:
            raise DependencyAnalysisError("dependency_requirement_unsupported", name)
        if action == "uninstall" and (
            requirement.specifier or requirement.extras or requirement.marker
        ):
            raise DependencyAnalysisError("dependency_uninstall_name_required", name)
        raw = str(requirement)
        if raw not in result.setdefault(name, []):
            result[name].append(raw)
    return {name: sorted(items) for name, items in sorted(result.items())}


def manual_requirements(manifest: dict[str, Any]) -> dict[str, list[str]]:
    from .dependencies import DependencyAnalysisError

    values = manifest.get("manual_requirements", {})
    if not isinstance(values, dict):
        raise DependencyAnalysisError("manual_dependency_manifest_invalid")
    result = {}
    for name, requirements in values.items():
        if not isinstance(requirements, list) or not requirements:
            raise DependencyAnalysisError("manual_dependency_manifest_invalid")
        normalized = normalize_requirements(requirements, "install")
        if set(normalized) != {name}:
            raise DependencyAnalysisError("manual_dependency_manifest_invalid")
        result.update(normalized)
    return result


def request_digest(action: str, values: list[str]) -> str:
    return sha256(
        json.dumps(
            [action, normalize_requirements(values, action)], sort_keys=True
        ).encode()
    ).hexdigest()


async def preview_manual_dependencies(
    values: list[str], action: str, *, manifest: dict[str, Any] | None = None
) -> dict[str, Any]:
    """Prepare a scoped plan without installing or changing the active manifest."""
    from zhenxun.services.plugin_store.plugin_store_receipts import (
        receipt_dependency_inputs,
    )

    from .dependencies import (
        FORBIDDEN_LAYER_PACKAGES,
        SOLVER_POLICY_VERSION,
        DependencyAnalysisError,
        _compile,
        environment_report,
        project_requirements,
        protected_core,
    )
    from .environment import capture_environment, dependency_roots, released_packages

    async def resolve(requirements, constraints, **options):
        active = [
            raw
            for raw in requirements
            if not Requirement(raw).marker or Requirement(raw).marker.evaluate()
        ]
        return await _compile(active, constraints, **options) if active else ({}, "")

    requested = normalize_requirements(values, action)
    snapshot = capture_environment()
    report = environment_report(snapshot=snapshot)
    if report["interpreter_mismatch"] or report["project_lock_stale"]:
        raise DependencyAnalysisError("dependency_environment_unconfirmed")
    if report["layer_mismatch"] or report["immutable_drift"]:
        raise DependencyAnalysisError("dependency_environment_repair_required")
    from .policy import (
        ORM_PACKAGES,
        external_requirements,
        guard_orm_changes,
        guard_orm_request,
        retained_conflicts,
        scoped_roots,
        summary_revision,
    )

    guard_orm_request(requested)
    target = deepcopy(manifest if manifest is not None else snapshot.manifest)
    original = deepcopy(target)
    roots = manual_requirements(target)
    if action == "install":
        roots.update(requested)
    else:
        missing = sorted(set(requested) - set(roots))
        if missing:
            raise DependencyAnalysisError("dependency_not_managed", details=missing)
        for name in requested:
            roots.pop(name)
    target["manual_requirements"] = roots
    packages = target.setdefault("packages", {})
    current = snapshot.versions
    baseline = {**current, **{name: info["version"] for name, info in packages.items()}}
    base = {name: dist.version for name, dist in snapshot.base.items()}
    core = protected_core()
    frozen = {
        **core,
        **{name: baseline[name] for name in ORM_PACKAGES if name in baseline},
    }
    plugin_roots = [
        raw
        for owner, raw in dependency_roots(target)
        if owner != "pyproject.toml" and not owner.startswith("manual:")
    ]
    root_inputs = [raw for items in roots.values() for raw in items]
    retained = []
    scope = set(requested)
    removing: set[str] = set()
    if action == "uninstall":
        for name in requested:
            owners = sorted(
                {
                    item["owner"]
                    for item in snapshot.declarations
                    if item["name"] == name
                    and item["owner"] not in {f"manual:{key}" for key in requested}
                    and item["owner"] not in requested
                }
                | ({"project"} if name in core else set())
            )
            if owners:
                retained.append(
                    {"name": name, "owners": owners, "version": baseline.get(name)}
                )
            else:
                packages.pop(name, None)
                removing.add(name)
                if name in base:
                    retained.append(
                        {"name": name, "owners": ["base"], "version": base[name]}
                    )
        candidate_inputs = root_inputs
    else:
        candidate_inputs = [raw for items in requested.values() for raw in items]
        probe, diagnostic = await resolve(
            candidate_inputs, frozen, wheels_only=False, preferences=baseline
        )
        if probe is None:
            raise DependencyAnalysisError("dependency_resolution_failed", diagnostic)
        scope.update(probe)
    external = external_requirements(snapshot, scope)
    requirements = [
        *scoped_roots(project_requirements(), scope),
        *scoped_roots(plugin_roots, scope),
        *scoped_roots(root_inputs, scope),
        *external,
    ]
    constraints = {**baseline, **frozen}
    for name in removing:
        constraints.pop(name, None)
        if name in base:
            constraints[name] = base[name]
    scoped = {
        **{name: version for name, version in constraints.items() if name not in scope},
        **frozen,
        **{name: base[name] for name in removing if name in base},
    }
    resolved, diagnostic = await resolve(
        requirements,
        constraints,
        wheels_only=False,
        preferences=baseline,
    )
    if resolved is None:
        resolved, diagnostic = await resolve(
            requirements,
            scoped,
            wheels_only=False,
            preferences=baseline,
        )
    if resolved is None:
        raise DependencyAnalysisError("dependency_resolution_failed", diagnostic)
    for name, version in resolved.items():
        if name in baseline and name not in scope and version != baseline[name]:
            raise DependencyAnalysisError("dependency_resolution_scope_changed", name)
        if name in core:
            if version != core[name]:
                raise DependencyAnalysisError("core_dependency_conflict", name)
            continue
        if name in FORBIDDEN_LAYER_PACKAGES:
            continue
        if name in scope or name not in baseline:
            if base.get(name) == version:
                packages.pop(name, None)
            else:
                packages[name] = {**packages.get(name, {}), "version": version}
    if action == "uninstall":
        try:
            sources = receipt_dependency_inputs()
            released, imported = released_packages(
                original, target, sources_before=sources, sources_after=sources
            )
        except (DependencyAnalysisError, OSError, KeyError, TypeError):
            released, imported = [], []
        for name in released:
            packages.pop(name, None)
        retained.extend(imported)
    wheels, wheel_error = (
        await resolve(
            [f"{name}=={item['version']}" for name, item in packages.items()],
            {},
            wheels_only=True,
            no_dependencies=True,
        )
        if packages
        else ({}, "")
    )
    changes = []
    effective = {**base, **{name: info["version"] for name, info in packages.items()}}
    guard_orm_changes(current, effective)
    for name in sorted(set(baseline) | set(effective)):
        before, after = baseline.get(name), effective.get(name)
        if before != after:
            changes.append(
                {
                    "name": name,
                    "from": before,
                    "to": after,
                    "source": "managed"
                    if name in packages
                    else "base"
                    if after
                    else "removed",
                }
            )
    affected = {item["name"] for item in changes}
    target["dependency_warnings"] = retained_conflicts(snapshot, affected)
    plan = {
        "target_manifest": target,
        "fingerprint": report["fingerprint"],
        "request_digest": request_digest(action, values),
        "solver_policy_version": SOLVER_POLICY_VERSION,
        "changes": changes,
        "retained": retained,
        "manual_requirements": roots,
        "source_build_required": wheels is None,
        "diagnostic": wheel_error if wheels is None else "",
        "no_change": target == original,
        "blocked_reasons": [],
        "remaining_conflicts": target["dependency_warnings"],
        "restart_required": bool(
            changes or target.get("packages") != original.get("packages")
        ),
        "declaration_only": not changes
        and target.get("packages") == original.get("packages"),
    }
    plan["summary_revision"] = summary_revision(plan)
    return plan


def stage_requirement_files(files: list[Path]) -> bool:
    """将插件 requirements 文件登记为手动依赖，并写入待应用的依赖层事务

    依赖层由启动器在下次启动 worker 前构建，核心虚拟环境不会被修改。

    参数:
        files: requirements 文件列表

    返回:
        bool: 是否写入了待应用的事务
    """
    import asyncio

    from .dependencies import DependencyAnalysisError, read_requirement_files
    from .storage import load_manifest, pending_transaction

    requirements = read_requirement_files(files)
    if not requirements:
        return False
    pending = pending_transaction()
    if pending and pending.get("state") != "pending_restart":
        raise DependencyAnalysisError("dependency_transaction_pending")
    plan = asyncio.run(
        preview_manual_dependencies(
            requirements,
            "install",
            manifest=(pending or {}).get("target_manifest") or load_manifest(),
        )
    )
    return stage_plan(plan, requirements, pending)


def stage_plan(
    plan: dict[str, Any], requirements: list[str], pending: dict | None = None
) -> bool:
    """把依赖计划写入待应用的依赖层事务，由启动器在下次启动 worker 前构建

    参数:
        plan: preview_manual_dependencies 返回的计划
        requirements: 本次登记的依赖声明
        pending: 当前待重启的事务，在其基础上追加

    返回:
        bool: 是否写入了待应用的事务
    """
    from .dependencies import DependencyAnalysisError
    from .storage import defer_transaction, draft_transaction, utc_now

    if plan["no_change"]:
        return False
    if plan["source_build_required"]:
        raise DependencyAnalysisError(
            "source_build_confirmation_required", plan["diagnostic"]
        )
    transaction = draft_transaction(
        pending,
        plan["target_manifest"],
        action="manual_dependencies",
        remaining_conflicts=deepcopy(plan.get("remaining_conflicts", [])),
    )
    transaction["operations"].append(
        {
            "kind": "manual_dependencies",
            "action": "install",
            "name": ", ".join(requirements),
            "created_at": utc_now(),
            "target_manifest": deepcopy(plan["target_manifest"]),
            "database_migration_possible": False,
            "source_build_confirmed": False,
        }
    )
    defer_transaction(transaction)
    return True
