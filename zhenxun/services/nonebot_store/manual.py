"""Resolve explicit package requests into the shared managed generation."""

from __future__ import annotations

from copy import deepcopy
from hashlib import sha256
import json
from pathlib import Path
from typing import Any

from .storage import transaction_lock


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
    values: list[str],
    action: str,
    *,
    manifest: dict[str, Any] | None = None,
    restore: bool = False,
    preferences: dict[str, str] | None = None,
) -> dict[str, Any]:
    """生成手动依赖的安装或卸载计划，不安装也不修改当前清单

    安装时与其他依赖来源一起解析；卸载时移除不再被其他来源声明的包，以及只为它们
    安装、未被源码插件导入的子依赖。恢复备份时 restore 为 True：解析允许构建源码包，
    ORM 与数据库驱动中不属于核心依赖的包可以随计划安装或变更。

    参数:
        values: 依赖声明
        action: install 或 uninstall
        manifest: 作为基线的依赖层清单，为空时使用当前清单
        restore: 是否为恢复备份生成的计划
        preferences: 当前环境中没有的包优先选用的版本

    返回:
        dict[str, Any]: 依赖计划
    """
    from zhenxun.services.plugin_store.plugin_store_receipts import (
        receipt_dependency_inputs,
    )

    from .dependencies import (
        SOLVER_POLICY_VERSION,
        DependencyAnalysisError,
        environment_report,
        protected_core,
        resolve_layer,
    )
    from .environment import capture_environment, released_packages
    from .policy import guard_orm_request, retained_conflicts, summary_revision

    requested = normalize_requirements(values, action)
    snapshot = capture_environment()
    report = environment_report(snapshot=snapshot)
    if report["interpreter_mismatch"] or report["project_lock_stale"]:
        raise DependencyAnalysisError("dependency_environment_unconfirmed")
    if report["layer_mismatch"] or report["immutable_drift"]:
        raise DependencyAnalysisError("dependency_environment_repair_required")
    if not restore:
        guard_orm_request(requested, snapshot.versions)
    target = deepcopy(manifest if manifest is not None else snapshot.manifest)
    original = deepcopy(target)
    roots = manual_requirements(target)
    current = snapshot.versions
    base = {name: dist.version for name, dist in snapshot.base.items()}
    retained = []
    diagnostic = ""
    source_build_required = False
    if action == "install":
        roots.update(requested)
        target["manual_requirements"] = roots
        result = await resolve_layer(
            [raw for items in requested.values() for raw in items],
            snapshot=snapshot,
            manifest=target,
            sources=receipt_dependency_inputs(),
            allow_build=restore,
            restore=restore,
            preferences=preferences,
        )
        target["packages"] = result["packages"]
        source_build_required = result["source_build_required"]
        diagnostic = result["diagnostic"]
    else:
        missing = sorted(set(requested) - set(roots))
        if missing:
            raise DependencyAnalysisError("dependency_not_managed", details=missing)
        for name in requested:
            roots.pop(name)
        target["manual_requirements"] = roots
        packages = target.setdefault("packages", {})
        core = protected_core()
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
                    {"name": name, "owners": owners, "version": current.get(name)}
                )
                continue
            packages.pop(name, None)
            if name in base:
                retained.append(
                    {"name": name, "owners": ["base"], "version": base[name]}
                )
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
    effective = {
        **base,
        **{name: info["version"] for name, info in target["packages"].items()},
    }
    changes = [
        {
            "name": name,
            "from": current.get(name),
            "to": effective.get(name),
            "source": "managed"
            if name in target["packages"]
            else "base"
            if effective.get(name)
            else "removed",
        }
        for name in sorted(set(current) | set(effective))
        if current.get(name) != effective.get(name)
    ]
    target["dependency_warnings"] = retained_conflicts(
        snapshot, {item["name"] for item in changes}
    )
    plan = {
        "target_manifest": target,
        "fingerprint": report["fingerprint"],
        "request_digest": request_digest(action, values),
        "solver_policy_version": SOLVER_POLICY_VERSION,
        "changes": changes,
        "retained": retained,
        "manual_requirements": roots,
        "source_build_required": source_build_required,
        "diagnostic": diagnostic,
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


@transaction_lock()
def stage_requirement_files(files: list[Path]) -> bool:
    """按插件 requirements 文件更新依赖声明，写入待应用的依赖层事务

    每个文件的依赖记在该文件路径名下，替换它此前的声明；文件已删除或为空时只移除
    声明，已安装的包保留。依赖不变且没有待应用事务时直接保存清单；否则由启动器在
    下次启动 worker 前构建依赖层，核心虚拟环境不会被修改。

    参数:
        files: requirements 文件，路径相对于项目根目录

    返回:
        bool: 是否写入了待应用的事务
    """
    import asyncio

    from zhenxun.services.plugin_store.plugin_store_receipts import (
        receipt_dependency_inputs,
    )

    from .dependencies import (
        DependencyAnalysisError,
        read_requirement_files,
        resolve_layer,
    )
    from .environment import capture_environment
    from .policy import retained_conflicts
    from .storage import (
        defer_transaction,
        draft_transaction,
        load_manifest,
        pending_transaction,
        save_manifest,
        utc_now,
    )

    pending = pending_transaction()
    if pending and pending.get("state") != "pending_restart":
        raise DependencyAnalysisError("dependency_transaction_pending")
    original = (pending or {}).get("target_manifest") or load_manifest()
    target = deepcopy(original)
    declared = target.setdefault("plugin_requirements", {})
    owners = {path.as_posix() for path in files}
    requests = []
    for path in files:
        values = read_requirement_files([path]) if path.is_file() else []
        if values:
            declared[path.as_posix()] = values
            requests.extend(values)
        else:
            declared.pop(path.as_posix(), None)
    if target == original:
        return False
    snapshot = capture_environment()
    if requests:
        result = asyncio.run(
            resolve_layer(
                requests,
                snapshot=snapshot,
                manifest=target,
                sources=receipt_dependency_inputs(),
                replaced=frozenset(owners),
            )
        )
        if result["source_build_required"]:
            raise DependencyAnalysisError(
                "source_build_confirmation_required", result["diagnostic"]
            )
        target["packages"] = result["packages"]
    before, after = original.get("packages", {}), target.get("packages", {})
    if not pending and before == after:
        save_manifest(target)
        return False
    changed = {
        name for name in set(before) | set(after) if before.get(name) != after.get(name)
    }
    transaction = draft_transaction(
        pending,
        target,
        action="plugin_requirements",
        remaining_conflicts=retained_conflicts(snapshot, changed),
    )
    transaction["operations"].append(
        {
            "kind": "plugin_requirements",
            "action": "install",
            "name": ", ".join(sorted(owners)),
            "created_at": utc_now(),
            "target_manifest": deepcopy(target),
            "database_migration_possible": False,
            "source_build_confirmed": False,
        }
    )
    defer_transaction(transaction)
    return True


@transaction_lock()
def stage_plan(
    plan: dict[str, Any],
    requirements: list[str],
    pending: dict | None = None,
    *,
    restore: bool = False,
) -> bool:
    """把依赖计划写入待应用的依赖层事务，由启动器在下次启动 worker 前构建

    恢复备份的计划允许构建源码包，并允许安装非核心的 ORM 与数据库驱动；
    目标依赖包含 nonebot-plugin-orm 时，启动器在启动 worker 前执行数据库迁移，
    迁移时同时加载带迁移脚本的源码插件。

    参数:
        plan: preview_manual_dependencies 返回的计划
        requirements: 本次登记的依赖声明
        pending: 当前待重启的事务，在其基础上追加
        restore: 是否为恢复备份生成的计划

    返回:
        bool: 是否写入了待应用的事务
    """
    from packaging.utils import canonicalize_name

    from .dependencies import DependencyAnalysisError, orm_database_type
    from .storage import defer_transaction, draft_transaction, utc_now

    if plan["no_change"]:
        return False
    if plan["source_build_required"] and not restore:
        raise DependencyAnalysisError(
            "source_build_confirmation_required", plan["diagnostic"]
        )
    migration = restore and "nonebot-plugin-orm" in {
        canonicalize_name(name) for name in plan["target_manifest"].get("packages", {})
    }
    transaction = draft_transaction(
        pending,
        plan["target_manifest"],
        action="manual_dependencies",
        remaining_conflicts=deepcopy(plan.get("remaining_conflicts", [])),
        **(
            {
                "source_build_confirmed": True,
                "orm_packages_allowed": True,
                "restore": True,
            }
            if restore
            else {}
        ),
    )
    if migration:
        transaction.update(
            database_migration_possible=True,
            database_migration_confirmed=True,
            database_type=orm_database_type(),
        )
    transaction["operations"].append(
        {
            "kind": "manual_dependencies",
            "action": "install",
            "name": ", ".join(requirements),
            "created_at": utc_now(),
            "target_manifest": deepcopy(plan["target_manifest"]),
            "database_migration_possible": False,
            "source_build_confirmed": restore,
        }
    )
    defer_transaction(transaction)
    return True
