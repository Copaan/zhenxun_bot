"""Shared boundaries for scoped dependency changes and compatibility trials."""

from hashlib import sha256
import json
import os
from pathlib import Path
import shlex
import sys

from packaging.requirements import Requirement
from packaging.utils import canonicalize_name

ORM_PACKAGES = frozenset(
    {
        "nonebot-plugin-orm",
        "tortoise-orm",
        "sqlalchemy",
        "alembic",
        "greenlet",
        "aiosqlite",
        "asyncpg",
        "aiomysql",
        "pymysql",
        "psycopg",
        "psycopg-binary",
        "psycopg2",
        "psycopg2-binary",
        "mysqlclient",
        "asyncmy",
        "sqlmodel",
        "databases",
        "ormar",
        "peewee",
        "piccolo",
        "gino",
    }
)


def guard_orm_changes(before: dict, after: dict) -> None:
    """已安装的 ORM 与数据库驱动只能在控制台改变版本，新增与移除不受限制

    参数:
        before: 变更前的包版本
        after: 变更后的包版本

    异常:
        DependencyAnalysisError: 已安装的 ORM 或数据库驱动版本会变化
    """
    from .dependencies import DependencyAnalysisError

    changes = [
        {"name": name, "from": before.get(name), "to": after.get(name)}
        for name in sorted(ORM_PACKAGES)
        if before.get(name) and after.get(name) and before[name] != after[name]
    ]
    if changes:
        raise DependencyAnalysisError(
            "orm_dependency_console_required", details=changes
        )


def guard_orm_request(names, installed) -> None:
    """已安装的 ORM 与数据库驱动只能在控制台安装、升级或卸载

    参数:
        names: 本次操作的包名
        installed: 当前已安装的包

    异常:
        DependencyAnalysisError: 操作涉及已安装的 ORM 或数据库驱动
    """
    from .dependencies import DependencyAnalysisError

    packages = sorted(set(names) & ORM_PACKAGES & set(installed))
    if packages:
        raise DependencyAnalysisError(
            "orm_dependency_console_required", details=packages
        )


def scoped_roots(requirements, scope) -> list[str]:
    return [
        raw for raw in requirements if canonicalize_name(Requirement(raw).name) in scope
    ]


def retained_conflicts(snapshot, affected) -> list[dict]:
    """Carry only unchanged issues outside the mutation's consumers and packages."""
    affected = set(affected)
    return [
        {**item, "baseline": conflict_evidence(snapshot, item)}
        for item in snapshot.conflicts
        if item["owner"] not in affected
        and item["name"] not in affected
        and not item.get("kind")
    ]


def external_requirements(snapshot, scope, replaced=frozenset()) -> list[str]:
    """解析范围外的来源对范围内包的依赖声明，已存在的冲突不计入

    参数:
        snapshot: 环境快照
        scope: 解析范围内的包
        replaced: 声明不再参与解析的来源

    返回:
        list[str]: 依赖声明
    """
    conflicts = {(i["owner"], i["requirement"]) for i in snapshot.conflicts}
    return [
        item["requirement"]
        for item in snapshot.declarations
        if item["name"] in scope
        and item["owner"] not in scope
        and item["owner"] not in replaced
        and not item["owner"].startswith("manual:")
        and (item["owner"], item["requirement"]) not in conflicts
    ]


def conflict_evidence(snapshot, issue: dict) -> dict:
    """记录冲突双方当前的版本与来源

    参数:
        snapshot: 环境快照
        issue: 依赖冲突

    返回:
        dict: 包名到版本与来源的映射
    """
    return {
        name: {
            "version": snapshot.versions.get(name),
            "source": snapshot.sources.get(name, {}).get("layer"),
        }
        for name in {issue["owner"], issue["name"]}
    }


def _same_state(recorded: dict | None, current: dict) -> bool:
    """只按当前记录的键比较已保存状态

    参数:
        recorded: 已保存的状态
        current: 当前状态

    返回:
        bool: 当前状态中的每个键是否都与已保存状态一致
    """
    recorded = recorded or {}
    return all(recorded.get(key) == value for key, value in current.items())


def unchanged_conflict(snapshot, issue: dict, permitted: list[dict]) -> bool:
    """判断冲突是否为已允许且双方版本与来源均未变化的冲突

    参数:
        snapshot: 环境快照
        issue: 依赖冲突
        permitted: 已允许保留的冲突

    返回:
        bool: 是否可以继续保留
    """
    from .environment import conflict_key

    evidence = conflict_evidence(snapshot, issue)
    return any(
        conflict_key(item) == conflict_key(issue)
        and all(
            _same_state((item.get("baseline") or {}).get(name), value)
            for name, value in evidence.items()
        )
        for item in permitted
    )


def trial_context(snapshot, owner: str, name: str) -> dict:
    """记录兼容性例外绑定的求解策略、插件版本与依赖版本

    参数:
        snapshot: 环境快照
        owner: 声明依赖的插件包名
        name: 依赖包名

    返回:
        dict: 兼容性例外的绑定条件
    """
    from .dependencies import SOLVER_POLICY_VERSION

    return {
        "policy_version": SOLVER_POLICY_VERSION,
        "owner_version": snapshot.versions.get(owner),
        "dependency_version": snapshot.versions.get(name),
        "dependency_source": snapshot.sources[name]["layer"],
    }


def bind_trials(snapshot) -> None:
    """Bind newly prepared exceptions; previously verified exceptions stay immutable."""
    for plugin in snapshot.manifest.get("plugins", {}).values():
        owner = canonicalize_name(plugin["project_link"])
        for item in plugin.get("compatibility_overrides", []):
            name = item["name"]
            if (
                not item.get("evidence")
                and owner in snapshot.selected
                and name in snapshot.selected
            ):
                item["evidence"] = trial_context(snapshot, owner, name)


def trial_permits(snapshot, conflict: dict) -> bool:
    from .dependencies import protected_core

    if conflict.get("kind") or not conflict.get("actual"):
        return False
    owner, name = conflict["owner"], conflict["name"]
    if name in ORM_PACKAGES or name in protected_core():
        return False
    for plugin in snapshot.manifest.get("plugins", {}).values():
        if canonicalize_name(plugin["project_link"]) != owner:
            continue
        for item in plugin.get("compatibility_overrides", []):
            if (
                item.get("name") == name
                and (
                    item.get("requirement", item.get("declared_requirement"))
                    == conflict["requirement"]
                )
                and item.get("effective_version") == conflict["actual"]
                and _same_state(
                    item.get("evidence"), trial_context(snapshot, owner, name)
                )
            ):
                return True
    return False


def summary_revision(value: dict) -> str:
    """Fingerprint user-visible effects independently of transient preview IDs."""
    keys = (
        "changes",
        "retained",
        "manual_requirements",
        "package_changes",
        "compatibility_overrides",
        "source_build_required",
        "database_type",
        "database_migration_possible",
        "warnings",
        "restart_required",
        "resolved_packages",
        "solver_policy_version",
        "plugin",
        "artifact_hashes",
    )
    return sha256(
        json.dumps(
            {key: value.get(key) for key in keys}, sort_keys=True, default=str
        ).encode()
    ).hexdigest()


def orm_console_instructions(transaction: dict) -> dict | None:
    if transaction.get("state") != "migration_blocked":
        return None
    arguments = [
        sys.executable,
        "-m",
        "zhenxun.services.nonebot_store.orm_migration",
        "manual-upgrade",
        "--revision",
        transaction["revision"],
        "--backup-confirmed",
    ]
    command = (
        ("& " + " ".join("'" + part.replace("'", "''") + "'" for part in arguments))
        if os.name == "nt"
        else shlex.join(arguments)
    )
    return {
        "working_directory": str(Path.cwd().resolve()),
        "command": command,
        "requires_stopped_instance": True,
        "requires_database_backup": True,
    }
