"""恢复备份时的依赖解析：把备份中的依赖解析为当前环境可用的版本"""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
import platform
import re
import sys

from packaging.requirements import InvalidRequirement, Requirement
from packaging.specifiers import SpecifierSet
from packaging.utils import canonicalize_name
from packaging.version import InvalidVersion, Version

from .deps import KINDS, _safe_url, plugin_requirement_files

_NEARBY_LIMIT = 5
_IMPLIED_KINDS = {"venv", "layer"}
"""自动记录的依赖来源：被其他依赖间接需要时只作为子依赖，无法安装时总是跳过"""
_UNAVAILABLE = re.compile(
    r"([A-Za-z0-9][A-Za-z0-9._-]*)(?:==\S+)?\s+ha(?:s|ve)\s+no\s+wheels\s+with\s+"
    r"a\s+matching\s+(platform|Python)"
)


def describe(details) -> str:
    """把依赖错误的详情整理为简短说明

    参数:
        details: DependencyAnalysisError.details

    返回:
        str: 说明，没有可读详情时为空
    """
    items = details if isinstance(details, list) else [details]
    parts = []
    for item in items[:5]:
        if isinstance(item, str):
            parts.append(item)
        elif not isinstance(item, dict):
            continue
        elif item.get("owner") and item.get("requirement"):
            actual = item.get("actual")
            parts.append(
                f"{item['owner']} 需要 {item['requirement']}，"
                + (f"当前为 {actual}" if actual else "当前未安装")
            )
        elif item.get("name") or item.get("package"):
            parts.append(str(item.get("name") or item.get("package")))
    if len(items) > 5 and parts:
        parts.append(f"等 {len(items)} 项")
    return "、".join(parts)


def layer_status(revision: str) -> dict:
    """恢复写入的依赖层事务的结果

    参数:
        revision: 事务修订号

    返回:
        dict: state 为 pending / done / failed / unknown（事务已被后续操作替换），
            失败时带 error_code 与 detail，启动验证失败时 detail 列出导致失败的问题；
            完成但有插件加载失败等告警时带 warnings
    """
    from zhenxun.services.nonebot_store.storage import (
        is_settled,
        pending_transaction,
        settled_transactions,
    )

    transaction = pending_transaction()
    if not transaction or transaction.get("revision") != revision:
        transaction = next(
            (item for item in settled_transactions() if item["revision"] == revision),
            None,
        )
        if transaction is None:
            return {"state": "unknown"}
    elif transaction.get("state") != "failed" and not is_settled(transaction):
        return {"state": "pending"}
    if transaction.get("state") == "failed" or transaction.get("rollback_confirmed"):
        modules = [
            str(item.get("module_name") or item.get("store_key"))
            for item in transaction.get("failure_reasons") or []
            if isinstance(item, dict)
            and (item.get("module_name") or item.get("store_key"))
        ]
        return {
            "state": "failed",
            "error_code": transaction.get("error_code")
            or "nonebot_plugin_apply_failed",
            "detail": describe(transaction.get("error_details"))
            or transaction.get("error")
            or (f"加载失败的插件：{'、'.join(modules)}" if modules else ""),
        }
    warnings = transaction.get("warnings")
    return {"state": "done", **({"warnings": warnings} if warnings else {})}


def _roots(saved: dict) -> dict[str, dict]:
    """备份中需要恢复的顶层依赖：商店插件、手动依赖与其他依赖

    备份时随本体安装、当前本体已不再包含的顶层包按虚拟环境中的包恢复。随本体
    安装的包不按备份版本固定，由解析器优先沿用当前环境中的版本。

    参数:
        saved: 备份中的 dependencies.json

    返回:
        dict[str, dict]: 键 -> {kind, name, requirements, version, url, plugin,
            unsupported}，商店插件以插件键为键，其余以包名为键
    """
    from zhenxun.services.nonebot_store.dependencies import project_closure

    shared = set(project_closure())
    foreign = _foreign_core(saved)
    dropped = [
        (name, {"kind": "venv", **item})
        for name, item in (saved.get("core") or {}).items()
        if name not in shared and name not in foreign
    ]
    roots: dict[str, dict] = {}
    for key, plugin in saved["plugins"].items():
        roots[key] = {
            "kind": "nonebot_plugin",
            "name": canonicalize_name(plugin["project_link"]),
            "requirements": [],
            "version": plugin["version"],
            "url": None,
            "plugin": plugin,
        }
    items = [
        (
            canonicalize_name(name),
            {
                "kind": "manual",
                "requirements": list(values),
                "version": (saved["packages"].get(canonicalize_name(name)) or {}).get(
                    "version"
                ),
            },
        )
        for name, values in saved["manual_requirements"].items()
    ]
    for name, item in [*dropped, *items, *saved["extra_requirements"].items()]:
        roots[name] = {
            "kind": item.get("kind") or "venv",
            "name": name,
            "requirements": list(item.get("requirements") or []),
            "version": None if name in shared else item.get("version"),
            "url": _safe_url(item["url"]) if item.get("url") else None,
            "plugin": None,
            "unsupported": item.get("unsupported"),
        }
    return roots


def _foreign_core(saved: dict) -> set[str]:
    """备份时随本体安装、当前本体仍然声明但不适用于当前平台的包

    参数:
        saved: 备份中的 dependencies.json

    返回:
        set[str]: 规范化包名
    """
    from zhenxun.services.nonebot_store.dependencies import (
        _lock_packages,
        project_closure,
    )

    locked = set(_lock_packages()[1]) - set(project_closure())
    return {name for name in saved.get("core") or {} if name in locked}


def _implicit_roots(saved: dict, files: Path | None, roots: dict) -> dict:
    """恢复的插件源码直接导入、备份时由本体间接带入、当前环境已不再提供的包

    按模块名与包名相同的约定匹配（如 pydantic_settings 对应 pydantic-settings），
    作为虚拟环境中的包恢复，无法安装时跳过。

    参数:
        saved: 备份中的 dependencies.json
        files: 备份包解压后的 files 目录，未恢复插件代码时为 None
        roots: 已确定的顶层依赖

    返回:
        dict: 键 -> 顶层依赖
    """
    from zhenxun.services.nonebot_store.dependencies import (
        FORBIDDEN_LAYER_PACKAGES,
        _lock_packages,
    )
    from zhenxun.services.nonebot_store.environment import (
        base_distributions,
        imported_modules,
    )

    installed = saved.get("installed") or {}
    if files is None or not installed:
        return {}
    present = set(base_distributions()[0]) | set(_lock_packages()[1])
    present |= {root["name"] for root in roots.values()} | set(saved["packages"])
    result = {}
    for module in imported_modules([files / "zhenxun/plugins"]):
        name = canonicalize_name(module)
        if (
            name in installed
            and name not in present
            and name not in FORBIDDEN_LAYER_PACKAGES
        ):
            result[name] = {
                "kind": "venv",
                "name": name,
                "requirements": [],
                "version": str(installed[name]),
                "url": None,
                "plugin": None,
                "unsupported": None,
            }
    return result


def _root_requirements(root: dict, pin: str | None) -> list[str]:
    """顶层依赖参与解析的依赖声明

    参数:
        root: 顶层依赖
        pin: 选定的版本，为 None 时按声明解析

    返回:
        list[str]: 依赖声明
    """
    plugin = root["plugin"]
    if plugin is not None:
        values = []
        found = False
        name = canonicalize_name(plugin["project_link"])
        for raw in plugin.get("resolution_inputs") or []:
            requirement = Requirement(raw)
            if canonicalize_name(requirement.name) == name:
                from zhenxun.services.nonebot_store.dependencies import (
                    DependencyAnalysisError,
                )

                found = True
                if (requirement.specifier and pin not in requirement.specifier) or (
                    requirement.url and pin != plugin["version"]
                ):
                    raise DependencyAnalysisError(
                        "dependency_explicit_version_conflict",
                        details=[f"{name} 的安装声明 {raw} 不包含版本 {pin}"],
                    )
                if not requirement.url:
                    requirement.specifier = SpecifierSet(f"=={pin}")
            values.append(str(requirement))
        if not found:
            values.append(f"{plugin['project_link']}=={pin}")
        return values
    declared = root["requirements"] or [root["name"]]
    if pin is None:
        return list(declared)
    result = []
    for raw in declared:
        requirement = Requirement(raw)
        if canonicalize_name(requirement.name) == root["name"]:
            if requirement.specifier and pin not in requirement.specifier:
                from zhenxun.services.nonebot_store.dependencies import (
                    DependencyAnalysisError,
                )

                raise DependencyAnalysisError(
                    "dependency_explicit_version_conflict",
                    details=[f"{root['name']} 的声明 {raw} 不包含版本 {pin}"],
                )
            requirement.specifier = SpecifierSet(f"=={pin}")
        if str(requirement) not in result:
            result.append(str(requirement))
    return result


async def _plan(roots: dict, pins: dict, preferences: dict) -> dict:
    """按选定版本解析顶层依赖，子依赖由解析器补齐，核心依赖保持当前版本

    参数:
        roots: 参与解析的顶层依赖
        pins: 键 -> 选定版本
        preferences: 子依赖优先选用的备份时版本

    返回:
        dict: 依赖计划，requirements 为参与解析的依赖声明
    """
    from copy import deepcopy

    from zhenxun.services.nonebot_store.manual import (
        normalize_requirements,
        preview_manual_dependencies,
    )
    from zhenxun.services.nonebot_store.storage import load_manifest

    plugins, values, manual = {}, [], {}
    for key, root in roots.items():
        values.extend(_root_requirements(root, pins[key]))
        plugin = root["plugin"]
        if plugin is None:
            if root["kind"] != "store":
                manual.update(
                    normalize_requirements(
                        root["requirements"] or [root["name"]], "install"
                    )
                )
        elif pins[key] == plugin["version"]:
            plugins[key] = plugin
        else:
            plugins[key] = {
                **{k: v for k, v in plugin.items() if k != "resolution_inputs"},
                "version": pins[key],
            }
    base = deepcopy(load_manifest())
    base.update(
        plugins=plugins, packages={}, manual_requirements={}, plugin_requirements={}
    )
    plan = await preview_manual_dependencies(
        values, "install", manifest=base, restore=True, preferences=preferences
    )
    from zhenxun.services.nonebot_store.dependencies import DependencyAnalysisError
    from zhenxun.services.nonebot_store.environment import capture_environment
    from zhenxun.services.nonebot_store.policy import ORM_PACKAGES

    current = capture_environment().versions
    changes = [
        f"{name} 已安装 {current[name]}，备份需要 {item['version']}，只能在控制台变更"
        for name, item in plan["target_manifest"]["packages"].items()
        if name in ORM_PACKAGES
        and current.get(name)
        and current[name] != str(item["version"])
    ]
    if changes:
        raise DependencyAnalysisError(
            "orm_dependency_console_required", details=changes
        )
    plan["target_manifest"]["manual_requirements"] = manual
    for plugin in plugins.values():
        name = canonicalize_name(plugin["project_link"])
        plan["target_manifest"]["packages"][name] = {
            **plan["target_manifest"]["packages"].get(name, {}),
            "version": str(plugin["version"]),
        }
    if plugins:
        plan["no_change"] = False
    plan["requirements"] = values
    return plan


async def _nearby(root: dict) -> list[str]:
    """包索引中与备份时版本相近的版本

    先取次版本号相同的版本，再取主版本号相同的版本，各自按与备份时版本的距离
    排列，距离相同时较新的在前。备份时不是预发布版本的不取预发布版本。

    参数:
        root: 顶层依赖

    返回:
        list[str]: 依次尝试的版本
    """
    from bisect import bisect_left, bisect_right

    import httpx

    from zhenxun.services.nonebot_store.source_metadata import index_versions

    try:
        current = Version(root["version"])
        versions = await index_versions(root["name"])
    except (InvalidVersion, httpx.HTTPError, OSError, ValueError):
        return []
    left, right = bisect_left(versions, current), bisect_right(versions, current)
    ranked = []
    for index, version in enumerate(versions):
        if (
            version == current
            or version.major != current.major
            or (version.is_prerelease and not current.is_prerelease)
        ):
            continue
        declarations = [
            *root["requirements"],
            *((root.get("plugin") or {}).get("resolution_inputs") or []),
        ]
        if any(
            canonicalize_name(Requirement(raw).name) == root["name"]
            and (
                Requirement(raw).url
                or (
                    Requirement(raw).specifier
                    and version not in Requirement(raw).specifier
                )
            )
            for raw in declarations
        ):
            continue
        newer = version > current
        steps = index - right + 1 if newer else left - index
        minor = version.release[:2] != current.release[:2]
        ranked.append(((minor, steps, not newer), str(version)))
    return [version for _, version in sorted(ranked)][:_NEARBY_LIMIT]


def _applies(raw: str) -> bool:
    """依赖声明的平台条件是否适用于当前环境

    参数:
        raw: 依赖声明

    返回:
        bool: 没有平台条件或条件成立时为 True
    """
    marker = Requirement(raw).marker
    return marker is None or marker.evaluate({"extra": ""})


def _reason(error: Exception) -> str:
    """依赖错误的简短说明，当前平台或 Python 版本没有可用安装包时直接指出

    参数:
        error: 依赖错误

    返回:
        str: 说明
    """
    details = getattr(error, "details", None)
    match = _UNAVAILABLE.search(f"{error} {details or ''}")
    if match:
        name = canonicalize_name(match.group(1))
        if match.group(2) == "platform":
            current = f"{sys.platform}-{platform.machine()}".lower()
            return f"当前平台（{current}）没有 {name} 可用的安装包"
        python = f"{sys.version_info.major}.{sys.version_info.minor}"
        return f"当前 Python {python} 没有 {name} 可用的安装包"
    detail = describe(details) or str(error).split("╰─▶", 1)[-1].strip()
    return detail if len(detail) <= 300 else detail[:300] + "…"


def _fallback_or_raise(error) -> None:
    """仅对已确认的包解析冲突、声明与备份版本不一致和已安装 ORM 包的版本变更
    尝试降级方案，环境与网络故障保持失败"""
    if error.code not in {
        "dependency_resolution_failed",
        "registry_version_not_on_pypi",
        "orm_dependency_console_required",
        "dependency_explicit_version_conflict",
    }:
        raise error


async def _choose(
    root: dict, key: str, preferences: dict
) -> tuple[str | None, str, dict]:
    """单独解析一个顶层依赖：先试备份时的版本，再依次试相近版本，商店插件以外的
    依赖最后放开版本由解析器选择

    参数:
        root: 顶层依赖
        key: 顶层依赖的键
        preferences: 子依赖优先选用的备份时版本

    返回:
        tuple[str | None, str, dict]: 选定版本（None 为不固定版本）、失败原因与
            解析出的包版本，没有可用版本时原因不为空
    """
    from zhenxun.services.nonebot_store.dependencies import DependencyAnalysisError

    reason, tried = "", []
    candidates = [root["version"]]
    while candidates:
        pin = candidates.pop(0)
        if pin in tried:
            continue
        tried.append(pin)
        try:
            plan = await _plan({key: root}, {key: pin}, preferences)
            return pin, "", plan["target_manifest"]["packages"]
        except DependencyAnalysisError as error:
            _fallback_or_raise(error)
            reason = reason or _reason(error)
        if pin is not None and pin == root["version"]:
            unpinned = [None] if root["plugin"] is None else []
            candidates = [*await _nearby(root), *unpinned]
    return None, reason or "包索引中没有相近的版本", {}


async def _combine(
    roots: dict,
    pins: dict,
    preferences: dict,
    skip: Callable[[str, str], None],
    singles: dict[str, dict] | None = None,
) -> dict | None:
    """解析全部顶层依赖；整体失败时逐个加入，与已加入依赖冲突的项先放开版本
    （商店插件改试相近版本），仍冲突的跳过

    按来源（见 KINDS）先后加入，同一来源中单独解析时与其他依赖版本分歧少的先加入。

    参数:
        roots: 顶层依赖，被跳过的项会从中移除
        pins: 键 -> 选定版本，放开或改用相近版本时随之更新
        preferences: 子依赖优先选用的备份时版本
        skip: 记录跳过的顶层依赖
        singles: 键 -> 单独解析时的包版本

    返回:
        dict | None: 依赖计划，没有可安装的依赖时为 None
    """
    from zhenxun.services.nonebot_store.dependencies import DependencyAnalysisError

    try:
        return await _plan(roots, pins, preferences)
    except DependencyAnalysisError as error:
        _fallback_or_raise(error)
    singles = singles or {}

    def disagreements(key: str) -> int:
        packages = singles.get(key) or {}
        return sum(
            any(
                name in items and items[name]["version"] != value["version"]
                for name, value in packages.items()
            )
            for other, items in singles.items()
            if other != key and other in roots
        )

    order = sorted(
        roots,
        key=lambda key: (KINDS.index(roots[key]["kind"]), disagreements(key), key),
    )
    plan, accepted = None, {}
    for key in order:
        root, reason, tried = roots[key], "", []
        candidates = [pins[key]]
        while candidates:
            pin = candidates.pop(0)
            if pin in tried:
                continue
            tried.append(pin)
            try:
                plan = await _plan(
                    {**accepted, key: root}, {**pins, key: pin}, preferences
                )
            except DependencyAnalysisError as error:
                _fallback_or_raise(error)
                reason = reason or _reason(error)
                if len(tried) == 1:
                    candidates = (
                        [None] if root["plugin"] is None else await _nearby(root)
                    )
                continue
            accepted[key], pins[key] = root, pin
            break
        else:
            skip(key, f"与其他依赖冲突：{reason}")
            roots.pop(key)
    return plan


def _declared_files(files: Path | None, packages: dict) -> dict[str, list[str]]:
    """恢复的插件代码中可以登记为插件依赖的 requirements 文件

    文件中的依赖声明都有效，且适用于当前环境的依赖都能由恢复后的环境满足时登记。

    参数:
        files: 备份包解压后的 files 目录，未恢复插件代码时为 None
        packages: 恢复后依赖层中的包

    返回:
        dict[str, list[str]]: 文件路径 -> 依赖声明
    """
    from zhenxun.services.nonebot_store.dependencies import (
        DependencyAnalysisError,
        parse_requirement,
    )
    from zhenxun.services.nonebot_store.environment import base_distributions

    if files is None:
        return {}
    versions = {name: dist.version for name, dist in base_distributions()[0].items()}
    versions.update({name: str(item["version"]) for name, item in packages.items()})
    result: dict[str, list[str]] = {}
    for path, lines in plugin_requirement_files(files).items():
        try:
            requirements = [parse_requirement(line) for line in lines]
        except DependencyAnalysisError:
            continue
        if requirements and all(
            not _applies(str(item))
            or (
                item.name in versions
                and item.specifier.contains(versions[item.name], prereleases=True)
            )
            for item in requirements
        ):
            result[path] = [str(item) for item in requirements]
    return result


def _saved_dependents(saved: dict, roots: dict) -> tuple[dict[str, set[str]], str]:
    """按目标平台读取包中静态依赖关系，缺失元数据时保留未知状态。"""
    metadata = saved.get("distributions")
    if not isinstance(metadata, dict):
        return {}, "unavailable"
    graph: dict[str, set[str]] = {}
    active: dict[str, set[str]] = {}
    complete = True
    pending = [
        (None, raw, frozenset({""}))
        for root in roots.values()
        for raw in _root_requirements(root, root["version"] if root["plugin"] else None)
    ]
    while pending:
        owner, raw, contexts = pending.pop()
        try:
            requirement = Requirement(raw)
            if requirement.marker and not any(
                requirement.marker.evaluate({"extra": extra}) for extra in contexts
            ):
                continue
        except (InvalidRequirement, ValueError, TypeError):
            complete = False
            continue
        name = canonicalize_name(requirement.name)
        if owner and owner != name:
            graph.setdefault(name, set()).add(owner)
        wanted = {""} | {canonicalize_name(extra) for extra in requirement.extras}
        new = wanted - active.setdefault(name, set())
        if not new:
            continue
        active[name].update(new)
        item = metadata.get(name)
        if not isinstance(item, dict) or not isinstance(
            item.get("requires_dist"), list
        ):
            complete = False
            continue
        complete = complete and item.get("metadata_complete") is True
        pending.extend((name, child, frozenset(new)) for child in item["requires_dist"])
    return graph, "complete" if complete else "partial"


def _implied(roots: dict, graph: dict[str, set[str]]) -> dict[str, str]:
    """被其他顶层依赖间接需要的顶层依赖

    依赖关系成环的顶层依赖互不视为被需要。

    参数:
        roots: 顶层依赖
        graph: 包名 -> 直接依赖它的包名

    返回:
        dict[str, str]: 键 -> 需要它的最上层顶层依赖的键
    """
    keys = {root["name"]: key for key, root in roots.items()}
    above: dict[str, set[str]] = {}
    for key, root in roots.items():
        seen: set[str] = set()
        queue = list(graph.get(root["name"], ()))
        while queue:
            name = queue.pop()
            if name not in seen:
                seen.add(name)
                queue.extend(graph.get(name, ()))
        above[key] = {keys[name] for name in seen if name in keys and keys[name] != key}
    owners = {
        key: sorted(item for item in above[key] if key not in above[item])
        for key in roots
    }
    return {
        key: next((item for item in items if not owners[item]), items[0])
        for key, items in owners.items()
        if items
    }


async def stage_restore(
    saved: dict,
    skip_failed: bool = False,
    files: Path | None = None,
    *,
    parent_operation_id: str | None = None,
) -> dict:
    """把备份中的依赖解析为当前环境可用的版本，写入待应用的依赖层事务

    平台条件由目标环境判断，依赖关系仅从包内静态元数据读取。元数据缺失时保留
    未确认状态，目标解析仍继续。显式声明保持约束，历史版本作为偏好；可选组件
    失败时按 skip_failed 处理，网络及工具故障保持失败。核心和 ORM 依赖保持目标
    环境约束，源码包通过候选环境的隔离构建处理。

    参数:
        saved: 备份中的 dependencies.json
        skip_failed: 是否跳过无法安装的插件与声明的依赖，虚拟环境包与依赖层包
            总是跳过
        files: 备份包解压后的 files 目录，恢复插件代码时传入

    返回:
        dict: {staged, revision, restored, implied, nearby, platform_only,
            skipped_plugins, skipped_requirements, skip_reasons,
            unsupported_requirements}，implied 为作为子依赖处理的顶层依赖 -> 需要
            它的顶层依赖，nearby 为改用其他版本的顶层依赖 -> [备份时版本, 安装版本]，
            platform_only 为只适用于其他平台的依赖 -> 平台条件
    """
    from zhenxun.services.nonebot_store.dependencies import (
        FORBIDDEN_LAYER_PACKAGES,
        DependencyAnalysisError,
        prebuild_packages,
    )
    from zhenxun.services.nonebot_store.manual import stage_plan
    from zhenxun.services.nonebot_store.storage import pending_transaction

    roots = _roots(saved)
    roots.update(_implicit_roots(saved, files, roots))
    tools = {
        key
        for key, root in roots.items()
        if root["name"] in FORBIDDEN_LAYER_PACKAGES or root.get("unsupported")
    }
    report = {
        "staged": False,
        "restored": [],
        "implied": {},
        "nearby": {},
        "platform_only": {},
        "skipped_plugins": [],
        "skipped_requirements": [],
        "skip_reasons": {},
        "unsupported_requirements": sorted(
            tools | set(saved.get("unsupported_requirements") or {})
        ),
    }
    for key in tools:
        roots.pop(key)
    for name in sorted(_foreign_core(saved)):
        report["platform_only"][name] = "本体依赖，不适用于当前平台"
    for key, root in list(roots.items()):
        applicable = [raw for raw in root["requirements"] if _applies(raw)]
        if root["requirements"] and not applicable:
            markers = {str(Requirement(raw).marker) for raw in root["requirements"]}
            report["platform_only"][key] = "；".join(sorted(markers))
            roots.pop(key)
        else:
            root["requirements"] = applicable

    def skip(key: str, reason: str) -> None:
        root = roots.get(key, {})
        url = root.get("url")
        if not skip_failed and root.get("kind") not in _IMPLIED_KINDS:
            raise DependencyAnalysisError(
                "dependency_restore_failed", details=[f"{key}：{reason}"]
            )
        kind = "skipped_plugins" if key in saved["plugins"] else "skipped_requirements"
        report[kind].append(key)
        report["skip_reasons"][key] = reason + (f"；原安装地址 {url}" if url else "")

    for key, root in list(roots.items()):
        if root.get("url") and not root["requirements"]:
            skip(
                key,
                "原依赖来自直链或本地路径，需在目标环境提供可信来源，未改为同名索引包",
            )
            roots.pop(key)
    preferences = {
        **{name: value["version"] for name, value in saved["packages"].items()},
        **(saved.get("installed") or {}),
    }
    graph, report["metadata_state"] = _saved_dependents(saved, roots)
    if len(roots) > 1:
        for key, owner in _implied(roots, graph).items():
            if roots[key]["kind"] in _IMPLIED_KINDS:
                report["implied"][key] = owner
                roots.pop(key)
            elif roots[key]["plugin"] is None:
                roots[key]["version"] = None
    pins = {key: root["version"] for key, root in roots.items()}
    plan, singles = None, {}
    if roots:
        try:
            plan = await _plan(roots, pins, preferences)
        except DependencyAnalysisError as error:
            _fallback_or_raise(error)
            for key, root in list(roots.items()):
                pins[key], reason, singles[key] = await _choose(root, key, preferences)
                if reason:
                    skip(key, reason)
                    roots.pop(key)
            plan = (
                await _combine(roots, pins, preferences, skip, singles)
                if roots
                else None
            )
    while plan and plan["source_build_required"]:
        failed = await prebuild_packages(
            {
                name: str(value["version"])
                for name, value in plan["target_manifest"]["packages"].items()
            }
        )
        if not failed:
            break
        owners = {}
        for key, root in list(roots.items()):
            single = await _plan({key: root}, pins, preferences)
            names = sorted(set(failed) & set(single["target_manifest"]["packages"]))
            if names:
                owners[key] = names
        if not owners:
            break
        for key, names in owners.items():
            skip(key, f"源码构建失败：{'、'.join(names)}（{failed[names[0]][-300:]}）")
            roots.pop(key)
        plan = (
            await _combine(roots, pins, preferences, skip, singles) if roots else None
        )
    report["restored"] = sorted(roots)
    report["skipped_plugins"].sort()
    report["skipped_requirements"].sort()
    report["implied"] = {
        key: owner for key, owner in report["implied"].items() if owner in roots
    }
    packages = plan["target_manifest"]["packages"] if plan else {}
    report["nearby"] = {
        key: [
            root["version"],
            pins[key]
            or (packages.get(root["name"]) or {}).get("version")
            or "当前环境中的版本",
        ]
        for key, root in roots.items()
        if root["version"] and pins[key] != root["version"]
    }
    if plan:
        target = plan["target_manifest"]
        target["plugin_requirements"] = _declared_files(files, packages)
        declared = {
            canonicalize_name(Requirement(raw).name)
            for values in target["plugin_requirements"].values()
            for raw in values
        }
        for root in roots.values():
            if root["kind"] == "plugin" and root["name"] in declared:
                target["manual_requirements"].pop(root["name"], None)
        report["staged"] = stage_plan(
            plan,
            plan["requirements"],
            restore=True,
            parent_operation_id=parent_operation_id,
        )
    if report["staged"]:
        report["revision"] = (pending_transaction() or {}).get("revision")
    return report
