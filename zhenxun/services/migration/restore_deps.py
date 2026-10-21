"""恢复备份时的依赖解析：把备份中的依赖解析为当前环境可用的版本"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from pathlib import Path
import platform
import re
import sys

from packaging.requirements import Requirement
from packaging.specifiers import SpecifierSet
from packaging.utils import canonicalize_name
from packaging.version import InvalidVersion, Version

from .deps import FORMAT, KINDS, _parse, _plugin_requirements, _version

_NEARBY_LIMIT = 5
_PLATFORMS = {
    "linux-x86_64": "x86_64-unknown-linux-gnu",
    "linux-amd64": "x86_64-unknown-linux-gnu",
    "linux-aarch64": "aarch64-unknown-linux-gnu",
    "linux-arm64": "aarch64-unknown-linux-gnu",
    "win32-amd64": "x86_64-pc-windows-msvc",
    "win32-x86_64": "x86_64-pc-windows-msvc",
    "win32-arm64": "aarch64-pc-windows-msvc",
    "darwin-arm64": "aarch64-apple-darwin",
    "darwin-x86_64": "x86_64-apple-darwin",
}
"""备份记录的平台 -> uv 解析使用的目标平台"""
_IMPLIED_KINDS = {"venv", "layer"}
_DEV_TOOLS = {
    "black",
    "coverage",
    "flake8",
    "isort",
    "mypy",
    "nonebug",
    "nox",
    "pre-commit",
    "pylint",
    "pyright",
    "pytest",
    "respx",
    "ruff",
    "tox",
}
"""开发与测试工具，没有插件声明时恢复不安装"""
_UNAVAILABLE = re.compile(
    r"([A-Za-z0-9][A-Za-z0-9._-]*)(?:==\S+)?\s+ha(?:s|ve)\s+no\s+wheels\s+with\s+"
    r"a\s+matching\s+(platform|Python)"
)


def _legacy_extras(saved: dict, files: Path) -> dict[str, dict]:
    """旧格式备份没有记录的真寻商店插件依赖与插件目录声明的依赖

    参数:
        saved: 备份中的 dependencies.json
        files: 备份包解压后的 files 目录

    返回:
        dict[str, dict]: 包名 -> {kind, requirements, version}
    """
    from zhenxun.services.nonebot_store.dependencies import project_closure

    skip = (
        set(project_closure())
        | set(saved["extra_requirements"])
        | {canonicalize_name(name) for name in saved["manual_requirements"]}
        | {
            canonicalize_name(str(item.get("project_link") or ""))
            for item in saved["plugins"].values()
        }
    )
    result: dict[str, dict] = {}

    def add(name: str, kind: str, requirements: list[str]) -> None:
        if name in skip:
            return
        item = result.setdefault(
            name,
            {
                "kind": kind,
                "requirements": [],
                "version": _version(saved["packages"].get(name)),
            },
        )
        for raw in requirements:
            if raw not in item["requirements"]:
                item["requirements"].append(raw)

    for receipt in (saved.get("store_receipts") or {}).values():
        if not isinstance(receipt, dict):
            continue
        for raw in receipt.get("dependency_inputs") or []:
            requirement = _parse(str(raw))
            if requirement:
                add(canonicalize_name(requirement.name), "store", [str(requirement)])
    for name, item in _plugin_requirements(files).items():
        add(name, "plugin", item["requirements"])
    return result


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


def _roots(saved: dict, files: Path | None) -> dict[str, dict]:
    """备份中需要恢复的顶层依赖：商店插件、手动依赖与其他依赖

    旧格式备份会从 files 中的商店回执与插件目录补齐依赖。随本体安装的包不按备份
    版本固定，由解析器优先沿用当前环境中的版本。

    参数:
        saved: 备份中的 dependencies.json
        files: 备份包解压后的 files 目录

    返回:
        dict[str, dict]: 键 -> {kind, name, requirements, version, url, plugin}，
            商店插件以插件键为键，其余以包名为键
    """
    from zhenxun.services.nonebot_store.dependencies import project_closure

    shared = set(project_closure())
    extras = dict(saved["extra_requirements"])
    if saved.get("format", 1) < FORMAT and files is not None:
        extras.update(_legacy_extras(saved, files))
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
                "version": _version(saved["packages"].get(canonicalize_name(name))),
            },
        )
        for name, values in saved["manual_requirements"].items()
    ]
    for name, item in [*items, *extras.items()]:
        roots[name] = {
            "kind": item.get("kind") or "venv",
            "name": name,
            "requirements": list(item.get("requirements") or []),
            "version": None if name in shared else item.get("version"),
            "url": item.get("url"),
            "plugin": None,
        }
    return roots


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
        if pin == plugin["version"] and plugin.get("resolution_inputs"):
            return list(plugin["resolution_inputs"])
        return [f"{plugin['project_link']}=={pin}"]
    declared = root["requirements"] or [root["name"]]
    if pin is None:
        return list(declared)
    result = []
    for raw in declared:
        requirement = Requirement(raw)
        if canonicalize_name(requirement.name) == root["name"]:
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
    plan["target_manifest"]["manual_requirements"] = manual
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
    except DependencyAnalysisError:
        pass
    singles = singles or {}

    def disagreements(key: str) -> int:
        packages = singles.get(key) or {}
        return sum(
            any(
                name in items and _version(items[name]) != _version(value)
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


def _source_target(saved: dict) -> tuple[str, str] | None:
    """备份时的平台与 Python 版本

    参数:
        saved: 备份中的 dependencies.json

    返回:
        tuple[str, str] | None: uv 目标平台与 Python 版本，与当前环境相同或无法
            识别时为 None
    """
    current = f"{sys.platform}-{platform.machine()}".lower()
    source = str(saved.get("platform") or current)
    python = ".".join(str(saved.get("python") or "").split(".")[:2])
    running = f"{sys.version_info.major}.{sys.version_info.minor}"
    if source == current and python in {"", running}:
        return None
    target = _PLATFORMS.get(source)
    return (target, python or running) if target else None


async def _dependents(
    roots: dict, preferences: dict, target: tuple[str, str] | None
) -> dict[str, set[str]]:
    """按备份时的平台解析顶层依赖，得到每个包被哪些包直接依赖

    先整体解析，失败时逐个解析顶层依赖再合并，仍无法解析的顶层依赖不计入。

    参数:
        roots: 顶层依赖
        preferences: 优先选用的备份时版本
        target: 备份时的平台与 Python 版本，为 None 时按当前环境解析

    返回:
        dict[str, set[str]]: 包名 -> 直接依赖它的包名
    """
    from zhenxun.services.nonebot_store.dependencies import (
        DependencyAnalysisError,
        dependency_graph,
    )

    values = [
        _root_requirements(root, root["version"] if root["plugin"] else None)
        for root in roots.values()
    ]
    try:
        return await dependency_graph(
            [raw for items in values for raw in items], preferences, target
        )
    except DependencyAnalysisError:
        pass
    semaphore = asyncio.Semaphore(4)

    async def single(requirements: list[str]) -> dict[str, set[str]]:
        async with semaphore:
            try:
                return await dependency_graph(requirements, preferences, target)
            except DependencyAnalysisError:
                return {}

    graph: dict[str, set[str]] = {}
    for part in await asyncio.gather(*(single(items) for items in values)):
        for name, owners in part.items():
            graph.setdefault(name, set()).update(owners)
    return graph


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
    saved: dict, skip_failed: bool = False, files: Path | None = None
) -> dict:
    """把备份中的依赖解析为当前环境可用的版本，写入待应用的依赖层事务

    平台条件不适用于当前环境的依赖声明不参与解析，全部声明都不适用的列入
    platform_only；虚拟环境与依赖层中的开发、测试工具列入 dev_tools，不安装。
    再按备份时的平台与 Python 版本解析全部顶层依赖（商店插件、手动依赖、插件声明
    与虚拟环境中的包），被其他顶层依赖间接需要的虚拟环境包与依赖层包只作为子依赖，
    由解析器按当前平台决定是否安装；被间接需要的手动依赖与插件声明不再固定备份时
    的版本。其余顶层依赖优先使用备份时的版本；整体解析失败时逐个检查，备份时的
    版本不可用的依次改用次版本号相同、主版本号相同的相近版本，商店插件以外的依赖
    最后放开版本。之后按来源与版本分歧逐个合并，冲突的依赖放开版本或改用相近版本，
    仍不可用的按 skip_failed 决定中止或跳过。子依赖由解析器补齐并优先沿用备份时
    的版本，允许构建源码包，构建失败的包所属的顶层依赖同样跳过。核心依赖保持当前
    环境的版本。pip、uv 等安装工具只能在控制台安装，列入 unsupported_requirements。

    参数:
        saved: 备份中的 dependencies.json
        skip_failed: 是否跳过无法安装的插件与依赖
        files: 备份包解压后的 files 目录，恢复插件代码时传入

    返回:
        dict: {staged, revision, restored, implied, nearby, platform_only, dev_tools,
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

    roots = _roots(saved, files)
    tools = {
        key for key, root in roots.items() if root["name"] in FORBIDDEN_LAYER_PACKAGES
    }
    report = {
        "staged": False,
        "restored": [],
        "implied": {},
        "nearby": {},
        "platform_only": {},
        "dev_tools": [],
        "skipped_plugins": [],
        "skipped_requirements": [],
        "skip_reasons": {},
        "unsupported_requirements": sorted(
            tools | set(saved.get("unsupported_requirements") or {})
        ),
    }
    for key in tools:
        roots.pop(key)
    for key, root in list(roots.items()):
        applicable = [raw for raw in root["requirements"] if _applies(raw)]
        if root["requirements"] and not applicable:
            markers = {str(Requirement(raw).marker) for raw in root["requirements"]}
            report["platform_only"][key] = "；".join(sorted(markers))
            roots.pop(key)
        elif root["kind"] in _IMPLIED_KINDS and (
            root["name"] in _DEV_TOOLS or root["name"].startswith("pytest-")
        ):
            report["dev_tools"].append(key)
            roots.pop(key)
        else:
            root["requirements"] = applicable
    report["dev_tools"].sort()

    def skip(key: str, reason: str) -> None:
        url = roots.get(key, {}).get("url")
        if not skip_failed:
            raise DependencyAnalysisError(
                "dependency_restore_failed", details=[f"{key}：{reason}"]
            )
        kind = "skipped_plugins" if key in saved["plugins"] else "skipped_requirements"
        report[kind].append(key)
        report["skip_reasons"][key] = reason + (f"；原安装地址 {url}" if url else "")

    preferences = {
        **{name: _version(value) for name, value in saved["packages"].items()},
        **(saved.get("installed") or {}),
    }
    if len(roots) > 1:
        graph = await _dependents(roots, preferences, _source_target(saved))
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
        except DependencyAnalysisError:
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
                name: str(_version(value))
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
    packages = plan["target_manifest"]["packages"] if plan else {}
    report["nearby"] = {
        key: [
            root["version"],
            pins[key] or _version(packages.get(root["name"])) or "当前环境中的版本",
        ]
        for key, root in roots.items()
        if root["version"] and pins[key] != root["version"]
    }
    if plan:
        report["staged"] = stage_plan(plan, plan["requirements"], restore=True)
    if report["staged"]:
        report["revision"] = (pending_transaction() or {}).get("revision")
    return report
