"""备份中的依赖清单：当前环境中插件使用的全部依赖"""

from __future__ import annotations

from collections.abc import Callable
import hashlib
import importlib.metadata
import json
from pathlib import Path
import platform
import sys

from packaging.requirements import InvalidRequirement, Requirement
from packaging.specifiers import SpecifierSet
from packaging.utils import canonicalize_name
from packaging.version import InvalidVersion, Version

try:
    import tomllib
except ModuleNotFoundError:  # pragma: no cover - Python 3.10
    import tomli as tomllib

FORMAT = 2
PLUGIN_DIRS = ("zhenxun/plugins", "plugins")
RECEIPTS_FILE = Path("data/runtime/plugin-store-receipts-v1.json")
_LOCAL_PLUGIN_FIELDS = ("environment_fingerprint", "installed_at")
KINDS = ("nonebot_plugin", "manual", "store", "plugin", "venv", "layer")
"""依赖来源：NoneBot 商店插件、手动依赖、真寻商店插件、插件目录声明、核心虚拟环境、
依赖层中没有归属的包"""
_EXTRA_KINDS = {"store", "plugin", "venv", "layer"}
_NEARBY_LIMIT = 5


def _parse(raw: str) -> Requirement | None:
    """解析依赖声明

    参数:
        raw: 依赖声明

    返回:
        Requirement | None: 依赖，无效或不适用于当前平台时为 None
    """
    from zhenxun.services.nonebot_store.dependencies import (
        DependencyAnalysisError,
        parse_requirement,
    )

    try:
        requirement = parse_requirement(raw)
    except DependencyAnalysisError:
        return None
    if requirement.marker and not requirement.marker.evaluate():
        return None
    return requirement


def _plugin_requirements(root: Path) -> dict[str, dict]:
    """插件目录中 requirement(s).txt 与 pyproject.toml 声明的依赖

    参数:
        root: 项目根目录

    返回:
        dict[str, dict]: 包名 -> {requirements, sources}
    """
    result: dict[str, dict] = {}

    def add(raw: str, source: str):
        requirement = _parse(raw)
        if requirement is None:
            return
        name = canonicalize_name(requirement.name)
        item = result.setdefault(name, {"requirements": [], "sources": []})
        if str(requirement) not in item["requirements"]:
            item["requirements"].append(str(requirement))
        if source not in item["sources"]:
            item["sources"].append(source)

    for base in PLUGIN_DIRS:
        directory = root / base
        if not directory.is_dir():
            continue
        for path in sorted(directory.glob("*/requirement*.txt")):
            source = path.relative_to(root).as_posix()
            for line in path.read_text("utf-8-sig", errors="replace").splitlines():
                line = line.split(" #", 1)[0].strip()
                if line and not line.startswith(("#", "-")):
                    add(line, source)
        for path in sorted(directory.glob("*/pyproject.toml")):
            try:
                data = tomllib.loads(path.read_text("utf-8"))
            except (OSError, ValueError):
                continue
            for raw in data.get("project", {}).get("dependencies", []):
                add(str(raw), path.relative_to(root).as_posix())
    return result


def _requires(name: str) -> set[str]:
    """已安装包的直接依赖，不含可选依赖

    参数:
        name: 包名

    返回:
        set[str]: 依赖包名，未安装时为空
    """
    try:
        requires = importlib.metadata.distribution(name).requires or []
    except importlib.metadata.PackageNotFoundError:
        return set()
    result = set()
    for raw in requires:
        try:
            requirement = Requirement(raw)
        except InvalidRequirement:
            continue
        if requirement.marker and not requirement.marker.evaluate({"extra": ""}):
            continue
        result.add(canonicalize_name(requirement.name))
    return result


def _requires_closure(names) -> set[str]:
    """包及其已安装依赖的传递闭包

    参数:
        names: 起始包名

    返回:
        set[str]: 闭包中的包名
    """
    seen: set[str] = set()
    queue = list(names)
    while queue:
        name = queue.pop()
        if name in seen:
            continue
        seen.add(name)
        queue.extend(_requires(name) - seen)
    return seen


def _version(value) -> str | None:
    """备份清单中记录的版本，兼容旧格式中直接记录版本字符串的写法

    参数:
        value: 清单中的包记录

    返回:
        str | None: 版本
    """
    return value.get("version") if isinstance(value, dict) else value


def _direct_url(name: str) -> str | None:
    """以直链（git、本地路径、压缩包地址）安装的包的原安装地址

    参数:
        name: 包名

    返回:
        str | None: 安装地址，从包索引安装时为 None
    """
    try:
        data = json.loads(
            importlib.metadata.distribution(name).read_text("direct_url.json") or "null"
        )
    except (importlib.metadata.PackageNotFoundError, ValueError):
        return None
    if not isinstance(data, dict) or not data.get("url"):
        return None
    vcs = data.get("vcs_info") or {}
    if not vcs.get("vcs"):
        return data["url"]
    commit = f"@{vcs['commit_id']}" if vcs.get("commit_id") else ""
    return f"{vcs['vcs']}+{data['url']}{commit}"


def _venv_extras(known: set[str]) -> dict[str, str]:
    """核心虚拟环境中不随本体安装、不在依赖层、也不被其他已安装包依赖的包

    参数:
        known: 已纳管的包名

    返回:
        dict[str, str]: 包名 -> 已安装版本
    """
    from zhenxun.services.nonebot_store.dependencies import (
        PROJECT_NAME,
        base_installed_inventory,
        project_closure,
    )

    excluded = set(project_closure()) | known | {PROJECT_NAME}
    installed = base_installed_inventory()
    for name in installed:
        excluded |= _requires(name) - {name}
    return {
        name: version
        for name, version in sorted(installed.items())
        if name not in excluded
    }


def _unsupported(name: str) -> str | None:
    """依赖不能由恢复装入插件依赖层的原因

    pip、uv 等安装工具以及依赖它们的包（如 nb-cli）在依赖层中无法使用。

    参数:
        name: 包名

    返回:
        str | None: 原因，可以装入时为 None
    """
    from zhenxun.services.nonebot_store.dependencies import FORBIDDEN_LAYER_PACKAGES

    if name in FORBIDDEN_LAYER_PACKAGES:
        return "安装工具只能在控制台安装"
    tools = sorted(_requires(name) & FORBIDDEN_LAYER_PACKAGES)
    return f"依赖安装工具 {tools[0]}，只能在控制台安装" if tools else None


def inventory(root: Path | None = None) -> list[dict]:
    """当前环境中插件使用的全部依赖，导出备份时默认全部记录

    来源见 KINDS，同一个包出现在多个来源时归入靠前的来源。随本体安装的包不在
    其中。以直链安装的包带有原安装地址 url。不能由恢复安装的依赖带有 unsupported
    原因，备份只记录名称，恢复后提示手动安装。

    参数:
        root: 项目根目录

    返回:
        list[dict]: [{name, kind, requirements, version, sources, url, unsupported}]
    """
    from zhenxun.services.nonebot_store.dependencies import (
        _distribution_inventory,
        project_closure,
    )
    from zhenxun.services.nonebot_store.storage import load_manifest
    from zhenxun.services.plugin_store.plugin_store_receipts import (
        receipt_dependency_inputs,
    )

    root = root or Path.cwd()
    manifest = load_manifest()
    core = set(project_closure())
    installed = _distribution_inventory(include_layer=True)
    items: dict[str, dict] = {}

    def add(name: str, kind: str, requirements: list[str], source: str) -> None:
        item = items.setdefault(
            name,
            {
                "name": name,
                "kind": kind,
                "requirements": [],
                "version": installed.get(name),
                "sources": [],
                "url": _direct_url(name) if kind == "venv" else None,
            },
        )
        for raw in requirements:
            if raw not in item["requirements"]:
                item["requirements"].append(raw)
        if source not in item["sources"]:
            item["sources"].append(source)

    for key, plugin in manifest["plugins"].items():
        if plugin.get("state") == "managed" and plugin.get("project_link"):
            add(canonicalize_name(plugin["project_link"]), "nonebot_plugin", [], key)
    for name, values in manifest["manual_requirements"].items():
        add(canonicalize_name(name), "manual", list(values), "manual")
    for key, values in receipt_dependency_inputs().items():
        for raw in values:
            requirement = _parse(raw)
            name = canonicalize_name(requirement.name) if requirement else ""
            if requirement and name not in core:
                add(name, "store", [str(requirement)], key)
    for name, item in _plugin_requirements(root).items():
        if name not in core:
            for source in item["sources"]:
                add(name, "plugin", item["requirements"], source)
    packages = set(manifest["packages"])
    for name in _venv_extras(core | packages | _requires_closure(items)):
        add(name, "venv", [], ".venv")
    for name in sorted(packages - core - _requires_closure(items)):
        add(name, "layer", [], "layer")
    for item in items.values():
        item["unsupported"] = (
            _unsupported(item["name"]) if item["kind"] in _EXTRA_KINDS else None
        )
    return sorted(
        items.values(), key=lambda item: (KINDS.index(item["kind"]), item["name"])
    )


def snapshot(items: list[dict] | None = None) -> dict:
    """生成备份中的 dependencies.json

    参数:
        items: 要记录的依赖（inventory 的返回项），为 None 时记录全部依赖

    返回:
        dict: 依赖清单
    """
    from zhenxun.services.nonebot_store.dependencies import (
        LOCK_FILE,
        _distribution_inventory,
    )
    from zhenxun.services.nonebot_store.storage import load_manifest, read_json

    items = inventory() if items is None else items
    selected = {item["name"] for item in items}
    manifest = load_manifest()
    version = Path("__version__")
    return {
        "format": FORMAT,
        "bot_version": version.read_text("utf-8").split(":")[-1].strip()
        if version.is_file()
        else None,
        "python": platform.python_version(),
        "platform": f"{sys.platform}-{platform.machine()}".lower(),
        "lock_sha256": hashlib.sha256(LOCK_FILE.read_bytes()).hexdigest()
        if LOCK_FILE.is_file()
        else None,
        "plugins": {
            key: {k: v for k, v in item.items() if k not in _LOCAL_PLUGIN_FIELDS}
            for key, item in manifest["plugins"].items()
            if item.get("state") == "managed"
            and canonicalize_name(str(item.get("project_link") or "")) in selected
        },
        "packages": manifest["packages"],
        "installed": _distribution_inventory(include_layer=True),
        "manual_requirements": {
            name: values
            for name, values in manifest["manual_requirements"].items()
            if canonicalize_name(name) in selected
        },
        "extra_requirements": {
            item["name"]: {
                "kind": item["kind"],
                "requirements": item["requirements"],
                "version": item["version"],
                **({"url": item["url"]} if item.get("url") else {}),
            }
            for item in items
            if item["kind"] in _EXTRA_KINDS and not item["unsupported"]
        },
        "unsupported_requirements": {
            item["name"]: item["unsupported"] for item in items if item["unsupported"]
        },
        "store_receipts": read_json(RECEIPTS_FILE, {}),
    }


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
            失败时带 error_code 与 detail
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
        return {
            "state": "failed",
            "error_code": transaction.get("error_code")
            or "nonebot_plugin_apply_failed",
            "detail": describe(transaction.get("error_details"))
            or transaction.get("error")
            or "",
        }
    return {"state": "done"}


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
    base.update(plugins=plugins, packages={}, manual_requirements={})
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


def _reason(error: Exception) -> str:
    """依赖错误的简短说明

    参数:
        error: 依赖错误

    返回:
        str: 说明
    """
    detail = describe(getattr(error, "details", None)) or str(error)
    return detail if len(detail) <= 300 else detail[:300] + "…"


async def _choose(root: dict, key: str, preferences: dict) -> tuple[str | None, str]:
    """单独解析一个顶层依赖：先试备份时的版本，再依次试相近版本

    参数:
        root: 顶层依赖
        key: 顶层依赖的键
        preferences: 子依赖优先选用的备份时版本

    返回:
        tuple[str | None, str]: 选定版本与失败原因，没有可用版本时原因不为空
    """
    from zhenxun.services.nonebot_store.dependencies import DependencyAnalysisError

    reason = ""
    candidates = [root["version"]]
    while candidates:
        pin = candidates.pop(0)
        try:
            await _plan({key: root}, {key: pin}, preferences)
            return pin, ""
        except DependencyAnalysisError as error:
            reason = reason or _reason(error)
        if pin is not None and pin == root["version"]:
            candidates = await _nearby(root)
    return None, reason or "包索引中没有相近的版本"


async def _combine(
    roots: dict, pins: dict, preferences: dict, skip: Callable[[str, str], None]
) -> dict | None:
    """解析全部顶层依赖；整体失败时逐个加入，跳过与已加入依赖冲突的项

    参数:
        roots: 顶层依赖，被跳过的项会从中移除
        pins: 键 -> 选定版本
        preferences: 子依赖优先选用的备份时版本
        skip: 记录跳过的顶层依赖

    返回:
        dict | None: 依赖计划，没有可安装的依赖时为 None
    """
    from zhenxun.services.nonebot_store.dependencies import DependencyAnalysisError

    try:
        return await _plan(roots, pins, preferences)
    except DependencyAnalysisError:
        pass
    plan, accepted = None, {}
    for key, root in list(roots.items()):
        try:
            plan = await _plan({**accepted, key: root}, pins, preferences)
            accepted[key] = root
        except DependencyAnalysisError as error:
            skip(key, f"与其他依赖冲突：{_reason(error)}")
            roots.pop(key)
    return plan


async def stage_restore(
    saved: dict, skip_failed: bool = False, files: Path | None = None
) -> dict:
    """把备份中的依赖解析为当前环境可用的版本，写入待应用的依赖层事务

    每个顶层依赖（商店插件、手动依赖、插件声明与虚拟环境中的包）优先使用备份时
    的版本；整体解析失败时逐个检查，备份时的版本不可用的依次改用次版本号相同、
    主版本号相同的相近版本，仍不可用的按 skip_failed 决定中止或跳过。子依赖由
    解析器补齐并优先沿用备份时的版本，允许构建源码包，构建失败的包所属的顶层依赖
    同样跳过。核心依赖保持当前环境的版本。pip、uv 等安装工具只能在控制台安装，
    列入 unsupported_requirements。

    参数:
        saved: 备份中的 dependencies.json
        skip_failed: 是否跳过无法安装的插件与依赖
        files: 备份包解压后的 files 目录，恢复插件代码时传入

    返回:
        dict: {staged, revision, restored, nearby, skipped_plugins,
            skipped_requirements, skip_reasons, unsupported_requirements}
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
        "nearby": {},
        "skipped_plugins": [],
        "skipped_requirements": [],
        "skip_reasons": {},
        "unsupported_requirements": sorted(
            tools | set(saved.get("unsupported_requirements") or {})
        ),
    }
    for key in tools:
        roots.pop(key)

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
    pins = {key: root["version"] for key, root in roots.items()}
    plan = None
    if roots:
        try:
            plan = await _plan(roots, pins, preferences)
        except DependencyAnalysisError:
            for key, root in list(roots.items()):
                pins[key], reason = await _choose(root, key, preferences)
                if reason:
                    skip(key, reason)
                    roots.pop(key)
            plan = await _combine(roots, pins, preferences, skip) if roots else None
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
        plan = await _combine(roots, pins, preferences, skip) if roots else None
    report["restored"] = sorted(roots)
    report["nearby"] = {
        key: [root["version"], pins[key]]
        for key, root in roots.items()
        if root["version"] and pins[key] != root["version"]
    }
    if plan:
        report["staged"] = stage_plan(plan, plan["requirements"], restore=True)
    if report["staged"]:
        report["revision"] = (pending_transaction() or {}).get("revision")
    return report
