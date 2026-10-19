"""备份中的依赖清单：依赖层清单与未纳管依赖"""

from __future__ import annotations

import hashlib
import importlib.metadata
from pathlib import Path
import platform
import sys

from packaging.requirements import InvalidRequirement, Requirement
from packaging.utils import canonicalize_name
from packaging.version import InvalidVersion, Version

try:
    import tomllib
except ModuleNotFoundError:  # pragma: no cover - Python 3.10
    import tomli as tomllib

FORMAT = 1
PLUGIN_DIRS = ("zhenxun/plugins", "plugins")
RECEIPTS_FILE = Path("data/runtime/plugin-store-receipts-v1.json")
_LOCAL_PLUGIN_FIELDS = ("environment_fingerprint", "installed_at")


def _plugin_requirements(root: Path) -> dict[str, dict]:
    """插件目录中 requirements*.txt 与 pyproject.toml 声明的依赖

    参数:
        root: 项目根目录

    返回:
        dict[str, dict]: 包名 -> {requirements, sources}
    """
    from zhenxun.services.nonebot_store.dependencies import (
        DependencyAnalysisError,
        parse_requirement,
    )

    result: dict[str, dict] = {}

    def add(raw: str, source: str):
        try:
            requirement = parse_requirement(raw)
        except DependencyAnalysisError:
            return
        if requirement.marker and not requirement.marker.evaluate():
            return
        item = result.setdefault(requirement.name, {"requirements": [], "sources": []})
        if str(requirement) not in item["requirements"]:
            item["requirements"].append(str(requirement))
        if source not in item["sources"]:
            item["sources"].append(source)

    for base in PLUGIN_DIRS:
        directory = root / base
        if not directory.is_dir():
            continue
        for path in sorted(directory.glob("*/requirements*.txt")):
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


def _venv_extras(known: set[str]) -> dict[str, str]:
    """核心虚拟环境中既不在项目锁定也不在依赖层的顶层包

    参数:
        known: 已纳管的包名

    返回:
        dict[str, str]: 包名 -> 已安装版本
    """
    from zhenxun.services.nonebot_store.dependencies import (
        FORBIDDEN_LAYER_PACKAGES,
        _lock_packages,
        base_installed_inventory,
    )

    locked = set(_lock_packages()[1])
    extras = {
        name: version
        for name, version in base_installed_inventory().items()
        if name not in locked | known | FORBIDDEN_LAYER_PACKAGES
    }
    required = set()
    for name in extras:
        try:
            requires = importlib.metadata.distribution(name).requires or []
        except importlib.metadata.PackageNotFoundError:
            continue
        for raw in requires:
            try:
                required.add(canonicalize_name(Requirement(raw).name))
            except InvalidRequirement:
                continue
    return {name: extras[name] for name in sorted(extras) if name not in required}


def _unsupported(name: str) -> str | None:
    """依赖不能由恢复装入插件依赖层的原因

    ORM 与数据库驱动只能在控制台安装；依赖 pip、uv 等安装工具的包（如 nb-cli）
    在依赖层中无法满足。

    参数:
        name: 包名

    返回:
        str | None: 原因，可以装入时为 None
    """
    from zhenxun.services.nonebot_store.dependencies import FORBIDDEN_LAYER_PACKAGES
    from zhenxun.services.nonebot_store.policy import ORM_PACKAGES

    if name in ORM_PACKAGES:
        return "ORM 与数据库驱动只能在控制台安装"
    try:
        requires = importlib.metadata.distribution(name).requires or []
    except importlib.metadata.PackageNotFoundError:
        return None
    for raw in requires:
        try:
            requirement = Requirement(raw)
        except InvalidRequirement:
            continue
        if requirement.marker and not requirement.marker.evaluate({"extra": ""}):
            continue
        dependency = canonicalize_name(requirement.name)
        if dependency in ORM_PACKAGES:
            return f"依赖 {dependency}，ORM 与数据库驱动只能在控制台安装"
        if dependency in FORBIDDEN_LAYER_PACKAGES:
            return f"依赖安装工具 {dependency}，只能在控制台安装"
    return None


def unmanaged(root: Path | None = None) -> list[dict]:
    """项目锁定文件与依赖层清单都没有记录的依赖，导出时由用户选择是否带上

    来源包括插件目录声明的依赖和手动安装进核心虚拟环境的包。不能由恢复安装的
    依赖带有 unsupported 原因，导出时不会记录。

    参数:
        root: 项目根目录

    返回:
        list[dict]: [{name, requirements, version, sources, unsupported}]
    """
    from zhenxun.services.nonebot_store.dependencies import (
        base_installed_inventory,
        project_closure,
    )
    from zhenxun.services.nonebot_store.storage import load_manifest

    root = root or Path.cwd()
    manifest = load_manifest()
    known = (
        set(project_closure())
        | set(manifest["packages"])
        | {canonicalize_name(name) for name in manifest["manual_requirements"]}
    )
    installed = base_installed_inventory()
    result = {
        name: {"name": name, **item, "version": installed.get(name)}
        for name, item in _plugin_requirements(root).items()
        if name not in known
    }
    for name, version in _venv_extras(known).items():
        item = result.setdefault(
            name, {"name": name, "requirements": [], "version": version}
        )
        item.setdefault("sources", []).append(".venv")
    for item in result.values():
        item["unsupported"] = _unsupported(item["name"])
    return [result[name] for name in sorted(result)]


def snapshot(extras: list[dict] | None = None) -> dict:
    """生成备份中的 dependencies.json

    参数:
        extras: 用户选择带上的未纳管依赖（unmanaged 的返回项）

    返回:
        dict: 依赖清单
    """
    from zhenxun.services.nonebot_store.dependencies import LOCK_FILE
    from zhenxun.services.nonebot_store.storage import load_manifest, read_json

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
        },
        "packages": manifest["packages"],
        "manual_requirements": manifest["manual_requirements"],
        "extra_requirements": {
            item["name"]: {
                "requirements": item["requirements"],
                "version": item["version"],
            }
            for item in extras or []
            if not item.get("unsupported")
        },
        "store_receipts": read_json(RECEIPTS_FILE, {}),
    }


def _requirements(saved: dict, plugins: dict, extras: dict, relaxed: set) -> list:
    values = []
    for key, item in plugins.items():
        for raw in item.get("resolution_inputs") or [item["project_link"]]:
            values.append(Requirement(raw).name if key in relaxed else raw)
    for items in saved["manual_requirements"].values():
        values.extend(items)
    for name, item in extras.items():
        if name in relaxed or not item.get("version"):
            values.extend(item["requirements"] or [name])
        else:
            values.append(f"{name}=={item['version']}")
    return values


async def _plan(
    saved: dict, plugins: dict, extras: dict, relaxed: set | None = None
) -> dict:
    """解析依赖，relaxed 中的插件与依赖不限版本，其余按备份时的精确版本

    参数:
        saved: 备份中的 dependencies.json
        plugins: 参与解析的商店插件
        extras: 参与解析的未纳管依赖
        relaxed: 放宽版本的插件键与依赖名

    返回:
        dict: 依赖计划，requirements 为参与解析的依赖声明
    """
    from copy import deepcopy

    from zhenxun.services.nonebot_store.dependencies import DependencyAnalysisError
    from zhenxun.services.nonebot_store.manual import preview_manual_dependencies
    from zhenxun.services.nonebot_store.storage import load_manifest

    base = deepcopy(load_manifest())
    base.update(plugins=plugins, packages={}, manual_requirements={})
    values = _requirements(saved, plugins, extras, relaxed or set())
    plan = await preview_manual_dependencies(values, "install", manifest=base)
    if plan["source_build_required"]:
        raise DependencyAnalysisError(
            "source_build_confirmation_required", plan["diagnostic"]
        )
    target = plan["target_manifest"]
    manual = {canonicalize_name(name) for name in saved["manual_requirements"]}
    target["manual_requirements"] = {
        name: items
        for name, items in target["manual_requirements"].items()
        if name in manual or name in extras
    }
    plan["requirements"] = values
    return plan


def _downgraded(saved: dict, extras: dict, target: dict) -> dict[str, list[str]]:
    """解析结果中低于备份时版本的包

    参数:
        saved: 备份中的 dependencies.json
        extras: 参与解析的未纳管依赖
        target: 解析得到的依赖层清单

    返回:
        dict[str, list[str]]: 包名 -> [备份时版本, 解析版本]
    """

    def version(value) -> str | None:
        return value.get("version") if isinstance(value, dict) else value

    before = {name: version(value) for name, value in saved["packages"].items()}
    before.update((name, item["version"]) for name, item in extras.items())
    result = {}
    for name, value in target.get("packages", {}).items():
        old, new = before.get(name), version(value)
        try:
            if old and new and Version(new) < Version(old):
                result[name] = [old, new]
        except InvalidVersion:
            continue
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


async def stage_restore(saved: dict, skip_failed: bool = False) -> dict:
    """把备份中的依赖解析为当前环境可用的版本，写入待应用的依赖层事务

    先按备份时的精确版本整体解析；失败时逐个检查，只放宽单独也无法按精确版本
    解析的插件与依赖，放宽后仍失败的按 skip_failed 决定中止或排除。ORM、数据库
    驱动与安装工具不会由恢复安装，总是排除。

    参数:
        saved: 备份中的 dependencies.json
        skip_failed: 是否跳过无法安装的插件与依赖

    返回:
        dict: {staged, revision, skipped_plugins, skipped_requirements,
            unsupported_requirements, relaxed, downgraded}
    """
    from zhenxun.services.nonebot_store.dependencies import (
        FORBIDDEN_LAYER_PACKAGES,
        DependencyAnalysisError,
    )
    from zhenxun.services.nonebot_store.manual import stage_plan
    from zhenxun.services.nonebot_store.policy import ORM_PACKAGES
    from zhenxun.services.nonebot_store.storage import pending_transaction

    plugins = dict(saved["plugins"])
    extras = dict(saved["extra_requirements"])
    blocked = ORM_PACKAGES | FORBIDDEN_LAYER_PACKAGES
    report = {
        "staged": False,
        "skipped_plugins": [],
        "skipped_requirements": [],
        "unsupported_requirements": sorted(set(extras) & blocked),
        "relaxed": [],
        "downgraded": {},
    }
    for name in report["unsupported_requirements"]:
        extras.pop(name)
    if not (plugins or extras or saved["manual_requirements"]):
        return report
    relaxed: set[str] = set()
    try:
        plan = await _plan(saved, plugins, extras)
    except DependencyAnalysisError:
        empty = {**saved, "manual_requirements": {}}
        for key in [*plugins, *extras]:
            if key in plugins:
                single = ({key: plugins[key]}, {})
            else:
                single = ({}, {key: extras[key]})
            try:
                await _plan(empty, *single)
                continue
            except DependencyAnalysisError:
                pass
            try:
                await _plan(empty, *single, {key})
                relaxed.add(key)
            except DependencyAnalysisError:
                if not skip_failed:
                    raise
                if key in plugins:
                    report["skipped_plugins"].append(key)
                    plugins.pop(key)
                else:
                    report["skipped_requirements"].append(key)
                    extras.pop(key)
        try:
            plan = await _plan(saved, plugins, extras, relaxed)
        except DependencyAnalysisError:
            relaxed = {*plugins, *extras}
            plan = await _plan(saved, plugins, extras, relaxed)
    report["relaxed"] = sorted(relaxed)
    report["downgraded"] = _downgraded(saved, extras, plan["target_manifest"])
    report["staged"] = stage_plan(plan, plan["requirements"])
    if report["staged"]:
        report["revision"] = (pending_transaction() or {}).get("revision")
    return report
