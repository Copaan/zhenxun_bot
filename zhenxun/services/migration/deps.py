"""备份中的依赖清单：依赖层清单与未纳管依赖"""

from __future__ import annotations

import hashlib
import importlib.metadata
from pathlib import Path
import platform
import sys

from packaging.requirements import InvalidRequirement, Requirement
from packaging.utils import canonicalize_name

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


def unmanaged(root: Path | None = None) -> list[dict]:
    """项目锁定文件与依赖层清单都没有记录的依赖，导出时由用户选择是否带上

    来源包括插件目录声明的依赖和手动安装进核心虚拟环境的包。

    参数:
        root: 项目根目录

    返回:
        list[dict]: [{name, requirements, version, sources}]
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
        },
        "store_receipts": read_json(RECEIPTS_FILE, {}),
    }


def _requirements(saved: dict, plugins: dict, extras: dict, relaxed: bool) -> list:
    values = []
    for item in plugins.values():
        for raw in item.get("resolution_inputs") or [item["project_link"]]:
            values.append(Requirement(raw).name if relaxed else raw)
    for items in saved["manual_requirements"].values():
        values.extend(items)
    for name, item in extras.items():
        if relaxed or not item.get("version"):
            values.extend(item["requirements"] or [name])
        else:
            values.append(f"{name}=={item['version']}")
    return values


async def _plan(saved: dict, plugins: dict, extras: dict) -> dict:
    """依次按备份时的精确版本、放宽版本解析依赖"""
    from copy import deepcopy

    from zhenxun.services.nonebot_store.dependencies import DependencyAnalysisError
    from zhenxun.services.nonebot_store.manual import preview_manual_dependencies
    from zhenxun.services.nonebot_store.storage import load_manifest

    base = deepcopy(load_manifest())
    base.update(plugins=plugins, packages={}, manual_requirements={})
    error = None
    for relaxed in (False, True):
        values = _requirements(saved, plugins, extras, relaxed)
        try:
            plan = await preview_manual_dependencies(values, "install", manifest=base)
        except DependencyAnalysisError as failure:
            error = failure
            continue
        if plan["source_build_required"]:
            error = DependencyAnalysisError(
                "source_build_confirmation_required", plan["diagnostic"]
            )
            continue
        target = plan["target_manifest"]
        manual = {canonicalize_name(name) for name in saved["manual_requirements"]}
        target["manual_requirements"] = {
            name: items
            for name, items in target["manual_requirements"].items()
            if name in manual or name in extras
        }
        plan["requirements"] = values
        return plan
    raise error  # type: ignore


async def stage_restore(saved: dict, skip_failed: bool = False) -> dict:
    """把备份中的依赖解析为当前环境可用的版本，写入待应用的依赖层事务

    先按备份时的精确版本解析，失败后放宽版本；仍失败时按 skip_failed 决定中止，
    或逐个排除无法安装的插件与依赖后继续。

    参数:
        saved: 备份中的 dependencies.json
        skip_failed: 是否跳过无法安装的插件与依赖

    返回:
        dict: {staged, skipped_plugins, skipped_requirements}
    """
    from zhenxun.services.nonebot_store.dependencies import DependencyAnalysisError
    from zhenxun.services.nonebot_store.manual import stage_plan

    plugins = dict(saved["plugins"])
    extras = dict(saved["extra_requirements"])
    report = {"staged": False, "skipped_plugins": [], "skipped_requirements": []}
    if not (plugins or extras or saved["manual_requirements"]):
        return report
    try:
        plan = await _plan(saved, plugins, extras)
    except DependencyAnalysisError:
        if not skip_failed:
            raise
        empty = {**saved, "manual_requirements": {}}
        for key in list(plugins):
            try:
                await _plan(empty, {key: plugins[key]}, {})
            except DependencyAnalysisError:
                report["skipped_plugins"].append(key)
                plugins.pop(key)
        for name in list(extras):
            try:
                await _plan(empty, {}, {name: extras[name]})
            except DependencyAnalysisError:
                report["skipped_requirements"].append(name)
                extras.pop(name)
        plan = await _plan(saved, plugins, extras)
    report["staged"] = stage_plan(plan, plan["requirements"])
    return report
