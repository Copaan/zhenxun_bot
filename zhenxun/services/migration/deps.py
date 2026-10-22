"""备份中的依赖清单：当前环境中插件使用的全部依赖"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
import platform
import sys

from packaging.requirements import InvalidRequirement, Requirement
from packaging.utils import canonicalize_name

FORMAT = 2
_LOCAL_PLUGIN_FIELDS = ("environment_fingerprint", "installed_at")
KINDS = ("nonebot_plugin", "manual", "store", "plugin", "venv", "layer")
"""依赖来源：NoneBot 商店插件、手动依赖、真寻商店插件、插件目录声明、核心虚拟环境、
依赖层中没有归属的包"""
_EXTRA_KINDS = {"store", "plugin", "venv", "layer"}


def _parse(raw: str) -> Requirement | None:
    """解析依赖声明，保留平台条件，由恢复时按目标平台判断是否适用

    参数:
        raw: 依赖声明

    返回:
        Requirement | None: 依赖，无效时为 None
    """
    from zhenxun.services.nonebot_store.dependencies import (
        DependencyAnalysisError,
        parse_requirement,
    )

    try:
        return parse_requirement(raw)
    except DependencyAnalysisError:
        return None


def plugin_requirement_files(root: Path) -> dict[str, list[str]]:
    """插件目录中 requirements 文件声明的依赖

    参数:
        root: 项目根目录

    返回:
        dict[str, list[str]]: 相对 root 的文件路径 -> 依赖声明行，无法读取的文件
            不在其中
    """
    from zhenxun.services.nonebot_store.dependencies import requirement_lines
    from zhenxun.services.nonebot_store.environment import (
        REQUIREMENT_FILES,
        SOURCE_PLUGIN_ROOT,
    )

    directory = root / SOURCE_PLUGIN_ROOT
    result: dict[str, list[str]] = {}
    for path in sorted(
        path for name in REQUIREMENT_FILES for path in directory.glob(f"*/{name}")
    ):
        try:
            result[path.relative_to(root).as_posix()] = requirement_lines(path)
        except (OSError, UnicodeDecodeError):
            continue
    return result


def _plugin_requirements(root: Path) -> dict[str, dict]:
    """插件目录中 requirements 文件声明的依赖，按包名汇总

    参数:
        root: 项目根目录

    返回:
        dict[str, dict]: 包名 -> {requirements, sources}
    """
    result: dict[str, dict] = {}
    for source, lines in plugin_requirement_files(root).items():
        for raw in lines:
            requirement = _parse(raw)
            if requirement is None:
                continue
            item = result.setdefault(
                canonicalize_name(requirement.name), {"requirements": [], "sources": []}
            )
            if str(requirement) not in item["requirements"]:
                item["requirements"].append(str(requirement))
            if source not in item["sources"]:
                item["sources"].append(source)
    return result


def _direct_url(distribution) -> str | None:
    """以直链（git、本地路径、压缩包地址）安装的包的原安装地址

    参数:
        distribution: 已安装的包

    返回:
        str | None: 安装地址，从包索引安装时为 None
    """
    try:
        data = json.loads(distribution.read_text("direct_url.json") or "null")
    except ValueError:
        return None
    if not isinstance(data, dict) or not data.get("url"):
        return None
    vcs = data.get("vcs_info") or {}
    if not vcs.get("vcs"):
        return data["url"]
    commit = f"@{vcs['commit_id']}" if vcs.get("commit_id") else ""
    return f"{vcs['vcs']}+{data['url']}{commit}"


def _venv_roots(base: dict) -> list[str]:
    """核心虚拟环境中的顶层包：不是本体或安装工具、不属于开发依赖、也不被其他可以
    恢复的已安装包依赖

    依赖安装工具的包（如 nb-cli）不能由恢复安装，它们依赖的其他包仍是顶层包；本体
    的元数据可能停留在初次安装时的版本，同样不计入。

    参数:
        base: 核心虚拟环境中的包

    返回:
        list[str]: 包名
    """
    from zhenxun.services.nonebot_store.dependencies import (
        FORBIDDEN_LAYER_PACKAGES,
        dev_packages,
    )
    from zhenxun.services.nonebot_store.environment import reachable_packages

    required = []
    for name, distribution in base.items():
        if _unsupported(name, base):
            continue
        for raw in distribution.requires or []:
            try:
                if canonicalize_name(Requirement(raw).name) != name:
                    required.append(raw)
            except InvalidRequirement:
                continue
    excluded = (
        dev_packages() | FORBIDDEN_LAYER_PACKAGES | reachable_packages(required, base)
    )
    return sorted(set(base) - excluded)


def _unsupported(name: str, selected: dict) -> str | None:
    """依赖不能由恢复装入插件依赖层的原因

    pip、uv 等安装工具以及依赖它们的包（如 nb-cli）在依赖层中无法使用。

    参数:
        name: 包名
        selected: 已安装的包

    返回:
        str | None: 原因，可以装入时为 None
    """
    from zhenxun.services.nonebot_store.dependencies import FORBIDDEN_LAYER_PACKAGES
    from zhenxun.services.nonebot_store.environment import reachable_packages

    if name in FORBIDDEN_LAYER_PACKAGES:
        return "安装工具只能在控制台安装"
    tools = sorted(
        reachable_packages([name], selected) & FORBIDDEN_LAYER_PACKAGES - {name}
    )
    return f"依赖安装工具 {tools[0]}，只能在控制台安装" if tools else None


def inventory(root: Path | None = None) -> list[dict]:
    """当前环境中插件使用的全部依赖，导出备份时默认全部记录

    来源见 KINDS，同一个包出现在多个来源时归入靠前的来源。随本体安装的包与开发
    依赖不在其中。以直链安装的包带有原安装地址 url。不能由恢复安装的依赖带有
    unsupported 原因，备份只记录名称，恢复后提示手动安装。

    参数:
        root: 项目根目录

    返回:
        list[dict]: [{name, kind, requirements, version, sources, url, unsupported}]
    """
    from zhenxun.services.nonebot_store.dependencies import project_closure
    from zhenxun.services.nonebot_store.environment import (
        capture_environment,
        reachable_packages,
    )
    from zhenxun.services.plugin_store.plugin_store_receipts import (
        receipt_dependency_inputs,
    )

    root = root or Path.cwd()
    environment = capture_environment()
    manifest = environment.manifest
    selected = environment.selected
    core = set(project_closure())
    items: dict[str, dict] = {}

    def add(name: str, kind: str, requirements: list[str], source: str) -> None:
        distribution = selected.get(name)
        item = items.setdefault(
            name,
            {
                "name": name,
                "kind": kind,
                "requirements": [],
                "version": distribution.version if distribution else None,
                "sources": [],
                "url": _direct_url(distribution)
                if kind == "venv" and distribution
                else None,
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
    used = reachable_packages(items, selected)
    for name in set(_venv_roots(environment.base)) - core - packages - used:
        add(name, "venv", [], ".venv")
    for name in sorted(packages - core - used):
        add(name, "layer", [], "layer")
    for item in items.values():
        item["unsupported"] = (
            _unsupported(item["name"], selected)
            if item["kind"] in _EXTRA_KINDS
            else None
        )
    return sorted(
        items.values(), key=lambda item: (KINDS.index(item["kind"]), item["name"])
    )


def snapshot(items: list[dict] | None = None) -> dict:
    """生成备份中的 dependencies.json

    core 记录随本体安装的顶层包，恢复到不再包含它们的本体版本时按虚拟环境中的包
    恢复。

    参数:
        items: 要记录的依赖（inventory 的返回项），为 None 时记录全部依赖

    返回:
        dict: 依赖清单
    """
    from zhenxun.services.nonebot_store.dependencies import LOCK_FILE, project_closure
    from zhenxun.services.nonebot_store.environment import capture_environment
    from zhenxun.services.plugin_store.plugin_store_receipts import StoreReceiptStore

    items = inventory() if items is None else items
    selected = {item["name"] for item in items}
    environment = capture_environment()
    manifest = environment.manifest
    core = set(project_closure())
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
        "installed": environment.versions,
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
        "core": {
            name: {
                "version": environment.versions.get(name),
                "unsupported": _unsupported(name, environment.selected),
            }
            for name in _venv_roots(environment.base)
            if name in core
        },
        "store_receipts": StoreReceiptStore.load(),
    }
