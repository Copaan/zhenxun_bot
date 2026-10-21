"""备份中的依赖清单：当前环境中插件使用的全部依赖"""

from __future__ import annotations

import hashlib
import importlib.metadata
import json
from pathlib import Path
import platform
import sys

from packaging.requirements import InvalidRequirement, Requirement
from packaging.utils import canonicalize_name

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


def _requires(name: str, extras: set[str] | None = None) -> dict[str, set[str]]:
    """已安装包的直接依赖，包含请求的可选依赖引入的包

    参数:
        name: 包名
        extras: 请求的可选依赖

    返回:
        dict[str, set[str]]: 依赖包名 -> 对其请求的可选依赖，未安装时为空
    """
    try:
        requires = importlib.metadata.distribution(name).requires or []
    except importlib.metadata.PackageNotFoundError:
        return {}
    contexts = {"", *(extras or ())}
    result: dict[str, set[str]] = {}
    for raw in requires:
        try:
            requirement = Requirement(raw)
        except InvalidRequirement:
            continue
        if requirement.marker and not any(
            requirement.marker.evaluate({"extra": extra}) for extra in contexts
        ):
            continue
        result.setdefault(canonicalize_name(requirement.name), set()).update(
            canonicalize_name(extra) for extra in requirement.extras
        )
    return result


def _requires_closure(names) -> dict[str, set[str]]:
    """包及其已安装依赖的传递闭包，按依赖方请求的可选依赖展开

    参数:
        names: 起始包名

    返回:
        dict[str, set[str]]: 闭包中的包名 -> 被请求的可选依赖
    """
    result: dict[str, set[str]] = {name: set() for name in names}
    queue = list(result)
    while queue:
        name = queue.pop()
        for dependency, extras in _requires(name, result[name]).items():
            known = result.get(dependency)
            if known is None or not extras <= known:
                result.setdefault(dependency, set()).update(extras)
                queue.append(dependency)
    return result


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
    for name, extras in _requires_closure(installed).items():
        excluded |= set(_requires(name, extras)) - {name}
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
    tools = sorted(set(_requires(name)) & FORBIDDEN_LAYER_PACKAGES)
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
    for name in _venv_extras(core | packages | set(_requires_closure(items))):
        add(name, "venv", [], ".venv")
    for name in sorted(packages - core - set(_requires_closure(items))):
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
