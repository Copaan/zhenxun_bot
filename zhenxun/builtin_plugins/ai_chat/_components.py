"""AI 聊天的可选扩展组件

组件的依赖通过 WebUI 的依赖管理装进依赖层，重启后生效。
"""

from dataclasses import dataclass
from importlib import metadata
import importlib.util
from typing import Any

from packaging.requirements import Requirement
from packaging.utils import canonicalize_name

from zhenxun.services.ai.utils.doc_parse import DOC_MODULES, OCR_MODULES


@dataclass(frozen=True)
class Component:
    key: str
    """组件标识"""
    title: str
    """名称"""
    description: str
    """说明"""
    packages: tuple[str, ...]
    """需要安装的包"""
    modules: tuple[str, ...]
    """安装后可导入的模块，用于判断是否已安装"""


COMPONENTS: dict[str, Component] = {
    c.key: c
    for c in (
        Component(
            "kb_docs",
            "知识库文档解析",
            "支持 docx / xlsx / pptx / pdf；不安装时只支持 txt / md / html / "
            "csv / json / epub 与图片",
            ("python-docx>=1.1", "openpyxl>=3.1", "python-pptx>=1.0", "pymupdf>=1.24"),
            DOC_MODULES,
        ),
        Component(
            "kb_ocr",
            "知识库 OCR",
            "识别扫描版 PDF 与图片中的文字（PDF 需同时安装文档解析）",
            ("rapidocr>=3.0", "onnxruntime>=1.18"),
            OCR_MODULES,
        ),
        Component(
            "characters",
            "角色识别本地模型",
            "lite / full 档的本地推理（onnxruntime）与安装时的模型量化（onnx）",
            ("onnxruntime>=1.18", "onnx>=1.16"),
            ("onnxruntime", "onnx"),
        ),
    )
}
"""可选组件"""


def installed(key: str) -> bool:
    """组件的依赖是否都已安装

    参数:
        key: 组件标识

    返回:
        bool: 是否已安装
    """
    return all(importlib.util.find_spec(m) is not None for m in COMPONENTS[key].modules)


def component_status(*keys: str) -> list[dict[str, Any]]:
    """组件的安装状态

    参数:
        keys: 组件标识，为空时返回所有组件

    返回:
        list[dict[str, Any]]: 组件信息与是否已安装
    """
    try:
        from zhenxun.services.nonebot_store.storage import pending_transaction

        transaction = pending_transaction()
        known = True
    except Exception:
        transaction, known = None, False
    return [
        {
            "key": c.key,
            "title": c.title,
            "description": c.description,
            "packages": list(c.packages),
            "installed": installed(c.key),
            **_install_status(c, transaction, known=known),
        }
        for c in COMPONENTS.values()
        if not keys or c.key in keys
    ]


def _install_status(
    component: Component, transaction: dict | None, *, known: bool
) -> dict[str, Any]:
    """从联合候选事务读取组件变更状态，不把候选包当作当前已安装。"""
    result = {
        "install_state": "installed" if installed(component.key) else "not_installed",
        "install_operation_ids": [],
        "install_transaction_revision": None,
    }
    if not known:
        return {**result, "install_state": "unknown"}
    if not transaction:
        return result
    try:
        requirements = [Requirement(value) for value in component.packages]
        names = {canonicalize_name(value.name) for value in requirements}
        previous = (transaction.get("base_manifest") or {}).get(
            "manual_requirements", {}
        )
        operations = []
        for operation in transaction.get("operations", []):
            target = operation.get("target_manifest") or {}
            roots = target.get("manual_requirements", previous)
            touched = {
                canonicalize_name(name)
                for name in set(previous) | set(roots)
                if previous.get(name) != roots.get(name)
            }
            for value in operation.get("requested_requirements", []):
                touched.add(canonicalize_name(Requirement(value).name))
            if names & touched and operation.get("operation_id"):
                operations.append(operation["operation_id"])
            previous = roots
        if not operations:
            return result
        target = transaction.get("target_manifest") or {}
        packages = target.get("packages", {})
        base_packages = (transaction.get("base_manifest") or {}).get("packages", {})
        ready = True
        for requirement in requirements:
            name = canonicalize_name(requirement.name)
            version = (packages.get(name) or {}).get("version")
            if version is None and name not in base_packages:
                try:
                    version = metadata.version(name)
                except metadata.PackageNotFoundError:
                    pass
            if version is None or not requirement.specifier.contains(
                str(version), prereleases=True
            ):
                ready = False
        state = {
            "pending_restart": "restart_pending",
            "building": "preparing",
            "preparing": "preparing",
            "verification_pending": "verifying",
            "committing": "verifying",
            "settling": "verifying",
            "migration_blocked": "attention_required",
            "recovery_required": "recovery_required",
            "failed": "attention_required",
            "committed": result["install_state"],
            "cleanup_pending": result["install_state"],
        }.get(transaction.get("state"), "unknown")
        if transaction.get("rollback_confirmed"):
            state = result["install_state"]
        return {
            **result,
            "install_state": state,
            "install_operation_ids": list(dict.fromkeys(operations)),
            "install_transaction_revision": transaction.get("revision"),
            "install_target_ready": ready,
        }
    except (AttributeError, TypeError, ValueError):
        return {**result, "install_state": "unknown"}
