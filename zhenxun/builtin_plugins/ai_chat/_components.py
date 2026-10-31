"""AI 聊天的可选扩展组件

组件的依赖通过 WebUI 的依赖管理装进依赖层，重启后生效。
"""

from dataclasses import dataclass
import importlib.util
from typing import Any

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
    return [
        {
            "key": c.key,
            "title": c.title,
            "description": c.description,
            "packages": list(c.packages),
            "installed": installed(c.key),
        }
        for c in COMPONENTS.values()
        if not keys or c.key in keys
    ]
