"""
JSON Schema 适配

不同协议对结构化输出 Schema 的限制不同，这里只做必要的最小转换。
"""

import copy
import json
from typing import Any

_ANTHROPIC_UNSUPPORTED = {
    "minimum",
    "maximum",
    "exclusiveMinimum",
    "exclusiveMaximum",
    "multipleOf",
    "minLength",
    "maxLength",
    "minItems",
    "maxItems",
    "default",
    "examples",
}


_MAP_KEYS = ("properties", "$defs", "definitions", "patternProperties")
_LIST_KEYS = ("anyOf", "oneOf", "allOf", "prefixItems")
_NODE_KEYS = ("items", "additionalProperties", "not", "contains")


def _walk(node: Any, visit) -> Any:
    """只在 Schema 关键字位置递归，避免把字段名误当作关键字"""
    if not isinstance(node, dict):
        return node
    node = dict(node)
    for key in _MAP_KEYS:
        if isinstance(node.get(key), dict):
            node[key] = {k: _walk(v, visit) for k, v in node[key].items()}
    for key in _LIST_KEYS:
        if isinstance(node.get(key), list):
            node[key] = [_walk(item, visit) for item in node[key]]
    for key in _NODE_KEYS:
        if isinstance(node.get(key), dict):
            node[key] = _walk(node[key], visit)
    return visit(node)


def _inline_root(schema: dict[str, Any]) -> dict[str, Any]:
    schema = copy.deepcopy(schema)
    ref = schema.get("$ref")
    defs = schema.get("$defs") or {}
    if isinstance(ref, str) and ref.startswith("#/$defs/"):
        name = ref.split("/")[-1]
        root = dict(defs.get(name) or {})
        if rest := {k: v for k, v in defs.items() if k != name}:
            root["$defs"] = rest
        schema = root
    schema.pop("$schema", None)
    return schema


def _allows_null(node: dict[str, Any]) -> bool:
    if node.get("type") == "null" or "null" in (node.get("type") or []):
        return True
    return any(
        isinstance(option, dict) and option.get("type") == "null"
        for option in node.get("anyOf") or []
    )


def strict_schema(schema: dict[str, Any]) -> dict[str, Any]:
    """转换为 OpenAI strict 模式要求的 Schema：对象封闭且全部字段必填

    参数:
        schema: 原始 JSON Schema

    返回:
        dict[str, Any]: 转换后的 Schema
    """

    def visit(node: dict[str, Any]) -> dict[str, Any]:
        node.pop("default", None)
        node.pop("examples", None)
        properties = node.get("properties")
        if node.get("type") == "object" or isinstance(properties, dict):
            properties = properties or {}
            required = set(node.get("required") or [])
            for name, prop in properties.items():
                if name not in required and isinstance(prop, dict):
                    if not _allows_null(prop):
                        properties[name] = {"anyOf": [prop, {"type": "null"}]}
            node["properties"] = properties
            node["required"] = list(properties)
            node["additionalProperties"] = False
        return node

    return _walk(_inline_root(schema), visit)


def tool_schema(schema: dict[str, Any] | None) -> dict[str, Any]:
    """整理函数工具的参数 Schema"""
    schema = _inline_root(schema or {})
    schema.pop("title", None)
    schema.setdefault("type", "object")
    schema.setdefault("properties", {})
    return schema


def anthropic_schema(schema: dict[str, Any]) -> dict[str, Any]:
    """转换为 Anthropic 结构化输出支持的 Schema：对象封闭并去掉数值与长度约束"""

    def visit(node: dict[str, Any]) -> dict[str, Any]:
        for key in _ANTHROPIC_UNSUPPORTED & node.keys():
            if not isinstance(node[key], dict):
                node.pop(key)
        if node.get("type") == "object" or isinstance(node.get("properties"), dict):
            node["additionalProperties"] = False
        return node

    return _walk(_inline_root(schema), visit)


def schema_instruction(schema: dict[str, Any]) -> str:
    """生成要求模型按 Schema 输出 JSON 的提示词"""
    return (
        "请只输出一个符合以下 JSON Schema 的 JSON 对象，"
        "不要输出 Markdown 代码块或其他文字：\n"
        + json.dumps(_inline_root(schema), ensure_ascii=False)
    )
