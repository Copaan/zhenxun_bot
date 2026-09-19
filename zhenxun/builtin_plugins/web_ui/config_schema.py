"""Type descriptions shared by configuration editors without reading files."""

from __future__ import annotations

from typing import Any

from nonebot.compat import PYDANTIC_V2


def schema_for_type(value_type: Any) -> dict[str, Any]:
    if value_type is None or value_type is Any:
        return {}
    try:
        if PYDANTIC_V2:
            from pydantic import TypeAdapter

            return TypeAdapter(value_type).json_schema()
        from pydantic import schema_of

        return schema_of(value_type)
    except (TypeError, ValueError, AttributeError):
        return {"description": "未提供完整类型定义，请按当前值编辑并由后端校验。"}


def inferred_schema(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return {"type": "object", "additionalProperties": {}}
    if isinstance(value, list):
        return {"type": "array", "items": {}}
    if value is None:
        return {}
    return schema_for_type(type(value))
