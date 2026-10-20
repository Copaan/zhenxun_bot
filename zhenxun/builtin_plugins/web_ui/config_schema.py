"""Type descriptions shared by configuration editors without reading files."""

from __future__ import annotations

from pathlib import Path
from types import UnionType
from typing import Annotated, Any, Union, get_args, get_origin

from fastapi.encoders import jsonable_encoder
from pydantic import TypeAdapter


def schema_for_type(value_type: Any, _seen: tuple[Any, ...] = ()) -> dict[str, Any]:
    if value_type is None or value_type is Any:
        return {}
    try:
        return TypeAdapter(value_type).json_schema()
    except Exception as error:
        # Some third-party annotations (for example SQLAlchemy URL/Engine)
        # cannot be represented by Pydantic's JSON Schema generator. Keep the
        # field visible and let the backend perform the authoritative check.
        diagnostic = {
            "description": "类型声明无法生成表单，请按当前值编辑并由后端校验。",
            "x-schema-error": type(error).__name__,
        }
        if value_type in _seen:
            return {**diagnostic, "x-schema-recursive": True}
        seen = (*_seen, value_type)
        origin, args = get_origin(value_type), get_args(value_type)
        if origin in (Union, UnionType):
            return {**diagnostic, "anyOf": [schema_for_type(t, seen) for t in args]}
        if origin is Annotated:
            return {**diagnostic, **schema_for_type(args[0], seen)}
        if origin is dict or value_type is dict:
            return {
                **diagnostic,
                "type": "object",
                "additionalProperties": schema_for_type(args[1], seen) if args else {},
            }
        if origin in (list, set, tuple) or value_type in (list, set, tuple):
            return {
                **diagnostic,
                "type": "array",
                "items": schema_for_type(args[0], seen) if args else {},
            }
        fields = getattr(value_type, "model_fields", None) or getattr(
            value_type, "__fields__", None
        )
        if fields:
            return {
                **diagnostic,
                "type": "object",
                "properties": {
                    name: schema_for_type(
                        getattr(field, "outer_type_", None)
                        or getattr(field, "annotation", Any),
                        seen,
                    )
                    for name, field in fields.items()
                },
            }
        module = str(getattr(value_type, "__module__", ""))
        if module.startswith(("sqlalchemy", "pathlib")):
            return {**diagnostic, "type": "string"}
        return diagnostic


def configuration_value(value: Any) -> Any:
    """Describe runtime-only values without exposing engine internals or credentials."""
    if isinstance(value, Path):
        return str(value)
    if type(value).__module__.startswith("sqlalchemy"):
        url = getattr(value, "url", value)
        render = getattr(url, "render_as_string", None)
        if callable(render):
            return render(hide_password=True)
        return str(value)
    if isinstance(value, dict):
        return {str(k): configuration_value(v) for k, v in value.items()}
    if isinstance(value, list | tuple | set):
        return [configuration_value(v) for v in value]
    try:
        return jsonable_encoder(value)
    except (TypeError, ValueError, RecursionError):
        return None


def inferred_schema(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return {"type": "object", "additionalProperties": {}}
    if isinstance(value, list):
        return {"type": "array", "items": {}}
    if value is None:
        return {}
    return schema_for_type(type(value))


def _accepts_null(schema: dict[str, Any]) -> bool:
    types = schema.get("type")
    if types == "null" or (isinstance(types, list) and "null" in types):
        return True
    return any(
        _accepts_null(branch)
        for branch in schema.get("anyOf", []) + schema.get("oneOf", [])
    )


def config_field_schema(value_type: Any, value: Any, default: Any) -> dict[str, Any]:
    """生成配置项的编辑 schema

    未声明类型时按当前值或默认值推断，两者都为空时按文本处理；
    值或默认值为空时允许 null，使未填写的配置项可以直接编辑和保存。

    参数:
        value_type: 声明的类型
        value: 当前值
        default: 默认值

    返回:
        dict[str, Any]: JSON Schema
    """
    schema = schema_for_type(value_type) or inferred_schema(
        value if value is not None else default
    )
    if not schema and value is None and default is None:
        schema = {"type": "string"}
    if not schema or (value is not None and default is not None):
        return schema
    if _accepts_null(schema):
        return schema
    branch = dict(schema)
    nullable: dict[str, Any] = {"anyOf": [branch, {"type": "null"}]}
    for key in ("$defs", "definitions", "title", "description"):
        if key in branch:
            nullable[key] = branch.pop(key)
    return nullable
