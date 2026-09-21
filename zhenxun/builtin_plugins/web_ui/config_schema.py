"""Type descriptions shared by configuration editors without reading files."""

from __future__ import annotations

from pathlib import Path
from types import UnionType
from typing import Annotated, Any, Union, get_args, get_origin

from fastapi.encoders import jsonable_encoder
from nonebot.compat import PYDANTIC_V2


def schema_for_type(value_type: Any, _seen: tuple[Any, ...] = ()) -> dict[str, Any]:
    if value_type is None or value_type is Any:
        return {}
    try:
        if PYDANTIC_V2:
            from pydantic import TypeAdapter

            return TypeAdapter(value_type).json_schema()
        from pydantic import schema_of

        return schema_of(value_type)
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
