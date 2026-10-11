"""
Pydantic 便捷函数模块

为 Pydantic V2 提供统一的便捷函数，
包括 model_dump, model_copy, model_json_schema, parse_as 等。
"""

from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any, TypeVar, get_args, get_origin

from nonebot.compat import (
    PYDANTIC_V2,
    model_dump,
    model_fields,
    type_validate_json,
    type_validate_python,
)
from pydantic import BaseModel, TypeAdapter, model_validator
from pydantic import computed_field as compat_computed_field
import ujson as json

T = TypeVar("T", bound=BaseModel)
V = TypeVar("V")


__all__ = [
    "PYDANTIC_V2",
    "TypeAdapter",
    "_dump_pydantic_obj",
    "_is_pydantic_type",
    "compat_computed_field",
    "dump_json_safely",
    "model_construct",
    "model_copy",
    "model_dump",
    "model_dump_json",
    "model_fields",
    "model_json_schema",
    "model_rebuild",
    "model_validate",
    "model_validator",
    "parse_as",
    "type_validate_json",
    "type_validate_python",
]


def model_copy(
    model: T, *, update: dict[str, Any] | None = None, deep: bool = False
) -> T:
    """
    复制模型实例，可同时更新部分字段。
    """
    return model.model_copy(update=update, deep=deep)


def model_construct(model_class: type[T], **kwargs: Any) -> T:
    """
    不经校验直接构造模型实例。
    """
    return model_class.model_construct(**kwargs)


def model_validate(model_class: type[T], obj: Any) -> T:
    """
    校验数据并构造模型实例。
    """
    return type_validate_python(model_class, obj)


def model_dump_json(model: BaseModel, **kwargs: Any) -> str:
    """
    将模型实例序列化为 JSON 字符串。
    """
    return model.model_dump_json(**kwargs)


def model_json_schema(model_class: type[BaseModel], **kwargs: Any) -> dict[str, Any]:
    """
    生成模型的 JSON Schema。
    """
    return model_class.model_json_schema(**kwargs)


def _is_pydantic_type(t: Any) -> bool:
    """
    递归检查一个类型注解是否与 Pydantic BaseModel 相关。
    """
    if t is None:
        return False
    origin = get_origin(t)
    if origin:
        return any(_is_pydantic_type(arg) for arg in get_args(t))
    return isinstance(t, type) and issubclass(t, BaseModel)


def _dump_pydantic_obj(obj: Any) -> Any:
    """
    递归地将一个对象内部的 Pydantic BaseModel 实例转换为字典。
    支持单个实例、实例列表、实例字典等情况。
    """
    if isinstance(obj, BaseModel):
        return model_dump(obj)
    if isinstance(obj, list):
        return [_dump_pydantic_obj(item) for item in obj]
    if isinstance(obj, dict):
        return {key: _dump_pydantic_obj(value) for key, value in obj.items()}
    return obj


parse_as = type_validate_python


def dump_json_safely(obj: Any, **kwargs) -> str:
    """
    安全地将可能包含 Pydantic 特定类型 (如 Enum) 的对象序列化为 JSON 字符串。
    """

    def default_serializer(o):
        if isinstance(o, Enum):
            return o.value
        if isinstance(o, datetime):
            return o.isoformat()
        if isinstance(o, Path):
            return str(o.as_posix())
        if isinstance(o, set):
            return list(o)
        if isinstance(o, BaseModel):
            return model_dump(o)
        raise TypeError(
            f"Object of type {o.__class__.__name__} is not JSON serializable"
        )

    return json.dumps(obj, default=default_serializer, **kwargs)


def model_rebuild(model_class: type[BaseModel], **kwargs: Any) -> None:
    """
    重建模型以解析前向引用。
    """
    model_class.model_rebuild(**kwargs)
