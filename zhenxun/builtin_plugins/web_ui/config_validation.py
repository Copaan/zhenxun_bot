from __future__ import annotations

from io import StringIO
from typing import Any

import cattrs
from dotenv.parser import parse_stream
from ruamel.yaml import YAML

from zhenxun.configs.config import Config
from zhenxun.utils.pydantic_compat import _is_pydantic_type, parse_as


class ConfigurationValidationError(ValueError):
    def __init__(self, issues: list[dict[str, Any]]):
        super().__init__(issues[0]["code"] if issues else "configuration_invalid")
        self.issues = issues


def _issue(
    code: str,
    file: str,
    message: str,
    *,
    path: str | None = None,
    line: int | None = None,
    column: int | None = None,
    severity: str = "error",
) -> dict[str, Any]:
    return {
        "code": code,
        "file": file,
        "path": path,
        "line": line,
        "column": column,
        "severity": severity,
        "message": message,
    }


def validate_dotenv(content: str, *, file: str = ".env.dev") -> list[dict[str, Any]]:
    first_lines: dict[str, int] = {}
    issues: list[dict[str, Any]] = []
    for binding in parse_stream(StringIO(content)):
        line = int(binding.original.line)
        if binding.error:
            issues.append(
                _issue(
                    "dotenv_invalid_statement",
                    file,
                    f"第 {line} 行不是有效的 dotenv 配置语句。",
                    line=line,
                    column=1,
                )
            )
            continue
        if binding.key is None:
            continue
        key = binding.key.upper()
        if key in first_lines:
            first_line = first_lines[key]
            issues.append(
                _issue(
                    "dotenv_duplicate_key",
                    file,
                    f"配置键 {key} 在第 {first_line} 行和第 {line} 行重复。",
                    path=key,
                    line=line,
                    column=1,
                )
            )
        else:
            first_lines[key] = line
    if issues:
        raise ConfigurationValidationError(issues)
    return []


def _yaml_parser() -> YAML:
    parser = YAML()
    parser.preserve_quotes = True
    parser.indent(mapping=2, sequence=4, offset=2)
    return parser


def _location(mapping: Any, key: Any) -> tuple[int | None, int | None]:
    try:
        line, column = mapping.lc.key(key)
        return int(line) + 1, int(column) + 1
    except (AttributeError, KeyError, TypeError, ValueError):
        return None, None


def _validate_registered_value(config: Any, value: Any) -> None:
    if config.arg_parser:
        config.arg_parser(value)
        return
    if config.type is None:
        return
    if _is_pydantic_type(config.type):
        parse_as(config.type, value)
    else:
        cattrs.structure(value, config.type)


def validate_simple_yaml(
    content: str, *, file: str = "config.yaml"
) -> list[dict[str, Any]]:
    try:
        data = _yaml_parser().load(StringIO(content)) or {}
    except Exception as exc:
        mark = getattr(exc, "problem_mark", None)
        line = int(mark.line) + 1 if mark is not None else None
        column = int(mark.column) + 1 if mark is not None else None
        message = "YAML 语法无效。"
        if line is not None:
            message = f"YAML 第 {line} 行、第 {column or 1} 列语法无效。"
        raise ConfigurationValidationError(
            [
                _issue(
                    "yaml_syntax_error",
                    file,
                    message,
                    line=line,
                    column=column,
                )
            ]
        ) from exc
    if not isinstance(data, dict):
        raise ConfigurationValidationError(
            [
                _issue(
                    "yaml_top_level_mapping_required",
                    file,
                    "config.yaml 顶层必须是映射。",
                    line=1,
                    column=1,
                )
            ]
        )

    warnings: list[dict[str, Any]] = []
    issues: list[dict[str, Any]] = []
    registered = Config.get_data()
    for module, values in data.items():
        module_name = str(module)
        module_line, module_column = _location(data, module)
        if module_name not in registered:
            warnings.append(
                _issue(
                    "yaml_unknown_group",
                    file,
                    f"未知配置组将原样保留: {module_name}",
                    path=module_name,
                    line=module_line,
                    column=module_column,
                    severity="warning",
                )
            )
            continue
        if not isinstance(values, dict):
            issues.append(
                _issue(
                    "yaml_group_mapping_required",
                    file,
                    f"配置组 {module_name} 必须是映射。",
                    path=module_name,
                    line=module_line,
                    column=module_column,
                )
            )
            continue
        known = registered[module_name].configs
        for key, value in values.items():
            key_name = str(key).upper()
            path = f"{module_name}.{key_name}"
            line, column = _location(values, key)
            config = known.get(key_name)
            if config is None:
                warnings.append(
                    _issue(
                        "yaml_unknown_key",
                        file,
                        f"未知配置项将原样保留: {path}",
                        path=path,
                        line=line,
                        column=column,
                        severity="warning",
                    )
                )
                continue
            try:
                _validate_registered_value(config, value)
            except Exception as exc:
                issues.append(
                    _issue(
                        "yaml_value_invalid",
                        file,
                        (
                            f"配置项 {path} 的值不符合声明类型"
                            f"（{exc.__class__.__name__}）。"
                        ),
                        path=path,
                        line=line,
                        column=column,
                    )
                )
    if issues:
        raise ConfigurationValidationError(issues)
    return warnings


def validation_detail(
    error: ConfigurationValidationError,
    *,
    message: str = "配置校验失败。",
) -> dict[str, Any]:
    return {"message": message, "issues": error.issues}


__all__ = [
    "ConfigurationValidationError",
    "validate_dotenv",
    "validate_simple_yaml",
    "validation_detail",
]
