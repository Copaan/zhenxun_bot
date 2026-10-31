from __future__ import annotations

from collections.abc import Callable
import inspect
import types

from zhenxun.services.ai.tools.models import ToolOptions
from zhenxun.utils.pydantic_compat import model_copy

from .capabilities import AdminLevelCapability, SuperuserCapability
from .tool import FunctionTool


class ToolkitMethodDescriptor:
    """用于 Toolkit 类方法的描述符，确保在实例化时绑定正确的 self 并生成 FunctionTool"""

    def __init__(
        self, func: Callable, name: str, description: str | None, settings: ToolOptions
    ):
        """
        初始化 Toolkit 类方法描述符。

        参数:
            func: 底层的 Python 可执行函数对象。
            name: 大模型识别与调用的工具名。
            description: 大模型阅读的工具功能描述说明。
            settings: 工具的配置项 (ToolOptions)。
        """
        self.func = func
        self.name = name
        self.description = description
        self.settings = settings
        self.__toolkit_tool__ = True
        self.__tool_original_name__ = name

    def __get__(self, instance, owner):
        if instance is None:
            return self
        bound_func = types.MethodType(self.func, instance)
        return FunctionTool(
            func=bound_func,
            name=self.name,
            description=self.description,
            settings=model_copy(self.settings, deep=True),
        )

    def __call__(self, *args, **kwargs):
        """使其兼容 Callable 协议，避免静态类型检查器报错"""
        return self.func(*args, **kwargs)


def tool(
    name: str | None = None,
    description: str | None = None,
    rules: list[ToolOptions] | ToolOptions | None = None,
):
    """
    将普通函数或类方法注册为 LLM 工具的装饰器。

    参数:
        name: 工具的名称(英文字母及下划线)，大模型将看到此名称，如果为空则默认使用函数名。
        description: 工具描述，大模型将基于此决定何时、如何使用该工具，如果为空，将读取函数 docstring。
        rules: 通过 Rules.xxx() 生成的规则或规则列表。
    返回:
        Callable | FunctionTool: 包装后的函数或方法。
    """  # noqa: E501

    def decorator(func: Callable):
        settings = ToolOptions()
        for r in rules if isinstance(rules, list) else [rules] if rules else []:
            settings = settings.merge(r)

        tool_name = name or func.__name__
        is_method = (
            hasattr(func, "__qualname__")
            and "." in func.__qualname__
            and "<locals>" not in func.__qualname__
        )
        if not is_method:
            try:
                params = list(inspect.signature(func).parameters)
                is_method = bool(params) and params[0] in ("self", "cls")
            except Exception:
                pass

        if is_method:
            return ToolkitMethodDescriptor(
                func=func, name=tool_name, description=description, settings=settings
            )
        return FunctionTool(
            func=func, name=tool_name, description=description, settings=settings
        )

    return decorator


def require_superuser() -> ToolOptions:
    """仅限超级管理员使用。若调用者无权限，该工具将直接对大模型隐藏。"""
    return ToolOptions(capabilities=[SuperuserCapability()])


def require_admin_level(min_level: int = 1) -> ToolOptions:
    """仅限超级用户与当前群的群主、管理员使用。"""
    return ToolOptions(
        capabilities=[AdminLevelCapability(min_level)],
        metadata={"admin_level": min_level},
    )


class Rules:
    """工具权限规则"""

    superuser = staticmethod(require_superuser)
    """权限：仅允许被超级管理员触发时，该工具才对大模型可见"""

    admin_level = staticmethod(require_admin_level)
    """权限：要求触发用户的群等级达到指定级别"""


__all__ = [
    "Rules",
    "tool",
]
