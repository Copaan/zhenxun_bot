import inspect
from typing import Any

from .context import RunContext, _is_run_context_type


class DependencyInjector:
    """为工具与动态提示词函数注入运行上下文"""

    @staticmethod
    def can_resolve_statically(param: inspect.Parameter) -> bool:
        """参数是否由框架注入，注入的参数不出现在工具的参数定义中

        参数:
            param: 函数参数

        返回:
            bool: 类型标注为 RunContext 时为 True
        """
        return _is_run_context_type(param.annotation)

    @classmethod
    async def resolve_all(
        cls,
        sig: inspect.Signature,
        call_kwargs: dict[str, Any],
        context: RunContext,
    ) -> dict[str, Any]:
        """在调用参数中补上 RunContext 参数

        参数:
            sig: 函数签名
            call_kwargs: 已有的调用参数
            context: 运行上下文

        返回:
            dict[str, Any]: 完整的调用参数
        """
        resolved = dict(call_kwargs)
        for name, param in sig.parameters.items():
            if (
                name in ("self", "cls")
                or name in resolved
                or param.kind
                in (inspect.Parameter.VAR_POSITIONAL, inspect.Parameter.VAR_KEYWORD)
            ):
                continue
            if cls.can_resolve_statically(param):
                resolved[name] = context
            elif param.default is inspect.Parameter.empty:
                raise ValueError(
                    f"参数 '{name}' 未被大模型提供，且不是 RunContext 类型的参数"
                )
        return resolved
