"""AI 运行时，顶层按需转发 `llm` 的对外接口与 Agent"""

from collections.abc import Callable
from importlib import import_module
from typing import Any


def forward(names: dict[str, str]) -> Callable[[str], Any]:
    """生成按需导入的模块级 `__getattr__`

    参数:
        names: 名称到定义模块的映射

    返回:
        Callable[[str], Any]: 模块级 `__getattr__`
    """

    def __getattr__(name: str) -> Any:
        if name not in names:
            raise AttributeError(name)
        return getattr(import_module(names[name]), name)

    return __getattr__


_forward = forward(
    {
        "Agent": "zhenxun.services.ai.flow.agent.agent",
        "RunContext": "zhenxun.services.ai.run.context",
    }
)


def __getattr__(name: str) -> Any:
    """转发 `zhenxun.services.ai.llm` 导出的名称及 Agent、RunContext

    参数:
        name: 属性名

    返回:
        Any: 对应对象
    """
    llm = import_module("zhenxun.services.ai.llm")
    if name in llm.__all__:
        return getattr(llm, name)
    return _forward(name)
