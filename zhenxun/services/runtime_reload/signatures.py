from functools import partial, wraps
import inspect

from nonebot.dependencies.utils import get_typed_annotation
from nonebot.utils import is_coroutine_callable


def async_callable(function):
    while isinstance(function, partial):
        function = function.func
    return inspect.iscoroutinefunction(function) or is_coroutine_callable(function)


def typed_wraps(function=None, *, asgi=False):
    """Keep dependency injection annotations in the original callable's namespace."""
    if function is None:
        return lambda value: typed_wraps(value, asgi=asgi)
    original = inspect.unwrap(function)
    while isinstance(original, partial):
        original = original.func
    annotation_target = (
        original
        if inspect.isfunction(original) or inspect.ismethod(original)
        else original.__call__
    )
    namespace = getattr(annotation_target, "__globals__", {})
    signature = inspect.signature(function)
    hints = {
        name: get_typed_annotation(parameter, namespace)
        for name, parameter in signature.parameters.items()
    }
    return_type = signature.return_annotation
    if asgi and isinstance(return_type, str):
        # FastAPI uses the return annotation as a response model. Let its
        # registration fail on an invalid model instead of silently dropping it.
        from typing import ForwardRef

        from nonebot.typing import evaluate_forwardref

        return_type = evaluate_forwardref(ForwardRef(return_type), namespace, namespace)
    signature = signature.replace(
        parameters=[
            parameter.replace(annotation=hints.get(name, parameter.annotation))
            for name, parameter in signature.parameters.items()
        ],
        return_annotation=return_type,
    )

    def decorate(wrapper):
        wrapper = wraps(function)(wrapper)
        wrapper.__signature__ = signature
        wrapper.__annotations__ = {**hints, "return": return_type}
        return wrapper

    return decorate
