"""插件登记的归属判定与清理

归属只依赖 NoneBot 自带的插件身份（Matcher._source、模块名）、回调函数的
``__module__`` 以及协程代码所在文件，不安装任何全局拦截。
每种核心注册表对应一个 ``Adapter``：``collect`` 找出属于插件的条目，
``remove`` 移除这些条目。

新增允许插件写入的注册表时，若条目会在重载后重复累积，或重复登记会报错，
必须在 ``ADAPTERS`` 中补上对应的 ``Adapter``；以名称为键、重载时直接覆盖的
注册表可以不处理。
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Iterable
import contextlib
import contextvars
from dataclasses import dataclass
import functools
import inspect
import os
from pathlib import Path
import sys
from types import ModuleType
from typing import Any
import weakref

import nonebot
from nonebot.plugin import Plugin, _managers

from zhenxun.services.log import logger


def _unwrap(obj: Any) -> Any:
    for _ in range(8):
        if hasattr(obj, "call") and type(obj).__name__ == "Dependent":
            obj = obj.call
        elif isinstance(obj, functools.partial):
            obj = obj.func
        elif inspect.ismethod(obj) or isinstance(obj, staticmethod | classmethod):
            obj = obj.__func__
        elif getattr(obj, "__wrapped__", None) is not None:
            obj = obj.__wrapped__
        else:
            break
    return obj


def iter_sub_plugins(plugin: Plugin) -> Iterable[Plugin]:
    for sub in list(plugin.sub_plugins):
        yield sub
        yield from iter_sub_plugins(sub)


@dataclass(frozen=True)
class Ownership:
    """一个插件（含子插件）拥有的模块前缀、源码目录和插件 id"""

    roots: tuple[str, ...]
    paths: tuple[str, ...]
    plugin_ids: frozenset[str]

    @classmethod
    def of(cls, *tops: Plugin) -> Ownership:
        """根据已加载的插件构造归属

        参数:
            tops: 插件，子插件一并归入

        返回:
            Ownership: 归属
        """
        plugins = [p for top in tops for p in (top, *iter_sub_plugins(top))]
        files = [getattr(p.module, "__file__", None) for p in plugins]
        return cls.build(
            {p.module_name for p in plugins},
            {p.id_ for p in plugins},
            [Path(file) for file in files if file],
        )

    @classmethod
    def build(
        cls, roots: Iterable[str], plugin_ids: Iterable[str], files: Iterable[Path]
    ) -> Ownership:
        """根据模块名、插件 id 和入口文件构造归属

        参数:
            roots: 模块名前缀
            plugin_ids: 插件 id
            files: 插件入口文件

        返回:
            Ownership: 归属
        """
        paths = set()
        for file in files:
            path = file.resolve()
            path = path.parent if path.name == "__init__.py" else path
            paths.add(os.path.normcase(str(path)))
        return cls(
            tuple(sorted(set(roots))), tuple(sorted(paths)), frozenset(plugin_ids)
        )

    def module_owned(self, name: str | None) -> bool:
        if not name:
            return False
        return any(name == root or name.startswith(f"{root}.") for root in self.roots)

    def owned(self, obj: Any) -> bool:
        if inspect.ismethod(obj) and self.owned(obj.__self__):
            return True
        obj = _unwrap(obj)
        source = getattr(obj, "_source", None) if isinstance(obj, type) else None
        if source is not None and hasattr(source, "module_name"):
            return source.plugin_id in self.plugin_ids or self.module_owned(
                source.module_name
            )
        name = getattr(obj, "__module__", None)
        if not isinstance(name, str):
            name = type(obj).__module__
        return self.module_owned(name)

    def closure_owned(self, func: Any) -> bool:
        if func is None:
            return False
        if self.owned(func):
            return True
        for cell in getattr(func, "__closure__", None) or ():
            with contextlib.suppress(ValueError):
                if self.owned(cell.cell_contents):
                    return True
        return False

    def file_owned(self, filename: str) -> bool:
        lowered = os.path.normcase(filename)
        return any(
            lowered == path or lowered.startswith(path + os.sep) for path in self.paths
        )

    def modules(self) -> list[str]:
        return [name for name in list(sys.modules) if self.module_owned(name)]


@dataclass
class Adapter:
    """一种核心注册表：collect 返回属于插件的条目，remove 移除这些条目"""

    name: str
    collect: Callable[[Ownership], list[Any]]
    remove: Callable[[list[Any]], None]


def _set_adapter(name: str, getter: Callable[[], set]) -> Adapter:
    def remove(items: list[Any]) -> None:
        target = getter()
        for item in items:
            target.discard(item)

    return Adapter(
        name, lambda own: [i for i in list(getter()) if own.owned(i)], remove
    )


def _list_adapter(name: str, getter: Callable[[], list]) -> Adapter:
    def remove(items: list[Any]) -> None:
        target = getter()
        for item in items:
            while item in target:
                target.remove(item)

    return Adapter(
        name, lambda own: [i for i in list(getter()) if own.owned(i)], remove
    )


def _remove_keyed(items: list[Any]) -> None:
    for container, key in items:
        if isinstance(container, list):
            while key in container:
                container.remove(key)
        else:
            container.pop(key, None)


def _matchers(own: Ownership) -> list[Any]:
    from nonebot.matcher import matchers

    return [
        matcher
        for group in list(matchers.values())
        for matcher in list(group)
        if matcher.plugin_id in own.plugin_ids or own.module_owned(matcher.module_name)
    ]


def _commands(matcher: Any) -> set[tuple[str, ...]]:
    from nonebot.rule import CommandRule, ShellCommandRule

    return {
        cmd
        for checker in matcher.rule.checkers
        if isinstance(checker.call, CommandRule | ShellCommandRule)
        for cmd in checker.call.cmds
    }


def _release_prefixes(commands: set[tuple[str, ...]]) -> None:
    """从 NoneBot 的指令前缀表中删除不再有 matcher 使用的指令"""
    from nonebot.matcher import matchers
    from nonebot.rule import TrieRule

    used = set()
    for group in list(matchers.values()):
        for matcher in list(group):
            used |= _commands(matcher)
    for key, value in list(TrieRule.prefix.items()):
        if value.command in commands - used:
            del TrieRule.prefix[key]


def _remove_matchers(items: list[Any]) -> None:
    commands = set().union(*(_commands(matcher) for matcher in items))
    executors = []
    for matcher in items:
        with contextlib.suppress(KeyError, ValueError):
            if hasattr(matcher, "clean") and hasattr(matcher, "executor"):
                executors.append(matcher.executor)
                matcher.clean()
            else:
                matcher.destroy()
    _release_executors(executors)
    _release_prefixes(commands)


def _release_executors(executors: list[Any]) -> None:
    """Alconna 扩展执行器用 weakref.finalize 持有自身方法，销毁后不会被回收；
    这里解除 finalize 并清掉其保存的最近一次事件上下文"""
    if not executors:
        return
    ids = {id(executor) for executor in executors}
    registry = getattr(weakref.finalize, "_registry")
    for finalizer in list(registry):
        info = registry.get(finalizer)
        args = getattr(info, "args", ()) or ()
        if any(id(getattr(arg, "__self__", None)) in ids for arg in args):
            finalizer.detach()
    for executor in executors:
        if getattr(executor, "_dependent_executor", None) is not None:
            executor._dependent_executor = None


def _alconna_commands(own: Ownership) -> list[Any]:
    try:
        from arclet.alconna import command_manager
    except ImportError:
        return []
    result = []
    store = getattr(command_manager, "_CommandManager__commands")
    for namespace in list(store.values()):
        for command in list(namespace.values()):
            source = command.meta.extra.get("matcher.source")
            if source is not None and (
                source.plugin_id in own.plugin_ids
                or own.module_owned(source.module_name)
            ):
                result.append(command)
    return result


def _remove_alconna(items: list[Any]) -> None:
    from arclet.alconna import command_manager

    shortcuts = getattr(command_manager, "_CommandManager__shortcuts")
    for command in items:
        namespace, name = command_manager._command_part(command.path)
        shortcuts.pop(f"{namespace}.{name}", None)
        with contextlib.suppress(Exception):
            command_manager.delete(command)


def _message_set(name: str) -> Callable[[], set]:
    def getter() -> set:
        import nonebot.message as message

        return getattr(message, name)

    return getter


def _driver_set(name: str) -> Callable[[], set]:
    return lambda: getattr(type(nonebot.get_driver()), name)


def _bot_set(name: str) -> Callable[[], set]:
    def getter() -> set:
        from nonebot.adapters import Bot

        return getattr(Bot, name)

    return getter


def lifespan_funcs(kind: str) -> list[Any]:
    """NoneBot 生命周期钩子列表

    参数:
        kind: startup / ready / shutdown

    返回:
        list[Any]: 钩子列表
    """
    return getattr(nonebot.get_driver()._lifespan, f"_{kind}_funcs")


def _priority_lifecycle(own: Ownership) -> list[Any]:
    from zhenxun.utils.manager.priority_manager import PriorityLifecycle

    return [
        (hook_type, priority, func)
        for hook_type, by_priority in PriorityLifecycle._data.items()
        for priority, funcs in list(by_priority.items())
        for func in list(funcs)
        if own.owned(func)
    ]


def _remove_priority_lifecycle(items: list[Any]) -> None:
    from zhenxun.utils.manager.priority_manager import PriorityLifecycle

    for hook_type, priority, func in items:
        by_priority = PriorityLifecycle._data.get(hook_type, {})
        funcs = by_priority.get(priority, [])
        while func in funcs:
            funcs.remove(func)
        if not funcs:
            by_priority.pop(priority, None)
        PriorityLifecycle._metadata.pop(func, None)


def _apscheduler_jobs(own: Ownership) -> list[Any]:
    module = sys.modules.get("nonebot_plugin_apscheduler")
    if module is None:
        return []
    return [job for job in module.scheduler.get_jobs() if own.owned(job.func)]


def _remove_jobs(items: list[Any]) -> None:
    for job in items:
        with contextlib.suppress(Exception):
            job.remove()


def _scheduler_tasks(own: Ownership) -> list[Any]:
    module = sys.modules.get("zhenxun.services.scheduler.registry")
    registry = getattr(module, "scheduler_registry", None) if module else None
    if registry is None:
        return []
    items: list[Any] = [
        (registry.tasks, key)
        for key, value in list(registry.tasks.items())
        if own.owned(value.get("func"))
    ]
    owned_names = {key for _, key in items}
    items.extend(
        (registry.persistent_declarations, item)
        for item in list(registry.persistent_declarations)
        if item.plugin_name in owned_names
    )
    items.extend(
        (registry.ephemeral_declarations, item)
        for item in list(registry.ephemeral_declarations)
        if own.owned(item.func)
    )
    return items


def _asgi_routes(own: Ownership) -> list[Any]:
    with contextlib.suppress(Exception):
        app = nonebot.get_app()
        return [
            route
            for route in list(app.router.routes)
            if own.owned(
                getattr(route, "endpoint", None) or getattr(route, "app", None)
            )
        ]
    return []


def _remove_routes(items: list[Any]) -> None:
    app = nonebot.get_app()
    for route in items:
        if route in app.router.routes:
            app.router.routes.remove(route)
    app.openapi_schema = None


def _sub_managers(own: Ownership) -> list[Any]:
    return [
        manager
        for manager in list(_managers)
        if manager.controlled_modules
        and all(own.module_owned(m) for m in manager.controlled_modules.values())
    ]


def _remove_managers(items: list[Any]) -> None:
    for manager in items:
        while manager in _managers:
            _managers.remove(manager)


def _native_hooks(own: Ownership) -> list[Any]:
    from zhenxun.services.startup_load import startup_load_planner

    return [
        hook
        for hook in list(startup_load_planner._native_hooks)
        if hook.owner in own.plugin_ids or own.owned(hook.func)
    ]


def _remove_native_hooks(items: list[Any]) -> None:
    from zhenxun.services.startup_load import startup_load_planner

    hooks = startup_load_planner._native_hooks
    for hook in items:
        if hook in hooks:
            hooks.remove(hook)


def _kernel_registrations(own: Ownership) -> list[Any]:
    from zhenxun.services.lifecycle import lifecycle_kernel

    registrations = lifecycle_kernel._registrations
    return [
        (registrations, key)
        for key, registration in list(registrations.items())
        if own.closure_owned(registration.start) or own.closure_owned(registration.stop)
    ]


def _plugin_init(own: Ownership) -> list[Any]:
    from zhenxun.services.plugin_init import PluginInitManager

    return [
        (PluginInitManager.plugins, key)
        for key in list(PluginInitManager.plugins)
        if own.module_owned(key)
    ]


def _shop_goods(own: Ownership) -> list[Any]:
    from zhenxun.utils.decorator.shop import shop_register

    items: list[Any] = []
    for name, goods in list(shop_register._data.items()):
        if own.owned(goods.func):
            items.append(("goods", name, None))
            continue
        items.extend(
            ("hook", name, func)
            for func in [*goods.before_handle, *goods.after_handle]
            if own.owned(func)
        )
    return items


def _remove_shop_goods(items: list[Any]) -> None:
    from zhenxun.utils.decorator.shop import shop_register

    data = shop_register._data
    for kind, name, func in items:
        if kind == "goods":
            data.pop(name, None)
        elif (goods := data.get(name)) is not None:
            for handlers in (goods.before_handle, goods.after_handle):
                while func in handlers:
                    handlers.remove(func)


def _shop_manage_goods(own: Ownership) -> list[Any]:
    module = sys.modules.get("zhenxun.builtin_plugins.shop._data_source")
    if module is None:
        return []
    goods = module.ShopManage.uuid2goods
    return [(goods, key) for key, value in list(goods.items()) if own.owned(value.func)]


def _low_priority_writers(own: Ownership) -> list[Any]:
    from zhenxun.services import low_priority_writer

    writers = low_priority_writer._WRITERS
    return [
        (writers, name)
        for name, state in list(writers.items())
        if own.closure_owned(state.config.write_batch)
    ]


def _publication_reconcilers() -> list:
    module = sys.modules.get("zhenxun.services.db_context.write")
    return module._RECONCILERS if module is not None else []


ADAPTERS: list[Adapter] = [
    Adapter("matcher", _matchers, _remove_matchers),
    Adapter("alconna_command", _alconna_commands, _remove_alconna),
    _set_adapter("event_preprocessor", _message_set("_event_preprocessors")),
    _set_adapter("event_postprocessor", _message_set("_event_postprocessors")),
    _set_adapter("run_preprocessor", _message_set("_run_preprocessors")),
    _set_adapter("run_postprocessor", _message_set("_run_postprocessors")),
    _set_adapter("bot_connect", _driver_set("_bot_connection_hook")),
    _set_adapter("bot_disconnect", _driver_set("_bot_disconnection_hook")),
    _set_adapter("calling_api", _bot_set("_calling_api_hook")),
    _set_adapter("called_api", _bot_set("_called_api_hook")),
    _list_adapter("lifespan_startup", lambda: lifespan_funcs("startup")),
    _list_adapter("lifespan_ready", lambda: lifespan_funcs("ready")),
    _list_adapter("lifespan_shutdown", lambda: lifespan_funcs("shutdown")),
    Adapter("priority_lifecycle", _priority_lifecycle, _remove_priority_lifecycle),
    Adapter("apscheduler_job", _apscheduler_jobs, _remove_jobs),
    Adapter("scheduler_task", _scheduler_tasks, _remove_keyed),
    Adapter("asgi_route", _asgi_routes, _remove_routes),
    Adapter("plugin_manager", _sub_managers, _remove_managers),
    Adapter("native_hook", _native_hooks, _remove_native_hooks),
    Adapter("kernel_registration", _kernel_registrations, _remove_keyed),
    Adapter("plugin_init", _plugin_init, _remove_keyed),
    Adapter("shop_goods", _shop_goods, _remove_shop_goods),
    Adapter("shop_manage_goods", _shop_manage_goods, _remove_keyed),
    Adapter("low_priority_writer", _low_priority_writers, _remove_keyed),
    _list_adapter("publication_reconciler", _publication_reconcilers),
]


def collect(own: Ownership) -> dict[str, list[Any]]:
    """收集插件在各注册表中的登记，单个注册表读取失败时视为没有登记

    参数:
        own: 归属

    返回:
        dict[str, list[Any]]: 注册表名称到条目列表
    """
    result = {}
    for adapter in ADAPTERS:
        try:
            result[adapter.name] = adapter.collect(own)
        except (ImportError, AttributeError, AssertionError, ValueError):
            result[adapter.name] = []
        except Exception as error:
            result[adapter.name] = []
            logger.warning(f"读取插件登记失败: {adapter.name}", "插件热重载", e=error)
    return result


def remove(collected: dict[str, list[Any]], notes: list[str]) -> None:
    """按注册表移除已收集的登记，单个注册表移除失败时记录后继续

    参数:
        collected: collect 的结果
        notes: 过程记录
    """
    for adapter in ADAPTERS:
        if items := collected.get(adapter.name):
            try:
                adapter.remove(items)
            except Exception as error:
                notes.append(
                    f"registry_remove_failed:{adapter.name}:{type(error).__name__}"
                )
                logger.warning(
                    f"移除插件登记失败: {adapter.name}", "插件热重载", e=error
                )


def _task_files(task: asyncio.Task) -> list[str]:
    files = []
    coro: Any = task.get_coro()
    for _ in range(64):
        if coro is None:
            break
        code = getattr(coro, "cr_code", None) or getattr(coro, "gi_code", None)
        if code is not None:
            files.append(code.co_filename)
        coro = getattr(coro, "cr_await", None) or getattr(coro, "gi_yieldfrom", None)
    return files


def owned_tasks(own: Ownership) -> tuple[list[asyncio.Task], list[asyncio.Task]]:
    """找出正在执行插件代码的任务

    参数:
        own: 归属

    返回:
        tuple[list[asyncio.Task], list[asyncio.Task]]:
            插件自己的后台任务、正在处理事件且调用到插件代码的任务
    """
    current = asyncio.current_task()
    background, handling = [], []
    nonebot_message = os.path.normcase(sys.modules["nonebot.message"].__file__ or "")
    for task in asyncio.all_tasks():
        if task is current or task.done():
            continue
        files = [os.path.normcase(file) for file in _task_files(task)]
        if not any(own.file_owned(file) for file in files):
            continue
        if nonebot_message in files and not own.file_owned(files[0]):
            handling.append(task)
        else:
            background.append(task)
    return background, handling


def model_modules(own: Ownership) -> set[str]:
    """需要保留复用的插件模块：定义 Tortoise 模型的模块及其引用的插件内模块

    Tortoise 只初始化一次，模型类必须沿用；模型引用的枚举等对象也随之保留，
    避免新旧两份类型并存。
    """
    from tortoise.models import Model

    owned = set(own.modules())
    result = set()
    for name in owned:
        module = sys.modules.get(name)
        if module is not None and any(
            isinstance(value, type)
            and issubclass(value, Model)
            and value.__module__ == name
            for value in list(vars(module).values())
        ):
            result.add(name)
    pending = list(result)
    while pending:
        module = sys.modules.get(pending.pop())
        if module is None:
            continue
        for key, value in list(vars(module).items()):
            if key.startswith("__"):
                continue
            if isinstance(value, ModuleType):
                ref = value.__name__
            elif callable(value) or isinstance(value, type):
                ref = getattr(_unwrap(value), "__module__", None)
            else:
                ref = None
            if isinstance(ref, str) and ref in owned and ref not in result:
                result.add(ref)
                pending.append(ref)
    return result


def drop_modules(own: Ownership, keep: set[str]) -> int:
    """从 sys.modules 与父包属性中移除插件模块

    参数:
        own: 归属
        keep: 需要保留的模块

    返回:
        int: 移除的模块数
    """
    names = [name for name in own.modules() if name not in keep]
    for name in sorted(names, key=len, reverse=True):
        sys.modules.pop(name, None)
        parent, _, child = name.rpartition(".")
        if (
            parent
            and not own.module_owned(parent)
            and (module := sys.modules.get(parent)) is not None
            and getattr(module, child, None) is not None
        ):
            with contextlib.suppress(AttributeError):
                delattr(module, child)
    return len(names)


def clear_runtime_caches() -> None:
    """清理可能持有已卸载插件对象的类型缓存与定时器上下文"""
    import typing

    for cleanup in list(getattr(typing, "_cleanups", [])):
        cleanup()
    with contextlib.suppress(ImportError, AttributeError):
        from pydantic._internal import _generics

        _generics._GENERIC_TYPES_CACHE.clear()
    with contextlib.suppress(ImportError):
        from arclet.alconna import arparma

        for value in vars(arparma).values():
            if hasattr(value, "cache_clear"):
                value.cache_clear()
        # Alconna 在类变量中保存最近一次事件的 state，事件处理时会重新写入
        arparma.Arparma._additional.pop("state", None)
    # apscheduler 唤醒定时器会携带插件导入时的上下文，在空上下文中重建
    module = sys.modules.get("nonebot_plugin_apscheduler")
    if module is not None and module.scheduler.running:
        wakeup = getattr(type(module.scheduler).wakeup, "__wrapped__", None)
        if wakeup is not None:
            contextvars.Context().run(wakeup, module.scheduler)
