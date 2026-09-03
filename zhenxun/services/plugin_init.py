from abc import ABC, abstractmethod
import asyncio
from collections.abc import Callable
from threading import RLock

import nonebot
from nonebot.utils import is_coroutine_callable
from pydantic import BaseModel

from zhenxun.services.log import logger
from zhenxun.utils.manager.priority_manager import PriorityLifecycle

driver = nonebot.get_driver()


class PluginInit(ABC):
    """
    插件安装与卸载模块
    """

    def __init_subclass__(cls, **kwargs):
        module_path = cls.__module__
        install_func = getattr(cls, "install", None)
        remove_func = getattr(cls, "remove", None)
        if install_func or remove_func:
            with PluginInitManager._registry_lock:
                PluginInitManager.plugins[module_path] = PluginInitData(
                    module_path=module_path,
                    install=install_func,
                    remove=remove_func,
                    class_=cls,
                )

    @abstractmethod
    async def install(self):
        raise NotImplementedError

    @abstractmethod
    async def remove(self):
        raise NotImplementedError


class PluginInitData(BaseModel):
    module_path: str
    """模块名"""
    install: Callable | None
    """安装方法"""
    remove: Callable | None
    """卸载方法"""
    class_: type[PluginInit]
    """类"""


class PluginInitManager:
    plugins: dict[str, PluginInitData] = {}  # noqa: RUF012
    _registry_lock = RLock()
    _operation_lock = asyncio.Lock()

    @classmethod
    def snapshot_modules(cls) -> list[str]:
        with cls._registry_lock:
            return list(cls.plugins)

    @classmethod
    def remove_registrations(cls, module_names: set[str]) -> None:
        with cls._registry_lock:
            for module_path in list(cls.plugins):
                if module_path in module_names or any(
                    module_path.startswith(f"{name}.") for name in module_names
                ):
                    cls.plugins.pop(module_path, None)

    @classmethod
    async def install_all(cls):
        """运行所有插件安装方法"""
        for module_path in cls.snapshot_modules():
            await cls.install(module_path)

    @classmethod
    async def install(cls, module_path: str, *, raise_on_error: bool = False):
        """运行指定插件安装方法"""
        async with cls._operation_lock:
            with cls._registry_lock:
                model = cls.plugins.get(module_path)
            if model and model.install:
                class_ = model.class_()
                try:
                    logger.debug(f"开始执行: {module_path}:install 方法")
                    if is_coroutine_callable(class_.install):
                        await class_.install()
                    else:
                        class_.install()  # type: ignore
                        logger.debug(f"执行: {module_path}:install 完成")
                except Exception as e:
                    logger.error(f"执行: {module_path}:install 失败", e=e)
                    if raise_on_error:
                        raise

    @classmethod
    async def remove(cls, module_path: str, *, raise_on_error: bool = False):
        """运行指定插件移除方法"""
        async with cls._operation_lock:
            with cls._registry_lock:
                model = cls.plugins.get(module_path)
            if model and model.remove:
                class_ = model.class_()
                try:
                    logger.debug(f"开始执行: {module_path}:remove 方法")
                    if is_coroutine_callable(class_.remove):
                        await class_.remove()
                    else:
                        class_.remove()  # type: ignore
                        logger.debug(f"执行: {module_path}:remove 完成")
                except Exception as e:
                    logger.error(f"执行: {module_path}:remove 失败", e=e)
                    if raise_on_error:
                        raise


@PriorityLifecycle.on_startup(priority=5)
async def _():
    await PluginInitManager.install_all()
