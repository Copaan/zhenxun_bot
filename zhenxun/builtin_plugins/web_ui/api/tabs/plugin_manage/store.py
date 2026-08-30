from fastapi import APIRouter
from fastapi.responses import JSONResponse
from nonebot import require
from nonebot.compat import model_dump

from zhenxun.services.log import logger

from ....base_model import Result
from ....utils import authentication
from .model import PluginIr

router = APIRouter(prefix="/store")


@router.get(
    "/get_plugin_store",
    dependencies=[authentication()],
    response_model=Result[dict],
    response_class=JSONResponse,
    description="获取插件商店插件信息",  # type: ignore
)
async def _() -> Result[dict]:
    try:
        require("plugin_store")
        from zhenxun.builtin_plugins.plugin_store import StoreManager

        official_plugins, community_plugins = await StoreManager.get_data()
        installed_plugins = await StoreManager.get_installed_plugins()
        plugin_list = []
        for idx, (plugin, source) in enumerate(
            [(item, "official") for item in official_plugins]
            + [(item, "community") for item in community_plugins]
        ):
            installed_version = installed_plugins.get(plugin.module)
            plugin_list.append(
                {
                    **model_dump(plugin),
                    "name": plugin.name,
                    "id": idx,
                    "source": source,
                    "installed": installed_version is not None,
                    "installed_version": installed_version,
                    "update_available": bool(
                        installed_version is not None
                        and str(installed_version) != str(plugin.version)
                    ),
                }
            )
        return Result.ok(
            {
                "install_module": list(installed_plugins),
                "plugin_list": plugin_list,
            }
        )
    except Exception as e:
        logger.error("获取插件商店插件信息失败", "WebUi", e=e)
        return Result.fail(f"获取插件商店插件信息失败: {type(e)}: {e}")


@router.post(
    "/install_plugin",
    dependencies=[authentication()],
    response_model=Result,
    response_class=JSONResponse,
    description="安装插件",  # type: ignore
)
async def _(param: PluginIr) -> Result:
    try:
        require("plugin_store")
        from zhenxun.builtin_plugins.plugin_store import StoreManager

        result = await StoreManager.add_plugin(str(param.id))  # type: ignore
        logger.info(result.replace("\n", "；"), "插件商店")
        return Result.ok(info=result)
    except Exception as e:
        return Result.fail(f"安装插件失败: {type(e)}: {e}")


@router.post(
    "/update_plugin",
    dependencies=[authentication()],
    response_model=Result,
    response_class=JSONResponse,
    description="更新插件",  # type: ignore
)
async def _(param: PluginIr) -> Result:
    try:
        require("plugin_store")
        from zhenxun.builtin_plugins.plugin_store import StoreManager

        result = await StoreManager.update_plugin(str(param.id))  # type: ignore
        return Result.ok(info=result)
    except Exception as e:
        return Result.fail(f"更新插件失败: {type(e)}: {e}")


@router.post(
    "/remove_plugin",
    dependencies=[authentication()],
    response_model=Result,
    response_class=JSONResponse,
    description="移除插件",  # type: ignore
)
async def _(param: PluginIr) -> Result:
    try:
        require("plugin_store")
        from zhenxun.builtin_plugins.plugin_store import StoreManager

        result = await StoreManager.remove_plugin(str(param.id))  # type: ignore
        return Result.ok(info=result)
    except Exception as e:
        return Result.fail(f"移除插件失败: {type(e)}: {e}")
