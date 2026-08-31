from nonebot.permission import SUPERUSER
from nonebot.plugin import PluginMetadata
from nonebot.rule import to_me
from nonebot_plugin_alconna import Alconna, Arparma, on_alconna
from nonebot_plugin_session import EventSession

from zhenxun.configs.config import Config
from zhenxun.configs.utils import PluginExtraData, RegisterConfig
from zhenxun.services.log import logger
from zhenxun.services.runtime_config_reload import reload_runtime_config
from zhenxun.utils.enum import PluginType
from zhenxun.utils.manager.priority_manager import PriorityLifecycle
from zhenxun.utils.message import MessageUtils

AUTO_RELOAD_JOB_ID = "zhenxun.reload_setting.auto_reload"

__plugin_meta__ = PluginMetadata(
    name="重载配置",
    description="重新加载config.yaml",
    usage="""
    重载配置
    """.strip(),
    extra=PluginExtraData(
        author="HibiKier",
        version="0.1",
        plugin_type=PluginType.SUPERUSER,
        configs=[
            RegisterConfig(
                key="AUTO_RELOAD",
                value=False,
                help="自动重载配置文件",
                default_value=False,
                type=bool,
            ),
            RegisterConfig(
                key="AUTO_RELOAD_TIME",
                value=180,
                help="自动重载配置文件时长",
                default_value=180,
                type=int,
            ),
        ],
    ).to_dict(),
)

_matcher = on_alconna(
    Alconna(
        "重载配置",
    ),
    rule=to_me(),
    permission=SUPERUSER,
    priority=1,
    block=True,
)


def _get_auto_reload_interval() -> int:
    value = Config.get_config("reload_setting", "AUTO_RELOAD_TIME", 180)
    try:
        seconds = int(value)
    except (TypeError, ValueError):
        logger.warning(
            f"AUTO_RELOAD_TIME 配置无效: {value!r}，已使用默认值 180 秒",
            "重载配置",
        )
        return 180
    if seconds <= 0:
        logger.warning(
            f"AUTO_RELOAD_TIME 配置小于等于 0: {seconds}，已使用默认值 180 秒",
            "重载配置",
        )
        return 180
    return seconds


async def _reload_runtime_config() -> None:
    await reload_runtime_config()


@PriorityLifecycle.on_startup(priority=1)
def _init_auto_reload_job() -> None:
    if Config.get_config("reload_setting", "AUTO_RELOAD"):
        logger.info(
            "AUTO_RELOAD 已由文件监听接管，旧定时轮询配置不再生效",
            "重载配置",
        )


@_matcher.handle()
async def _(session: EventSession, arparma: Arparma):
    await _reload_runtime_config()
    logger.debug("自动重载配置文件", arparma.header_result, session=session)
    await MessageUtils.build_message("重载完成!").send(reply_to=True)
