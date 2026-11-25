from datetime import datetime, timezone
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
import platform

import aiofiles
import nonebot
from nonebot.plugin import PluginMetadata
from nonebot.rule import to_me
from nonebot_plugin_alconna import Alconna, Arparma, on_alconna
from nonebot_plugin_uninfo import Uninfo
from tortoise import Tortoise

from zhenxun import ui
from zhenxun.configs.config import BotConfig
from zhenxun.configs.path_config import DATA_PATH
from zhenxun.configs.utils import PluginCdBlock, PluginExtraData
from zhenxun.services.help_service import is_night_mode
from zhenxun.services.log import logger
from zhenxun.services.startup import startup_coordinator
from zhenxun.utils.message import MessageUtils
from zhenxun.utils.platform import PlatformUtils

__plugin_meta__ = PluginMetadata(
    name="关于",
    description="想要更加了解真寻吗",
    usage="""
    指令：
        关于
    """.strip(),
    extra=PluginExtraData(
        author="HibiKier",
        version="0.2",
        menu_type="其他",
        limits=[PluginCdBlock(cd=10, result="每10秒只能查看一次哦~")],
    ).to_dict(),
)


_matcher = on_alconna(Alconna("关于"), priority=5, block=True, rule=to_me())

INTRO = (
    "基于 NoneBot2 开发，支持多平台，是一个非常可爱的 Bot 呀，希望与大家要好好相处～"
)

QQ_INFO = """
『绪山真寻Bot』
版本：{version}
简介：基于Nonebot2开发，支持多平台，是一个非常可爱的Bot呀，希望与大家要好好相处
""".strip()

INFO = """
『绪山真寻Bot』
版本：{version}
简介：基于Nonebot2开发，支持多平台，是一个非常可爱的Bot呀，希望与大家要好好相处
项目地址：https://github.com/zhenxun-org/zhenxun_bot
文档地址：https://zhenxun-org.github.io/zhenxun_bot/
""".strip()

DIALECTS = {"sqlite": "SQLite", "mysql": "MySQL", "postgres": "PostgreSQL"}


async def get_version() -> str:
    """读取真寻版本号

    返回:
        str: 版本号，读取不到时为空字符串
    """
    ver_file = Path() / "__version__"
    if not ver_file.exists():
        return ""
    async with aiofiles.open(ver_file, encoding="utf8") as f:
        text = await f.read()
    return text.split(":")[-1].strip()


def format_uptime(now: datetime | None = None) -> str:
    """格式化运行时长

    参数:
        now: 计算用的当前时间，默认为现在

    返回:
        str: 如 3 天 4 小时、2 小时 5 分、7 分钟
    """
    started = datetime.fromisoformat(startup_coordinator.started_at)
    seconds = int(((now or datetime.now(timezone.utc)) - started).total_seconds())
    days, rest = divmod(max(seconds, 0), 86400)
    hours, rest = divmod(rest, 3600)
    minutes = rest // 60
    if days:
        return f"{days} 天 {hours} 小时"
    if hours:
        return f"{hours} 小时 {minutes} 分"
    return f"{minutes} 分钟"


def get_stats() -> list[tuple[str, str]]:
    """收集关于页面展示的运行信息

    返回:
        list[tuple[str, str]]: (名称, 值) 列表
    """
    try:
        nonebot_version = version("nonebot2")
    except PackageNotFoundError:
        nonebot_version = "-"
    try:
        dialect = Tortoise.get_connection("default").capabilities.dialect
    except Exception:
        dialect = ""
    adapters = " / ".join(nonebot.get_adapters()) or "-"
    return [
        ("运行时长", format_uptime()),
        ("已加载插件", f"{len(nonebot.get_loaded_plugins())} 个"),
        ("适配器", adapters),
        ("NoneBot", nonebot_version),
        ("Python", platform.python_version()),
        ("数据库", DIALECTS.get(dialect, dialect or "-")),
    ]


async def build_about_image(ver: str) -> bytes:
    """渲染关于图片

    参数:
        ver: 版本号

    返回:
        bytes: 图片数据
    """
    data = {
        "nickname": BotConfig.self_nickname or "绪山真寻",
        "version": ver or "-",
        "intro": INTRO,
        "stats": get_stats(),
        "night": is_night_mode(),
    }
    return await ui.render_template(
        "pages/builtin/about",
        data=data,
        clip_selector=".page",
        clip_padding=0,
        disable_animations=True,
    )


@_matcher.handle()
async def _(session: Uninfo, arparma: Arparma):
    ver = await get_version()
    is_qbot = PlatformUtils.is_qbot(session)
    result: list[str | Path | bytes] = []
    try:
        result.append(await build_about_image(ver))
    except Exception as e:
        logger.error("关于图片渲染失败", arparma.header_result, session=session, e=e)
        result.append((QQ_INFO if is_qbot else INFO).format(version=ver))
    if is_qbot and (path := DATA_PATH / "about.png").exists():
        result.append(path)
    await MessageUtils.build_message(result).send()  # type: ignore
    logger.info("查看关于", arparma.header_result, session=session)
