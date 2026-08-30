from fastapi import APIRouter
from fastapi.responses import JSONResponse
import nonebot

from zhenxun.adapters.qq_official.config import QQOfficialConfig
from zhenxun.configs.config import BotConfig

from ...base_model import Result
from ...utils import authentication
from .model import ProtocolConnection, ProtocolStatus

router = APIRouter(prefix="/protocol")


def _platform(adapter_name: str) -> str:
    normalized = adapter_name.strip().lower()
    if normalized == "onebot v11":
        return "onebot_v11"
    if normalized == "qq":
        return "qq_official"
    return "other"


def build_protocol_status() -> ProtocolStatus:
    connections: list[ProtocolConnection] = []
    for bot in nonebot.get_bots().values():
        adapter_name = str(bot.adapter.get_name())
        connections.append(
            ProtocolConnection(
                self_id=str(bot.self_id),
                adapter=adapter_name,
                platform=_platform(adapter_name),
            )
        )
    connections.sort(key=lambda item: (item.platform, item.self_id))
    qq_config = nonebot.get_plugin_config(QQOfficialConfig)
    platforms = {item.platform for item in connections}
    return ProtocolStatus(
        onebot_v11_connected="onebot_v11" in platforms,
        qq_official_enabled=BotConfig.qq_adapter_load,
        qq_official_connected="qq_official" in platforms,
        qq_webhook_mode=qq_config.qq_webhook_mode,
        connections=connections,
    )


@router.get(
    "/status",
    dependencies=[authentication()],
    response_model=Result[ProtocolStatus],
    response_class=JSONResponse,
    description="获取协议端连接状态",
)
async def _() -> Result[ProtocolStatus]:
    return Result.ok(build_protocol_status())
