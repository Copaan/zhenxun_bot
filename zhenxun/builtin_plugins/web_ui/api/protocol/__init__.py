from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse
import nonebot

from zhenxun.adapters.qq_official.config import QQOfficialConfig
from zhenxun.adapters.qq_official.diagnostics import (
    connection_diagnostic,
    public_identity,
    safe_avatar_url,
)
from zhenxun.configs.config import BotConfig

from ...base_model import Result
from ...utils import authentication
from .configuration import router as configuration_router
from .model import (
    ProtocolConnection,
    ProtocolQQBotStatus,
    ProtocolQQError,
    ProtocolStatus,
)
from .qq_registration import router as qq_registration_router

router = APIRouter(prefix="/protocol")
router.include_router(configuration_router)
router.include_router(qq_registration_router)


def _platform(adapter_name: str) -> str:
    normalized = adapter_name.strip().lower()
    if normalized == "onebot v11":
        return "onebot_v11"
    if normalized == "qq":
        return "qq_official"
    return "other"


def build_protocol_status() -> ProtocolStatus:
    connections: list[ProtocolConnection] = []
    qq_runtime_bots: dict[str, object] = {}
    for bot in nonebot.get_bots().values():
        adapter_name = str(bot.adapter.get_name())
        platform = _platform(adapter_name)
        try:
            self_info = getattr(bot, "self_info", None)
        except Exception:
            self_info = None
        nickname = getattr(self_info, "username", None)
        avatar_url = safe_avatar_url(getattr(self_info, "avatar", None))
        if platform == "qq_official":
            qq_runtime_bots[str(bot.self_id)] = bot
        connections.append(
            ProtocolConnection(
                self_id=str(bot.self_id),
                adapter=adapter_name,
                platform=platform,
                nickname=str(nickname) if nickname else None,
                avatar_url=avatar_url,
            )
        )
    connections.sort(key=lambda item: (item.platform, item.self_id))
    qq_config = nonebot.get_plugin_config(QQOfficialConfig)
    platforms = {item.platform for item in connections}
    qq_bots: list[ProtocolQQBotStatus] = []
    if BotConfig.qq_adapter_load:
        for configured_bot in qq_config.qq_bots:
            app_id = str(configured_bot.id)
            runtime_bot = qq_runtime_bots.get(app_id)
            try:
                self_info = getattr(runtime_bot, "self_info", None)
            except Exception:
                self_info = None
            diagnostic = connection_diagnostic(app_id)
            cached_identity = public_identity(app_id)
            connected = runtime_bot is not None
            error = (
                ProtocolQQError(**diagnostic.error.to_dict())
                if diagnostic and diagnostic.error
                else None
            )
            qq_bots.append(
                ProtocolQQBotStatus(
                    app_id=app_id,
                    bot_id=(
                        str(getattr(self_info, "id", "") or "")
                        or (cached_identity.bot_id if cached_identity else None)
                    ),
                    username=(
                        str(getattr(self_info, "username", "") or "")
                        or (cached_identity.username if cached_identity else None)
                    ),
                    avatar_url=(
                        safe_avatar_url(getattr(self_info, "avatar", None))
                        or (cached_identity.avatar_url if cached_identity else None)
                    ),
                    mode=("websocket" if configured_bot.use_websocket else "webhook"),
                    state=(
                        "connected"
                        if connected
                        else diagnostic.state
                        if diagnostic
                        else "connecting"
                    ),
                    connected=connected,
                    updated_at=diagnostic.updated_at if diagnostic else None,
                    error=error,
                )
            )
    return ProtocolStatus(
        onebot_v11_connected="onebot_v11" in platforms,
        qq_official_enabled=BotConfig.qq_adapter_load,
        qq_official_connected="qq_official" in platforms,
        qq_webhook_mode=qq_config.qq_webhook_mode,
        qq_webhook_callback_url=(
            f"{qq_config.qq_webhook_public_base_url.rstrip('/')}/qq/webhook"
            if qq_config.qq_webhook_public_base_url
            else None
        ),
        connections=connections,
        qq_bots=qq_bots,
    )


@router.get(
    "/status",
    dependencies=[authentication()],
    response_model=Result[ProtocolStatus],
    response_class=JSONResponse,
    description="获取协议端连接状态",
)
async def _(request: Request) -> Result[ProtocolStatus]:
    from zhenxun.services.onebot_endpoint import current_reverse_ws_diagnostic

    status = build_protocol_status()
    status.onebot_endpoint = current_reverse_ws_diagnostic(request.url.hostname or "")
    from zhenxun.services.qq_ingress_state import read_ingress_state

    status.qq_webhook_ingress = (
        read_ingress_state()
        if status.qq_webhook_mode == "builtin_https" and status.qq_official_enabled
        else {"state": "unknown" if status.qq_official_enabled else "disabled"}
    )
    return Result.ok(status)
