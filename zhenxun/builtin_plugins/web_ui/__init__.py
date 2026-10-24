import asyncio
import os
import secrets
import sys

from fastapi import APIRouter, FastAPI
import nonebot
from nonebot.plugin import PluginMetadata

from zhenxun.configs.config import Config as gConfig
from zhenxun.configs.utils import PluginExtraData, RegisterConfig
from zhenxun.configs.webui_tls import current_webui_scheme
from zhenxun.services.log import logger
from zhenxun.services.startup import startup_coordinator
from zhenxun.utils.enum import PluginType
from zhenxun.utils.manager.priority_manager import PriorityLifecycle
from zhenxun.utils.network import emit_webui_console_banner

from .api.configure import router as configure_router
from .api.configure.persistence import ensure_webui_secret
from .api.configure.setup_access import setup_access
from .api.logs import router as ws_log_routes
from .api.menu import router as menu_router
from .api.protocol import router as protocol_router
from .api.tabs.ai import router as ai_router
from .api.tabs.backup import file_router as backup_file_router
from .api.tabs.backup import router as backup_router
from .api.tabs.backup import setup_router as backup_setup_router
from .api.tabs.dashboard import router as dashboard_router
from .api.tabs.database import router as database_router
from .api.tabs.main import router as main_router
from .api.tabs.main import ws_router as status_routes
from .api.tabs.manage import router as manage_router
from .api.tabs.manage.chat import ws_router as chat_routes
from .api.tabs.network_proxy import router as network_proxy_router
from .api.tabs.plugin_manage import router as plugin_router
from .api.tabs.plugin_manage.nonebot_store import router as nonebot_store_router
from .api.tabs.plugin_manage.store import router as store_router
from .api.tabs.plugin_policy import router as plugin_policy_router
from .api.tabs.system import router as system_router
from .auth import router as auth_router
from .config import install_cors_middleware
from .console_access import console_access
from .public import init_public
from .ready_banner import webui_ready_banner
from .security import bind_lifecycle_context

__plugin_meta__ = PluginMetadata(
    name="WebUi",
    description="WebUi API",
    usage='"""\n    """.strip(),',
    extra=PluginExtraData(
        author="HibiKier",
        version="0.1",
        plugin_type=PluginType.HIDDEN,
        configs=[
            RegisterConfig(
                module="web-ui",
                key="username",
                value="admin",
                help="前端管理用户名",
                type=str,
                default_value="admin",
            ),
            RegisterConfig(
                module="web-ui",
                key="password",
                value=None,
                help="前端管理密码",
                type=str,
                default_value=None,
            ),
            RegisterConfig(
                module="web-ui",
                key="secret",
                value=secrets.token_urlsafe(32),
                help="JWT密钥",
                type=str,
                default_value=None,
            ),
        ],
    ).to_dict(),
)

driver = nonebot.get_driver()
install_cors_middleware()


gConfig.set_name("web-ui", "web-ui")


BaseApiRouter = APIRouter(prefix="/zhenxun/api")


BaseApiRouter.include_router(auth_router)
BaseApiRouter.include_router(store_router)
BaseApiRouter.include_router(nonebot_store_router)
BaseApiRouter.include_router(dashboard_router)
BaseApiRouter.include_router(ai_router)
BaseApiRouter.include_router(main_router)
BaseApiRouter.include_router(manage_router)
BaseApiRouter.include_router(database_router)
BaseApiRouter.include_router(plugin_router)
BaseApiRouter.include_router(plugin_policy_router)
BaseApiRouter.include_router(network_proxy_router)
BaseApiRouter.include_router(backup_router)
BaseApiRouter.include_router(backup_file_router)
BaseApiRouter.include_router(backup_setup_router)
BaseApiRouter.include_router(system_router)
BaseApiRouter.include_router(menu_router)
BaseApiRouter.include_router(configure_router)
BaseApiRouter.include_router(protocol_router)

WsApiRouter = APIRouter(prefix="/zhenxun/socket")

WsApiRouter.include_router(ws_log_routes)
WsApiRouter.include_router(status_routes)
WsApiRouter.include_router(chat_routes)


@PriorityLifecycle.on_startup(
    priority=0,
    stage="management",
    timeout=20,
    component_id="management:webui",
    depends_on=("management:runtime_concurrency",),
    pass_context=True,
)
async def _(context):
    from .api.tabs.dashboard.overview import start_probes

    start_probes()
    bind_lifecycle_context(context)
    try:
        await asyncio.to_thread(ensure_webui_secret)
        app: FastAPI = nonebot.get_app()
        app.include_router(BaseApiRouter)
        app.include_router(WsApiRouter)
        public_ready = await init_public(app)
        logger.info("<g>API启动成功</g>", "WebUi")

        async def emit_ready_banner() -> None:
            startup_coordinator.mark_server_bound()
            if not public_ready:
                return
            available = await startup_coordinator.wait_final_available()
            from zhenxun.services.startup_banner import ready_summary

            sys.stderr.write(ready_summary(startup_coordinator.snapshot()))
            sys.stderr.flush()
            if not available:
                return
            try:
                connection_code = await console_access.prepare()
                if not connection_code:
                    return
                if not os.getenv("ZHENXUN_LAUNCHER_PID"):
                    from zhenxun.services.update.update_service import (
                        finalize_applied_update,
                    )

                    finalize_applied_update()
                from zhenxun.configs.webui_tls import runtime_webui_settings
                from zhenxun.services.webui_http import http_compat_state

                tls_settings = runtime_webui_settings()
                emit_webui_console_banner(
                    str(driver.config.host),
                    int(driver.config.port),
                    connection_code=connection_code,
                    state=setup_access.state(),
                    username=str(gConfig.get_config("web-ui", "username", "")),
                    scheme=current_webui_scheme(),
                    http_compatibility_port=(
                        tls_settings.redirect_port
                        if http_compat_state().get("state") == "ready"
                        else None
                    ),
                )
            except Exception as e:
                logger.error("WebUI 启动链接输出失败", "WebUi", e=e)

        webui_ready_banner.arm(
            emit_ready_banner,
            host=str(driver.config.host),
            port=int(driver.config.port),
            context=context,
        )
        if not public_ready:
            logger.error("WebUI 静态资源未就绪，未输出访问链接", "WebUi")
    except Exception as e:
        logger.error("<g>API启动失败</g>", "WebUi", e=e)
        raise


@PriorityLifecycle.on_startup(
    priority=1,
    stage="management",
    timeout=5,
    component_id="management:webui-http",
    depends_on=("management:webui",),
    pass_context=True,
)
async def _start_http_compat(context):
    from zhenxun.configs.webui_tls import runtime_webui_settings
    from zhenxun.services.webui_http import webui_http_compat

    await webui_http_compat.start(context, nonebot.get_app(), runtime_webui_settings())


@PriorityLifecycle.on_shutdown(priority=1000, component_id="management:webui")
async def _cleanup_ready_banner():
    from .api.tabs.dashboard.overview import invalidate_probes

    await invalidate_probes(stopping=True)
    bind_lifecycle_context(None)
    webui_ready_banner.reset()
