from fastapi import APIRouter, Depends, FastAPI
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse

from zhenxun.services.log import logger
from zhenxun.services.webui_resources import webui_resources
from zhenxun.utils.manager.zhenxun_repo_manager import ZhenxunRepoManager

from ..security import PrivateNetworkStaticFiles, require_private_request

router = APIRouter(dependencies=[Depends(require_private_request)])


@router.get("/")
async def index():
    snapshot = webui_resources.snapshot
    if not snapshot.ready:
        return HTMLResponse(
            '<meta charset="utf-8"><meta http-equiv="refresh" content="3">'
            "WebUI 资源正在更新，准备完成后会自动重试。",
            status_code=503,
            headers={"Cache-Control": "no-store"},
        )
    return HTMLResponse(snapshot.html, headers={"Cache-Control": "no-store"})


@router.get("/version.json")
async def version_manifest():
    return JSONResponse(
        webui_resources.snapshot.public(),
        headers={"Cache-Control": "no-store"},
    )


@router.get("/favicon.ico")
async def favicon():
    return FileResponse(
        ZhenxunRepoManager.config.WEBUI_PATH / "favicon.ico",
        headers={"Cache-Control": "no-cache"},
    )


async def init_public(app: FastAPI, context=None) -> bool:
    from zhenxun.services.webui_dev import worker_endpoint

    if endpoint := worker_endpoint():
        from .development import install_development

        if context is None:
            raise RuntimeError("WebUI development gateway requires lifecycle ownership")
        await install_development(app, context, endpoint)
        return True
    try:
        if not ZhenxunRepoManager.check_webui_exists():
            await ZhenxunRepoManager.webui_update(branch="dist")
        if context is None:
            raise RuntimeError("WebUI resource watcher requires lifecycle ownership")
        await webui_resources.start(context, ZhenxunRepoManager.config.WEBUI_PATH)
        folders = [
            x.name for x in ZhenxunRepoManager.config.WEBUI_PATH.iterdir() if x.is_dir()
        ]
        app.include_router(router)
        for pathname in folders:
            logger.debug(f"挂载文件夹: {pathname}")
            app.mount(
                f"/{pathname}",
                PrivateNetworkStaticFiles(
                    directory=ZhenxunRepoManager.config.WEBUI_PATH / pathname,
                    check_dir=True,
                ),
                name=f"public_{pathname}",
            )
        return (ZhenxunRepoManager.config.WEBUI_PATH / "index.html").is_file()
    except Exception as e:
        logger.error("初始化 WebUI资源 失败", "WebUI", e=e)
        return False
