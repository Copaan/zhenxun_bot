from fastapi import APIRouter, Depends, FastAPI
from fastapi.responses import FileResponse

from zhenxun.services.log import logger
from zhenxun.utils.manager.zhenxun_repo_manager import ZhenxunRepoManager

from ..security import PrivateNetworkStaticFiles, require_private_request

router = APIRouter(dependencies=[Depends(require_private_request)])


@router.get("/")
async def index():
    return FileResponse(
        ZhenxunRepoManager.config.WEBUI_PATH / "index.html",
        headers={"Cache-Control": "no-store"},
    )


@router.get("/version.json")
async def version_manifest():
    return FileResponse(
        ZhenxunRepoManager.config.WEBUI_PATH / "version.json",
        headers={"Cache-Control": "no-store"},
    )


@router.get("/favicon.ico")
async def favicon():
    return FileResponse(ZhenxunRepoManager.config.WEBUI_PATH / "favicon.ico")


async def init_public(app: FastAPI) -> bool:
    try:
        if not ZhenxunRepoManager.check_webui_exists():
            await ZhenxunRepoManager.webui_update(branch="dist")
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
