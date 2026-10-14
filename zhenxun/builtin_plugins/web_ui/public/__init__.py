import asyncio
import json
from pathlib import Path
import re

from fastapi import APIRouter, FastAPI
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse

from zhenxun.services.log import logger
from zhenxun.utils.manager.zhenxun_repo_manager import ZhenxunRepoManager

from ..security import WebUIStaticFiles

router = APIRouter()
_ENTRY_FILES = ("index.html", "version.json")
_LOCAL_REFERENCE = re.compile(r"""(?:src|href)=["']?(/[^"'\s>]+)""", re.I)


class WebUIResources:
    """已发布的 WebUI 入口页与版本信息，入口文件变化时重新读取"""

    def __init__(self) -> None:
        self.root = Path("data/web_ui/public")
        self.ready = False
        self.html = ""
        self.manifest: dict = {}
        self._stamp: tuple | None = None

    def _entry_stamp(self) -> tuple:
        stamp = []
        for name in _ENTRY_FILES:
            try:
                stat = (self.root / name).stat()
                stamp.append((stat.st_mtime_ns, stat.st_size))
            except OSError:
                stamp.append(None)
        return tuple(stamp)

    def _load(self) -> None:
        try:
            html = (self.root / "index.html").read_text("utf-8-sig")
            try:
                manifest = json.loads((self.root / "version.json").read_bytes())
            except FileNotFoundError:
                manifest = {}
            missing = [
                ref
                for ref in _LOCAL_REFERENCE.findall(html)
                if not ref.startswith("//")
                and not (self.root / ref.split("?", 1)[0].lstrip("/")).is_file()
            ]
            self.ready = not missing
            self.html = html
            self.manifest = manifest if isinstance(manifest, dict) else {}
        except (OSError, ValueError):
            self.ready = False

    async def refresh(self) -> "WebUIResources":
        """入口文件变化时重新读取入口页与版本信息

        返回:
            WebUIResources: 自身
        """
        stamp = self._entry_stamp()
        if stamp != self._stamp or not self.ready:
            self._stamp = stamp
            await asyncio.to_thread(self._load)
        return self

    async def start(self, root: Path) -> None:
        """设置资源目录并读取入口页

        参数:
            root: 资源目录
        """
        self.root = root
        self._stamp = None
        await self.refresh()

    def public(self) -> dict:
        return {**self.manifest, "resources_ready": self.ready}


webui_resources = WebUIResources()


@router.get("/")
async def index():
    resources = await webui_resources.refresh()
    if not resources.ready:
        return HTMLResponse(
            '<meta charset="utf-8"><meta http-equiv="refresh" content="3">'
            "WebUI 资源正在更新，准备完成后会自动重试。",
            status_code=503,
            headers={"Cache-Control": "no-store"},
        )
    return HTMLResponse(resources.html, headers={"Cache-Control": "no-store"})


@router.get("/version.json")
async def version_manifest():
    return JSONResponse(
        (await webui_resources.refresh()).public(),
        headers={"Cache-Control": "no-store"},
    )


@router.get("/favicon.ico")
async def favicon():
    return FileResponse(
        ZhenxunRepoManager.config.WEBUI_PATH / "favicon.ico",
        headers={"Cache-Control": "no-cache"},
    )


async def init_public(app: FastAPI) -> bool:
    try:
        if not ZhenxunRepoManager.check_webui_exists():
            await ZhenxunRepoManager.webui_update(branch="dist")
        await webui_resources.start(ZhenxunRepoManager.config.WEBUI_PATH)
        folders = [
            x.name for x in ZhenxunRepoManager.config.WEBUI_PATH.iterdir() if x.is_dir()
        ]
        app.include_router(router)
        for pathname in folders:
            logger.debug(f"挂载文件夹: {pathname}")
            app.mount(
                f"/{pathname}",
                WebUIStaticFiles(
                    directory=ZhenxunRepoManager.config.WEBUI_PATH / pathname,
                    check_dir=True,
                ),
                name=f"public_{pathname}",
            )
        return (ZhenxunRepoManager.config.WEBUI_PATH / "index.html").is_file()
    except Exception as e:
        logger.error("初始化 WebUI资源 失败", "WebUI", e=e)
        return False
