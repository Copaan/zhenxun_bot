import asyncio
from html import escape
import json
from pathlib import Path
import re

from fastapi import APIRouter, FastAPI
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse

from zhenxun.services.log import logger
from zhenxun.services.update import update_service
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
        self.app: FastAPI | None = None
        self._mounts: list = []
        self._lock = asyncio.Lock()

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
            self.ready = not missing and update_service.webui_resources_compatible(
                self.root
            )
            self.html = html
            self.manifest = manifest if isinstance(manifest, dict) else {}
        except (OSError, ValueError):
            self.ready = False
            self.html = ""
            self.manifest = {}

    async def refresh(self) -> "WebUIResources":
        """入口文件变化时重新读取入口页与版本信息

        返回:
            WebUIResources: 自身
        """
        async with self._lock, update_service.webui_publication_lock:
            stamp = self._entry_stamp()
            if stamp != self._stamp or not self.ready:
                self._stamp = stamp
                await asyncio.to_thread(self._load)
                self._mount_resources()
        return self

    def _mount_resources(self) -> None:
        """按已发布资源重新挂载静态目录，不覆盖 API 路由。"""
        if self.app is None:
            return
        self.app.router.routes[:] = [
            route for route in self.app.router.routes if route not in self._mounts
        ]
        self._mounts = []
        if not self.ready:
            return
        for directory in self.root.iterdir():
            if not directory.is_dir() or directory.name.startswith("."):
                continue
            self.app.mount(
                f"/{directory.name}",
                WebUIStaticFiles(directory=directory, check_dir=False),
                name=f"public_{directory.name}",
            )
            self._mounts.append(self.app.router.routes[-1])

    async def published(self) -> None:
        """发布后刷新入口缓存和静态目录映射。"""
        self._stamp = None
        await self.refresh()

    async def start(self, root: Path) -> None:
        """设置资源目录并读取入口页

        参数:
            root: 资源目录
        """
        self.root = root
        self._stamp = None
        await self.refresh()

    def public(self) -> dict:
        manifest = {
            key: self.manifest.get(key)
            for key in ("version", "commit", "revision", "built_at", "api_version")
        }
        return {
            **manifest,
            "resources_ready": self.ready,
            "startup_update": update_service.webui_startup_status(),
        }


webui_resources = WebUIResources()


@router.get("/")
async def index():
    resources = await webui_resources.refresh()
    if not resources.ready:
        status = update_service.webui_startup_status()
        reason = escape(str(status.get("reason") or "正在准备新版管理页面。"))
        retry_at = escape(str(status.get("next_retry_at") or ""))
        phase = {
            "queued": "等待更新",
            "preparing": "检查发布版本",
            "downloading": "下载新版资源",
            "validating": "校验资源完整性",
            "publishing": "切换新版资源",
            "waiting_retry": "等待下次重试",
            "completed": "刷新页面资源",
        }.get(status.get("state"), "准备更新")
        return HTMLResponse(
            '<meta charset="utf-8"><meta http-equiv="refresh" content="3">'
            '<meta name="viewport" content="width=device-width, initial-scale=1">'
            '<title>WebUI 更新</title><main style="max-width:42rem;margin:10vh auto;'
            'padding:24px;font-family:sans-serif;overflow-wrap:anywhere">'
            f"<h1>WebUI 正在更新</h1><p>当前阶段：{phase}</p><p>{reason}</p>"
            f"<p>下次自动尝试：{retry_at or '本轮正在处理'}</p>"
            "<p>准备完成后本页自动加载新版，无需重新启动 Bot。</p></main>",
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
        webui_resources.app = app
        await webui_resources.start(ZhenxunRepoManager.config.WEBUI_PATH)
        app.include_router(router)
        update_service.start_webui_update(webui_resources.published)
        return True
    except Exception as e:
        logger.error("初始化 WebUI资源 失败", "WebUI", e=e)
        return False
