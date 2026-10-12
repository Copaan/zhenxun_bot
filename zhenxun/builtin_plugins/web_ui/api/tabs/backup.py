import asyncio
from urllib.parse import urlsplit

from fastapi import APIRouter, UploadFile
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel

from zhenxun.configs.webui_tls import WebUITLSConfigError, settings_from_values
from zhenxun.services.migration import backup, restore

from ...base_model import Result
from ...restart_service import preferred_access_urls, request_webui_restart
from ...utils import authentication
from ..configure.data_source import build_database_url, probe_database
from ..configure.model import DatabaseConfig

router = APIRouter(prefix="/backup", dependencies=[authentication()])


class ExportPayload(BaseModel):
    categories: list[str] = list(backup.DEFAULT_CATEGORIES)
    """备份的文件分类"""
    extras: list[str] = []
    """一并带上的未纳管依赖包名"""
    password: str | None = None
    """加密密码"""


class InspectPayload(BaseModel):
    name: str
    """备份包文件名"""
    password: str | None = None
    """加密备份的密码"""


class RestorePayload(InspectPayload):
    categories: list[str]
    """恢复的文件分类"""
    skip_failed: bool = False
    """依赖无法安装时跳过对应插件继续"""


def _restored_access(preview: dict | None) -> dict | None:
    """备份中的配置恢复后 WebUI 的访问地址与管理员账号

    参数:
        preview: restore.configuration_preview 的结果

    返回:
        dict | None: access_urls 为访问地址（无法确定时为空），username 为管理员账号
    """
    if preview is None:
        return None
    urls = []
    if preview["environment"]:
        try:
            settings = settings_from_values(preview["environment"])
            urls = preferred_access_urls(
                settings.host, settings.port, settings=settings, sidecar_available=True
            )
        except (WebUITLSConfigError, ValueError):
            pass
    return {"access_urls": urls, "username": preview["username"]}


class _Restart:
    """调用 WebUI 重启并保留重启信息，供前端等待新进程

    恢复配置时，新进程按备份中的配置监听，重启信息中的访问地址改为备份中的地址
    """

    def __init__(self, categories: list[str] | None = None):
        self.categories = categories or []
        self.data: dict = {}

    async def __call__(self) -> tuple[bool, str]:
        access = None
        if "configuration" in self.categories:
            access = _restored_access(
                await asyncio.to_thread(restore.configuration_preview)
            )
        ok, message, self.data = await request_webui_restart("webui.backup-restore")
        if access and access["access_urls"]:
            targets = [
                {"url": url, "scheme": urlsplit(url).scheme}
                for url in access["access_urls"]
            ]
            self.data.update(
                access_urls=access["access_urls"],
                target_access_urls=access["access_urls"],
                access_targets=targets,
                target_access_targets=targets,
                preferred_url=access["access_urls"][0],
            )
        self.data["restored_access"] = access
        return ok, message


@router.get("/scan", response_class=JSONResponse, description="导出预览")
async def _() -> Result[dict]:
    return Result.ok(await asyncio.to_thread(backup.scan))


@router.post("/export", response_class=JSONResponse, description="开始导出备份")
async def _(payload: ExportPayload) -> Result[dict]:
    try:
        status = backup.ExportTask.start(
            payload.categories, payload.extras, payload.password or None
        )
    except backup.BackupError as error:
        return Result.fail(str(error), 400)
    return Result.ok(status)


@router.get("/export", response_class=JSONResponse, description="导出任务状态")
async def _() -> Result[dict]:
    return Result.ok(backup.ExportTask.status)


@router.get("/list", response_class=JSONResponse, description="备份列表")
async def _() -> Result[list[dict]]:
    return Result.ok(await asyncio.to_thread(backup.list_backups))


@router.get("/download/{name}", description="下载备份包")
async def _(name: str):
    try:
        path = backup.backup_path(name)
    except backup.BackupError as error:
        return JSONResponse({"detail": str(error)}, 404)
    return FileResponse(path, filename=path.name, media_type="application/zip")


@router.post("/upload", response_class=JSONResponse, description="上传备份包")
async def _(file: UploadFile) -> Result[str]:
    try:
        path = await asyncio.to_thread(
            backup.save_upload, file.filename or "", file.file
        )
    except backup.BackupError as error:
        return Result.fail(str(error), 400)
    return Result.ok(path.name)


@router.post("/inspect", response_class=JSONResponse, description="读取备份包摘要")
async def _(payload: InspectPayload) -> Result[dict]:
    try:
        path = backup.backup_path(payload.name)
        summary = await asyncio.to_thread(
            restore.inspect, path, payload.password or None
        )
    except backup.BackupError as error:
        return Result.fail(str(error), 400)
    summary["restored_access"] = _restored_access(summary.pop("configuration_preview"))
    return Result.ok(summary)


@router.post("/restore", response_class=JSONResponse, description="从备份包恢复")
async def _(payload: RestorePayload) -> Result[dict]:
    restart = _Restart(payload.categories)
    try:
        status = await restore.request_restore(
            backup.backup_path(payload.name),
            payload.password or None,
            payload.categories,
            payload.skip_failed,
            restart,
        )
    except (backup.BackupError, restore.RestoreError) as error:
        return Result.fail(str(error), 400)
    return Result.ok({**restart.data, "restore": status}, "已开始恢复，Bot 即将重启")


@router.post("/switch", response_class=JSONResponse, description="切换到新数据库")
async def _(payload: DatabaseConfig) -> Result[dict]:
    probe = await probe_database(payload)
    if probe.status == "error":
        return Result.fail(probe.message, 400)
    restart = _Restart()
    try:
        status = await restore.request_switch(build_database_url(payload), restart)
    except restore.RestoreError as error:
        return Result.fail(str(error), 400)
    return Result.ok(
        {**restart.data, "restore": status}, "已开始切换数据库，Bot 即将重启"
    )


@router.get("/restore", response_class=JSONResponse, description="恢复状态")
async def _() -> Result[dict]:
    return Result.ok(restore.status())


@router.delete("/restore", response_class=JSONResponse, description="清除恢复记录")
async def _() -> Result:
    try:
        restore.dismiss()
    except restore.RestoreError as error:
        return Result.fail(str(error), 400)
    return Result.ok()


@router.delete("/{name}", response_class=JSONResponse, description="删除备份包")
async def _(name: str) -> Result:
    try:
        backup.backup_path(name).unlink()
    except backup.BackupError as error:
        return Result.fail(str(error), 404)
    return Result.ok()
