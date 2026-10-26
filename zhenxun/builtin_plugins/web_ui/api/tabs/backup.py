import asyncio
import secrets
import time
from typing import Annotated
from urllib.parse import urlsplit

from fastapi import APIRouter, Depends, Request
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel

from zhenxun.configs.webui_tls import WebUITLSConfigError, settings_from_values
from zhenxun.services.migration import backup, restore

from ...base_model import Result
from ...restart_service import preferred_access_urls, request_webui_restart
from ...utils import authentication
from ..configure.data_source import build_database_url, probe_database
from ..configure.model import DatabaseConfig
from ..configure.setup_access import SetupSession, require_setup_token, setup_access

router = APIRouter(prefix="/backup", dependencies=[authentication()])
file_router = APIRouter(prefix="/backup")
setup_router = APIRouter(
    prefix="/configure/backup", dependencies=[Depends(require_setup_token)]
)


DOWNLOAD_TTL = 7200
"""下载凭证的有效期（秒），每次使用后顺延"""
_download_tickets: dict[str, tuple[str, float]] = {}


class BackupFileResponse(FileResponse):
    """备份包下载响应，无法交给内核发送时按 4MB 分块读取"""

    chunk_size = 4 * 1024 * 1024


def issue_download_ticket(name: str) -> str:
    """为备份包签发下载凭证

    参数:
        name: 备份包文件名

    返回:
        str: 下载凭证
    """
    now = time.monotonic()
    for token, (_, expires) in list(_download_tickets.items()):
        if expires <= now:
            del _download_tickets[token]
    token = secrets.token_urlsafe(32)
    _download_tickets[token] = (name, now + DOWNLOAD_TTL)
    return token


def use_download_ticket(token: str, name: str) -> bool:
    """校验下载凭证并顺延有效期

    参数:
        token: 下载凭证
        name: 请求的备份包文件名

    返回:
        bool: 凭证是否有效且属于该备份包
    """
    ticket = _download_tickets.get(token)
    now = time.monotonic()
    if ticket is None or ticket[0] != name or ticket[1] <= now:
        return False
    _download_tickets[token] = (name, now + DOWNLOAD_TTL)
    return True


class ExportPayload(BaseModel):
    categories: list[str] = list(backup.DEFAULT_CATEGORIES)
    """备份的文件分类"""
    extras: list[str] | None = None
    """高级模式选择记录的依赖包名，为空时记录全部依赖"""
    password: str | None = None
    """加密密码"""


class UploadPayload(BaseModel):
    name: str
    """备份包文件名"""
    size: int
    """备份包总字节数"""


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


class SetupRestorePayload(RestorePayload):
    database: DatabaseConfig
    """恢复到的数据库，须为空库"""


def _restored_access(preview: dict | None) -> dict | None:
    """备份中的配置恢复后 WebUI 的访问地址与管理员账号

    参数:
        preview: restore.configuration_preview 的结果

    返回:
        dict | None: access_urls 为访问地址（无法确定时为空），username 为管理员账号，
            password_set 为是否设置了管理员密码
    """
    if preview is None:
        return None
    urls = []
    if preview["environment"]:
        try:
            settings = settings_from_values(preview["environment"])
            urls = preferred_access_urls(
                settings.host, settings.port, settings=settings, http_available=True
            )
        except (WebUITLSConfigError, ValueError):
            pass
    return {
        "access_urls": urls,
        "username": preview["username"],
        "password_set": preview["password_set"],
    }


class _Restart:
    """调用 WebUI 重启并保留重启信息，供前端等待新进程

    恢复配置时，重启信息中的访问地址按恢复后生效的配置计算
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


@router.post(
    "/download/{name}", response_class=JSONResponse, description="签发下载凭证"
)
async def _(name: str) -> Result[str]:
    try:
        backup.backup_path(name)
    except backup.BackupError as error:
        return Result.fail(str(error), 404)
    return Result.ok(issue_download_ticket(name))


@file_router.get("/file/{token}/{name}", description="凭下载凭证下载备份包")
async def _(token: str, name: str):
    if not use_download_ticket(token, name):
        return JSONResponse({"detail": "下载链接无效或已过期，请重新下载"}, 403)
    try:
        path = backup.backup_path(name)
    except backup.BackupError as error:
        return JSONResponse({"detail": str(error)}, 404)
    return BackupFileResponse(path, filename=path.name, media_type="application/zip")


@setup_router.post(
    "/upload", response_class=JSONResponse, description="首次部署开始分片上传备份包"
)
@router.post("/upload", response_class=JSONResponse, description="开始分片上传备份包")
async def _(payload: UploadPayload) -> Result[dict]:
    try:
        upload_id = await asyncio.to_thread(
            backup.ChunkUpload.start, payload.name, payload.size
        )
    except backup.BackupError as error:
        return Result.fail(str(error), 400)
    return Result.ok({"id": upload_id, "chunk_size": backup.ChunkUpload.CHUNK_SIZE})


@setup_router.get(
    "/upload/{upload_id}", response_class=JSONResponse, description="首次部署上传进度"
)
@router.get("/upload/{upload_id}", response_class=JSONResponse, description="上传进度")
async def _(upload_id: str) -> Result[int]:
    try:
        return Result.ok(await asyncio.to_thread(backup.ChunkUpload.offset, upload_id))
    except backup.BackupError as error:
        return Result.fail(str(error), 404)


@setup_router.put(
    "/upload/{upload_id}", response_class=JSONResponse, description="首次部署上传分片"
)
@router.put("/upload/{upload_id}", response_class=JSONResponse, description="上传分片")
async def _(upload_id: str, offset: int, request: Request) -> Result[int]:
    limit = backup.ChunkUpload.CHUNK_LIMIT
    if int(request.headers.get("content-length") or 0) > limit:
        return Result.fail("分片过大", 413)
    chunk = bytearray()
    async for data in request.stream():
        chunk += data
        if len(chunk) > limit:
            return Result.fail("分片过大", 413)
    try:
        received = await asyncio.to_thread(
            backup.ChunkUpload.write, upload_id, offset, bytes(chunk)
        )
    except backup.UploadOffsetError as error:
        return Result.fail(str(error), 409, {"offset": error.offset})
    except backup.BackupError as error:
        return Result.fail(str(error), 400)
    return Result.ok(received)


@setup_router.post(
    "/upload/{upload_id}/finish",
    response_class=JSONResponse,
    description="首次部署完成分片上传",
)
@router.post(
    "/upload/{upload_id}/finish",
    response_class=JSONResponse,
    description="完成分片上传",
)
async def _(upload_id: str) -> Result[str]:
    try:
        path = await asyncio.to_thread(backup.ChunkUpload.finish, upload_id)
    except backup.BackupError as error:
        return Result.fail(str(error), 400)
    return Result.ok(path.name)


@setup_router.delete(
    "/upload/{upload_id}", response_class=JSONResponse, description="首次部署取消上传"
)
@router.delete(
    "/upload/{upload_id}", response_class=JSONResponse, description="取消上传"
)
async def _(upload_id: str) -> Result:
    try:
        await asyncio.to_thread(backup.ChunkUpload.cancel, upload_id)
    except backup.BackupError as error:
        return Result.fail(str(error), 404)
    return Result.ok()


@setup_router.post(
    "/inspect", response_class=JSONResponse, description="首次部署读取备份包摘要"
)
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


@setup_router.post(
    "/restore", response_class=JSONResponse, description="首次部署从备份包恢复"
)
async def _(
    payload: SetupRestorePayload,
    session: Annotated[SetupSession, Depends(require_setup_token)],
) -> Result[dict]:
    if "configuration" not in payload.categories:
        return Result.fail("首次部署恢复必须包含配置分类", 400)
    probe = await probe_database(payload.database)
    if probe.status == "error":
        return Result.fail(probe.message, 400)
    try:
        path = backup.backup_path(payload.name)
        summary = await asyncio.to_thread(
            restore.inspect, path, payload.password or None
        )
        preview = summary["configuration_preview"] or {}
        if not (preview.get("username") and preview.get("password_set")):
            return Result.fail(
                "备份中没有 WebUI 管理员账号或密码，无法用于首次部署", 400
            )
        restart = _Restart(payload.categories)
        status = await restore.request_restore(
            path,
            payload.password or None,
            payload.categories,
            payload.skip_failed,
            restart,
            db_url=build_database_url(payload.database),
        )
    except (backup.BackupError, restore.RestoreError, ValueError) as error:
        return Result.fail(str(error), 400)
    await setup_access.mark_applied(session)
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


@setup_router.get(
    "/restore", response_class=JSONResponse, description="首次部署上次恢复的状态"
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
