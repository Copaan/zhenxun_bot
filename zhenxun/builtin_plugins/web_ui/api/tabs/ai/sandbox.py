"""AI 沙箱：Docker 检测、镜像安装、测试运行、资源设置、会话清理与工作区文件"""

import posixpath
from urllib.parse import quote

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import Response
from pydantic import BaseModel, Field

from zhenxun.services.ai.sandbox.docker import DockerSandbox
from zhenxun.services.ai.sandbox.manager import SandboxManager
from zhenxun.utils.pydantic_compat import model_dump

from ....base_model import Result
from ....utils import authentication
from ._store import ai_set, config_revision, persist, read_config

router = APIRouter()


class SandboxPayload(BaseModel):
    enable_sandbox: bool
    docker_image: str = Field(
        min_length=1, max_length=200, pattern=r"^[A-Za-z0-9][\w.\-/:@]*$"
    )
    cpus: float = Field(ge=0.1, le=64)
    memory_mb: int = Field(ge=64, le=65536)
    exec_timeout: int = Field(ge=5, le=3600)
    network: bool
    cleanup_timeout: int = Field(ge=0, le=604800)


async def _state() -> dict:
    """沙箱设置与状态"""
    data = await SandboxManager.status()
    data["settings"] = model_dump(SandboxManager.settings())
    return data


@router.get("/sandbox", dependencies=[authentication()], response_model=Result)
async def sandbox_state() -> Result:
    return Result.ok(await _state())


@router.put("/sandbox", dependencies=[authentication()], response_model=Result)
async def save_sandbox(payload: SandboxPayload) -> Result:
    value = model_dump(payload)
    await persist(
        config_revision(read_config()), lambda ai: ai_set(ai, "sandbox", value)
    )
    return Result.ok(await _state(), info="沙箱设置已保存，新建的沙箱按新设置运行。")


@router.post("/sandbox/pull", dependencies=[authentication()], response_model=Result)
async def pull_image() -> Result:
    if not SandboxManager.pull():
        return Result.fail("镜像正在安装中。")
    return Result.ok(await _state(), info="已开始安装镜像。")


@router.post("/sandbox/test", dependencies=[authentication()], response_model=Result)
async def test_sandbox() -> Result:
    return Result.ok(await SandboxManager.test())


@router.delete(
    "/sandbox/sessions", dependencies=[authentication()], response_model=Result
)
async def clean_sessions(key: str | None = Query(None, max_length=300)) -> Result:
    if key:
        count = int(await SandboxManager.close(key))
    else:
        count = await SandboxManager.close_all()
    return Result.ok(await _state(), info=f"已清理 {count} 个沙箱。")


def _box(key: str) -> DockerSandbox:
    box = SandboxManager.find(key)
    if box is None:
        raise HTTPException(404, detail={"code": "sandbox_not_running"})
    return box


@router.get("/sandbox/files", dependencies=[authentication()], response_model=Result)
async def list_files(
    key: str = Query(min_length=1, max_length=300),
    path: str = Query(".", max_length=1000),
) -> Result:
    try:
        items = await _box(key).list_dir(path)
    except HTTPException:
        raise
    except NotADirectoryError:
        return Result.fail("不是目录。")
    except Exception as e:
        return Result.fail(f"无法读取目录：{e}")
    return Result.ok({"path": path, "items": items})


@router.get("/sandbox/file", dependencies=[authentication()])
async def read_file(
    key: str = Query(min_length=1, max_length=300),
    path: str = Query(min_length=1, max_length=1000),
    download: bool = False,
):
    try:
        data = await _box(key).read_file(path)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(404, detail={"code": "file_not_found"}) from e
    if download:
        name = quote(posixpath.basename(path) or "file")
        return Response(
            data,
            media_type="application/octet-stream",
            headers={"Content-Disposition": f"attachment; filename*=UTF-8''{name}"},
        )
    try:
        text = data.decode("utf-8")
    except UnicodeDecodeError:
        return Result.ok({"path": path, "binary": True, "size": len(data)})
    return Result.ok({"path": path, "binary": False, "text": text})
