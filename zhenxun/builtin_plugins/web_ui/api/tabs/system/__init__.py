import os
from pathlib import Path
import shutil
from typing import Any

import aiofiles
from fastapi import APIRouter
from fastapi.responses import JSONResponse

from zhenxun.services.lifecycle import lifecycle_kernel
from zhenxun.services.startup import startup_coordinator
from zhenxun.utils._build_image import BuildImage

from ....base_model import Result, SystemFolderSize
from ....restart_service import transaction_verification_status
from ....utils import authentication, get_system_disk, validate_filename, validate_path
from .configuration import router as configuration_router
from .model import (
    AddFile,
    DeleteFile,
    DirFile,
    LifecycleComponentStatus,
    LifecycleStatus,
    RenameFile,
    SaveFile,
)
from .restart import router as restart_router
from .runtime import router as runtime_router
from .update import router as update_router

router = APIRouter(prefix="/system")
router.include_router(configuration_router)
router.include_router(update_router)
router.include_router(restart_router)
router.include_router(runtime_router)


@router.get(
    "/startup/status",
    response_model=Result[dict[str, Any]],
    response_class=JSONResponse,
    description="获取worker分级启动状态",
)
async def get_startup_status() -> Result[dict[str, Any]]:
    return Result.ok(
        {**startup_coordinator.snapshot(), **transaction_verification_status()}
    )


@router.get(
    "/startup/report",
    dependencies=[authentication()],
    response_model=Result[dict[str, Any]],
    response_class=JSONResponse,
    description="获取worker完整启动事务报告",
)
async def get_startup_report() -> Result[dict[str, Any]]:
    return Result.ok(startup_coordinator.report())


@router.get(
    "/lifecycle/status",
    dependencies=[authentication()],
    response_model=Result[LifecycleStatus],
    response_class=JSONResponse,
    description="获取统一生命周期组件状态",
)
async def get_lifecycle_status() -> Result[LifecycleStatus]:
    from zhenxun.services.lifecycle.launcher import launcher_lifecycle_snapshot
    from zhenxun.services.lifecycle.operations import operation_registry
    from zhenxun.services.runtime_reload import plugin_runtime_manager
    from zhenxun.services.webui_transport import transport_runtime

    return Result.ok(
        {
            **lifecycle_kernel.status(),
            "launcher": launcher_lifecycle_snapshot(),
            "operation_registry": operation_registry.status(),
            "plugin_runtime": plugin_runtime_manager.status(),
            "transport": transport_runtime.snapshot(),
        }
    )


@router.get(
    "/lifecycle/components/{component_id:path}",
    dependencies=[authentication()],
    response_model=Result[LifecycleComponentStatus],
    response_class=JSONResponse,
    description="获取生命周期组件详情",
)
async def get_lifecycle_component(
    component_id: str,
) -> Result[LifecycleComponentStatus]:
    component = lifecycle_kernel.component_status(component_id)
    if component is None:
        return Result.fail("生命周期组件不存在。", code=404)
    return Result.ok(component)


@router.get(
    "/lifecycle/operations",
    dependencies=[authentication()],
    response_model=Result[dict[str, Any]],
    response_class=JSONResponse,
    description="获取生命周期后台操作",
)
async def get_lifecycle_operations() -> Result[dict[str, Any]]:
    from zhenxun.services.lifecycle.operations import operation_registry

    return Result.ok(operation_registry.status())


@router.get(
    "/lifecycle/operations/{operation_id}",
    dependencies=[authentication()],
    response_model=Result[dict[str, Any]],
    response_class=JSONResponse,
    description="获取生命周期后台操作详情",
)
async def get_lifecycle_operation(operation_id: str) -> Result[dict[str, Any]]:
    from zhenxun.services.lifecycle.operations import operation_registry

    operation = operation_registry.get(operation_id)
    if operation is None:
        return Result.fail("lifecycle_operation_not_found", code=404)
    return Result.ok(operation)


IMAGE_TYPE = ["jpg", "jpeg", "png", "gif", "bmp", "webp", "svg"]


@router.get(
    "/get_dir_list",
    dependencies=[authentication()],
    response_model=Result[list[DirFile]],
    response_class=JSONResponse,
    description="获取文件列表",
)
async def _(path: str | None = None) -> Result[list[DirFile]]:
    try:
        base_path, error = validate_path(path)
        if error:
            return Result.fail(error)
        if not base_path:
            return Result.fail("无效的路径")
        data_list = []
        for file in os.listdir(base_path):
            file_path = base_path / file
            is_image = any(file.endswith(f".{t}") for t in IMAGE_TYPE)
            data_list.append(
                DirFile(
                    is_file=not file_path.is_dir(),
                    is_image=is_image,
                    name=file,
                    parent=path,
                    size=None if file_path.is_dir() else file_path.stat().st_size,
                    mtime=file_path.stat().st_mtime,
                )
            )
        data_list.sort(key=lambda f: f.name)
        return Result.ok(data_list)
    except Exception as e:
        return Result.fail(f"获取文件列表失败: {e!s}")


@router.get(
    "/get_resources_size",
    dependencies=[authentication()],
    response_model=Result[list[SystemFolderSize]],
    response_class=JSONResponse,
    description="获取文件列表",
)
async def _(full_path: str | None = None) -> Result[list[SystemFolderSize]]:
    return Result.ok(await get_system_disk(full_path))


@router.post(
    "/delete_file",
    dependencies=[authentication()],
    response_model=Result,
    response_class=JSONResponse,
    description="删除文件",
)
async def _(param: DeleteFile) -> Result:
    path, error = validate_path(param.full_path)
    if error:
        return Result.fail(error)
    if not path:
        return Result.fail("无效的路径")
    if not path.exists():
        return Result.warning_("文件不存在...")
    try:
        path.unlink()
        return Result.ok("删除成功!")
    except Exception as e:
        return Result.warning_(f"删除失败: {e!s}")


@router.post(
    "/delete_folder",
    dependencies=[authentication()],
    response_model=Result,
    response_class=JSONResponse,
    description="删除文件夹",
)
async def _(param: DeleteFile) -> Result:
    path, error = validate_path(param.full_path)
    if error:
        return Result.fail(error)
    if not path:
        return Result.fail("无效的路径")
    if not path.exists() or path.is_file():
        return Result.warning_("文件夹不存在...")
    try:
        shutil.rmtree(path.absolute())
        return Result.ok("删除成功!")
    except Exception as e:
        return Result.warning_(f"删除失败: {e!s}")


@router.post(
    "/rename_file",
    dependencies=[authentication()],
    response_model=Result,
    response_class=JSONResponse,
    description="重命名文件",
)
async def _(param: RenameFile) -> Result:
    parent_path, error = validate_path(param.parent)
    if error:
        return Result.fail(error)
    if not parent_path:
        return Result.fail("无效的路径")

    if err := validate_filename(param.old_name):
        return Result.fail(err)
    if err := validate_filename(param.name):
        return Result.fail(err)

    root = os.path.realpath(Path())
    path = Path(os.path.realpath(parent_path / param.old_name))
    if not str(path).startswith(root + os.sep):
        return Result.fail("访问路径超出允许范围")
    if not path.exists():
        return Result.warning_("文件不存在...")
    try:
        dest = Path(os.path.realpath(path.parent / param.name))
        if not str(dest).startswith(root + os.sep):
            return Result.fail("目标路径超出允许范围")
        path.rename(dest)
        return Result.ok("重命名成功!")
    except Exception as e:
        return Result.warning_(f"重命名失败: {e!s}")


@router.post(
    "/rename_folder",
    dependencies=[authentication()],
    response_model=Result,
    response_class=JSONResponse,
    description="重命名文件夹",
)
async def _(param: RenameFile) -> Result:
    parent_path, error = validate_path(param.parent)
    if error:
        return Result.fail(error)
    if not parent_path:
        return Result.fail("无效的路径")

    if err := validate_filename(param.old_name):
        return Result.fail(err)
    if err := validate_filename(param.name):
        return Result.fail(err)

    root = os.path.realpath(Path())
    path = Path(os.path.realpath(parent_path / param.old_name))
    if not str(path).startswith(root + os.sep):
        return Result.fail("访问路径超出允许范围")
    if not path.exists() or path.is_file():
        return Result.warning_("文件夹不存在...")
    try:
        dest = Path(os.path.realpath(path.parent / param.name))
        if not str(dest).startswith(root + os.sep):
            return Result.fail("目标路径超出允许范围")
        shutil.move(path.absolute(), dest)
        return Result.ok("重命名成功!")
    except Exception as e:
        return Result.warning_(f"重命名失败: {e!s}")


@router.post(
    "/add_file",
    dependencies=[authentication()],
    response_model=Result,
    response_class=JSONResponse,
    description="新建文件",
)
async def _(param: AddFile) -> Result:
    parent_path, error = validate_path(param.parent)
    if error:
        return Result.fail(error)
    if not parent_path:
        return Result.fail("无效的路径")

    if err := validate_filename(param.name):
        return Result.fail(err)

    path = (parent_path / param.name) if param.parent else Path(param.name)
    # 二次确认拼接后路径仍在允许范围内
    resolved, err = validate_path(str(path))
    if err or not resolved:
        return Result.fail(err or "无效的路径")
    path = resolved
    if path.exists():
        return Result.warning_("文件已存在...")
    try:
        path.touch()
        return Result.ok("新建文件成功!")
    except Exception as e:
        return Result.warning_(f"新建文件失败: {e!s}")


@router.post(
    "/add_folder",
    dependencies=[authentication()],
    response_model=Result,
    response_class=JSONResponse,
    description="新建文件夹",
)
async def _(param: AddFile) -> Result:
    parent_path, error = validate_path(param.parent)
    if error:
        return Result.fail(error)
    if not parent_path:
        return Result.fail("无效的路径")

    if err := validate_filename(param.name):
        return Result.fail(err)

    path = (parent_path / param.name) if param.parent else Path(param.name)
    # 二次确认拼接后路径仍在允许范围内
    resolved, err = validate_path(str(path))
    if err or not resolved:
        return Result.fail(err or "无效的路径")
    path = resolved
    if path.exists():
        return Result.warning_("文件夹已存在...")
    try:
        path.mkdir()
        return Result.ok("新建文件夹成功!")
    except Exception as e:
        return Result.warning_(f"新建文件夹失败: {e!s}")


@router.get(
    "/read_file",
    dependencies=[authentication()],
    response_model=Result[str],
    response_class=JSONResponse,
    description="读取文件",
)
async def _(full_path: str) -> Result:
    path, error = validate_path(full_path)
    if error:
        return Result.fail(error)
    if not path:
        return Result.fail("无效的路径")
    if not path.exists():
        return Result.warning_("文件不存在...")
    try:
        text = path.read_text(encoding="utf-8")
        return Result.ok(text)
    except Exception as e:
        return Result.warning_(f"读取文件失败: {e!s}")


@router.post(
    "/save_file",
    dependencies=[authentication()],
    response_model=Result[str],
    response_class=JSONResponse,
    description="读取文件",
)
async def _(param: SaveFile) -> Result[str]:
    path, error = validate_path(param.full_path)
    if error:
        return Result.fail(error)
    if not path:
        return Result.fail("无效的路径")
    try:
        async with aiofiles.open(str(path), "w", encoding="utf-8") as f:
            await f.write(param.content)
        return Result.ok("更新成功!")
    except Exception as e:
        return Result.warning_(f"保存文件失败: {e!s}")


@router.get(
    "/get_image",
    dependencies=[authentication()],
    response_model=Result[str],
    response_class=JSONResponse,
    description="读取图片base64",
)
async def _(full_path: str) -> Result[str]:
    path, error = validate_path(full_path)
    if error:
        return Result.fail(error)
    if not path:
        return Result.fail("无效的路径")
    if not path.exists():
        return Result.warning_("文件不存在...")
    try:
        return Result.ok(BuildImage.open(path).pic2bs4())
    except Exception as e:
        return Result.warning_(f"获取图片失败: {e!s}")


@router.get(
    "/ping",
    response_model=Result[str],
    response_class=JSONResponse,
    description="检查服务器状态",
)
async def _() -> Result[str]:
    return Result.ok("pong")
