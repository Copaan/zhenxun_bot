from typing import Any

from pydantic import BaseModel, Field


class LifecycleComponentStatus(BaseModel):
    component_id: str
    stage: str
    depends_on: list[str]
    state: str
    error_code: str | None = None


class LifecycleStatus(BaseModel):
    shutdown_requested: bool = False
    component_count: int
    state_counts: dict[str, int]
    current_mutation: dict[str, Any] | None = None
    plugin_runtime: dict[str, Any] = Field(default_factory=dict)
    network: dict[str, Any] = Field(default_factory=dict)
    components: list[LifecycleComponentStatus]


class DirFile(BaseModel):
    """
    文件或文件夹
    """

    is_file: bool
    """是否为文件"""
    is_image: bool
    """是否为图片"""
    name: str
    """文件夹或文件名称"""
    parent: str | None = None
    """父级"""
    size: int | None = None
    """文件大小"""
    mtime: float | None = None
    """修改时间"""


class DeleteFile(BaseModel):
    """
    删除文件
    """

    full_path: str
    """文件全路径"""


class RenameFile(BaseModel):
    """
    删除文件
    """

    parent: str | None
    """父路径"""
    old_name: str
    """旧名称"""
    name: str
    """新名称"""


class AddFile(BaseModel):
    """
    新建文件
    """

    parent: str | None = None
    """父路径"""
    name: str
    """新名称"""


class SaveFile(BaseModel):
    """
    保存文件
    """

    full_path: str
    """全路径"""
    content: str
    """内容"""
