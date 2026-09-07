from typing import Any

from pydantic import BaseModel, Field


class LifecycleComponentStatus(BaseModel):
    component_id: str
    scope: str
    stage: str
    depends_on: list[str]
    provides: list[str]
    resource_group: str | None = None
    timeout: float | None = None
    drain_timeout: float = 10.0
    cancel_timeout: float = 5.0
    finalizer_timeout: float = 10.0
    failure_policy: str
    restart_policy: str
    config_keys: list[str]
    priority: int
    stop_priority: int | None = None
    parallel_safe: bool
    source: str
    state: str
    runtime_generation: int
    observed_generation: int
    started_at: str | None = None
    stopped_at: str | None = None
    duration_ms: float | None = None
    health: str
    error_code: str | None = None
    last_health_checked_at: str | None = None
    consecutive_health_failures: int = 0
    active_activities: int = 0
    metadata: dict[str, Any] = Field(default_factory=dict)
    resource_count: int
    resource_counts: dict[str, int] = Field(default_factory=dict)
    dynamic_scopes: list[dict[str, Any]] = Field(default_factory=list)
    recovery_required: bool = False


class LifecycleStatus(BaseModel):
    version: int = 2
    snapshot_at: str | None = None
    snapshot_phase: str | None = None
    snapshot_identity: dict[str, Any] = Field(default_factory=dict)
    terminal_shutdown: dict[str, Any] | None = None
    persistence: dict[str, Any] = Field(default_factory=dict)
    cleanup_tasks: list[dict[str, Any]] = Field(default_factory=list)
    unresolved_resources: list[dict[str, Any]] = Field(default_factory=list)
    runtime_generation: int
    component_count: int
    scope_counts: dict[str, int]
    state_counts: dict[str, int]
    current_operation: dict[str, Any] | None = None
    current_operations: list[dict[str, Any]]
    process: dict[str, Any] = Field(default_factory=dict)
    current_mutation: dict[str, Any] | None = None
    dynamic_scope_count: int = 0
    active_scope_count: int = 0
    dynamic_scopes: list[dict[str, Any]] = Field(default_factory=list)
    recovery_required: list[str] = Field(default_factory=list)
    ownership: dict[str, Any] = Field(default_factory=dict)
    operation_registry: dict[str, Any] = Field(default_factory=dict)
    plugin_runtime: dict[str, Any] = Field(default_factory=dict)
    launcher: dict[str, Any] = Field(default_factory=dict)
    transport: dict[str, Any] = Field(default_factory=dict)
    http_sidecar: dict[str, Any] = Field(default_factory=dict)
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
