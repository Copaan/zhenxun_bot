from typing import Any, Literal

from pydantic import BaseModel, Field

from zhenxun.utils.enum import BlockType


class PluginSwitch(BaseModel):
    """
    插件开关
    """

    module: str
    """模块"""
    status: bool
    expected_revision: str | None = Field(default=None, min_length=64, max_length=64)
    """开关状态"""


class PluginUnload(BaseModel):
    """
    手动卸载/重新加载全局禁用插件
    """

    module: str
    """模块"""
    unloaded: bool
    """True 为卸载，False 为重新加载"""


class UpdatePlugin(BaseModel):
    """
    插件修改参数
    """

    module: str
    expected_revision: str | None = Field(default=None, min_length=64, max_length=64)
    """模块"""
    default_status: bool
    """是否默认开启"""
    limit_superuser: bool
    """是否限制超级用户"""
    level: int
    """等级"""
    cost_gold: int
    """花费金币"""
    menu_type: str
    """菜单类型"""
    block_type: BlockType | None = None
    """禁用类型"""


class PluginInfo(BaseModel):
    """
    基本插件信息
    """

    id: int
    policy_revision: str | None = None
    store_key: str | None = None
    runtime_module: str | None = None
    uninstall_supported: bool = False
    uninstall_reason: str | None = None
    usage: str = ""
    homepage: str | None = None
    management_source: str = "manual"
    management_route: str | None = None
    management_key: str | None = None
    """插件id"""
    module: str
    """模块"""
    plugin_name: str
    """插件名称"""
    default_status: bool
    """是否默认开启"""
    limit_superuser: bool
    """是否限制超级用户"""
    level: int
    """等级"""
    cost_gold: int
    """花费金币"""
    menu_type: str
    """菜单类型"""
    version: str
    """版本"""
    status: bool
    """状态"""
    author: str | None = None
    """作者"""
    block_type: BlockType | None = Field(None, description="插件禁用状态 (None: 启用)")
    """禁用状态"""
    is_builtin: bool = False
    """是否为内置插件"""
    allow_switch: bool = True
    """是否允许开关"""
    allow_setting: bool = True
    """是否允许设置"""
    parked: bool = False
    """是否已手动卸载以释放资源"""


class PluginCount(BaseModel):
    """
    插件数量
    """

    normal: int = 0
    """普通插件"""
    admin: int = 0
    """管理员插件"""
    superuser: int = 0
    """超级用户插件"""
    other: int = 0
    """其他插件"""


class BatchUpdatePluginItem(BaseModel):
    module: str = Field(..., description="插件模块名")
    default_status: bool | None = Field(None, description="默认状态(开关)")
    menu_type: str | None = Field(None, description="菜单类型")
    block_type: BlockType | None = Field(
        None, description="插件禁用状态 (None: 启用, ALL: 禁用)"
    )


class BatchUpdatePlugins(BaseModel):
    updates: list[BatchUpdatePluginItem] = Field(
        ..., description="要批量更新的插件列表"
    )


class PluginDetail(PluginInfo):
    """
    插件详情
    """

    config_groups: list[dict[str, Any]] = Field(default_factory=list)
    """插件相关的 config.yaml 配置组"""
    config_revision: str = ""
    """config.yaml 文件版本"""


class RenameMenuTypePayload(BaseModel):
    old_name: str = Field(..., description="旧菜单类型名称")
    new_name: str = Field(..., description="新菜单类型名称")


class PluginIr(BaseModel):
    download_source: Literal["auto", "ali", "git"] = "auto"
    id: int | None = None
    """插件id"""
    store_key: str | None = Field(default=None, min_length=3, max_length=300)
    """稳定商店键 source:module"""
    operation_id: str | None = Field(default=None, pattern=r"^[0-9a-f]{32}$")
    confirm_source_build: bool = False


class PluginReloadPayload(BaseModel):
    module: str | None = Field(
        default=None,
        min_length=1,
        max_length=200,
        # Dotted Python module path. Constrained so a reference cannot carry
        # path separators or other non-identifier text into unit resolution.
        pattern=r"^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)*$",
    )
    store_key: str | None = Field(default=None, min_length=3, max_length=300)
    operation_id: str | None = Field(default=None, pattern=r"^[0-9a-f]{32}$")


class BatchUpdateResult(BaseModel):
    """
    批量更新插件结果
    """

    success: bool = Field(..., description="是否全部成功")
    """是否全部成功"""
    updated_count: int = Field(..., description="更新成功的数量")
    """更新成功的数量"""
    errors: list[dict[str, str]] = Field(
        default_factory=list, description="错误信息列表"
    )
    """错误信息列表"""


class InstallDependenciesPayload(BaseModel):
    """
    安装依赖
    """

    handle_type: Literal["install", "uninstall"] = Field(..., description="处理类型")
    """处理类型"""

    dependencies: list[str] = Field(..., description="依赖列表")
    """依赖列表"""
    preview_id: str | None = None
    expected_fingerprint: str | None = None
    operation_id: str | None = Field(default=None, pattern=r"^[0-9a-f]{32}$")
    confirmed: bool = False
    confirm_source_build: bool = False
    summary_revision: str | None = None
