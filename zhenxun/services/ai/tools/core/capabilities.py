from typing import Any

from zhenxun.services.ai.capabilities.base import AbstractCapability
from zhenxun.services.ai.run.context import RunContext
from zhenxun.services.ai.utils.runtime import PermissionUtils


class SuperuserCapability(AbstractCapability):
    """仅超级管理员可见"""

    async def prepare_tools(
        self, context: RunContext, tool_defs: list[Any]
    ) -> list[Any]:
        if await PermissionUtils.check_superuser(context):
            return tool_defs
        return []


class AdminLevelCapability(AbstractCapability):
    """需要群主或管理员权限"""

    def __init__(self, min_level: int):
        """
        初始化群管理权限限制能力。

        参数:
            min_level: 权限等级，大于 0 时要求超级用户或当前群的群主、管理员。
        """
        self.min_level = min_level

    async def prepare_tools(
        self, context: RunContext, tool_defs: list[Any]
    ) -> list[Any]:
        if await PermissionUtils.check_admin_level(context, self.min_level):
            return tool_defs
        return []
