from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from zhenxun.utils.enum import PluginType

from .auth_snapshot import AuthSnapshot

PolicyEffect = Literal["allow", "deny", "skip"]


@dataclass(frozen=True, slots=True)
class PolicyDecision:
    effect: PolicyEffect
    reason: str = ""

    @property
    def allowed(self) -> bool:
        return self.effect == "allow"

    @property
    def denied(self) -> bool:
        return self.effect == "deny"

    @property
    def skipped(self) -> bool:
        return self.effect == "skip"


@dataclass(frozen=True, slots=True)
class PolicyContext:
    snapshot: AuthSnapshot
    allow_sleep_bypass: bool = False
    allow_group_sleep_bypass: bool = False


class PolicyDecisionPoint:
    """基于权限快照的无副作用权限判定，限制、金币等副作用由 auth_checker 处理"""

    @staticmethod
    def superuser_exempt(snapshot: AuthSnapshot) -> bool:
        """超级用户是否免除账号休眠、群组与管理员权限、限制与金币检查

        超级用户插件或未开启 limit_superuser 的插件免除；插件全局关闭与账号屏蔽仍由
        decide_plugin 判断，账号或群组数据缺失时仍拒绝。

        参数:
            snapshot: 权限快照

        返回:
            bool: 是否免除
        """
        profile = snapshot.profile
        return snapshot.is_superuser and (
            profile.plugin_type == PluginType.SUPERUSER or not profile.limit_superuser
        )

    def decide(self, context: PolicyContext) -> PolicyDecision:
        snapshot = context.snapshot
        profile = snapshot.profile
        if profile.hidden:
            return PolicyDecision("allow", "hidden_plugin_skip_auth")
        if profile.superuser_only and not snapshot.is_superuser:
            return PolicyDecision("deny", "superuser_required")
        return PolicyDecision("skip", "needs_detail_check")

    def decide_bot(self, context: PolicyContext) -> PolicyDecision:
        bot_data = context.snapshot.bot_data
        if bot_data is None:
            return PolicyDecision("deny", "bot_not_found")
        if (
            not bot_data.status
            and not context.allow_sleep_bypass
            and not self.superuser_exempt(context.snapshot)
        ):
            return PolicyDecision("deny", "bot_sleeping")
        return PolicyDecision("allow", "bot_allowed")

    def decide_group(self, context: PolicyContext) -> PolicyDecision:
        snapshot = context.snapshot
        if not snapshot.group_id:
            return PolicyDecision("skip", "not_group_event")
        group = snapshot.group
        if group is None:
            return PolicyDecision("deny", "group_not_found")
        if self.superuser_exempt(snapshot):
            return PolicyDecision("allow", "superuser")
        if group.level < 0:
            return PolicyDecision("deny", "group_blacklisted")
        if (
            not group.status
            and not context.allow_group_sleep_bypass
            and not snapshot.is_superuser
        ):
            return PolicyDecision("deny", "group_sleeping")
        if snapshot.profile.level > group.level:
            return PolicyDecision("deny", "group_level_low")
        return PolicyDecision("allow", "group_allowed")

    def decide_plugin(self, context: PolicyContext) -> PolicyDecision:
        snapshot = context.snapshot
        from zhenxun.services.plugin_policy import plugin_policy_service

        event = snapshot.context
        reason = plugin_policy_service.availability(
            snapshot.profile,
            snapshot.bot_data,
            snapshot.group,
            bot_id=event.bot_id,
            platform_scope=event.platform_scope,
            group_id=event.group_id,
            channel_id=event.channel_id,
            is_superuser=snapshot.is_superuser,
        )
        return (
            PolicyDecision("deny", reason)
            if reason
            else PolicyDecision("allow", "plugin_allowed")
        )

    def decide_admin(self, context: PolicyContext) -> PolicyDecision:
        snapshot = context.snapshot
        profile = snapshot.profile
        if not profile.need_admin:
            return PolicyDecision("skip", "admin_not_required")
        if profile.plugin_type in {PluginType.SUPERUSER, PluginType.SUPER_AND_ADMIN}:
            if snapshot.is_superuser:
                return PolicyDecision("allow", "superuser")
            if profile.plugin_type == PluginType.SUPERUSER:
                return PolicyDecision("deny", "superuser_required")
        if self.superuser_exempt(snapshot):
            return PolicyDecision("allow", "superuser")
        if not profile.admin_level:
            return PolicyDecision("skip", "admin_level_empty")
        global_user, group_user = snapshot.admin_levels or (None, None)
        user_level = global_user.user_level if global_user else 0
        if snapshot.group_id and group_user:
            user_level = max(user_level, group_user.user_level)
        if user_level < profile.admin_level:
            return PolicyDecision("deny", "admin_level_low")
        return PolicyDecision("allow", "admin_allowed")


__all__ = ["PolicyContext", "PolicyDecision", "PolicyDecisionPoint"]
