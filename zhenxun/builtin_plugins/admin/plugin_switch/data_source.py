from nonebot.adapters import Bot

from zhenxun.models.group_console import GroupConsole
from zhenxun.services.cache.runtime_cache import GroupMemoryCache
from zhenxun.services.platform_identity import scoped_group_id
from zhenxun.services.plugin_policy import scene_global_state
from zhenxun.utils.common_utils import CommonUtils
from zhenxun.utils.enum import BlockType
from zhenxun.utils.platform import PlatformUtils

from .strategy import get_strategy

BLOCK_REASON_LABELS = {
    "not_loaded": "未加载",
    "global_disabled": "全局关闭",
    "global_group_disabled": "全局群聊关闭",
    "global_private_disabled": "全局私聊关闭",
    "account_disabled": "账号禁用",
    "administrator_disabled": "超管锁定",
    "group_disabled": "本群禁用",
    "private_disabled": "私聊禁用",
}


class PluginManager:
    @staticmethod
    def blocked_notice(views: list[dict], modules: set[str], is_task: bool) -> str:
        """汇总开启后仍不可用的功能及原因

        参数:
            views: 写入后返回的场景策略视图
            modules: 本次开启的模块
            is_task: 是否为被动技能

        返回:
            str: 提示文本，全部可用时为空
        """
        reasons = [
            row["blocked_by"]
            for view in views
            for row in view["effective"]["tasks" if is_task else "plugins"]
            if row["module"] in modules and row["blocked_by"]
        ]
        if not reasons:
            return ""
        labels = "、".join(
            label
            for reason, label in BLOCK_REASON_LABELS.items()
            if any(reason in items for items in reasons)
        )
        return f"\n其中 {len(reasons)} 项仍不可用，原因：{labels}"

    @staticmethod
    def _modify_block_string(current_str: str, module: str, add: bool) -> str:
        """辅助: 添加或移除禁用模块字符串"""
        items = CommonUtils.convert_module_format(current_str)
        if add:
            if module not in items:
                items.append(module)
        else:
            if module in items:
                items.remove(module)
        return CommonUtils.convert_module_format(items)

    @classmethod
    async def _calculate_affected_groups(
        cls,
        target_groups: set[str],
        status: bool,
        is_whitelist_mode: bool,
        bot: Bot | None,
    ) -> tuple[set[str], set[str]]:
        """提取公用的目标群组计算逻辑（白名单/普通模式交并集）"""
        groups_to_open = set()
        groups_to_close = set()
        clean_targets = {str(gid) for gid in target_groups if gid}

        if is_whitelist_mode and status:
            if bot:
                active_groups, _ = await PlatformUtils.get_group_list(
                    bot, only_group=True
                )
                all_group_set = {str(g.group_id) for g in active_groups if g.group_id}
            else:
                all_group_ids = await GroupConsole.all().values_list(
                    "group_id", flat=True
                )
                all_group_set = {str(gid) for gid in all_group_ids}
            groups_to_open = clean_targets
            groups_to_close = all_group_set - clean_targets
        else:
            if status:
                groups_to_open = clean_targets
            else:
                groups_to_close = clean_targets
        return groups_to_open, groups_to_close

    @classmethod
    async def batch_update_status(
        cls,
        name: str,
        target_groups: set[str],
        status: bool,
        is_task: bool = False,
        is_superuser: bool = False,
        is_whitelist_mode: bool = False,
        bot: Bot | None = None,
        use_su_field: bool = False,
        channel_id: str | None = None,
    ) -> str:
        """批量开关多个群中的功能，超级用户开启时同时解除本群禁用与超管锁定

        参数:
            name: 功能名称
            target_groups: 目标群
            status: 开启或关闭
            is_task: 是否为被动技能
            is_superuser: 是否为超级用户
            is_whitelist_mode: 仅在目标群开启，其余群关闭
            bot: 当前账号
            use_su_field: 关闭时写入超管锁定
            channel_id: 频道

        返回:
            str: 回复消息，包含开启后仍不可用的原因
        """
        from zhenxun.services.bot_group_policy import bot_group_policy_service

        if bot is None:
            return "缺少 Bot 作用域，请指定目标账号后重试。"
        strategy = get_strategy(is_task)
        entity = await strategy.get_entity(name)
        if not entity:
            return f"未找到{strategy.entity_type_name}: {name}"
        to_open, to_close = await cls._calculate_affected_groups(
            target_groups, status, is_whitelist_mode, bot
        )
        bot_id = PlatformUtils.get_storage_bot_id(bot)
        scope = PlatformUtils.get_platform_scope(bot)
        opened = []
        for gid in sorted(to_open | to_close):
            view = await bot_group_policy_service.set_features(
                bot_id,
                scope,
                gid,
                [entity.module],
                gid in to_open,
                task=is_task,
                channel_id=channel_id,
                force=use_su_field,
                is_superuser=is_superuser,
            )
            if gid in to_open:
                opened.append(view)
        return (
            f"已更新当前账号策略：开启 {len(to_open)} 群，关闭 {len(to_close)} 群。"
        ) + cls.blocked_notice(opened, {entity.module}, is_task)

    @classmethod
    async def set_default_status(
        cls, plugin_name: str, status: bool, is_task: bool = False
    ) -> str:
        strategy = get_strategy(is_task)
        entity = await strategy.get_entity(plugin_name)
        if entity:
            await strategy.set_default_status(entity, status)
            status_text = "开启" if status else "关闭"
            return (
                f"成功将 {getattr(entity, 'name', plugin_name)} "
                f"进群默认状态修改为: {status_text}"
            )
        return "没有找到这个功能喔..."

    @classmethod
    async def set_all_plugin_status(
        cls,
        status: bool,
        is_default: bool = False,
        group_id: str | None = None,
        is_task: bool = False,
        is_superuser: bool = False,
        use_su_field: bool = False,
        bot: Bot | None = None,
        channel_id: str | None = None,
    ) -> str:
        strategy = get_strategy(is_task)
        type_str = strategy.entity_type_name

        if is_default:
            await strategy.set_all_default_status(status)
            return (
                f"成功将所有{type_str}进群默认状态修改为: "
                f"{'开启' if status else '关闭'}"
            )

        if group_id or bot is not None:
            if bot is None:
                return "缺少 Bot 作用域，请指定目标账号后重试。"
            from zhenxun.services.bot_group_policy import bot_group_policy_service

            modules = set(await strategy.get_all_modules())
            view = await bot_group_policy_service.set_features(
                PlatformUtils.get_storage_bot_id(bot),
                PlatformUtils.get_platform_scope(bot),
                group_id,
                modules,
                status,
                task=is_task,
                channel_id=channel_id,
                force=use_su_field,
                is_superuser=is_superuser,
            )
            notice = cls.blocked_notice([view], modules, is_task) if status else ""
            return (
                f"当前账号{'当前群' if group_id else '所有私聊'}的所有{type_str}"
                f"已{'开启' if status else '关闭'}{notice}"
            )

        await strategy.set_all_global_status(status)
        return f"成功将所有{type_str}全局状态修改为: {'开启' if status else '关闭'}"

    @classmethod
    async def superuser_set_status(
        cls, plugin_name: str, status: bool, scene: BlockType
    ) -> str:
        """在群聊/私聊/全部场景全局开启或关闭功能

        参数:
            plugin_name: 功能名称
            status: 开启或关闭
            scene: 操作场景

        返回:
            str: 回复消息
        """
        strategy = get_strategy(False)
        entity = await strategy.get_entity(plugin_name)
        if not entity:
            return "没有找到这个功能喔..."
        new_status, block_type = scene_global_state(
            entity.status, entity.block_type, scene, status
        )
        await strategy.set_global_status(entity, new_status, block_type)
        await strategy.refresh_cache()
        state = {
            None: "群聊和私聊均可用",
            BlockType.GROUP: "仅私聊可用",
            BlockType.PRIVATE: "仅群聊可用",
            BlockType.ALL: "全局关闭",
        }[block_type]
        scene_cn = {
            BlockType.ALL: "全局",
            BlockType.GROUP: "群聊中",
            BlockType.PRIVATE: "私聊中",
        }[scene]
        return (
            f"已在{scene_cn}{'开启' if status else '关闭'} {entity.name}，"
            f"当前状态：{state}"
        )

    @classmethod
    async def batch_set_group_active_status(
        cls,
        target_groups: set[str],
        status: bool,
        is_whitelist_mode: bool = False,
        bot: Bot | None = None,
    ) -> str:
        """批量设置群组激活状态 (休眠/醒来) - 采用与插件相同的目标计算逻辑"""
        groups_to_wake, groups_to_sleep = await cls._calculate_affected_groups(
            target_groups, status, is_whitelist_mode, bot
        )

        groups_to_wake = {scoped_group_id(gid) for gid in groups_to_wake}
        groups_to_sleep = {scoped_group_id(gid) for gid in groups_to_sleep}
        affected_ids = groups_to_wake | groups_to_sleep
        if not affected_ids:
            return "没有目标群组需要操作。"

        if groups_to_wake:
            await GroupConsole.filter(group_id__in=list(groups_to_wake)).update(
                status=True
            )
        if groups_to_sleep:
            await GroupConsole.filter(group_id__in=list(groups_to_sleep)).update(
                status=False
            )

        await GroupMemoryCache.refresh()

        action_str = "醒来" if status else "休眠"
        return f"已完成目标群组的 {action_str} 操作。"
