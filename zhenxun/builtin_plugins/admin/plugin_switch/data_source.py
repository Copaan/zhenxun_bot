from nonebot.adapters import Bot

from zhenxun.models.group_console import GroupConsole
from zhenxun.services.cache.runtime_cache import GroupMemoryCache
from zhenxun.utils.common_utils import CommonUtils
from zhenxun.utils.enum import BlockType
from zhenxun.utils.platform import PlatformUtils

from .strategy import get_strategy


class PluginManager:
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
        """批量更新状态 (已用策略模式完全重构)"""
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
        for gid in sorted(to_open | to_close):
            await bot_group_policy_service.set_features(
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
        return (
            f"已更新当前账号策略：开启 {len(to_open)} 群，关闭 {len(to_close)} 群。"
            "上层关闭仍然有效。"
        )

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

            await bot_group_policy_service.set_features(
                PlatformUtils.get_storage_bot_id(bot),
                PlatformUtils.get_platform_scope(bot),
                group_id,
                await strategy.get_all_modules(),
                status,
                task=is_task,
                channel_id=channel_id,
                force=use_su_field,
                is_superuser=is_superuser,
            )
            return (
                f"当前账号{'当前群' if group_id else '所有私聊'}的所有{type_str}"
                f"已{'开启' if status else '关闭'}；"
                "上层限制仍然有效。"
            )

        await strategy.set_all_global_status(status)
        return f"成功将所有{type_str}全局状态修改为: {'开启' if status else '关闭'}"

    @classmethod
    async def superuser_set_status(
        cls,
        plugin_name: str,
        status: bool,
        block_type: BlockType | None,
        group_id: str | None,
        is_task: bool = False,
        bot: Bot | None = None,
        channel_id: str | None = None,
    ) -> str:
        strategy = get_strategy(is_task)
        entity = await strategy.get_entity(plugin_name)
        action_cn = "开启" if status else "关闭"

        if entity:
            if group_id:
                if bot is None:
                    from nonebot.matcher import current_bot

                    try:
                        bot = current_bot.get()
                    except LookupError:
                        return "缺少 Bot 作用域，请指定目标账号后重试。"
                return await cls.batch_update_status(
                    plugin_name,
                    {group_id},
                    status,
                    is_task=is_task,
                    is_superuser=True,
                    use_su_field=True,
                    bot=bot,
                    channel_id=channel_id,
                )

            await strategy.set_global_status(entity, status, block_type)
            await strategy.refresh_cache()

            if not block_type or block_type == BlockType.ALL:
                return f"已成功将 {entity.name} 全局{action_cn}!"
            if block_type == BlockType.GROUP:
                return f"已成功将 {entity.name} 全局群组{action_cn}!"
            if block_type == BlockType.PRIVATE:
                return f"已成功将 {entity.name} 全局私聊{action_cn}!"

        return "没有找到这个功能喔..."

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
