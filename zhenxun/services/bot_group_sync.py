import asyncio
import time

import nonebot
from nonebot_plugin_uninfo import get_interface
from nonebot_plugin_uninfo.adapters import alter_get_fetcher

from zhenxun.models.group_console import GroupConsole
from zhenxun.services.bot_group_policy import bot_group_policy_service
from zhenxun.services.cache.runtime_cache import GroupMemoryCache
from zhenxun.services.log import logger
from zhenxun.services.runtime_mutation import runtime_mutation_coordinator
from zhenxun.utils.platform import PlatformUtils


class BotGroupSync:
    """One bounded query sequence per live connection; no network under DB locks."""

    def __init__(self):
        self.context = None
        self.connections = {}
        self.status = {}

    def start(self, context):
        self.context = context
        context.add_finalizer(self.close)
        for bot in nonebot.get_bots().values():
            self.connect(bot)

    def close(self):
        self.context = None
        for _, task in self.connections.values():
            task.cancel()
        self.connections.clear()
        self.status.clear()

    def connect(self, bot):
        if not self.context or not self.context.accepting:
            return
        key = PlatformUtils.get_storage_bot_id(bot)
        previous = self.connections.get(key)
        if previous:
            previous[1].cancel()
        task = self.context.spawn_task(
            self._sync(bot, key), name="bot-group-sync", persistent=False
        )
        self.connections[key] = (bot, task)

    def disconnect(self, bot):
        key = PlatformUtils.get_storage_bot_id(bot)
        current = self.connections.get(key)
        if current and current[0] is bot:
            current[1].cancel()
            self.connections.pop(key, None)
            self.status.pop(key, None)

    def _current(self, bot, key):
        current = self.connections.get(key)
        return bool(
            self.context
            and self.context.accepting
            and current
            and current[0] is bot
            and current[1] is asyncio.current_task()
        )

    async def _sync(self, bot, key):
        started = time.monotonic()
        scope = PlatformUtils.get_platform_scope(bot)
        for attempt, offset in enumerate((0, 1, 4, 14, 44), 1):
            await asyncio.sleep(max(0, started + offset - time.monotonic()))
            if not self._current(bot, key):
                return
            groups = []
            code = "group_query_empty"
            try:
                if get_interface(bot) is None:
                    alter_get_fetcher(bot.adapter.get_name())
                if get_interface(bot) is None:
                    code = "group_query_interface_missing"
                else:
                    groups, _ = await asyncio.wait_for(
                        PlatformUtils.get_group_list(bot), timeout=5.0
                    )
                    if groups:
                        code = "group_query_ok"
            except asyncio.TimeoutError:
                code = "group_query_timeout"
            except NotImplementedError:
                code = "group_query_unsupported"
            except Exception as error:
                from nonebot.exception import NetworkError

                code = (
                    "group_query_network_failed"
                    if isinstance(error, NetworkError | OSError)
                    else "group_query_failed"
                )
            if not self._current(bot, key):
                return
            self.status[key] = {"source": "uninfo", "attempt": attempt, "code": code}
            if groups:
                async with runtime_mutation_coordinator.operation("bot_group_sync"):
                    if not self._current(bot, key):
                        return
                    if scope == "qq_client":
                        tasks = await GroupConsole._get_task_modules(
                            default_status=False
                        )
                        plugins = await GroupConsole._get_plugin_modules(
                            default_status=False
                        )
                        for candidate in groups:
                            (
                                group,
                                created,
                            ) = await GroupConsole.get_or_create_root_group(
                                candidate.group_id,
                                defaults={
                                    "group_name": candidate.group_name,
                                    "group_flag": 1,
                                },
                            )
                            if created and (tasks or plugins):
                                await GroupConsole._update_modules(
                                    group, tasks, plugins
                                )
                            await GroupMemoryCache.upsert_from_model(group)
                    await bot_group_policy_service.record_memberships(
                        [
                            {
                                "bot_id": key,
                                "platform_scope": scope,
                                "group_id": str(group.group_id),
                                "channel_id": str(group.channel_id or ""),
                                "group_name": group.group_name or "",
                            }
                            for group in groups
                        ],
                        source="sync",
                    )
                logger.info(
                    "群同步完成 | source=uninfo | "
                    f"attempt={attempt} | count={len(groups)}",
                    "群认证同步",
                )
                return
            if code == "group_query_unsupported":
                break
        logger.info(
            "群同步未取得数据，保留已有群认证 | source=uninfo | "
            f"attempt={attempt} | code={code}",
            "群认证同步",
        )


bot_group_sync = BotGroupSync()
