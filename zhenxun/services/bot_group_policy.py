import asyncio
from datetime import datetime, timezone

from tortoise.transactions import in_transaction

from zhenxun.models.bot_group_policy import BotGroupMembership, BotGroupPluginPolicy
from zhenxun.services.log import logger
from zhenxun.services.plugin_policy import (
    PluginPolicyError,
    PluginPolicyNotFound,
    _decode_modules,
    _revision,
    plugin_policy_service,
)
from zhenxun.services.runtime_mutation import runtime_mutation_coordinator


def group_key(bot_id, platform_scope, group_id, channel_id=None):
    key = str(bot_id), str(platform_scope), str(group_id), str(channel_id or "")
    if any(len(value) > limit for value, limit in zip(key, (191, 32, 191, 191))):
        raise PluginPolicyError("账号或群标识超过支持长度")
    return key


def key_fields(key):
    return dict(zip(("bot_id", "platform_scope", "group_id", "channel_id"), key))


class BotGroupPolicyService:
    def __init__(self):
        self._policies = {}
        self._memberships = set()
        self._pending = set()
        self._queue = asyncio.Queue(maxsize=1000)
        self.context = None
        self.loaded = False
        self.write_counts = {
            "queued": 0,
            "capacity_skipped": 0,
            "shutdown_pending": 0,
            "write_failed": 0,
            "written": 0,
        }

    async def start(self, context):
        self.context = context
        self._policies = {
            group_key(row.bot_id, row.platform_scope, row.group_id, row.channel_id): (
                frozenset(row.block_plugins),
                frozenset(row.block_tasks),
            )
            for row in await BotGroupPluginPolicy.all()
        }
        self._memberships = {
            group_key(row.bot_id, row.platform_scope, row.group_id, row.channel_id)
            for row in await BotGroupMembership.all()
        }
        self.loaded = True
        context.add_finalizer(self.close)
        context.spawn_task(self._write_memberships(), name="bot-group-memberships")

    def close(self):
        self.write_counts["shutdown_pending"] += len(self._pending)
        self.loaded = False
        self.context = None
        self._pending.clear()
        self._policies.clear()
        self._memberships.clear()
        while not self._queue.empty():
            self._queue.get_nowait()
            self._queue.task_done()

    def blocked(self, bot_id, scope, group_id, module, *, task=False, channel_id=None):
        if not group_id:
            return False
        state = self._policies.get(group_key(bot_id, scope, group_id, channel_id))
        return bool(state and module in state[int(task)])

    def observe(self, bot_id, scope, group_id, channel_id=None):
        if not group_id or not self.context or not self.context.accepting:
            return
        key = group_key(bot_id, scope, group_id, channel_id)
        if key in self._memberships or key in self._pending:
            return
        try:
            self._queue.put_nowait(key)
            self._pending.add(key)
            self.write_counts["queued"] += 1
        except asyncio.QueueFull:
            self.write_counts["capacity_skipped"] += 1

    async def _write_memberships(self):
        while True:
            batch = [await self._queue.get()]
            while len(batch) < 100 and not self._queue.empty():
                batch.append(self._queue.get_nowait())
            try:
                await self.record_memberships(
                    [key_fields(key) for key in batch], source="event"
                )
                self.write_counts["written"] += len(batch)
            except Exception:
                self.write_counts["write_failed"] += len(batch)
                logger.warning(
                    "Bot群关联写入失败 | code=group_membership_write_failed",
                    "PluginPolicy",
                )
                await asyncio.sleep(1)
            finally:
                for key in batch:
                    self._pending.discard(key)
                    self._queue.task_done()

    async def record_memberships(self, entries, *, source):
        if not entries:
            return
        keys = set()
        async with runtime_mutation_coordinator.operation("bot_group_membership"):
            async with in_transaction() as connection:
                for entry in entries:
                    fields = {
                        k: entry[k]
                        for k in ("bot_id", "platform_scope", "group_id", "channel_id")
                    }
                    defaults = {
                        "source": source,
                        "last_seen": datetime.now(timezone.utc),
                    }
                    if entry.get("group_name"):
                        defaults["group_name"] = entry["group_name"]
                    await BotGroupMembership.update_or_create(
                        defaults=defaults, using_db=connection, **fields
                    )
                    keys.add(group_key(**fields))
            self._memberships.update(keys)

    async def list_groups(self, bot_id):
        bot_id = await plugin_policy_service._resolve_bot_id(bot_id)
        account = await plugin_policy_service.get_account(bot_id)
        rows = await BotGroupMembership.filter(bot_id=bot_id).order_by(
            "group_name", "group_id"
        )
        return [
            {
                "bot_id": bot_id,
                "platform_scope": row.platform_scope,
                "group_id": row.group_id,
                "channel_id": row.channel_id,
                "group_name": row.group_name or row.group_id,
                "last_seen": row.last_seen.isoformat(),
                "source": row.source,
                "connected": account["connected"],
            }
            for row in rows
        ]

    async def get_group(self, bot_id, scope, group_id, channel_id=""):
        bot_id = await plugin_policy_service._resolve_bot_id(bot_id)
        key = group_key(bot_id, scope, group_id, channel_id)
        fields = key_fields(key)
        membership = await BotGroupMembership.get_or_none(**fields)
        if membership is None:
            raise PluginPolicyNotFound("该账号没有此群的关联记录")
        policy = await BotGroupPluginPolicy.get_or_none(**fields)
        plugins = sorted(policy.block_plugins if policy else [])
        tasks = sorted(policy.block_tasks if policy else [])
        state = {**fields, "block_plugins": plugins, "block_tasks": tasks}
        account = await plugin_policy_service.get_account(bot_id)
        from zhenxun.services.cache.runtime_cache import GroupMemoryCache

        group = (
            None
            if scope == "qq_api"
            else await GroupMemoryCache.get(group_id, channel_id or None)
        )
        catalog_plugins, catalog_tasks = await plugin_policy_service._catalog_modules()
        return {
            **state,
            "revision": _revision(state),
            "group_name": membership.group_name or group_id,
            "account_block_plugins": account["block_plugins"],
            "account_block_tasks": account["block_tasks"],
            "group_block_plugins": sorted(
                set(_decode_modules(getattr(group, "block_plugin", "")))
                | set(_decode_modules(getattr(group, "superuser_block_plugin", "")))
            ),
            "group_block_tasks": sorted(
                set(_decode_modules(getattr(group, "block_task", "")))
                | set(_decode_modules(getattr(group, "superuser_block_task", "")))
            ),
            "missing_plugins": sorted(set(plugins) - catalog_plugins),
            "missing_tasks": sorted(set(tasks) - catalog_tasks),
        }

    PRIVATE_KEY = "__all__"

    async def get_private(self, bot_id):
        bot_id = await plugin_policy_service._resolve_bot_id(bot_id)
        key = group_key(bot_id, "private", self.PRIVATE_KEY, "")
        row = await BotGroupPluginPolicy.get_or_none(**key_fields(key))
        account = await plugin_policy_service.get_account(bot_id)
        plugins = sorted(row.block_plugins if row else [])
        tasks = sorted(row.block_tasks if row else [])
        return {
            **key_fields(key),
            "block_plugins": plugins,
            "block_tasks": tasks,
            "account_block_plugins": account["block_plugins"],
            "account_block_tasks": account["block_tasks"],
            "revision": _revision({"block_plugins": plugins, "block_tasks": tasks}),
        }

    async def update_private(
        self, bot_id, *, expected_revision, block_plugins, block_tasks
    ):
        async with runtime_mutation_coordinator.operation("bot_private_policy"):
            current = await self.get_private(bot_id)
            plugin_policy_service._check_revision(
                expected_revision, current["revision"]
            )
            plugins, tasks = await plugin_policy_service._validate_modules(
                block_plugins, block_tasks
            )
            key = group_key(current["bot_id"], "private", self.PRIVATE_KEY, "")
            async with in_transaction() as connection:
                await BotGroupPluginPolicy.update_or_create(
                    defaults={"block_plugins": plugins, "block_tasks": tasks},
                    using_db=connection,
                    **key_fields(key),
                )
            self._policies[key] = (frozenset(plugins), frozenset(tasks))
            return await self.get_private(current["bot_id"])

    async def update_group(
        self,
        bot_id,
        scope,
        group_id,
        channel_id,
        *,
        expected_revision,
        block_plugins,
        block_tasks,
    ):
        async with runtime_mutation_coordinator.operation("bot_group_policy"):
            current = await self.get_group(bot_id, scope, group_id, channel_id)
            plugin_policy_service._check_revision(
                expected_revision, current["revision"]
            )
            plugins, tasks = await plugin_policy_service._validate_modules(
                block_plugins,
                block_tasks,
                preserve_plugins=current["missing_plugins"],
                preserve_tasks=current["missing_tasks"],
            )
            key = group_key(current["bot_id"], scope, group_id, channel_id)
            async with in_transaction() as connection:
                await BotGroupPluginPolicy.update_or_create(
                    defaults={"block_plugins": plugins, "block_tasks": tasks},
                    using_db=connection,
                    **key_fields(key),
                )
            self._policies[key] = (frozenset(plugins), frozenset(tasks))
            return await self.get_group(*key)


bot_group_policy_service = BotGroupPolicyService()
