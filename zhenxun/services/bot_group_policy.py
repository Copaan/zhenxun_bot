import asyncio
from datetime import datetime, timezone

from zhenxun.models.bot_group_policy import BotGroupMembership, BotGroupPluginPolicy
from zhenxun.services.log import logger
from zhenxun.services.plugin_policy import (
    PluginPolicyError,
    PluginPolicyNotFound,
    _decode_modules,
    _revision,
    plugin_policy_service,
    policy_transaction,
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
        self._epoch = 0
        self._policies = {}
        self._migrated = set()
        self._superuser_exempt = {}
        self._stale = set()
        self._refresh_tasks = {}
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
        self._epoch += 1
        self.context = context
        self._policies.clear()
        self._migrated.clear()
        self._superuser_exempt.clear()
        self._stale.clear()
        for row in await BotGroupPluginPolicy.all():
            self._accept_row(
                group_key(row.bot_id, row.platform_scope, row.group_id, row.channel_id),
                row,
            )
        self._memberships = {
            group_key(row.bot_id, row.platform_scope, row.group_id, row.channel_id)
            for row in await BotGroupMembership.all()
        }
        self.loaded = True
        context.add_finalizer(self.close)
        context.spawn_task(self._write_memberships(), name="bot-group-memberships")

    def close(self):
        self._epoch += 1
        for task in self._refresh_tasks.values():
            task.cancel()
        self._refresh_tasks.clear()
        self.write_counts["shutdown_pending"] += len(self._pending)
        self.loaded = False
        self.context = None
        self._pending.clear()
        self._policies.clear()
        self._migrated.clear()
        self._superuser_exempt.clear()
        self._stale.clear()
        self._memberships.clear()
        while not self._queue.empty():
            self._queue.get_nowait()
            self._queue.task_done()

    def blocked(
        self,
        bot_id,
        scope,
        group_id,
        module,
        *,
        task=False,
        channel_id=None,
        is_superuser=False,
    ):
        if not group_id:
            return False
        key = group_key(bot_id, scope, group_id, channel_id)
        if key in self._stale:
            raise PluginPolicyError("策略缓存待重新核验")
        state = self._policies.get(key)
        if is_superuser and module in self._superuser_exempt.get(key, ()):
            return False
        return bool(state and module in state[int(task)])

    def _accept_row(self, key, row):
        if row is None:
            self._policies.pop(key, None)
            self._migrated.discard(key)
            self._superuser_exempt.pop(key, None)
        else:
            self._policies[key] = (
                frozenset((row.block_plugins or []) + (row.forced_plugins or [])),
                frozenset((row.block_tasks or []) + (row.forced_tasks or [])),
            )
            if row.migration_version:
                self._migrated.add(key)
            else:
                self._migrated.discard(key)
            self._superuser_exempt[key] = frozenset(
                (row.migration_snapshot or {}).get("superuser_exempt_plugins", [])
            )
        self._stale.discard(key)

    def migrated(self, bot_id, scope, group_id, channel_id=None):
        return group_key(bot_id, scope, group_id, channel_id) in self._migrated

    async def refresh_policy(self, key):
        epoch = self._epoch
        row = await BotGroupPluginPolicy.get_or_none(**key_fields(key))
        if epoch != self._epoch:
            return
        self._accept_row(key, row)

    async def ensure_fresh(self, bot_id, scope, group_id, channel_id=None):
        key = group_key(
            bot_id,
            scope if group_id else "private",
            group_id or self.PRIVATE_KEY,
            channel_id,
        )
        initialize = bool(group_id and self.loaded and key not in self._memberships)
        if key not in self._stale and not initialize:
            return
        task = self._refresh_tasks.get(key)
        if task is None:
            task = asyncio.create_task(
                self.record_memberships([key_fields(key)], source="event")
                if initialize
                else self.refresh_policy(key)
            )
            self._refresh_tasks[key] = task

            def completed(done):
                if self._refresh_tasks.get(key) is done:
                    self._refresh_tasks.pop(key, None)
                if not done.cancelled():
                    done.exception()

            task.add_done_callback(completed)
        await asyncio.shield(task)

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
            async with policy_transaction() as connection:
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
                    is_new = (
                        not await BotGroupMembership.filter(**fields)
                        .using_db(connection)
                        .exists()
                    )
                    await BotGroupMembership.update_or_create(
                        defaults=defaults, using_db=connection, **fields
                    )
                    keys.add(group_key(**fields))
                    if is_new:
                        key = group_key(**fields)
                        row, state = await self._state(key, connection)
                        if row is None:
                            from zhenxun.models.group_console import GroupConsole
                            from zhenxun.models.plugin_info import PluginInfo
                            from zhenxun.models.task_info import TaskInfo

                            legacy = (
                                key[1] == "qq_client"
                                and await GroupConsole.filter(
                                    group_id=key[2], channel_id=key[3] or None
                                )
                                .using_db(connection)
                                .exists()
                            )
                            if not legacy:
                                state["block_plugins"] = list(
                                    await PluginInfo.filter(default_status=False)
                                    .using_db(connection)
                                    .values_list("module", flat=True)
                                )
                                state["block_tasks"] = list(
                                    await TaskInfo.filter(default_status=False)
                                    .using_db(connection)
                                    .values_list("module", flat=True)
                                )
                            await self._write(
                                key,
                                lambda current, initial=state: {
                                    **current,
                                    "block_plugins": initial["block_plugins"],
                                    "block_tasks": initial["block_tasks"],
                                },
                            )
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

    PRIVATE_KEY = "__all__"

    async def _state(self, key, connection=None):
        from zhenxun.models.group_console import GroupConsole

        row = await BotGroupPluginPolicy.get_or_none(**key_fields(key)).using_db(
            connection
        )
        original_plugins = set(row.block_plugins or []) if row else set()
        state = {
            **key_fields(key),
            "block_plugins": sorted(row.block_plugins or []) if row else [],
            "block_tasks": sorted(row.block_tasks or []) if row else [],
            "forced_plugins": sorted(row.forced_plugins or []) if row else [],
            "forced_tasks": sorted(row.forced_tasks or []) if row else [],
            "revision_number": row.revision if row else 0,
            "migration_version": row.migration_version if row else 0,
            "superuser_exempt_plugins": (row.migration_snapshot or {}).get(
                "superuser_exempt_plugins", []
            )
            if row
            else [],
        }
        if not state["migration_version"] and key[1] == "qq_client":
            from zhenxun.models.plugin_policy import PluginPolicyMigration

            for receipt in await PluginPolicyMigration.filter(
                key__startswith="task-create:"
            ).using_db(connection):
                if [key[2], key[3]] in receipt.previous.get("groups", []):
                    state["block_tasks"] = sorted(
                        set(state["block_tasks"])
                        | set(receipt.previous.get("block_tasks", []))
                    )
            query = GroupConsole.filter(group_id=key[2], channel_id=key[3] or None)
            groups = await query.using_db(connection)
            if len(groups) > 1:
                raise PluginPolicyError("旧群策略归属不唯一，需要核验后迁移")
            if groups:
                group = groups[0]
                if group.platform not in {
                    None,
                    "qq",
                    "qq_client",
                    "OneBot V11",
                    "QQClient",
                }:
                    raise PluginPolicyError("旧群策略平台归属需要核验")
                for target, source in (
                    ("block_plugins", "block_plugin"),
                    ("block_tasks", "block_task"),
                    ("forced_plugins", "superuser_block_plugin"),
                    ("forced_tasks", "superuser_block_task"),
                ):
                    state[target] = sorted(
                        set(state[target])
                        | set(_decode_modules(getattr(group, source)))
                    )
        if not state["migration_version"]:
            state["superuser_exempt_plugins"] = sorted(
                (set(state["block_plugins"]) | set(state["forced_plugins"]))
                - original_plugins
            )
        state["revision"] = _revision(state)
        return row, state

    async def _view(self, key):
        row, state = await self._state(key)
        account = await plugin_policy_service.get_account(key[0])
        catalog = await plugin_policy_service.catalog()
        decisions = {}
        private = key[1] == "private"
        super_group = False
        if key[1] == "qq_client":
            from zhenxun.models.group_console import GroupConsole

            legacy_group = await GroupConsole.get_or_none(
                group_id=key[2], channel_id=key[3] or None
            )
            super_group = bool(legacy_group and legacy_group.is_super)
        for kind in ("plugins", "tasks"):
            values = []
            local = set(state[f"block_{kind}"])
            forced = set(state[f"forced_{kind}"])
            upper = set(account[f"block_{kind}"])
            for item in catalog[kind]:
                module = item["module"]
                reasons = []
                if not item["load_status"]:
                    reasons.append("not_loaded")
                block_type = getattr(
                    item.get("block_type"), "value", item.get("block_type")
                )
                if (
                    (
                        not item["global_status"]
                        and block_type in {None, "ALL"}
                        and not (super_group and block_type == "ALL")
                    )
                    or (private and block_type == "PRIVATE")
                    or (not private and block_type == "GROUP")
                ):
                    reasons.append("global_disabled")
                if module in upper:
                    reasons.append("account_disabled")
                if module in forced:
                    reasons.append("administrator_disabled")
                if module in local:
                    reasons.append("private_disabled" if private else "group_disabled")
                values.append(
                    {
                        "module": module,
                        "local_enabled": module not in local,
                        "effective_enabled": not reasons,
                        "blocked_by": reasons,
                    }
                )
            decisions[kind] = values
        return {
            **state,
            "scope": "private" if private else key[1],
            "scope_label": "所有私聊" if private else "当前账号群聊设置",
            "account_block_plugins": account["block_plugins"],
            "account_block_tasks": account["block_tasks"],
            "group_block_plugins": state["forced_plugins"],
            "group_block_tasks": state["forced_tasks"],
            "private_policy_exists": row is not None,
            "missing_plugins": sorted(
                set(state["block_plugins"]) - {p["module"] for p in catalog["plugins"]}
            ),
            "missing_tasks": sorted(
                set(state["block_tasks"]) - {p["module"] for p in catalog["tasks"]}
            ),
            "effective": decisions,
            "source": "database",
            "editable_scope": key_fields(key),
            "publication_pending": key in self._stale,
        }

    async def get_group(self, bot_id, scope, group_id, channel_id=""):
        bot_id = await plugin_policy_service._resolve_bot_id(bot_id)
        key = group_key(bot_id, scope, group_id, channel_id)
        membership = await BotGroupMembership.get_or_none(**key_fields(key))
        if membership is None:
            raise PluginPolicyNotFound("该账号没有此群的关联记录")
        return {
            **await self._view(key),
            "group_name": membership.group_name or group_id,
        }

    async def get_private(self, bot_id):
        bot_id = await plugin_policy_service._resolve_bot_id(bot_id)
        return await self._view(group_key(bot_id, "private", self.PRIVATE_KEY))

    async def _write(self, key, transform, expected_revision=None):
        from zhenxun.models.bot_console import BotConsole
        from zhenxun.services.cache.write import defer, require_publication

        async with runtime_mutation_coordinator.operation("plugin_policy_scope"):
            try:
                async with policy_transaction() as connection:
                    # A stable parent row also serializes the first scope insert.
                    parent = (
                        await BotConsole.filter(bot_id=key[0])
                        .using_db(connection)
                        .select_for_update()
                        .first()
                    )
                    if parent is None:
                        raise PluginPolicyNotFound("机器人账号不存在")
                    row, current = await self._state(key, connection)
                    if expected_revision is not None:
                        plugin_policy_service._check_revision(
                            expected_revision, current["revision"]
                        )
                    changed = transform(current)
                    plugins, tasks = await plugin_policy_service._validate_modules(
                        changed["block_plugins"],
                        changed["block_tasks"],
                        preserve_plugins=current["block_plugins"],
                        preserve_tasks=current["block_tasks"],
                    )
                    (
                        forced_plugins,
                        forced_tasks,
                    ) = await plugin_policy_service._validate_modules(
                        changed["forced_plugins"],
                        changed["forced_tasks"],
                        preserve_plugins=current["forced_plugins"],
                        preserve_tasks=current["forced_tasks"],
                    )
                    fields = {
                        "block_plugins": plugins,
                        "block_tasks": tasks,
                        "forced_plugins": forced_plugins,
                        "forced_tasks": forced_tasks,
                        "migration_version": 1,
                        "migration_snapshot": {
                            **(
                                row.migration_snapshot
                                if row and row.migration_version
                                else current
                            ),
                            "superuser_exempt_plugins": changed[
                                "superuser_exempt_plugins"
                            ],
                        },
                        "revision": current["revision_number"] + 1,
                    }
                    if row is None:
                        row = await BotGroupPluginPolicy.create(
                            **key_fields(key), **fields, using_db=connection
                        )
                    else:
                        for name, value in fields.items():
                            setattr(row, name, value)
                        await row.save(using_db=connection, update_fields=list(fields))

                    async def publish():
                        try:
                            self._accept_row(key, row)
                            require_publication(self._publish_policy_revision(key))
                        except BaseException:
                            self._stale.add(key)
                            raise

                    if not defer(("bot_group_policy", *key), publish):
                        raise RuntimeError("policy_transaction_publication_unavailable")
            except BaseException:
                self._stale.add(key)
                raise
        return await self._view(key)

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
        current = await self.get_group(bot_id, scope, group_id, channel_id)
        key = group_key(current["bot_id"], scope, group_id, channel_id)
        return await self._write(
            key,
            lambda state: {
                **state,
                "block_plugins": block_plugins,
                "block_tasks": block_tasks,
            },
            expected_revision,
        )

    async def update_private(
        self, bot_id, *, expected_revision, block_plugins, block_tasks
    ):
        bot_id = await plugin_policy_service._resolve_bot_id(bot_id)
        key = group_key(bot_id, "private", self.PRIVATE_KEY)
        return await self._write(
            key,
            lambda state: {
                **state,
                "block_plugins": block_plugins,
                "block_tasks": block_tasks,
            },
            expected_revision,
        )

    async def set_features(
        self,
        bot_id,
        scope,
        group_id,
        modules,
        enabled,
        *,
        task=False,
        channel_id=None,
        force=False,
        is_superuser=False,
    ):
        if force and not is_superuser:
            raise PluginPolicyError("需要超级用户权限")
        bot_id = await plugin_policy_service._resolve_bot_id(bot_id)
        key = group_key(
            bot_id,
            scope if group_id else "private",
            group_id or self.PRIVATE_KEY,
            channel_id,
        )
        modules = set(modules)

        def change(state):
            field = ("forced_" if force else "block_") + (
                "tasks" if task else "plugins"
            )
            old = set(state[field])
            return {
                **state,
                field: sorted(old - modules if enabled else old | modules),
                "superuser_exempt_plugins": sorted(
                    set(state["superuser_exempt_plugins"]) - modules
                ),
            }

        return await self._write(key, change)

    async def legacy_set_feature(
        self,
        group_id,
        module,
        enabled,
        *,
        task=False,
        is_superuser=False,
        platform=None,
        channel_id=None,
    ):
        bot_id, scope = await self._legacy_identity(group_id, channel_id, platform)
        return await self.set_features(
            bot_id,
            scope,
            group_id,
            [module],
            enabled,
            task=task,
            channel_id=channel_id,
            force=is_superuser,
            is_superuser=is_superuser,
        )

    async def _legacy_identity(self, group_id, channel_id=None, platform=None):
        from nonebot.matcher import current_bot

        from zhenxun.utils.platform import PlatformUtils

        try:
            bot = current_bot.get()
        except LookupError:
            bot = None
        if bot is not None:
            return PlatformUtils.get_storage_bot_id(
                bot
            ), PlatformUtils.get_platform_scope(bot)
        query = BotGroupMembership.filter(
            group_id=str(group_id), channel_id=str(channel_id or "")
        )
        if platform in {"qq_client", "qq_api"}:
            query = query.filter(platform_scope=platform)
        identities = {(row.bot_id, row.platform_scope) for row in await query}
        if len(identities) != 1:
            raise PluginPolicyError("旧接口缺少唯一 Bot 作用域，请使用策略服务指定账号")
        return identities.pop()

    async def legacy_blocked(
        self, group_id, module, *, channel_id=None, kind="plugins", forced=None
    ):
        try:
            bot_id, scope = await self._legacy_identity(group_id, channel_id)
        except PluginPolicyError:
            if not await BotGroupMembership.filter(
                group_id=str(group_id), channel_id=str(channel_id or "")
            ).exists():
                return None
            raise
        key = group_key(bot_id, scope, group_id, channel_id)
        _, state = await self._state(key)
        fields = (
            [f"forced_{kind}"]
            if forced is True
            else [f"block_{kind}"]
            if forced is False
            else [f"forced_{kind}", f"block_{kind}"]
        )
        return any(module in state[field] for field in fields)

    async def migration_preview(self):
        from zhenxun.models.group_console import GroupConsole

        result = []
        members = await BotGroupMembership.all()
        known = {
            (row.group_id, row.channel_id)
            for row in members
            if row.platform_scope == "qq_client"
        }
        for member in members:
            key = group_key(
                member.bot_id, member.platform_scope, member.group_id, member.channel_id
            )
            try:
                _, state = await self._state(key)
                result.append(
                    {
                        "status": "migrated"
                        if state["migration_version"]
                        else "pending",
                        **state,
                    }
                )
            except PluginPolicyError as error:
                result.append(
                    {**key_fields(key), "status": "needs_review", "reason": str(error)}
                )
        for group in await GroupConsole.all():
            if (group.group_id, group.channel_id or "") not in known:
                result.append(
                    {
                        "group_id": group.group_id,
                        "channel_id": group.channel_id or "",
                        "status": "needs_review",
                        "reason": "旧群资料尚无可核验 Bot 关联，未自动选择账号",
                    }
                )
        return result

    async def initialize_created_tasks(self, modules):
        from zhenxun.models.group_console import GroupConsole
        from zhenxun.models.plugin_policy import PluginPolicyMigration

        modules = sorted(set(modules))
        if not modules:
            return
        key = "task-create:" + _revision({"modules": modules})
        async with runtime_mutation_coordinator.operation("plugin_policy_new_tasks"):
            async with policy_transaction() as connection:
                if (
                    await PluginPolicyMigration.filter(key=key)
                    .using_db(connection)
                    .exists()
                ):
                    return
                groups = await GroupConsole.all().using_db(connection)
                await PluginPolicyMigration.create(
                    key=key,
                    previous={
                        "groups": [
                            [row.group_id, row.channel_id or ""] for row in groups
                        ],
                        "block_tasks": modules,
                    },
                    using_db=connection,
                )
                for member in (
                    await BotGroupMembership.all()
                    .using_db(connection)
                    .order_by("bot_id", "group_id", "channel_id")
                ):
                    scope_key = group_key(
                        member.bot_id,
                        member.platform_scope,
                        member.group_id,
                        member.channel_id,
                    )
                    await self._write(
                        scope_key,
                        lambda state: {
                            **state,
                            "block_tasks": sorted(
                                set(state["block_tasks"]) | set(modules)
                            ),
                        },
                    )

    async def migrate_scope(
        self, bot_id, scope, group_id, channel_id, expected_revision
    ):
        key = group_key(
            await plugin_policy_service._resolve_bot_id(bot_id),
            scope,
            group_id,
            channel_id,
        )
        _, state = await self._state(key)
        plugin_policy_service._check_revision(expected_revision, state["revision"])
        if state["migration_version"]:
            return await self._view(key)
        return await self._write(key, lambda current: current, expected_revision)

    @staticmethod
    def _publish_policy_revision(key):
        from zhenxun.services.cache.runtime_cache import RuntimeCacheMutation

        return RuntimeCacheMutation.publish(
            "permission", "policy_changed", key_fields(key)
        )


bot_group_policy_service = BotGroupPolicyService()
