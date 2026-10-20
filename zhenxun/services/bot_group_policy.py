import asyncio
from datetime import datetime, timezone

from zhenxun.models.bot_group_policy import BotGroupMembership, BotGroupPluginPolicy
from zhenxun.services.db_context.write import CacheUnavailable, on_publication_failed
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

_BLOCK_FIELDS = ("block_plugins", "block_tasks", "forced_plugins", "forced_tasks")
_LEGACY_PLATFORMS = {None, "qq", "qq_client", "OneBot V11", "QQClient"}


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
        self._stale = set()
        self._refresh_tasks = {}
        self._memberships = set()
        self._pending = set()
        self._queue = asyncio.Queue(maxsize=1000)
        self.context = None
        self.loaded = False

    async def start(self, context):
        self._epoch += 1
        self.context = context
        self._policies.clear()
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
        self.loaded = False
        self.context = None
        self._pending.clear()
        self._policies.clear()
        self._stale.clear()
        self._memberships.clear()
        while not self._queue.empty():
            self._queue.get_nowait()
            self._queue.task_done()

    def scene_blocks(
        self,
        bot_id,
        platform_scope,
        group_id,
        channel_id=None,
        *,
        task=False,
    ):
        """读取当前群聊或私聊场景的禁用模块，尚无策略记录时为空

        参数:
            bot_id: 账号
            platform_scope: 平台范围
            group_id: 群号，私聊为空
            channel_id: 频道
            task: 是否为被动技能

        返回:
            tuple[Collection[str], Collection[str]]: 超级管理员禁用与普通禁用的模块
        """
        key = group_key(
            bot_id,
            platform_scope if group_id else "private",
            group_id or self.PRIVATE_KEY,
            channel_id,
        )
        if key in self._stale:
            raise CacheUnavailable("plugin_policy_stale")
        if key in self._policies:
            return self._policies[key][int(task)]
        return (), ()

    def _accept_row(self, key, row):
        if row is None:
            self._policies.pop(key, None)
        else:
            self._policies[key] = tuple(
                (frozenset(forced or []), frozenset(local or []))
                for forced, local in (
                    (row.forced_plugins, row.block_plugins),
                    (row.forced_tasks, row.block_tasks),
                )
            )
        self._stale.discard(key)

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
        initialize = bool(
            group_id
            and self.loaded
            and (key not in self._memberships or key not in self._policies)
        )
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
        except asyncio.QueueFull:
            pass

    async def _write_memberships(self):
        while True:
            batch = [await self._queue.get()]
            while len(batch) < 100 and not self._queue.empty():
                batch.append(self._queue.get_nowait())
            try:
                await self.record_memberships(
                    [key_fields(key) for key in batch], source="event"
                )
            except Exception:
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
                    await BotGroupMembership.update_or_create(
                        defaults=defaults, using_db=connection, **fields
                    )
                    key = group_key(**fields)
                    keys.add(key)
                    if (
                        not await BotGroupPluginPolicy.filter(**fields)
                        .using_db(connection)
                        .exists()
                    ):
                        await self._write(key, lambda current: current)
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
        row = await BotGroupPluginPolicy.get_or_none(**key_fields(key)).using_db(
            connection
        )
        blocks = (
            {field: sorted(getattr(row, field) or []) for field in _BLOCK_FIELDS}
            if row
            else await self._initial_blocks(key, connection)
        )
        state = {
            **key_fields(key),
            **blocks,
            "revision_number": row.revision if row else 0,
            "migration_version": row.migration_version if row else 0,
        }
        state["revision"] = _revision(state)
        return row, state

    async def _initial_blocks(self, key, connection=None):
        """尚无策略记录的作用域的初始禁用列表

        私聊为空；群聊优先导入旧群资料中的禁用列表，否则使用进群默认禁用的插件与被动技能。

        参数:
            key: 作用域
            connection: 数据库连接

        返回:
            dict[str, list[str]]: 四个禁用列表
        """
        from zhenxun.models.group_console import GroupConsole

        if key[1] == "private":
            return {field: [] for field in _BLOCK_FIELDS}
        if key[1] == "qq_client" and (
            legacy := await self._legacy_blocks(key, connection)
        ):
            return legacy
        return {
            "block_plugins": sorted(
                await GroupConsole._get_plugin_modules(default_status=False)
            ),
            "block_tasks": sorted(
                await GroupConsole._get_task_modules(default_status=False)
            ),
            "forced_plugins": [],
            "forced_tasks": [],
        }

    async def _legacy_blocks(self, key, connection=None):
        """从旧群资料导入禁用列表

        仅导入唯一、平台为 QQ 客户端且禁用列表不全为空的旧记录，并补上旧记录建立后
        新增的默认禁用被动技能；不在当前插件目录中的模块会被丢弃。

        参数:
            key: 作用域
            connection: 数据库连接

        返回:
            dict[str, list[str]] | None: 四个禁用列表，没有可导入的旧记录时为 None
        """
        from zhenxun.models.group_console import GroupConsole
        from zhenxun.models.plugin_policy import PluginPolicyMigration

        groups = await GroupConsole.filter(
            group_id=key[2], channel_id=key[3] or None
        ).using_db(connection)
        if len(groups) != 1 or groups[0].platform not in _LEGACY_PLATFORMS:
            return None
        blocks = {
            target: set(_decode_modules(getattr(groups[0], source)))
            for target, source in (
                ("block_plugins", "block_plugin"),
                ("block_tasks", "block_task"),
                ("forced_plugins", "superuser_block_plugin"),
                ("forced_tasks", "superuser_block_task"),
            )
        }
        if not any(blocks.values()):
            return None
        for receipt in await PluginPolicyMigration.filter(
            key__startswith="task-create:"
        ).using_db(connection):
            if [key[2], key[3]] in receipt.previous.get("groups", []):
                blocks["block_tasks"] |= set(receipt.previous.get("block_tasks", []))
        plugins, tasks = plugin_policy_service._catalog_modules(
            await plugin_policy_service.catalog()
        )
        return {
            field: sorted(modules & (tasks if field.endswith("tasks") else plugins))
            for field, modules in blocks.items()
        }

    async def _view(self, key):
        row, state = await self._state(key)
        catalog = await plugin_policy_service.catalog()
        account = await plugin_policy_service.get_account(key[0], catalog=catalog)
        private = key[1] == "private"
        super_group = False
        if not private:
            from zhenxun.models.group_console import GroupConsole

            super_group = await GroupConsole.filter(
                group_id=key[2], channel_id=key[3] or None, is_super=True
            ).exists()
        decisions = {
            kind: plugin_policy_service.decisions(
                catalog[kind],
                private=private,
                account=account[f"block_{kind}"],
                local=state[f"block_{kind}"],
                forced=state[f"forced_{kind}"],
                super_group=super_group,
            )
            for kind in ("plugins", "tasks")
        }
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
        from zhenxun.services.db_context.write import defer, require_publication

        written = False
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
                    modules = plugin_policy_service._catalog_modules(
                        await plugin_policy_service.catalog()
                    )
                    plugins, tasks = plugin_policy_service._validate_modules(
                        modules,
                        changed["block_plugins"],
                        changed["block_tasks"],
                        preserve_plugins=current["block_plugins"],
                        preserve_tasks=current["block_tasks"],
                    )
                    (
                        forced_plugins,
                        forced_tasks,
                    ) = plugin_policy_service._validate_modules(
                        modules,
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
                        "migration_snapshot": (
                            row.migration_snapshot
                            if row and row.migration_version
                            else current
                        ),
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
                    written = True

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
                if written:
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
        forced_plugins=None,
        forced_tasks=None,
    ):
        current = await self.get_group(bot_id, scope, group_id, channel_id)
        key = group_key(current["bot_id"], scope, group_id, channel_id)
        return await self._write(
            key,
            self._replace_blocks(
                block_plugins, block_tasks, forced_plugins, forced_tasks
            ),
            expected_revision,
        )

    async def update_private(
        self, bot_id, *, expected_revision, block_plugins, block_tasks
    ):
        bot_id = await plugin_policy_service._resolve_bot_id(bot_id)
        key = group_key(bot_id, "private", self.PRIVATE_KEY)
        return await self._write(
            key, self._replace_blocks(block_plugins, block_tasks), expected_revision
        )

    @staticmethod
    def _replace_blocks(
        block_plugins, block_tasks, forced_plugins=None, forced_tasks=None
    ):
        """生成整体替换禁用列表的变更函数，超管锁定为空时保持不变

        参数:
            block_plugins: 禁用插件
            block_tasks: 禁用被动技能
            forced_plugins: 超管锁定的插件
            forced_tasks: 超管锁定的被动技能

        返回:
            Callable: 供 _write 使用的变更函数
        """

        replaced = {
            "block_plugins": block_plugins,
            "block_tasks": block_tasks,
            "forced_plugins": forced_plugins,
            "forced_tasks": forced_tasks,
        }

        def change(state):
            return {
                **state,
                **{
                    name: value for name, value in replaced.items() if value is not None
                },
            }

        return change

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

        kind = "tasks" if task else "plugins"
        if enabled:
            fields = [f"block_{kind}"] + [f"forced_{kind}"] * is_superuser
        else:
            fields = [f"{'forced' if force else 'block'}_{kind}"]

        def change(state):
            return {
                **state,
                **{
                    field: sorted(
                        set(state[field]) - modules
                        if enabled
                        else set(state[field]) | modules
                    )
                    for field in fields
                },
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
        """旧群资料迁移概况

        已关联 Bot 的群：有策略记录为 migrated，否则为 pending（首次收到消息或修改时
        按旧群资料或进群默认写入）；尚未关联任何 Bot、但旧禁用列表不为空的旧群资料
        为 unlinked，等 Bot 关联该群时再导入。

        返回:
            list[dict]: 迁移概况
        """
        from zhenxun.models.group_console import GroupConsole

        result = []
        members = await BotGroupMembership.all()
        known = {(row.group_id, row.channel_id) for row in members}
        for member in members:
            key = group_key(
                member.bot_id, member.platform_scope, member.group_id, member.channel_id
            )
            _, state = await self._state(key)
            migrated = bool(state["migration_version"])
            result.append(
                {
                    **state,
                    "status": "migrated" if migrated else "pending",
                    "reason": ""
                    if migrated
                    else "首次收到该群消息或修改策略时按旧群资料或进群默认写入",
                }
            )
        for group in await GroupConsole.all():
            if (group.group_id, group.channel_id or "") in known or not any(
                _decode_modules(getattr(group, field))
                for field in (
                    "block_plugin",
                    "block_task",
                    "superuser_block_plugin",
                    "superuser_block_task",
                )
            ):
                continue
            result.append(
                {
                    "group_id": group.group_id,
                    "channel_id": group.channel_id or "",
                    "status": "unlinked",
                    "reason": "旧群资料尚未关联 Bot，Bot 关联该群时导入",
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

    async def export_document(self) -> dict:
        """导出全部插件策略

        返回:
            dict: 群/私聊作用域、账号、共享策略与全局开关，修订号作为 expected_revision
        """

        def expected(state: dict) -> dict:
            state["expected_revision"] = state.pop("revision")
            return state

        scopes = []
        for row in await BotGroupPluginPolicy.all().order_by(
            "bot_id", "platform_scope", "group_id", "channel_id"
        ):
            _, state = await self._state(
                group_key(row.bot_id, row.platform_scope, row.group_id, row.channel_id)
            )
            scopes.append(expected(state))
        accounts = [
            expected(await plugin_policy_service.get_account(account["bot_id"]))
            for account in await plugin_policy_service.list_accounts()
        ]
        return {
            "version": 1,
            "scopes": scopes,
            "accounts": accounts,
            "policies": [
                expected(policy)
                for policy in await plugin_policy_service.list_policies()
            ],
            "global_policy": expected(await plugin_policy_service.global_state()),
            "global_tasks": expected(
                await plugin_policy_service.global_state(task=True)
            ),
            "authority": "database",
        }

    async def import_document(self, document) -> int:
        """在同一事务内导入插件策略，任一修订号不匹配则整体回滚

        参数:
            document: 导出格式的策略文档

        返回:
            int: 写入的群/私聊作用域数量
        """
        from tortoise.transactions import in_transaction

        from zhenxun.models.bot_console import BotConsole
        from zhenxun.models.plugin_info import PluginInfo
        from zhenxun.models.plugin_policy import PluginPolicy
        from zhenxun.models.task_info import TaskInfo

        service = plugin_policy_service
        scopes = sorted(
            document.scopes,
            key=lambda row: (
                row.bot_id,
                row.platform_scope,
                row.group_id,
                row.channel_id,
            ),
        )
        keys = [
            group_key(row.bot_id, row.platform_scope, row.group_id, row.channel_id)
            for row in scopes
        ]
        if len(set(keys)) != len(keys):
            raise PluginPolicyError("策略作用域重复")
        if len({row.bot_id for row in document.accounts}) != len(
            document.accounts
        ) or len({row.id for row in document.policies}) != len(document.policies):
            raise PluginPolicyError("重复的账号或模板")
        async with runtime_mutation_coordinator.operation("plugin_policy_import"):
            async with in_transaction() as connection:
                locked = [PluginPolicy, BotConsole]
                if document.global_policy:
                    locked.append(PluginInfo)
                if document.global_tasks:
                    locked.append(TaskInfo)
                for model in locked:
                    await (
                        model.all()
                        .using_db(connection)
                        .select_for_update()
                        .order_by("id")
                    )
                for row in document.accounts:
                    current = await service.get_account(row.bot_id)
                    service._check_revision(row.expected_revision, current["revision"])
                if document.global_policy:
                    state = await service.global_state()
                    service._check_revision(
                        document.global_policy.expected_revision, state["revision"]
                    )
                    for module, values in sorted(
                        document.global_policy.plugins.items()
                    ):
                        await service.set_global_settings(
                            module,
                            default_status=values.default_status,
                            block_type=values.block_type
                            or (None if values.status else "ALL"),
                        )
                if document.global_tasks:
                    state = await service.global_state(task=True)
                    service._check_revision(
                        document.global_tasks.expected_revision, state["revision"]
                    )
                    for module, values in sorted(document.global_tasks.tasks.items()):
                        await service.set_global(module, values.status, task=True)
                        await service.set_global(
                            module, values.default_status, task=True, default=True
                        )
                for row in sorted(document.policies, key=lambda item: item.id):
                    await service.update_policy(
                        row.id,
                        expected_revision=row.expected_revision,
                        name=row.name,
                        description=row.description,
                        block_plugins=row.block_plugins,
                        block_tasks=row.block_tasks,
                    )
                for row in sorted(document.accounts, key=lambda item: item.bot_id):
                    current = await service.get_account(row.bot_id)
                    if row.policy_id is not None:
                        await service.bind_accounts(
                            policy_id=row.policy_id,
                            bot_ids=[row.bot_id],
                            expected_revisions={row.bot_id: current["revision"]},
                        )
                    else:
                        await service.update_account(
                            row.bot_id,
                            expected_revision=current["revision"],
                            block_plugins=row.block_plugins,
                            block_tasks=row.block_tasks,
                        )
                for row in scopes:
                    if row.platform_scope == "private":
                        if row.group_id != self.PRIVATE_KEY or row.channel_id:
                            raise PluginPolicyError("私聊策略范围无效")
                    else:
                        await self.get_group(
                            row.bot_id, row.platform_scope, row.group_id, row.channel_id
                        )
                    await self._write(
                        group_key(
                            await service._resolve_bot_id(row.bot_id),
                            row.platform_scope,
                            row.group_id,
                            row.channel_id,
                        ),
                        lambda state, item=row: {
                            **state,
                            "block_plugins": item.block_plugins,
                            "block_tasks": item.block_tasks,
                            "forced_plugins": item.forced_plugins,
                            "forced_tasks": item.forced_tasks,
                        },
                        row.expected_revision,
                    )
        return len(scopes)

    @staticmethod
    def _publish_policy_revision(key):
        from zhenxun.services.cache.runtime_sync import RuntimeCacheMutation

        return RuntimeCacheMutation.publish(
            "permission", "policy_changed", key_fields(key)
        )


bot_group_policy_service = BotGroupPolicyService()


@on_publication_failed
def _mark_policy_stale(key: tuple | None) -> None:
    if key and key[0] == "bot_group_policy":
        bot_group_policy_service._stale.add(tuple(key[1:]))
