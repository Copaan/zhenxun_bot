from __future__ import annotations

from collections.abc import Iterable
from datetime import datetime, timezone
import hashlib
import json
from typing import Any, Literal

import nonebot
from tortoise.backends.base.client import BaseDBAsyncClient
from tortoise.expressions import F
from tortoise.transactions import in_transaction

from zhenxun.models.bot_console import BotConsole
from zhenxun.models.plugin_info import PluginInfo
from zhenxun.models.plugin_policy import BotPluginPolicyBinding, PluginPolicy
from zhenxun.models.task_info import TaskInfo
from zhenxun.services.cache.runtime_cache import BotMemoryCache
from zhenxun.services.log import logger
from zhenxun.services.runtime_mutation import runtime_mutation_coordinator
from zhenxun.utils.enum import PluginType
from zhenxun.utils.platform import PlatformUtils

FeatureKind = Literal["plugins", "tasks"]
CatalogModules = tuple[set[str], set[str]]
_MANAGED_PLUGIN_TYPES = {
    PluginType.NORMAL,
    PluginType.DEPENDANT,
    PluginType.ADMIN,
    PluginType.SUPERUSER,
    PluginType.SUPER_AND_ADMIN,
}


class PluginPolicyError(ValueError):
    code = "plugin_policy_invalid"

    def __init__(self, message: str, *, details: dict[str, Any] | None = None):
        super().__init__(message)
        self.details = details or {}


class PluginPolicyConflict(PluginPolicyError):
    code = "plugin_policy_revision_conflict"


class PluginPolicyNotFound(PluginPolicyError):
    code = "plugin_policy_not_found"


class PluginPolicyInUse(PluginPolicyError):
    code = "plugin_policy_in_use"


def _normalize_modules(values: Iterable[str] | None) -> list[str]:
    return sorted({str(value).strip() for value in values or [] if str(value).strip()})


def _decode_modules(value: str | None) -> list[str]:
    return _normalize_modules(BotConsole.convert_module_format(value or ""))


def _encode_modules(values: Iterable[str]) -> str:
    return str(BotConsole.convert_module_format(_normalize_modules(values)))


def _revision(payload: dict[str, Any]) -> str:
    content = json.dumps(
        payload, ensure_ascii=True, sort_keys=True, separators=(",", ":")
    )
    return hashlib.sha256(content.encode()).hexdigest()


class PluginPolicyService:
    async def catalog(self) -> dict[str, list[dict[str, Any]]]:
        plugins = await PluginInfo.get_plugins(load_status=None, filter_parent=False)
        tasks = await TaskInfo.get_tasks(status=None, load_status=None)
        return {
            "plugins": [
                {
                    "module": plugin.module,
                    "name": plugin.name,
                    "feature_type": (
                        plugin.plugin_type.value if plugin.plugin_type else "NORMAL"
                    ),
                    "menu_type": plugin.menu_type or "",
                    "load_status": bool(plugin.load_status),
                    "global_status": bool(plugin.status),
                }
                for plugin in plugins
                if plugin.plugin_type in _MANAGED_PLUGIN_TYPES
            ],
            "tasks": [
                {
                    "module": task.module,
                    "name": task.name,
                    "feature_type": "TASK",
                    "menu_type": "",
                    "load_status": bool(task.load_status),
                    "global_status": bool(task.status),
                }
                for task in tasks
            ],
        }

    async def _catalog_modules(self) -> tuple[set[str], set[str]]:
        catalog = await self.catalog()
        return (
            {item["module"] for item in catalog["plugins"]},
            {item["module"] for item in catalog["tasks"]},
        )

    async def _validate_modules(
        self,
        block_plugins: Iterable[str],
        block_tasks: Iterable[str],
        *,
        preserve_plugins: Iterable[str] = (),
        preserve_tasks: Iterable[str] = (),
    ) -> tuple[list[str], list[str]]:
        plugins = _normalize_modules(block_plugins)
        tasks = _normalize_modules(block_tasks)
        catalog_plugins, catalog_tasks = await self._catalog_modules()
        invalid_plugins = sorted(set(plugins) - catalog_plugins - set(preserve_plugins))
        invalid_tasks = sorted(set(tasks) - catalog_tasks - set(preserve_tasks))
        if invalid_plugins or invalid_tasks:
            raise PluginPolicyError(
                "包含不可配置或未知的模块",
                details={
                    "plugins": invalid_plugins,
                    "tasks": invalid_tasks,
                },
            )
        return plugins, tasks

    async def _known_accounts(self) -> dict[str, dict[str, Any]]:
        accounts: dict[str, dict[str, Any]] = {}
        for record in await BotConsole.all():
            accounts[str(record.bot_id)] = {
                "bot_id": str(record.bot_id),
                "runtime_bot_id": str(record.bot_id),
                "nickname": str(record.bot_id),
                "platform": record.platform or "other",
                "connected": False,
            }
        try:
            for bot in nonebot.get_bots().values():
                storage_id = PlatformUtils.get_storage_bot_id(bot)
                self_info = getattr(bot, "self_info", None)
                accounts[storage_id] = {
                    "bot_id": storage_id,
                    "runtime_bot_id": str(bot.self_id),
                    "nickname": str(
                        getattr(self_info, "username", None) or bot.self_id
                    ),
                    "platform": PlatformUtils.get_platform(bot),
                    "connected": True,
                }
        except Exception:
            pass
        try:
            from zhenxun.adapters.qq_official.config import QQOfficialConfig

            config = nonebot.get_plugin_config(QQOfficialConfig)
            for item in config.qq_bots:
                storage_id = f"qq_api:{item.id}"
                accounts.setdefault(
                    storage_id,
                    {
                        "bot_id": storage_id,
                        "runtime_bot_id": str(item.id),
                        "nickname": str(item.id),
                        "platform": "qq_official",
                        "connected": False,
                    },
                )
        except Exception:
            pass
        return accounts

    async def _resolve_bot_id(self, bot_id: str) -> str:
        """Resolve legacy runtime IDs to the canonical persistence identity."""
        candidate = str(bot_id).strip()
        known = await self._known_accounts()
        if candidate in known:
            return candidate
        matches = [
            key
            for key, identity in known.items()
            if identity.get("runtime_bot_id") == candidate
        ]
        if len(matches) == 1:
            return matches[0]
        if len(matches) > 1:
            raise PluginPolicyError(
                "机器人运行账号标识不唯一，请使用规范账号标识",
                details={"bot_id": candidate, "matches": sorted(matches)},
            )
        raise PluginPolicyNotFound("机器人账号不存在", details={"bot_id": candidate})

    async def _resolve_bot_ids(self, bot_ids: Iterable[str]) -> list[str]:
        resolved: list[str] = []
        for bot_id in bot_ids:
            canonical = await self._resolve_bot_id(bot_id)
            if canonical not in resolved:
                resolved.append(canonical)
        return sorted(resolved)

    @staticmethod
    def _policy_payload(policy: PluginPolicy) -> dict[str, Any]:
        return {
            "id": policy.id,
            "name": policy.name,
            "description": policy.description or "",
            "block_plugins": _normalize_modules(policy.block_plugins),
            "block_tasks": _normalize_modules(policy.block_tasks),
            "revision_number": int(policy.revision),
            "created_at": policy.create_time.isoformat()
            if policy.create_time
            else None,
            "updated_at": policy.update_time.isoformat()
            if policy.update_time
            else None,
        }

    def _policy_view(
        self, policy: PluginPolicy, bot_ids: Iterable[str] = ()
    ) -> dict[str, Any]:
        payload = self._policy_payload(policy)
        return {
            **payload,
            "bound_bot_ids": sorted(set(bot_ids)),
            "revision": _revision(payload),
        }

    async def list_policies(self) -> list[dict[str, Any]]:
        policies = await PluginPolicy.all().order_by("name")
        bindings = await BotPluginPolicyBinding.all()
        by_policy: dict[int, list[str]] = {}
        for binding in bindings:
            by_policy.setdefault(int(binding.policy_id), []).append(binding.bot_id)
        return [self._policy_view(p, by_policy.get(int(p.id), [])) for p in policies]

    async def get_policy(self, policy_id: int) -> dict[str, Any]:
        policy = await PluginPolicy.get_or_none(id=policy_id)
        if policy is None:
            raise PluginPolicyNotFound("策略不存在")
        bot_ids = await BotPluginPolicyBinding.filter(policy_id=policy_id).values_list(
            "bot_id", flat=True
        )
        return self._policy_view(policy, bot_ids)

    async def _state_for_bot(
        self, bot_id: str
    ) -> tuple[BotConsole | None, BotPluginPolicyBinding | None, PluginPolicy | None]:
        bot = await BotConsole.get_or_none(bot_id=bot_id)
        binding = await BotPluginPolicyBinding.get_or_none(bot_id=bot_id)
        policy = None
        if binding is not None:
            policy = await PluginPolicy.get_or_none(id=binding.policy_id)
        return bot, binding, policy

    def _account_payload(
        self,
        bot_id: str,
        bot: BotConsole | None,
        binding: BotPluginPolicyBinding | None,
        policy: PluginPolicy | None,
    ) -> dict[str, Any]:
        block_plugins = _decode_modules(bot.block_plugins if bot else "")
        block_tasks = _decode_modules(bot.block_tasks if bot else "")
        payload = {
            "bot_id": bot_id,
            "mode": "linked" if binding and policy else "independent",
            "policy_id": int(policy.id) if policy else None,
            "policy_revision": int(policy.revision) if policy else None,
            "block_plugins": block_plugins,
            "block_tasks": block_tasks,
        }
        return {**payload, "revision": _revision(payload)}

    async def list_accounts(self) -> list[dict[str, Any]]:
        known = await self._known_accounts()
        bindings = {b.bot_id: b for b in await BotPluginPolicyBinding.all()}
        policies = {int(p.id): p for p in await PluginPolicy.all()}
        records = {str(b.bot_id): b for b in await BotConsole.all()}
        result = []
        for bot_id, identity in known.items():
            binding = bindings.get(bot_id)
            policy = policies.get(int(binding.policy_id)) if binding else None
            state = self._account_payload(bot_id, records.get(bot_id), binding, policy)
            result.append(
                {
                    **identity,
                    "mode": state["mode"],
                    "policy": (
                        {"id": int(policy.id), "name": policy.name} if policy else None
                    ),
                    "blocked_plugin_count": len(state["block_plugins"]),
                    "blocked_task_count": len(state["block_tasks"]),
                    "revision": state["revision"],
                }
            )
        return sorted(result, key=lambda item: (not item["connected"], item["bot_id"]))

    async def get_account(self, bot_id: str) -> dict[str, Any]:
        bot_id = await self._resolve_bot_id(bot_id)
        known = await self._known_accounts()
        bot, binding, policy = await self._state_for_bot(bot_id)
        state = self._account_payload(bot_id, bot, binding, policy)
        plugin_modules, task_modules = await self._catalog_modules()
        return {
            **known[bot_id],
            **state,
            "policy": self._policy_view(policy, [bot_id]) if policy else None,
            "missing_plugins": sorted(set(state["block_plugins"]) - plugin_modules),
            "missing_tasks": sorted(set(state["block_tasks"]) - task_modules),
        }

    async def _ensure_known(self, bot_ids: Iterable[str]) -> None:
        known = await self._known_accounts()
        unknown = sorted(set(bot_ids) - set(known))
        if unknown:
            raise PluginPolicyNotFound("机器人账号不存在", details={"bot_ids": unknown})

    async def _materialize(
        self,
        connection: BaseDBAsyncClient,
        bot_id: str,
        block_plugins: Iterable[str],
        block_tasks: Iterable[str],
        catalog_modules: CatalogModules,
    ) -> None:
        plugin_modules, task_modules = catalog_modules
        blocked_plugins = _normalize_modules(block_plugins)
        blocked_tasks = _normalize_modules(block_tasks)
        defaults = {
            "block_plugins": _encode_modules(blocked_plugins),
            "block_tasks": _encode_modules(blocked_tasks),
            "available_plugins": _encode_modules(plugin_modules - set(blocked_plugins)),
            "available_tasks": _encode_modules(task_modules - set(blocked_tasks)),
        }
        bot = await BotConsole.filter(bot_id=bot_id).using_db(connection).first()
        if bot is None:
            bot = BotConsole(bot_id=bot_id, **defaults)
            await super(BotConsole, bot).save(
                using_db=connection,
                force_create=True,
            )
        else:
            await BotConsole.filter(id=bot.id).using_db(connection).update(**defaults)

    async def _publish_accounts(self, bot_ids: Iterable[str]) -> None:
        for bot in await BotConsole.filter(bot_id__in=list(set(bot_ids))):
            await BotMemoryCache.upsert_from_model(bot)

    @staticmethod
    def _check_revision(expected: str, actual: str) -> None:
        if expected != actual:
            raise PluginPolicyConflict(
                "配置已被其他操作修改，请重新加载",
                details={"current_revision": actual},
            )

    async def update_account(
        self,
        bot_id: str,
        *,
        expected_revision: str | None,
        block_plugins: Iterable[str],
        block_tasks: Iterable[str],
    ) -> dict[str, Any]:
        bot_id = await self._resolve_bot_id(bot_id)
        async with runtime_mutation_coordinator.operation("plugin_policy_account"):
            await self._ensure_known([bot_id])
            current = await self.get_account(bot_id)
            if expected_revision:
                self._check_revision(expected_revision, current["revision"])
            clean_plugins, clean_tasks = await self._validate_modules(
                block_plugins,
                block_tasks,
                preserve_plugins=current["missing_plugins"],
                preserve_tasks=current["missing_tasks"],
            )
            catalog_modules = await self._catalog_modules()
            async with in_transaction() as connection:
                await (
                    BotPluginPolicyBinding.filter(bot_id=bot_id)
                    .using_db(connection)
                    .delete()
                )
                await self._materialize(
                    connection,
                    bot_id,
                    clean_plugins,
                    clean_tasks,
                    catalog_modules,
                )
            await self._publish_accounts([bot_id])
        return await self.get_account(bot_id)

    async def create_policy(
        self,
        *,
        name: str,
        description: str = "",
        source_bot_id: str | None = None,
        source_revision: str | None = None,
        block_plugins: Iterable[str] = (),
        block_tasks: Iterable[str] = (),
        bind_bot_ids: Iterable[str] = (),
        expected_revisions: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        name = name.strip()
        if not name:
            raise PluginPolicyError("策略名称不能为空")
        targets = await self._resolve_bot_ids(bind_bot_ids)
        if source_bot_id:
            source_bot_id = await self._resolve_bot_id(source_bot_id)
        async with runtime_mutation_coordinator.operation("plugin_policy_create"):
            if source_bot_id:
                source = await self.get_account(source_bot_id)
                if not source_revision:
                    raise PluginPolicyConflict("缺少源账号修订号，请重新加载")
                self._check_revision(source_revision, source["revision"])
                block_plugins = source["block_plugins"]
                block_tasks = source["block_tasks"]
                preserve_plugins = source["missing_plugins"]
                preserve_tasks = source["missing_tasks"]
            else:
                preserve_plugins = ()
                preserve_tasks = ()
            clean_plugins, clean_tasks = await self._validate_modules(
                block_plugins,
                block_tasks,
                preserve_plugins=preserve_plugins,
                preserve_tasks=preserve_tasks,
            )
            await self._ensure_known(targets)
            for bot_id in targets:
                current = await self.get_account(bot_id)
                self._check_revision(
                    (expected_revisions or {}).get(bot_id, ""),
                    current["revision"],
                )
            existing_names = [p.name.casefold() for p in await PluginPolicy.all()]
            if name.casefold() in existing_names:
                raise PluginPolicyError("策略名称已存在")
            catalog_modules = await self._catalog_modules()
            async with in_transaction() as connection:
                policy = await PluginPolicy.create(
                    using_db=connection,
                    name=name,
                    description=description.strip(),
                    block_plugins=clean_plugins,
                    block_tasks=clean_tasks,
                )
                for bot_id in targets:
                    await (
                        BotPluginPolicyBinding.filter(bot_id=bot_id)
                        .using_db(connection)
                        .delete()
                    )
                    await BotPluginPolicyBinding.create(
                        using_db=connection, bot_id=bot_id, policy_id=policy.id
                    )
                    await self._materialize(
                        connection,
                        bot_id,
                        policy.block_plugins,
                        policy.block_tasks,
                        catalog_modules,
                    )
            await self._publish_accounts(targets)
        return self._policy_view(policy, targets)

    async def update_policy(
        self,
        policy_id: int,
        *,
        expected_revision: str,
        name: str,
        description: str,
        block_plugins: Iterable[str],
        block_tasks: Iterable[str],
    ) -> dict[str, Any]:
        async with runtime_mutation_coordinator.operation("plugin_policy_update"):
            policy = await PluginPolicy.get_or_none(id=policy_id)
            if policy is None:
                raise PluginPolicyNotFound("策略不存在")
            bot_ids = await BotPluginPolicyBinding.filter(
                policy_id=policy_id
            ).values_list("bot_id", flat=True)
            current = self._policy_view(policy, bot_ids)
            self._check_revision(expected_revision, current["revision"])
            clean_name = name.strip()
            if not clean_name:
                raise PluginPolicyError("策略名称不能为空")
            duplicate = await PluginPolicy.exclude(id=policy_id).all()
            if clean_name.casefold() in {p.name.casefold() for p in duplicate}:
                raise PluginPolicyError("策略名称已存在")
            clean_plugins = _normalize_modules(block_plugins)
            clean_tasks = _normalize_modules(block_tasks)
            catalog_plugins, catalog_tasks = await self._catalog_modules()
            catalog_modules = (catalog_plugins, catalog_tasks)
            invalid_plugins = sorted(
                set(clean_plugins)
                - catalog_plugins
                - set(_normalize_modules(policy.block_plugins))
            )
            invalid_tasks = sorted(
                set(clean_tasks)
                - catalog_tasks
                - set(_normalize_modules(policy.block_tasks))
            )
            if invalid_plugins or invalid_tasks:
                raise PluginPolicyError(
                    "包含不可配置或未知的模块",
                    details={"plugins": invalid_plugins, "tasks": invalid_tasks},
                )
            async with in_transaction() as connection:
                await (
                    PluginPolicy.filter(id=policy_id)
                    .using_db(connection)
                    .update(
                        name=clean_name,
                        description=description.strip(),
                        block_plugins=clean_plugins,
                        block_tasks=clean_tasks,
                        revision=F("revision") + 1,
                        update_time=datetime.now(timezone.utc),
                    )
                )
                for bot_id in bot_ids:
                    await self._materialize(
                        connection,
                        bot_id,
                        clean_plugins,
                        clean_tasks,
                        catalog_modules,
                    )
            await self._publish_accounts(bot_ids)
            policy = await PluginPolicy.get(id=policy_id)
        return self._policy_view(policy, bot_ids)

    async def delete_policy(self, policy_id: int, expected_revision: str) -> None:
        async with runtime_mutation_coordinator.operation("plugin_policy_delete"):
            policy = await PluginPolicy.get_or_none(id=policy_id)
            if policy is None:
                raise PluginPolicyNotFound("策略不存在")
            bot_ids = await BotPluginPolicyBinding.filter(
                policy_id=policy_id
            ).values_list("bot_id", flat=True)
            self._check_revision(
                expected_revision, self._policy_view(policy, bot_ids)["revision"]
            )
            if bot_ids:
                raise PluginPolicyInUse(
                    "策略仍被机器人账号使用", details={"bot_ids": sorted(bot_ids)}
                )
            await policy.delete()

    async def bind_accounts(
        self,
        *,
        policy_id: int,
        bot_ids: Iterable[str],
        expected_revisions: dict[str, str],
    ) -> dict[str, Any]:
        targets = await self._resolve_bot_ids(bot_ids)
        async with runtime_mutation_coordinator.operation("plugin_policy_bind"):
            await self._ensure_known(targets)
            policy = await PluginPolicy.get_or_none(id=policy_id)
            if policy is None:
                raise PluginPolicyNotFound("策略不存在")
            changes: list[dict[str, Any]] = []
            for bot_id in targets:
                current = await self.get_account(bot_id)
                self._check_revision(
                    expected_revisions.get(bot_id, ""), current["revision"]
                )
                changes.append(
                    {
                        "bot_id": bot_id,
                        "previous_mode": current["mode"],
                        "previous_policy_id": current["policy_id"],
                        "plugins_added": sorted(
                            set(policy.block_plugins) - set(current["block_plugins"])
                        ),
                        "plugins_removed": sorted(
                            set(current["block_plugins"]) - set(policy.block_plugins)
                        ),
                        "tasks_added": sorted(
                            set(policy.block_tasks) - set(current["block_tasks"])
                        ),
                        "tasks_removed": sorted(
                            set(current["block_tasks"]) - set(policy.block_tasks)
                        ),
                    }
                )
            catalog_modules = await self._catalog_modules()
            async with in_transaction() as connection:
                for bot_id in targets:
                    await (
                        BotPluginPolicyBinding.filter(bot_id=bot_id)
                        .using_db(connection)
                        .delete()
                    )
                    await BotPluginPolicyBinding.create(
                        using_db=connection, bot_id=bot_id, policy_id=policy.id
                    )
                    await self._materialize(
                        connection,
                        bot_id,
                        policy.block_plugins,
                        policy.block_tasks,
                        catalog_modules,
                    )
            await self._publish_accounts(targets)
        return {
            "updated_bot_ids": targets,
            "policy_id": policy_id,
            "changes": changes,
        }

    async def copy_account(
        self,
        *,
        source_bot_id: str,
        source_revision: str,
        target_bot_ids: Iterable[str],
        expected_revisions: dict[str, str],
    ) -> dict[str, Any]:
        source_bot_id = await self._resolve_bot_id(source_bot_id)
        targets = await self._resolve_bot_ids(target_bot_ids)
        targets = [bot_id for bot_id in targets if bot_id != source_bot_id]
        async with runtime_mutation_coordinator.operation("plugin_policy_copy"):
            await self._ensure_known([source_bot_id, *targets])
            source = await self.get_account(source_bot_id)
            self._check_revision(source_revision, source["revision"])
            for bot_id in targets:
                current = await self.get_account(bot_id)
                self._check_revision(
                    expected_revisions.get(bot_id, ""), current["revision"]
                )
            catalog_modules = await self._catalog_modules()
            async with in_transaction() as connection:
                for bot_id in targets:
                    await (
                        BotPluginPolicyBinding.filter(bot_id=bot_id)
                        .using_db(connection)
                        .delete()
                    )
                    await self._materialize(
                        connection,
                        bot_id,
                        source["block_plugins"],
                        source["block_tasks"],
                        catalog_modules,
                    )
            await self._publish_accounts(targets)
        return {"source_bot_id": source_bot_id, "updated_bot_ids": targets}

    async def set_feature_enabled(
        self,
        bot_id: str | None,
        kind: FeatureKind,
        module: str,
        enabled: bool,
    ) -> None:
        module = module.strip()
        if not module:
            raise PluginPolicyError("模块名不能为空")
        catalog_plugins, catalog_tasks = await self._catalog_modules()
        catalog = catalog_plugins if kind == "plugins" else catalog_tasks
        if module not in catalog:
            raise PluginPolicyError("模块不可配置", details={"module": module})
        if bot_id is not None:
            bot_id = await self._resolve_bot_id(bot_id)
            current = await self.get_account(bot_id)
            plugins = set(current["block_plugins"])
            tasks = set(current["block_tasks"])
            target = plugins if kind == "plugins" else tasks
            target.discard(module) if enabled else target.add(module)
            await self.update_account(
                bot_id,
                expected_revision=None,
                block_plugins=plugins,
                block_tasks=tasks,
            )
            return
        await self._mutate_all(kind, enabled=enabled, module=module)

    async def set_all_features_enabled(
        self,
        bot_id: str | None,
        kind: FeatureKind,
        enabled: bool,
    ) -> None:
        if bot_id is not None:
            bot_id = await self._resolve_bot_id(bot_id)
            current = await self.get_account(bot_id)
            catalog_plugins, catalog_tasks = await self._catalog_modules()
            plugins = (
                set(current["missing_plugins"])
                if enabled
                else catalog_plugins | set(current["missing_plugins"])
            )
            tasks = (
                set(current["missing_tasks"])
                if enabled
                else catalog_tasks | set(current["missing_tasks"])
            )
            if kind == "plugins":
                tasks = set(current["block_tasks"])
            else:
                plugins = set(current["block_plugins"])
            await self.update_account(
                bot_id,
                expected_revision=None,
                block_plugins=plugins,
                block_tasks=tasks,
            )
            return
        await self._mutate_all(kind, enabled=enabled, module=None)

    async def _mutate_all(
        self,
        kind: FeatureKind,
        *,
        enabled: bool,
        module: str | None,
    ) -> None:
        catalog_plugins, catalog_tasks = await self._catalog_modules()
        catalog_modules = (catalog_plugins, catalog_tasks)
        catalog = catalog_plugins if kind == "plugins" else catalog_tasks

        def mutate(values: Iterable[str]) -> list[str]:
            result = set(_normalize_modules(values))
            if module is None:
                result = result - catalog if enabled else result | catalog
            elif enabled:
                result.discard(module)
            else:
                result.add(module)
            return sorted(result)

        async with runtime_mutation_coordinator.operation("plugin_policy_legacy_all"):
            policies = await PluginPolicy.all()
            bindings = await BotPluginPolicyBinding.all()
            bots = {str(bot.bot_id): bot for bot in await BotConsole.all()}
            policy_updates: dict[int, tuple[list[str], list[str]]] = {}
            for policy in policies:
                plugins = _normalize_modules(policy.block_plugins)
                tasks = _normalize_modules(policy.block_tasks)
                if kind == "plugins":
                    plugins = mutate(plugins)
                else:
                    tasks = mutate(tasks)
                policy_updates[int(policy.id)] = (plugins, tasks)
            binding_by_bot = {
                binding.bot_id: int(binding.policy_id) for binding in bindings
            }
            affected = set(bots) | set(binding_by_bot)
            async with in_transaction() as connection:
                for policy_id, (plugins, tasks) in policy_updates.items():
                    await (
                        PluginPolicy.filter(id=policy_id)
                        .using_db(connection)
                        .update(
                            block_plugins=plugins,
                            block_tasks=tasks,
                            revision=F("revision") + 1,
                            update_time=datetime.now(timezone.utc),
                        )
                    )
                for bot_id in affected:
                    policy_id = binding_by_bot.get(bot_id)
                    if policy_id is not None:
                        plugins, tasks = policy_updates[policy_id]
                    else:
                        bot = bots[bot_id]
                        plugins = _decode_modules(bot.block_plugins)
                        tasks = _decode_modules(bot.block_tasks)
                        if kind == "plugins":
                            plugins = mutate(plugins)
                        else:
                            tasks = mutate(tasks)
                    await self._materialize(
                        connection, bot_id, plugins, tasks, catalog_modules
                    )
            await self._publish_accounts(affected)

    async def reconcile(self) -> dict[str, int]:
        changed: list[str] = []
        async with runtime_mutation_coordinator.operation("plugin_policy_reconcile"):
            bindings = await BotPluginPolicyBinding.all()
            policies = {int(p.id): p for p in await PluginPolicy.all()}
            catalog_modules = await self._catalog_modules()
            async with in_transaction() as connection:
                bots = {
                    str(bot.bot_id): bot
                    for bot in await BotConsole.all().using_db(connection)
                }
                binding_by_bot = {binding.bot_id: binding for binding in bindings}
                bot_ids = sorted(set(bots) | set(binding_by_bot))
                plugin_modules, task_modules = catalog_modules
                for bot_id in bot_ids:
                    bot = bots.get(bot_id)
                    binding = binding_by_bot.get(bot_id)
                    policy = policies.get(int(binding.policy_id)) if binding else None
                    block_plugins = (
                        _normalize_modules(policy.block_plugins)
                        if policy
                        else _decode_modules(bot.block_plugins if bot else "")
                    )
                    block_tasks = (
                        _normalize_modules(policy.block_tasks)
                        if policy
                        else _decode_modules(bot.block_tasks if bot else "")
                    )
                    expected_plugins = _encode_modules(block_plugins)
                    expected_tasks = _encode_modules(block_tasks)
                    expected_available_plugins = _encode_modules(
                        plugin_modules - set(block_plugins)
                    )
                    expected_available_tasks = _encode_modules(
                        task_modules - set(block_tasks)
                    )
                    if (
                        bot is None
                        or bot.block_plugins != expected_plugins
                        or bot.block_tasks != expected_tasks
                        or bot.available_plugins != expected_available_plugins
                        or bot.available_tasks != expected_available_tasks
                    ):
                        await self._materialize(
                            connection,
                            bot_id,
                            block_plugins,
                            block_tasks,
                            catalog_modules,
                        )
                        changed.append(bot_id)
            await self._publish_accounts(changed)
        if changed:
            logger.info(
                f"已修复 {len(changed)} 个账号的插件策略物化状态",
                "PluginPolicy",
            )
        return {"bindings": len(bindings), "repaired": len(changed)}


plugin_policy_service = PluginPolicyService()

__all__ = [
    "PluginPolicyConflict",
    "PluginPolicyError",
    "PluginPolicyInUse",
    "PluginPolicyNotFound",
    "PluginPolicyService",
    "plugin_policy_service",
]
