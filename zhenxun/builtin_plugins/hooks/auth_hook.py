import asyncio
from dataclasses import dataclass
from functools import wraps
import math
import time
import uuid
import weakref

from nonebot import get_driver
from nonebot.adapters import Bot, Event
from nonebot.exception import IgnoredException
from nonebot.matcher import Matcher
from nonebot.message import event_preprocessor, run_postprocessor, run_preprocessor
from nonebot.typing import T_State
from nonebot_plugin_alconna import UniMsg
from nonebot_plugin_uninfo import Uninfo

from zhenxun.services.cache.runtime_cache import is_cache_ready, wait_cache_ready
from zhenxun.services.log import logger
from zhenxun.services.message_load import mark_activity
from zhenxun.services.runtime_bootstrap import register_runtime_bootstrap
from zhenxun.utils.exception import DependencyUnavailable
from zhenxun.utils.manager.priority_manager import PriorityLifecycle

from .auth.config import LOGGER_COMMAND
from .auth.context import (
    get_event_context,
    get_or_create_event_context,
    get_permission_side_effect_cache,
    resolve_event_channel_id,
    resolve_event_group_id,
    set_route_modules,
)
from .auth_activation import adapter_contract_matches, matcher_supported_adapters
from .auth_checker import auth
from .auth_dispatch import get_route_context, install_dispatch, uninstall_dispatch

_SKIP_AUTH_PLUGINS = {"chat_history", "chat_message"}


@dataclass(frozen=True)
class ConnectionEpoch:
    bot: weakref.ReferenceType
    connected_second: int
    connection_id: str


class ConnectionEpochs:
    """按连接记录 Bot 接入时间，用于丢弃接入前积压的旧消息"""

    def __init__(self) -> None:
        self._entries: dict[tuple[str, str], ConnectionEpoch] = {}

    def connect(self, bot, scope: str) -> ConnectionEpoch:
        key = (scope, str(bot.self_id))

        def expired(reference):
            current = self._entries.get(key)
            if current is not None and current.bot is reference:
                self._entries.pop(key, None)

        epoch = ConnectionEpoch(
            weakref.ref(bot, expired), math.floor(time.time()), uuid.uuid4().hex
        )
        self._entries[key] = epoch
        return epoch

    def get(self, bot, scope: str) -> ConnectionEpoch | None:
        epoch = self._entries.get((scope, str(bot.self_id)))
        return epoch if epoch is not None and epoch.bot() is bot else None

    def disconnect(self, bot, scope: str) -> None:
        if self.get(bot, scope) is not None:
            self._entries.pop((scope, str(bot.self_id)), None)

    def is_backlog(self, bot, scope: str, timestamp: object) -> bool:
        if (
            isinstance(timestamp, bool)
            or not isinstance(timestamp, int | float)
            or (isinstance(timestamp, float) and not math.isfinite(timestamp))
            or timestamp < 0
        ):
            return False
        epoch = self.get(bot, scope)
        return epoch is not None and timestamp < epoch.connected_second

    def clear(self) -> None:
        self._entries.clear()


connection_epochs = ConnectionEpochs()


@dataclass(frozen=True)
class PlatformCapabilities:
    available: frozenset[str]
    authorization_unknown: frozenset[str] = frozenset()


def event_capabilities(bot: Bot, event: Event, session) -> PlatformCapabilities:
    """根据事件判断本次可用的平台能力，远端接口权限由调用时校验

    参数:
        bot: Bot
        event: Event
        session: Uninfo

    返回:
        PlatformCapabilities: 可用能力与未确认授权的能力
    """
    from nonebot.adapters.qq.event import (
        DirectMessageCreateEvent,
        InteractionCreateEvent,
    )

    from zhenxun.services.business_identity import supports_business_identity
    from zhenxun.services.platform_identity import is_namespaced_scope
    from zhenxun.utils.platform import PlatformUtils

    available = {"uni_message"}
    remote = set()
    scene = session.scene
    if isinstance(event, DirectMessageCreateEvent):
        available.add("guild_dm")
    elif scene.is_channel:
        available.update({"guild", "channel"})
    elif scene.is_guild:
        available.add("guild")
    elif scene.is_group:
        available.add("group")
    elif scene.is_private:
        available.add("c2c")
    if event.get_type() == "message":
        available.add("passive_reply")
    if isinstance(event, InteractionCreateEvent) and event.type in (11, 12):
        available.add("interaction_response")
        if event.event_id:
            available.add("passive_reply")
    scope = PlatformUtils.get_platform_scope(bot) if isinstance(bot, Bot) else "unknown"
    supports_members = (scope == "qq_api" and bool(available & {"group", "guild"})) or (
        scope == "qq_client" and "group" in available
    )
    if supports_members:
        available.add("members_query")
        # SDK support cannot establish server-side authorization for this Bot.
        remote.add("members_query")
    if scope == "qq_client":
        available.add("legacy_identity")
    if scope == "qq_client" or is_namespaced_scope(scope):
        available.add("scoped_management")
    if event.get_type() == "message" and supports_business_identity(scope):
        available.add("business_identity")
    return PlatformCapabilities(frozenset(available), frozenset(remote))


driver = get_driver()
register_runtime_bootstrap(driver)


@driver.on_bot_connect
async def _mark_bot_connected(bot: Bot):
    from zhenxun.utils.platform import PlatformUtils

    connection_epochs.connect(bot, PlatformUtils.get_platform_scope(bot))


@driver.on_bot_disconnect
async def _mark_bot_disconnected(bot: Bot):
    from zhenxun.services.uninfo_patch import clear_uninfo_sessions
    from zhenxun.utils.platform import PlatformUtils

    connection_epochs.disconnect(bot, PlatformUtils.get_platform_scope(bot))
    await clear_uninfo_sessions(bot)


@PriorityLifecycle.on_startup(
    priority=7,
    component_id="runtime:auth_tasks",
    depends_on=("runtime:runtime_cache",),
)
async def _install_dispatch():
    await install_dispatch()


@PriorityLifecycle.on_shutdown(priority=7, component_id="runtime:auth_tasks")
async def _uninstall_dispatch():
    from zhenxun.services.uninfo_patch import clear_uninfo_sessions

    try:
        uninstall_dispatch()
    finally:
        connection_epochs.clear()
        await clear_uninfo_sessions()


def _skip_auth_for_plugin(matcher: Matcher) -> bool:
    if not matcher.plugin:
        return False
    name = (matcher.plugin.name or "").lower()
    if name in _SKIP_AUTH_PLUGINS:
        return True
    module_name = getattr(matcher.plugin, "module_name", "") or ""
    return "chat_history" in module_name


def _enforce_platform_contract(
    matcher: Matcher, platform_scope: str, bot, event, session
) -> None:
    if matcher.plugin is None:
        return
    metadata = getattr(matcher.plugin, "metadata", None)
    extra = getattr(metadata, "extra", None)
    if not isinstance(extra, dict):
        return
    supported = extra.get("supported_platform_scopes")
    if supported and platform_scope not in set(supported):
        raise IgnoredException("plugin platform scope unsupported")
    required = set(extra.get("required_platform_capabilities") or ())
    if not required:
        return
    capabilities = event_capabilities(bot, event, session)
    if not required.issubset(capabilities.available):
        raise IgnoredException("plugin platform capability unavailable")


def _skip_unavailable(function):
    """数据库或缓存不可用、超时时跳过本条消息并记录日志

    参数:
        function: 事件或运行前处理函数

    返回:
        包装后的处理函数
    """

    @wraps(function)
    async def wrapped(*args, **kwargs):
        try:
            return await function(*args, **kwargs)
        except (DependencyUnavailable, TimeoutError, asyncio.TimeoutError) as error:
            reason = str(error) or "permission_dependency_timeout"
            logger.warning(f"依赖不可用，跳过本条消息: {reason}", LOGGER_COMMAND)
            raise IgnoredException("permission_dependency_unavailable") from None

    return wrapped


@event_preprocessor
@_skip_unavailable
async def _drop_message_before_cache_ready(event: Event, bot: Bot):
    mark_activity()
    if event.get_type() == "message" and not await wait_cache_ready(10):
        raise IgnoredException("cache not ready ignore")
    if is_cache_ready():
        from zhenxun.services.bot_group_policy import bot_group_policy_service
        from zhenxun.utils.platform import PlatformUtils

        group_id = resolve_event_group_id(
            event,
            getattr(event, "group_openid", None) or getattr(event, "guild_id", None),
        )
        await bot_group_policy_service.ensure_fresh(
            PlatformUtils.get_storage_bot_id(bot),
            PlatformUtils.get_platform_scope(bot),
            group_id,
            resolve_event_channel_id(event, None),
        )
        bot_group_policy_service.observe(
            PlatformUtils.get_storage_bot_id(bot),
            PlatformUtils.get_platform_scope(bot),
            group_id,
            resolve_event_channel_id(event, None),
        )
    if event.get_type() != "message":
        return
    from zhenxun.utils.platform import PlatformUtils

    if connection_epochs.is_backlog(
        bot, PlatformUtils.get_platform_scope(bot), getattr(event, "time", None)
    ):
        raise IgnoredException("drop backlog message")


def _admit_plugin(matcher: Matcher) -> None:
    """加载失败而停用的插件不处理事件

    参数:
        matcher: Matcher
    """
    from zhenxun.services.startup_load import startup_load_planner

    plugin_id = matcher.plugin_id
    if not plugin_id:
        return
    owner = (
        startup_load_planner.owner_for_module(matcher.module_name or "")
        or plugin_id.split(":", 1)[0]
    )
    if owner in startup_load_planner.failed_plugins:
        raise IgnoredException("plugin not admitted")


def _reject_disabled_owner(matcher: Matcher, context=None) -> None:
    """所属插件（含上级插件）全局关闭时不处理事件，超级群与超级用户按统一规则豁免

    参数:
        matcher: Matcher
        context: EventContext，为空时不豁免
    """
    from zhenxun.services.plugin_policy import (
        PluginPolicyNotReady,
        plugin_policy_service,
    )

    try:
        blocked = plugin_policy_service.owner_blocked(
            matcher.plugin_id,
            super_group=context is not None and _is_super_group(context),
            is_superuser=context is not None and context.is_superuser,
        )
    except PluginPolicyNotReady:
        raise IgnoredException("plugin policy not ready") from None
    if blocked:
        raise IgnoredException("plugin globally disabled")


def _is_super_group(context) -> bool:
    """当前事件所在群是否为超级群

    参数:
        context: EventContext

    返回:
        bool: 是否为超级群
    """
    if not context.group_id:
        return False
    from zhenxun.services.cache.runtime_cache import GroupMemoryCache
    from zhenxun.services.platform_identity import is_namespaced_scope

    if is_namespaced_scope(context.platform_scope):
        group = GroupMemoryCache.get_if_ready(context.storage_group_id)
    else:
        group = GroupMemoryCache.get_if_ready(context.group_id, context.channel_id)
    return bool(group and group.is_super)


@run_preprocessor
@_skip_unavailable
async def _auth_preprocessor(
    matcher: Matcher,
    event: Event,
    bot: Bot,
    session: Uninfo,
    state: T_State,
    message: UniMsg | None = None,
):
    if event.get_type() == "message" and not is_cache_ready():
        raise IgnoredException("cache not ready ignore")
    _admit_plugin(matcher)

    if not adapter_contract_matches(matcher_supported_adapters(matcher), bot.adapter):
        raise IgnoredException("plugin adapter unsupported")

    # 提前判断是否跳过权限检查
    if _skip_auth_for_plugin(matcher):
        _reject_disabled_owner(matcher)
        from zhenxun.utils.platform import PlatformUtils

        _enforce_platform_contract(
            matcher, PlatformUtils.get_platform_scope(session), bot, event, session
        )
        return

    start_time = time.time()
    event_context = get_or_create_event_context(
        bot,
        event,
        session,
        state,
        message=message,
    )
    _enforce_platform_contract(
        matcher, event_context.platform_scope, bot, event, session
    )
    _reject_disabled_owner(matcher, event_context)
    from zhenxun.services.bot_group_policy import bot_group_policy_service

    await bot_group_policy_service.ensure_fresh(
        event_context.bot_id,
        event_context.platform_scope,
        event_context.group_id,
        event_context.channel_id,
    )
    bot_group_policy_service.observe(
        event_context.bot_id,
        event_context.platform_scope,
        event_context.group_id,
        event_context.channel_id,
    )

    if not event_context.route_modules_loaded:
        route_modules = await get_route_context(
            event_context.plain_text,
            event_context.event_cache,
        )
        set_route_modules(state, event_context, route_modules)

    try:
        await auth(
            matcher,
            event,
            bot,
            session,
            context=event_context,
            skip_ban=False,
            state=state,
        )
    except IgnoredException:
        raise
    except (DependencyUnavailable, TimeoutError, asyncio.TimeoutError):
        raise
    except Exception as exc:
        logger.error("auth check failed", LOGGER_COMMAND, e=exc)
        raise IgnoredException("auth failed") from exc

    now = time.monotonic()
    last_log = getattr(_auth_preprocessor, "_last_log", 0.0)
    if now - last_log > 1.0:
        setattr(_auth_preprocessor, "_last_log", now)
        logger.debug(
            f"auth check cost: {time.time() - start_time:.3f}s",
            LOGGER_COMMAND,
        )


@run_postprocessor
async def _unblock_after_matcher(
    matcher: Matcher,
    state: T_State,
    exception: Exception | None = None,
):
    """matcher 结束后提交或回滚本 matcher 预留的金币与限制，并解除其 block

    参数:
        matcher: Matcher
        state: T_State
        exception: matcher 异常
    """
    if not matcher.plugin:
        return
    context = get_event_context(state)
    module = matcher.plugin.name
    side_effects = get_permission_side_effect_cache(
        state=state,
        event_cache=context.event_cache if context is not None else None,
    )
    commit = side_effects.commits.get(module)
    if commit is None or commit.committed or commit.owner_matcher_id != id(matcher):
        return
    try:
        if await commit.finish(commit=exception is None, reason="matcher_exception"):
            side_effects.settled.add(module)
    finally:
        side_effects.commits.pop(module, None)
