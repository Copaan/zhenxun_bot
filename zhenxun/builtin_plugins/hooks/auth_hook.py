import time

from nonebot import get_driver
from nonebot.adapters import Bot, Event
from nonebot.exception import IgnoredException
from nonebot.matcher import Matcher
from nonebot.message import event_preprocessor, run_postprocessor, run_preprocessor
from nonebot.typing import T_State
from nonebot_plugin_alconna import UniMsg
from nonebot_plugin_uninfo import Uninfo

from zhenxun.services.cache.runtime_cache import is_cache_ready
from zhenxun.services.log import logger
from zhenxun.services.message_admission import connection_epochs
from zhenxun.services.message_load import is_overloaded, mark_activity
from zhenxun.services.runtime_bootstrap import register_runtime_bootstrap
from zhenxun.utils.manager.priority_manager import PriorityLifecycle

from .auth.config import LOGGER_COMMAND
from .auth.context import (
    get_event_context,
    get_or_create_event_context,
    get_permission_side_effect_cache,
    resolve_actor_user_id,
    resolve_event_channel_id,
    resolve_event_group_id,
    set_route_modules,
)
from .auth_checker import (
    LimitManager,
    _get_route_context,
    auth,
    start_auth_runtime_tasks,
    stop_auth_runtime_tasks,
)

_SKIP_AUTH_PLUGINS = {"chat_history", "chat_message"}

driver = get_driver()
register_runtime_bootstrap(driver)


@driver.on_bot_connect
async def _mark_bot_connected(bot: Bot):
    from zhenxun.utils.platform import PlatformUtils

    connection_epochs.connect(bot, PlatformUtils.get_platform_scope(bot))


@driver.on_bot_disconnect
async def _mark_bot_disconnected(bot: Bot):
    from zhenxun.utils.platform import PlatformUtils

    connection_epochs.disconnect(bot, PlatformUtils.get_platform_scope(bot))


@PriorityLifecycle.on_startup(
    priority=7,
    component_id="runtime:auth_tasks",
    depends_on=("runtime:runtime_cache",),
    pass_context=True,
)
async def _start_auth_runtime_tasks(context):
    await start_auth_runtime_tasks(context)


@PriorityLifecycle.on_shutdown(priority=7, component_id="runtime:auth_tasks")
async def _stop_auth_runtime_tasks():
    try:
        await stop_auth_runtime_tasks()
    finally:
        connection_epochs.clear()


def _skip_auth_for_plugin(matcher: Matcher) -> bool:
    if not matcher.plugin:
        return False
    name = (matcher.plugin.name or "").lower()
    if name in _SKIP_AUTH_PLUGINS:
        return True
    module_name = getattr(matcher.plugin, "module_name", "") or ""
    return "chat_history" in module_name


def _enforce_platform_contract(matcher: Matcher, event_context) -> None:
    if matcher.plugin is None:
        return
    metadata = getattr(matcher.plugin, "metadata", None)
    extra = getattr(metadata, "extra", None)
    if not isinstance(extra, dict):
        return
    supported = extra.get("supported_platform_scopes")
    if supported and event_context.platform_scope not in set(supported):
        raise IgnoredException("plugin platform scope unsupported")
    required = set(extra.get("required_platform_capabilities") or ())
    if not required:
        return
    available = {"uni_message", "passive_reply"}
    if event_context.group_id:
        available.add("group")
    else:
        available.add("c2c")
    if not required.issubset(available):
        raise IgnoredException("plugin platform capability unavailable")


@event_preprocessor
async def _drop_message_before_cache_ready(event: Event, bot: Bot):
    mark_activity()
    if is_cache_ready():
        from zhenxun.services.bot_group_policy import bot_group_policy_service
        from zhenxun.utils.platform import PlatformUtils

        group_id = resolve_event_group_id(
            event,
            getattr(event, "group_openid", None) or getattr(event, "guild_id", None),
        )
        bot_group_policy_service.observe(
            PlatformUtils.get_storage_bot_id(bot),
            PlatformUtils.get_platform_scope(bot),
            group_id,
            resolve_event_channel_id(event, None),
        )
    if event.get_type() != "message":
        return
    if not is_cache_ready():
        raise IgnoredException("cache not ready ignore")
    from zhenxun.utils.platform import PlatformUtils

    if connection_epochs.is_backlog(
        bot, PlatformUtils.get_platform_scope(bot), getattr(event, "time", None)
    ):
        raise IgnoredException("drop backlog message")


@run_preprocessor
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

    # 提前判断是否跳过权限检查
    if _skip_auth_for_plugin(matcher):
        return

    start_time = time.time()
    event_context = get_or_create_event_context(
        bot,
        event,
        session,
        state,
        message=message,
    )
    _enforce_platform_contract(matcher, event_context)
    from zhenxun.services.bot_group_policy import bot_group_policy_service

    bot_group_policy_service.observe(
        event_context.bot_id,
        event_context.platform_scope,
        event_context.group_id,
        event_context.channel_id,
    )

    if not event_context.route_modules_loaded:
        route_modules = await _get_route_context(
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
    except Exception as exc:
        logger.error("auth check failed", LOGGER_COMMAND, e=exc)
        raise IgnoredException("auth failed") from exc

    now = time.monotonic()
    last_log = getattr(_auth_preprocessor, "_last_log", 0.0)
    if now - last_log > 1.0 and not is_overloaded():
        setattr(_auth_preprocessor, "_last_log", now)
        logger.debug(
            f"auth check cost: {time.time() - start_time:.3f}s",
            LOGGER_COMMAND,
        )


@run_postprocessor
async def _unblock_after_matcher(
    matcher: Matcher,
    session: Uninfo,
    event: Event,
    state: T_State,
    exception: Exception | None = None,
):
    context = get_event_context(state)
    if context is not None:
        limit_entity = context.limit_entity
        user_id = limit_entity.user_id
        group_id = limit_entity.group_id
        channel_id = limit_entity.channel_id
    else:
        user_id = resolve_actor_user_id(event, session.user.id)
        group_id = resolve_event_group_id(event, None)
        channel_id = resolve_event_channel_id(event, None)
        if session.group:
            if session.group.parent:
                group_id = session.group.parent.id
                channel_id = session.group.id
            else:
                group_id = session.group.id
    if user_id and matcher.plugin:
        module = matcher.plugin.name
        side_effects = get_permission_side_effect_cache(
            state=state,
            event_cache=context.event_cache if context is not None else None,
        )
        commit = side_effects.commits.get(module)
        if (
            commit is not None
            and not commit.committed
            and commit.owner_matcher_id == id(matcher)
        ):
            side_effects.commits.pop(module, None)
            if exception is None:
                try:
                    await commit.commit_all()
                    side_effects.auth_results[module] = (True, None)
                except Exception as exc:
                    await commit.rollback_all("commit_failed")
                    logger.error(
                        "auth side effect commit failed",
                        LOGGER_COMMAND,
                        e=exc,
                    )
            else:
                await commit.rollback_all("matcher_exception")
            if commit.limit_should_auto_unblock:
                limit_entity = commit.limit_entity
                LimitManager.unblock(
                    module,
                    limit_entity.user_id if limit_entity else user_id,
                    limit_entity.group_id if limit_entity else group_id,
                    limit_entity.channel_id if limit_entity else channel_id,
                )
        else:
            LimitManager.unblock(module, user_id, group_id, channel_id)
