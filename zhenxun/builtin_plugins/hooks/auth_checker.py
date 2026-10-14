import asyncio
from dataclasses import dataclass
import re
import time

from nonebot.adapters import Bot, Event
from nonebot.exception import IgnoredException
from nonebot.matcher import Matcher
from nonebot_plugin_alconna import At
from nonebot_plugin_uninfo import Uninfo

from zhenxun.models.plugin_info import PluginInfo
from zhenxun.models.user_console import UserConsole
from zhenxun.services.cache.runtime_sync import RuntimeCacheMutation
from zhenxun.services.log import logger
from zhenxun.services.message_load import is_db_unhealthy
from zhenxun.utils.enum import GoldHandle, PluginType
from zhenxun.utils.exception import DependencyUnavailable, InsufficientGold
from zhenxun.utils.platform import PlatformUtils

from .auth.auth_ban import auth_ban
from .auth.auth_group import _is_group_wake_command
from .auth.auth_limit import reserve_auth_limit
from .auth.bot_filter import bot_filter
from .auth.config import LOGGER_COMMAND, WARNING_THRESHOLD
from .auth.context import (
    EventContext,
    PermissionContext,
    get_permission_side_effect_cache,
    store_permission_context,
)
from .auth.exception import (
    IsSuperuserException,
    PermissionExemption,
    SkipPluginException,
)
from .auth.utils import freq, is_poke
from .auth_policy import PolicyContext, PolicyDecision, PolicyDecisionPoint
from .auth_side_effect import SideEffectCommit
from .auth_snapshot import (
    AuthSnapshot,
    PluginAuthProfile,
    build_auth_snapshot,
    get_plugin,
    get_plugin_auth_profile,
)

_AUTH_PDP = PolicyDecisionPoint()
_BOT_WAKE_COMMAND_PATTERN = re.compile(r"^bot醒来(?:\s+\S+)?$", re.IGNORECASE)
_BOT_WAKE_CANONICAL_PATTERN = re.compile(
    r"^bot_manage\s+bot_switch\s+enable(?:\s+\S+)?$", re.IGNORECASE
)

_POLICY_SKIP_MESSAGES = {
    "superuser_required": "超级管理员权限不足...",
    "bot_not_found": "Bot不存在，阻断权限检测...",
    "bot_sleeping": "Bot休眠中阻断权限检测...",
    "group_not_found": "群组信息不存在...",
    "group_blacklisted": "群组黑名单, 目标群组群权限权限-1...",
    "group_sleeping": "群组休眠状态...",
    "group_level_low": "群等级限制...",
    "admin_level_low": "管理员权限不足...",
}
# 插件策略禁止调用时发送给用户的提示
_PLUGIN_POLICY_TIPS = {
    "global_disabled": "该功能在当前场景全局关闭...",
    "administrator_disabled": "超级管理员禁用了该群此功能...",
    "group_disabled": "该群未开启此功能...",
    "private_disabled": "当前账号未开启此私聊功能...",
    "account_disabled": "当前账号未开启此功能...",
    "not_loaded": "该功能尚未加载...",
}


@dataclass(slots=True)
class AuthPreparation:
    plugin: PluginInfo
    profile: PluginAuthProfile
    snapshot: AuthSnapshot
    permission_context: PermissionContext
    policy_context: PolicyContext


def _is_bot_wake_command(module: str, text: str | None) -> bool:
    if "bot_manage" not in (module or "") or not text:
        return False
    normalized = re.sub(r"\s+", " ", text.strip())
    return bool(normalized) and (
        _BOT_WAKE_COMMAND_PATTERN.match(normalized) is not None
        or _BOT_WAKE_CANONICAL_PATTERN.match(normalized) is not None
    )


def _is_hidden_plugin(matcher: Matcher) -> bool:
    plugin = matcher.plugin
    if not plugin or not plugin.metadata:
        return False
    extra = plugin.metadata.extra or {}
    return extra.get("plugin_type") == PluginType.HIDDEN


def _policy_denied(
    decision: PolicyDecision, prep: AuthPreparation, event: Event
) -> SkipPluginException:
    """根据拒绝原因构造跳过异常，按配置附带提示消息

    参数:
        decision: 权限判定结果
        prep: 权限检查准备数据
        event: Event

    返回:
        SkipPluginException: 跳过插件异常
    """
    reason = decision.reason
    plugin = prep.plugin
    context = prep.permission_context
    if reason == "admin_level_low":
        tip = f"你的权限不足喔，该功能需要的权限等级: {plugin.admin_level}"
        return SkipPluginException(
            f"{plugin.name}({plugin.module}) 管理员权限不足...",
            tip_message=[At(flag="user", target=context.user_id), tip]
            if context.group_id
            else tip,
            tip_check_tag=context.user_id,
            tip_background=True,
        )
    if tip := _PLUGIN_POLICY_TIPS.get(reason):
        sid = context.group_id or context.user_id
        should_tip = freq.is_send_limit_message(plugin, sid, is_poke(event))
        return SkipPluginException(
            f"插件策略禁止调用: {reason}",
            tip_message=tip if should_tip else None,
            tip_check_tag=sid if should_tip else None,
            tip_background=should_tip,
        )
    return SkipPluginException(_POLICY_SKIP_MESSAGES.get(reason, reason))


async def _prepare_auth_state(
    *,
    module: str,
    context: EventContext,
    bot: Bot,
    state: dict | None,
) -> AuthPreparation:
    """从运行态缓存读取插件信息与权限快照

    参数:
        module: 插件模块
        context: EventContext
        bot: Bot
        state: matcher state

    异常:
        CacheUnavailable: 缓存不可用，跳过本条消息
        PermissionExemption: 插件未注册或为隐藏插件，跳过权限检查

    返回:
        AuthPreparation: 权限检查准备数据
    """
    plugin = await get_plugin(module)
    if plugin is None:
        raise PermissionExemption(f"plugin:{module} not registered, skip")
    if plugin.plugin_type == PluginType.HIDDEN:
        raise PermissionExemption(f"plugin {plugin.name}:{plugin.module} hidden, skip")

    permission_context = PermissionContext(event=context, module=module, plugin=plugin)
    profile = await get_plugin_auth_profile(plugin)
    snapshot = await build_auth_snapshot(
        context=context, plugin=plugin, profile=profile, bot=bot
    )
    permission_context.group = snapshot.group
    permission_context.bot_data = snapshot.bot_data
    if snapshot.admin_levels is not None:
        permission_context.admin_levels = snapshot.admin_levels
    store_permission_context(state, permission_context)
    return AuthPreparation(
        plugin=plugin,
        profile=profile,
        snapshot=snapshot,
        permission_context=permission_context,
        policy_context=PolicyContext(
            snapshot=snapshot,
            allow_sleep_bypass=_is_bot_wake_command(module, context.plain_text),
            allow_group_sleep_bypass=_is_group_wake_command(plugin, context.plain_text),
        ),
    )


def _check_policy(prep: AuthPreparation, event: Event) -> bool:
    """依次执行 Bot、群组、插件、管理员权限判定

    参数:
        prep: 权限检查准备数据
        event: Event

    异常:
        SkipPluginException: 权限不足

    返回:
        bool: 是否直接放行（隐藏插件）
    """
    policy_context = prep.policy_context
    decision = _AUTH_PDP.decide(policy_context)
    if decision.denied:
        raise _policy_denied(decision, prep, event)
    if decision.allowed:
        return True
    for check in (
        _AUTH_PDP.decide_bot,
        _AUTH_PDP.decide_group,
        _AUTH_PDP.decide_plugin,
        _AUTH_PDP.decide_admin,
    ):
        decision = check(policy_context)
        if decision.denied:
            raise _policy_denied(decision, prep, event)
    return False


async def _check_ban(prep: AuthPreparation, matcher: Matcher, session: Uninfo) -> None:
    """检查用户与群组 ban，QQ 官方适配器不做 ban 检查

    参数:
        prep: 权限检查准备数据
        matcher: Matcher
        session: Uninfo

    异常:
        SkipPluginException: 用户或群组被 ban
    """
    if prep.snapshot.context.platform_scope == "qq_api":
        return
    await auth_ban(matcher, session, prep.plugin, context=prep.permission_context)


def _is_superuser_exempt(prep: AuthPreparation) -> bool:
    """超级用户是否免除限制与金币消耗

    参数:
        prep: 权限检查准备数据

    返回:
        bool: 是否免除
    """
    profile = prep.profile
    return prep.permission_context.is_superuser and (
        profile.plugin_type == PluginType.SUPERUSER or not profile.limit_superuser
    )


async def _reserve_gold(
    prep: AuthPreparation,
    session: Uninfo,
    cost_gold: int,
    commit: SideEffectCommit,
) -> None:
    """预扣金币，余额不足时拒绝，matcher 未实际完成时由 SideEffectCommit 回滚

    参数:
        prep: 权限检查准备数据
        session: Uninfo
        cost_gold: 金币数量
        commit: 副作用提交器

    异常:
        DependencyUnavailable: 数据库不可用，跳过本条消息
        SkipPluginException: 当前平台无法解析业务身份或金币不足
    """
    from zhenxun.services.business_identity import (
        business_user_id,
        supports_business_identity,
    )

    module = prep.profile.module
    if not supports_business_identity(prep.snapshot.context.platform_scope):
        raise SkipPluginException(
            f"{module} 当前平台不支持金币消耗，已取消执行...",
            tip_message="当前平台暂不支持金币功能..",
        )
    if is_db_unhealthy():
        raise DependencyUnavailable("cost_database_unavailable")
    try:
        reservation = await UserConsole.reserve_gold(
            await business_user_id(session),
            cost_gold,
            GoldHandle.PLUGIN,
            module,
            PlatformUtils.get_platform(session),
        )
    except InsufficientGold:
        raise SkipPluginException(
            f"{module} 金币不足，已取消执行...",
            tip_message=f"金币不足..该功能需要{cost_gold}金币..",
        ) from None
    await commit.reserve_gold(
        reservation, amount=cost_gold, metadata={"module": module}
    )
    logger.debug(f"预扣功能花费金币: {cost_gold}", LOGGER_COMMAND, session=session)


async def _run_auth(
    matcher: Matcher,
    event: Event,
    bot: Bot,
    session: Uninfo,
    context: EventContext,
    skip_ban: bool,
    state: dict | None,
    commit: SideEffectCommit,
) -> None:
    """权限检查主流程，拒绝时抛出 SkipPluginException

    参数:
        matcher: matcher
        event: Event
        bot: Bot
        session: Uninfo
        context: EventContext
        skip_ban: 是否跳过 ban 检查
        state: matcher state
        commit: 副作用提交器
    """
    prep = await _prepare_auth_state(
        module=commit.module, context=context, bot=bot, state=state
    )
    if not skip_ban:
        await _check_ban(prep, matcher, session)
    if _check_policy(prep, event):
        return
    if _is_superuser_exempt(prep):
        raise IsSuperuserException()
    bot_filter(session, context=prep.permission_context)
    if prep.profile.has_limit:
        await commit.reserve_limit(
            await reserve_auth_limit(
                prep.plugin, session, context=prep.permission_context
            )
        )
    if prep.profile.cost_gold > 0:
        await _reserve_gold(prep, session, prep.profile.cost_gold, commit)


async def auth(
    matcher: Matcher,
    event: Event,
    bot: Bot,
    session: Uninfo,
    *,
    context: EventContext,
    skip_ban: bool = False,
    state: dict | None = None,
):
    """权限检查

    参数:
        matcher: matcher
        event: Event
        bot: bot
        session: Uninfo
        context: EventContext
        skip_ban: 是否跳过 ban 检查
        state: matcher state

    异常:
        IgnoredException: 权限不足或依赖不可用
    """
    module = matcher.plugin_name or ""
    if not module or _is_hidden_plugin(matcher):
        return
    start_time = time.time()
    limit_entity = context.limit_entity
    side_effect_cache = get_permission_side_effect_cache(
        state=state, event_cache=context.event_cache
    )
    commit = SideEffectCommit(
        session=session,
        module=module,
        owner_matcher_id=id(matcher),
        limit_entity=limit_entity,
    )
    # result: None 表示不缓存，否则为 (是否放行, 拒绝原因)
    result: tuple[bool, str | None] | None = None
    async with side_effect_cache.lock_for(module):
        revision = RuntimeCacheMutation.revision
        if side_effect_cache.revision != revision:
            side_effect_cache.auth_results.clear()
            side_effect_cache.revision = revision
        cached = side_effect_cache.auth_results.get(module)
        try:
            if cached is not None:
                if not cached[0]:
                    raise SkipPluginException(cached[1] or "auth cached skip")
                return
            await _run_auth(
                matcher, event, bot, session, context, skip_ban, state, commit
            )
            if commit.has_pending:
                side_effect_cache.commits[module] = commit
            else:
                result = (True, None)
        except (DependencyUnavailable, TimeoutError, asyncio.TimeoutError) as error:
            reason = (
                str(error)
                if isinstance(error, DependencyUnavailable)
                else "permission_timeout"
            )
            await commit.rollback_all("auth_unavailable")
            logger.warning(
                f"权限检查依赖不可用，跳过本条消息: {reason}",
                LOGGER_COMMAND,
                session=session,
            )
            raise IgnoredException("权限检测 ignore") from None
        except SkipPluginException as e:
            await commit.rollback_all("auth_skip")
            if e.tip_message:
                await commit.send_permission_tip(
                    e.tip_message,
                    e.tip_check_tag,
                    background=e.tip_background,
                    timeout=e.tip_timeout,
                )
            logger.info(str(e), LOGGER_COMMAND, session=session)
            if "deferred" not in str(e):
                result = (False, str(e) or "skip_plugin")
            raise IgnoredException("权限检测 ignore") from None
        except IsSuperuserException:
            logger.debug("超级用户跳过权限检测...", LOGGER_COMMAND, session=session)
            if commit.has_pending:
                side_effect_cache.commits[module] = commit
            else:
                result = (True, None)
        except PermissionExemption as e:
            await commit.rollback_all("permission_exemption")
            logger.info(str(e), LOGGER_COMMAND, session=session)
            result = (True, None)
        except Exception:
            await commit.rollback_all("auth_exception")
            raise
        finally:
            if result is not None and revision == RuntimeCacheMutation.revision:
                side_effect_cache.auth_results[module] = result
            elapsed = time.time() - start_time
            if elapsed > WARNING_THRESHOLD:
                logger.warning(
                    f"权限检查耗时过长: {elapsed:.3f}s, 模块: {module}",
                    LOGGER_COMMAND,
                    session=session,
                )
