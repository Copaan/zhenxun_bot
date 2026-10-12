import asyncio
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any, ClassVar

from nonebot_plugin_uninfo import Uninfo

from zhenxun.models.plugin_info import PluginInfo
from zhenxun.services.cache.runtime_cache import PluginLimitMemoryCache
from zhenxun.services.cache.runtime_snapshot import PluginLimitSnapshot
from zhenxun.services.log import logger
from zhenxun.utils.enum import LimitWatchType, PluginLimitType
from zhenxun.utils.limiters import CountLimiter, FreqLimiter, UserBlockLimiter
from zhenxun.utils.message import MessageUtils
from zhenxun.utils.time_utils import TimeUtils

from .config import LOGGER_COMMAND
from .context import PermissionContext
from .exception import SkipPluginException

_LIMIT_NOTICE_CD = 2
_LIMIT_NOTICE_LIMITER = FreqLimiter(_LIMIT_NOTICE_CD)
_LIMIT_NOTICE_TASKS: set[asyncio.Task] = set()
_LIMIT_ORDER = (PluginLimitType.CD, PluginLimitType.BLOCK, PluginLimitType.COUNT)

Limiter = FreqLimiter | UserBlockLimiter | CountLimiter


@dataclass(slots=True)
class LimitState:
    limit: PluginLimitSnapshot
    limiter: Limiter


@dataclass(slots=True)
class LimitReservation:
    module: str
    releases: list[Callable[[], None]] = field(default_factory=list)
    should_auto_unblock: bool = False
    active: bool = True

    def commit(self) -> None:
        self.active = False
        self.releases.clear()

    def release(self) -> None:
        if not self.active:
            return
        for release in reversed(self.releases):
            release()
        self.active = False
        self.releases.clear()


def _limit_key(
    limit: PluginLimitSnapshot,
    user_id: str,
    group_id: str | None,
    channel_id: str | None,
) -> str:
    if group_id and limit.watch_type == LimitWatchType.GROUP:
        return channel_id or group_id
    return user_id


def _send_limit_notice(message: str, format_kwargs: dict[str, Any], key: str) -> None:
    if not _LIMIT_NOTICE_LIMITER.check(key):
        return
    _LIMIT_NOTICE_LIMITER.start_cd(key)

    async def _send():
        try:
            await MessageUtils.build_message(message, format_args=format_kwargs).send()
        except Exception as exc:
            logger.error("limit notice send failed", LOGGER_COMMAND, e=exc)

    task = asyncio.create_task(_send())
    _LIMIT_NOTICE_TASKS.add(task)
    task.add_done_callback(_LIMIT_NOTICE_TASKS.discard)


class LimitManager:
    _states: ClassVar[dict[tuple[str, PluginLimitType], LimitState]] = {}

    @classmethod
    def _state(cls, limit: PluginLimitSnapshot) -> LimitState | None:
        """获取限制对应的限流器状态，配置变化时原地更新参数

        参数:
            limit: 插件限制

        返回:
            LimitState | None: 限流器状态，限制无效时返回 None
        """
        key = (limit.module, limit.limit_type)
        state = cls._states.get(key)
        if limit.limit_type == PluginLimitType.CD:
            cd = int(limit.cd or 0)
            if state is None or not isinstance(state.limiter, FreqLimiter):
                state = LimitState(limit, FreqLimiter(cd))
            else:
                state.limiter.default_cd = cd
        elif limit.limit_type == PluginLimitType.COUNT:
            max_count = int(limit.max_count or 0)
            if max_count <= 0:
                return None
            if state is None or not isinstance(state.limiter, CountLimiter):
                state = LimitState(limit, CountLimiter(max_count))
            else:
                state.limiter.max = max_count
        elif limit.limit_type == PluginLimitType.BLOCK:
            state = state or LimitState(limit, UserBlockLimiter())
        else:
            return None
        state.limit = limit
        cls._states[key] = state
        return state

    @classmethod
    def unblock(
        cls, module: str, user_id: str, group_id: str | None, channel_id: str | None
    ):
        """解除插件block

        参数:
            module: 模块名
            user_id: 用户id
            group_id: 群组id
            channel_id: 频道id
        """
        state = cls._states.get((module, PluginLimitType.BLOCK))
        if state and isinstance(state.limiter, UserBlockLimiter):
            key = _limit_key(state.limit, user_id, group_id, channel_id)
            logger.debug(
                f"解除对象: {key} 的block限制",
                LOGGER_COMMAND,
                session=user_id,
                group_id=group_id,
            )
            state.limiter.set_false(key)

    @classmethod
    async def reserve(
        cls,
        module: str,
        user_id: str,
        group_id: str | None,
        channel_id: str | None,
    ) -> LimitReservation:
        """检查并预留插件限制，调用方失败时可 release 回滚

        参数:
            module: 模块名
            user_id: 用户id
            group_id: 群组id
            channel_id: 频道id

        返回:
            LimitReservation: 限制预留

        异常:
            SkipPluginException: 正在限制中
        """
        by_type = {
            limit.limit_type: limit
            for limit in await PluginLimitMemoryCache.get_limits(module)
        }
        reservation = LimitReservation(module=module)
        try:
            for limit_type in _LIMIT_ORDER:
                if (limit := by_type.get(limit_type)) is None:
                    continue
                if (state := cls._state(limit)) is None:
                    continue
                if limit_type == PluginLimitType.BLOCK:
                    reservation.should_auto_unblock = True
                reservation.releases.append(
                    cls._reserve(state, user_id, group_id, channel_id)
                )
        except Exception:
            reservation.release()
            raise
        return reservation

    @classmethod
    def _reserve(
        cls,
        state: LimitState,
        user_id: str,
        group_id: str | None,
        channel_id: str | None,
    ) -> Callable[[], None]:
        """检查单项限制并占用

        参数:
            state: 限流器状态
            user_id: 用户id
            group_id: 群组id
            channel_id: 频道id

        返回:
            Callable[[], None]: 回滚函数

        异常:
            SkipPluginException: 正在限制中
        """
        limit, limiter = state.limit, state.limiter
        is_limit = (
            limit.watch_type == LimitWatchType.ALL
            or (group_id and limit.watch_type == LimitWatchType.GROUP)
            or (not group_id and limit.watch_type == LimitWatchType.USER)
        )
        if not is_limit:
            return lambda: None
        key = _limit_key(limit, user_id, group_id, channel_id)
        if not limiter.check(key):
            if limit.result:
                format_kwargs = {}
                if isinstance(limiter, FreqLimiter):
                    cd_str = TimeUtils.format_duration(limiter.left_time(key))
                    format_kwargs = {"cd": cd_str}
                _send_limit_notice(
                    limit.result,
                    format_kwargs,
                    f"{limit.module}:{limit.limit_type}:{key}",
                )
            raise SkipPluginException(
                f"{limit.module}({limit.limit_type}) 正在限制中..."
            )
        logger.debug(
            f"开始进行限制 {limit.module}({limit.limit_type})...",
            LOGGER_COMMAND,
            session=user_id,
            group_id=group_id,
        )
        if isinstance(limiter, FreqLimiter):
            had_next_time = key in limiter.next_time
            old_next_time = limiter.next_time.get(key, 0.0)
            limiter.start_cd(key)

            def release_freq() -> None:
                if had_next_time:
                    limiter.next_time[key] = old_next_time
                else:
                    limiter.next_time.pop(key, None)

            return release_freq
        if isinstance(limiter, UserBlockLimiter):
            old_flag = limiter.flag_data.get(key, False)
            old_time = limiter.time.get(key, 0.0)
            limiter.set_true(key)

            def release_block() -> None:
                limiter.flag_data[key] = old_flag
                if old_time:
                    limiter.time[key] = old_time
                else:
                    limiter.time.pop(key, None)

            return release_block
        old_count = limiter.count.get(key, 0)
        limiter.increase(key)

        def release_count() -> None:
            limiter.count[key] = old_count

        return release_count


async def reserve_auth_limit(
    plugin: PluginInfo,
    session: Uninfo,
    *,
    context: PermissionContext | None = None,
) -> LimitReservation:
    """预留插件限制

    参数:
        plugin: PluginInfo
        session: Uninfo
        context: 权限上下文

    返回:
        LimitReservation: 限制预留
    """
    del session
    if context is None:
        raise RuntimeError("reserve_auth_limit requires context")
    entity = context.event.limit_entity
    return await LimitManager.reserve(
        plugin.module, entity.user_id, entity.group_id, entity.channel_id
    )
