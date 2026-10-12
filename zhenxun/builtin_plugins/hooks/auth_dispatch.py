import asyncio
import contextlib
import re
import time
from urllib.parse import urlparse

from nonebot import get_loaded_plugins
from nonebot.adapters import Bot, Event
from nonebot.consts import CMD_ARG_KEY, CMD_KEY, PREFIX_KEY, RAW_CMD_KEY
from nonebot.matcher import Matcher
import nonebot.message as nb_message

from zhenxun.configs.config import BotConfig
from zhenxun.configs.utils import PluginExtraData
from zhenxun.services.cache.cache_containers import CacheDict
from zhenxun.services.message_load import signal_overload
from zhenxun.services.message_resources import matcher_resource_lane, resource_lanes

from .auth.context import (
    STATE_PLAIN_TEXT,
    get_event_context,
    get_permission_side_effect_cache,
    set_route_modules,
)
from .auth_activation import (
    ActivationContext,
    HandlerActivationIndex,
    classify_matcher_lane,
    text_match_candidates,
)
from .auth_event_selector import (
    EventDispatchContext,
    HandleEventSelectorDependencies,
    install_handle_event_selector,
    uninstall_handle_event_selector,
)


def _passive_db_limit() -> int:
    scheme = urlparse(BotConfig.db_url or "").scheme.lower()
    if scheme == "sqlite":
        return 1
    if scheme.startswith("postgres"):
        return 6
    return 4 if scheme == "mysql" else 2


# 各分发通道同时执行的 matcher 上限
DISPATCH_LANE_LIMITS: dict[str, int] = {
    "command_exact": 96,
    "command_shortcut": 32,
    "command_regex": 8,
    "system": 64,
    "passive_light": 12,
    "passive_db": _passive_db_limit(),
    "passive_http": 4,
    "passive_ai": 2,
    "passive_render": 2,
}
# 单个事件选中 matcher 数超过该值时视为过载
OVERLOAD_SELECTED_THRESHOLD = 48
# 通道等待超过该毫秒数时视为过载
OVERLOAD_LANE_WAIT_MS = 200.0
# 事件路由结果缓存秒数
ROUTE_CACHE_TTL = 2

_DISPATCH_LANE_SEMAPHORES = {
    lane: asyncio.Semaphore(limit) for lane, limit in DISPATCH_LANE_LIMITS.items()
}
_HANDLER_ACTIVATION_INDEX = HandlerActivationIndex()
_ROUTE_CACHE = CacheDict("AUTH_MATCHER_ROUTE_CACHE", expire=ROUTE_CACHE_TTL)

_ROUTE_INDEX_LOCK = asyncio.Lock()
_ROUTE_INDEX_READY = False
_ROUTE_COMMAND_MAP: dict[str, set[str]] = {}
_ROUTE_PREFIX_MAP: dict[str, set[str]] = {}

_LEADING_PLACEHOLDER_PATTERN = re.compile(r"^(?:\s*(?:\[[^\]]*]|\<[^>]*>))+\s*")


def _normalize_command(command: str) -> str:
    text = _LEADING_PLACEHOLDER_PATTERN.sub("", command.strip())
    cut_points = [idx for idx in (text.find("["), text.find("<")) if idx >= 0]
    if cut_points:
        text = text[: min(cut_points)]
    text = re.sub(r"\s+", " ", text).strip()
    return re.sub(r"(?:\s+[?*]+|[?*]+)$", "", text).strip()


def _split_command_variants(command: str) -> tuple[str, ...]:
    text = command.strip()
    if not text:
        return ()
    if text.startswith("/"):
        return (text,)
    if "/" in text and " " not in text:
        parts = tuple(part.strip() for part in text.split("/") if part.strip())
        if parts:
            return parts
    return (text,)


def _is_ambiguous_route_command(command: str) -> bool:
    text = command.strip()
    if not text:
        return True
    if any(token in text for token in ("?", "*", "|", "(", ")", "^", "$", "re:")):
        return True
    return "xx" in text.lower()


def _extract_commands(extra: PluginExtraData) -> tuple[set[str], bool]:
    commands = {c.command for c in extra.commands if c.command}
    commands.update(extra.aliases or set())
    normalized_commands: set[str] = set()
    has_ambiguous = False
    for command in commands:
        normalized = _normalize_command(command)
        if not normalized:
            continue
        for variant in _split_command_variants(normalized):
            if _is_ambiguous_route_command(variant):
                has_ambiguous = True
                continue
            normalized_commands.add(variant)
    return normalized_commands, has_ambiguous


async def ensure_route_index():
    """根据插件元数据中的命令建立 命令 -> 插件 的路由索引"""
    global _ROUTE_INDEX_READY
    if _ROUTE_INDEX_READY:
        return
    async with _ROUTE_INDEX_LOCK:
        if _ROUTE_INDEX_READY:
            return
        _ROUTE_COMMAND_MAP.clear()
        _ROUTE_PREFIX_MAP.clear()
        for plugin in get_loaded_plugins():
            if not plugin.metadata:
                continue
            extra_data = PluginExtraData.from_extra(plugin.metadata.extra, plugin.name)
            command_set, has_ambiguous = _extract_commands(extra_data)
            if not command_set or has_ambiguous:
                continue
            for normalized in command_set:
                _ROUTE_COMMAND_MAP.setdefault(normalized, set()).add(plugin.name)
                _ROUTE_PREFIX_MAP.setdefault(normalized[0], set()).add(normalized)
        _ROUTE_INDEX_READY = True


def _match_route_modules(text: str) -> set[str]:
    text = text.strip()
    if not text:
        return set()
    matched_modules: set[str] = set()
    for command in _ROUTE_PREFIX_MAP.get(text[0], ()):
        if text == command or (
            text.startswith(command) and text[len(command)].isspace()
        ):
            matched_modules.update(_ROUTE_COMMAND_MAP.get(command, ()))
    return matched_modules


async def get_route_context(text: str, event_cache: dict | None) -> set[str]:
    """获取文本命中的插件模块

    参数:
        text: 消息纯文本
        event_cache: 事件级缓存

    返回:
        set[str]: 命中的插件模块
    """
    if not text:
        return set()
    if event_cache is not None and "route_modules" in event_cache:
        return event_cache["route_modules"]
    await ensure_route_index()
    matched = set()
    for candidate in text_match_candidates(text):
        matched.update(_match_route_modules(candidate))
    if event_cache is not None:
        event_cache["route_modules"] = matched
    return matched


def _collect_ai_route_values(
    event: Event, state: dict | None, attr: str, *, casefold: bool
) -> set[str]:
    state_key = f"_zx{attr}"
    if state is not None:
        cached = state.get(state_key)
        if isinstance(cached, set):
            return cached
    raw_value = getattr(event, attr, None)
    if isinstance(raw_value, str):
        raw_value = (raw_value,)
    result: set[str] = set()
    if isinstance(raw_value, set | frozenset | list | tuple):
        for item in raw_value:
            if isinstance(item, str) and (normalized := item.strip()):
                result.add(normalized.casefold() if casefold else normalized)
    if state is not None and result:
        state[state_key] = result
    return result


def _normalize_dispatch_text(text: str) -> str:
    return _LEADING_PLACEHOLDER_PATTERN.sub("", text.strip()).strip()


def _event_plain_text(event: Event) -> str:
    with contextlib.suppress(Exception):
        # OneBot v11 的 raw_message 保留昵称前缀，保证“真寻日报”等命令能被匹配
        raw = getattr(event, "raw_message", None)
        if isinstance(raw, str) and raw:
            return _normalize_dispatch_text(raw)
        return _normalize_dispatch_text(event.get_plaintext() or "")
    return ""


def _state_plain_text(state: dict | None) -> str:
    if state is None:
        return ""
    context = get_event_context(state)
    if context is not None:
        return context.plain_text.strip()
    text = state.get(STATE_PLAIN_TEXT)
    return text.strip() if isinstance(text, str) else ""


def _trie_prefix(state: dict | None) -> dict:
    prefix = state.get(PREFIX_KEY) if state is not None else None
    return prefix if isinstance(prefix, dict) else {}


def _trie_command_text(state: dict | None) -> str:
    command = _trie_prefix(state).get(CMD_KEY)
    if isinstance(command, tuple):
        return _normalize_dispatch_text(" ".join(str(item) for item in command))
    return _normalize_dispatch_text(command) if isinstance(command, str) else ""


def _trie_raw_command(state: dict | None) -> str:
    raw_command = _trie_prefix(state).get(RAW_CMD_KEY)
    return _normalize_dispatch_text(raw_command) if isinstance(raw_command, str) else ""


def _trie_command_arg_text(state: dict | None) -> str:
    message = _trie_prefix(state).get(CMD_ARG_KEY)
    if message is None:
        return ""
    with contextlib.suppress(Exception):
        extractor = getattr(message, "extract_plain_text", None)
        if callable(extractor):
            return _normalize_dispatch_text(str(extractor() or ""))
    return _normalize_dispatch_text(str(message))


def _event_raw_message_text(event: Event) -> str:
    with contextlib.suppress(Exception):
        message = getattr(event, "message", None)
        if message is not None:
            return str(message)
    return ""


def _event_text_candidates(
    event: Event,
    state: dict | None,
    plain_text: str = "",
    raw_text: str = "",
) -> tuple[str, ...]:
    candidates: list[str] = []

    def add(text: object) -> None:
        if not isinstance(text, str):
            return
        normalized = _normalize_dispatch_text(text)
        if normalized and normalized not in candidates:
            candidates.append(normalized)

    add(_trie_command_text(state))
    trie_raw = _trie_raw_command(state)
    add(trie_raw)
    trie_arg = _trie_command_arg_text(state)
    if trie_raw and trie_arg:
        add(f"{trie_raw} {trie_arg}")
    add(plain_text)
    with contextlib.suppress(Exception):
        getter = getattr(event, "get_plaintext", None)
        if callable(getter):
            add(getter())
    add(raw_text)
    return tuple(candidates)


def _event_to_me(event: Event) -> bool:
    with contextlib.suppress(Exception):
        getter = getattr(event, "is_tome", None)
        if callable(getter):
            return bool(getter())
    return bool(getattr(event, "to_me", False))


def _route_cache_key(event: Event) -> str:
    msg_id = getattr(event, "message_id", None)
    if msg_id is None:
        msg_id = getattr(event, "id", None)
    if msg_id is None:
        msg_id = id(event)
    user_id = getattr(event, "user_id", "")
    group_id = getattr(event, "group_id", "")
    channel_id = getattr(event, "channel_id", "")
    return f"{msg_id}:{user_id}:{group_id}:{channel_id}"


def _get_route_modules_for_event(event: Event, state: dict | None = None) -> set[str]:
    if state is not None:
        context = get_event_context(state)
        if context is not None and context.route_modules_loaded:
            return context.route_modules
        route_modules = state.get("_zx_route_modules")
        if isinstance(route_modules, set):
            return route_modules
    key = _route_cache_key(event)
    try:
        route_modules = _ROUTE_CACHE[key]
    except KeyError:
        raw_text = _event_raw_message_text(event)
        plain_text = _state_plain_text(state) or _event_plain_text(event)
        route_modules = set()
        for text in _event_text_candidates(event, state, plain_text, raw_text):
            route_modules.update(_match_route_modules(text))
        _ROUTE_CACHE[key] = route_modules
    if state is not None:
        context = get_event_context(state)
        if context is not None:
            set_route_modules(state, context, route_modules)
        else:
            state["_zx_route_modules"] = route_modules
    return route_modules


async def _build_dispatch_context(
    event: Event, state: dict | None = None
) -> EventDispatchContext:
    if state is not None:
        context = state.get("_zx_dispatch_context")
        if isinstance(context, EventDispatchContext):
            return context
    await ensure_route_index()
    plain_text = _state_plain_text(state)
    if not plain_text:
        plain_text = _event_plain_text(event)
        if state is not None and plain_text:
            state[STATE_PLAIN_TEXT] = plain_text
    _get_route_modules_for_event(event, state)
    ai_route_modules = _collect_ai_route_values(
        event, state, "_ai_route_modules", casefold=False
    )
    raw_text = _event_raw_message_text(event)
    trie_command_text = _trie_command_text(state)
    trie_raw_command = _trie_raw_command(state)
    context = EventDispatchContext(
        event_type=event.get_type(),
        plain_text=plain_text,
        raw_text=raw_text,
        trie_command_text=trie_command_text,
        trie_raw_command=trie_raw_command,
        text_candidates=_event_text_candidates(event, state, plain_text, raw_text),
        to_me=_event_to_me(event),
        ai_route_modules=ai_route_modules,
    )
    if state is not None:
        state["_zx_dispatch_context"] = context
    return context


def _dispatch_lane_for_matcher(
    matcher_cls: type[Matcher], context: EventDispatchContext
) -> str:
    descriptor = _HANDLER_ACTIVATION_INDEX.descriptor_for(matcher_cls)
    if descriptor is not None:
        return descriptor.lane
    if getattr(matcher_cls, "temp", False):
        return "system"
    matcher_type = getattr(matcher_cls, "type", "") or ""
    if (
        isinstance(matcher_type, str)
        and matcher_type
        and matcher_type != (context.event_type)
    ):
        return "system"
    return classify_matcher_lane(
        matcher_cls,
        ai_route_modules=context.ai_route_modules,
    )


def _activation_context_from_dispatch(
    context: EventDispatchContext,
    event: Event,
) -> ActivationContext:
    return ActivationContext(
        event=event,
        event_type=context.event_type,
        plain_text=context.text_candidates[0]
        if context.text_candidates
        else context.plain_text,
        raw_text="\n".join(context.text_candidates)
        if context.text_candidates
        else context.raw_text,
        to_me=context.to_me,
    )


def _new_dispatch_budget() -> dict[str, int]:
    return dict(DISPATCH_LANE_LIMITS)


def _merge_dispatch_budget(target: dict[str, int], source: dict[str, int]) -> None:
    for lane in DISPATCH_LANE_LIMITS:
        target[lane] = source.get(lane, target.get(lane, 0))


async def _acquire_dispatch_lane(lane: str):
    semaphore = _DISPATCH_LANE_SEMAPHORES.get(lane)
    if semaphore is None:
        return lambda: None
    started = time.perf_counter()
    await semaphore.acquire()
    if (time.perf_counter() - started) * 1000 >= OVERLOAD_LANE_WAIT_MS:
        signal_overload(2.0)
    released = False

    def release():
        nonlocal released
        if not released:
            released = True
            semaphore.release()

    async def reacquire():
        nonlocal released
        if released:
            await semaphore.acquire()
            released = False

    setattr(release, "reacquire", reacquire)
    return release


def has_active_dispatch() -> bool:
    """是否有 matcher 正在分发通道中执行

    返回:
        bool: 是否有正在执行的 matcher
    """
    return any(
        semaphore._value < DISPATCH_LANE_LIMITS[lane]
        for lane, semaphore in _DISPATCH_LANE_SEMAPHORES.items()
    )


def _prepare_handle_event_state(event: Event, state: dict) -> None:
    get_permission_side_effect_cache(state=state)
    if event.get_type() != "message":
        return
    plain_text = _event_plain_text(event)
    text_candidates = _event_text_candidates(
        event, state, plain_text, _event_raw_message_text(event)
    )
    if text_candidates:
        state["_zx_text_candidates"] = text_candidates
    if plain_text and not _state_plain_text(state):
        state[STATE_PLAIN_TEXT] = plain_text


def _build_matcher_state(base_state: dict) -> dict:
    # 副作用缓存先写入 base_state，copy 后各 matcher 共享同一对象
    get_permission_side_effect_cache(state=base_state)
    return base_state.copy()


async def _run_admitted_matcher(
    matcher: type[Matcher],
    bot: Bot,
    event: Event,
    state: dict,
    stack,
    dependency_cache,
    lane: str,
) -> None:
    from zhenxun.services.message_execution import current_dispatch_lease

    state["_zx_dispatch_lane"] = lane
    token = current_dispatch_lease.set(state.get("_zx_dispatch_lease"))
    try:
        await nb_message.check_and_run_matcher(
            matcher, bot, event, state, stack, dependency_cache
        )
    finally:
        current_dispatch_lease.reset(token)


async def _run_selected_matcher(
    matcher: type[Matcher],
    bot: Bot,
    event: Event,
    state: dict,
    stack,
    dependency_cache,
    lane: str = "command_exact",
) -> None:
    release = await _acquire_dispatch_lane(lane)
    try:
        await _run_admitted_matcher(
            matcher, bot, event, state, stack, dependency_cache, lane
        )
    finally:
        release()


_SELECTOR_DEPS = HandleEventSelectorDependencies(
    activation_index=_HANDLER_ACTIVATION_INDEX,
    overload_selected_threshold=OVERLOAD_SELECTED_THRESHOLD,
    prepare_handle_event_state=_prepare_handle_event_state,
    build_dispatch_context=_build_dispatch_context,
    activation_context_from_dispatch=_activation_context_from_dispatch,
    new_dispatch_budget=_new_dispatch_budget,
    dispatch_lane_for_matcher=_dispatch_lane_for_matcher,
    merge_dispatch_budget=_merge_dispatch_budget,
    build_matcher_state=_build_matcher_state,
    run_selected_matcher=_run_selected_matcher,
    acquire_dispatch_lane=_acquire_dispatch_lane,
    resource_lane_for_matcher=matcher_resource_lane,
    acquire_resource_lane=resource_lanes.acquire,
    run_admitted_matcher=_run_admitted_matcher,
)


async def install_dispatch() -> None:
    """建立路由索引并接管 NoneBot 事件分发"""
    await ensure_route_index()
    install_handle_event_selector(_SELECTOR_DEPS)


def uninstall_dispatch() -> None:
    """恢复 NoneBot 原生事件分发"""
    uninstall_handle_event_selector()
