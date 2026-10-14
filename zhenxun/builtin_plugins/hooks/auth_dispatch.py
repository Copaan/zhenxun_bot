import asyncio
from collections.abc import Callable
from contextlib import AsyncExitStack, suppress
import importlib
import re
import sys
from typing import Any

import anyio
from exceptiongroup import catch
from nonebot import get_loaded_plugins
from nonebot.adapters import Bot, Event
from nonebot.consts import CMD_ARG_KEY, CMD_KEY, PREFIX_KEY, RAW_CMD_KEY
from nonebot.exception import NoLogException, StopPropagation
from nonebot.log import logger as nb_logger
from nonebot.matcher import Matcher, matchers
import nonebot.message as nb_message
from nonebot.rule import TrieRule
from nonebot.utils import escape_tag

from zhenxun.adapters.qq_official.context import with_official_event_context
from zhenxun.configs.utils import PluginExtraData
from zhenxun.services.log import logger

from .auth.config import LOGGER_COMMAND
from .auth.context import (
    STATE_PLAIN_TEXT,
    get_event_context,
    get_permission_side_effect_cache,
    set_route_modules,
)
from .auth_activation import (
    ActivationContext,
    HandlerActivationIndex,
    adapter_contract_matches,
    matcher_supported_adapters,
    text_match_candidates,
)

_HANDLER_ACTIVATION_INDEX = HandlerActivationIndex()
_active_matchers = 0
# 同时处理的普通消息上限，限制积压时每轮事件循环的工作量
EVENT_ADMISSION_LIMIT = 64
# 普通消息占用名额的最长秒数，超时后让出，等待 I/O 的处理不阻塞后续消息
EVENT_ADMISSION_SECONDS = 1.0
_event_admission: tuple[asyncio.AbstractEventLoop, asyncio.Semaphore] | None = None

_ROUTE_INDEX_LOCK = asyncio.Lock()
_ROUTE_INDEX_READY = False
_ROUTE_COMMAND_MAP: dict[str, set[str]] = {}
_ROUTE_PREFIX_MAP: dict[str, set[str]] = {}

_LEADING_PLACEHOLDER_PATTERN = re.compile(r"^(?:\s*(?:\[[^\]]*]|\<[^>]*>))+\s*")
_original_handle_events: dict[Any, Any] = {}


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


def _normalize_dispatch_text(text: str) -> str:
    return _LEADING_PLACEHOLDER_PATTERN.sub("", text.strip()).strip()


def _event_plain_text(event: Event) -> str:
    with suppress(Exception):
        # OneBot v11 的 raw_message 保留昵称前缀，保证“真寻日报”等命令能被匹配
        raw = getattr(event, "raw_message", None)
        if isinstance(raw, str) and raw:
            return _normalize_dispatch_text(raw)
        return _normalize_dispatch_text(event.get_plaintext() or "")
    return ""


def _state_plain_text(state: dict) -> str:
    context = get_event_context(state)
    if context is not None:
        return context.plain_text.strip()
    text = state.get(STATE_PLAIN_TEXT)
    return text.strip() if isinstance(text, str) else ""


def _trie_prefix(state: dict) -> dict:
    prefix = state.get(PREFIX_KEY)
    return prefix if isinstance(prefix, dict) else {}


def _trie_command_text(state: dict) -> str:
    command = _trie_prefix(state).get(CMD_KEY)
    if isinstance(command, tuple):
        return _normalize_dispatch_text(" ".join(str(item) for item in command))
    return _normalize_dispatch_text(command) if isinstance(command, str) else ""


def _trie_raw_command(state: dict) -> str:
    raw_command = _trie_prefix(state).get(RAW_CMD_KEY)
    return _normalize_dispatch_text(raw_command) if isinstance(raw_command, str) else ""


def _trie_command_arg_text(state: dict) -> str:
    message = _trie_prefix(state).get(CMD_ARG_KEY)
    if message is None:
        return ""
    with suppress(Exception):
        extractor = getattr(message, "extract_plain_text", None)
        if callable(extractor):
            return _normalize_dispatch_text(str(extractor() or ""))
    return _normalize_dispatch_text(str(message))


def _event_raw_message_text(event: Event) -> str:
    with suppress(Exception):
        message = getattr(event, "message", None)
        if message is not None:
            return str(message)
    return ""


def _event_text_candidates(
    event: Event, state: dict, plain_text: str, raw_text: str
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
    with suppress(Exception):
        getter = getattr(event, "get_plaintext", None)
        if callable(getter):
            add(getter())
    add(raw_text)
    return tuple(candidates)


def _event_to_me(event: Event) -> bool:
    with suppress(Exception):
        getter = getattr(event, "is_tome", None)
        if callable(getter):
            return bool(getter())
    return bool(getattr(event, "to_me", False))


async def _prepare_dispatch_state(
    bot: Bot, event: Event, state: dict
) -> ActivationContext:
    """写入事件纯文本与命中的插件模块，并生成 matcher 预筛上下文

    参数:
        bot: Bot
        event: Event
        state: 事件 state

    返回:
        ActivationContext: matcher 预筛上下文
    """
    await ensure_route_index()
    get_permission_side_effect_cache(state=state)
    plain_text = _state_plain_text(state)
    if not plain_text:
        plain_text = _event_plain_text(event)
        if plain_text:
            state[STATE_PLAIN_TEXT] = plain_text
    raw_text = _event_raw_message_text(event)
    texts = _event_text_candidates(event, state, plain_text, raw_text)
    route_modules: set[str] = set()
    for text in texts:
        route_modules.update(_match_route_modules(text))
    if (context := get_event_context(state)) is not None:
        set_route_modules(state, context, route_modules)
    else:
        state["_zx_route_modules"] = route_modules
    return ActivationContext(
        event_type=event.get_type(),
        event=event,
        plain_text=texts[0] if texts else plain_text,
        raw_text="\n".join(texts) if texts else raw_text,
        to_me=_event_to_me(event),
        adapter=bot.adapter,
    )


def _trim_leading_text(message: Any) -> None:
    if not message:
        return
    segment = message[0]
    if getattr(segment, "type", None) != "text":
        return
    data = getattr(segment, "data", None)
    if not isinstance(data, dict):
        return
    data["text"] = str(data.get("text", "")).lstrip("\xa0").lstrip()
    if not data["text"]:
        del message[0]


def _is_self_mention_segment(segment: Any, bot: Bot) -> bool:
    if getattr(segment, "type", None) not in {"mention_user", "group_mention_user"}:
        return False
    data = getattr(segment, "data", None)
    if not isinstance(data, dict):
        return False
    if data.get("is_you") or data.get("is_bot"):
        return True
    user_id = data.get("user_id")
    return user_id is not None and str(user_id) == str(bot.self_id)


def _normalize_qq_self_at_message(bot: Bot, event: Event) -> None:
    """去掉 QQ 官方 @ 事件开头指向 bot 自身的提及段，使命令头能被正常匹配

    参数:
        bot: Bot
        event: Event
    """
    if event.__class__.__name__ not in {
        "AtMessageCreateEvent",
        "GroupAtMessageCreateEvent",
    }:
        return
    with suppress(Exception):
        if str(bot.adapter.get_name()).lower() != "qq":
            return
        message = event.get_message()
        if not message or not _is_self_mention_segment(message[0], bot):
            return
        message.pop(0)
        setattr(event, "to_me", True)
        _trim_leading_text(message)
        if not message:
            segment = importlib.import_module("nonebot.adapters.qq.message")
            message.append(segment.MessageSegment.text(""))


def _is_account_binding_command(event: Event, bot: Bot) -> bool:
    """账号绑定指令不交给 ChatInter 兜底 matcher 处理

    参数:
        event: Event
        bot: Bot

    返回:
        bool: 是否为账号绑定指令
    """
    from zhenxun.services.account_binding import (
        bare_binding_code,
        binding_command,
    )

    return (
        binding_command(event) is not None or bare_binding_code(bot, event) is not None
    )


def _is_chatinter_matcher(matcher: type[Matcher]) -> bool:
    plugin = getattr(matcher, "plugin", None)
    module = str(
        getattr(plugin, "module_name", "") or getattr(plugin, "module", "") or ""
    )
    return module == "zhenxun.plugins.chatinter" or module.startswith(
        "zhenxun.plugins.chatinter."
    )


def has_active_dispatch() -> bool:
    """是否有 matcher 正在执行

    返回:
        bool: 是否有正在执行的 matcher
    """
    return _active_matchers > 0


def _select_matchers(
    priority: int,
    priority_matchers: list[type[Matcher]],
    context: ActivationContext,
    bot: Bot,
    indexed: bool,
    reserve_binding: bool,
) -> list[type[Matcher]]:
    selected = None
    if indexed:
        try:
            selected = _HANDLER_ACTIVATION_INDEX.select_priority(
                priority, priority_matchers, context
            )
        except Exception as exc:
            logger.warning(
                "matcher 预筛失败，当前优先级按适配器筛选", LOGGER_COMMAND, e=exc
            )
    if selected is None:
        selected = [
            matcher
            for matcher in priority_matchers
            if adapter_contract_matches(
                matcher_supported_adapters(matcher), bot.adapter
            )
        ]
    if reserve_binding:
        selected = [m for m in selected if not _is_chatinter_matcher(m)]
    return selected


async def _dispatch_priority(
    selected: list[type[Matcher]],
    bot: Bot,
    event: Event,
    state: dict,
    stack: AsyncExitStack,
    dependency_cache: dict,
    on_stop,
) -> None:
    """并发执行同一优先级选中的 matcher

    参数:
        selected: 选中的 matcher
        bot: Bot
        event: Event
        state: 事件 state
        stack: 事件 AsyncExitStack
        dependency_cache: 依赖缓存
        on_stop: 阻断事件传播时的回调
    """

    async def run(matcher: type[Matcher]) -> None:
        global _active_matchers
        descriptor = _HANDLER_ACTIVATION_INDEX.descriptor_for(matcher)
        matcher_state = state.copy()
        matcher_state["_zx_passive_matcher"] = (
            descriptor is not None
            and not descriptor.temp
            and not descriptor.command_like
        )
        _active_matchers += 1
        try:
            # 异常在子任务内处理，避免一个 matcher 失败取消同级 matcher
            with catch(
                {
                    StopPropagation: on_stop,
                    Exception: nb_message._handle_exception(
                        "Error when checking Matcher."
                    ),
                }
            ):
                await nb_message.check_and_run_matcher(
                    matcher, bot, event, matcher_state, stack, dependency_cache
                )
        finally:
            _active_matchers -= 1

    async with anyio.create_task_group() as tg:
        for matcher in selected:
            tg.start_soon(run, matcher)


async def _admit_event(event: Event) -> Callable[[], None]:
    """普通消息占用处理名额，命令消息与非消息事件不占用

    名额在处理结束或占用满 EVENT_ADMISSION_SECONDS 秒时释放。

    参数:
        event: Event

    返回:
        Callable[[], None]: 释放名额，可重复调用
    """
    global _event_admission
    if event.get_type() != "message" or await get_route_context(
        _event_plain_text(event), None
    ):
        return lambda: None
    loop = asyncio.get_running_loop()
    if _event_admission is None or _event_admission[0] is not loop:
        _event_admission = (loop, asyncio.Semaphore(EVENT_ADMISSION_LIMIT))
    gate = _event_admission[1]
    await gate.acquire()
    released = False

    def release() -> None:
        nonlocal released
        if not released:
            released = True
            gate.release()

    loop.call_later(EVENT_ADMISSION_SECONDS, release)
    return release


@with_official_event_context
async def handle_event(bot: Bot, event: Event) -> None:
    """处理事件：普通消息限时占用处理名额，按命令文本预筛 matcher，再按优先级并发执行

    参数:
        bot: Bot
        event: Event
    """
    _normalize_qq_self_at_message(bot, event)
    reserve_binding = _is_account_binding_command(event, bot)
    show_log = True
    log_msg = f"<m>{escape_tag(bot.type)} {escape_tag(bot.self_id)}</m> | "
    try:
        log_msg += event.get_log_string()
    except NoLogException:
        show_log = False
    if show_log:
        nb_logger.opt(colors=True).success(log_msg)

    state: dict[Any, Any] = {}
    dependency_cache = {}
    release = await _admit_event(event)
    async with AsyncExitStack() as stack:
        stack.callback(release)
        if not await nb_message._apply_event_preprocessors(
            bot=bot,
            event=event,
            state=state,
            stack=stack,
            dependency_cache=dependency_cache,
        ):
            return
        try:
            TrieRule.get_value(bot, event, state)
        except Exception as e:
            nb_logger.opt(colors=True, exception=e).warning(
                "Error while parsing command for event"
            )
        context = await _prepare_dispatch_state(bot, event, state)
        indexed = True
        try:
            _HANDLER_ACTIVATION_INDEX.ensure_fresh(matchers)
        except Exception as exc:
            indexed = False
            logger.warning(
                "matcher 预筛索引构建失败，按适配器筛选", LOGGER_COMMAND, e=exc
            )

        break_flag = False

        def _handle_stop_propagation(_exc_group) -> None:
            nonlocal break_flag
            break_flag = True
            nb_logger.debug("Stop event propagation")

        for priority in sorted(matchers.keys()):
            if break_flag:
                break
            if show_log:
                nb_logger.debug(f"Checking for matchers in priority {priority}...")
            if not (priority_matchers := matchers[priority]):
                continue
            with catch(
                {
                    StopPropagation: _handle_stop_propagation,
                    Exception: nb_message._handle_exception(
                        "<r><bg #f8bbd0>Error when checking Matcher.</bg #f8bbd0></r>"
                    ),
                }
            ):
                selected = _select_matchers(
                    priority, priority_matchers, context, bot, indexed, reserve_binding
                )
                if selected:
                    await _dispatch_priority(
                        selected,
                        bot,
                        event,
                        state,
                        stack,
                        dependency_cache,
                        _handle_stop_propagation,
                    )

        if show_log:
            nb_logger.debug("Checking for matchers completed")
        await nb_message._apply_event_postprocessors(
            bot, event, state, stack, dependency_cache
        )


async def install_dispatch() -> None:
    """建立路由索引并接管 NoneBot 事件分发"""
    await ensure_route_index()
    if _original_handle_events:
        return
    original = nb_message.handle_event
    # 适配器以 `from nonebot.message import handle_event` 引用分发入口
    for module in list(sys.modules.values()):
        namespace = getattr(module, "__dict__", None)
        if isinstance(namespace, dict) and namespace.get("handle_event") is original:
            _original_handle_events[module] = original
            setattr(module, "handle_event", handle_event)


def uninstall_dispatch() -> None:
    """恢复 NoneBot 原生事件分发"""
    for module, original in _original_handle_events.items():
        setattr(module, "handle_event", original)
    _original_handle_events.clear()
