from __future__ import annotations

from collections.abc import Iterable, Mapping
import contextlib
from dataclasses import dataclass
import html
import re
from typing import Any, Literal
import weakref

from loguru import logger
from nonebot.adapters import Adapter
from nonebot.matcher import Matcher

ActivationDecision = Literal["match", "miss", "unknown"]
ALCONNA_SHORTCUT_TOKEN_LIMIT = 16
ActivationLane = Literal[
    "command_exact",
    "command_shortcut",
    "command_regex",
    "system",
    "passive_light",
    "passive_db",
    "passive_http",
    "passive_ai",
    "passive_render",
]


@dataclass(frozen=True, slots=True)
class ActivationRuleDescriptor:
    kind: str
    value: object | None = None
    flags: int = 0
    ignorecase: bool = False
    deterministic_text: bool = False
    command_like: bool = False


@dataclass(slots=True)
class HandlerDescriptor:
    matcher: type[Matcher]
    module: str
    matcher_type: str
    priority: int
    lane: ActivationLane
    temp: bool = False
    block: bool = False
    command_like: bool = False
    deterministic_text: bool = False
    has_custom_rule: bool = False
    commands: tuple[str, ...] = ()
    shortcuts: tuple[str, ...] | None = None
    alconna: tuple[AlconnaDescriptor, ...] = ()
    rules: tuple[ActivationRuleDescriptor, ...] = ()
    supported_adapters: tuple[type[Adapter], ...] | None = None


@dataclass(frozen=True, slots=True)
class AlconnaShortcutDescriptor:
    pattern: re.Pattern[str]
    fuzzy: bool = True
    exact_tail: bool = False


@dataclass(frozen=True, slots=True)
class AlconnaDescriptor:
    """Alconna 命令头元数据

    prefilter 为 False 时（非字符串命令头/前缀、括号命令头、use_origin、
    context_style、会改写输入的扩展、快捷指令无法读取）该 matcher 不参与预筛。
    AlconnaRule 在命令头与快捷指令都未命中时必定返回 False，before/after rule
    不影响这一结论，因此自定义 rule 不阻止预筛。
    """

    command: str = ""
    aliases: tuple[str, ...] = ()
    prefixes: tuple[str, ...] = ()
    separators: str = " "
    shortcuts: tuple[AlconnaShortcutDescriptor, ...] = ()
    compact: bool = False
    prefilter: bool = False
    reply_merge: bool = False


def _alconna_shortcut_signature() -> tuple | None:
    """Alconna 全局快捷指令签名，运行期新增或删除快捷指令时变化

    返回:
        tuple | None: 签名，无法读取时为 None
    """
    try:
        from arclet.alconna import command_manager

        store = getattr(command_manager, "_CommandManager__shortcuts", None)
        if not isinstance(store, dict):
            return None
        return tuple((name, tuple(value[1])) for name, value in store.items())
    except Exception:
        return None


@dataclass(slots=True)
class ActivationContext:
    event_type: str
    event: object | None = None
    plain_text: str = ""
    raw_text: str = ""
    to_me: bool = False
    adapter: Adapter | None = None
    texts: tuple[str, ...] | None = None

    def text_candidates(self) -> tuple[str, ...]:
        """事件文本候选，每个事件只计算一次

        返回:
            tuple[str, ...]: 各类 rule 可见的文本形式
        """
        if self.texts is None:
            self.texts = text_match_candidates(
                self.plain_text, self.raw_text, self.event
            )
        return self.texts


class HandlerActivationIndex:
    """In-memory matcher activation index.

    The index is intentionally fail-open: only known NoneBot rule misses are
    rejected before matcher task creation. Custom rules, incomplete command
    metadata, and Alconna shortcut misses stay selected so plugin compatibility
    wins over dispatch aggressiveness.
    """

    def __init__(self) -> None:
        self._matcher_map: dict[type[Matcher], HandlerDescriptor] = {}
        self._source_keys: set[tuple[int, tuple[int, ...]]] = set()
        self._shortcut_signature: tuple | None = None
        self._compiled = False

    @property
    def compiled(self) -> bool:
        return self._compiled

    def rebuild(self, matchers: Mapping[int, list[type[Matcher]]]) -> None:
        self._matcher_map.clear()
        source_keys: set[tuple[int, tuple[int, ...]]] = set()
        for priority, priority_matchers in matchers.items():
            for matcher in priority_matchers:
                self._matcher_map[matcher] = self._build_descriptor(matcher, priority)
            source_keys.add(
                (int(priority), tuple(id(matcher) for matcher in priority_matchers))
            )
        self._source_keys = source_keys
        self._shortcut_signature = _alconna_shortcut_signature()
        self._compiled = True

    def ensure_fresh(self, matchers: Mapping[int, list[type[Matcher]]]) -> None:
        source_keys = {
            (int(priority), tuple(id(matcher) for matcher in items))
            for priority, items in matchers.items()
        }
        if (
            not self._compiled
            or source_keys != self._source_keys
            or _alconna_shortcut_signature() != self._shortcut_signature
        ):
            self.rebuild(matchers)

    def descriptor_for(self, matcher: type[Matcher]) -> HandlerDescriptor | None:
        return self._matcher_map.get(matcher)

    def select_priority(
        self,
        priority: int,
        priority_matchers: list[type[Matcher]],
        context: ActivationContext,
    ) -> list[type[Matcher]]:
        """选出本优先级中可能命中当前事件的 matcher

        参数:
            priority: 优先级
            priority_matchers: 该优先级的 matcher
            context: 预筛上下文

        返回:
            list[type[Matcher]]: 选中的 matcher
        """
        selected = []
        for matcher in priority_matchers:
            descriptor = self._matcher_map.get(matcher) or self._build_descriptor(
                matcher, priority
            )
            if context.adapter is not None and not adapter_contract_matches(
                descriptor.supported_adapters, context.adapter
            ):
                logger.debug("plugin adapter unsupported: {}", descriptor.module)
                continue
            if self._select_descriptor(descriptor, context):
                selected.append(matcher)
        return selected

    def _select_descriptor(
        self,
        descriptor: HandlerDescriptor,
        context: ActivationContext,
    ) -> bool:
        if descriptor.temp:
            return True
        matcher_type = descriptor.matcher_type
        if matcher_type and matcher_type != context.event_type:
            return False
        if context.event_type != "message":
            return not descriptor.command_like
        texts = context.text_candidates()
        if descriptor.command_like and not texts:
            return True
        if (
            matcher_rule_matches_text(
                descriptor.rules,
                context.raw_text,
                context.plain_text,
                event=context.event,
                to_me=context.to_me,
                candidates=texts,
            )
            == "miss"
        ):
            return False
        if not descriptor.command_like:
            return True
        if descriptor.alconna:
            if self._shortcut_signature is None:
                return True
            return any(
                matcher_alconna_head_matches(alconna, texts, context) != "miss"
                for alconna in descriptor.alconna
            )
        if descriptor.commands:
            if any(
                matcher_command_matches(text, command)
                for text in texts
                for command in descriptor.commands
            ):
                return True
            if matcher_alconna_shortcut_matches_any(descriptor.shortcuts, texts) == (
                "match"
            ):
                return True
            return descriptor.has_custom_rule
        return True

    def _build_descriptor(
        self,
        matcher: type[Matcher],
        priority: int,
    ) -> HandlerDescriptor:
        rules = extract_matcher_rule_descriptors(matcher)
        command_like = any(rule.command_like for rule in rules)
        deterministic = any(rule.deterministic_text for rule in rules)
        if hasattr(matcher, "command"):
            command_like = True
        commands = extract_matcher_command_literals(matcher) or ()
        alconna_descriptors = extract_matcher_alconna_descriptors(matcher)
        shortcuts = extract_matcher_alconna_shortcuts(matcher)
        if alconna_descriptors:
            command_like = True
        if shortcuts is not None:
            command_like = True
        module = matcher_module_name(matcher)
        lane = classify_lane(
            matcher,
            module=module,
            command_like=command_like,
            deterministic_text=deterministic,
            shortcuts=shortcuts,
            commands=commands,
        )
        return HandlerDescriptor(
            matcher=matcher,
            module=module,
            matcher_type=getattr(matcher, "type", "") or "",
            priority=priority,
            lane=lane,
            temp=bool(getattr(matcher, "temp", False)),
            block=bool(getattr(matcher, "block", False)),
            command_like=command_like,
            deterministic_text=deterministic,
            has_custom_rule=matcher_has_custom_rule(matcher),
            commands=commands,
            shortcuts=shortcuts,
            alconna=alconna_descriptors,
            rules=rules,
            supported_adapters=matcher_supported_adapters(matcher),
        )


def matcher_supported_adapters(matcher) -> tuple[type[Adapter], ...] | None:
    metadata = getattr(getattr(matcher, "plugin", None), "metadata", None)
    if metadata is None or getattr(metadata, "supported_adapters", None) is None:
        return None
    return tuple(metadata.get_supported_adapters() or ())


def adapter_contract_matches(
    supported: tuple[type[Adapter], ...] | None, adapter: Adapter
) -> bool:
    return supported is None or isinstance(adapter, supported)


def matcher_module_name(matcher_cls: type[Matcher]) -> str:
    module = getattr(matcher_cls, "plugin_name", "") or ""
    if module:
        return module
    plugin = getattr(matcher_cls, "plugin", None)
    if not plugin:
        return ""
    return (getattr(plugin, "name", "") or "").strip()


def classify_lane(
    matcher_cls: type[Matcher],
    *,
    module: str,
    command_like: bool,
    deterministic_text: bool,
    shortcuts: tuple[str, ...] | None,
    commands: tuple[str, ...],
) -> ActivationLane:
    if getattr(matcher_cls, "temp", False):
        return "system"
    if command_like:
        if shortcuts:
            return "command_shortcut"
        has_regex_command = any(
            is_regex_like_command_literal(item) for item in commands
        )
        if deterministic_text or has_regex_command:
            return "command_regex"
        return "command_exact"
    module_l = (module or "").casefold()
    if any(hint in module_l for hint in PASSIVE_AI_HINTS):
        return "passive_ai"
    if any(hint in module_l for hint in PASSIVE_RENDER_HINTS):
        return "passive_render"
    if any(hint in module_l for hint in PASSIVE_HTTP_HINTS):
        return "passive_http"
    if matcher_has_custom_rule(matcher_cls):
        return "passive_light"
    if any(hint in module_l for hint in PASSIVE_DB_HINTS):
        return "passive_db"
    return "passive_light"


def matcher_has_custom_rule(matcher_cls: type[Matcher]) -> bool:
    rule = getattr(matcher_cls, "rule", None)
    checkers = getattr(rule, "checkers", ()) or ()
    for checker in checkers:
        call = getattr(checker, "call", None)
        if call is None:
            continue
        call_module = call.__class__.__module__
        if call_module.startswith("nonebot.rule") or call_module.startswith(
            "nonebot_plugin_alconna.rule"
        ):
            continue
        return True
    return False


def extract_matcher_rule_descriptors(
    matcher_cls: type[Matcher],
) -> tuple[ActivationRuleDescriptor, ...]:
    descriptors: list[ActivationRuleDescriptor] = []
    rule = getattr(matcher_cls, "rule", None)
    checkers = getattr(rule, "checkers", ()) or ()
    for checker in checkers:
        call = getattr(checker, "call", None)
        if call is None:
            continue
        call_module = call.__class__.__module__
        call_name = call.__class__.__name__
        if call_module.startswith("nonebot.rule"):
            if call_name == "CommandRule":
                descriptors.append(
                    ActivationRuleDescriptor(
                        "command",
                        getattr(call, "cmds", ()),
                        command_like=True,
                    )
                )
            elif call_name == "ShellCommandRule":
                descriptors.append(
                    ActivationRuleDescriptor(
                        "shell_command",
                        getattr(call, "cmds", ()),
                        command_like=True,
                    )
                )
            elif call_name == "RegexRule":
                descriptors.append(
                    ActivationRuleDescriptor(
                        "regex",
                        getattr(call, "regex", ""),
                        flags=int(getattr(call, "flags", 0) or 0),
                        deterministic_text=True,
                        command_like=True,
                    )
                )
            elif call_name == "StartswithRule":
                descriptors.append(
                    ActivationRuleDescriptor(
                        "startswith",
                        normalize_rule_string_tuple(getattr(call, "msg", ())),
                        ignorecase=bool(getattr(call, "ignorecase", False)),
                        deterministic_text=True,
                        command_like=True,
                    )
                )
            elif call_name == "EndswithRule":
                descriptors.append(
                    ActivationRuleDescriptor(
                        "endswith",
                        normalize_rule_string_tuple(getattr(call, "msg", ())),
                        ignorecase=bool(getattr(call, "ignorecase", False)),
                        deterministic_text=True,
                        command_like=True,
                    )
                )
            elif call_name == "FullmatchRule":
                descriptors.append(
                    ActivationRuleDescriptor(
                        "fullmatch",
                        normalize_rule_string_tuple(getattr(call, "msg", ())),
                        ignorecase=bool(getattr(call, "ignorecase", False)),
                        deterministic_text=True,
                        command_like=True,
                    )
                )
            elif call_name == "KeywordsRule":
                descriptors.append(
                    ActivationRuleDescriptor(
                        "keywords",
                        normalize_rule_string_tuple(getattr(call, "keywords", ())),
                        deterministic_text=True,
                        command_like=True,
                    )
                )
            elif call_name == "IsTypeRule":
                descriptors.append(
                    ActivationRuleDescriptor("is_type", getattr(call, "types", ()))
                )
            elif call_name == "ToMeRule":
                descriptors.append(ActivationRuleDescriptor("to_me"))
            else:
                descriptors.append(ActivationRuleDescriptor("custom"))
        elif (
            call_module.startswith("nonebot_plugin_alconna.rule")
            and call_name == "AlconnaRule"
        ):
            descriptors.append(ActivationRuleDescriptor("alconna", command_like=True))
        else:
            descriptors.append(ActivationRuleDescriptor("custom"))
    return tuple(descriptors)


def normalize_rule_string_tuple(value: object) -> tuple[str, ...]:
    if isinstance(value, str):
        return (value,)
    if isinstance(value, list | tuple | set | frozenset):
        return tuple(str(item) for item in value if str(item))
    return ()


def text_match_candidates(
    plain_text: str,
    raw_text: str = "",
    event: object | None = None,
) -> tuple[str, ...]:
    """Return text variants visible to different matcher rule providers."""

    candidates: list[str] = []

    def add(text: object) -> None:
        if not isinstance(text, str):
            return
        normalized = text.strip()
        if normalized and normalized not in candidates:
            candidates.append(normalized)

        unescaped = _unescape_message_text(normalized)
        if unescaped and unescaped not in candidates:
            candidates.append(unescaped)

    add(plain_text)
    if event is not None:
        with contextlib.suppress(Exception):
            getter = getattr(event, "get_plaintext", None)
            if callable(getter):
                add(getter())
        add(_leading_text_run(event))
    add(raw_text)
    return tuple(candidates)


def _leading_text_run(event: object) -> str:
    """消息开头连续文本段，Alconna 解析时非文本段同样是参数边界

    参数:
        event: 事件

    返回:
        str: 跳过开头非文本段后首段连续文本
    """
    parts: list[str] = []
    with contextlib.suppress(Exception):
        for segment in event.get_message():  # type: ignore[attr-defined]
            if segment.is_text():
                parts.append(str(segment))
            elif parts:
                break
    return "".join(parts)


def _unescape_message_text(text: str) -> str:
    if not text:
        return ""
    unescaped = html.unescape(text)
    return unescaped.replace("\\/", "/").replace("\\u002F", "/").replace("\\u002f", "/")


def matcher_rule_matches_text(
    descriptors: tuple[ActivationRuleDescriptor, ...],
    raw_text: str,
    plain_text: str,
    *,
    event: object | None = None,
    to_me: bool = False,
    candidates: tuple[str, ...] | None = None,
) -> ActivationDecision:
    matched_any = False
    saw_deterministic = False
    saw_unknown = False
    message_text = raw_text or plain_text
    plain_candidates = (
        candidates
        if candidates is not None
        else text_match_candidates(plain_text, raw_text, event)
    )

    for descriptor in descriptors:
        kind = descriptor.kind
        if kind in {"custom", "alconna"}:
            saw_unknown = True
            continue
        if kind in {"command", "shell_command"}:
            saw_deterministic = True
            commands: set[str] = set()
            collect_command_literals(descriptor.value, commands)
            normalized_commands = {
                normalized
                for item in commands
                if (normalized := normalize_command(item))
            }
            if any(
                matcher_command_matches(text, command)
                for text in plain_candidates
                for command in normalized_commands
            ):
                matched_any = True
            else:
                return "miss"
        elif kind in {"regex", "regex_fullmatch"}:
            saw_deterministic = True
            pattern = str(descriptor.value or "")
            if not pattern:
                continue
            try:
                matched = (
                    re.fullmatch(pattern, message_text, descriptor.flags)
                    if kind == "regex_fullmatch"
                    else re.search(pattern, message_text, descriptor.flags)
                )
                if matched:
                    matched_any = True
                else:
                    return "miss"
            except re.error:
                return "unknown"
        elif kind == "startswith":
            saw_deterministic = True
            values = descriptor.value if isinstance(descriptor.value, tuple) else ()
            candidates = (
                tuple(item.casefold() for item in values)
                if descriptor.ignorecase
                else values
            )
            texts = (
                tuple(item.casefold() for item in plain_candidates)
                if descriptor.ignorecase
                else plain_candidates
            )
            if any(
                text.startswith(item) for text in texts for item in candidates if item
            ):
                matched_any = True
            else:
                return "miss"
        elif kind == "endswith":
            saw_deterministic = True
            values = descriptor.value if isinstance(descriptor.value, tuple) else ()
            candidates = (
                tuple(item.casefold() for item in values)
                if descriptor.ignorecase
                else values
            )
            texts = (
                tuple(item.casefold() for item in plain_candidates)
                if descriptor.ignorecase
                else plain_candidates
            )
            if any(
                text.endswith(item) for text in texts for item in candidates if item
            ):
                matched_any = True
            else:
                return "miss"
        elif kind == "fullmatch":
            saw_deterministic = True
            values = descriptor.value if isinstance(descriptor.value, tuple) else ()
            candidates = (
                tuple(item.casefold() for item in values)
                if descriptor.ignorecase
                else values
            )
            texts = (
                tuple(item.casefold() for item in plain_candidates)
                if descriptor.ignorecase
                else plain_candidates
            )
            if any(text in candidates for text in texts):
                matched_any = True
            else:
                return "miss"
        elif kind == "keywords":
            saw_deterministic = True
            values = descriptor.value if isinstance(descriptor.value, tuple) else ()
            if any(
                item and item in text for text in plain_candidates for item in values
            ):
                matched_any = True
            else:
                return "miss"
        elif kind == "to_me":
            if not to_me:
                return "miss"
        elif kind == "is_type":
            if event is None:
                return "unknown"
            types = descriptor.value
            if isinstance(types, type):
                if not isinstance(event, types):
                    return "miss"
            elif isinstance(types, tuple) and types:
                if not isinstance(event, types):
                    return "miss"
    if matched_any:
        return "unknown" if saw_unknown else "match"
    if saw_unknown:
        return "unknown"
    if saw_deterministic:
        return "miss"
    return "unknown"


def extract_matcher_command_literals(
    matcher_cls: type[Matcher],
) -> tuple[str, ...] | None:
    commands: set[str] = set()
    collect_command_literals(getattr(matcher_cls, "command", None), commands)
    rule = getattr(matcher_cls, "rule", None)
    checkers = getattr(rule, "checkers", ()) or ()
    for checker in checkers:
        call = getattr(checker, "call", None)
        if call is None:
            continue
        for attr in ("cmds", "command", "commands", "cmd"):
            collect_command_literals(getattr(call, attr, None), commands)
    normalized_commands = {
        normalized for item in commands if (normalized := normalize_command(item))
    }
    normalized = tuple(sorted(normalized_commands))
    return normalized or None


def collect_command_literals(value: Any, target: set[str], depth: int = 0) -> None:
    if depth > 3 or value is None:
        return
    if isinstance(value, str):
        text = value.strip()
        if text:
            target.add(text)
        return
    if isinstance(value, weakref.ReferenceType):
        resolved = value()
        if resolved is not None and resolved is not value:
            collect_command_literals(resolved, target, depth + 1)
        return
    if isinstance(value, list | tuple | set | frozenset):
        if all(isinstance(item, str) for item in value):
            parts = tuple(str(item).strip() for item in value if str(item).strip())
            if parts:
                target.add(" ".join(parts))
                target.add("".join(parts))
            return
        for item in value:
            collect_command_literals(item, target, depth + 1)
        return
    if callable(value) and getattr(value, "__self__", None) is not None:
        with contextlib.suppress(TypeError, RuntimeError, ReferenceError):
            resolved = value()
            if resolved is not None and resolved is not value:
                collect_command_literals(resolved, target, depth + 1)
                return
    for attr in (
        "name",
        "path",
        "aliases",
        "header_display",
        "command",
        "commands",
        "cmd",
        "cmds",
    ):
        nested = getattr(value, attr, None)
        if nested is not None and nested is not value:
            collect_command_literals(nested, target, depth + 1)


def extract_matcher_alconna_shortcuts(
    matcher_cls: type[Matcher],
) -> tuple[str, ...] | None:
    shortcuts: set[str] = set()
    for attr in ("command", "_rule", "rule"):
        collect_alconna_shortcuts(getattr(matcher_cls, attr, None), shortcuts)
    rule = getattr(matcher_cls, "rule", None)
    checkers = getattr(rule, "checkers", ()) or ()
    for checker in checkers:
        call = getattr(checker, "call", None)
        if call is None:
            continue
        if call.__class__.__name__ != "AlconnaRule":
            continue
        command = resolve_maybe_weakref(
            getattr(call, "command", None) or getattr(call, "alconna", None)
        )
        collect_alconna_shortcuts(command, shortcuts)
    normalized_shortcuts = {
        normalized
        for item in shortcuts
        if item and (normalized := normalize_shortcut_pattern(item))
    }
    normalized = tuple(sorted(normalized_shortcuts))
    return normalized if normalized else None


def collect_alconna_shortcuts(value: Any, target: set[str], depth: int = 0) -> None:
    if depth > 4 or value is None:
        return
    if isinstance(value, weakref.ReferenceType):
        resolved = value()
        if resolved is not None and resolved is not value:
            collect_alconna_shortcuts(resolved, target, depth + 1)
        return
    get_shortcuts = getattr(value, "get_shortcuts", None)
    if callable(get_shortcuts):
        with contextlib.suppress(Exception):
            raw_shortcuts = get_shortcuts()
            if isinstance(raw_shortcuts, list | tuple | set | frozenset):
                for shortcut in raw_shortcuts:
                    if isinstance(shortcut, str) and shortcut.strip():
                        target.add(shortcut.strip())
    elif callable(value):
        with contextlib.suppress(Exception):
            resolved = value()
            if resolved is not None and resolved is not value:
                collect_alconna_shortcuts(resolved, target, depth + 1)
                return
    formatter = getattr(value, "formatter", None)
    data = getattr(formatter, "data", None)
    if isinstance(data, dict):
        for trace in data.values():
            trace_shortcuts = getattr(trace, "shortcuts", None)
            if not isinstance(trace_shortcuts, dict):
                continue
            for shortcut in trace_shortcuts:
                if isinstance(shortcut, str) and shortcut.strip():
                    target.add(shortcut.strip())
    for attr in ("shortcut", "shortcuts"):
        shortcuts = getattr(value, attr, None)
        if isinstance(shortcuts, dict):
            for key in shortcuts:
                if isinstance(key, str) and key.strip():
                    target.add(key.strip())
        elif isinstance(shortcuts, list | tuple | set | frozenset):
            for item in shortcuts:
                if isinstance(item, str) and item.strip():
                    target.add(item.strip())
    with contextlib.suppress(Exception):
        from arclet.alconna import command_manager

        for shortcut_map in command_manager.get_shortcut(value).values():  # type: ignore[arg-type]
            origin_key = getattr(shortcut_map, "origin_key", None)
            if isinstance(origin_key, str) and origin_key.strip():
                target.add(origin_key.strip())
    for attr in ("command", "commands", "base", "formatter", "source"):
        nested = getattr(value, attr, None)
        if nested is not None and nested is not value:
            collect_alconna_shortcuts(nested, target, depth + 1)


def extract_matcher_alconna_descriptors(
    matcher_cls: type[Matcher],
) -> tuple[AlconnaDescriptor, ...]:
    descriptors: list[AlconnaDescriptor] = []
    rule = getattr(matcher_cls, "rule", None)
    checkers = getattr(rule, "checkers", ()) or ()
    for checker in checkers:
        call = getattr(checker, "call", None)
        if call is None:
            continue
        if call.__class__.__name__ != "AlconnaRule":
            continue
        if not call.__class__.__module__.startswith("nonebot_plugin_alconna.rule"):
            continue
        command = resolve_maybe_weakref(
            getattr(call, "command", None) or getattr(call, "alconna", None)
        )
        descriptor = _build_alconna_descriptor(call, command)
        if descriptor is not None:
            descriptors.append(descriptor)
    return tuple(descriptors)


def _build_alconna_descriptor(call: Any, command: Any) -> AlconnaDescriptor | None:
    if command is None:
        return None
    raw_command = getattr(command, "command", "")
    command_text = raw_command.strip() if isinstance(raw_command, str) else ""
    raw_prefixes = tuple(getattr(command, "prefixes", ()) or ())
    separators = getattr(command, "separators", " ")
    meta = getattr(command, "meta", None)
    shortcuts = _extract_alconna_shortcut_descriptors(command)
    rewrite_extensions = _extract_alconna_input_rewrite_extensions(call)
    prefilter = (
        bool(command_text)
        and (command_text.startswith("re:") or "{" not in command_text)
        and all(isinstance(item, str) for item in raw_prefixes)
        and isinstance(separators, str)
        and bool(separators)
        and shortcuts is not None
        and not getattr(call, "use_origin", False)
        and not getattr(meta, "context_style", None)
        and all(name == "ReplyMergeExtension" for name in rewrite_extensions)
    )
    aliases = tuple(
        str(item).strip()
        for item in getattr(command, "aliases", ()) or ()
        if str(item).strip() and str(item).strip() != command_text
    )
    return AlconnaDescriptor(
        command=command_text,
        aliases=aliases,
        prefixes=tuple(item for item in raw_prefixes if isinstance(item, str)),
        separators=separators if isinstance(separators, str) and separators else " ",
        shortcuts=shortcuts or (),
        compact=bool(getattr(meta, "compact", False)),
        prefilter=prefilter,
        reply_merge="ReplyMergeExtension" in rewrite_extensions,
    )


def _extract_alconna_shortcut_descriptors(
    command: Any,
) -> tuple[AlconnaShortcutDescriptor, ...] | None:
    """按 Alconna 实际匹配用的键还原快捷指令

    参数:
        command: Alconna 命令

    返回:
        tuple[AlconnaShortcutDescriptor, ...] | None: 快捷指令，无法读取时为 None
    """
    try:
        from arclet.alconna import command_manager
        from arclet.alconna.typing import InnerShortcutArgs

        raw_shortcuts = command_manager.get_shortcut(command)
        shortcuts: list[AlconnaShortcutDescriptor] = []
        for key, args in raw_shortcuts.items():
            if isinstance(args, InnerShortcutArgs):
                keys = (
                    [f"{re.escape(item)}{args.origin_key}" for item in args.prefixes]
                    if args.prefixes
                    else [args.origin_key]
                )
                fuzzy = bool(args.fuzzy)
                exact_tail = not fuzzy
            else:
                keys = [key]
                fuzzy = exact_tail = False
            flags = int(getattr(args, "flags", 0) or 0)
            shortcuts.extend(
                AlconnaShortcutDescriptor(
                    pattern=re.compile(item, flags),
                    fuzzy=fuzzy,
                    exact_tail=exact_tail,
                )
                for item in keys
            )
        return tuple(shortcuts)
    except Exception:
        return None


def _extract_alconna_input_rewrite_extensions(call: Any) -> tuple[str, ...]:
    executor = getattr(call, "executor", None)
    if executor is None:
        return ()
    result: list[str] = []
    for attr in ("extensions", "_extensions", "exts", "context"):
        extensions = getattr(executor, attr, None)
        if not isinstance(extensions, list | tuple | set | frozenset):
            continue
        for extension in extensions:
            overrides = getattr(extension.__class__, "_overrides", None)
            if not isinstance(overrides, dict):
                overrides = getattr(extension, "_overrides", None)
            if not isinstance(overrides, dict):
                continue
            if not (
                bool(overrides.get("message_provider"))
                or bool(overrides.get("receive_wrapper"))
            ):
                continue
            name = extension.__class__.__name__
            if name not in result:
                result.append(name)
    return tuple(result)


def matcher_alconna_head_matches(
    alconna: AlconnaDescriptor,
    texts: Iterable[str],
    context: ActivationContext,
) -> ActivationDecision:
    if not alconna.prefilter:
        return "unknown"
    if alconna.reply_merge and _event_has_reply(context.event):
        return "unknown"

    candidates = tuple(text.strip() for text in texts if text and text.strip())
    if not candidates:
        return "unknown"

    saw_unknown = False
    command_heads = (alconna.command, *alconna.aliases)
    for text in candidates:
        for command in command_heads:
            if not command:
                continue
            decision = _alconna_command_head_matches(text, command, alconna)
            if decision == "match":
                return "match"
            if decision == "unknown":
                saw_unknown = True
        if not alconna.shortcuts:
            continue
        tokens = _split_alconna_tokens(text, alconna.separators)
        if len(tokens) > ALCONNA_SHORTCUT_TOKEN_LIMIT:
            saw_unknown = True
            continue
        for shortcut in alconna.shortcuts:
            if _alconna_shortcut_matches(tokens, shortcut, alconna.separators):
                return "match"
    return "unknown" if saw_unknown else "miss"


def _alconna_command_head_matches(
    text: str,
    command: str,
    alconna: AlconnaDescriptor,
) -> ActivationDecision:
    normalized = command.strip()
    if not normalized:
        return "unknown"
    if normalized.startswith("re:"):
        pattern = normalized.removeprefix("re:").strip()
        if not pattern:
            return "unknown"
        for prefix in alconna.prefixes or ("",):
            try:
                if re.match(rf"^{re.escape(prefix)}(?:{pattern})", text):
                    return "match"
            except re.error:
                return "unknown"
        return "miss"
    for prefix in alconna.prefixes or ("",):
        if _alconna_literal_head_matches(text, f"{prefix}{normalized}", alconna):
            return "match"
    return "miss"


def _alconna_literal_head_matches(
    text: str, head: str, alconna: AlconnaDescriptor
) -> bool:
    if not text or not head or not text.startswith(head):
        return False
    if len(text) == len(head):
        return True
    rest = text[len(head)]
    if rest.isspace() or rest in alconna.separators:
        return True
    return bool(alconna.compact or not head[-1].isascii())


def _split_alconna_tokens(text: str, separators: str) -> list[str]:
    if separators == " ":
        return [item for item in text.split(" ") if item]
    return [item for item in re.split(f"[{re.escape(separators)}]", text) if item]


def _alconna_shortcut_matches(
    tokens: list[str],
    shortcut: AlconnaShortcutDescriptor,
    separators: str,
) -> bool:
    """按 Alconna find_shortcut 的方式逐步拼接参数匹配快捷指令

    参数:
        tokens: 按分隔符切分的文本
        shortcut: 快捷指令
        separators: 命令分隔符

    返回:
        bool: 是否可能命中
    """
    query = ""
    last = len(tokens) - 1
    for index, token in enumerate(tokens):
        query = token if index == 0 else f"{query}{separators[0]}{token}"
        if shortcut.fuzzy:
            if shortcut.pattern.match(query):
                return True
        elif shortcut.pattern.fullmatch(query) and (
            not shortcut.exact_tail or index == last
        ):
            return True
    return False


def _event_has_reply(event: object | None) -> bool:
    if event is None:
        return False
    with contextlib.suppress(Exception):
        message = event.get_message()  # type: ignore[attr-defined]
        for segment in message:
            segment_type = getattr(segment, "type", None)
            if segment_type == "reply":
                return True
            if isinstance(segment, dict) and segment.get("type") == "reply":
                return True
    for attr in ("reply", "reply_message", "source"):
        with contextlib.suppress(Exception):
            if getattr(event, attr, None) is not None:
                return True
    raw_text = ""
    with contextlib.suppress(Exception):
        raw_text = str(event.get_message())  # type: ignore[attr-defined]
    lowered = raw_text.casefold()
    return any(
        marker in lowered
        for marker in ("[cq:reply", "type=reply", '"reply"', "'reply'")
    )


def resolve_maybe_weakref(value: Any) -> Any:
    if isinstance(value, weakref.ReferenceType):
        resolved = value()
        return resolved if resolved is not None else value
    if callable(value) and getattr(value, "__self__", None) is not None:
        with contextlib.suppress(TypeError, RuntimeError, ReferenceError):
            resolved = value()
            if resolved is not None and resolved is not value:
                return resolved
    return value


def normalize_command(command: str) -> str:
    text = command.strip()
    if not text:
        return ""
    text = re.sub(r"^(?:\s*(?:\[[^\]]*]|\<[^>]*>))+\s*", "", text)
    cut_points = [idx for idx in (text.find("["), text.find("<")) if idx >= 0]
    if cut_points:
        text = text[: min(cut_points)]
    text = re.sub(r"\s+", " ", text).strip()
    text = re.sub(r"(?:\s+[?*]+|[?*]+)$", "", text).strip()
    return text


def matcher_command_matches(text: str, command: str) -> bool:
    normalized = command.strip()
    if not normalized:
        return False
    if normalized.startswith("re:"):
        pattern = normalized.removeprefix("re:").strip()
        if not pattern:
            return False
        try:
            return re.search(pattern, text) is not None
        except re.error:
            return False
    if command_matches(text, normalized):
        return True
    return text.startswith(normalized) and not normalized[-1].isascii()


def command_matches(text: str, command: str) -> bool:
    if not text or not command:
        return False
    if text == command:
        return True
    if text.startswith(command):
        if len(text) == len(command):
            return True
        return text[len(command)].isspace()
    return False


def is_regex_like_command_literal(command: str) -> bool:
    text = command.strip()
    if not text:
        return False
    if text.startswith("re:"):
        return True
    return any(token in text for token in ("\\", "(", ")", "[", "]", "|", "^", "$"))


def normalize_shortcut_pattern(pattern: str) -> str:
    text = str(pattern or "").strip()
    if not text:
        return ""
    text = re.sub(r"^\[(?:[^\]]*)\]\s*", "", text)
    text = re.sub(r"\s*\.\.\.args?$", "", text).strip()
    text = re.sub(r"\s+", " ", text)
    text = re.sub(r"\s*\.\.\.$", "", text).strip()
    text = re.sub(r"^\^", "", text)
    return text


def matcher_alconna_shortcut_matches(
    shortcuts: tuple[str, ...] | None,
    text: str,
) -> ActivationDecision:
    if shortcuts is None:
        return "unknown"
    for shortcut in shortcuts:
        if shortcut_matches_text(text, shortcut):
            return "match"
    return "unknown"


def matcher_alconna_shortcut_matches_any(
    shortcuts: tuple[str, ...] | None,
    texts: Iterable[str],
) -> ActivationDecision:
    if shortcuts is None:
        return "unknown"
    for text in texts:
        if matcher_alconna_shortcut_matches(shortcuts, text) == "match":
            return "match"
    return "unknown"


def shortcut_matches_text(text: str, shortcut: str) -> bool:
    pattern = normalize_shortcut_pattern(shortcut)
    if not pattern:
        return False
    if placeholder_shortcut_matches(text, pattern):
        return True
    if is_regex_like_shortcut(pattern):
        try:
            return re.match(pattern, text) is not None
        except re.error:
            return False
    return matcher_command_matches(text, pattern)


def placeholder_shortcut_matches(text: str, pattern: str) -> bool:
    return _placeholder_shortcut_decision(text, pattern) == "match"


def _placeholder_shortcut_decision(text: str, pattern: str) -> ActivationDecision:
    if "{" not in pattern or "}" not in pattern:
        return "miss"
    pieces: list[str] = []
    last = 0
    for match in re.finditer(r"\{[^{}]+\}", pattern):
        pieces.append(re.escape(pattern[last : match.start()]))
        pieces.append(r"\S+")
        last = match.end()
    if not pieces:
        return "miss"
    pieces.append(re.escape(pattern[last:]))
    try:
        return (
            "match"
            if re.match(rf"^{''.join(pieces)}(?:\s|$)", text) is not None
            else "miss"
        )
    except re.error:
        return "unknown"


def is_regex_like_shortcut(pattern: str) -> bool:
    return any(token in pattern for token in ("\\", "(", ")", "[", "]", "|", "^", "$"))


PASSIVE_DB_HINTS = (
    "word_bank",
    "black_word",
    "history",
    "statistics",
    "sign",
    "gold",
    "redbag",
    "mute",
    "group",
    "user",
    "admin",
    "ban",
    "limit",
    "check",
)
PASSIVE_HTTP_HINTS = (
    "http",
    "translate",
    "bilibili",
    "music",
    "comment",
    "nbnhhsh",
    "quote",
    "search",
    "jitang",
    "poetry",
    "anime",
    "cover",
)
PASSIVE_AI_HINTS = (
    "chatinter",
    "dialogue",
    "ai",
    "llm",
    "fudu",
    "bym_ai",
)
PASSIVE_RENDER_HINTS = (
    "render",
    "image",
    "meme",
    "memes",
    "word_cloud",
    "wordcloud",
    "pic",
    "picture",
    "coser",
    "luxun",
)


__all__ = [
    "ActivationContext",
    "ActivationDecision",
    "ActivationRuleDescriptor",
    "HandlerActivationIndex",
    "HandlerDescriptor",
    "command_matches",
    "extract_matcher_alconna_shortcuts",
    "extract_matcher_command_literals",
    "extract_matcher_rule_descriptors",
    "matcher_command_matches",
    "matcher_has_custom_rule",
    "matcher_rule_matches_text",
]
