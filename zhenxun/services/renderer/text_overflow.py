from __future__ import annotations

from dataclasses import dataclass
from html import escape, unescape
from html.parser import HTMLParser
import re
from typing import Any

_COMMENT = re.compile(r"/\*.*?\*/", re.DOTALL)
_STYLE_BLOCK = re.compile(r"<style\b[^>]*>(.*?)</style\s*>", re.IGNORECASE | re.DOTALL)
_RULE = re.compile(r"([^{}]+)\{([^{}]*)\}")
_ELLIPSIS = re.compile(r"text-overflow\s*:\s*ellipsis", re.IGNORECASE)
_LINE_CLAMP = re.compile(r"-webkit-line-clamp\s*:\s*(\d+)", re.IGNORECASE)
_COMBINATOR = re.compile(r"\s*>\s*|\s+")
_COMPOUND = re.compile(r"^(\*|[a-zA-Z][\w-]*)?((?:[.#][\w-]+)*)$")
_VOID_TAGS = frozenset(
    "area base br col embed hr img input link meta param source track wbr".split()
)
_MARK = "…"
_PROBE = "display:inline-block;"
_MAX_TEXTS = 300
"""单页最多处理的溢出文字数"""

Compound = tuple[str | None, frozenset[str]]
"""标签名（None 为任意）与需要的 #id、.class"""
Selector = list[tuple[str, Compound]]
"""从左到右的复合选择器，每项附带与前一项之间的组合符"""
Rule = tuple[list[Selector], int]
"""选择器列表与省略方式：0 为单行省略，正数为多行省略的行数"""
StartTag = tuple[int, str, str | None]
"""开始标签的位置、原始文本与已有 id"""


def _overflow_mode(declarations: str) -> int | None:
    """解析声明中的省略方式

    参数:
        declarations: CSS 声明

    返回:
        int | None: 多行省略的行数，单行省略为 0，没有省略为 None
    """
    if clamp := _LINE_CLAMP.search(declarations):
        return max(int(clamp.group(1)), 1)
    return 0 if _ELLIPSIS.search(declarations) else None


def _parse_selector(text: str) -> Selector | None:
    """解析由标签、#id、.class 与后代、子代组合符构成的选择器

    参数:
        text: 选择器

    返回:
        Selector | None: 解析结果，含不支持的语法时为 None
    """
    text = text.strip()
    if not text or any(char in text for char in ":[+~()"):
        return None
    selector: Selector = []
    combinators = [" ", *_COMBINATOR.findall(text)]
    for combinator, part in zip(combinators, _COMBINATOR.split(text), strict=True):
        match = _COMPOUND.match(part)
        if not part or match is None:
            return None
        tag = match.group(1)
        tag = None if tag in (None, "*") else tag.lower()
        parts = frozenset(re.findall(r"[.#][\w-]+", part))
        selector.append((combinator.strip() or " ", (tag, parts)))
    return selector


def overflow_rules(html: str, css: str) -> list[Rule] | None:
    """收集声明了 text-overflow: ellipsis 或 -webkit-line-clamp 的样式规则

    参数:
        html: 页面 HTML
        css: 外链样式表内容

    返回:
        list[Rule] | None: 样式规则，页面完全没有用到省略时为 None
    """
    lowered = (html + css).lower()
    if "text-overflow" not in lowered and "line-clamp" not in lowered:
        return None
    text = _COMMENT.sub("", "".join(_STYLE_BLOCK.findall(html)) + css)
    rules: list[Rule] = []
    for selectors, declarations in _RULE.findall(text):
        mode = _overflow_mode(declarations)
        if mode is None or selectors.lstrip().startswith("@"):
            continue
        parsed = [_parse_selector(item) for item in selectors.split(",")]
        if selector_list := [item for item in parsed if item]:
            rules.append((selector_list, mode))
    return rules


def _matches(selector: Selector, chain: list[Compound], index: int) -> bool:
    """从右向左匹配选择器

    参数:
        selector: 选择器
        chain: 根到当前元素的标签链
        index: 选择器最后一项对应的元素下标

    返回:
        bool: 是否匹配
    """
    combinator, (tag, parts) = selector[-1]
    element_tag, element_parts = chain[index]
    if (tag is not None and tag != element_tag) or not parts <= element_parts:
        return False
    if len(selector) == 1:
        return True
    rest = selector[:-1]
    if combinator == ">":
        return index > 0 and _matches(rest, chain, index - 1)
    return any(_matches(rest, chain, parent) for parent in range(index - 1, -1, -1))


class _TextScan(HTMLParser):
    """找出需要省略、且内容只有文字的元素"""

    def __init__(self, rules: list[Rule]):
        super().__init__(convert_charrefs=False)
        self.rules = rules
        self.texts: list[tuple[StartTag, int, str]] = []
        self._html = ""
        self._stack: list[list[Any]] = []
        self._line_starts = [0]

    def scan(self, html: str) -> list[tuple[StartTag, int, str]]:
        """扫描页面

        参数:
            html: 页面 HTML

        返回:
            list[tuple[StartTag, int, str]]: 元素开始标签、省略方式与原始内容
        """
        self._html = html
        self._line_starts = [0, *(m.end() for m in re.finditer("\n", html))]
        self.feed(html)
        self.close()
        return self.texts

    def _offset(self) -> int:
        line, column = self.getpos()
        return self._line_starts[line - 1] + column

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if self._stack:
            self._stack[-1][3] = True
        if tag in _VOID_TAGS:
            return
        values = dict(attrs)
        parts = {f".{name}" for name in (values.get("class") or "").split()}
        if element_id := values.get("id"):
            parts.add(f"#{element_id}")
        compound: Compound = (tag, frozenset(parts))
        chain = [item[0] for item in self._stack] + [compound]
        mode = None
        for selectors, rule_mode in self.rules:
            if any(_matches(selector, chain, len(chain) - 1) for selector in selectors):
                mode = rule_mode
        if (style_mode := _overflow_mode(values.get("style") or "")) is not None:
            mode = style_mode
        start = (self._offset(), self.get_starttag_text() or "", element_id)
        self._stack.append([compound, tag, start, False, mode])

    def handle_startendtag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if self._stack:
            self._stack[-1][3] = True

    def handle_endtag(self, tag: str) -> None:
        for index in range(len(self._stack) - 1, -1, -1):
            if self._stack[index][1] != tag:
                continue
            _, _, start, has_child, mode = self._stack[index]
            del self._stack[index:]
            if mode is None or has_child or len(self.texts) >= _MAX_TEXTS:
                return
            raw = self._html[start[0] + len(start[1]) : self._offset()]
            if raw.strip():
                self.texts.append((start, mode, raw))
            return


def find_overflow_texts(
    html: str, rules: list[Rule]
) -> list[tuple[StartTag, int, str]]:
    """找出需要省略、且内容只有文字的元素

    参数:
        html: 页面 HTML
        rules: overflow_rules 收集的样式规则

    返回:
        list[tuple[StartTag, int, str]]: 元素开始标签、省略方式与原始内容
    """
    return _TextScan(rules).scan(html)


@dataclass
class _Overflow:
    """一段待截断文字的二分状态"""

    value: str
    mode: int
    limit: float
    kept: int = 0
    """已确认放得下的字数"""
    over: int = 0
    """已确认放不下的字数"""


def clamp_overflow_text(
    template: Any, texts: list[tuple[str, int, str]]
) -> dict[str, str]:
    """在已排版的页面上按实际排版截断溢出的文字，结束后页面恢复为截断后的内容

    参数:
        template: blitz Template
        texts: 元素 id、省略方式与原始内容

    返回:
        dict[str, str]: 需要截断的元素 id 与截断后的文字
    """
    values = {
        element_id: " ".join(unescape(raw).split()) for element_id, _, raw in texts
    }
    for element_id, _, _ in texts:
        template.set_html(
            element_id,
            f'<span id="{element_id}-s" style="{_PROBE}width:0"></span>'
            f'<span id="{element_id}-t" style="{_PROBE}">'
            f"{escape(values[element_id], quote=False)}</span><br>"
            f'<span id="{element_id}-w" style="{_PROBE}width:100%;height:0"></span>',
        )
    boxes = template.boxes()
    states: dict[str, _Overflow] = {}
    heights: dict[str, float] = {}
    indented: list[str] = []
    for element_id, mode, _ in texts:
        left, _, width, _ = boxes.get(f"{element_id}-w", (0, 0, 0, 0))
        indent = max(boxes.get(f"{element_id}-s", (left,))[0] - left, 0)
        width -= indent
        box = boxes.get(f"{element_id}-t")
        if width <= 0 or box is None or (not mode and box[2] <= width + 0.5):
            continue
        value = values[element_id]
        states[element_id] = _Overflow(value, mode, width + 0.5, over=len(value))
        heights[element_id] = box[3]
        if mode and indent > 0.5:
            indented.append(element_id)
            template.set_attribute(
                f"{element_id}-t", "style", f"{_PROBE}text-indent:{indent}px"
            )
    if indented:
        boxes = template.boxes()
        heights.update({i: boxes[f"{i}-t"][3] for i in indented})
    if lines := {i: s for i, s in states.items() if s.mode}:
        template.update(**{f"{i}-t": s.value[:1] for i, s in lines.items()})
        boxes = template.boxes()
        for element_id, state in lines.items():
            state.limit = state.mode * boxes[f"{element_id}-t"][3] + 0.5
            if heights[element_id] <= state.limit:
                del states[element_id]
    while pending := {i: s for i, s in states.items() if s.over - s.kept > 1}:
        middles = {i: (s.kept + s.over) // 2 for i, s in pending.items()}
        template.update(
            **{
                f"{i}-t": s.value[: middles[i]].rstrip() + _MARK
                for i, s in pending.items()
            }
        )
        boxes = template.boxes()
        for element_id, state in pending.items():
            _, _, width, height = boxes[f"{element_id}-t"]
            if (height if state.mode else width) <= state.limit:
                state.kept = middles[element_id]
            else:
                state.over = middles[element_id]
    result = {
        element_id: state.value[: state.kept].rstrip() + _MARK
        for element_id, state in states.items()
    }
    for element_id, _, raw in texts:
        text = result.get(element_id)
        template.set_html(element_id, raw if text is None else escape(text, False))
    return result


def replace_text(html: str, element_id: str, raw: str, text: str) -> str:
    """替换只含文字的元素的内容

    参数:
        html: 页面 HTML
        element_id: 元素 id
        raw: 元素的原始内容
        text: 新文字

    返回:
        str: 新的 HTML
    """
    pattern = re.compile(
        rf"""(<[a-zA-Z][\w-]*\b[^>]*?\bid\s*=\s*["']?{re.escape(element_id)}"""
        rf"""(?=["'\s/>])[^>]*>){re.escape(raw)}""",
        re.IGNORECASE,
    )
    return pattern.sub(
        lambda match: match.group(1) + escape(text, quote=False), html, count=1
    )
