from __future__ import annotations

from html import escape
import json
import math
from typing import Any

from markupsafe import Markup

PALETTE = (
    "#5470c6",
    "#91cc75",
    "#fac858",
    "#ee6666",
    "#73c0de",
    "#3ba272",
    "#fc8452",
    "#9a60b4",
    "#ea7ccc",
)
"""未指定颜色时的系列配色"""
AXIS_COLOR = "#6e7079"
SPLIT_COLOR = "#e0e6f1"
TITLE_COLOR = "#464646"


def _merge(target: Any, source: Any) -> Any:
    """深度合并 option，列表按下标合并

    参数:
        target: 原 option
        source: 覆盖的 option

    返回:
        Any: 合并结果
    """
    if isinstance(target, dict) and isinstance(source, dict):
        merged = dict(target)
        for key, value in source.items():
            merged[key] = _merge(target[key], value) if key in target else value
        return merged
    if isinstance(target, list) and isinstance(source, list):
        return [
            _merge(item, source[i]) if i < len(source) else item
            for i, item in enumerate(target)
        ]
    return source


def _one(value: Any) -> dict:
    if isinstance(value, list):
        value = value[0] if value else {}
    return value if isinstance(value, dict) else {}


def _many(value: Any) -> list[dict]:
    if isinstance(value, dict):
        return [value]
    return [item for item in value or [] if isinstance(item, dict)]


def _num(value: Any) -> float:
    if isinstance(value, dict):
        value = value.get("value")
    if isinstance(value, list):
        value = value[-1] if value else 0
    try:
        number = float(value)
    except (TypeError, ValueError):
        return 0.0
    return number if math.isfinite(number) else 0.0


def _fmt(value: float) -> str:
    if float(value).is_integer():
        return str(int(value))
    return f"{value:.2f}".rstrip("0").rstrip(".")


def _size(value: Any, total: float, default: float) -> float:
    if isinstance(value, str) and value.endswith("%"):
        try:
            return total * float(value[:-1]) / 100
        except ValueError:
            return default
    if isinstance(value, int | float):
        return float(value)
    return default


def _text_width(text: str, size: float) -> float:
    return sum(size if ord(ch) > 255 else size * 0.6 for ch in text)


def _truncate(text: str, size: float, width: float | None) -> str:
    if not width or _text_width(text, size) <= width:
        return text
    while text and _text_width(f"{text}...", size) > width:
        text = text[:-1]
    return f"{text}..."


def _nice_ticks(low: float, high: float, count: int = 5) -> list[float]:
    """计算坐标轴刻度

    参数:
        low: 最小值
        high: 最大值
        count: 期望的分段数

    返回:
        list[float]: 刻度值
    """
    if high <= low:
        high = low + 1
    raw = (high - low) / count
    magnitude = 10 ** math.floor(math.log10(raw))
    step = next(
        (m * magnitude for m in (1, 2, 2.5, 5, 10) if m * magnitude >= raw),
        10 * magnitude,
    )
    start = math.floor(low / step) * step
    end = math.ceil(high / step) * step
    return [start + step * i for i in range(round((end - start) / step) + 1)]


class _Svg:
    """收集图形与文字，图形绘制为 SVG，文字使用 HTML 以沿用页面字体"""

    def __init__(self, width: float, height: float):
        self.width = width
        self.height = height
        self.defs: list[str] = []
        self.items: list[str] = []
        self.texts: list[str] = []

    def paint(self, color: Any, fallback: str) -> str:
        """把颜色或 ECharts 线性渐变转为 SVG 填充值"""
        if isinstance(color, str) and color:
            return escape(color)
        if isinstance(color, dict) and color.get("type") == "linear":
            gradient_id = f"g{len(self.defs)}"
            stops = "".join(
                f'<stop offset="{_num(stop.get("offset"))}" '
                f'stop-color="{escape(str(stop.get("color", fallback)))}"/>'
                for stop in color.get("colorStops", [])
            )
            self.defs.append(
                f'<linearGradient id="{gradient_id}" x1="{_num(color.get("x"))}" '
                f'y1="{_num(color.get("y"))}" x2="{_num(color.get("x2"))}" '
                f'y2="{_num(color.get("y2", 1))}">{stops}</linearGradient>'
            )
            return f"url(#{gradient_id})"
        return fallback

    def text(
        self,
        x: float,
        y: float,
        text: Any,
        style: dict | None = None,
        anchor: str = "middle",
        rotate: float = 0,
    ):
        """添加文字，x 按 anchor 对齐，y 为文字中线"""
        style = style or {}
        size = _num(style.get("fontSize")) or 12
        box = min(_text_width(str(text), size) * 1.3 + 8, self.width)
        left = {"start": x, "end": x - box}.get(anchor, x - box / 2)
        left = min(max(left, 0), self.width - box)
        align = {"start": "left", "end": "right"}.get(anchor, "center")
        css = (
            f"position:absolute;left:{left:.1f}px;top:{y - size * 0.7:.1f}px;"
            f"width:{box}px;height:{size * 1.4:.1f}px;line-height:{size * 1.4:.1f}px;"
            f"font-size:{size:g}px;text-align:{align};white-space:nowrap;"
            f"color:{style.get('color') or AXIS_COLOR};"
        )
        if weight := style.get("fontWeight"):
            css += f"font-weight:{weight};"
        if family := style.get("fontFamily"):
            css += f"font-family:{family};"
        if rotate:
            origin = {"start": "0%", "end": "100%"}.get(anchor, "50%")
            css += f"transform:rotate({-rotate:g}deg);transform-origin:{origin} 50%;"
        self.texts.append(f'<span style="{escape(css)}">{escape(str(text))}</span>')

    def build(self) -> Markup:
        defs = f"<defs>{''.join(self.defs)}</defs>" if self.defs else ""
        return Markup(
            f'<div class="zx-chart" style="position:relative;overflow:hidden;'
            f'width:{self.width:g}px;height:{self.height:g}px">'
            f'<svg xmlns="http://www.w3.org/2000/svg" width="{self.width:g}" '
            f'height="{self.height:g}" viewBox="0 0 {self.width:g} {self.height:g}" '
            f'style="position:absolute;left:0;top:0">'
            f"{defs}{''.join(self.items)}</svg>{''.join(self.texts)}</div>"
        )


def _round_rect(x: float, y: float, w: float, h: float, radius: Any) -> str:
    """生成圆角矩形路径，radius 顺序为左上、右上、右下、左下"""
    if isinstance(radius, int | float):
        radius = [radius] * 4
    if not isinstance(radius, list) or len(radius) != 4:
        radius = [0, 0, 0, 0]
    limit = min(abs(w), abs(h)) / 2
    tl, tr, br, bl = (min(_num(r), limit) for r in radius)
    return (
        f'<path d="M{x + tl:.1f},{y:.1f}H{x + w - tr:.1f}'
        f"Q{x + w:.1f},{y:.1f} {x + w:.1f},{y + tr:.1f}V{y + h - br:.1f}"
        f"Q{x + w:.1f},{y + h:.1f} {x + w - br:.1f},{y + h:.1f}H{x + bl:.1f}"
        f"Q{x:.1f},{y + h:.1f} {x:.1f},{y + h - bl:.1f}V{y + tl:.1f}"
        f'Q{x:.1f},{y:.1f} {x + tl:.1f},{y:.1f}Z"'
    )


def _label(series: dict) -> dict:
    label = dict(series.get("label") or {})
    label.update(label.pop("normal", None) or {})
    label.update(label.pop("textStyle", None) or {})
    return label


def _label_text(label: dict, name: Any, value: float) -> str:
    formatter = label.get("formatter")
    if isinstance(formatter, str):
        return formatter.replace("{c}", _fmt(value)).replace("{b}", str(name))
    return _fmt(value)


def _title(svg: _Svg, option: dict) -> float:
    """绘制标题

    参数:
        svg: SVG
        option: 图表配置

    返回:
        float: 标题占用的高度
    """
    title = _one(option.get("title"))
    if not title.get("text"):
        return 0
    style = {"fontSize": 18, "fontWeight": "bold", "color": TITLE_COLOR}
    style.update(title.get("textStyle") or {})
    size = _num(style["fontSize"]) or 18
    top = _size(title.get("top"), svg.height, 8)
    align = title.get("left", "center")
    x, anchor = {
        "left": (10, "start"),
        "right": (svg.width - 10, "end"),
    }.get(align, (svg.width / 2, "middle"))
    svg.text(x, top + size / 2, title["text"], style, anchor)
    return top + size + 12


def _legend(svg: _Svg, names: list[str], colors: list[str], top: float) -> float:
    """在顶部横向绘制图例

    返回:
        float: 图例底部位置
    """
    widths = [_text_width(name, 12) + 34 for name in names]
    x = (svg.width - sum(widths)) / 2
    for name, color, width in zip(names, colors, widths):
        svg.items.append(
            f'<rect x="{x:.1f}" y="{top + 3:.1f}" width="25" height="14" rx="3" '
            f'fill="{color}"/>'
        )
        svg.text(x + 30, top + 10, name, {"color": "#333"}, "start")
        x += width
    return top + 28


def _cartesian(svg: _Svg, option: dict, series: list[dict], top: float):
    """绘制折线图与柱状图"""
    x_axis, y_axis = _one(option.get("xAxis")), _one(option.get("yAxis"))
    horizontal = y_axis.get("type") == "category"
    cat_axis, val_axis = (y_axis, x_axis) if horizontal else (x_axis, y_axis)
    count = max([len(s.get("data") or []) for s in series] + [0])
    categories = list(cat_axis.get("data") or range(1, count + 1))
    count = max(count, len(categories), 1)
    colors = [str(c) for c in option.get("color") or PALETTE]
    names = [str(s.get("name") or "") for s in series]
    if len(series) > 1 and all(names):
        top = _legend(svg, names, colors, top)

    values = [_num(v) for s in series for v in s.get("data") or []] or [0]
    ticks = _nice_ticks(min(0, *values), max(0, *values))
    low, high = ticks[0], ticks[-1]

    def label_style(axis: dict) -> dict:
        style = {"color": AXIS_COLOR, "fontSize": 12}
        style.update(axis.get("axisLabel") or {})
        return style

    cat_style, val_style = label_style(cat_axis), label_style(val_axis)
    cat_show = cat_axis.get("show", True) and cat_style.get("show", True)
    val_show = val_axis.get("show", True) and val_style.get("show", True)
    cat_size = _num(cat_style["fontSize"]) or 12
    val_size = _num(val_style["fontSize"]) or 12
    cat_limit = _num(cat_style.get("width")) or None
    cat_texts = [_truncate(str(c), cat_size, cat_limit) for c in categories]

    grid = _one(option.get("grid"))
    contain = grid.get("containLabel", False)
    left = _size(grid.get("left"), svg.width, svg.width * 0.1)
    right = _size(grid.get("right"), svg.width, svg.width * 0.1)
    top = max(top, _size(grid.get("top"), svg.height, 60))
    bottom = _size(grid.get("bottom"), svg.height, 60)
    if contain:
        side_texts = cat_texts if horizontal else [_fmt(t) for t in ticks]
        side_size = cat_size if horizontal else val_size
        if (cat_show if horizontal else val_show) and side_texts:
            left += max(_text_width(t, side_size) for t in side_texts) + 8
        if horizontal and val_show:
            bottom += val_size + 8
        elif not horizontal and cat_show and cat_texts:
            angle = math.radians(_num(cat_style.get("rotate")))
            longest = max(_text_width(t, cat_size) for t in cat_texts)
            bottom += cat_size * math.cos(angle) + longest * math.sin(angle) + 8
    x0, x1 = left, svg.width - right
    y0, y1 = top, svg.height - bottom
    band = ((y1 - y0) if horizontal else (x1 - x0)) / count

    def val_pos(value: float) -> float:
        ratio = (value - low) / (high - low)
        return x0 + ratio * (x1 - x0) if horizontal else y1 - ratio * (y1 - y0)

    def cat_pos(index: int) -> float:
        if horizontal:
            return y1 - band * (index + 0.5)
        return x0 + band * (index + 0.5)

    if val_show:
        for tick in ticks:
            p = val_pos(tick)
            line = (
                f'x1="{p:.1f}" y1="{y0:.1f}" x2="{p:.1f}" y2="{y1:.1f}"'
                if horizontal
                else f'x1="{x0:.1f}" y1="{p:.1f}" x2="{x1:.1f}" y2="{p:.1f}"'
            )
            svg.items.append(f'<line {line} stroke="{SPLIT_COLOR}" stroke-width="1"/>')
            if horizontal:
                svg.text(p, y1 + val_size / 2 + 8, _fmt(tick), val_style)
            else:
                svg.text(x0 - 8, p, _fmt(tick), val_style, "end")
    if cat_show:
        base = val_pos(max(low, 0))
        line = (
            f'x1="{base:.1f}" y1="{y0:.1f}" x2="{base:.1f}" y2="{y1:.1f}"'
            if horizontal
            else f'x1="{x0:.1f}" y1="{base:.1f}" x2="{x1:.1f}" y2="{base:.1f}"'
        )
        line_style = (cat_axis.get("axisLine") or {}).get("show", True)
        if line_style:
            svg.items.append(f'<line {line} stroke="{AXIS_COLOR}" stroke-width="1"/>')
        rotate = _num(cat_style.get("rotate"))
        for index, text in enumerate(cat_texts):
            p = cat_pos(index)
            if horizontal:
                svg.text(x0 - 8, p, text, cat_style, "end")
            else:
                anchor = "end" if rotate else "middle"
                svg.text(p, y1 + cat_size / 2 + 8, text, cat_style, anchor, rotate)

    bars = [s for s in series if s.get("type") == "bar"]
    bar_width = band * 0.6 / max(len(bars), 1)
    zero = val_pos(max(low, 0))
    for index, item in enumerate(series):
        color = colors[index % len(colors)]
        item_style = item.get("itemStyle") or {}
        label = _label(item)
        data = [_num(v) for v in item.get("data") or []]
        if item.get("type") == "bar":
            slot = bars.index(item)
            width = min(bar_width, _num(item.get("barMaxWidth")) or bar_width)
            fill = svg.paint(item_style.get("color"), color)
            radius = item_style.get("borderRadius", 0)
            for i, value in enumerate(data):
                center = cat_pos(i) + (slot - (len(bars) - 1) / 2) * bar_width
                end = val_pos(value)
                if horizontal:
                    rect = _round_rect(
                        min(zero, end),
                        center - width / 2,
                        abs(end - zero),
                        width,
                        radius,
                    )
                else:
                    rect = _round_rect(
                        center - width / 2,
                        min(zero, end),
                        width,
                        abs(end - zero),
                        radius,
                    )
                svg.items.append(f'{rect} fill="{fill}"/>')
                if label.get("show"):
                    text = _label_text(
                        label, categories[i] if i < len(categories) else "", value
                    )
                    padding = label.get("padding") or [0, 0, 0, 0]
                    gap = 5 + (
                        _num(padding[3])
                        if isinstance(padding, list) and len(padding) == 4
                        else 0
                    )
                    style = {"color": color, "fontSize": 12, **label}
                    if horizontal:
                        svg.text(end + gap, center, text, style, "start")
                    else:
                        svg.text(
                            center, end - gap - _num(style["fontSize"]) / 2, text, style
                        )
            continue
        line_style = item.get("lineStyle") or {}
        stroke = svg.paint(line_style.get("color"), color)
        points = [(cat_pos(i), val_pos(v)) for i, v in enumerate(data)]
        if horizontal:
            points = [(y, x) for x, y in points]
        if not points:
            continue
        path = f"M{points[0][0]:.1f},{points[0][1]:.1f}"
        for i in range(1, len(points)):
            if item.get("smooth"):
                p0 = points[max(i - 2, 0)]
                p1, p2 = points[i - 1], points[i]
                p3 = points[min(i + 1, len(points) - 1)]
                c1 = (p1[0] + (p2[0] - p0[0]) / 6, p1[1] + (p2[1] - p0[1]) / 6)
                c2 = (p2[0] - (p3[0] - p1[0]) / 6, p2[1] - (p3[1] - p1[1]) / 6)
                path += (
                    f"C{c1[0]:.1f},{c1[1]:.1f} {c2[0]:.1f},{c2[1]:.1f} "
                    f"{p2[0]:.1f},{p2[1]:.1f}"
                )
            else:
                path += f"L{points[i][0]:.1f},{points[i][1]:.1f}"
        if area := item.get("areaStyle"):
            fill = svg.paint(area.get("color"), stroke)
            opacity = _num(area.get("opacity", 0.3))
            svg.items.append(
                f'<path d="{path}L{points[-1][0]:.1f},{zero:.1f}'
                f'L{points[0][0]:.1f},{zero:.1f}Z" fill="{fill}" '
                f'fill-opacity="{opacity:g}"/>'
            )
        svg.items.append(
            f'<path d="{path}" fill="none" stroke="{stroke}" '
            f'stroke-width="{_num(line_style.get("width", 2)):g}"/>'
        )
        dot = svg.paint(item_style.get("color"), stroke)
        for (px, py), value in zip(points, data):
            svg.items.append(
                f'<circle cx="{px:.1f}" cy="{py:.1f}" r="3" fill="#fff" '
                f'stroke="{dot}" stroke-width="1.5"/>'
            )
            if label.get("show"):
                style = {"color": AXIS_COLOR, "fontSize": 12, **label}
                size = _num(style["fontSize"]) or 12
                svg.text(px, py - size / 2 - 6, _fmt(value), style)


def _polar(cx: float, cy: float, radius: float, angle: float) -> tuple[float, float]:
    return cx + radius * math.sin(angle), cy - radius * math.cos(angle)


def _pie(svg: _Svg, option: dict, item: dict, top: float):
    """绘制饼图"""
    data = [d if isinstance(d, dict) else {"value": d} for d in item.get("data") or []]
    names = [str(d.get("name", "")) for d in data]
    colors = [
        str((d.get("itemStyle") or {}).get("color") or "")
        or str(
            (option.get("color") or PALETTE)[i % len(option.get("color") or PALETTE)]
        )
        for i, d in enumerate(data)
    ]
    if names and all(names):
        top = _legend(svg, names, colors, top)
    center = item.get("center") or ["50%", "50%"]
    cx = _size(center[0], svg.width, svg.width / 2)
    cy = top + _size(center[1], svg.height - top, (svg.height - top) / 2)
    space = min(svg.width, svg.height - top) / 2
    radius = item.get("radius", "75%")
    inner, outer = radius if isinstance(radius, list) else (0, radius)
    r_out = _size(outer, space, space * 0.75)
    r_in = _size(inner, space, 0)
    total = sum(max(_num(d), 0) for d in data) or 1
    angle = 0.0
    for d, name, color in zip(data, names, colors):
        sweep = max(_num(d), 0) / total * math.tau
        if sweep <= 0:
            continue
        end = angle + min(sweep, math.tau - 1e-4)
        large = 1 if sweep > math.pi else 0
        ox1, oy1 = _polar(cx, cy, r_out, angle)
        ox2, oy2 = _polar(cx, cy, r_out, end)
        path = (
            f"M{ox1:.1f},{oy1:.1f}"
            f"A{r_out:.1f},{r_out:.1f} 0 {large} 1 {ox2:.1f},{oy2:.1f}"
        )
        if r_in:
            ix1, iy1 = _polar(cx, cy, r_in, end)
            ix2, iy2 = _polar(cx, cy, r_in, angle)
            path += (
                f"L{ix1:.1f},{iy1:.1f}"
                f"A{r_in:.1f},{r_in:.1f} 0 {large} 0 {ix2:.1f},{iy2:.1f}Z"
            )
        else:
            path += f"L{cx:.1f},{cy:.1f}Z"
        svg.items.append(
            f'<path d="{path}" fill="{color}" stroke="#fff" stroke-width="1"/>'
        )
        middle = angle + sweep / 2
        sx, sy = _polar(cx, cy, r_out, middle)
        ex, ey = _polar(cx, cy, r_out + 16, middle)
        right = math.sin(middle) >= 0
        tx = ex + (16 if right else -16)
        svg.items.append(
            f'<path d="M{sx:.1f},{sy:.1f}L{ex:.1f},{ey:.1f}L{tx:.1f},{ey:.1f}" '
            f'fill="none" stroke="{color}" stroke-width="1"/>'
        )
        svg.text(
            tx + (4 if right else -4),
            ey,
            name,
            {"color": "#333", "fontSize": 12},
            "start" if right else "end",
        )
        angle += sweep


def _radar(svg: _Svg, option: dict, series: list[dict], top: float):
    """绘制雷达图"""
    radar = _one(option.get("radar"))
    indicators = [i for i in radar.get("indicator") or [] if isinstance(i, dict)]
    if len(indicators) < 3:
        return
    colors = [str(c) for c in option.get("color") or PALETTE]
    entries = [
        (entry if isinstance(entry, dict) else {"value": entry}, item)
        for item in series
        for entry in item.get("data") or []
    ]
    names = [str(entry.get("name") or "") for entry, _ in entries]
    if len(entries) > 1 and all(names):
        top = _legend(svg, names, colors, top)
    maxima = []
    for index, indicator in enumerate(indicators):
        found = [
            _num((entry.get("value") or [])[index])
            for entry, _ in entries
            if index < len(entry.get("value") or [])
        ]
        maxima.append(_num(indicator.get("max")) or max([*found, 1]))
    cx, cy = svg.width / 2, top + (svg.height - top) / 2
    radius = min(svg.width, svg.height - top) / 2 * 0.65
    step = math.tau / len(indicators)
    name_style = {"color": AXIS_COLOR, "fontSize": 12}
    name_style.update((radar.get("name") or {}).get("textStyle") or {})
    name_style.update(radar.get("axisName") or {})
    for ring in range(5, 0, -1):
        points = " ".join(
            "{:.1f},{:.1f}".format(*_polar(cx, cy, radius * ring / 5, step * i))
            for i in range(len(indicators))
        )
        svg.items.append(
            f'<polygon points="{points}" fill="none" stroke="#ccc" stroke-width="1"/>'
        )
    for i, indicator in enumerate(indicators):
        ex, ey = _polar(cx, cy, radius, step * i)
        svg.items.append(
            f'<line x1="{cx:.1f}" y1="{cy:.1f}" x2="{ex:.1f}" y2="{ey:.1f}" '
            f'stroke="#ccc" stroke-width="1"/>'
        )
        lx, ly = _polar(cx, cy, radius + 16, step * i)
        sin = math.sin(step * i)
        anchor = "middle" if abs(sin) < 0.1 else ("start" if sin > 0 else "end")
        svg.text(lx, ly, indicator.get("name", ""), name_style, anchor)
    for index, (entry, item) in enumerate(entries):
        color = colors[index % len(colors)]
        line_style = {**(item.get("lineStyle") or {}), **(entry.get("lineStyle") or {})}
        stroke = svg.paint(line_style.get("color"), color)
        values = list(entry.get("value") or [])
        points = " ".join(
            "{:.1f},{:.1f}".format(
                *_polar(
                    cx,
                    cy,
                    radius * min(_num(values[i]) / maxima[i], 1)
                    if i < len(values)
                    else 0,
                    step * i,
                )
            )
            for i in range(len(indicators))
        )
        area = {**(item.get("areaStyle") or {}), **(entry.get("areaStyle") or {})}
        fill = svg.paint(area.get("color"), stroke) if area else "none"
        opacity = f' fill-opacity="{_num(area.get("opacity", 0.3)):g}"' if area else ""
        svg.items.append(
            f'<polygon points="{points}" fill="{fill}"{opacity} stroke="{stroke}" '
            f'stroke-width="{_num(line_style.get("width", 2)):g}"/>'
        )


def chart_svg(
    option: dict,
    style: dict | str | None = None,
    width: float = 800,
    height: float | None = None,
) -> Markup:
    """把 ECharts 风格的 option 绘制为图表，支持折线、柱状、饼图与雷达图

    参数:
        option: 图表配置
        style: 主题样式，会深度合并到 option 上
        width: 图片宽度
        height: 图片高度，为空时按图表类型计算

    返回:
        Markup: 图表 HTML
    """
    if isinstance(style, str):
        style = json.loads(style) if style.strip() else None
    if style:
        option = _merge(option, style)
    series = _many(option.get("series"))
    kind = series[0].get("type", "line") if series else "line"
    if height is None:
        y_axis = _one(option.get("yAxis"))
        if kind == "bar" and y_axis.get("type") == "category":
            height = len(y_axis.get("data") or []) * 40 + 120
        else:
            height = width * 0.6
    svg = _Svg(width, height)
    top = _title(svg, option)
    if kind == "pie":
        _pie(svg, option, series[0], top)
    elif kind == "radar":
        _radar(svg, option, series, top)
    else:
        _cartesian(svg, option, series, top)
    return svg.build()
