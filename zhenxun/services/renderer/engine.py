import asyncio
from collections.abc import AsyncIterator
import contextlib
from html.parser import HTMLParser
from importlib import import_module
import io
import math
from pathlib import Path
import re
from typing import Any
from urllib.parse import urljoin, urlparse
from urllib.request import url2pathname

from PIL import Image, ImageChops

from zhenxun.configs.path_config import FONT_PATH
from zhenxun.services.lifecycle.deadline import remaining_timeout
from zhenxun.services.log import logger
from zhenxun.utils.exception import RenderingError

from .resources import (
    EMOJI_PATTERN,
    EMOJI_UNICODE_RANGE,
    emoji_font,
    fetch_remote_resources,
    normalize_svg,
)
from .types import BaseScreenshotEngine

_DEFAULT_FONT = "Microsoft YaHei"
_DEFAULT_FONT_FILES = ("msyh.ttf", "msyhbd.ttf")
"""默认字体的常规与粗体字形，粗体缺失时粗体文字按常规字形显示"""
_MAX_SIDE = 16000
"""单张图片的最大物理边长，blitz 上限为 16384"""
_MAX_PIXELS = 60_000_000
"""单张图片的最大物理像素数，blitz 上限为 64M"""
_MAX_DEPTH = 512
_JPEG_MAX_SIDE = 65000
_ROOT_ID = "zx-render-root"
_CLIP_ID = "zx-render-clip"
_NO_ANIMATION_CSS = (
    "*,*::before,*::after{animation:none!important;transition:none!important;"
    "caret-color:transparent!important}"
)
_CSS_URL = re.compile(r"url\(\s*['\"]?(https?://[^'\")\s]+)", re.IGNORECASE)
_FONT_FACE = re.compile(r"@font-face\s*\{[^}]*\}", re.IGNORECASE)
_FONT_FAMILY = re.compile(r"font-family\s*:\s*([^;}]+)", re.IGNORECASE)
_VH_UNIT = re.compile(r"[\d.]vh\b", re.IGNORECASE)
_MAX_BLUR = 100
"""模糊图层的最大物理半径"""
_PAINT_RESET = (
    "{background:transparent!important;border-color:transparent!important;"
    "box-shadow:none!important;outline-color:transparent!important}"
)
_TEXT_RESET = "{color:transparent!important;text-shadow:none!important}"
_SELECTOR = re.compile(r"^([a-zA-Z][\w-]*)?(?:#([\w-]+))?((?:\.[\w-]+)*)$")
_VOID_TAGS = frozenset(
    "area base br col embed hr img input link meta param source track wbr".split()
)
_fonts_registered = False


class _HtmlScan(HTMLParser):
    """扫描 HTML 的嵌套深度、远程资源，以及根元素与裁剪元素的开始标签"""

    def __init__(self, selector: str | None):
        super().__init__(convert_charrefs=True)
        self.max_depth = 0
        self.urls: dict[str, None] = {}
        self.root: tuple[int, str, str | None] | None = None
        self.clip: tuple[int, str, str | None] | None = None
        self.blurs: set[int] = set()
        self.hosts: dict[int, str] = {}
        self.stylesheets: list[str] = []
        self._stack: list[tuple[str, int, str]] = []
        self._in_style = False
        self._line_starts = [0]
        self._selector = _SELECTOR.match(selector.strip()) if selector else None

    def scan(self, html: str) -> None:
        self._line_starts.extend(m.end() for m in re.finditer("\n", html))
        self.feed(html)
        self.close()

    def _offset(self) -> int:
        line, column = self.getpos()
        return self._line_starts[line - 1] + column

    def _add_css_urls(self, text: str) -> None:
        for match in _CSS_URL.finditer(text):
            self.urls[match.group(1)] = None

    def _matches(self, tag: str, attrs: dict[str, str | None]) -> bool:
        if self._selector is None:
            return False
        name, element_id, classes = self._selector.groups()
        if name and name.lower() != tag:
            return False
        if element_id and attrs.get("id") != element_id:
            return False
        own = set((attrs.get("class") or "").split())
        return all(cls in own for cls in classes.split(".") if cls)

    def _start(self, tag: str, attr_list: list[tuple[str, str | None]]) -> None:
        attrs = dict(attr_list)
        text = self.get_starttag_text() or ""
        if tag == "html" and self.root is None:
            self.root = (self._offset(), text, attrs.get("id"))
        if self.clip is None and self._selector and self._matches(tag, attrs):
            self.clip = (self._offset(), text, attrs.get("id"))
        keys = ["src", "poster", "xlink:href"]
        if tag in ("link", "image", "use"):
            keys.append("href")
        for key in keys:
            value = attrs.get(key)
            if value and value.lower().startswith(("http://", "https://")):
                self.urls[value] = None
        if tag == "link" and "stylesheet" in (attrs.get("rel") or "").lower():
            if href := attrs.get("href"):
                self.stylesheets.append(href)
        if style := attrs.get("style"):
            self._add_css_urls(style)
        radius = attrs.get("data-blur") or ""
        if radius.isdigit() and int(radius):
            self.blurs.add(int(radius))
            for name, offset, start_text in self._stack:
                if name not in ("html", "body"):
                    self.hosts[offset] = start_text

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        self._start(tag, attrs)
        if tag in _VOID_TAGS:
            return
        self._stack.append((tag, self._offset(), self.get_starttag_text() or ""))
        self.max_depth = max(self.max_depth, len(self._stack))
        self._in_style = tag == "style"

    def handle_startendtag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        self._start(tag, attrs)

    def handle_endtag(self, tag: str) -> None:
        if any(name == tag for name, _, _ in self._stack):
            while self._stack and self._stack.pop()[0] != tag:
                pass
        self._in_style = False

    def handle_data(self, data: str) -> None:
        if self._in_style:
            self._add_css_urls(data)


def _with_id(
    html: str, found: tuple[int, str, str | None] | None, tag: str, new_id: str
) -> tuple[str, str]:
    """确保开始标签带有 id

    参数:
        html: 页面 HTML
        found: 标签位置、原始文本与已有 id
        tag: 标签名，标签不存在时在开头补上该标签
        new_id: 标签没有 id 时使用的 id

    返回:
        tuple[str, str]: 新的 HTML 与标签 id
    """
    if found is None:
        return f'<{tag} id="{new_id}">{html}', new_id
    offset, text, element_id = found
    if element_id:
        return html, element_id
    name_end = 1 + len(text[1:].split(None, 1)[0].rstrip("/>"))
    tagged = f'{text[:name_end]} id="{new_id}"{text[name_end:]}'
    return html[:offset] + tagged + html[offset + len(text) :], new_id


def _mark_hosts(html: str, hosts: dict[int, str]) -> str:
    """给模糊元素的祖先加上 data-blur-host 标记

    参数:
        html: 页面 HTML
        hosts: 祖先开始标签的位置与原始文本

    返回:
        str: 新的 HTML
    """
    for offset, text in sorted(hosts.items(), reverse=True):
        name_end = 1 + len(text[1:].split(None, 1)[0].rstrip("/>"))
        tagged = f"{text[:name_end]} data-blur-host{text[name_end:]}"
        html = html[:offset] + tagged + html[offset + len(text) :]
    return html


def _local_css(hrefs: list[str], base_url: str) -> str:
    """读取页面链接的本地样式表

    参数:
        hrefs: 样式表链接
        base_url: 页面基准地址

    返回:
        str: 样式表内容
    """
    texts = []
    for href in hrefs:
        url = urljoin(base_url, href)
        if not url.startswith("file:"):
            continue
        with contextlib.suppress(OSError, UnicodeDecodeError):
            texts.append(Path(url2pathname(urlparse(url).path)).read_text("utf-8"))
    return "\n".join(texts)


def _prune_font_faces(html: str, context: str) -> str:
    """删除页面没有引用的 @font-face，blitz 每次渲染都会加载全部 @font-face 字体

    参数:
        html: 页面 HTML
        context: 页面以外的样式，如链接的样式表

    返回:
        str: 新的 HTML
    """
    if "@font-face" not in html.lower():
        return html
    used = (_FONT_FACE.sub("", html) + context).lower()

    def keep(match: re.Match[str]) -> str:
        family = _FONT_FAMILY.search(match.group(0))
        if family is None:
            return match.group(0)
        name = family.group(1).strip().strip("'\"").lower()
        return match.group(0) if name in used else ""

    return _FONT_FACE.sub(keep, html)


_BLUR_HIDDEN = "[data-blur]{visibility:hidden!important}"
_BLUR_BASE_CSS = (
    f"*{_TEXT_RESET}*::before,*::after{_PAINT_RESET}"
    "img,svg,video{visibility:hidden!important}"
    f"*:not(html,body,[data-blur-host]){_PAINT_RESET}{_BLUR_HIDDEN}"
)
"""模糊合成底层：只画模糊元素祖先的背景"""
_BLUR_FRONT_CSS = f"html,body,[data-blur-host]{_PAINT_RESET}{_BLUR_HIDDEN}"
"""模糊合成顶层：画模糊元素以外的内容"""


def _blur_css(radius: int, mask: bool) -> str:
    """生成只画指定半径模糊元素的样式

    参数:
        radius: 模糊半径
        mask: 是否为遮罩，遮罩把模糊元素画为不透明色块，用于裁剪模糊后的边缘；
            否则取消祖先的溢出裁剪，让模糊边缘取到元素外的内容

    返回:
        str: 样式
    """
    own = f'[data-blur="{radius}"]'
    other = f":not({own},{own} *)"
    css = (
        "html,body{background:transparent!important}"
        f"*{other}{_TEXT_RESET}*{other}{_PAINT_RESET}"
        f"*{other}::before,*{other}::after{_PAINT_RESET}"
        f"img{other},svg{other},video{other}{{visibility:hidden!important}}"
        f"[data-blur]:not({own}){{visibility:hidden!important}}"
    )
    if mask:
        return f"{css}{own}{{background:#000!important}}"
    return f"{css}[data-blur-host]{{overflow:visible!important}}"


def _positive(value: Any, default: float) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return default
    return number if number > 0 else default


def _fit_scale(width: int, height: float, scale: float) -> float:
    """缩小倍率使整页不超过单图上限，最低为 1

    参数:
        width: 页面宽度
        height: 页面高度
        scale: 期望倍率

    返回:
        float: 实际倍率
    """
    limit = min(
        _MAX_SIDE / max(height, 1),
        _MAX_SIDE / width,
        math.sqrt(_MAX_PIXELS / (width * max(height, 1))),
    )
    return max(1.0, min(scale, limit))


def _fits(width: int, height: float, scale: float) -> bool:
    return (
        height * scale <= _MAX_SIDE
        and width * scale <= _MAX_SIDE
        and width * height * scale * scale <= _MAX_PIXELS
    )


def _encode(image: Image.Image, image_type: str, quality: int) -> bytes:
    buffer = io.BytesIO()
    if image_type == "png":
        image.save(buffer, "PNG")
    else:
        image.convert("RGB").save(buffer, "JPEG", quality=quality)
    return buffer.getvalue()


def _find_cut(image: Image.Image) -> int:
    """在页面底部寻找最接近纯色的行作为分页位置，避免切断文字

    参数:
        image: 当前页面图像

    返回:
        int: 分页行
    """
    width, height = image.size
    gray = image.convert("L")
    best_row, best_score = height, width + 1
    for row in range(height - 1, max(height - min(height // 5, 600), 1), -1):
        colors = gray.crop((0, row, width, row + 1)).getcolors(width) or []
        score = width - max(count for count, _ in colors)
        if score < best_score:
            best_row, best_score = row, score
            if score == 0:
                break
    return best_row


def stitch_pages(pages: list[bytes]) -> bytes:
    """把分页图片纵向拼接为一张，超出 JPEG 尺寸上限时输出 PNG

    参数:
        pages: 分页图片

    返回:
        bytes: 拼接后的图片
    """
    if len(pages) == 1:
        return pages[0]
    images = [Image.open(io.BytesIO(page)) for page in pages]
    width = max(image.width for image in images)
    height = sum(image.height for image in images)
    image_type = (
        "png" if height > _JPEG_MAX_SIDE or images[0].format == "PNG" else "jpeg"
    )
    canvas = Image.new("RGB", (width, height), "#ffffff")
    top = 0
    for image in images:
        canvas.paste(image.convert("RGB"), (0, top))
        top += image.height
    return _encode(canvas, image_type, 90)


class _RenderGate:
    """统计进行中的渲染，排空期间新渲染等待排空结束"""

    def __init__(self) -> None:
        self.active = 0
        self._idle = asyncio.Event()
        self._idle.set()
        self._open = asyncio.Event()
        self._open.set()

    @contextlib.asynccontextmanager
    async def track(self) -> AsyncIterator[None]:
        while not self._open.is_set():
            await self._open.wait()
        self.active += 1
        self._idle.clear()
        try:
            yield
        finally:
            self.active -= 1
            if not self.active:
                self._idle.set()

    async def wait_idle(self) -> None:
        await self._idle.wait()

    @contextlib.asynccontextmanager
    async def drain(self, timeout: float) -> AsyncIterator[None]:
        self._open.clear()
        try:
            await asyncio.wait_for(self.wait_idle(), timeout)
            yield
        finally:
            self._open.set()


_GATE = _RenderGate()


@contextlib.asynccontextmanager
async def drain_rendering(reason: str, *, timeout: float = 15.0):
    """等待进行中的渲染结束，期间新的渲染排队等待

    参数:
        reason: 排空原因
        timeout: 等待进行中渲染的超时时间
    """
    logger.debug(f"渲染排空: {reason}", "Renderer")
    async with _GATE.drain(timeout):
        yield


class _RenderJob:
    """一次渲染的参数"""

    def __init__(self, html: str, base_url_path: Path, options: dict[str, Any]):
        viewport = options.get("viewport") or {}
        self.width = int(_positive(viewport.get("width"), 800))
        self.min_height = int(_positive(viewport.get("height"), 0))
        self.full_page = bool(options.get("full_page", True)) or not self.min_height
        self.scale = _positive(options.get("device_scale_factor"), 2.0)
        if options.get("screenshot_scale", options.get("scale")) == "css":
            self.scale = 1.0
        self.image_type = "png" if options.get("type") == "png" else "jpeg"
        self.quality = int(_positive(options.get("quality"), 90))
        selector = options.get("clip_selector")
        self.selector = selector if isinstance(selector, str) and selector else None
        self.clip_padding = int(_positive(options.get("clip_padding"), 0))
        self.extra_resources = options.get("resources") or {}
        base_url = base_url_path.absolute().as_uri()
        self.base_url = base_url if base_url.endswith("/") else base_url + "/"
        self.html = html
        self.root_id = _ROOT_ID
        self.clip_id: str | None = None
        self.blurs: list[int] = []
        self.uses_vh = False
        self.urls: list[str] = []
        self.css: list[str] = [_NO_ANIMATION_CSS]
        self.resources: dict[str, bytes] = {}

    def prepare(self) -> None:
        """改写内联 SVG，检查嵌套深度，收集远程资源，给根元素与裁剪元素加上 id，
        删除未引用的 @font-face，并记录页面是否使用 vh 单位"""
        self.html = normalize_svg(self.html)
        scan = _HtmlScan(self.selector)
        scan.scan(self.html)
        if scan.hosts:
            self.html = _mark_hosts(self.html, scan.hosts)
            scan = _HtmlScan(self.selector)
            scan.scan(self.html)
        self.blurs = sorted(scan.blurs)
        if scan.max_depth > _MAX_DEPTH:
            raise RenderingError(f"页面嵌套层级过深 ({scan.max_depth})，无法渲染")
        self.urls = list(scan.urls)
        edits = [(scan.root, "html", _ROOT_ID)]
        if self.selector:
            if scan.clip is None:
                logger.debug(f"未找到裁剪元素 {self.selector}", "Renderer")
            else:
                edits.append((scan.clip, "div", _CLIP_ID))
        html = self.html
        ids: dict[str, str] = {}
        for found, tag, new_id in sorted(
            edits, key=lambda item: item[0][0] if item[0] else -1, reverse=True
        ):
            html, ids[new_id] = _with_id(html, found, tag, new_id)
        self.html = html
        self.root_id = ids[_ROOT_ID]
        self.clip_id = ids.get(_CLIP_ID)
        if self.full_page and self.min_height:
            self.css.append(f"html{{min-height:{self.min_height}px!important}}")
        linked = _local_css(scan.stylesheets, self.base_url)
        self.html = _prune_font_faces(self.html, linked + "".join(self.css))
        self.uses_vh = bool(_VH_UNIT.search(self.html) or _VH_UNIT.search(linked))


class BlitzEngine(BaseScreenshotEngine):
    """使用 blitz 将 HTML 渲染为图片的截图引擎"""

    _MAX_CONCURRENT_RENDER = 3

    def __init__(self) -> None:
        self._blitz: Any = None
        self._semaphore = asyncio.Semaphore(self._MAX_CONCURRENT_RENDER)

    def busy(self) -> bool:
        return _GATE.active > 0

    async def initialize(self) -> None:
        global _fonts_registered
        if self._blitz is not None:
            return
        blitz = import_module("blitz_py")
        files = [FONT_PATH / name for name in _DEFAULT_FONT_FILES]
        if _fonts_registered:
            pass
        elif files[0].is_file():
            fonts = [
                await asyncio.to_thread(file.read_bytes)
                for file in files
                if file.is_file()
            ]
            await asyncio.to_thread(
                blitz.register_fonts, fonts, default_family=_DEFAULT_FONT
            )
            _fonts_registered = True
        else:
            logger.warning(f"缺少默认字体 {files[0]}，中文可能无法显示", "Renderer")
        self._blitz = blitz

    async def warmup(self) -> None:
        await self.initialize()
        await asyncio.to_thread(
            self._blitz.render_jpeg, "<p>真寻</p>", width=100, height=40
        )

    async def close(self) -> None:
        with contextlib.suppress(asyncio.TimeoutError):
            await asyncio.wait_for(_GATE.wait_idle(), remaining_timeout(10.0))

    def _options(self, job: _RenderJob, css: str | None = None) -> dict[str, Any]:
        return {
            "base_url": job.base_url,
            "allow_file_urls": True,
            "resources": job.resources or None,
            "css": "".join([*job.css, css or ""]),
        }

    def _layer(
        self,
        job: _RenderJob,
        height: int,
        scale: float,
        css: str,
        *,
        transparent: bool = False,
        blur: int = 0,
    ) -> Image.Image:
        """绘制一层页面

        参数:
            job: 渲染参数
            height: 页面高度
            scale: 倍率
            css: 附加样式
            transparent: 是否使用透明背景
            blur: 模糊半径

        返回:
            Image.Image: 图像
        """
        options = self._options(job, css)
        if transparent:
            options["background"] = None
        if blur:
            layer = {"html": job.html, "width": job.width, "height": height}
            layer.update(scale=scale, blur=min(blur * scale, _MAX_BLUR), **options)
            size = (round(job.width * scale), round(height * scale))
            width, real_height, data = self._blitz.render_layers(
                [layer], width=size[0], height=size[1], background="#00000000"
            )
        else:
            width, real_height, data = self._blitz.render_rgba(
                job.html, width=job.width, height=height, scale=scale, **options
            )
        return Image.frombytes("RGBA", (width, real_height), data)

    def _rgba(
        self, job: _RenderJob, height: int, scale: float, css: str | None = None
    ) -> Image.Image:
        """绘制页面，带 data-blur 元素时分层绘制：底层画模糊元素祖先的背景，
        各模糊元素按半径模糊并按原形状裁剪后叠加，顶层画其余内容

        参数:
            job: 渲染参数
            height: 页面高度
            scale: 倍率
            css: 附加样式

        返回:
            Image.Image: 图像
        """
        css = css or ""
        if not job.blurs:
            return self._layer(job, height, scale, css)
        image = self._layer(job, height, scale, css + _BLUR_BASE_CSS)
        for radius in job.blurs:
            blurred = self._layer(
                job,
                height,
                scale,
                css + _blur_css(radius, False),
                transparent=True,
                blur=radius,
            )
            mask = self._layer(
                job, height, scale, css + _blur_css(radius, True), transparent=True
            )
            alpha = ImageChops.multiply(blurred.getchannel("A"), mask.getchannel("A"))
            blurred.putalpha(alpha)
            image.alpha_composite(blurred)
        front = self._layer(job, height, scale, css + _BLUR_FRONT_CSS, transparent=True)
        image.alpha_composite(front)
        return image

    def _render(self, job: _RenderJob, height: int | None, scale: float) -> bytes:
        if job.blurs:
            if height is None:
                _, y, _, box_height = self._box(job, job.root_id)
                height = max(math.ceil(y + box_height), job.min_height, 1)
            return _encode(self._rgba(job, height, scale), job.image_type, job.quality)
        options = self._options(job)
        if job.image_type == "png":
            return self._blitz.render_png(
                job.html, width=job.width, height=height, scale=scale, **options
            )
        return self._blitz.render_jpeg(
            job.html,
            width=job.width,
            height=height,
            scale=scale,
            quality=job.quality,
            **options,
        )

    def _box(self, job: _RenderJob, element_id: str) -> tuple[float, ...]:
        template = self._blitz.Template(
            job.html,
            width=job.width,
            height=max(job.min_height, 1),
            **self._options(job),
        )
        return template.get_box(element_id)

    def _render_clip(self, job: _RenderJob) -> list[bytes] | None:
        assert job.clip_id
        x, y, width, height = self._box(job, job.clip_id)
        if width <= 0 or height <= 0:
            return None
        padding = job.clip_padding
        bottom = math.ceil(y + height + padding)
        page_height = max(job.min_height, bottom)
        if not _fits(job.width, page_height, 1.0):
            return None
        scale = _fit_scale(job.width, page_height, job.scale)
        image = self._rgba(job, page_height, scale)
        box = (
            max(math.floor((x - padding) * scale), 0),
            max(math.floor((y - padding) * scale), 0),
            min(math.ceil((x + width + padding) * scale), image.width),
            min(math.ceil(bottom * scale), image.height),
        )
        return [_encode(image.crop(box), job.image_type, job.quality)]

    def _render_pages(self, job: _RenderJob) -> list[bytes]:
        """渲染整页，超出单图上限时先降低倍率，仍超出则分页

        参数:
            job: 渲染参数

        返回:
            list[bytes]: 页面图片
        """
        if job.clip_id and (clipped := self._render_clip(job)):
            return clipped
        if not job.full_page:
            height = job.min_height
            return [self._render(job, height, _fit_scale(job.width, height, job.scale))]
        if (not job.min_height or not job.uses_vh) and _fits(
            job.width, 1000, job.scale
        ):
            with contextlib.suppress(ValueError):
                return [self._render(job, None, job.scale)]
        _, y, _, height = self._box(job, job.root_id)
        total = max(math.ceil(y + height), job.min_height, 1)
        scale = _fit_scale(job.width, total, job.scale)
        if _fits(job.width, total, scale):
            return [self._render(job, total, scale)]
        page_height = min(_MAX_SIDE, _MAX_PIXELS // job.width) // scale
        pages: list[bytes] = []
        offset = 0.0
        while offset < total - 0.5:
            height = math.ceil(min(page_height, total - offset))
            image = self._rgba(
                job, height, scale, f"html{{margin-top:{-offset}px!important}}"
            )
            last = offset + height >= total - 0.5
            cut = image.height if last else _find_cut(image)
            pages.append(
                _encode(image.crop((0, 0, image.width, cut)), job.image_type, 90)
            )
            offset += cut / scale
        logger.debug(f"长页面 {total}px 分为 {len(pages)} 页", "Renderer")
        return pages

    async def render_pages(
        self, html: str, base_url_path: Path, **render_options
    ) -> list[bytes]:
        """渲染 HTML，超长页面分为多张图片

        参数:
            html: 页面 HTML
            base_url_path: 相对资源的基准目录
            **render_options: viewport、device_scale_factor、type、quality、
                full_page、clip_selector、clip_padding、resources 等参数

        返回:
            list[bytes]: 页面图片
        """
        await self.initialize()
        job = _RenderJob(html, base_url_path, render_options)
        await asyncio.to_thread(job.prepare)
        if job.urls:
            job.resources = await fetch_remote_resources(job.urls)
        job.resources.update(job.extra_resources)
        if EMOJI_PATTERN.search(job.html) and (font := await emoji_font()):
            job.css.append(
                f"@font-face{{font-family:'{_DEFAULT_FONT}';src:url('{font.absolute().as_uri()}');"
                f"unicode-range:{EMOJI_UNICODE_RANGE}}}"
            )
        async with _GATE.track(), self._semaphore:
            try:
                return await asyncio.to_thread(self._render_pages, job)
            except ValueError as error:
                raise RenderingError(f"页面渲染失败: {error}") from error

    async def render(self, html: str, base_url_path: Path, **render_options) -> bytes:
        pages = await self.render_pages(html, base_url_path, **render_options)
        if len(pages) == 1:
            return pages[0]
        return await asyncio.to_thread(stitch_pages, pages)


async def stop_preparation(task: asyncio.Task | None) -> None:
    if task is None or task is asyncio.current_task():
        return
    if task.done():
        if not task.cancelled():
            task.exception()
        return
    task.cancel()
    _, pending = await asyncio.wait({task}, timeout=remaining_timeout(5.0))
    if pending:
        raise TimeoutError("renderer_preparation_cleanup_timeout")
    if not task.cancelled():
        task.exception()


class EngineManager:
    """截图引擎的共享初始化与预热"""

    def __init__(self):
        self._engine_class: type[BaseScreenshotEngine] = BlitzEngine
        self._instance: BaseScreenshotEngine | None = None
        self._init_task: asyncio.Task | None = None
        self._warmup_task: asyncio.Task | None = None
        self._closing = False

    async def _initialize(self) -> BaseScreenshotEngine:
        engine = self._engine_class()
        self._instance = engine
        await engine.initialize()
        return engine

    async def get_engine(self) -> BaseScreenshotEngine:
        if self._closing:
            raise RuntimeError("renderer_closing")
        if self._init_task is None:
            self._init_task = asyncio.create_task(
                self._initialize(), name="renderer-initialize"
            )
        return await asyncio.shield(self._init_task)

    async def _prepare(self) -> None:
        engine = await self.get_engine()
        if isinstance(engine, BlitzEngine):
            await engine.warmup()
        if self._closing:
            raise RuntimeError("renderer_closing")

    async def warmup(self) -> None:
        if self._closing:
            raise RuntimeError("renderer_closing")
        if self._warmup_task is None:
            self._warmup_task = asyncio.create_task(
                self._prepare(), name="renderer-warmup"
            )
        await asyncio.shield(self._warmup_task)

    async def close(self):
        self._closing = True
        await stop_preparation(self._warmup_task)
        await stop_preparation(self._init_task)
        if self._instance is not None:
            await self._instance.close()
            self._instance = None
        self._init_task = self._warmup_task = None


engine_manager = EngineManager()
