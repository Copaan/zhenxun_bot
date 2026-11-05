from __future__ import annotations

import asyncio
import base64
from collections import OrderedDict
from collections.abc import Callable, Hashable
import hashlib
from html import escape
from html.parser import HTMLParser
import io
import math
from pathlib import Path
import re
import tarfile
import threading
import time
from urllib.parse import urljoin, urlparse
from urllib.request import url2pathname

from PIL import Image

from zhenxun.configs.path_config import DATA_PATH
from zhenxun.services.cache.bounded_ttl import BoundedTTLCache
from zhenxun.services.log import logger
from zhenxun.utils.http_utils import AsyncHttpx

EMOJI_PATTERN = re.compile(
    "[\U0001f000-\U0001faff\u2300-\u23ff\u2600-\u27bf\u2b00-\u2bff]"
)
"""需要 emoji 字体的字符"""
EMOJI_UNICODE_RANGE = (
    "U+1F000-1FAFF,U+2300-23FF,U+2600-27BF,U+2B00-2BFF,U+200D,U+20E3,U+FE0F"
)

_EMOJI_FONT = DATA_PATH / "renderer" / "twemoji-15.0.3.woff2"
_EMOJI_URLS = (
    "https://registry.npmmirror.com/twemoji-colr-font/-/twemoji-colr-font-15.0.3.tgz",
    "https://registry.npmjs.org/twemoji-colr-font/-/twemoji-colr-font-15.0.3.tgz",
)
_EMOJI_INTEGRITY = (
    "UsK4JUpaczeVoMGeYMnKaMTxKt7fujg1nQOk4NaC0teZmdOo+uAai0DIuaSzsMkShtG4J75F0OEsVd"
    "GJ+Q1pHQ=="
)
_EMOJI_WAIT_SECONDS = 8.0
_EMOJI_RETRY_SECONDS = 600.0

_REMOTE_MAX_URLS = 48
_REMOTE_MAX_BYTES = 8 * 1024 * 1024
_REMOTE_TIMEOUT = 8.0
_REMOTE_CONCURRENCY = 8
_remote_cache: BoundedTTLCache[str, bytes] = BoundedTTLCache(
    "RENDER_REMOTE_RESOURCE",
    ttl_seconds=600,
    max_items=256,
    max_total_bytes=48 * 1024 * 1024,
)
_remote_failed: BoundedTTLCache[str, bool] = BoundedTTLCache(
    "RENDER_REMOTE_FAILED", ttl_seconds=120, max_items=512
)
_remote_limit: asyncio.Semaphore | None = None
_CSS_IMAGE = re.compile(r"""url\(\s*(['"]?)([^'"()\s]+)\1\s*\)""", re.IGNORECASE)
_LOCAL_SUFFIXES = (".webp", ".png")
_PLACEHOLDER_SUFFIXES = (".webp", ".png", ".jpg", ".jpeg", ".gif")
_LOCAL_MIN_PIXELS = 256 * 256
"""像素数小于该值的图片解码很快，不转码"""
_LOCAL_MAX_URLS = 64
_LOCAL_CACHE_BYTES = 128 * 1024**2
_local_cache: OrderedDict[tuple[str, int, int], bytes | None] = OrderedDict()
_local_cache_bytes = 0
_local_lock = threading.Lock()
SHRINK_MIN_SIDE = 128
"""原图长边小于该值时不预先缩小"""
_SHRINK_RATIO = 0.75
"""所需尺寸不超过原图的该比例时预先缩小"""
_SHRINK_CACHE_SIZE = 64
_shrink_cache: OrderedDict[tuple[Hashable, int, int], bytes | None] = OrderedDict()
_shrink_lock = threading.Lock()
_IMG_SRC = re.compile(r"""(?<![\w-])src\s*=\s*("[^"]*"|'[^']*'|[^\s>]+)""", re.I)
_emoji_task: asyncio.Task[Path | None] | None = None
_emoji_retry_at = 0.0


_SVG_BLOCK = re.compile(r"<svg\b.*?</svg\s*>", re.IGNORECASE | re.DOTALL)
_CSS_VAR_DECL = re.compile(r"(--[\w-]+)\s*:\s*([^;{}<>]+)")
_CSS_VAR_USE = re.compile(r"var\(\s*(--[\w-]+)\s*(?:,\s*([^()]*))?\)")
_SVG_PAINT = frozenset(
    (
        "fill fill-opacity fill-rule stroke stroke-width stroke-opacity "
        "stroke-linecap stroke-linejoin stroke-dasharray stop-color stop-opacity "
        "font-size font-family font-weight text-anchor"
    ).split()
)
_SVG_INHERITED = _SVG_PAINT - frozenset(("stop-color", "stop-opacity"))
_SVG_SHAPES = frozenset(
    "path circle rect ellipse line polyline polygon text tspan use".split()
)


def _resolve_vars(value: str, variables: dict[str, str], depth: int = 0) -> str:
    """把 var() 替换为变量值，未定义时使用默认值

    参数:
        value: 属性或样式值
        variables: CSS 变量
        depth: 递归深度

    返回:
        str: 替换后的值
    """
    if "var(" not in value or depth > 5:
        return value

    def replace(match: re.Match) -> str:
        found = variables.get(match.group(1), match.group(2) or "")
        return _resolve_vars(found.strip(), variables, depth + 1)

    return _CSS_VAR_USE.sub(replace, value)


class _SvgFix(HTMLParser):
    """把 SVG 中继承的填充与描边写到每个图形上，并替换其中的 CSS 变量"""

    def __init__(self, variables: dict[str, str]):
        super().__init__(convert_charrefs=False)
        self.variables = variables
        self.parts: list[str] = []
        self._stack: list[tuple[str, dict[str, str]]] = []

    def _tag(self, tag: str, attr_list: list[tuple[str, str | None]], end: bool):
        attrs = {
            name: None if value is None else _resolve_vars(value, self.variables)
            for name, value in attr_list
        }
        own: dict[str, str] = {
            name: value
            for name, value in attrs.items()
            if name in _SVG_PAINT and value is not None
        }
        for item in (attrs.get("style") or "").split(";"):
            name, _, value = item.partition(":")
            if name.strip().lower() in _SVG_PAINT and value.strip():
                own[name.strip().lower()] = value.strip()
        inherited = dict(self._stack[-1][1]) if self._stack else {}
        inherited.update((k, v) for k, v in own.items() if k in _SVG_INHERITED)
        if tag in _SVG_SHAPES:
            inherited.setdefault("fill", "currentColor")
            for name, value in inherited.items():
                if name not in own:
                    attrs[name] = value
            for name, value in own.items():
                attrs[name] = value
        text = "".join(
            f" {name}" if value is None else f' {name}="{escape(value)}"'
            for name, value in attrs.items()
        )
        self.parts.append(f"<{tag}{text}{' /' if end else ''}>")
        if not end:
            self._stack.append((tag, inherited))

    def handle_starttag(self, tag, attrs):
        self._tag(tag, attrs, False)

    def handle_startendtag(self, tag, attrs):
        self._tag(tag, attrs, True)

    def handle_endtag(self, tag):
        while self._stack and self._stack.pop()[0] != tag:
            pass
        self.parts.append(f"</{tag}>")

    def handle_data(self, data):
        self.parts.append(data)

    def handle_entityref(self, name):
        self.parts.append(f"&{name};")

    def handle_charref(self, name):
        self.parts.append(f"&#{name};")


def normalize_svg(html: str) -> str:
    """改写内联 SVG：渲染引擎不支持 SVG 内的属性继承与 CSS 变量，
    把继承的填充、描边写到每个图形上，并替换 var()

    参数:
        html: 页面 HTML

    返回:
        str: 改写后的 HTML
    """
    if "<svg" not in html.lower():
        return html
    variables: dict[str, str] = {}
    for name, value in _CSS_VAR_DECL.findall(html):
        variables[name] = value.strip()

    def fix(match: re.Match) -> str:
        parser = _SvgFix(variables)
        parser.feed(match.group(0))
        parser.close()
        return "".join(parser.parts)

    return _SVG_BLOCK.sub(fix, html)


async def _fetch_remote(url: str) -> bytes | None:
    """下载一个远程资源，失败的地址短时间内不再重试

    参数:
        url: 资源地址

    返回:
        bytes | None: 资源内容，下载失败时为 None
    """
    global _remote_limit
    if (data := await _remote_cache.get(url)) is not None:
        return data
    if await _remote_failed.get(url):
        return None
    if _remote_limit is None:
        _remote_limit = asyncio.Semaphore(_REMOTE_CONCURRENCY)
    try:
        async with _remote_limit:
            response = await asyncio.wait_for(
                AsyncHttpx.get(url, timeout=_REMOTE_TIMEOUT), _REMOTE_TIMEOUT + 2
            )
        if response.status_code != 200:
            raise ValueError(f"HTTP {response.status_code}")
        data = response.content
        if len(data) > _REMOTE_MAX_BYTES:
            raise ValueError(f"资源过大 {len(data)} bytes")
    except Exception as error:
        await _remote_failed.set(url, True)
        logger.debug(f"渲染远程资源下载失败: {url} ({error})", "Renderer")
        return None
    await _remote_cache.set(url, data)
    return data


async def fetch_remote_resources(urls: list[str]) -> dict[str, bytes]:
    """预取页面引用的远程资源，交给渲染引擎在内存中加载

    参数:
        urls: 远程资源地址

    返回:
        dict[str, bytes]: 下载成功的资源，键为原地址
    """
    urls = urls[:_REMOTE_MAX_URLS]
    contents = await asyncio.gather(*(_fetch_remote(url) for url in urls))
    return {url: data for url, data in zip(urls, contents) if data is not None}


def css_image_urls(css: str, base_url: str) -> list[str]:
    """找出样式中 url() 引用的地址

    参数:
        css: 样式
        base_url: 解析相对地址的基准地址

    返回:
        list[str]: 解析后的地址
    """
    return [urljoin(base_url, match.group(2)) for match in _CSS_IMAGE.finditer(css)]


def _local_path(url: str, suffixes: tuple[str, ...]) -> Path | None:
    """本地图片地址对应的文件路径

    参数:
        url: 图片地址
        suffixes: 允许的扩展名

    返回:
        Path | None: 文件路径，不是本地文件或扩展名不符时为 None
    """
    parsed = urlparse(url)
    if parsed.scheme != "file" or not parsed.path.lower().endswith(suffixes):
        return None
    return Path(url2pathname(parsed.path))


def _file_key(path: Path) -> tuple[str, int, int] | None:
    """文件的缓存键，由路径、修改时间与大小组成

    参数:
        path: 文件路径

    返回:
        tuple[str, int, int] | None: 缓存键，文件不存在时为 None
    """
    try:
        stat = path.stat()
    except OSError:
        return None
    return (str(path), stat.st_mtime_ns, stat.st_size)


def image_placeholder(url: str) -> tuple[Hashable, bytes] | None:
    """生成与本地图片尺寸相同的空白 SVG，测量排版时代替原图，免去解码原图

    参数:
        url: 图片地址

    返回:
        tuple[Hashable, bytes] | None: 原图的缓存键与占位图，图片过小或无法读取时为 None
    """
    path = _local_path(url, _PLACEHOLDER_SUFFIXES)
    if path is None or (key := _file_key(path)) is None:
        return None
    try:
        with Image.open(path) as image:
            width, height = image.size
            if max(width, height) < SHRINK_MIN_SIDE:
                return None
    except (OSError, ValueError, Image.DecompressionBombError):
        return None
    svg = (
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" '
        f'height="{height}" viewBox="0 0 {width} {height}"></svg>'
    )
    return key, svg.encode()


def _transcode(path: Path) -> bytes | None:
    """把 WebP 与压缩过的 PNG 转为无压缩 PNG

    参数:
        path: 图片路径

    返回:
        bytes | None: 转码后的图片，无需转码或无法解码时为 None
    """
    try:
        data = path.read_bytes()
        image = Image.open(io.BytesIO(data))
        pixels = image.width * image.height
        if getattr(image, "n_frames", 1) > 1 or pixels < _LOCAL_MIN_PIXELS:
            return None
        if image.format not in ("WEBP", "PNG"):
            return None
        if image.format == "PNG" and len(data) > pixels * len(image.getbands()):
            return None
        alpha = image.mode in ("RGBA", "LA", "PA") or "transparency" in image.info
        image = image.convert("RGBA" if alpha else "RGB")
    except (OSError, ValueError, Image.DecompressionBombError):
        return None
    buffer = io.BytesIO()
    image.save(buffer, "PNG", compress_level=0)
    return buffer.getvalue()


def local_images(urls: list[str]) -> dict[str, tuple[Hashable, bytes]]:
    """把页面引用的本地 WebP、PNG 图片转为无压缩 PNG，blitz 解码无压缩 PNG
    比 WebP 快数倍，结果按文件与修改时间缓存

    参数:
        urls: 图片地址

    返回:
        dict[str, tuple[Hashable, bytes]]: 转码后的图片，键为原地址，值为缓存键与图片
    """
    global _local_cache_bytes
    images = {}
    for url in list(dict.fromkeys(urls))[:_LOCAL_MAX_URLS]:
        path = _local_path(url, _LOCAL_SUFFIXES)
        if path is None or (key := _file_key(path)) is None:
            continue
        with _local_lock:
            cached = key in _local_cache
            data = _local_cache.get(key)
            if cached:
                _local_cache.move_to_end(key)
        if not cached:
            data = _transcode(path)
            size = len(data or b"")
            with _local_lock:
                if key not in _local_cache and size <= _LOCAL_CACHE_BYTES // 4:
                    _local_cache[key] = data
                    _local_cache_bytes += size
                    while _local_cache_bytes > _LOCAL_CACHE_BYTES:
                        _, old = _local_cache.popitem(last=False)
                        _local_cache_bytes -= len(old or b"")
        if data is not None:
            images[url] = (key, data)
    return images


def _shrunk_image(data: bytes, width: int, height: int) -> bytes | None:
    """按显示尺寸用 Lanczos 缩小图片，blitz 大幅缩小图片时采样粗糙，会出现颗粒和锯齿

    参数:
        data: 原图
        width: 显示宽度（物理像素）
        height: 显示高度（物理像素）

    返回:
        bytes | None: 缩小后的图片，无需缩小或无法解码时为 None
    """
    try:
        image = Image.open(io.BytesIO(data))
        if getattr(image, "n_frames", 1) > 1 or max(image.size) < SHRINK_MIN_SIDE:
            return None
        factor = max(width / image.width, height / image.height)
        if factor > _SHRINK_RATIO:
            return None
        size = (
            max(math.ceil(image.width * factor), 1),
            max(math.ceil(image.height * factor), 1),
        )
        alpha = image.mode in ("RGBA", "LA", "PA") or "transparency" in image.info
        image = image.convert("RGBA" if alpha else "RGB").resize(
            size, Image.Resampling.LANCZOS
        )
    except (OSError, ValueError, Image.DecompressionBombError):
        return None
    buffer = io.BytesIO()
    if alpha:
        image.save(buffer, "PNG", compress_level=1)
    else:
        image.save(buffer, "JPEG", quality=95, subsampling=0)
    return buffer.getvalue()


def shrink_image(
    key: Hashable, width: int, height: int, load: Callable[[], bytes | None]
) -> bytes | None:
    """按显示尺寸缩小图片，结果按图片与尺寸缓存，命中缓存时不读取原图

    参数:
        key: 原图的缓存键
        width: 显示宽度（物理像素）
        height: 显示高度（物理像素）
        load: 读取原图的函数

    返回:
        bytes | None: 缩小后的图片，无需缩小或无法解码时为 None
    """
    cache_key = (key, width, height)
    with _shrink_lock:
        if cache_key in _shrink_cache:
            _shrink_cache.move_to_end(cache_key)
            return _shrink_cache[cache_key]
    data = load()
    shrunk = None if data is None else _shrunk_image(data, width, height)
    with _shrink_lock:
        _shrink_cache[cache_key] = shrunk
        while len(_shrink_cache) > _SHRINK_CACHE_SIZE:
            _shrink_cache.popitem(last=False)
    return shrunk


def replace_img(
    html: str, element_id: str, url: str, width: float, height: float
) -> str:
    """替换图片地址，并补上宽高属性以保持原有布局

    参数:
        html: 页面 HTML
        element_id: 图片 id
        url: 新地址
        width: 显示宽度
        height: 显示高度

    返回:
        str: 新的 HTML
    """
    pattern = re.compile(
        rf"""<img\b[^>]*?\bid\s*=\s*["']?{re.escape(element_id)}(?=["'\s/>])[^>]*>""",
        re.IGNORECASE,
    )
    match = pattern.search(html)
    if match is None:
        return html
    tag = _IMG_SRC.sub(f'src="{url}"', match.group(0), count=1)
    for name, value in (("width", width), ("height", height)):
        if not re.search(rf"(?<![\w-]){name}\s*=", tag, re.IGNORECASE):
            tag = f'<img {name}="{value:g}"{tag[4:]}'
    return html[: match.start()] + tag + html[match.end() :]


def _extract_emoji_font(data: bytes) -> bytes:
    """校验 npm 包并取出 emoji 字体

    参数:
        data: npm 包内容

    返回:
        bytes: 字体内容
    """
    digest = base64.b64encode(hashlib.sha512(data).digest()).decode()
    if digest != _EMOJI_INTEGRITY:
        raise ValueError("emoji 字体包校验失败")
    with tarfile.open(fileobj=io.BytesIO(data), mode="r:gz") as archive:
        member = archive.extractfile("package/twemoji.woff2")
        if member is None:
            raise ValueError("emoji 字体包缺少字体文件")
        return member.read()


def _save_emoji_font(font: bytes) -> None:
    _EMOJI_FONT.parent.mkdir(parents=True, exist_ok=True)
    temp = _EMOJI_FONT.with_suffix(".tmp")
    temp.write_bytes(font)
    temp.replace(_EMOJI_FONT)


async def _download_emoji_font() -> Path | None:
    global _emoji_retry_at
    error: Exception | None = None
    for url in _EMOJI_URLS:
        try:
            response = await AsyncHttpx.get(url, timeout=30)
            if response.status_code != 200:
                raise ValueError(f"HTTP {response.status_code}")
            font = await asyncio.to_thread(_extract_emoji_font, response.content)
            await asyncio.to_thread(_save_emoji_font, font)
            logger.info("emoji 字体下载完成", "Renderer")
            return _EMOJI_FONT
        except Exception as e:
            error = e
    _emoji_retry_at = time.monotonic() + _EMOJI_RETRY_SECONDS
    logger.warning(f"emoji 字体下载失败，emoji 暂用系统字体显示: {error}", "Renderer")
    return None


async def emoji_font() -> Path | None:
    """获取 emoji 字体，首次使用时下载，下载较慢时本次先不使用

    返回:
        Path | None: 字体路径，不可用时为 None
    """
    global _emoji_task
    if _EMOJI_FONT.is_file():
        return _EMOJI_FONT
    if _emoji_task is None or (
        _emoji_task.done() and time.monotonic() >= _emoji_retry_at
    ):
        _emoji_task = asyncio.create_task(
            _download_emoji_font(), name="renderer-emoji-font"
        )
    done, _ = await asyncio.wait({_emoji_task}, timeout=_EMOJI_WAIT_SECONDS)
    return _emoji_task.result() if done else None
