import asyncio
from collections import OrderedDict
from collections.abc import AsyncIterator, Awaitable, Callable, Coroutine
import contextlib
from dataclasses import dataclass, field
import hashlib
from importlib import import_module
import inspect
import json
from pathlib import Path
import re
import sys
import time
from typing import Any, ClassVar, cast
from urllib.parse import unquote, urlsplit

import nonebot
import psutil

from zhenxun.configs.config import Config
from zhenxun.services.log import logger

from .types import BaseScreenshotEngine

_PLAYWRIGHT_DISCONNECT_ERROR = "Connection closed while reading from the driver"
_PLAYWRIGHT_TARGET_CLOSED_ERROR_MARKERS = (
    "TargetClosedError",
    "Target page, context or browser has been closed",
    "browser has been closed",
    "BrowserContext.new_page",
)
_UNRETRIEVED_FUTURE_MESSAGE = "Future exception was never retrieved"
_LOOP_EXCEPTION_FILTER_STATE_ATTR = "_zhenxun_playwright_exception_filter_state"
_DISCONNECT_SUPPRESSION_WINDOW_SECONDS = 10.0

htmlrender_module: Any | None = None
htmlrender_browser: Any | None = None
_DIAGNOSTIC_ROOT = Path(__file__).resolve().parents[3]
_DIAGNOSTIC_WINDOW_SECONDS = 30.0
_DIAGNOSTIC_MAX_ITEMS = 256
_DIAGNOSTIC_TEMPLATE_BURST = 5
_SCRIPT_ERROR_CATEGORIES = frozenset(
    {
        "Error",
        "EvalError",
        "RangeError",
        "ReferenceError",
        "SyntaxError",
        "TypeError",
        "URIError",
        "AggregateError",
    }
)
_render_diagnostic_times: OrderedDict[str, float] = OrderedDict()
_render_diagnostic_templates: OrderedDict[str, tuple[float, int]] = OrderedDict()


def _diagnostic_resource(value: Any) -> str:
    """Only disclose repository-local paths, never URL authorities or payloads."""
    if not isinstance(value, str) or not value or len(value) > 4096:
        return "unknown"
    try:
        url = urlsplit(value)
        if url.scheme not in {"", "file"}:
            return "remote" if url.scheme in {"http", "https"} else "redacted"
        if url.netloc:
            return "external"
        path_text = unquote(url.path)
        if re.match(r"^/[A-Za-z]:/", path_text):
            path_text = path_text[1:]
        path = Path(path_text)
        if not path.is_absolute():
            path = _DIAGNOSTIC_ROOT / path
        relative = path.resolve().relative_to(_DIAGNOSTIC_ROOT).as_posix()
        if len(relative) > 180 or not re.fullmatch(r"[\w ./-]+", relative):
            return "local"
        return relative
    except (ValueError, OSError):
        return "external"


def _script_error_location(error: Any) -> tuple[str, int | None, int | None]:
    stack = getattr(error, "stack", None)
    if isinstance(stack, str):
        stack = stack[:16384]
        message = getattr(error, "message", "")
        name = getattr(error, "name", "")
        if isinstance(message, str) and message:
            if len(message) > 8192 or not isinstance(name, str) or len(name) > 128:
                return "inline", None, None
            prefix = f"{name}: {message}"
            if not stack.startswith(prefix):
                return "inline", None, None
            stack = stack[len(prefix) :]
        # Ignore the message, source excerpts and function names. Only parse frames.
        for frame in stack.splitlines()[1:21]:
            match = re.fullmatch(
                r"\s*at (?:.*? \()?((?:file|https?)://[^\r\n]+):(\d{1,9}):(\d{1,9})\)?",
                frame,
            )
            if match:
                return (_diagnostic_resource(match[1]), int(match[2]), int(match[3]))
    return "inline", None, None


def _render_diagnostic(
    code: str,
    *,
    template: str,
    category: str,
    resource: str,
    line: int | None = None,
    column: int | None = None,
) -> None:
    details = {
        "template": template,
        "category": category,
        "resource": resource,
        "line": line,
        "column": column,
    }
    serialized = json.dumps(details, ensure_ascii=True, sort_keys=True)
    diagnostic_id = hashlib.sha256(f"{code}:{serialized}".encode()).hexdigest()[:16]
    now = time.monotonic()
    for cache in (_render_diagnostic_times, _render_diagnostic_templates):
        while cache:
            value = next(iter(cache.values()))
            timestamp = value[0] if isinstance(value, tuple) else value
            if now - timestamp < _DIAGNOSTIC_WINDOW_SECONDS:
                break
            cache.popitem(last=False)
    if diagnostic_id in _render_diagnostic_times:
        return
    started, count = _render_diagnostic_templates.get(template, (now, 0))
    if count >= _DIAGNOSTIC_TEMPLATE_BURST:
        return
    _render_diagnostic_times[diagnostic_id] = now
    _render_diagnostic_templates[template] = (started, count + 1)
    for cache in (_render_diagnostic_times, _render_diagnostic_templates):
        while len(cache) > _DIAGNOSTIC_MAX_ITEMS:
            cache.popitem(last=False)
    logger.warning(
        f"渲染页面异常 | code={code} | diagnostic_id={diagnostic_id} | {serialized}",
        "Renderer",
    )


def _load_htmlrender_modules() -> tuple[Any, Any]:
    global htmlrender_module, htmlrender_browser
    if htmlrender_module is None or htmlrender_browser is None:
        if "nonebot_plugin_htmlrender" not in sys.modules:
            nonebot.require("nonebot_plugin_htmlrender")
        htmlrender_module = import_module("nonebot_plugin_htmlrender")
        htmlrender_browser = import_module("nonebot_plugin_htmlrender.browser")
    return htmlrender_module, htmlrender_browser


class HtmlrenderTaskTracker:
    """只追踪 htmlrender 渲染任务的轻量运行时。"""

    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._idle_event = asyncio.Event()
        self._idle_event.set()
        self._active_tasks = 0
        self._draining = False
        self._drain_reason: str | None = None

    @property
    def active_tasks(self) -> int:
        return self._active_tasks

    @property
    def is_draining(self) -> bool:
        return self._draining

    async def reset(self) -> None:
        async with self._lock:
            self._draining = False
            self._drain_reason = None
            if self._active_tasks == 0:
                self._idle_event.set()

    async def resume(self) -> None:
        async with self._lock:
            self._draining = False
            self._drain_reason = None

    async def mark_draining(self, reason: str) -> None:
        async with self._lock:
            self._draining = True
            self._drain_reason = reason

    async def begin(self, owner: str) -> None:
        async with self._lock:
            if self._draining:
                reason = self._drain_reason or "unknown"
                message = (
                    "htmlrender 正在排空，拒绝新的渲染任务: "
                    f"owner={owner}, reason={reason}"
                )
                raise RuntimeError(message)
            self._active_tasks += 1
            self._idle_event.clear()

    async def end(self) -> None:
        async with self._lock:
            self._active_tasks = max(0, self._active_tasks - 1)
            if self._active_tasks == 0:
                self._idle_event.set()

    async def wait_for_idle(self) -> None:
        await self._idle_event.wait()

    @contextlib.asynccontextmanager
    async def track(self, owner: str):
        await self.begin(owner)
        try:
            yield
        finally:
            await self.end()


_HTMLRENDER_TASK_TRACKER = HtmlrenderTaskTracker()


@contextlib.asynccontextmanager
async def drain_rendering(reason: str, *, timeout: float = 15.0):
    await _HTMLRENDER_TASK_TRACKER.mark_draining(reason)
    try:
        await asyncio.wait_for(_HTMLRENDER_TASK_TRACKER.wait_for_idle(), timeout)
        yield
    finally:
        await _HTMLRENDER_TASK_TRACKER.resume()


@dataclass(slots=True)
class ContextGeneration:
    generation_id: int
    context_pool: asyncio.LifoQueue[Any] = field(default_factory=asyncio.LifoQueue)
    all_contexts: set[Any] = field(default_factory=set)
    active_leases: int = 0
    retiring: bool = False

    def snapshot(self) -> dict[str, int | bool]:
        return {
            "generation_id": self.generation_id,
            "pool_size": self.context_pool.qsize(),
            "context_count": len(self.all_contexts),
            "active_leases": self.active_leases,
            "retiring": self.retiring,
        }


async def _await_if_needed(value: Any) -> Any:
    if inspect.isawaitable(value):
        return await cast(Awaitable[Any], value)
    return value


async def _get_browser_instance() -> Any:
    _, browser_module = _load_htmlrender_modules()
    for attr_name in ("get_browser", "get_new_browser"):
        browser_getter = getattr(browser_module, attr_name, None)
        if callable(browser_getter):
            return await _await_if_needed(browser_getter())
    raise RuntimeError("nonebot_plugin_htmlrender.browser 未提供可用浏览器获取函数。")


def _is_ignorable_playwright_disconnect(ctx: dict[str, Any]) -> bool:
    exc = ctx.get("exception")
    return (
        ctx.get("message") == _UNRETRIEVED_FUTURE_MESSAGE
        and isinstance(exc, Exception)
        and _PLAYWRIGHT_DISCONNECT_ERROR in str(exc)
    )


def _is_playwright_target_closed_error(exc: Exception) -> bool:
    exc_name = type(exc).__name__
    if exc_name == "TargetClosedError":
        return True
    message = str(exc)
    return any(marker in message for marker in _PLAYWRIGHT_TARGET_CLOSED_ERROR_MARKERS)


def _get_loop_exception_filter_state(
    loop: asyncio.AbstractEventLoop,
) -> dict[str, Any] | None:
    state = getattr(loop, _LOOP_EXCEPTION_FILTER_STATE_ATTR, None)
    if isinstance(state, dict):
        return state
    return None


def _ensure_loop_exception_filter(
    loop: asyncio.AbstractEventLoop,
) -> dict[str, Any]:
    state = _get_loop_exception_filter_state(loop)
    if state is not None:
        return state

    state = {
        "original_handler": loop.get_exception_handler(),
        "suppress_until": 0.0,
    }

    def _filter(lp: asyncio.AbstractEventLoop, ctx: dict[str, Any]) -> None:
        if _is_ignorable_playwright_disconnect(ctx):
            suppress_until = float(state.get("suppress_until", 0.0))
            if suppress_until >= time.monotonic():
                return
        original_handler = state.get("original_handler")
        if callable(original_handler):
            original_handler(lp, ctx)
            return
        lp.default_exception_handler(ctx)

    loop.set_exception_handler(_filter)
    setattr(loop, _LOOP_EXCEPTION_FILTER_STATE_ATTR, state)
    return state


def _arm_disconnect_exception_suppression(
    loop: asyncio.AbstractEventLoop,
    *,
    seconds: float = _DISCONNECT_SUPPRESSION_WINDOW_SECONDS,
) -> None:
    state = _ensure_loop_exception_filter(loop)
    deadline = time.monotonic() + max(seconds, 0.0)
    state["suppress_until"] = max(float(state.get("suppress_until", 0.0)), deadline)


async def _shutdown_browser_instance() -> None:
    if htmlrender_browser is None:
        return
    loop = asyncio.get_running_loop()
    _arm_disconnect_exception_suppression(loop)

    browser_obj = getattr(htmlrender_browser, "_browser", None)
    playwright_obj = getattr(htmlrender_browser, "_playwright", None)
    if browser_obj is None and playwright_obj is None:
        return
    failures = []
    close_func = getattr(browser_obj, "close", None) if browser_obj else None
    disconnected = browser_obj is not None and not browser_obj.is_connected()
    if callable(close_func) and not disconnected:
        try:
            await _await_if_needed(close_func())
        except Exception as error:
            if browser_obj.is_connected():
                failures.append(error)
    stop_func = getattr(playwright_obj, "stop", None) if playwright_obj else None
    if callable(stop_func):
        try:
            await _await_if_needed(stop_func())
        except Exception as error:
            failures.append(error)
    if failures:
        raise RuntimeError("renderer_browser_cleanup_failed") from failures[0]
    setattr(htmlrender_browser, "_browser", None)
    setattr(htmlrender_browser, "_playwright", None)
    await asyncio.sleep(0)


def _patch_htmlrender_task_tracking() -> None:
    render_module, browser_module = _load_htmlrender_modules()
    if getattr(browser_module, "_zhenxun_task_tracking_patched", False):
        return

    try:
        import nonebot_plugin_htmlrender.data_source as htmlrender_data_source
    except Exception as e:
        logger.warning("导入 htmlrender.data_source 失败，跳过任务追踪补丁。", e=e)
        return

    original_get_new_page = getattr(browser_module, "get_new_page", None)
    if not callable(original_get_new_page):
        logger.warning("htmlrender 未提供 get_new_page，跳过任务追踪补丁。")
        return
    original_get_new_page = cast(Callable[..., Any], original_get_new_page)

    @contextlib.asynccontextmanager
    async def _tracked_get_new_page(*args: Any, **kwargs: Any) -> AsyncIterator[Any]:
        async with _HTMLRENDER_TASK_TRACKER.track("htmlrender"):
            page_context = cast(Any, original_get_new_page(*args, **kwargs))
            async with page_context as page:
                yield page

    setattr(browser_module, "get_new_page", _tracked_get_new_page)
    setattr(render_module, "get_new_page", _tracked_get_new_page)
    setattr(htmlrender_data_source, "get_new_page", _tracked_get_new_page)
    setattr(browser_module, "_zhenxun_task_tracking_patched", True)


def _patch_htmlrender_shutdown() -> None:
    render_module, browser_module = _load_htmlrender_modules()
    if getattr(browser_module, "_zhenxun_shutdown_patched", False):
        return

    async def _patched_shutdown_browser() -> None:
        if _HTMLRENDER_TASK_TRACKER.is_draining:
            await _HTMLRENDER_TASK_TRACKER.wait_for_idle()
        await _shutdown_browser_instance()

    setattr(browser_module, "shutdown_browser", _patched_shutdown_browser)
    setattr(render_module, "shutdown_browser", _patched_shutdown_browser)
    setattr(browser_module, "_zhenxun_shutdown_patched", True)


def _patch_playwright_env_check_once() -> None:
    _, browser_module = _load_htmlrender_modules()
    _patch_htmlrender_task_tracking()
    _patch_htmlrender_shutdown()
    if getattr(browser_module, "_zhenxun_check_once_patched", False):
        return

    original_check: Callable[..., Awaitable[Any]] | None = None
    check_attr_name = ""
    for attr_name in ("check_playwright_env", "check_browser_env"):
        candidate = getattr(browser_module, attr_name, None)
        if callable(candidate):
            original_check = cast(Callable[..., Awaitable[Any]], candidate)
            check_attr_name = attr_name
            break

    if original_check is None:
        logger.debug(
            "未找到 htmlrender 环境检查函数，跳过 check_once 补丁。",
            "PlaywrightEngine",
        )
        setattr(browser_module, "_zhenxun_check_once_patched", True)
        return

    check_func = original_check
    state: dict[str, Any] = {"checked": False, "result": None}
    check_lock: asyncio.Lock | None = None

    def _is_browser_usable(browser_obj: Any) -> bool:
        if browser_obj is None:
            return False
        is_connected = getattr(browser_obj, "is_connected", None)
        if callable(is_connected):
            with contextlib.suppress(Exception):
                return bool(is_connected())
        # 无法判断连接状态时，保守认为可用
        return True

    def _get_current_browser_candidate() -> Any:
        current = state["result"]
        if _is_browser_usable(current):
            return current
        fallback = getattr(browser_module, "_browser", None)
        if _is_browser_usable(fallback):
            return fallback
        return None

    def _local_browser_executable_exists() -> bool:
        playwright = getattr(browser_module, "_playwright", None)
        plugin_config = getattr(browser_module, "plugin_config", None)
        browser_name = str(getattr(plugin_config, "htmlrender_browser", "chromium"))
        browser_type = getattr(playwright, browser_name, None)
        executable_path = getattr(browser_type, "executable_path", None)
        return bool(executable_path and Path(executable_path).is_file())

    async def _check_once(**kwargs: Any) -> Any:
        nonlocal check_lock
        if state["checked"]:
            cached_browser = _get_current_browser_candidate()
            if cached_browser is not None:
                return cached_browser
            state["checked"] = False
            state["result"] = None

        if check_lock is None:
            check_lock = asyncio.Lock()
        async with check_lock:
            if state["checked"]:
                cached_browser = _get_current_browser_candidate()
                if cached_browser is not None:
                    return cached_browser
                state["checked"] = False
                state["result"] = None

            # start_browser launches this same executable immediately afterwards.
            # Avoid a redundant probe process when Playwright already resolved it.
            result = (
                True
                if _local_browser_executable_exists()
                else await check_func(**kwargs)
            )
            state["checked"] = True
            state["result"] = result

            browser = _get_current_browser_candidate()
            if browser is not None:
                return browser
            return result

    setattr(browser_module, check_attr_name, _check_once)
    setattr(browser_module, "_zhenxun_check_once_patched", True)


class PlaywrightEngine(BaseScreenshotEngine):
    """使用 nonebot-plugin-htmlrender 实现的截图引擎。"""

    _MAX_CONCURRENT_RENDER = 4
    _CONTEXT_POOL_SIZE = 4
    _PREWARM_CONTEXT_COUNT = 2
    _SET_CONTENT_WAIT_UNTIL = "domcontentloaded"
    _READY_STATE_TIMEOUT_MS = 2_000
    _IMAGE_READY_TIMEOUT_MS = 1_800
    _FONT_READY_TIMEOUT_MS = 1_200
    _FULL_PAGE_VIEWPORT_MAX_HEIGHT = 4_096
    _FULL_PAGE_VIEWPORT_MAX_WIDTH = 4_096
    _CLIP_PADDING_DEFAULT = 0
    _DISABLE_ANIMATIONS_STYLE = """
        *, *::before, *::after {
            animation: none !important;
            transition: none !important;
            caret-color: transparent !important;
            scroll-behavior: auto !important;
        }
    """
    _RECENT_RESULT_TTL_SECONDS = 1.5
    _RECENT_RESULT_MAX_ITEMS = 64
    _RSS_RECYCLE_MIN_THRESHOLD_BYTES = 700 * 1024 * 1024
    _RSS_RECYCLE_MAX_THRESHOLD_BYTES = 1200 * 1024 * 1024
    _RSS_RECYCLE_HEADROOM_BYTES = 224 * 1024 * 1024
    _RECYCLE_COOLDOWN_SECONDS = 300
    _RECYCLE_CHECK_EVERY = 8
    _IDLE_CHECK_INTERVAL_SECONDS = 15
    _IDLE_RECYCLE_SECONDS = 180
    _POOL_UNSAFE_OPTION_KEYS: ClassVar[set[str]] = {
        "color_scheme",
        "extra_http_headers",
        "forced_colors",
        "geolocation",
        "has_touch",
        "http_credentials",
        "ignore_https_errors",
        "is_mobile",
        "java_script_enabled",
        "locale",
        "permissions",
        "proxy",
        "record_har_content",
        "record_har_mode",
        "record_har_omit_content",
        "record_har_path",
        "record_video_dir",
        "record_video_size",
        "reduced_motion",
        "screen",
        "service_workers",
        "storage_state",
        "timezone_id",
        "user_agent",
    }
    _POOL_DEVICE_SCALE_FACTOR = 2

    def __init__(self):
        _patch_playwright_env_check_once()
        self._render_semaphore = asyncio.Semaphore(self._MAX_CONCURRENT_RENDER)
        self._debug_console_log = bool(Config.get_config("UI", "DEBUG_MODE", False))
        self._state_lock = asyncio.Lock()
        self._recycle_lock = asyncio.Lock()
        self._active_renders = 0
        self._render_count = 0
        self._recycle_pending = False
        self._last_recycle_at = 0.0
        self._last_render_finished_at = time.monotonic()
        self._rss_baseline_bytes: int | None = None
        self._recent_results: OrderedDict[str, tuple[float, bytes]] = OrderedDict()
        self._inflight_tasks: dict[str, asyncio.Task[bytes]] = {}
        self._generation_counter = 0
        self._active_generation: ContextGeneration | None = None
        self._retiring_generations: list[ContextGeneration] = []
        self._preparing_generations: list[ContextGeneration] = []
        self._idle_recycle_task: asyncio.Task[None] | None = None
        self._closing = False
        self._process = psutil.Process()
        self._preparation_timings: dict[str, float] = {}
        self._preparation_tasks: set[asyncio.Task] = set()

    @staticmethod
    def _normalize_base_url(path: Path) -> str:
        base_url = path.absolute().as_uri()
        if not base_url.endswith("/"):
            base_url += "/"
        return base_url

    @staticmethod
    def _build_render_key(
        html: str, template_path: str, render_options: dict[str, Any]
    ) -> str:
        options_json = json.dumps(render_options, sort_keys=True, default=str)
        hasher = hashlib.sha256()
        hasher.update(template_path.encode("utf-8"))
        hasher.update(b"\0")
        hasher.update(options_json.encode("utf-8"))
        hasher.update(b"\0")
        hasher.update(html.encode("utf-8", errors="ignore"))
        return hasher.hexdigest()

    def _cleanup_recent_results_nolock(self, now: float) -> None:
        while self._recent_results:
            expire_at, _ = next(iter(self._recent_results.values()))
            if expire_at > now:
                break
            self._recent_results.popitem(last=False)
        while len(self._recent_results) > self._RECENT_RESULT_MAX_ITEMS:
            self._recent_results.popitem(last=False)

    def _get_recent_result_nolock(self, key: str, now: float) -> bytes | None:
        entry = self._recent_results.get(key)
        if not entry:
            return None
        expire_at, result = entry
        if expire_at <= now:
            self._recent_results.pop(key, None)
            return None
        self._recent_results.move_to_end(key)
        return result

    def _get_total_rss(self) -> int | None:
        try:
            total_rss = self._process.memory_info().rss
            for child in self._process.children(recursive=True):
                with contextlib.suppress(Exception):
                    total_rss += child.memory_info().rss
            return total_rss
        except Exception:
            return None

    def _update_rss_baseline_nolock(self, current_rss: int) -> None:
        if self._rss_baseline_bytes is None or current_rss < self._rss_baseline_bytes:
            self._rss_baseline_bytes = current_rss
            return

        threshold = self._rss_baseline_bytes + self._RSS_RECYCLE_HEADROOM_BYTES * 2
        if current_rss >= threshold:
            self._rss_baseline_bytes = int(
                self._rss_baseline_bytes * 0.9 + current_rss * 0.1
            )

    def _get_dynamic_threshold_nolock(self, current_rss: int) -> int:
        self._update_rss_baseline_nolock(current_rss)
        baseline = self._rss_baseline_bytes or current_rss
        dynamic = baseline + self._RSS_RECYCLE_HEADROOM_BYTES
        dynamic = max(dynamic, self._RSS_RECYCLE_MIN_THRESHOLD_BYTES)
        dynamic = min(dynamic, self._RSS_RECYCLE_MAX_THRESHOLD_BYTES)
        return dynamic

    def _mark_recycle_if_needed_nolock(self, now: float) -> None:
        if self._render_count % self._RECYCLE_CHECK_EVERY != 0:
            return
        if now - self._last_recycle_at < self._RECYCLE_COOLDOWN_SECONDS:
            return
        current_rss = self._get_total_rss()
        if current_rss is None:
            return
        threshold = self._get_dynamic_threshold_nolock(current_rss)
        if current_rss >= threshold:
            self._recycle_pending = True

    async def get_runtime_snapshot(self) -> dict[str, Any]:
        async with self._state_lock:
            active_generation = (
                self._active_generation.snapshot()
                if self._active_generation is not None
                else None
            )
            retiring_generations = [
                generation.snapshot() for generation in self._retiring_generations
            ]
            return {
                "closing": self._closing,
                "preparation_timings": dict(self._preparation_timings),
                "preparation_task_count": sum(
                    not task.done() for task in self._preparation_tasks
                ),
                "ready_contexts": len(self._active_generation.all_contexts)
                if self._active_generation
                else 0,
                "active_renders": self._active_renders,
                "render_count": self._render_count,
                "recycle_pending": self._recycle_pending,
                "last_recycle_at": self._last_recycle_at,
                "generation_counter": self._generation_counter,
                "active_generation": active_generation,
                "retiring_generations": retiring_generations,
                "retiring_generation_count": len(retiring_generations),
                "preparing_generation_count": len(self._preparing_generations),
                "inflight_task_count": len(self._inflight_tasks),
                "recent_result_count": len(self._recent_results),
                "htmlrender_active_tasks": _HTMLRENDER_TASK_TRACKER.active_tasks,
                "htmlrender_draining": _HTMLRENDER_TASK_TRACKER.is_draining,
            }

    async def _log_runtime_snapshot(self, reason: str) -> None:
        snapshot = await self.get_runtime_snapshot()
        logger.trace(
            f"截图引擎状态快照[{reason}]: {snapshot}",
        )

    def _create_generation_nolock(self) -> ContextGeneration:
        self._generation_counter += 1
        return ContextGeneration(generation_id=self._generation_counter)

    def _ensure_active_generation_nolock(self) -> ContextGeneration:
        if self._active_generation is None:
            self._active_generation = self._create_generation_nolock()
        return self._active_generation

    async def initialize(self) -> None:
        async with self._state_lock:
            if self._idle_recycle_task and not self._idle_recycle_task.done():
                return
            self._closing = False
            self._generation_counter = 0
            self._active_generation = None
            self._retiring_generations.clear()
            self._last_render_finished_at = time.monotonic()
            if current_rss := self._get_total_rss():
                self._rss_baseline_bytes = current_rss
            self._idle_recycle_task = asyncio.create_task(self._idle_recycle_loop())
        await _HTMLRENDER_TASK_TRACKER.reset()
        await self._log_runtime_snapshot("initialize")
        # 浏览器在首次 _acquire_context 时按需启动，无需预热

    async def close(self) -> None:
        idle_task: asyncio.Task[None] | None = None
        async with self._state_lock:
            self._closing = True
            idle_task = self._idle_recycle_task
            self._idle_recycle_task = None
            self._recent_results.clear()
            self._recycle_pending = False

        for task in tuple(self._preparation_tasks):
            await stop_preparation(task)
            self._preparation_tasks.discard(task)
        await _HTMLRENDER_TASK_TRACKER.mark_draining("engine_close")

        if idle_task:
            idle_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await idle_task

        await _HTMLRENDER_TASK_TRACKER.wait_for_idle()

        async with self._state_lock:
            inflight_tasks = list(self._inflight_tasks.values())

        if inflight_tasks:
            await asyncio.gather(*inflight_tasks, return_exceptions=True)

        async with self._state_lock:
            self._inflight_tasks.clear()

        await self._log_runtime_snapshot("close:before_dispose")
        await self._dispose_context_pool()
        await _shutdown_browser_instance()
        await self._log_runtime_snapshot("close:after_shutdown")

    async def _on_render_begin(self) -> None:
        await _HTMLRENDER_TASK_TRACKER.begin("zhenxun_renderer")
        async with self._state_lock:
            self._active_renders += 1

    async def _on_render_end(self) -> None:
        should_recycle = False
        async with self._state_lock:
            self._active_renders = max(0, self._active_renders - 1)
            self._render_count += 1
            now = time.monotonic()
            self._last_render_finished_at = now
            self._mark_recycle_if_needed_nolock(now)
            if self._recycle_pending and _HTMLRENDER_TASK_TRACKER.active_tasks == 0:
                self._recycle_pending = False
                self._last_recycle_at = now
                should_recycle = True
        await _HTMLRENDER_TASK_TRACKER.end()
        if should_recycle:
            await self._recycle_browser("active")

    @staticmethod
    def _build_page_options(
        render_options: dict[str, Any], *, pooled: bool
    ) -> dict[str, Any]:
        options = render_options.copy()
        options.pop("wait", None)
        options.pop("type", None)
        options.pop("quality", None)
        options.pop("scale", None)
        options.pop("screenshot_scale", None)
        options.pop("screenshot_timeout", None)
        options.pop("full_page", None)
        options.pop("clip_selector", None)
        options.pop("clip_padding", None)
        options.pop("disable_animations", None)
        options.pop("diagnostic_template", None)
        if pooled:
            options.pop("base_url", None)
            options.pop("device_scale_factor", None)
        return options

    @staticmethod
    def _build_screenshot_options(render_options: dict[str, Any]) -> dict[str, Any]:
        scale = render_options.get("screenshot_scale", render_options.get("scale"))
        if scale not in ("css", "device"):
            scale = None
        return {
            "full_page": bool(render_options.get("full_page", True)),
            "type": render_options.get("type", "png"),
            "quality": render_options.get("quality"),
            "scale": scale,
            "timeout": render_options.get("screenshot_timeout", 30_000),
        }

    @staticmethod
    def _get_wait_timeout(render_options: dict[str, Any]) -> int:
        wait = render_options.get("wait", 0)
        if isinstance(wait, int):
            return max(wait, 0)
        return 0

    @staticmethod
    def _coerce_non_negative_int(value: Any, default: int = 0) -> int:
        try:
            value_int = int(value)
        except (TypeError, ValueError):
            return default
        return value_int if value_int >= 0 else default

    @classmethod
    def _should_use_context_pool(cls, render_options: dict[str, Any]) -> bool:
        for key in cls._POOL_UNSAFE_OPTION_KEYS:
            if key in render_options:
                return False
        dsf = render_options.get("device_scale_factor")
        if dsf is not None and dsf != cls._POOL_DEVICE_SCALE_FACTOR:
            return False
        return True

    async def _render_with_page(
        self,
        page: Any,
        html: str,
        template_path: str,
        render_options: dict[str, Any],
    ) -> bytes:
        # Finish the base-directory document before observing rendered content.
        # Chromium directory-listing scripts are not part of the template.
        await page.goto(template_path, wait_until="load")
        template = _diagnostic_resource(
            render_options.get("diagnostic_template", template_path)
        )

        def script_error(error):
            name = getattr(error, "name", None)
            category = (
                name
                if isinstance(name, str) and name in _SCRIPT_ERROR_CATEGORIES
                else "ScriptError"
            )
            resource, line, column = _script_error_location(error)
            _render_diagnostic(
                "render_page_script_error",
                template=template,
                category=category,
                resource=resource,
                line=line,
                column=column,
            )

        def request_failed(request):
            if request.resource_type in {"script", "stylesheet"}:
                _render_diagnostic(
                    "render_page_resource_failed",
                    template=template,
                    category=request.resource_type,
                    resource=_diagnostic_resource(request.url),
                )

        def console_message(message):
            # Console text and arguments can contain rendered data or credentials.
            if message.type not in {"warning", "error"}:
                return
            location = message.location
            _render_diagnostic(
                "render_page_console",
                template=template,
                category=f"console_{message.type}",
                resource=_diagnostic_resource(location.get("url")),
                line=location.get("lineNumber", 0) + 1,
                column=location.get("columnNumber", 0) + 1,
            )

        page.on("pageerror", script_error)
        page.on("requestfailed", request_failed)
        if self._debug_console_log:
            page.on("console", console_message)
        try:
            return await self._capture_rendered_page(
                page, html, template_path, render_options
            )
        finally:
            page.remove_listener("pageerror", script_error)
            page.remove_listener("requestfailed", request_failed)
            if self._debug_console_log:
                page.remove_listener("console", console_message)

    async def _capture_rendered_page(self, page, html, template_path, render_options):
        await page.set_content(html, wait_until=self._SET_CONTENT_WAIT_UNTIL)
        if bool(render_options.get("disable_animations", False)):
            await self._disable_page_animations(page)
        await self._wait_for_visual_stability(page)
        if wait_ms := self._get_wait_timeout(render_options):
            await page.wait_for_timeout(wait_ms)
        screenshot_options = self._build_screenshot_options(render_options)
        clip_selector = render_options.get("clip_selector")
        if isinstance(clip_selector, str) and clip_selector.strip():
            if image_bytes := await self._capture_by_selector(
                page,
                selector=clip_selector.strip(),
                screenshot_options=screenshot_options,
                clip_padding=self._coerce_non_negative_int(
                    render_options.get("clip_padding"),
                    self._CLIP_PADDING_DEFAULT,
                ),
            ):
                return image_bytes
        await self._optimize_full_page_capture(page, screenshot_options)
        return await page.screenshot(**screenshot_options)

    async def _disable_page_animations(self, page: Any) -> None:
        with contextlib.suppress(Exception):
            await page.add_style_tag(content=self._DISABLE_ANIMATIONS_STYLE)

    async def _capture_by_selector(
        self,
        page: Any,
        selector: str,
        screenshot_options: dict[str, Any],
        clip_padding: int,
    ) -> bytes | None:
        element = await page.query_selector(selector)
        if element is None:
            return None

        element_screenshot_options = {
            "type": screenshot_options.get("type", "png"),
            "quality": screenshot_options.get("quality"),
            "timeout": screenshot_options.get("timeout", 30_000),
        }
        with contextlib.suppress(Exception):
            box = await element.bounding_box()
            if box and clip_padding > 0:
                viewport = page.viewport_size or {}
                width = int(viewport.get("width") or 0)
                if width > 0:
                    target_height = int(box["y"] + box["height"] + clip_padding)
                    current_height = int(viewport.get("height") or 0)
                    if target_height > current_height:
                        await page.set_viewport_size(
                            {"width": width, "height": target_height}
                        )

        if clip_padding <= 0:
            return await element.screenshot(**element_screenshot_options)

        with contextlib.suppress(Exception):
            clip_box = await element.bounding_box()
            if clip_box is None:
                return await element.screenshot(**element_screenshot_options)
            clip = {
                "x": max(clip_box["x"] - clip_padding, 0),
                "y": max(clip_box["y"] - clip_padding, 0),
                "width": clip_box["width"] + clip_padding * 2,
                "height": clip_box["height"] + clip_padding * 2,
            }
            page_options = {
                "type": screenshot_options.get("type", "png"),
                "quality": screenshot_options.get("quality"),
                "timeout": screenshot_options.get("timeout", 30_000),
                "clip": clip,
            }
            return await page.screenshot(**page_options)

        return await element.screenshot(**element_screenshot_options)

    async def _wait_for_visual_stability(self, page: Any) -> None:
        # 先做一次快速预检，判断页面是否有外部图片和自定义字体
        resource_hints: dict[str, bool] | None = None
        with contextlib.suppress(Exception):
            resource_hints = await page.evaluate(
                """
                () => {
                    const imgs = document.images || [];
                    let hasUnloadedImages = false;
                    for (let i = 0; i < imgs.length; i++) {
                        if (!imgs[i].complete) { hasUnloadedImages = true; break; }
                    }
                    const hasCustomFonts = !!(
                        document.fonts && document.fonts.size > 0
                    );
                    return {
                        ready: document.readyState === 'complete',
                        images: hasUnloadedImages,
                        fonts: hasCustomFonts,
                    };
                }
                """
            )

        # 如果预检已知全部就绪，直接返回
        if (
            isinstance(resource_hints, dict)
            and resource_hints.get("ready") is True
            and resource_hints.get("images") is not True
            and resource_hints.get("fonts") is not True
        ):
            return

        # 对需要的等待项并行执行
        waiters: list[Coroutine[Any, Any, None]] = []

        need_ready = not (
            isinstance(resource_hints, dict) and resource_hints.get("ready") is True
        )
        need_images = (
            not isinstance(resource_hints, dict) or resource_hints.get("images") is True
        )
        need_fonts = (
            not isinstance(resource_hints, dict) or resource_hints.get("fonts") is True
        )

        if need_ready:
            waiters.append(self._wait_ready_state(page))
        if need_images:
            waiters.append(self._wait_images_loaded(page))
        if need_fonts:
            waiters.append(self._wait_fonts_ready(page))

        if waiters:
            await asyncio.gather(*waiters)

    async def _wait_ready_state(self, page: Any) -> None:
        with contextlib.suppress(Exception):
            await page.wait_for_function(
                "() => document.readyState === 'complete'",
                timeout=self._READY_STATE_TIMEOUT_MS,
            )

    async def _wait_images_loaded(self, page: Any) -> None:
        with contextlib.suppress(Exception):
            await page.wait_for_function(
                "() => Array.from(document.images || []).every(img => img.complete)",
                timeout=self._IMAGE_READY_TIMEOUT_MS,
            )

    async def _wait_fonts_ready(self, page: Any) -> None:
        with contextlib.suppress(Exception):
            await page.evaluate(
                """
                async (timeoutMs) => {
                    if (!document.fonts || !document.fonts.ready) return;
                    await Promise.race([
                        document.fonts.ready,
                        new Promise(resolve => setTimeout(resolve, timeoutMs)),
                    ]);
                }
                """,
                self._FONT_READY_TIMEOUT_MS,
            )

    async def _optimize_full_page_capture(
        self, page: Any, screenshot_options: dict[str, Any]
    ) -> None:
        if not bool(screenshot_options.get("full_page")):
            return

        viewport = page.viewport_size or {}
        width = viewport.get("width")
        height = viewport.get("height")
        if (
            not isinstance(width, int)
            or width <= 0
            or not isinstance(height, int)
            or height <= 0
        ):
            return

        with contextlib.suppress(Exception):
            content_size = await page.evaluate(
                """
                () => {
                    const body = document.body;
                    const doc = document.documentElement;
                    const bodyWidth = body ? Math.max(
                        body.scrollWidth,
                        body.offsetWidth,
                        body.clientWidth
                    ) : 0;
                    const bodyHeight = body ? Math.max(
                        body.scrollHeight,
                        body.offsetHeight,
                        body.clientHeight
                    ) : 0;
                    const docWidth = doc ? Math.max(
                        doc.scrollWidth,
                        doc.offsetWidth,
                        doc.clientWidth
                    ) : 0;
                    const docHeight = doc ? Math.max(
                        doc.scrollHeight,
                        doc.offsetHeight,
                        doc.clientHeight
                    ) : 0;
                    return {
                        width: Math.ceil(Math.max(bodyWidth, docWidth, 10)),
                        height: Math.ceil(Math.max(bodyHeight, docHeight, 10)),
                    };
                }
                """
            )
            if not isinstance(content_size, dict):
                return

            content_width = content_size.get("width")
            content_height = content_size.get("height")
            if not isinstance(content_width, int) or not isinstance(
                content_height, int
            ):
                return
            if content_width < 10 or content_height < 10:
                return

            target_width = min(
                max(width, content_width), self._FULL_PAGE_VIEWPORT_MAX_WIDTH
            )
            target_height = max(height, content_height)
            await page.set_viewport_size(
                {"width": target_width, "height": target_height}
            )
            screenshot_options["full_page"] = False

    async def _render_with_oneoff_page(
        self,
        html: str,
        template_path: str,
        render_options: dict[str, Any],
    ) -> bytes:
        browser = await _get_browser_instance()
        page_options = self._build_page_options(render_options, pooled=False)
        page = await browser.new_page(**page_options)
        try:
            return await self._render_with_page(
                page, html, template_path, render_options
            )
        finally:
            with contextlib.suppress(Exception):
                await page.close()

    async def _dispose_generation(self, generation: ContextGeneration) -> None:
        contexts = list(generation.all_contexts)
        while True:
            try:
                generation.context_pool.get_nowait()
            except asyncio.QueueEmpty:
                break

        failures = []
        for context in contexts:
            try:
                await context.close()
            except Exception as error:
                failures.append(error)
            else:
                generation.all_contexts.discard(context)
        if failures:
            raise RuntimeError("renderer_context_cleanup_failed") from failures[0]

    async def _cleanup_retiring_generations(self) -> None:
        async with self._state_lock:
            disposable = [
                generation
                for generation in self._retiring_generations
                if generation.active_leases <= 0
            ]
        for generation in disposable:
            await self._dispose_generation(generation)
            async with self._state_lock:
                if generation in self._retiring_generations:
                    self._retiring_generations.remove(generation)

    async def _dispose_context_pool(self) -> None:
        async with self._state_lock:
            generations = [*self._retiring_generations, *self._preparing_generations]
            if self._active_generation is not None:
                generations.append(self._active_generation)
        for generation in generations:
            await self._dispose_generation(generation)
            async with self._state_lock:
                if self._active_generation is generation:
                    self._active_generation = None
                if generation in self._retiring_generations:
                    self._retiring_generations.remove(generation)
                if generation in self._preparing_generations:
                    self._preparing_generations.remove(generation)

    async def _build_generation(self, *, strict: bool = False) -> ContextGeneration:
        async with self._state_lock:
            if self._closing:
                raise RuntimeError("renderer_closing")
            generation = self._create_generation_nolock()
            self._preparing_generations.append(generation)
        tasks: list[asyncio.Task] = []
        started = time.monotonic()
        try:
            browser = await _get_browser_instance()
            if not browser.is_connected():
                raise RuntimeError("renderer_browser_disconnected")
            self._preparation_timings["browser_start_ms"] = (
                time.monotonic() - started
            ) * 1000
            semaphore = asyncio.Semaphore(2)

            async def prepare_context() -> None:
                async with semaphore:
                    if self._closing:
                        raise RuntimeError("renderer_closing")
                    context = await browser.new_context(
                        viewport={"width": 800, "height": 10}, device_scale_factor=2
                    )
                    generation.all_contexts.add(context)
                    page = await context.new_page()
                    try:
                        await page.goto("about:blank", wait_until="domcontentloaded")
                        await page.set_content(
                            "<html><body></body></html>", wait_until="domcontentloaded"
                        )
                    finally:
                        await page.close()
                    generation.context_pool.put_nowait(context)

            started = time.monotonic()
            count = max(1, min(self._PREWARM_CONTEXT_COUNT, self._CONTEXT_POOL_SIZE))
            tasks = [
                asyncio.create_task(prepare_context(), name="renderer-prewarm-context")
                for _ in range(count)
            ]
            self._preparation_tasks.update(tasks)
            await asyncio.gather(*tasks)
            if self._closing or not browser.is_connected():
                raise RuntimeError("renderer_browser_disconnected")
            self._preparation_timings["context_prewarm_ms"] = (
                time.monotonic() - started
            ) * 1000
            return generation
        except BaseException:
            for task in tasks:
                if not task.done():
                    task.cancel()
            for task in tasks:
                await stop_preparation(task)
            await self._dispose_generation(generation)
            if generation in self._preparing_generations:
                self._preparing_generations.remove(generation)
            raise
        finally:
            self._preparation_tasks.difference_update(
                task for task in tasks if task.done()
            )

    async def _swap_generation(self, reason: str, *, strict: bool = False) -> None:
        new_generation = await self._build_generation(strict=strict)
        async with self._state_lock:
            if self._closing:
                await self._dispose_generation(new_generation)
                if new_generation in self._preparing_generations:
                    self._preparing_generations.remove(new_generation)
                raise RuntimeError("renderer_closing")
            old_generation = self._active_generation
            if old_generation is not None:
                old_generation.retiring = True
                self._retiring_generations.append(old_generation)
            self._active_generation = new_generation
            self._preparing_generations.remove(new_generation)

        await self._cleanup_retiring_generations()
        logger.debug(
            f"截图引擎触发代际切换({reason})，新代={new_generation.generation_id}",
            "PlaywrightEngine",
        )
        await self._log_runtime_snapshot(f"swap_generation:{reason}")

    async def warmup(self) -> None:
        async with self._recycle_lock:
            async with self._state_lock:
                active = self._active_generation
                if active is not None and active.all_contexts:
                    return
            await self._swap_generation("startup_warmup", strict=True)

    async def _acquire_context(self) -> tuple[ContextGeneration, Any]:
        generation: ContextGeneration | None = None
        create_new = False
        async with self._state_lock:
            generation = self._ensure_active_generation_nolock()
            try:
                context = generation.context_pool.get_nowait()
                generation.active_leases += 1
                return generation, context
            except asyncio.QueueEmpty:
                create_new = len(generation.all_contexts) < self._CONTEXT_POOL_SIZE

        if create_new:
            browser = await _get_browser_instance()
            context = await browser.new_context(
                viewport={"width": 800, "height": 10},
                device_scale_factor=2,
            )
            async with self._state_lock:
                target_generation = generation
                if target_generation.retiring and self._active_generation is not None:
                    target_generation = self._active_generation
                target_generation.all_contexts.add(context)
                target_generation.active_leases += 1
                return target_generation, context

        context = await generation.context_pool.get()
        async with self._state_lock:
            generation.active_leases += 1
        return generation, context

    async def _release_context(
        self,
        generation: ContextGeneration,
        context: Any,
        broken: bool = False,
    ) -> None:
        should_discard = broken
        async with self._state_lock:
            generation.active_leases = max(0, generation.active_leases - 1)
            if self._closing or generation.retiring:
                should_discard = True
            elif context not in generation.all_contexts:
                should_discard = True
            elif not should_discard:
                generation.context_pool.put_nowait(context)

        if should_discard:
            await self._discard_context(generation, context)

        await self._cleanup_retiring_generations()

    async def _discard_context(
        self, generation: ContextGeneration, context: Any
    ) -> None:
        async with self._state_lock:
            existed = context in generation.all_contexts
            if existed:
                generation.all_contexts.remove(context)
        if existed:
            with contextlib.suppress(Exception):
                await context.close()

    async def _render_with_context_pool(
        self,
        html: str,
        template_path: str,
        render_options: dict[str, Any],
    ) -> bytes:
        last_error: Exception | None = None
        for attempt in range(2):
            generation, context = await self._acquire_context()
            page = None
            broken = False
            try:
                page = await context.new_page()
                page_options = self._build_page_options(render_options, pooled=True)
                viewport = page_options.get("viewport")
                if isinstance(viewport, dict):
                    width = viewport.get("width")
                    height = viewport.get("height")
                    if isinstance(width, int) and isinstance(height, int):
                        await page.set_viewport_size({"width": width, "height": height})
                return await self._render_with_page(
                    page, html, template_path, render_options
                )
            except Exception as e:
                broken = True
                last_error = e
                if attempt == 0:
                    if _is_playwright_target_closed_error(e):
                        logger.warning(
                            "截图引擎浏览器上下文代已失效，切换新代后重试一次。",
                            "PlaywrightEngine",
                            e=e,
                        )
                        try:
                            await self._swap_generation("target_closed")
                        except Exception:
                            raise e
                    else:
                        logger.warning(
                            "截图引擎上下文已失效，丢弃后重试一次。",
                            "PlaywrightEngine",
                            e=e,
                        )
                    continue
                raise
            finally:
                if page is not None:
                    with contextlib.suppress(Exception):
                        await page.close()
                await self._release_context(generation, context, broken=broken)

        if last_error is not None:
            raise last_error
        raise RuntimeError("截图引擎上下文池渲染失败。")

    async def _render_html(
        self,
        html: str,
        template_path: str,
        render_options: dict[str, Any],
    ) -> bytes:
        if self._should_use_context_pool(render_options):
            return await self._render_with_context_pool(
                html, template_path, render_options
            )
        return await self._render_with_oneoff_page(html, template_path, render_options)

    async def _recycle_browser(self, reason: str) -> None:
        async with self._recycle_lock:
            try:
                await self._swap_generation(reason)
                current_rss = self._get_total_rss()
                if current_rss is not None:
                    self._update_rss_baseline_nolock(current_rss)
                await self._log_runtime_snapshot(f"recycle:{reason}")
            except Exception as e:
                logger.warning("浏览器实例重建失败。", "PlaywrightEngine", e=e)

    async def _prewarm_browser_and_pool(self) -> None:
        if self._closing:
            return
        async with self._state_lock:
            has_active_generation = self._active_generation is not None
        if has_active_generation:
            return

        generation = await self._build_generation()
        dispose_generation = False
        async with self._state_lock:
            if self._closing:
                dispose_generation = True
            elif self._active_generation is None:
                self._active_generation = generation
                self._preparing_generations.remove(generation)
                return
            else:
                dispose_generation = True

        if dispose_generation:
            await self._dispose_generation(generation)
            if generation in self._preparing_generations:
                self._preparing_generations.remove(generation)

    async def _idle_recycle_loop(self) -> None:
        while True:
            await asyncio.sleep(self._IDLE_CHECK_INTERVAL_SECONDS)
            should_recycle = False
            async with self._state_lock:
                if self._closing:
                    return
                now = time.monotonic()
                if _HTMLRENDER_TASK_TRACKER.active_tasks > 0:
                    continue
                if now - self._last_recycle_at < self._RECYCLE_COOLDOWN_SECONDS:
                    continue
                idle_for = now - self._last_render_finished_at
                if idle_for < self._IDLE_RECYCLE_SECONDS:
                    continue
                current_rss = self._get_total_rss()
                if current_rss is None:
                    continue
                threshold = self._get_dynamic_threshold_nolock(current_rss)
                if current_rss >= threshold:
                    self._last_recycle_at = now
                    should_recycle = True
            if should_recycle:
                await self._recycle_browser("idle")

    async def _render_and_store_result(
        self,
        key: str,
        html: str,
        base_url_for_browser: str,
        render_options: dict[str, Any],
    ) -> bytes:
        async with self._render_semaphore:
            await self._on_render_begin()
            try:
                result = await self._render_html(
                    html,
                    base_url_for_browser,
                    render_options,
                )
            finally:
                await self._on_render_end()

        async with self._state_lock:
            now = time.monotonic()
            self._recent_results[key] = (
                now + self._RECENT_RESULT_TTL_SECONDS,
                result,
            )
            self._recent_results.move_to_end(key)
            self._cleanup_recent_results_nolock(now)
        return result

    async def render(self, html: str, base_url_path: Path, **render_options) -> bytes:
        if self._closing or _HTMLRENDER_TASK_TRACKER.is_draining:
            raise RuntimeError("截图引擎正在排空/关闭，暂不接受新的渲染任务。")

        base_url_for_browser = self._normalize_base_url(base_url_path)

        final_render_options = {
            "viewport": {"width": 800, "height": 10},
            **render_options,
            "base_url": base_url_for_browser,
        }

        dedupe_key = self._build_render_key(
            html,
            base_url_for_browser,
            final_render_options,
        )

        owner = False
        async with self._state_lock:
            now = time.monotonic()
            self._cleanup_recent_results_nolock(now)
            if cached_result := self._get_recent_result_nolock(dedupe_key, now):
                return cached_result

            task = self._inflight_tasks.get(dedupe_key)
            if task is None:
                task = asyncio.create_task(
                    self._render_and_store_result(
                        dedupe_key,
                        html,
                        base_url_for_browser,
                        final_render_options,
                    )
                )
                self._inflight_tasks[dedupe_key] = task
                owner = True

        try:
            return await task
        finally:
            if owner:
                async with self._state_lock:
                    if self._inflight_tasks.get(dedupe_key) is task:
                        self._inflight_tasks.pop(dedupe_key, None)


async def stop_preparation(task: asyncio.Task | None) -> None:
    if task is None or task is asyncio.current_task():
        return
    if task.done():
        if not task.cancelled():
            task.exception()
        return
    from zhenxun.services.lifecycle.deadline import remaining_timeout

    task.cancel()
    _, pending = await asyncio.wait({task}, timeout=remaining_timeout(5.0))
    if pending:
        raise TimeoutError("renderer_preparation_cleanup_timeout")
    if not task.cancelled():
        task.exception()


class EngineManager:
    """One shared, owned initialization and warmup per engine generation."""

    def __init__(self):
        self._engine_class: type[BaseScreenshotEngine] = PlaywrightEngine
        self._instance: BaseScreenshotEngine | None = None
        self._init_task: asyncio.Task | None = None
        self._warmup_task: asyncio.Task | None = None
        self._closing = False
        self._state = "not_started"

    def reopen(self) -> None:
        if self._instance is not None or any(
            task and not task.done() for task in (self._init_task, self._warmup_task)
        ):
            if self._closing:
                raise RuntimeError("renderer_cleanup_pending")
            return
        self._closing = False
        self._state = "not_started"
        self._init_task = self._warmup_task = None

    async def _initialize(self) -> BaseScreenshotEngine:
        self._state = "initializing"
        engine = self._engine_class()
        self._instance = engine
        try:
            await engine.initialize()
            return engine
        except BaseException:
            self._state = "failed"
            raise

    async def get_engine(self) -> BaseScreenshotEngine:
        if self._closing:
            raise RuntimeError("renderer_closing")
        if self._init_task is None:
            self._init_task = asyncio.create_task(
                self._initialize(), name="renderer-initialize"
            )
        return await asyncio.shield(self._init_task)

    async def get_runtime_snapshot(self) -> dict[str, Any]:
        engine = self._instance
        snapshot = (
            await engine.get_runtime_snapshot()
            if isinstance(engine, PlaywrightEngine)
            else {}
        )
        return {
            **snapshot,
            "initialization_state": self._state,
            "closing": self._closing or snapshot.get("closing", False),
        }

    async def _prepare(self) -> None:
        try:
            engine = await self.get_engine()
            self._state = "warming"
            if isinstance(engine, PlaywrightEngine):
                await engine.warmup()
            if self._closing:
                raise RuntimeError("renderer_closing")
            self._state = "ready"
        except BaseException:
            self._state = "failed"
            raise

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
        self._state = "closing"
        await stop_preparation(self._warmup_task)
        await stop_preparation(self._init_task)
        if self._instance is not None:
            await self._instance.close()
            self._instance = None
        self._init_task = self._warmup_task = None
        self._state = "closed"


engine_manager = EngineManager()
