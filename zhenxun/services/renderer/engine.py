import asyncio
from collections import OrderedDict
from collections.abc import AsyncIterator, Awaitable, Callable, Coroutine
import contextlib
import hashlib
from importlib import import_module
import inspect
import json
from pathlib import Path
import re
import subprocess
import sys
import time
from typing import Any, ClassVar, cast
from urllib.parse import unquote, urlsplit

import nonebot

from zhenxun.configs.config import Config
from zhenxun.services.lifecycle.deadline import remaining_timeout, shutdown_budget
from zhenxun.services.log import logger

from .resources import browser_gone, browser_resources
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


def _isolate_playwright_signals() -> None:
    if sys.platform != "win32":
        return
    import playwright._impl._transport as transport

    if getattr(transport.asyncio, "_zx_process_group", False):
        return
    original = transport.asyncio

    class PlaywrightAsyncio:
        _zx_process_group = True

        def __getattr__(self, name):
            return getattr(original, name)

        async def create_subprocess_exec(self, *args, **kwargs):
            # CTRL_BREAK to the business worker must not kill the Node driver
            # before its owner has closed pages and the Playwright transport.
            kwargs["creationflags"] = (
                kwargs.get("creationflags", 0) | subprocess.CREATE_NEW_PROCESS_GROUP
            )
            return await original.create_subprocess_exec(*args, **kwargs)

    transport.asyncio = PlaywrightAsyncio()


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
        self._open = asyncio.Event()
        self._open.set()
        self.last_finished = time.monotonic()
        """最近一次任务结束的时间"""

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
        while True:
            await self._open.wait()
            async with self._lock:
                if self._draining:
                    reason = self._drain_reason or "unknown"
                    message = (
                        "htmlrender 正在排空，拒绝新的渲染任务: "
                        f"owner={owner}, reason={reason}"
                    )
                    raise RuntimeError(message)
                if not self._open.is_set():
                    continue
                self._active_tasks += 1
                self._idle_event.clear()
                return

    async def end(self) -> None:
        self._active_tasks = max(0, self._active_tasks - 1)
        self.last_finished = time.monotonic()
        if self._active_tasks == 0:
            self._idle_event.set()

    async def pause(self) -> bool:
        """没有进行中的任务时暂停接受新任务，新任务等待 unpause 后开始

        返回:
            bool: 是否已暂停
        """
        async with self._lock:
            if self._active_tasks or self._draining:
                return False
            self._open.clear()
            return True

    def unpause(self) -> None:
        self._open.set()

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


def _browser_running() -> bool:
    browser = getattr(htmlrender_browser, "_browser", None)
    with contextlib.suppress(Exception):
        return browser is not None and bool(browser.is_connected())
    return False


_browser_cleanup_task: asyncio.Task | None = None
_browser_cleanup_identity: tuple | None = None


async def _shutdown_browser_instance() -> None:
    global _browser_cleanup_task, _browser_cleanup_identity
    if htmlrender_browser is None:
        return
    identity = (
        getattr(htmlrender_browser, "_browser", None),
        getattr(htmlrender_browser, "_playwright", None),
    )
    if identity == (None, None):
        return
    retry_failed = (
        _browser_cleanup_task is not None
        and _browser_cleanup_task.done()
        and (
            _browser_cleanup_task.cancelled()
            or _browser_cleanup_task.exception() is not None
        )
    )
    if _browser_cleanup_identity != identity or retry_failed:
        if _browser_cleanup_task is not None and not _browser_cleanup_task.done():
            await asyncio.shield(_browser_cleanup_task)
        _browser_cleanup_identity = identity
        _browser_cleanup_task = asyncio.create_task(
            _close_browser_resources(*identity), name="renderer-browser-cleanup"
        )
    await asyncio.shield(_browser_cleanup_task)


async def _close_browser_resources(browser_obj, playwright_obj) -> None:
    if htmlrender_browser is None:
        return
    loop = asyncio.get_running_loop()
    _arm_disconnect_exception_suppression(loop)

    if browser_obj is None and playwright_obj is None:
        return
    failures = []
    close_func = getattr(browser_obj, "close", None) if browser_obj else None
    disconnected = browser_obj is not None and not browser_obj.is_connected()
    if callable(close_func) and not disconnected:
        try:
            await _await_if_needed(close_func())
        except Exception as error:
            if browser_obj.is_connected() and not browser_gone(error):
                failures.append(error)
    stop_func = getattr(playwright_obj, "stop", None) if playwright_obj else None
    if callable(stop_func):
        try:
            await _await_if_needed(stop_func())
        except Exception as error:
            if not browser_gone(error):
                failures.append(error)
    if failures:
        raise RuntimeError("renderer_browser_cleanup_failed") from failures[0]
    browser_resources.confirm_browser_closed(browser_obj)
    if getattr(htmlrender_browser, "_browser", None) is browser_obj:
        setattr(htmlrender_browser, "_browser", None)
    if getattr(htmlrender_browser, "_playwright", None) is playwright_obj:
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
    _isolate_playwright_signals()
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
    _RECENT_RESULT_MAX_BYTES = 64 * 1024 * 1024
    _IDLE_CHECK_INTERVAL_SECONDS = 30
    _IDLE_CLOSE_DEFAULT_MINUTES = 10
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
        self._warmup_lock = asyncio.Lock()
        self._context_changed = asyncio.Event()
        self._active_renders = 0
        self._render_count = 0
        self._recent_results: OrderedDict[str, tuple[float, bytes]] = OrderedDict()
        self._inflight_tasks: dict[str, asyncio.Task[bytes]] = {}
        self._render_waiters: dict[asyncio.Task, int] = {}
        self._close_task: asyncio.Task | None = None
        self._pool: asyncio.LifoQueue[Any] = asyncio.LifoQueue()
        """空闲的上下文"""
        self._contexts: set[Any] = set()
        """上下文池拥有的全部上下文，含借出中的"""
        self._pool_browser: Any | None = None
        """上下文池所属的浏览器"""
        self._leases = 0
        self._pending_creations = 0
        self._idle_task: asyncio.Task[None] | None = None
        self._closing = False
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
        for key, (expire_at, _) in tuple(self._recent_results.items()):
            if expire_at <= now:
                self._recent_results.pop(key)
        size = sum(len(value) for _, value in self._recent_results.values())
        while (
            len(self._recent_results) > self._RECENT_RESULT_MAX_ITEMS
            or size > self._RECENT_RESULT_MAX_BYTES
        ):
            _, (_, value) = self._recent_results.popitem(last=False)
            size -= len(value)

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

    async def get_runtime_snapshot(self) -> dict[str, Any]:
        async with self._state_lock:
            return {
                "closing": self._closing,
                "preparation_timings": dict(self._preparation_timings),
                "preparation_task_count": sum(
                    not task.done() for task in self._preparation_tasks
                ),
                "browser_running": _browser_running(),
                "ready_contexts": self._pool.qsize(),
                "context_count": len(self._contexts),
                "active_leases": self._leases,
                "active_renders": self._active_renders,
                "render_count": self._render_count,
                "idle_seconds": round(
                    time.monotonic() - _HTMLRENDER_TASK_TRACKER.last_finished
                ),
                "inflight_task_count": len(self._inflight_tasks),
                "recent_result_count": len(self._recent_results),
                "recent_result_bytes": sum(
                    len(value) for _, value in self._recent_results.values()
                ),
                "browser_resources": browser_resources.snapshot(),
                "htmlrender_active_tasks": _HTMLRENDER_TASK_TRACKER.active_tasks,
                "htmlrender_draining": _HTMLRENDER_TASK_TRACKER.is_draining,
            }

    async def _log_runtime_snapshot(self, reason: str) -> None:
        snapshot = await self.get_runtime_snapshot()
        logger.trace(
            f"截图引擎状态快照[{reason}]: {snapshot}",
        )

    async def initialize(self) -> None:
        async with self._state_lock:
            if self._idle_task and not self._idle_task.done():
                return
            self._closing = False
            self._idle_task = asyncio.create_task(
                self._idle_close_loop(), name="renderer-idle-close"
            )
        await _HTMLRENDER_TASK_TRACKER.reset()
        await self._log_runtime_snapshot("initialize")

    async def close(self) -> None:
        if self._close_task is None:
            self._close_task = browser_resources.spawn(
                self._close_bounded(), name="renderer-engine-close"
            )
        await asyncio.shield(self._close_task)

    async def _close_bounded(self) -> None:
        with shutdown_budget(15.0):
            await self._close_once()

    async def _close_once(self) -> None:
        idle_task: asyncio.Task[None] | None = None
        async with self._state_lock:
            self._closing = True
            self._context_changed.set()
            idle_task = self._idle_task
            self._idle_task = None
            self._recent_results.clear()

        await _HTMLRENDER_TASK_TRACKER.mark_draining("engine_close")
        failures = []
        for task in tuple(self._preparation_tasks):
            try:
                await asyncio.wait_for(stop_preparation(task), remaining_timeout(10.0))
            except Exception as error:
                failures.append(error)
            if task.done():
                self._preparation_tasks.discard(task)

        if idle_task:
            idle_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await idle_task

        try:
            await asyncio.wait_for(
                _HTMLRENDER_TASK_TRACKER.wait_for_idle(), remaining_timeout(10.0)
            )
        except Exception as error:
            failures.append(error)

        async with self._state_lock:
            inflight_tasks = list(self._inflight_tasks.values())

        if inflight_tasks:
            try:
                await asyncio.wait_for(
                    asyncio.shield(
                        asyncio.gather(*inflight_tasks, return_exceptions=True)
                    ),
                    remaining_timeout(10.0),
                )
            except Exception as error:
                failures.append(error)

        await self._log_runtime_snapshot("close:before_dispose")

        async def contexts():
            with shutdown_budget(10.0):
                results = await asyncio.gather(
                    self._dispose_pool(),
                    browser_resources.close_all(),
                    return_exceptions=True,
                )
                for result in results:
                    if isinstance(result, BaseException):
                        raise result

        for operation in (contexts, _shutdown_browser_instance):
            try:
                await asyncio.wait_for(operation(), remaining_timeout(10.0))
            except Exception as error:
                failures.append(error)
        try:
            await browser_resources.drain()
        except Exception as error:
            failures.append(error)
        if (
            browser_resources.contexts
            or _HTMLRENDER_TASK_TRACKER.active_tasks
            or any(not task.done() for task in self._inflight_tasks.values())
            or any(not task.done() for task in self._preparation_tasks)
            or any(
                task is not asyncio.current_task() and not task.done()
                for task in browser_resources.tasks
            )
        ):
            raise RuntimeError("renderer_resource_release_unconfirmed") from (
                failures[0] if failures else None
            )
        if (
            failures
            and htmlrender_browser is not None
            and any(
                getattr(htmlrender_browser, name, None) is not None
                for name in ("_browser", "_playwright")
            )
        ):
            raise RuntimeError("renderer_browser_cleanup_failed") from failures[0]
        self._inflight_tasks.clear()
        await self._log_runtime_snapshot("close:after_shutdown")

    async def _on_render_begin(self) -> None:
        await _HTMLRENDER_TASK_TRACKER.begin("zhenxun_renderer")
        self._active_renders += 1

    async def _on_render_end(self) -> None:
        await _HTMLRENDER_TASK_TRACKER.end()
        self._active_renders = max(0, self._active_renders - 1)
        self._render_count += 1

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
        context = await browser_resources.create_context(
            browser, owner="renderer_oneoff", **page_options
        )
        page = None
        failed = False
        try:
            page = await context.new_page()
            return await self._render_with_page(
                page, html, template_path, render_options
            )
        except BaseException:
            failed = True
            raise
        finally:
            try:
                await browser_resources.cleanup(
                    browser_resources.finish_page(page, context, retain=False),
                    name="renderer-oneoff-cleanup",
                )
            except Exception:
                if not failed:
                    raise

    def _drop_pool_nolock(self) -> list[Any]:
        """清空上下文池，返回需要关闭的上下文，借出中的上下文归还时关闭"""
        stale = list(self._contexts)
        self._contexts.clear()
        while not self._pool.empty():
            self._pool.get_nowait()
        self._pool_browser = None
        self._context_changed.set()
        return stale

    async def _dispose_pool(self) -> None:
        async with self._state_lock:
            stale = self._drop_pool_nolock()
        with shutdown_budget(10.0):
            results = await asyncio.gather(
                *(browser_resources.close_context(context) for context in stale),
                return_exceptions=True,
            )
        if any(isinstance(result, BaseException) for result in results):
            raise RuntimeError("renderer_context_cleanup_failed")

    async def warmup(self) -> None:
        """启动浏览器并预建上下文，使首次渲染无需等待浏览器启动"""
        async with self._warmup_lock:
            async with self._state_lock:
                if self._closing:
                    raise RuntimeError("renderer_closing")
                if self._contexts:
                    return
            await self._prewarm()

    async def _prewarm(self) -> None:
        started = time.monotonic()
        await _get_browser_instance()
        self._preparation_timings["browser_start_ms"] = (
            time.monotonic() - started
        ) * 1000
        started = time.monotonic()
        tasks = [
            asyncio.create_task(self._prewarm_context(), name="renderer-prewarm")
            for _ in range(min(self._PREWARM_CONTEXT_COUNT, self._CONTEXT_POOL_SIZE))
        ]
        self._preparation_tasks.update(tasks)
        try:
            await asyncio.gather(*tasks)
        except BaseException:
            for task in tasks:
                await stop_preparation(task)
            raise
        finally:
            self._preparation_tasks.difference_update(
                task for task in tasks if task.done()
            )
        self._preparation_timings["context_prewarm_ms"] = (
            time.monotonic() - started
        ) * 1000

    async def _prewarm_context(self) -> None:
        context = await self._acquire_context()
        page = None
        ready = False
        try:
            page = await context.new_page()
            await page.goto("about:blank", wait_until="domcontentloaded")
            await page.set_content(
                "<html><body></body></html>", wait_until="domcontentloaded"
            )
            ready = True
        finally:
            await self._finish_pooled_page(context, page, broken=not ready)

    async def _acquire_context(self) -> Any:
        """借出上下文，池空且未满时新建，浏览器已更换时先清空旧池

        返回:
            Any: 浏览器上下文
        """
        while True:
            browser = await _get_browser_instance()
            stale: list[Any] = []
            context = None
            create = False
            async with self._state_lock:
                if self._closing:
                    raise RuntimeError("renderer_closing")
                if browser is not self._pool_browser:
                    stale = self._drop_pool_nolock()
                    self._pool_browser = browser
                if not self._pool.empty():
                    context = self._pool.get_nowait()
                    self._leases += 1
                elif (
                    len(self._contexts) + self._pending_creations
                    < self._CONTEXT_POOL_SIZE
                ):
                    self._pending_creations += 1
                    create = True
                else:
                    self._context_changed.clear()
            for item in stale:
                browser_resources.spawn(
                    browser_resources.close_context(item),
                    name="renderer-stale-context-cleanup",
                )
            if context is not None:
                return context
            if create:
                break
            await self._context_changed.wait()

        context = None
        try:
            context = await browser_resources.create_context(
                browser,
                owner="renderer_pool",
                viewport={"width": 800, "height": 10},
                device_scale_factor=self._POOL_DEVICE_SCALE_FACTOR,
            )
        finally:
            async with self._state_lock:
                self._pending_creations -= 1
                self._context_changed.set()
                usable = (
                    context is not None
                    and not self._closing
                    and browser is self._pool_browser
                )
                if usable:
                    self._contexts.add(context)
                    self._leases += 1
            if context is not None and not usable:
                browser_resources.spawn(
                    browser_resources.close_context(context),
                    name="renderer-unused-context-cleanup",
                )
        if not usable:
            raise RuntimeError("renderer_closing")
        return context

    async def _release_context(self, context: Any, *, reusable: bool) -> None:
        async with self._state_lock:
            self._leases = max(0, self._leases - 1)
            keep = reusable and not self._closing and context in self._contexts
            if keep:
                self._pool.put_nowait(context)
            else:
                self._contexts.discard(context)
            self._context_changed.set()
        if not keep:
            await browser_resources.close_context(context)

    async def _finish_pooled_page(self, context, page, broken) -> None:
        with shutdown_budget(10.0):
            try:
                reusable = await browser_resources.finish_page(
                    page, context, retain=not broken
                )
            except BaseException:
                # 关闭失败的上下文仍由 browser_resources 持有，不再放回池中
                async with self._state_lock:
                    self._leases = max(0, self._leases - 1)
                    self._contexts.discard(context)
                    self._context_changed.set()
                raise
            await self._release_context(context, reusable=reusable)

    async def _render_with_context_pool(
        self,
        html: str,
        template_path: str,
        render_options: dict[str, Any],
    ) -> bytes:
        last_error: Exception | None = None
        for attempt in range(2):
            context = await self._acquire_context()
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
            except asyncio.CancelledError:
                broken = True
                raise
            except Exception as e:
                broken = True
                last_error = e
                if attempt == 0:
                    if _is_playwright_target_closed_error(e):
                        logger.warning(
                            "截图引擎浏览器上下文已失效，清空上下文池后重试一次。",
                            "PlaywrightEngine",
                            e=e,
                        )
                        async with self._state_lock:
                            stale = self._drop_pool_nolock()
                        for item in stale:
                            browser_resources.spawn(
                                browser_resources.close_context(item),
                                name="renderer-stale-context-cleanup",
                            )
                    else:
                        logger.warning(
                            "截图引擎上下文已失效，丢弃后重试一次。",
                            "PlaywrightEngine",
                            e=e,
                        )
                    continue
                raise
            finally:
                try:
                    await browser_resources.cleanup(
                        self._finish_pooled_page(context, page, broken),
                        name="renderer-pooled-page-cleanup",
                    )
                except Exception:
                    if not broken:
                        raise

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

    async def _idle_close_loop(self) -> None:
        """浏览器空闲超过 UI.BROWSER_IDLE_CLOSE 分钟后关闭，下次渲染时重新启动"""
        while True:
            await asyncio.sleep(self._IDLE_CHECK_INTERVAL_SECONDS)
            now = time.monotonic()
            async with self._state_lock:
                if self._closing:
                    return
                self._cleanup_recent_results_nolock(now)
            try:
                minutes = float(
                    Config.get_config(
                        "UI", "BROWSER_IDLE_CLOSE", self._IDLE_CLOSE_DEFAULT_MINUTES
                    )
                )
            except (TypeError, ValueError):
                minutes = self._IDLE_CLOSE_DEFAULT_MINUTES
            idle = now - _HTMLRENDER_TASK_TRACKER.last_finished
            if minutes <= 0 or idle < minutes * 60 or not _browser_running():
                continue
            try:
                if await self._close_idle_browser():
                    logger.info(
                        f"浏览器已空闲 {idle / 60:.0f} 分钟，已关闭以释放资源",
                        "PlaywrightEngine",
                    )
            except Exception as e:
                logger.warning("关闭空闲浏览器失败", "PlaywrightEngine", e=e)

    async def _close_idle_browser(self) -> bool:
        """没有进行中的渲染、且浏览器上的上下文都属于上下文池时关闭浏览器

        关闭期间新的渲染任务等待关闭完成后重新启动浏览器。

        返回:
            bool: 是否已关闭
        """
        browser = getattr(htmlrender_browser, "_browser", None)
        if browser is None or not await _HTMLRENDER_TASK_TRACKER.pause():
            return False
        try:
            async with self._state_lock:
                if (
                    self._closing
                    or self._leases
                    or self._pending_creations
                    or self._preparation_tasks
                    or not set(browser.contexts) <= self._contexts
                ):
                    return False
                stale = self._drop_pool_nolock()
            await asyncio.gather(
                *(browser_resources.close_context(context) for context in stale),
                return_exceptions=True,
            )
            await _shutdown_browser_instance()
            return True
        finally:
            _HTMLRENDER_TASK_TRACKER.unpause()

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
            if len(result) <= self._RECENT_RESULT_MAX_BYTES and not self._closing:
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

        async with self._state_lock:
            if self._closing or _HTMLRENDER_TASK_TRACKER.is_draining:
                raise RuntimeError("renderer_closing")
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

                def finished(done: asyncio.Task) -> None:
                    if self._inflight_tasks.get(dedupe_key) is done:
                        self._inflight_tasks.pop(dedupe_key, None)
                    if not done.cancelled():
                        done.exception()

                task.add_done_callback(finished)
            self._render_waiters[task] = self._render_waiters.get(task, 0) + 1

        try:
            return await asyncio.shield(task)
        finally:
            remaining = self._render_waiters[task] - 1
            if remaining:
                self._render_waiters[task] = remaining
            else:
                self._render_waiters.pop(task, None)
                if not task.done():
                    task.cancel()
                    try:
                        await asyncio.wait_for(
                            asyncio.shield(task), remaining_timeout(10.0)
                        )
                    except (asyncio.CancelledError, Exception):
                        pass


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
    """截图引擎的共享初始化与预热"""

    def __init__(self):
        self._engine_class: type[BaseScreenshotEngine] = PlaywrightEngine
        self._instance: BaseScreenshotEngine | None = None
        self._init_task: asyncio.Task | None = None
        self._warmup_task: asyncio.Task | None = None
        self._closing = False
        self._state = "not_started"
        self._last_error: str | None = None

    def reopen(self) -> None:
        if self._instance is not None or any(
            task and not task.done() for task in (self._init_task, self._warmup_task)
        ):
            if self._closing:
                raise RuntimeError("renderer_cleanup_pending")
            return
        self._closing = False
        self._state = "not_started"
        self._last_error = None
        self._init_task = self._warmup_task = None

    def _record_failure(self, phase: str, error: BaseException) -> None:
        # Keep diagnostics useful without disclosing browser URLs or launch arguments.
        reason = str(error)
        if not re.fullmatch(r"renderer_[a-z_]+", reason):
            reason = type(error).__name__
        self._last_error = f"{phase}:{reason}"
        self._state = "failed"

    async def _initialize(self) -> BaseScreenshotEngine:
        self._state = "initializing"
        try:
            engine = self._engine_class()
            self._instance = engine
            await engine.initialize()
            return engine
        except BaseException as error:
            self._record_failure("initialize", error)
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
            "initialization_error": self._last_error,
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
            self._last_error = None
        except BaseException as error:
            if self._last_error is None:
                self._record_failure("warmup", error)
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
