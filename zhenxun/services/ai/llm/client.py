"""
LLM 请求执行

解析线路 -> 编码 -> 选择密钥 -> 发送 (重试 / 密钥轮换 / 线路故障转移) -> 解码。
"""

import asyncio
from dataclasses import dataclass
import email.utils
import json
import re
import ssl
import time
from typing import Any

import httpx

from zhenxun.services.ai.config import PRESETS, ModelConfig, ProviderConfig
from zhenxun.services.ai.config.manager import get_llm_config
from zhenxun.services.ai.core.exceptions import (
    AuthenticationException,
    ConfigurationException,
    ContentFilteredException,
    ContextLengthExceededException,
    InvalidRequestException,
    LLMException,
    LocationNotSupportedException,
    NetworkTimeoutException,
    PermissionDeniedException,
    QuotaExceededException,
    RateLimitException,
    ResponseParseException,
    UpstreamServerException,
)
from zhenxun.services.ai.core.models import CancellationToken, ModelProfile
from zhenxun.services.ai.core.options import CommonLLMConfig, GenerationConfig
from zhenxun.services.ai.utils.logger import log_llm as logger
from zhenxun.services.lifecycle.deadline import remaining_timeout
from zhenxun.services.network_proxy import ManagedAsyncClient, ProxyRequestError
from zhenxun.utils.log_sanitizer import sanitize_for_logging
from zhenxun.utils.pydantic_compat import dump_json_safely
from zhenxun.utils.user_agent import get_user_agent

from .health import health_manager, key_id
from .protocols import HttpCall, get_task, native_tool_allowed, resolve_tool_choice

_COMMON_FIELDS = set(CommonLLMConfig.model_fields)
_SANITIZE_CONTEXT = {
    "openai_chat": "openai",
    "openai_responses": "openai_responses",
    "gemini": "gemini",
}


@dataclass
class Route:
    """一条可调用的线路：提供商 + 模型 + 能力档案"""

    provider: ProviderConfig
    model: ModelConfig
    profile: ModelProfile

    @property
    def name(self) -> str:
        return f"{self.provider.name}/{self.model.name}"

    @property
    def protocol(self) -> str:
        return self.model.protocol or self.provider.protocol

    @property
    def base_url(self) -> str:
        return (self.model.base_url or self.provider.base_url).rstrip("/")

    @property
    def preset(self) -> dict[str, Any]:
        return PRESETS.get(self.provider.preset or "", {})

    def generation_config(self, override: GenerationConfig | None) -> GenerationConfig:
        """合并提供商参数、模型参数与请求配置，后者优先

        参数:
            override: 请求携带的生成配置

        返回:
            GenerationConfig: 合并后的配置
        """
        merged = GenerationConfig()
        for params in (self.provider.params, self.model.params):
            if not params:
                continue
            common = {k: v for k, v in params.items() if k in _COMMON_FIELDS}
            extra = {k: v for k, v in params.items() if k not in _COMMON_FIELDS}
            layer = GenerationConfig(common=CommonLLMConfig(**common), params=extra)
            merged = merged.merge_with(layer)
        return merged.merge_with(override)


def join_url(base_url: str, path: str) -> str:
    """拼接接口地址与路径"""
    return f"{base_url.rstrip('/')}/{path.lstrip('/')}" if path else base_url


class LLMHttpClient:
    """LLM 请求专用的 HTTP 客户端，连接尚未建立时允许重连一次"""

    def __init__(self, timeout: float = 120):
        self.timeout = timeout
        self._client: httpx.AsyncClient | None = None

    def _ensure_client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            self._client = ManagedAsyncClient(
                headers=get_user_agent(),
                limits=httpx.Limits(max_connections=100, max_keepalive_connections=20),
                timeout=httpx.Timeout(self.timeout),
                follow_redirects=True,
            )
        return self._client

    async def request(self, method: str, url: str, **kwargs: Any) -> httpx.Response:
        """发送请求，受当前生命周期剩余时间约束"""
        from zhenxun.services.ai.chat_switch import require_chat_enabled

        require_chat_enabled()
        client = self._ensure_client()
        timeout = kwargs.get("timeout") or self.timeout
        if isinstance(timeout, httpx.Timeout):
            timeout = max(v or self.timeout for v in timeout.as_dict().values())
        try:
            return await asyncio.wait_for(
                self._request_with_recovery(client, method, url, kwargs),
                remaining_timeout(float(timeout)),
            )
        except asyncio.TimeoutError as error:
            raise httpx.ReadTimeout("LLM request budget exhausted") from error

    async def _request_with_recovery(self, client, method, url, kwargs):
        # httpcore 的 trace 能区分“TCP 连接失败且尚未发送请求头”，
        # 只有这种情况可以安全重连；已复用连接上的读写失败不能重放。
        extensions = dict(kwargs.pop("extensions", {}) or {})
        original_trace = extensions.get("trace")
        application_started = False
        connect_failed = False

        async def trace(event, info):
            nonlocal application_started, connect_failed
            if event.endswith("send_request_headers.started"):
                request = info.get("request")
                if getattr(request, "method", None) != b"CONNECT":
                    application_started = True
            if event == "connection.connect_tcp.failed":
                connect_failed = True
            if original_trace is not None:
                await original_trace(event, info)

        extensions["trace"] = trace
        async with client.proxy_runtime.request_scope() as scope:
            for attempt in range(2):
                connect_failed = False
                try:
                    return await client.request(
                        method, url, extensions=extensions, **kwargs
                    )
                except (
                    ProxyRequestError,
                    httpx.ConnectError,
                    httpx.ConnectTimeout,
                ) as error:
                    cause = (
                        error.__cause__
                        if isinstance(error, ProxyRequestError)
                        else error
                    )
                    seen = set()
                    tls_failure = False
                    current = cause
                    while current is not None and id(current) not in seen:
                        seen.add(id(current))
                        tls_failure |= isinstance(current, ssl.SSLError)
                        current = current.__cause__ or current.__context__
                    retry = (
                        attempt == 0
                        and connect_failed
                        and not application_started
                        and not tls_failure
                        and isinstance(cause, httpx.ConnectError | httpx.ConnectTimeout)
                        and not client.proxy_runtime.stopping
                        and client.proxy_runtime.current is scope.generation
                    )
                    if not retry:
                        raise
                    await asyncio.sleep(0.2)
                    if (
                        client.proxy_runtime.stopping
                        or client.proxy_runtime.current is not scope.generation
                    ):
                        raise
                    logger.debug("AI 连接尚未建立，重新连接一次")

    async def close(self) -> None:
        """关闭连接池"""
        if self._client and not self._client.is_closed:
            await self._client.aclose()
        self._client = None


_clients: dict[tuple[str, int], LLMHttpClient] = {}


def get_http_client(provider: ProviderConfig) -> LLMHttpClient:
    """获取提供商共享的 HTTP 客户端"""
    key = (provider.name, provider.timeout)
    if key not in _clients:
        _clients[key] = LLMHttpClient(provider.timeout)
    return _clients[key]


async def close_http_clients() -> None:
    """关闭全部 HTTP 客户端"""
    clients = list(_clients.values())
    _clients.clear()
    await asyncio.gather(*(c.close() for c in clients), return_exceptions=True)


def auth_headers(protocol: str, api_key: str) -> dict[str, str]:
    """生成协议对应的认证请求头"""
    if protocol == "anthropic":
        return {"x-api-key": api_key, "anthropic-version": "2023-06-01"}
    if protocol == "gemini":
        return {"x-goog-api-key": api_key}
    return {"Authorization": f"Bearer {api_key}"}


def _retry_after(response: httpx.Response, text: str) -> float | None:
    value = response.headers.get("retry-after")
    if value:
        try:
            return max(float(value), 0.0)
        except ValueError:
            parsed = email.utils.parsedate_to_datetime(value)
            return max(parsed.timestamp() - time.time(), 0.0)
    if match := re.search(r'"retryDelay"\s*:\s*"(\d+(?:\.\d+)?)s"', text):
        return float(match.group(1))
    return None


def _error_message(text: str) -> tuple[str, str]:
    try:
        data = json.loads(text)
    except ValueError:
        return text[:500], ""
    if isinstance(data, list) and data:
        data = data[0]
    if not isinstance(data, dict):
        return text[:500], ""
    error = data.get("error", data)
    if isinstance(error, dict):
        message = error.get("message") or error.get("msg") or text[:500]
        status = error.get("status") or error.get("code") or error.get("type") or ""
        return str(message), str(status)
    return str(error), ""


def map_http_error(response: httpx.Response, route_name: str) -> LLMException:
    """把 HTTP 错误响应映射为统一异常

    参数:
        response: 状态码 >= 400 的响应
        route_name: 线路名称

    返回:
        LLMException: 对应的异常
    """
    status = response.status_code
    text = response.content.decode("utf-8", errors="ignore")
    message, code = _error_message(text)
    lower = f"{message} {code} {text[:2000]}".lower()
    details = {"status_code": status, "response": text[:1000], "route": route_name}
    prefix = f"[{route_name}] HTTP {status}: {message}"
    if "location is not supported" in lower or (
        status in (400, 403) and "failed_precondition" in lower
    ):
        return LocationNotSupportedException(prefix, details)
    if status == 401 or "api_key_invalid" in lower or "api key not valid" in lower:
        return AuthenticationException(prefix, details)
    if status == 403:
        if "country" in lower or "region" in lower or "unsupported" in lower:
            return LocationNotSupportedException(prefix, details)
        return PermissionDeniedException(prefix, details)
    if status == 404:
        return ConfigurationException(prefix, details)
    if status == 402:
        return QuotaExceededException(prefix, details)
    if status == 413 or re.search(
        r"context.?length|maximum context|too many tokens|too long|exceeds max length",
        lower,
    ):
        return ContextLengthExceededException(prefix, details)
    if status == 429:
        details["retry_after"] = _retry_after(response, text)
        if re.search(r"per.?minute|perminute|rate", lower):
            return RateLimitException(prefix, details)
        if re.search(r"quota|insufficient|balance|billing|余额|欠费|exhausted", lower):
            return QuotaExceededException(prefix, details)
        return RateLimitException(prefix, details)
    if status >= 500:
        return UpstreamServerException(prefix, details)
    if re.search(r"content.?filter|safety|sensitive|敏感|违规", lower):
        return ContentFilteredException(prefix, details)
    return InvalidRequestException(prefix, details)


async def send(
    route: Route,
    call: HttpCall,
    api_key: str,
    token: CancellationToken | None = None,
) -> Any:
    """发送一次 HTTP 请求并返回 JSON 或二进制响应

    参数:
        route: 线路
        call: 编码后的请求
        api_key: 使用的密钥
        token: 取消令牌

    返回:
        Any: JSON 响应为 dict，raw 请求为 httpx.Response
    """
    url = join_url(call.base_url or route.base_url, call.path)
    headers = {
        **auth_headers(route.protocol, api_key),
        **route.provider.headers,
        **call.headers,
    }
    context = _SANITIZE_CONTEXT.get(route.protocol, route.protocol)
    if logger.is_enabled("debug"):
        body = sanitize_for_logging(
            call.json or call.data, context=f"{context}_request"
        )
        logger.debug(f"请求 {route.name} {call.method} {url}")
        logger.debug(f"请求体: {dump_json_safely(body, ensure_ascii=False)}")
    kwargs: dict[str, Any] = {"headers": headers, "timeout": route.provider.timeout}
    if call.json is not None:
        kwargs["json"] = call.json
    if call.data is not None:
        kwargs["data"] = call.data
    if call.files is not None:
        kwargs["files"] = call.files
    request = asyncio.ensure_future(
        get_http_client(route.provider).request(call.method, url, **kwargs)
    )
    if token:
        token.link_future(request)
    try:
        response = await request
    except httpx.TimeoutException as e:
        raise NetworkTimeoutException(f"[{route.name}] 请求超时: {e}") from e
    except httpx.HTTPError as e:
        raise NetworkTimeoutException(f"[{route.name}] 网络请求失败: {e!r}") from e
    if response.status_code >= 400:
        raise map_http_error(response, route.name)
    if call.raw:
        return response
    try:
        payload = response.json()
    except ValueError as e:
        raise ResponseParseException(
            f"[{route.name}] 响应不是有效的 JSON",
            details={"response": response.text[:500]},
        ) from e
    if logger.is_enabled("debug"):
        cleaned = sanitize_for_logging(payload, context=f"{context}_response")
        logger.debug(f"响应: {dump_json_safely(cleaned, ensure_ascii=False)}")
    return payload


def _no_key_error(route: Route) -> LLMException:
    provider = route.provider
    recovery = health_manager.key_recovery_time(provider.name, provider.api_keys)
    if recovery is None:
        return AuthenticationException(f"[{route.name}] 没有可用的 API 密钥")
    wait = max(int(recovery - time.time()), 1)
    return RateLimitException(
        f"[{route.name}] 所有密钥都在冷却中，约 {wait} 秒后恢复",
        details={"retry_after": wait},
    )


async def call_route(
    route: Route,
    task: str,
    request: Any,
    token: CancellationToken | None = None,
) -> Any:
    """在单条线路上执行请求，处理同线路重试、能力降级与密钥轮换

    参数:
        route: 线路
        task: 任务类型
        request: 统一请求对象
        token: 取消令牌

    返回:
        Any: 统一响应对象
    """
    from .capability import adapt_route, observe_response

    build, parse = get_task(route.protocol, task)
    call = await build(route, request)
    settings = get_llm_config().client_settings
    cooldowns = {"rate": settings.rate_limit_cooldown, "quota": settings.quota_cooldown}
    provider = route.provider
    tried: set[str] = set()
    api_key = health_manager.pick_key(provider.name, provider.api_keys, tried)
    if api_key is None:
        raise _no_key_error(route)
    attempt = 0
    adaptations = 0
    while True:
        if token:
            token.raise_if_cancelled()
        try:
            payload = await send(route, call, api_key, token)
            response = parse(route, request, payload)
        except LLMException as e:
            adapted = (
                adapt_route(route, request, call.json or {}, e)
                if adaptations < 3
                else None
            )
            if adapted is not None:
                adaptations += 1
                route = adapted
                call = await build(route, request)
                continue
            others = health_manager.available_keys(
                provider.name, provider.api_keys, tried | {key_id(api_key)}
            )
            if (
                e.is_retryable
                and attempt < settings.max_retries
                and not (e.should_rotate_key and others)
            ):
                attempt += 1
                delay = e.details.get("retry_after") or settings.retry_delay * (
                    2 ** (attempt - 1)
                )
                logger.debug(f"{route.name} 第 {attempt} 次重试，{delay:.1f} 秒后: {e}")
                await asyncio.sleep(min(float(delay), 30.0))
                continue
            health_manager.record_key_failure(provider.name, api_key, e, cooldowns)
            if e.should_rotate_key:
                tried.add(key_id(api_key))
                api_key = health_manager.pick_key(
                    provider.name, provider.api_keys, tried
                )
                if api_key is not None:
                    logger.info(f"{route.name} 切换密钥后重试: {e}")
                    continue
            raise
        health_manager.record_key_success(provider.name, api_key)
        observe_response(route, request, response)
        return response


def request_needs(request: Any) -> set[str]:
    """对话请求需要线路具备的能力

    参数:
        request: 统一请求对象

    返回:
        set[str]: image (图片输入) / tools (函数工具) / web_search (原生网页搜索)
    """
    from zhenxun.services.ai.core.messages import ChatRequest, ImagePart

    if not isinstance(request, ChatRequest):
        return set()
    needs = set()
    if any(
        isinstance(part, ImagePart)
        for message in request.messages
        for part in message.content
    ):
        needs.add("image")
    tools = request.tools or []
    items = tools.values() if isinstance(tools, dict) else tools
    for tool in items:
        if getattr(tool, "execution_side", "client") != "server":
            if resolve_tool_choice(request.tool_choice, "AUTO")[0] != "none":
                needs.add("tools")
        elif getattr(tool, "type_id", "") == "web_search":
            needs.add("web_search")
    return needs


def unmet_needs(route: Route, needs: set[str]) -> set[str]:
    """线路不具备的请求能力

    参数:
        route: 线路
        needs: 请求需要的能力

    返回:
        set[str]: 线路缺少的能力
    """
    checks = {
        "image": lambda: route.profile.accepts_input("image"),
        "tools": lambda: route.profile.tools,
        "web_search": lambda: native_tool_allowed(route, "web_search"),
    }
    return {need for need in needs if not checks[need]()}


def _order_routes(routes: list[Route], order: list[int], request: Any) -> list[int]:
    """按请求需要的能力排列候选线路：能力齐全的优先，有其他线路可用时
    跳过不支持函数调用的线路"""
    needs = request_needs(request)
    if not needs:
        return order
    missing = {index: unmet_needs(routes[index], needs) for index in order}
    if any("tools" not in missing[index] for index in order):
        order = [index for index in order if "tools" not in missing[index]]
    ranked = sorted(order, key=lambda index: len(missing[index]))
    if ranked[0] != order[0]:
        logger.debug(
            f"请求需要 {'、'.join(sorted(needs))}，优先使用 {routes[ranked[0]].name}"
        )
    return ranked


async def execute(
    task: str,
    routes: list[Route],
    request: Any,
    token: CancellationToken | None = None,
) -> Any:
    """按顺序在多条线路上执行请求，失败时故障转移

    参数:
        task: 任务类型
        routes: 候选线路
        request: 统一请求对象 (extra 中记录上次成功的线路序号)
        token: 取消令牌

    返回:
        Any: 统一响应对象
    """
    from zhenxun.services.ai.chat_switch import admitted_ai_request

    route_cooldown = get_llm_config().client_settings.route_cooldown
    grouped = len(routes) > 1
    start = request.extra.get("_working_route_index", 0) % len(routes)
    order = [*range(start, len(routes)), *range(start)]
    if task == "chat":
        order = _order_routes(routes, order, request)
    errors: list[str] = []
    last_error: LLMException | None = None

    async def attempt(index: int) -> Any:
        route = routes[index]
        started = time.monotonic()
        try:
            response = await call_route(route, task, request, token)
        except LLMException as e:
            local = isinstance(e, ConfigurationException) and not e.details.get(
                "status_code"
            )
            if (
                e.should_failover
                and not local
                and not isinstance(e, ContextLengthExceededException)
            ):
                health_manager.record_route_failure(route.name, e, route_cooldown)
            raise
        health_manager.record_route_success(route.name, time.monotonic() - started)
        request.extra["_working_route_index"] = index
        return response

    with admitted_ai_request():
        attempted = False
        for index in order:
            route = routes[index]
            if grouped and not health_manager.route_available(route.name):
                errors.append(f"{route.name}(熔断中)")
                continue
            attempted = True
            try:
                return await attempt(index)
            except LLMException as e:
                if not grouped or not e.should_failover:
                    raise
                logger.warning(f"线路 {route.name} 失败，切换下一条: {e}")
                errors.append(f"{route.name}({type(e).__name__})")
                last_error = e
        if not attempted:
            fallback = health_manager.best_fallback([r.name for r in routes])
            logger.warning(f"全部线路都在熔断中，强制探测 {fallback}")
            index = next(i for i, r in enumerate(routes) if r.name == fallback)
            return await attempt(index)
    raise UpstreamServerException(
        f"所有线路均失败: {', '.join(errors)}", cause=last_error
    ) from last_error
