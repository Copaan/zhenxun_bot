from typing import Any
from urllib.parse import quote

from zhenxun.services.ai.config import get_llm_config
from zhenxun.services.ai.config.presets import detect_preset
from zhenxun.services.ai.core.exceptions import LLMException
from zhenxun.services.ai.tools.core.decorators import tool
from zhenxun.services.ai.tools.core.toolkit import BaseToolkit
from zhenxun.services.ai.tools.models import ToolResult
from zhenxun.services.ai.utils.logger import log_tool as logger
from zhenxun.services.network_proxy import ManagedAsyncClient

from .native import Native

_REUSE_KEY = ("deepseek", "kimi", "glm", "jina")
"""可以复用同厂商服务商密钥的搜索服务"""
_FETCH = ("deepseek", "kimi", "jina")
"""提供网页读取的搜索服务，其余服务读取网页时使用 Jina Reader"""
_SNIPPET_LIMIT = 500
_FETCH_LIMIT = 8000


def _search_service(
    vendor: str | None = None,
) -> tuple[str, str | None, str | None] | None:
    """确定搜索服务与密钥

    参数:
        vendor: 指定服务，为空时使用配置的服务，未配置时取第一个可复用密钥的服务商

    返回:
        tuple | None: (服务, 密钥, 接口域名)，没有可用服务时为 None
    """
    settings = get_llm_config().web_search
    vendor = vendor or settings.provider
    if vendor and settings.api_key and vendor == settings.provider:
        return vendor, settings.api_key, None
    for provider in get_llm_config().providers:
        preset = detect_preset(provider.base_url)
        if preset in _REUSE_KEY and provider.api_keys and preset == (vendor or preset):
            host = provider.base_url.split("://", 1)[-1].split("/", 1)[0]
            return preset, provider.api_keys[0], host
    if vendor == "jina":
        return vendor, None, None
    return None


def _clip(text: Any, limit: int) -> str:
    text = str(text or "").strip()
    return text if len(text) <= limit else f"{text[:limit]}…"


def _format_results(items: list[tuple[Any, Any, Any, Any]]) -> str:
    """把 (标题, 链接, 摘要, 日期) 列表格式化为文本"""
    if not items:
        return "没有找到相关结果。"
    lines = []
    for index, (title, url, snippet, date) in enumerate(items, 1):
        head = f"{index}. {title or url}"
        if date:
            head += f" ({date})"
        lines.append(f"{head}\n   链接: {url}\n   {_clip(snippet, _SNIPPET_LIMIT)}")
    return "\n".join(lines)


async def _request(
    method: str, url: str, key: str | None, json: dict | None = None
) -> Any:
    headers = {"Accept": "application/json"}
    if key:
        headers["Authorization"] = f"Bearer {key}"
    async with ManagedAsyncClient(timeout=30) as client:
        response = await client.request(method, url, json=json, headers=headers)
    if response.status_code >= 400:
        raise LLMException(f"HTTP {response.status_code}: {_clip(response.text, 300)}")
    return response.json()


async def search_web(query: str, count: int | None = None) -> str:
    """调用搜索服务搜索网页

    参数:
        query: 搜索关键词
        count: 结果数量，为空时使用配置

    返回:
        str: 格式化的搜索结果
    """
    service = _search_service()
    if service is None:
        raise LLMException("尚未配置联网搜索服务 (AI.web_search)")
    vendor, key, host = service
    count = count or get_llm_config().web_search.max_results
    logger.info(f"联网搜索 [{vendor}]: {query}")
    if vendor == "deepseek":
        data = await _request(
            "POST",
            f"https://{host or 'api.deepseek.com'}/v1/tools/search",
            key,
            {"query": query[:128], "count": count},
        )
        items = [
            (r.get("title"), r.get("url"), r.get("summary") or r.get("snippet"), None)
            for r in data.get("results") or []
        ]
    elif vendor == "kimi":
        data = await _request(
            "POST",
            f"https://{host or 'api.moonshot.cn'}/v1/tools/search",
            key,
            {"text_query": query, "limit": min(count, 20)},
        )
        items = [
            (r.get("title"), r.get("url"), r.get("snippet"), r.get("date"))
            for r in data.get("search_results") or []
        ]
    elif vendor == "glm":
        data = await _request(
            "POST",
            "https://open.bigmodel.cn/api/paas/v4/web_search",
            key,
            {
                "search_query": query[:70],
                "search_engine": "search_std",
                "search_intent": False,
                "count": count,
            },
        )
        items = [
            (r.get("title"), r.get("link"), r.get("content"), r.get("publish_date"))
            for r in data.get("search_result") or []
        ]
    elif vendor == "jina":
        if not key:
            raise LLMException("Jina 搜索需要 API Key")
        data = await _request("GET", f"https://s.jina.ai/?q={quote(query)}", key)
        items = [
            (
                r.get("title"),
                r.get("url"),
                r.get("description") or r.get("content"),
                r.get("date"),
            )
            for r in (data.get("data") or [])[:count]
        ]
    elif vendor == "tavily":
        data = await _request(
            "POST",
            "https://api.tavily.com/search",
            key,
            {"query": query, "max_results": count, "search_depth": "basic"},
        )
        items = [
            (r.get("title"), r.get("url"), r.get("content"), r.get("published_date"))
            for r in data.get("results") or []
        ]
    else:
        data = await _request(
            "POST",
            "https://api.bochaai.com/v1/web-search",
            key,
            {"query": query, "count": count, "summary": False},
        )
        pages = ((data.get("data") or {}).get("webPages") or {}).get("value") or []
        items = [
            (
                r.get("name") or r.get("title"),
                r.get("url"),
                r.get("snippet"),
                r.get("datePublished"),
            )
            for r in pages
        ]
    return _format_results(items)


async def fetch_web(url: str) -> str:
    """通过搜索服务读取网页正文，网页由服务端抓取

    参数:
        url: 网页地址

    返回:
        str: 网页标题与正文 (Markdown)
    """
    if not url.startswith(("http://", "https://")):
        raise LLMException("只能读取 http/https 网页")
    service = _search_service()
    if service is None or service[0] not in _FETCH:
        service = _search_service("jina")
    vendor, key, host = service or ("jina", None, None)
    logger.info(f"读取网页 [{vendor}]: {url}")
    if vendor == "deepseek":
        data = await _request(
            "POST",
            f"https://{host or 'api.deepseek.com'}/v1/tools/fetch",
            key,
            {"url": url},
        )
        title, content = data.get("title"), data.get("content")
    elif vendor == "kimi":
        data = await _request(
            "POST",
            f"https://{host or 'api.moonshot.cn'}/v1/tools/fetch",
            key,
            {"url": url},
        )
        title, content = data.get("title"), data.get("markdown")
    else:
        data = (await _request("GET", f"https://r.jina.ai/{url}", key)).get("data")
        data = data or {}
        title, content = data.get("title"), data.get("content")
    if not content:
        return "网页没有可读取的正文。"
    return f"# {title or url}\n\n{_clip(content, _FETCH_LIMIT)}"


class WebSearchToolkit(BaseToolkit):
    """联网搜索工具箱：通过 AI.web_search 配置的搜索服务搜索网页、读取网页正文"""

    default_instructions = (
        "需要实时、最新或你不确定的信息时，使用 `web_search` 搜索；"
        "需要阅读某个网页的完整内容时，使用 `web_fetch`。回答时注明信息来源链接。"
    )

    @tool(name="web_search")
    async def web_search(self, query: str) -> ToolResult:
        """
        搜索互联网，返回相关网页的标题、链接与摘要。

        Args:
            query: 搜索关键词，简洁明确。
        """
        try:
            return ToolResult(output=await search_web(query))
        except Exception as e:
            logger.warning(f"联网搜索失败: {e}")
            return ToolResult(output=f"搜索失败: {e}").as_error()

    @tool(name="web_fetch")
    async def web_fetch(self, url: str) -> ToolResult:
        """
        读取网页正文 (Markdown)，用于查看搜索结果或用户给出的链接的完整内容。

        Args:
            url: 网页地址，以 http:// 或 https:// 开头。
        """
        try:
            return ToolResult(output=await fetch_web(url))
        except Exception as e:
            logger.warning(f"读取网页失败: {e}")
            return ToolResult(output=f"读取失败: {e}").as_error()


def web_tools(model: str | None = None) -> list[Any]:
    """按 AI.web_search 配置为模型选择联网搜索工具

    auto 时模型的全部线路都支持原生网页搜索则用原生搜索，否则用搜索服务工具箱；
    native 只用原生搜索；api 只用搜索服务；off 不提供。

    参数:
        model: 模型全名或路由组名，为空时使用默认对话模型

    返回:
        list[Any]: 传给 tools 的工具列表，可能为空
    """
    from zhenxun.services.ai.llm.manager import resolve_routes
    from zhenxun.services.ai.llm.protocols import native_tool_allowed

    mode = get_llm_config().web_search.mode
    if mode == "off":
        return []
    if mode == "native":
        return [Native.web_search()]
    if mode == "api":
        return [WebSearchToolkit()]
    try:
        support = [native_tool_allowed(r, "web_search") for r in resolve_routes(model)]
    except LLMException:
        support = []
    if support and all(support):
        return [Native.web_search()]
    if _search_service():
        return [WebSearchToolkit()]
    if any(support):
        return [Native.web_search()]
    logger.debug(f"模型 {model or '默认对话模型'} 没有可用的联网搜索方式")
    return []
