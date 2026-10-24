"""
模型能力实测

记录配置后核验与运行时观察到的能力 (data/ai/model_observations.json)，
接口拒绝某项能力时降级重试；用户强制开启的能力只降级本次请求并警告。
"""

from copy import deepcopy
import json
import re
import time
from typing import Any

from zhenxun.configs.path_config import DATA_PATH
from zhenxun.services.ai.config import ModelConfig, ProviderConfig
from zhenxun.services.ai.config.presets import detect_preset
from zhenxun.services.ai.core.exceptions import (
    InvalidRequestException,
    LLMException,
    UpstreamServerException,
)
from zhenxun.services.ai.core.messages import ChatRequest, ImagePart
from zhenxun.services.ai.core.models import ModelProfile
from zhenxun.services.ai.utils.logger import log_llm as logger
from zhenxun.utils.pydantic_compat import model_copy

from .client import Route
from .profile import load_json_file, save_json_file

OBSERVATION_FILE = DATA_PATH / "ai" / "model_observations.json"

_TOOL_ALIASES = {
    "web_search": ("web_search", "google_search", "googlesearch", "websearch"),
    "code_execution": ("code_execution", "code_interpreter", "codeexecution"),
    "url_context": ("url_context", "urlcontext"),
    "google_map": ("google_map", "googlemaps"),
    "file_search": ("file_search", "filesearch"),
    "computer_use": ("computer_use",),
    "image_generation": ("image_generation",),
}
"""内置工具在各协议请求与报错中的写法"""
_STRUCTURED = re.compile(
    r"response_format|json_schema|json_object|output_config|responsejsonschema"
    r"|responsemimetype|structured.?output|text\.format"
)
_IMAGE = re.compile(r"image|vision|vlm|multimodal|多模态|图片|图像")
_UNSUPPORTED = re.compile(
    r"not\s+support|unsupported|not\s+a\s+vlm|does\s*n[o']t|unknown variant|不支持"
)
_THINKING = re.compile(r"thinking|budget_tokens|adaptive")
_REASONING_FLIP = {
    "anthropic": {"adaptive": "budget", "budget": "adaptive"},
    "gemini": {"level": "budget", "budget": "level"},
}
"""推理参数写法被拒绝时改用的另一种写法"""
_observations: dict[str, dict[str, Any]] | None = None


def _scope(provider: ProviderConfig, model: ModelConfig) -> list[str]:
    protocol = model.protocol or provider.protocol
    return [protocol, (model.base_url or provider.base_url).rstrip("/")]


def _load() -> dict[str, dict[str, Any]]:
    global _observations
    if _observations is None:
        _observations = {
            key: entry
            for key, entry in load_json_file(OBSERVATION_FILE).items()
            if isinstance(entry, dict)
        }
    return _observations


def get_observation(
    provider: ProviderConfig, model: ModelConfig
) -> dict[str, Any] | None:
    """读取模型的实测记录

    参数:
        provider: 提供商配置
        model: 模型配置

    返回:
        dict[str, Any] | None: 实测记录，协议或接口地址变更后视为没有记录
    """
    entry = _load().get(f"{provider.name}/{model.name}")
    if not entry or [entry.get("protocol"), entry.get("base_url")] != _scope(
        provider, model
    ):
        return None
    return entry


def record_observation(
    provider: ProviderConfig,
    model: ModelConfig,
    *,
    fields: dict[str, Any] | None = None,
    inputs: dict[str, bool] | None = None,
    native_tools: dict[str, bool] | None = None,
    results: list[dict[str, Any]] | None = None,
    skipped: bool = False,
) -> None:
    """合并保存模型的实测记录并清空能力档案缓存

    参数:
        provider: 提供商配置
        model: 模型配置
        fields: 实测得到的能力字段
        inputs: 输入模态 -> 是否支持
        native_tools: 内置工具类型 -> 是否支持
        results: 本次核验的逐项结果，提供时记为已核验
        skipped: 配置时跳过了核验，已核验的记录不受影响
    """
    from .manager import clear_all_cache

    protocol, base_url = _scope(provider, model)
    entry = deepcopy(get_observation(provider, model) or {})
    entry.update(protocol=protocol, base_url=base_url)
    for key, value in (
        ("fields", fields),
        ("inputs", inputs),
        ("native_tools", native_tools),
    ):
        if value:
            entry[key] = {**(entry.get(key) or {}), **value}
    if results is not None:
        entry.update(checked_at=time.time(), results=results)
        entry.pop("skipped", None)
    elif skipped and not entry.get("checked_at"):
        entry["skipped"] = True
    data = _load()
    data[f"{provider.name}/{model.name}"] = entry
    save_json_file(OBSERVATION_FILE, data)
    clear_all_cache()


def check_status(provider: ProviderConfig, model: ModelConfig) -> dict[str, Any]:
    """模型的核验状态

    参数:
        provider: 提供商配置
        model: 模型配置

    返回:
        dict[str, Any]: status 为 checked / skipped / unchecked，附核验时间与逐项结果
    """
    entry = get_observation(provider, model) or {}
    if entry.get("checked_at"):
        status = "checked"
    elif entry.get("skipped"):
        status = "skipped"
    else:
        status = "unchecked"
    return {
        "status": status,
        "checked_at": entry.get("checked_at"),
        "results": entry.get("results") or [],
    }


def server_tool_types(request: Any) -> list[str]:
    """请求中服务端内置工具的类型

    参数:
        request: 统一请求对象

    返回:
        list[str]: 内置工具类型
    """
    tools = getattr(request, "tools", None) or []
    items = tools.values() if isinstance(tools, dict) else tools
    return [
        tool.type_id
        for tool in items
        if getattr(tool, "execution_side", "client") == "server"
        and getattr(tool, "type_id", "")
    ]


def _rejected_tools(text: str, body: dict[str, Any], types: list[str]) -> list[str]:
    def named(source: str) -> list[str]:
        return [
            t for t in types if any(a in source for a in _TOOL_ALIASES.get(t, (t,)))
        ]

    found = named(text)
    tools = body.get("tools") or []
    for index in re.findall(r"tools\[(\d+)\]", text):
        if int(index) < len(tools):
            entry = json.dumps(tools[int(index)], ensure_ascii=False).lower()
            found += [t for t in named(entry) if t not in found]
    return found


def _has_image(request: Any) -> bool:
    return any(
        isinstance(part, ImagePart)
        for message in getattr(request, "messages", None) or []
        for part in message.content
    )


def _warn_or_record(
    route: Route, label: str, forced: bool, record: dict[str, Any]
) -> None:
    if forced:
        logger.warning(
            f"模型 {route.name} 的 {label} 被强制开启，但接口拒绝，本次已降级重试"
        )
        return
    logger.info(f"模型 {route.name} 不支持{label}，已记录并降级重试")
    record_observation(route.provider, route.model, **record)


def adapt_route(
    route: Route, request: Any, body: dict[str, Any], error: LLMException
) -> Route | None:
    """接口因某项能力拒绝请求时，返回去掉该能力的线路

    参数:
        route: 线路
        request: 统一请求对象
        body: 被拒绝的请求体
        error: 请求异常

    返回:
        Route | None: 降级后的线路，无法判断原因时为 None
    """
    if not isinstance(request, ChatRequest):
        return None
    status = error.details.get("status_code")
    if not (
        (isinstance(error, InvalidRequestException) and status in (400, 422))
        or (isinstance(error, UpstreamServerException) and status == 500)
    ):
        return None
    profile = route.profile
    sources = profile.sources
    text = f"{error.message} {error.details.get('response', '')}".lower()
    sent = [t for t in server_tool_types(request) if t not in profile.native_tools_off]
    if rejected := _rejected_tools(text, body, sent):
        forced = [t for t in rejected if sources.get(f"native_tools.{t}") == "user"]
        learned = [t for t in rejected if t not in forced]
        if forced:
            _warn_or_record(route, f"内置工具 {'、'.join(forced)}", True, {})
        if learned:
            _warn_or_record(
                route,
                f"内置工具 {'、'.join(learned)}",
                False,
                {"native_tools": dict.fromkeys(learned, False)},
            )
        return _copy(
            route,
            native_tools=profile.native_tools - set(rejected),
            native_tools_off=profile.native_tools_off | set(rejected),
        )
    if status == 500:
        return None
    output = route.generation_config(request.config).output
    if (
        (output.response_format == "json" or output.response_schema)
        and profile.structured_output != "none"
        and _STRUCTURED.search(text)
    ):
        mode = "json_object"
        if profile.structured_output == "json_object" or route.protocol == "anthropic":
            mode = "none"
        label = {"json_schema": "JSON Schema", "json_object": "JSON 模式"}
        _warn_or_record(
            route,
            f"{label[profile.structured_output]}结构化输出",
            sources.get("structured_output") == "user",
            {"fields": {"structured_output": mode}},
        )
        return _copy(route, structured_output=mode)
    if (
        _has_image(request)
        and profile.accepts_input("image")
        and _IMAGE.search(text)
        and _UNSUPPORTED.search(text)
    ):
        _warn_or_record(
            route,
            "图片输入",
            sources.get("input_modalities") == "user",
            {"inputs": {"image": False}},
        )
        inputs = {m for m in profile.input_modalities if m != "image"}
        return _copy(route, input_modalities=inputs)
    flip = _REASONING_FLIP.get(route.protocol, {}).get(profile.reasoning_param)
    if flip and "thinking" in json.dumps(body) and _THINKING.search(text):
        _warn_or_record(
            route,
            f"推理参数写法 {profile.reasoning_param}",
            sources.get("reasoning_param") == "user",
            {"fields": {"reasoning_param": flip}},
        )
        return _copy(route, reasoning_param=flip)
    return None


def _copy(route: Route, **update: Any) -> Route:
    profile: ModelProfile = model_copy(route.profile, update=update)
    return Route(route.provider, route.model, profile)


def observe_response(route: Route, request: Any, response: Any) -> None:
    """响应带有网页搜索溯源时，记录模型支持网页搜索

    参数:
        route: 线路
        request: 统一请求对象
        response: 统一响应对象
    """
    profile = route.profile
    if (
        getattr(response, "grounding_metadata", None)
        and "web_search" in server_tool_types(request)
        and "web_search" not in profile.native_tools
    ):
        record_observation(
            route.provider, route.model, native_tools={"web_search": True}
        )


CHECK_ITEMS = {
    "chat": "基本对话",
    "tools": "工具调用",
    "structured_output": "结构化输出",
    "image": "图片输入",
    "reasoning": "推理",
    "web_search": "网页搜索",
    "url_context": "网页读取",
}
"""可核验的能力项"""
DEFAULT_CHECK_ITEMS = ("chat", "tools", "structured_output", "image", "reasoning")
"""默认核验项，网页搜索与网页读取会产生额外的工具费用，需单独勾选"""
_STATUS_LABELS = {
    "supported": "支持",
    "unsupported": "不支持",
    "unknown": "未确认",
    "skipped": "跳过",
    "failed": "失败",
}
_STRUCTURED_LABELS = {
    "json_schema": "JSON Schema",
    "json_object": "JSON 模式",
    "none": "仅提示词",
}
_FETCH_URL = "https://timeapi.io/api/time/current/zone?timeZone=UTC"
_DATE = re.compile(r"(\d{4})\s*[-/年.]\s*(\d{1,2})\s*[-/月.]\s*(\d{1,2})")
_ANSWER_TOKENS = 2048
_REASONING_PROMPT = (
    "一个三位数各位数字之和为 14，百位数字比个位数字大 2，"
    "十位数字是个位数字的 2 倍，这个数是多少？只回答数字。"
)
"""推理核验题：动态思考的模型遇到过于简单的问题会跳过思考"""


def minimal_request(task: str, protocol: str) -> Any:
    """生成用于连通性测试的最小请求

    参数:
        task: 任务类型
        protocol: 接口协议

    返回:
        Any: 统一请求对象
    """
    from zhenxun.services.ai.core.messages import (
        EmbeddingRequest,
        ImageRequest,
        LLMMessage,
        RerankRequest,
        SpeechRequest,
    )
    from zhenxun.services.ai.core.messages.parts import (
        EmbedBatch,
        EmbedPayload,
        TextPart,
    )
    from zhenxun.services.ai.core.options import GenerationConfig

    if task == "embedding":
        return EmbeddingRequest(
            batch=EmbedBatch(payloads=[EmbedPayload(parts=[TextPart(text="test")])]),
        )
    if task == "rerank":
        return RerankRequest(query="a", documents=["a", "b"], top_n=1)
    if task == "image":
        return ImageRequest(prompt="one dot")
    if task == "tts":
        return SpeechRequest(input_text="test")
    config = GenerationConfig()
    config.common.max_tokens = 64 if protocol == "gemini" else 32
    return ChatRequest(messages=[LLMMessage.user("Reply OK")], config=config)


async def probe_once(route: Route, task: str, request: Any, api_key: str) -> Any:
    """用指定密钥直接发送一次请求，不经过重试、降级与故障转移

    参数:
        route: 线路
        task: 任务类型
        request: 统一请求对象
        api_key: 使用的密钥

    返回:
        Any: 统一响应对象
    """
    from zhenxun.services.ai.chat_switch import scene_admitted

    from .client import send
    from .protocols import get_task

    build, parse = get_task(route.protocol, task)
    call = await build(route, request)
    with scene_admitted():
        payload = await send(route, call, api_key)
    return parse(route, request, payload)


def _chat(content: Any, **kwargs: Any) -> ChatRequest:
    from zhenxun.services.ai.core.messages import LLMMessage
    from zhenxun.services.ai.core.options import GenerationConfig

    config = GenerationConfig()
    config.common.max_tokens = kwargs.pop("max_tokens", _ANSWER_TOKENS)
    config.common.reasoning_effort = kwargs.pop("effort", None)
    config.output.response_schema = kwargs.pop("schema", None)
    return ChatRequest(messages=[LLMMessage.user(content)], config=config, **kwargs)


def _red_square() -> bytes:
    from io import BytesIO

    from PIL import Image

    buffer = BytesIO()
    Image.new("RGB", (64, 64), (220, 20, 20)).save(buffer, format="PNG")
    return buffer.getvalue()


def _rejected(error: Exception) -> bool:
    return isinstance(error, InvalidRequestException) or (
        isinstance(error, UpstreamServerException)
        and error.details.get("status_code") == 500
    )


def _today() -> set[tuple[int, int, int]]:
    from datetime import datetime, timedelta, timezone

    now = datetime.now(timezone.utc)
    return {
        (moment.year, moment.month, moment.day)
        for moment in (now, now + timedelta(hours=8), now - timedelta(hours=8))
    }


def _mentions_today(text: str) -> bool:
    today = _today()
    return any(
        (int(y), int(m), int(d)) in today for y, m, d in _DATE.findall(text or "")
    )


def _json_answer(text: str) -> bool:
    text = re.sub(r"^```(?:json)?|```$", "", (text or "").strip()).strip()
    try:
        value = json.loads(text)
    except ValueError:
        return False
    return isinstance(value, dict) and str(value.get("answer")) == "2"


def _reasoning_tokens(value: Any) -> int:
    if isinstance(value, dict):
        total = 0
        for key, item in value.items():
            if key in ("reasoning_tokens", "thoughtsTokenCount") and isinstance(
                item, int
            ):
                total += item
            else:
                total += _reasoning_tokens(item)
        return total
    return 0


def _searched(response: Any) -> bool:
    """判断响应中是否有网页搜索的查询词或来源

    参数:
        response: 对话响应

    返回:
        bool: 接口确实执行了网页搜索
    """
    meta = response.grounding_metadata
    return bool(meta and (meta.web_search_queries or meta.grounding_attributions))


def _row(item: str, status: str, value: Any = None, detail: str = "") -> dict:
    return {"item": item, "status": status, "value": value, "detail": detail}


def _error_text(error: Exception) -> str:
    return str(getattr(error, "message", "") or error)[:200]


async def _probe_tools(route: Route, key: str) -> dict:
    from zhenxun.services.ai.core.models import ToolDefinition

    tool = ToolDefinition(
        name="get_time",
        description="查询指定时区的当前时间",
        parameters={
            "type": "object",
            "properties": {"zone": {"type": "string"}},
            "required": ["zone"],
        },
    )
    request = _chat("请调用 get_time 工具查询 UTC 时间，不要直接回答。", tools=[tool])
    try:
        response = await probe_once(_copy(route, tools=True), "chat", request, key)
    except LLMException as error:
        if _rejected(error):
            return _row("tools", "unsupported", False, _error_text(error))
        raise
    if response.tool_calls:
        return _row("tools", "supported", True)
    return _row("tools", "unknown", None, "模型没有调用工具")


async def _probe_structured(route: Route, key: str) -> dict:
    schema = {
        "type": "object",
        "properties": {"answer": {"type": "integer"}},
        "required": ["answer"],
    }
    modes = ["json_schema"]
    if route.protocol != "anthropic":
        modes.append("json_object")
    detail = ""
    for mode in modes:
        request = _chat("1 加 1 等于几？用 answer 字段回答。", schema=schema)
        route_mode = _copy(route, structured_output=mode)
        try:
            response = await probe_once(route_mode, "chat", request, key)
        except LLMException as error:
            if not _rejected(error):
                raise
            detail = _error_text(error)
            continue
        if _json_answer(response.text):
            return _row("structured_output", "supported", mode)
        detail = "返回内容不是要求的 JSON"
    return _row("structured_output", "unsupported", "none", detail)


async def _probe_image(route: Route, key: str) -> dict:
    from zhenxun.services.ai.core.messages.parts import TextPart

    content = [
        TextPart(text="图片是什么颜色？只回答颜色名称。"),
        ImagePart(raw=_red_square(), mime_type="image/png"),
    ]
    inputs = set(route.profile.input_modalities) | {"image"}
    try:
        response = await probe_once(
            _copy(route, input_modalities=inputs), "chat", _chat(content), key
        )
    except LLMException as error:
        if _rejected(error):
            return _row("image", "unsupported", False, _error_text(error))
        raise
    if re.search(r"红|red", response.text, re.IGNORECASE):
        return _row("image", "supported", True)
    return _row("image", "unsupported", False, "模型没有识别出图片内容")


async def _probe_reasoning(route: Route, key: str) -> dict:
    profile = route.profile
    if profile.reasoning_param == "none":
        return _row("reasoning", "skipped", None, "能力档案未声明推理")
    params = [profile.reasoning_param]
    if flip := _REASONING_FLIP.get(route.protocol, {}).get(profile.reasoning_param):
        params.append(flip)
    detail = ""
    for param in params:
        request = _chat(_REASONING_PROMPT, effort="medium", max_tokens=None)
        try:
            response = await probe_once(
                _copy(route, reasoning_param=param), "chat", request, key
            )
        except LLMException as error:
            if not _rejected(error):
                raise
            detail = _error_text(error)
            continue
        if response.thought_text or _reasoning_tokens(response.usage_info):
            return _row("reasoning", "supported", param)
        return _row("reasoning", "unknown", param, "响应中没有推理内容")
    return _row("reasoning", "unsupported", "none", detail)


async def _probe_native(route: Route, key: str, item: str) -> dict:
    from zhenxun.services.ai.tools.providers.builtin.native import (
        UrlContextTool,
        WebSearchTool,
    )

    from .protocols import native_tool_allowed

    if item == "url_context":
        if route.protocol != "gemini":
            return _row(item, "skipped", None, "当前协议没有网页读取工具")
        tool: Any = UrlContextTool()
        prompt = f"读取 {_FETCH_URL} 的内容，只回答其中的日期，格式 YYYY-MM-DD。"
    else:
        tool = WebSearchTool()
        prompt = "联网搜索今天的日期，只回答日期，格式 YYYY-MM-DD。"
    probe = _copy(
        route,
        native_tools=route.profile.native_tools | {item},
        native_tools_off=route.profile.native_tools_off - {item},
    )
    if not native_tool_allowed(probe, item):
        return _row(item, "skipped", None, "当前协议下该厂商没有这个内置工具")
    for _ in range(2):
        try:
            response = await probe_once(probe, "chat", _chat(prompt, tools=[tool]), key)
        except LLMException as error:
            if _rejected(error):
                return _row(item, "unsupported", False, _error_text(error))
            raise
        if item == "web_search":
            found = _searched(response)
        else:
            found = response.grounding_metadata or _mentions_today(response.text)
        if found:
            return _row(item, "supported", True)
    if item == "web_search":
        vendor = detect_preset(route.base_url) or route.provider.preset
        detail = (
            "百炼 Chat Completions 接口不返回搜索记录，无法确认是否执行了搜索"
            if vendor == "dashscope" and route.protocol == "openai_chat"
            else "两次请求都没有返回搜索记录，接口可能忽略了该工具"
        )
        return _row(item, "unknown", None, detail)
    return _row(item, "unknown", None, "两次回答都不是今天的日期，接口可能忽略了该工具")


async def check_model(
    route: Route, api_key: str, items: list[str] | tuple[str, ...] | None = None
) -> list[dict[str, Any]]:
    """核验模型能力，基本对话失败时直接抛出异常

    参数:
        route: 线路，能力档案为当前解析结果
        api_key: 使用的密钥
        items: 核验项，为空时使用默认核验项

    返回:
        list[dict[str, Any]]: 逐项结果 (item / status / value / detail)
    """
    selected = [item for item in CHECK_ITEMS if item in (items or DEFAULT_CHECK_ITEMS)]
    task = (
        "chat"
        if route.profile.supports_task("chat")
        else next(iter(sorted(route.profile.tasks)), "chat")
    )
    await probe_once(route, task, minimal_request(task, route.protocol), api_key)
    results = [_row("chat", "supported", task)]
    if task != "chat":
        return results
    probes = {
        "tools": _probe_tools,
        "structured_output": _probe_structured,
        "image": _probe_image,
        "reasoning": _probe_reasoning,
    }
    for item in selected:
        if item == "chat":
            continue
        try:
            if item in probes:
                results.append(await probes[item](route, api_key))
            else:
                results.append(await _probe_native(route, api_key, item))
        except Exception as error:
            results.append(_row(item, "failed", None, _error_text(error)))
    return results


def _record_results(route: Route, results: list[dict[str, Any]]) -> None:
    fields: dict[str, Any] = {}
    inputs: dict[str, bool] = {}
    native: dict[str, bool] = {}
    for row in results:
        item, status, value = row["item"], row["status"], row["value"]
        if status not in ("supported", "unsupported"):
            continue
        if item == "tools":
            fields["tools"] = value
        elif item == "structured_output":
            fields["structured_output"] = value
        elif item == "image":
            inputs["image"] = value
        elif item == "reasoning":
            fields["reasoning_param"] = value
        elif item in ("web_search", "url_context"):
            native[item] = value
    record_observation(
        route.provider,
        route.model,
        fields=fields,
        inputs=inputs,
        native_tools=native,
        results=results,
    )


async def run_check(
    route: Route, api_key: str, items: list[str] | tuple[str, ...] | None = None
) -> list[dict[str, Any]]:
    """核验模型能力并保存为实测记录

    参数:
        route: 线路
        api_key: 使用的密钥
        items: 核验项，为空时使用默认核验项

    返回:
        list[dict[str, Any]]: 逐项结果
    """
    results = await check_model(route, api_key, items)
    _record_results(route, results)
    return results


def format_results(results: list[dict[str, Any]]) -> str:
    """把核验结果转换为一行摘要

    参数:
        results: 逐项结果

    返回:
        str: 摘要文本
    """
    parts = []
    for row in results:
        if row["item"] == "chat":
            continue
        label = CHECK_ITEMS.get(row["item"], row["item"])
        if row["item"] == "structured_output" and row["status"] == "supported":
            text = _STRUCTURED_LABELS.get(row["value"], str(row["value"]))
        else:
            text = _STATUS_LABELS.get(row["status"], row["status"])
        parts.append(f"{label} {text}")
    return "，".join(parts) or "连通正常"


async def check_unverified_models() -> None:
    """核验配置文件中从未核验过的已启用对话模型，失败的模型下次启动时重试"""
    from zhenxun.services.ai.core.exceptions import AuthenticationException

    from .health import health_manager
    from .manager import get_configured_providers, get_profile

    pending = [
        Route(provider, model, get_profile(provider, model))
        for provider in get_configured_providers()
        if provider.base_url and provider.api_keys
        for model in provider.models
        if model.enabled and get_observation(provider, model) is None
    ]
    pending = [route for route in pending if route.profile.supports_task("chat")]
    if not pending:
        return
    names = "、".join(route.name for route in pending)
    logger.info(f"发现 {len(pending)} 个未核验的模型，开始核验能力: {names}")
    for route in pending:
        provider = route.provider
        available = health_manager.available_keys(
            provider.name, provider.api_keys, set()
        )
        keys = [*available, *(k for k in provider.api_keys if k not in available)]
        error: Exception | None = None
        for key in keys:
            try:
                results = await run_check(route, key)
            except AuthenticationException as e:
                error = e
                continue
            except Exception as e:
                error = e
                break
            logger.info(f"模型 {route.name} 核验完成: {format_results(results)}")
            error = None
            break
        if error is not None:
            logger.warning(
                f"模型 {route.name} 核验失败，下次启动时重试: {_error_text(error)}"
            )
