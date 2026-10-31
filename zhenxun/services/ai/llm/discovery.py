"""
模型发现

调用各协议的模型列表接口，记录上游模型名称、接口给出的能力字段
以及中转站声明的可用接口，保存为快照参与能力档案合并。
"""

import time
from typing import Any

from zhenxun.configs.path_config import DATA_PATH
from zhenxun.services.ai.config import ProviderConfig
from zhenxun.services.ai.core.exceptions import ResponseParseException
from zhenxun.services.ai.core.models import REASONING_LEVELS

from .client import auth_headers, get_http_client, join_url, map_http_error
from .profile import load_json_file, parse_modalities, save_json_file

SNAPSHOT_FILE = DATA_PATH / "ai" / "model_snapshots.json"
DISCOVERY_MAX_AGE = 86400
"""模型列表自动刷新间隔 (秒)"""

_ENDPOINT_PROTOCOLS = {
    "openai": "openai_chat",
    "openai-response": "openai_responses",
    "anthropic": "anthropic",
    "gemini": "gemini",
}
_ENDPOINT_TASKS = {
    "embeddings": "embedding",
    "image-generation": "image",
    "jina-rerank": "rerank",
}
_snapshots: dict[str, dict[str, Any]] | None = None


def _load_snapshots() -> dict[str, dict[str, Any]]:
    global _snapshots
    if _snapshots is None:
        _snapshots = {
            name: entry
            for name, entry in load_json_file(SNAPSHOT_FILE).items()
            if isinstance(entry, dict) and isinstance(entry.get("models"), dict)
        }
    return _snapshots


def get_discovery(provider: ProviderConfig) -> dict[str, Any]:
    """读取提供商最近一次模型发现的结果

    参数:
        provider: 提供商配置

    返回:
        dict[str, Any]: 含 updated_at 与 models，接口地址或协议变更后视为未发现
    """
    entry = _load_snapshots().get(provider.name) or {}
    if (entry.get("protocol"), entry.get("base_url")) != (
        provider.protocol,
        provider.base_url,
    ):
        return {}
    return entry


def get_snapshot(provider: ProviderConfig, model_name: str) -> dict[str, Any] | None:
    """读取模型的发现快照

    参数:
        provider: 提供商配置
        model_name: 模型名称

    返回:
        dict[str, Any] | None: 能力字段，未发现过时为 None
    """
    return get_discovery(provider).get("models", {}).get(model_name)


def save_snapshots(
    provider: ProviderConfig, snapshots: dict[str, dict[str, Any]]
) -> None:
    """覆盖保存提供商的模型发现结果

    参数:
        provider: 提供商配置
        snapshots: 模型名称 -> 能力字段
    """
    data = _load_snapshots()
    data[provider.name] = {
        "updated_at": time.time(),
        "protocol": provider.protocol,
        "base_url": provider.base_url,
        "models": snapshots,
    }
    save_json_file(SNAPSHOT_FILE, data)


def _openai_snapshot(item: dict[str, Any]) -> dict[str, Any]:
    """OpenAI 兼容接口通常只返回 id；OpenRouter、DeepSeek 等会额外返回
    上下文、模态与推理档位，new-api 类中转站会返回 supported_endpoint_types"""
    top, architecture, effort = (
        value if isinstance(value := item.get(key), dict) else {}
        for key in ("top_provider", "architecture", "effort")
    )
    snapshot: dict[str, Any] = {
        "context_window": item.get("context_length")
        or item.get("context_window")
        or top.get("context_length"),
        "max_output_tokens": top.get("max_completion_tokens")
        or item.get("max_output_tokens"),
        "input_modalities": parse_modalities(
            architecture.get("input_modalities") or item.get("input_modalities")
        ),
        "output_modalities": parse_modalities(
            architecture.get("output_modalities") or item.get("output_modalities")
        ),
    }
    levels = effort.get("supported_levels")
    if isinstance(levels, list):
        snapshot["reasoning_levels"] = [v for v in levels if v in REASONING_LEVELS]
    parameters = item.get("supported_parameters")
    if isinstance(parameters, list):
        snapshot["tools"] = "tools" in parameters
        if "structured_outputs" in parameters:
            snapshot["structured_output"] = "json_schema"
        elif "response_format" in parameters:
            snapshot["structured_output"] = "json_object"
    endpoints = item.get("supported_endpoint_types")
    if isinstance(endpoints, list):
        endpoints = [e for e in endpoints if isinstance(e, str)]
        tasks = [_ENDPOINT_TASKS[e] for e in endpoints if e in _ENDPOINT_TASKS]
        snapshot["tasks"] = tasks or None
        protocols = [
            _ENDPOINT_PROTOCOLS[e] for e in endpoints if e in _ENDPOINT_PROTOCOLS
        ]
        snapshot["protocols"] = protocols or None
    return snapshot


def _gemini_snapshot(item: dict[str, Any]) -> dict[str, Any]:
    methods = item.get("supportedGenerationMethods") or []
    snapshot: dict[str, Any] = {
        "context_window": item.get("inputTokenLimit"),
        "max_output_tokens": item.get("outputTokenLimit"),
    }
    if "generateContent" not in methods and any("embed" in m for m in methods):
        snapshot["tasks"] = ["embedding"]
    return snapshot


def _supported(node: Any) -> bool:
    return isinstance(node, dict) and node.get("supported") is True


def _anthropic_snapshot(item: dict[str, Any]) -> dict[str, Any]:
    snapshot: dict[str, Any] = {
        "context_window": item.get("max_input_tokens"),
        "max_output_tokens": item.get("max_tokens"),
    }
    caps = item.get("capabilities")
    if not isinstance(caps, dict):
        return snapshot
    inputs = ["text"]
    if _supported(caps.get("image_input")):
        inputs.append("image")
    if _supported(caps.get("pdf_input")):
        inputs.append("file")
    snapshot["input_modalities"] = inputs
    if "structured_outputs" in caps:
        snapshot["structured_output"] = (
            "json_schema" if _supported(caps["structured_outputs"]) else "none"
        )
    thinking = caps.get("thinking") or {}
    types = thinking.get("types") or {}
    if _supported(types.get("adaptive")):
        snapshot["reasoning_param"] = "adaptive"
    elif _supported(types.get("enabled")):
        snapshot["reasoning_param"] = "budget"
    effort = caps.get("effort") or {}
    if _supported(effort):
        snapshot["reasoning_levels"] = [
            level for level in REASONING_LEVELS if _supported(effort.get(level))
        ]
    return snapshot


async def _get_pages(
    provider: ProviderConfig, protocol: str, api_key: str
) -> list[dict[str, Any]]:
    client = get_http_client(provider)
    headers = {**auth_headers(protocol, api_key), **provider.headers}
    path = "/v1/models" if protocol == "anthropic" else "/models"
    url = join_url(provider.base_url, path)
    params: dict[str, Any] = (
        {"limit": 1000}
        if protocol == "anthropic"
        else {"pageSize": 1000}
        if protocol == "gemini"
        else {}
    )
    items: list[dict[str, Any]] = []
    for _ in range(20):
        response = await client.request(
            "GET", url, headers=headers, params=params, timeout=provider.timeout
        )
        if response.status_code >= 400:
            raise map_http_error(response, provider.name)
        try:
            body = response.json()
        except ValueError as e:
            raise ResponseParseException("模型列表响应不是有效的 JSON") from e
        if not isinstance(body, dict):
            raise ResponseParseException("模型列表响应格式无效")
        page = body.get("models" if protocol == "gemini" else "data") or []
        items.extend(item for item in page if isinstance(item, dict))
        if protocol == "gemini" and body.get("nextPageToken"):
            params["pageToken"] = body["nextPageToken"]
        elif protocol == "anthropic" and body.get("has_more") and body.get("last_id"):
            params["after_id"] = body["last_id"]
        else:
            break
    return items


async def discover_models(
    provider: ProviderConfig, api_key: str, protocol: str | None = None
) -> dict[str, dict[str, Any]]:
    """查询提供商的模型列表及能力字段

    参数:
        provider: 提供商配置
        api_key: 使用的密钥
        protocol: 查询使用的协议，为空时使用提供商协议

    返回:
        dict[str, dict[str, Any]]: 模型名称 -> 能力字段 (只含接口给出的字段)
    """
    protocol = protocol or provider.protocol
    result: dict[str, dict[str, Any]] = {}
    for item in await _get_pages(provider, protocol, api_key):
        if protocol == "gemini":
            name, snapshot = item.get("name"), _gemini_snapshot(item)
        elif protocol == "anthropic":
            name, snapshot = item.get("id"), _anthropic_snapshot(item)
        else:
            name, snapshot = item.get("id"), _openai_snapshot(item)
        if name:
            result[str(name).removeprefix("models/")] = {
                k: v for k, v in snapshot.items() if v is not None
            }
    return result
