"""
Gemini generateContent 协议

思维签名 (thoughtSignature) 保存在片段元数据中，回传给同协议请求。
"""

import base64
import io
import json
from typing import Any
import uuid
import wave

from zhenxun.services.ai.core.exceptions import (
    ConfigurationException,
    ContentFilteredException,
    NoImageException,
    ResponseParseException,
)
from zhenxun.services.ai.core.messages import (
    AssistantMessage,
    AudioPart,
    AudioResponse,
    ChatRequest,
    ChatResponse,
    EmbeddingRequest,
    EmbeddingResponse,
    FilePart,
    ImagePart,
    ImageRequest,
    ImageResponse,
    LLMGroundingAttribution,
    LLMGroundingMetadata,
    SpeechRequest,
    SystemMessage,
    TextPart,
    ThoughtPart,
    ToolCallPart,
    ToolMessage,
    UsageInfo,
    UserMessage,
    VideoPart,
)
from zhenxun.services.ai.core.options import (
    GenerationConfig,
    LLMEmbeddingConfig,
    OutputFormatConfig,
    TTSConfig,
    deep_merge,
)
from zhenxun.services.ai.llm.profile import map_reasoning_effort
from zhenxun.services.ai.utils.logger import log_llm as logger

from . import (
    HttpCall,
    ensure_content,
    filter_modalities,
    native_tool_allowed,
    output_image,
    parse_arguments,
    replay_data,
    replay_metadata,
    resolve_tool_choice,
    split_tools,
)
from .schema import schema_instruction, tool_schema
from .tasks import to_image_part

PROTOCOL = "gemini"

_BUDGETS = {"minimal": 512, "low": 1024, "medium": 8192, "high": 24576}
_BLOCKED = {"SAFETY", "PROHIBITED_CONTENT", "BLOCKLIST", "SPII", "IMAGE_SAFETY"}
_EMBED_TASKS = {
    "RETRIEVAL_QUERY": "search result",
    "QUESTION_ANSWERING": "question answering",
    "FACT_VERIFICATION": "fact checking",
    "CODE_RETRIEVAL_QUERY": "code retrieval",
}
_FILE_URI_PREFIXES = (
    "gs://",
    "https://generativelanguage.googleapis.com/",
    "https://www.youtube.com/",
    "https://youtu.be/",
)


def _model_path(route, method: str) -> str:
    name = route.model.name.removeprefix("models/")
    return f"/models/{name}:{method}"


async def _media(part: Any, default_mime: str) -> dict[str, Any]:
    mime = part.mime_type or default_mime
    if part.url and part.url.startswith(_FILE_URI_PREFIXES):
        payload: dict[str, Any] = {"fileData": {"fileUri": part.url, "mimeType": mime}}
    else:
        payload = {
            "inlineData": {"mimeType": mime, "data": await part.get_base64_data()}
        }
    resolution = getattr(part, "media_resolution", None)
    if resolution:
        level = resolution.upper()
        if not level.startswith("MEDIA_RESOLUTION_"):
            level = f"MEDIA_RESOLUTION_{level}"
        payload["mediaResolution"] = {"level": level}
    return payload


async def _part(part: Any) -> dict[str, Any] | None:
    if isinstance(part, TextPart):
        payload: dict[str, Any] | None = {"text": part.text}
    elif isinstance(part, ThoughtPart):
        payload = {"text": part.thought_text, "thought": True}
    elif isinstance(part, ImagePart):
        payload = await _media(part, "image/png")
    elif isinstance(part, AudioPart):
        payload = await _media(part, "audio/mp3")
    elif isinstance(part, VideoPart):
        payload = await _media(part, "video/mp4")
    elif isinstance(part, FilePart):
        payload = await _media(part, "application/pdf")
    elif isinstance(part, ToolCallPart):
        payload = {
            "functionCall": {
                "id": part.id,
                "name": part.tool_name,
                "args": parse_arguments(part.args),
            }
        }
    else:
        payload = None
    if payload is not None and isinstance(data := replay_data(part, PROTOCOL), dict):
        payload.update(data)
    return payload


def _function_response(item: Any) -> dict[str, Any]:
    output = item.output
    if isinstance(output, str):
        try:
            output = json.loads(output)
        except json.JSONDecodeError:
            pass
    if not isinstance(output, dict):
        output = {"result": output}
    return {
        "functionResponse": {
            "id": item.tool_call_id,
            "name": item.tool_name,
            "response": output,
        }
    }


async def build_contents(
    request: ChatRequest, route
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """把统一消息编码为 Gemini contents

    参数:
        request: 对话请求
        route: 线路

    返回:
        tuple: (contents, systemInstruction 的 parts)
    """
    messages = filter_modalities(request.messages, route.profile, route.name)
    system: list[dict[str, Any]] = []
    contents: list[dict[str, Any]] = []

    def append(role: str, parts: list[dict[str, Any]]) -> None:
        if not parts:
            return
        if contents and contents[-1]["role"] == role:
            contents[-1]["parts"].extend(parts)
        else:
            contents.append({"role": role, "parts": parts})

    for message in messages:
        if isinstance(message, SystemMessage):
            system.extend({"text": p.text} for p in message.content if p.text)
        elif isinstance(message, ToolMessage):
            append("user", [_function_response(i) for i in message.tool_returns])
        elif isinstance(message, AssistantMessage):
            parts = []
            for part in message.content:
                if isinstance(part, ThoughtPart) and not replay_data(part, PROTOCOL):
                    continue
                if payload := await _part(part):
                    parts.append(payload)
            append("model", parts)
        else:
            parts = [payload for p in message.content if (payload := await _part(p))]
            append("user", parts)
    return contents, system


def _server_tool(tool: Any) -> dict[str, Any] | None:
    type_id = getattr(tool, "type_id", "")
    if type_id == "web_search":
        return {"googleSearch": {}}
    if type_id == "code_execution":
        return {"codeExecution": {}}
    if type_id == "url_context":
        return {"urlContext": {}}
    if type_id == "google_map":
        return {"googleMaps": {}}
    if type_id == "file_search":
        if names := tool.metadata.get("file_search_store_names"):
            return {"fileSearch": {"fileSearchStoreNames": names}}
    return None


def _thinking_config(config: GenerationConfig, profile) -> dict[str, Any]:
    thinking: dict[str, Any] = {}
    if profile.reasoning_levels and "image" not in profile.output_modalities:
        thinking["includeThoughts"] = True
    effort = map_reasoning_effort(config.common.reasoning_effort, profile)
    if effort == "none":
        thinking = {"thinkingBudget": 0}
    elif effort and profile.reasoning_param == "level":
        thinking["thinkingLevel"] = effort
    elif effort and profile.reasoning_param == "budget":
        thinking["thinkingBudget"] = _BUDGETS.get(effort, -1)
    return thinking


def _image_size(resolution: str | None) -> str | None:
    if not resolution:
        return None
    value = resolution.upper()
    if value in ("1K", "2K", "4K"):
        return value
    longest = max((int(n) for n in value.split("X") if n.isdigit()), default=1024)
    return "1K" if longest <= 1024 else "2K" if longest <= 2048 else "4K"


def _generation_config(config: GenerationConfig, route) -> dict[str, Any]:
    common = config.common
    profile = route.profile
    result: dict[str, Any] = {}
    if profile.sampling:
        for key, name in (
            ("temperature", "temperature"),
            ("top_p", "topP"),
            ("top_k", "topK"),
            ("presence_penalty", "presencePenalty"),
            ("frequency_penalty", "frequencyPenalty"),
        ):
            if (value := getattr(common, key)) is not None:
                result[name] = value
    max_tokens = common.max_tokens
    if max_tokens and profile.max_output_tokens:
        max_tokens = min(max_tokens, profile.max_output_tokens)
    if max_tokens:
        result["maxOutputTokens"] = max_tokens
    if common.stop is not None:
        result["stopSequences"] = (
            common.stop if isinstance(common.stop, list) else [common.stop]
        )
    if thinking := _thinking_config(config, profile):
        result["thinkingConfig"] = thinking
    output = config.output
    if output.response_modalities:
        result["responseModalities"] = [m.upper() for m in output.response_modalities]
        image_config = {
            "aspectRatio": config.media.aspect_ratio,
            "imageSize": _image_size(config.media.resolution),
        }
        if image_config := {k: v for k, v in image_config.items() if v}:
            result["imageConfig"] = image_config
    return result


async def build_chat(route, request: ChatRequest) -> HttpCall:
    """编码对话请求"""
    config = route.generation_config(request.config)
    profile = route.profile
    contents, system = await build_contents(request, route)
    body: dict[str, Any] = {"contents": contents}
    generation = _generation_config(config, route)

    definitions, server = await split_tools(request.tools)
    kind, allowed, name = resolve_tool_choice(request.tool_choice, config.tools.mode)
    tools: list[dict[str, Any]] = []
    if definitions and kind != "none":
        if not profile.tools:
            raise ConfigurationException(f"模型 {route.name} 不支持函数调用")
        tools.append(
            {
                "functionDeclarations": [
                    {
                        "name": d.name,
                        "description": d.description,
                        "parametersJsonSchema": tool_schema(d.parameters),
                    }
                    for d in definitions
                ]
            }
        )
        if kind != "auto" or allowed:
            calling: dict[str, Any] = {"mode": "ANY" if kind != "auto" else "AUTO"}
            if names := ([name] if name else allowed):
                calling["allowedFunctionNames"] = names
                calling["mode"] = "ANY"
            body["toolConfig"] = {"functionCallingConfig": calling}
    server_payloads = []
    for tool in server:
        payload = _server_tool(tool)
        type_id = getattr(tool, "type_id", "")
        if payload and native_tool_allowed(route, type_id):
            server_payloads.append(payload)
        else:
            logger.warning(
                f"模型 {route.name} 不支持或未确认支持内置工具 {type_id}，已忽略"
            )
    if server_payloads and tools and not profile.mixed_tools:
        logger.warning(
            f"模型 {route.name} 不支持内置工具与函数工具混用，已忽略内置工具"
        )
        server_payloads = []
    if server_payloads:
        tools.extend(server_payloads)
        if len(tools) > len(server_payloads):
            body.setdefault("toolConfig", {})["includeServerSideToolInvocations"] = True
    if tools:
        body["tools"] = tools

    output = config.output
    schema = output.response_schema
    if output.response_format == "json" or schema:
        if profile.structured_output != "none":
            generation["responseMimeType"] = "application/json"
        if schema and profile.structured_output == "json_schema":
            generation["responseJsonSchema"] = tool_schema(schema)
        else:
            hint = schema_instruction(schema) if schema else "请只输出一个 JSON 对象。"
            system.append({"text": hint})
    if system:
        body["systemInstruction"] = {"parts": system}
    if generation:
        body["generationConfig"] = generation
    return HttpCall(
        _model_path(route, "generateContent"), json=deep_merge(body, config.params)
    )


def _grounding(data: dict[str, Any]) -> LLMGroundingMetadata:
    attributions = [
        LLMGroundingAttribution(title=web.get("title"), uri=web.get("uri"))
        for chunk in data.get("groundingChunks") or []
        if (web := chunk.get("web"))
    ]
    entry = data.get("searchEntryPoint")
    return LLMGroundingMetadata(
        web_search_queries=data.get("webSearchQueries"),
        grounding_attributions=attributions or None,
        search_entry_point=entry.get("renderedContent")
        if isinstance(entry, dict)
        else None,
        map_widget_token=data.get("googleMapsWidgetContextToken"),
    )


def _candidate(route, payload: dict[str, Any]) -> dict[str, Any]:
    feedback = payload.get("promptFeedback") or {}
    if reason := feedback.get("blockReason"):
        raise ContentFilteredException(
            f"[{route.name}] 输入被安全策略拦截: {reason}",
            details={"safety_ratings": feedback.get("safetyRatings")},
        )
    candidates = payload.get("candidates")
    if not candidates:
        raise ResponseParseException(
            f"[{route.name}] 响应中没有 candidates",
            details={"response": str(payload)[:500]},
        )
    candidate = candidates[0]
    if not (candidate.get("content") or {}).get("parts") and (
        candidate.get("finishReason") in _BLOCKED
    ):
        raise ContentFilteredException(
            f"[{route.name}] 输出被安全策略拦截: {candidate['finishReason']}"
        )
    return candidate


def parse_chat(route, request: Any, payload: dict[str, Any]) -> ChatResponse:
    """解码对话响应"""
    candidate = _candidate(route, payload)
    parts: list[Any] = []
    for item in (candidate.get("content") or {}).get("parts") or []:
        signature = item.get("thoughtSignature")
        metadata = (
            replay_metadata(PROTOCOL, {"thoughtSignature": signature})
            if signature
            else None
        )
        if item.get("thought") is True:
            parts.append(
                ThoughtPart(thought_text=item.get("text") or "", metadata=metadata)
            )
        elif "text" in item:
            parts.append(TextPart(text=item["text"], metadata=metadata))
        elif call := item.get("functionCall"):
            parts.append(
                ToolCallPart(
                    id=call.get("id") or f"call_{uuid.uuid4().hex[:24]}",
                    tool_name=call.get("name") or "",
                    args=call.get("args") or {},
                    metadata=metadata,
                )
            )
        elif inline := item.get("inlineData"):
            mime = inline.get("mimeType") or "image/png"
            if mime.startswith("image/"):
                parts.append(output_image(inline["data"], mime))
        elif signature and parts:
            parts[-1].metadata = deep_merge(parts[-1].metadata or {}, metadata or {})
    ensure_content(route, parts, candidate.get("finishReason") != "MAX_TOKENS")
    grounding = candidate.get("groundingMetadata")
    return ChatResponse(
        content_parts=parts,
        usage_info=payload.get("usageMetadata"),
        raw_response=payload,
        grounding_metadata=_grounding(grounding) if grounding else None,
    )


async def build_embedding(route, request: EmbeddingRequest) -> HttpCall:
    """编码向量嵌入请求"""
    config = request.config or LLMEmbeddingConfig()
    model = f"models/{route.model.name.removeprefix('models/')}"
    multimodal = config.multimodal is not False and any(
        route.profile.accepts_input(m) for m in ("image", "audio", "video", "file")
    )
    prefix = ""
    if multimodal and config.task_type == "RETRIEVAL_DOCUMENT":
        prefix = f"title: {config.title or 'none'} | text: "
    elif multimodal and config.task_type:
        prefix = (
            f"task: {_EMBED_TASKS.get(config.task_type, 'search result')} | query: "
        )
    requests = []
    for index, payload in enumerate(request.batch.payloads):
        if multimodal:
            parts = []
            for part in payload.parts:
                modality = getattr(part, "type", "")
                allowed = config.multimodal is True or modality in config.multimodal
                if isinstance(part, TextPart) or (
                    allowed and route.profile.accepts_input(modality)
                ):
                    parts.append(await _part(part))
        else:
            parts = [{"text": request.batch.to_text_only(route.name)[index]}]
        parts = [p for p in parts if p] or [{"text": " "}]
        if prefix and "text" in parts[0]:
            parts[0]["text"] = prefix + parts[0]["text"]
        item: dict[str, Any] = {"model": model, "content": {"parts": parts}}
        if config.task_type and not multimodal:
            item["taskType"] = config.task_type
            if config.title and config.task_type == "RETRIEVAL_DOCUMENT":
                item["title"] = config.title
        if config.output_dimensionality:
            item["outputDimensionality"] = config.output_dimensionality
        requests.append(deep_merge(item, config.params))
    return HttpCall(
        _model_path(route, "batchEmbedContents"), json={"requests": requests}
    )


def parse_embedding(
    route, request: EmbeddingRequest, payload: dict[str, Any]
) -> EmbeddingResponse:
    """解码向量嵌入响应"""
    embeddings = payload.get("embeddings")
    if not isinstance(embeddings, list) or not embeddings:
        raise ResponseParseException(f"[{route.name}] 响应中没有向量数据")
    return EmbeddingResponse(
        embeddings=[item.get("values") or [] for item in embeddings],
        model_name=route.name,
    )


async def build_speech(route, request: SpeechRequest) -> HttpCall:
    """编码语音合成请求"""
    config = request.config or TTSConfig()
    voice = request.voice or route.profile.default_voice or "Aoede"
    body = {
        "contents": [{"parts": [{"text": request.input_text}]}],
        "generationConfig": {
            "responseModalities": ["AUDIO"],
            "speechConfig": {
                "voiceConfig": {"prebuiltVoiceConfig": {"voiceName": voice}}
            },
        },
    }
    return HttpCall(
        _model_path(route, "generateContent"), json=deep_merge(body, config.params)
    )


def parse_speech(route, request: SpeechRequest, payload: dict[str, Any]):
    """解码语音合成响应，PCM 数据封装为 WAV"""
    candidate = _candidate(route, payload)
    try:
        inline = candidate["content"]["parts"][0]["inlineData"]
    except (KeyError, IndexError) as e:
        raise ResponseParseException(f"[{route.name}] 响应中没有音频数据") from e
    pcm = base64.b64decode(inline["data"])
    config = request.config or TTSConfig()
    if config.response_format == "pcm":
        audio, audio_format = pcm, "pcm"
    else:
        buffer = io.BytesIO()
        with wave.open(buffer, "wb") as wav:
            wav.setnchannels(1)
            wav.setsampwidth(2)
            wav.setframerate(24000)
            wav.writeframes(pcm)
        audio, audio_format = buffer.getvalue(), "wav"
    usage = payload.get("usageMetadata") or {}
    return AudioResponse(
        audio_bytes=audio,
        audio_format=audio_format,
        usage=UsageInfo(
            prompt_tokens=usage.get("promptTokenCount") or 0,
            total_tokens=usage.get("totalTokenCount") or 0,
        ),
        model_name=route.name,
    )


def _image_chat_request(request: ImageRequest) -> ChatRequest:
    config = (request.config or GenerationConfig()).merge_with(
        GenerationConfig(
            output=OutputFormatConfig(response_modalities=["text", "image"])
        )
    )
    images = [to_image_part(item) for item in request.images or []]
    message = UserMessage(content=[TextPart(text=request.prompt), *images])
    return ChatRequest(messages=[message], config=config)


async def build_image(route, request: ImageRequest) -> HttpCall:
    """编码图像生成请求"""
    return await build_chat(route, _image_chat_request(request))


def parse_image(route, request: ImageRequest, payload: dict[str, Any]):
    """解码图像生成响应"""
    response = parse_chat(route, request, payload)
    if not response.images:
        raise NoImageException(f"[{route.name}] 响应中没有图片: {response.text[:200]}")
    return ImageResponse(content_parts=response.content_parts, raw_response=payload)


TASKS = {
    "chat": (build_chat, parse_chat),
    "embedding": (build_embedding, parse_embedding),
    "tts": (build_speech, parse_speech),
    "image": (build_image, parse_image),
}
