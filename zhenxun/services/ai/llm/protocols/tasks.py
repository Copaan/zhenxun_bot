"""
OpenAI 兼容的非对话任务：向量嵌入、重排、语音合成、图像生成

厂商差异 (Jina 嵌入、MiniMax / MiMo 语音、经对话接口出图) 由预设中的
embedding_api / tts_api / image_api 选择。
"""

import array
import base64
from pathlib import Path
import re
from typing import Any

import httpx

from zhenxun.services.ai.core.exceptions import (
    AuthenticationException,
    InvalidRequestException,
    NoImageException,
    QuotaExceededException,
    RateLimitException,
    ResponseParseException,
)
from zhenxun.services.ai.core.messages import (
    AudioResponse,
    ChatRequest,
    EmbeddingRequest,
    EmbeddingResponse,
    ImagePart,
    ImageRequest,
    ImageResponse,
    RerankDocument,
    RerankRequest,
    RerankResponse,
    RerankResult,
    SpeechRequest,
    TextPart,
    UsageInfo,
    UserMessage,
)
from zhenxun.services.ai.core.options import (
    GenerationConfig,
    LLMEmbeddingConfig,
    OutputFormatConfig,
    TTSConfig,
    deep_merge,
)

from . import HttpCall, decode_data_uri, output_image

_JINA_TASKS = {
    "RETRIEVAL_QUERY": "retrieval.query",
    "RETRIEVAL_DOCUMENT": "retrieval.passage",
    "SEMANTIC_SIMILARITY": "text-matching",
    "CLASSIFICATION": "classification",
    "CLUSTERING": "clustering",
}

_IMAGE_SIZES = {
    "1K": ("1536x1024", "1024x1536", "1024x1024"),
    "2K": ("2048x1152", "1152x2048", "2048x2048"),
    "4K": ("3840x2160", "2160x3840", "2880x2880"),
}


def _usage(payload: dict[str, Any]) -> UsageInfo:
    usage = payload.get("usage") or {}
    return UsageInfo(
        prompt_tokens=usage.get("prompt_tokens") or 0,
        completion_tokens=usage.get("completion_tokens") or 0,
        total_tokens=usage.get("total_tokens") or 0,
    )


def _allowed_modality(config: LLMEmbeddingConfig, route, modality: str) -> bool:
    allowed = config.multimodal
    return bool(
        allowed is True or (isinstance(allowed, list) and modality in allowed)
    ) and route.profile.accepts_input(modality)


async def _jina_input(request: EmbeddingRequest, route, config) -> list[Any]:
    if not any(
        _allowed_modality(config, route, m) for m in ("image", "audio", "video")
    ):
        return request.batch.to_text_only(route.name)
    result = []
    for payload in request.batch.payloads:
        content = []
        for part in payload.parts:
            modality = getattr(part, "type", "")
            if isinstance(part, TextPart):
                content.append({"text": part.text})
            elif _allowed_modality(config, route, modality):
                value = getattr(part, "url", None) or await part.get_base64_data()
                content.append({modality: value})
        result.append({"content": content or [{"text": " "}]})
    return result


async def build_embedding(route, request: EmbeddingRequest) -> HttpCall:
    """编码向量嵌入请求"""
    config = request.config or LLMEmbeddingConfig()
    body: dict[str, Any] = {"model": route.model.name}
    if route.preset.get("embedding_api") == "jina":
        body["input"] = await _jina_input(request, route, config)
        if task := _JINA_TASKS.get(config.task_type or ""):
            body["task"] = task
        if config.encoding_format and config.encoding_format != "float":
            body["embedding_type"] = config.encoding_format
    else:
        body["input"] = request.batch.to_text_only(route.name)
        if config.encoding_format:
            body["encoding_format"] = config.encoding_format
    if config.output_dimensionality:
        body["dimensions"] = config.output_dimensionality
    return HttpCall("/embeddings", json=deep_merge(body, config.params))


def _vector(value: Any) -> list[float]:
    if isinstance(value, str):
        floats = array.array("f")
        floats.frombytes(base64.b64decode(value))
        return floats.tolist()
    return value


def parse_embedding(
    route, request: EmbeddingRequest, payload: dict[str, Any]
) -> EmbeddingResponse:
    """解码向量嵌入响应"""
    data = sorted(payload.get("data") or [], key=lambda item: item.get("index", 0))
    if not data:
        raise ResponseParseException(f"[{route.name}] 响应中没有向量数据")
    return EmbeddingResponse(
        embeddings=[_vector(item["embedding"]) for item in data],
        usage=_usage(payload),
        model_name=route.name,
    )


async def build_rerank(route, request: RerankRequest) -> HttpCall:
    """编码重排请求"""
    documents = [
        doc if isinstance(doc, str) else doc.get("text") or ""
        for doc in request.documents
    ]
    body = {
        "model": route.model.name,
        "query": request.query,
        "documents": documents,
        "top_n": min(request.top_n, len(documents)),
    }
    return HttpCall("/rerank", json=body)


def parse_rerank(route, request: RerankRequest, payload: dict[str, Any]):
    """解码重排响应"""
    results = []
    for item in payload.get("results") or []:
        document = item.get("document")
        if isinstance(document, str):
            document = {"text": document}
        results.append(
            RerankResult(
                index=item["index"],
                relevance_score=item["relevance_score"],
                document=RerankDocument(**document) if document else None,
            )
        )
    results.sort(key=lambda r: r.relevance_score, reverse=True)
    return RerankResponse(results=results)


def _voice(route, request: SpeechRequest) -> str:
    return request.voice or route.profile.default_voice or "alloy"


async def build_speech(route, request: SpeechRequest) -> HttpCall:
    """编码语音合成请求"""
    config = request.config or TTSConfig()
    api = route.preset.get("tts_api")
    if api == "minimax_t2a":
        audio_format = config.response_format
        body: dict[str, Any] = {
            "model": route.model.name,
            "text": request.input_text,
            "stream": False,
            "voice_setting": {
                "voice_id": _voice(route, request),
                "speed": config.speed,
            },
            "audio_setting": {
                "sample_rate": 32000,
                "bitrate": 128000,
                "format": audio_format
                if audio_format in ("mp3", "pcm", "flac", "wav")
                else "mp3",
                "channel": 1,
            },
        }
        return HttpCall("/t2a_v2", json=deep_merge(body, config.params))
    if api == "mimo_chat":
        body = {
            "model": route.model.name,
            "messages": [{"role": "assistant", "content": request.input_text}],
            "audio": {
                "format": "pcm16" if config.response_format == "pcm" else "wav",
                "voice": _voice(route, request),
            },
        }
        return HttpCall("/chat/completions", json=deep_merge(body, config.params))
    body = {
        "model": route.model.name,
        "input": request.input_text,
        "voice": _voice(route, request),
        "response_format": config.response_format,
        "speed": config.speed,
    }
    return HttpCall("/audio/speech", json=deep_merge(body, config.params), raw=True)


def _minimax_error(payload: dict[str, Any], route) -> Exception | None:
    base = payload.get("base_resp") or {}
    code = base.get("status_code", 0)
    if not code:
        return None
    message = f"[{route.name}] MiniMax 语音合成失败 ({code}): {base.get('status_msg')}"
    if code in (1004, 2049):
        return AuthenticationException(message)
    if code == 1008:
        return QuotaExceededException(message)
    if code in (1002, 1039):
        return RateLimitException(message)
    return InvalidRequestException(message)


def parse_speech(route, request: SpeechRequest, payload: Any) -> AudioResponse:
    """解码语音合成响应"""
    config = request.config or TTSConfig()
    if isinstance(payload, httpx.Response):
        return AudioResponse(
            audio_bytes=payload.content,
            audio_format=config.response_format,
            model_name=route.name,
        )
    api = route.preset.get("tts_api")
    try:
        if api == "minimax_t2a":
            if error := _minimax_error(payload, route):
                raise error
            extra = payload.get("extra_info") or {}
            return AudioResponse(
                audio_bytes=bytes.fromhex(payload["data"]["audio"]),
                audio_format=extra.get("audio_format") or "mp3",
                usage=UsageInfo(prompt_tokens=extra.get("usage_characters") or 0),
                model_name=route.name,
                raw_response=payload,
            )
        audio = payload["choices"][0]["message"]["audio"]
        return AudioResponse(
            audio_bytes=base64.b64decode(audio["data"]),
            audio_format="pcm" if config.response_format == "pcm" else "wav",
            usage=_usage(payload),
            model_name=route.name,
        )
    except (KeyError, IndexError, TypeError, ValueError) as e:
        raise ResponseParseException(
            f"[{route.name}] 无法解析语音数据: {e}",
            details={"response": str(payload)[:500]},
        ) from e


def _image_size(config: GenerationConfig) -> str | None:
    media = config.media
    resolution = (media.resolution or "").upper()
    if re.fullmatch(r"\d+X\d+", resolution):
        return resolution.lower()
    if resolution not in _IMAGE_SIZES and not media.aspect_ratio:
        return None
    landscape, portrait, square = _IMAGE_SIZES.get(resolution, _IMAGE_SIZES["1K"])
    ratio = media.aspect_ratio or "1:1"
    if ratio in ("9:16", "2:3", "3:4"):
        return portrait
    if ratio == "1:1":
        return square
    return landscape


def to_image_part(item: Any) -> ImagePart:
    """把参考图 (片段、字节、路径、URL 或 data URI) 转换为图片片段"""
    if isinstance(item, ImagePart):
        return item
    if isinstance(item, bytes):
        return ImagePart(raw=item, mime_type="image/png")
    if isinstance(item, Path):
        return ImagePart(path=item)
    if isinstance(item, str) and item.startswith("data:"):
        data, mime = decode_data_uri(item)
        return ImagePart(raw=data, mime_type=mime)
    if isinstance(item, str):
        return ImagePart(url=item)
    raise InvalidRequestException(f"不支持的参考图类型: {type(item).__name__}")


def _chat_image(route) -> bool:
    return route.preset.get("image_api") == "chat" or "chat" in route.profile.tasks


async def build_image(route, request: ImageRequest) -> HttpCall:
    """编码图像生成请求"""
    config = request.config or GenerationConfig()
    images = [to_image_part(item) for item in request.images or []]
    if _chat_image(route):
        from .openai_chat import build_chat

        chat_config = config.merge_with(
            GenerationConfig(
                output=OutputFormatConfig(response_modalities=["image", "text"])
            )
        )
        message = UserMessage(content=[TextPart(text=request.prompt), *images])
        return await build_chat(
            route, ChatRequest(messages=[message], config=chat_config)
        )
    fields: dict[str, Any] = {"model": route.model.name, "prompt": request.prompt}
    if size := _image_size(config):
        fields["size"] = size
    if config.media.quality:
        fields["quality"] = config.media.quality
    fields = deep_merge(fields, config.params)
    if not images:
        return HttpCall("/images/generations", json=fields)
    name = "image[]" if len(images) > 1 else "image"
    files = [
        (name, (f"image_{i}.png", await part.get_raw_bytes(), "image/png"))
        for i, part in enumerate(images)
    ]
    data = {k: str(v) for k, v in fields.items()}
    return HttpCall("/images/edits", data=data, files=files)


def parse_image(route, request: ImageRequest, payload: dict[str, Any]):
    """解码图像生成响应"""
    if _chat_image(route):
        from .openai_chat import parse_chat

        response = parse_chat(route, ChatRequest(messages=[]), payload)
        parts = response.content_parts
    else:
        parts = [
            output_image(item.get("b64_json") or item["url"])
            for item in payload.get("data") or []
            if item.get("b64_json") or item.get("url")
        ]
    if not any(isinstance(p, ImagePart) for p in parts):
        text = "".join(p.text for p in parts if isinstance(p, TextPart))
        raise NoImageException(
            f"[{route.name}] 响应中没有图片: {text[:200]}",
            details={"response": str(payload)[:500]},
        )
    return ImageResponse(content_parts=parts, raw_response=payload)


def openai_tasks() -> dict[str, tuple[Any, Any]]:
    """OpenAI 兼容协议共用的非对话任务"""
    return {
        "embedding": (build_embedding, parse_embedding),
        "rerank": (build_rerank, parse_rerank),
        "tts": (build_speech, parse_speech),
        "image": (build_image, parse_image),
    }
