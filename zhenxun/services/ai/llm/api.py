"""
LLM 服务的高级 API 接口 - 便捷函数入口 (无状态)
"""

import asyncio
from collections import OrderedDict
import hashlib
from pathlib import Path
from typing import Any, Literal, TypeVar, overload

from pydantic import BaseModel

from zhenxun.services.ai.config import get_llm_config
from zhenxun.services.ai.core.exceptions import (
    ControlFlowExit,
    LLMException,
    ModelRetry,
    NetworkTimeoutException,
    get_user_friendly_error_message,
)
from zhenxun.services.ai.core.messages import (
    AudioResponse,
    ChatRequest,
    ChatResponse,
    EmbeddingRequest,
    EmbeddingResponse,
    ImagePart,
    ImageRequest,
    ImageResponse,
    LLMMessage,
    PromptInput,
    RerankRequest,
    RerankResult,
    SpeechRequest,
    TextPart,
    UsageInfo,
    UserMessage,
)
from zhenxun.services.ai.core.models import CancellationToken
from zhenxun.services.ai.core.options import (
    GenerationConfig,
    LLMEmbeddingConfig,
    OutputFormatConfig,
    TTSConfig,
)
from zhenxun.services.ai.guardrails import GuardrailSource, parse_guardrails
from zhenxun.services.ai.utils.logger import log_llm as logger
from zhenxun.utils.pydantic_compat import model_copy

from .client import Route, execute
from .manager import get_default_model, resolve_routes

T = TypeVar("T", bound=BaseModel)

_CAPTION_PROMPT = (
    "请客观、详细地描述这张图片的内容，包括其中出现的文字。只输出描述本身。"
)
_caption_cache: OrderedDict[str, str] = OrderedDict()


async def _caption(
    part: ImagePart, model: str, token: CancellationToken | None
) -> str | None:
    """用视觉模型描述一张图片，结果按图片地址或内容缓存

    参数:
        part: 图片片段
        model: 视觉模型
        token: 取消令牌

    返回:
        str | None: 图片描述，失败时为 None
    """
    if part.url and not part.url.startswith("data:"):
        key = part.url
    elif part.url:
        key = hashlib.sha1(part.url.encode()).hexdigest()
    else:
        key = hashlib.sha1(await part.get_raw_bytes()).hexdigest()
    if key in _caption_cache:
        _caption_cache.move_to_end(key)
        return _caption_cache[key]
    request = ChatRequest(
        messages=[UserMessage(content=[TextPart(text=_CAPTION_PROMPT), part])],
        timeout=120,
    )
    try:
        response = await invoke("chat", request, model, token)
    except LLMException as e:
        logger.warning(f"视觉模型 {model} 描述图片失败: {e}")
        return None
    if not (text := response.text.strip()):
        return None
    _caption_cache[key] = text
    if len(_caption_cache) > 256:
        _caption_cache.popitem(last=False)
    return text


async def _describe_images(
    request: Any, routes: list[Route], token: CancellationToken | None
) -> Any:
    """对话线路都不支持图片输入时，用视觉模型 (default_models.vision)
    把请求中的图片替换为文字描述

    参数:
        request: 对话请求
        routes: 对话线路
        token: 取消令牌

    返回:
        Any: 处理后的请求，无需处理时为原请求
    """
    if not isinstance(request, ChatRequest) or any(
        route.profile.accepts_input("image") for route in routes
    ):
        return request
    images = {
        id(part): part
        for message in request.messages
        for part in message.content
        if isinstance(part, ImagePart)
    }
    if not images or not (model := get_default_model("vision")):
        return request
    try:
        vision = resolve_routes(model, "chat")
    except LLMException as e:
        logger.warning(f"视觉模型 {model} 不可用: {e}")
        return request
    if not any(route.profile.accepts_input("image") for route in vision):
        logger.warning(f"视觉模型 {model} 不支持图片输入，已跳过图片描述")
        return request
    results = await asyncio.gather(
        *(_caption(part, model, token) for part in images.values())
    )
    captions = {key: text for key, text in zip(images, results) if text}
    if not captions:
        return request
    messages = []
    for message in request.messages:
        if not any(id(part) in captions for part in message.content):
            messages.append(message)
            continue
        content = [
            TextPart(text=f"<图片描述>{captions[id(part)]}</图片描述>")
            if id(part) in captions
            else part
            for part in message.content
        ]
        messages.append(model_copy(message, update={"content": content}))
    logger.debug(f"已用视觉模型 {model} 描述 {len(captions)} 张图片")
    return model_copy(request, update={"messages": messages})


_EMBED_TASKS = {
    "query": "RETRIEVAL_QUERY",
    "document": "RETRIEVAL_DOCUMENT",
    "similarity": "SEMANTIC_SIMILARITY",
    "classification": "CLASSIFICATION",
    "clustering": "CLUSTERING",
}


async def invoke(
    task: str,
    request: Any,
    model: str | None = None,
    token: CancellationToken | None = None,
) -> Any:
    """解析模型线路并执行请求

    参数:
        task: 任务类型
        request: 统一请求对象
        model: 模型全名或路由组名，为空时使用任务默认模型
        token: 取消令牌

    返回:
        Any: 统一响应对象
    """
    routes = resolve_routes(model, task)
    if task == "chat":
        request = await _describe_images(request, routes, token)
    if not request.timeout:
        return await execute(task, routes, request, token)
    try:
        return await asyncio.wait_for(
            execute(task, routes, request, token), request.timeout
        )
    except asyncio.TimeoutError as e:
        raise NetworkTimeoutException(f"请求超过 {request.timeout} 秒未完成") from e


def _wrap_error(action: str, error: Exception) -> LLMException:
    friendly = get_user_friendly_error_message(error)
    logger.error(f"{action}失败: {error} | 建议: {friendly}", e=error)
    return LLMException(f"{action}失败: {friendly}", cause=error)


async def chat(
    message: PromptInput | list[LLMMessage],
    *,
    model: str | None = None,
    instruction: str | None = None,
    config: GenerationConfig | None = None,
    timeout: float | None = None,
) -> ChatResponse:
    """
    无状态的聊天对话便捷函数，单次执行后立即销毁上下文。

    示例:
        response = await chat("你好", model="OpenAI/gpt-4o", instruction="你是一个助手")
        print(response.text)

    参数:
        message: 用户输入的消息内容，支持多种格式。
        model: 模型全名或路由组名，为空时使用默认模型。
        instruction: 系统指令，用于指导AI的行为和回复风格。
        config: 生成配置。
        timeout: 本次请求的超时时间（秒）。

    返回:
        ChatResponse: 包含AI回复内容、使用信息和工具调用等的完整响应对象。
    """
    from zhenxun.services.ai.message_builder import MessageBuilder

    try:
        messages = await MessageBuilder.normalize_to_llm_messages(
            message, instruction=instruction
        )
    except Exception as e:
        raise _wrap_error("消息构建", e).with_traceback(None) from None
    return await generate(messages, model=model, config=config, timeout=timeout)


@overload
async def embed(
    input_batch: PromptInput,
    *,
    model: str | None = None,
    task: Literal[
        "general", "query", "document", "similarity", "classification", "clustering"
    ] = "general",
    dimensions: int | None = None,
    multimodal: bool | list[str] = False,
    config: LLMEmbeddingConfig | None = None,
) -> EmbeddingResponse: ...


@overload
async def embed(
    input_batch: list[PromptInput],
    *,
    model: str | None = None,
    task: Literal[
        "general", "query", "document", "similarity", "classification", "clustering"
    ] = "general",
    dimensions: int | None = None,
    multimodal: bool | list[str] = False,
    config: LLMEmbeddingConfig | None = None,
) -> EmbeddingResponse: ...


async def embed(
    input_batch: PromptInput | list[PromptInput],
    *,
    model: str | None = None,
    task: Literal[
        "general", "query", "document", "similarity", "classification", "clustering"
    ] = "general",
    dimensions: int | None = None,
    multimodal: bool | list[str] = False,
    config: LLMEmbeddingConfig | None = None,
) -> EmbeddingResponse:
    """
    无状态的向量嵌入便捷函数，支持文本批量与图文多模态融合。

    参数:
        input_batch: 要生成嵌入的内容。传入单条视为单向量；传入多条视为批量。
        model: 嵌入模型全名或路由组名，为空时使用默认模型。
        task: 生成意图 (query 检索词 / document 目标文档 / similarity 相似度 等)。
        dimensions: 要求输出的向量维度。
        multimodal: 是否开启多模态嵌入，默认纯文本。
        config: 嵌入配置。

    返回:
        EmbeddingResponse: 包含向量和 Token 消耗统计的响应对象。
    """
    from zhenxun.services.ai.message_builder import MessageBuilder

    final_config = config or LLMEmbeddingConfig()
    if multimodal is not False:
        final_config.multimodal = multimodal
    if dimensions is not None:
        final_config.output_dimensionality = dimensions
    if task != "general":
        final_config.task_type = _EMBED_TASKS[task]
    try:
        batch = await MessageBuilder.normalize_to_embed_batch(
            input_batch, config=final_config
        )
        if not batch.payloads:
            return EmbeddingResponse(
                embeddings=[], usage=UsageInfo(), model_name=str(model)
            )
        request = EmbeddingRequest(batch=batch, config=final_config)
        return await invoke("embedding", request, model)
    except LLMException as e:
        raise e.with_traceback(None) from None
    except Exception as e:
        raise _wrap_error("文本嵌入", e).with_traceback(None) from None


async def rerank(
    query: str,
    documents: list[str | dict[str, str]],
    top_n: int = 3,
    *,
    model: str | None = None,
) -> list[RerankResult]:
    """
    无状态的文本重排便捷函数。

    参数:
        query: 用户查询问题
        documents: 候选文档列表 (纯文本或含 text 字段的字典)
        top_n: 返回匹配度最高的前 n 个文档
        model: 重排模型全名或路由组名

    返回:
        list[RerankResult]: 按相关度从高到低排列的结果
    """
    try:
        request = RerankRequest(query=query, documents=documents, top_n=top_n)
        response = await invoke("rerank", request, model)
        return response.results
    except LLMException as e:
        raise e.with_traceback(None) from None
    except Exception as e:
        raise _wrap_error("文档重排", e).with_traceback(None) from None


async def generate_structured(
    message: PromptInput | list[LLMMessage],
    response_model: type[T],
    *,
    guardrails: list[GuardrailSource] | None = None,
    model: str | None = None,
    config: GenerationConfig | None = None,
    max_retries: int | None = None,
    error_prompt_template: str | None = None,
    instruction: str | None = None,
    timeout: float | None = None,
) -> T:
    """
    请求大模型生成结构化数据，并自动验证/解析为指定的 Pydantic 模型。

    示例:
        class UserInfo(BaseModel):
            name: str
        info = await generate_structured("提取张三的信息", response_model=UserInfo)

    参数:
        message: 输入的消息内容，支持纯文本、UniMessage、消息对象列表等。
        response_model: 目标结构化输出的 Pydantic 模型类。
        guardrails: 护栏来源列表，支持自然语言规则、自定义校验函数。
        model: 模型全名或路由组名，为空时使用默认模型。
        config: 生成配置。
        max_retries: 解析或护栏校验失败时的最大自我修正次数，为空时使用全局配置。
        error_prompt_template: 校验失败时引导模型自我修正的提示词模板。
        instruction: 系统指令。
        timeout: 本次请求的超时时间（秒）。

    返回:
        T: 解析验证通过后的模型实例。
    """
    from zhenxun.services.ai.capabilities.builtin import ReflexionCapability
    from zhenxun.services.ai.core.engine.structured_parser import (
        BaseOutputProcessor,
    )
    from zhenxun.services.ai.message_builder import MessageBuilder

    try:
        processor = BaseOutputProcessor(
            response_model=response_model, error_template=error_prompt_template
        )
        structured = GenerationConfig(
            output=OutputFormatConfig(
                response_format="json", response_schema=processor.get_json_schema()
            )
        )
        messages = await MessageBuilder.normalize_to_llm_messages(
            message if message is not None else [], instruction=instruction
        )
        if max_retries is None:
            max_retries = get_llm_config().client_settings.structured_retries
        response = await generate(
            messages,
            model=model,
            config=structured.merge_with(config),
            timeout=timeout,
            extra={
                "output_processor": processor,
                "guardrails": parse_guardrails(guardrails),
                "max_retries": max_retries,
                "__sys_capabilities": [ReflexionCapability()],
            },
        )
    except (LLMException, ModelRetry, ControlFlowExit) as e:
        raise e.with_traceback(None) from None
    except Exception as e:
        raise _wrap_error("生成结构化响应", e).with_traceback(None) from None
    if response.parsed_obj is None:
        raise LLMException("结构化输出失败：未得到解析后的对象。")
    return response.parsed_obj


async def generate(
    messages: list[LLMMessage],
    *,
    model: str | None = None,
    config: GenerationConfig | None = None,
    timeout: float | None = None,
    extra: dict[str, Any] | None = None,
    token: CancellationToken | None = None,
) -> ChatResponse:
    """
    直接传入消息列表生成响应，一般业务插件推荐使用 `chat`。

    参数:
        messages: 完整的消息历史列表。
        model: 模型全名或路由组名，为空时使用默认模型。
        config: 生成配置，与提供商和模型参数合并后生效。
        timeout: 本次请求的超时时间（秒）。
        extra: 透传给能力钩子的额外数据。
        token: 取消令牌。

    返回:
        ChatResponse: 包含AI回复内容、使用信息和工具调用等的完整响应对象。
    """
    request = ChatRequest(
        messages=messages, config=config, timeout=timeout, extra=extra or {}
    )
    capabilities = request.extra.pop("__sys_capabilities", [])
    run_context = request.extra.pop("run_context", None)
    try:
        if not capabilities:
            return await invoke("chat", request, model, token)

        from zhenxun.services.ai.capabilities import CombinedCapability
        from zhenxun.services.ai.core.models import LLMContext
        from zhenxun.services.ai.run import RunContext

        async def handler(ctx: LLMContext[ChatRequest, ChatResponse]) -> ChatResponse:
            return await invoke("chat", ctx.request, model, token)

        return await CombinedCapability(capabilities).wrap_model_request(
            run_context or RunContext(), LLMContext(request=request), handler
        )
    except (LLMException, ModelRetry, ControlFlowExit) as e:
        raise e.with_traceback(None) from None
    except Exception as e:
        raise _wrap_error("生成响应", e).with_traceback(None) from None


@overload
async def create_image(
    prompt: PromptInput,
    *,
    images: None = None,
    model: str | None = None,
    config: GenerationConfig | None = None,
) -> ImageResponse:
    """根据文本提示生成一张新图片。"""
    ...


@overload
async def create_image(
    prompt: PromptInput,
    *,
    images: list[Path | bytes | str] | Path | bytes | str,
    model: str | None = None,
    config: GenerationConfig | None = None,
) -> ImageResponse:
    """在给定图片的基础上，根据文本提示进行编辑或重新生成。"""
    ...


async def create_image(
    prompt: PromptInput,
    *,
    images: list[Path | bytes | str] | Path | bytes | str | None = None,
    model: str | None = None,
    config: GenerationConfig | None = None,
) -> ImageResponse:
    """
    多模态图片生成/编辑函数。

    示例:
        res = await create_image("画一只猫", model="OpenAI/gpt-image-1")
        img_bytes = res.images[0]

    说明:
        - 若 `images` 为 None，执行文本生成图片。
        - 若提供 `images`，执行图像编辑。
    """
    text = getattr(prompt, "extract_plain_text", lambda: str(prompt))()
    image_list = images if isinstance(images, list) else [images] if images else []
    try:
        request = ImageRequest(
            prompt=text, images=image_list or None, config=config or GenerationConfig()
        )
        return await invoke("image", request, model)
    except LLMException as e:
        raise e.with_traceback(None) from None
    except Exception as e:
        raise _wrap_error("图片生成", e).with_traceback(None) from None


async def create_speech(
    text: str,
    voice: str | None = None,
    *,
    model: str | None = None,
    config: TTSConfig | None = None,
) -> AudioResponse:
    """
    通用文本转语音便捷函数。

    参数:
        text: 待合成的文本内容。
        voice: 音色，为空时使用模型的默认音色。
        model: 语音模型全名或路由组名。
        config: 语音合成配置。

    示例:
        res = await create_speech("你好，世界", voice="alloy", model="OpenAI/tts-1")
        Path("out.mp3").write_bytes(res.audio_bytes)
    """
    if not text:
        raise LLMException("TTS 输入文本不能为空")
    try:
        request = SpeechRequest(input_text=text, voice=voice, config=config)
        return await invoke("tts", request, model)
    except LLMException as e:
        raise e.with_traceback(None) from None
    except Exception as e:
        raise _wrap_error("语音生成", e).with_traceback(None) from None
