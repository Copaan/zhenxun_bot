"""AI 模型发现、能力库更新与模型测试、核验"""

from __future__ import annotations

import asyncio
import time
from typing import Any, Literal

from fastapi import APIRouter, HTTPException
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse
import httpx
from pydantic import BaseModel, Field

from zhenxun.services.ai.config import get_llm_config
from zhenxun.services.ai.config.models import ModelConfig, ProviderConfig
from zhenxun.services.ai.core.exceptions import (
    AuthenticationException,
    ConfigurationException,
    EmptyResponseException,
    InvalidRequestException,
    LLMException,
    LocationNotSupportedException,
    NoImageException,
    QuotaExceededException,
    RateLimitException,
    ResponseParseException,
)
from zhenxun.services.ai.llm.capability import (
    CHECK_ITEMS,
    DEFAULT_CHECK_ITEMS,
    check_status,
    minimal_request,
    probe_once,
    run_check,
)
from zhenxun.services.ai.llm.client import Route
from zhenxun.services.ai.llm.discovery import discover_models as fetch_models
from zhenxun.services.ai.llm.discovery import save_snapshots
from zhenxun.services.ai.llm.health import health_manager
from zhenxun.services.ai.llm.manager import clear_all_cache
from zhenxun.services.ai.llm.profile import refresh_catalog
from zhenxun.services.log import logger
from zhenxun.utils.pydantic_compat import model_dump, parse_as

from ....base_model import Result
from ....utils import authentication
from ._store import (
    clean_keys,
    config_revision,
    discovered_profile,
    draft_profile,
    load_config,
    read_config,
    validate_ai,
)

router = APIRouter()

_DISCOVERY_TIMEOUT = 12
_TEST_TIMEOUT = 60


class ProviderDiscoveryRequest(BaseModel):
    expected_revision: str | None = Field(default=None, min_length=64, max_length=64)
    provider_name: str | None = None
    saved_provider_name: str | None = Field(default=None, max_length=80)
    preset: str | None = None
    protocol: str | None = None
    base_url: str | None = None
    api_key: str | None = None


class ModelTestRequest(BaseModel):
    model: str
    task: Literal["chat", "embedding", "rerank", "image", "tts", "stt"] = "chat"
    confirmed_paid_request: bool = False
    expected_revision: str | None = Field(default=None, min_length=64, max_length=64)
    provider_name: str | None = Field(default=None, max_length=80)
    saved_provider_name: str | None = Field(default=None, max_length=80)
    preset: str | None = Field(default=None, max_length=64)
    protocol: str | None = Field(default=None, max_length=64)
    base_url: str | None = Field(default=None, max_length=500)
    api_key: str | None = None
    timeout: int | None = Field(default=None, ge=1, le=1800)
    model_settings: dict[str, Any] | None = Field(default=None, alias="model_config")


class ModelCheckRequest(ModelTestRequest):
    items: list[str] = Field(default_factory=lambda: list(DEFAULT_CHECK_ITEMS))


def _probe_error(status_code: int, code: str, message: str) -> HTTPException:
    return HTTPException(
        status_code=status_code, detail={"code": code, "message": message}
    )


_PROBE_ERRORS: tuple[tuple[Any, int, str, str], ...] = (
    (
        LocationNotSupportedException,
        422,
        "provider_location_unsupported",
        "服务商不支持当前出口所在地区。",
    ),
    (
        AuthenticationException,
        422,
        "provider_credentials_invalid",
        "服务商拒绝了当前凭据。",
    ),
    (
        RateLimitException | QuotaExceededException,
        429,
        "provider_rate_limited",
        "服务商当前限流或额度不可用。",
    ),
    (
        asyncio.TimeoutError | httpx.TimeoutException,
        504,
        "provider_timeout",
        "连接服务商超时。",
    ),
    (httpx.NetworkError, 502, "provider_unreachable", "无法连接到服务商。"),
    (EmptyResponseException, 502, "provider_empty_response", "模型返回了空回复。"),
    (NoImageException, 502, "provider_no_image", "模型没有生成图片。"),
    (
        ResponseParseException,
        502,
        "provider_response_parse_failed",
        "服务商响应与所选 API 协议不匹配。",
    ),
    (
        ConfigurationException,
        422,
        "provider_protocol_mismatch",
        "所选 API 协议、地址或模型配置不匹配。",
    ),
)


def _map_probe_error(error: Exception) -> HTTPException:
    """把服务商探测异常转换为前端可读的错误

    参数:
        error: 探测异常

    返回:
        HTTPException: 带错误码与说明的异常
    """
    from zhenxun.services.network_proxy import ProxyPolicyError, ProxyRequestError

    if isinstance(error, ProxyPolicyError | ProxyRequestError):
        return _probe_error(
            502, error.code, "代理配置、连接或认证失败，请检查网络代理设置。"
        )
    if isinstance(error, LLMException) and error.details.get("status_code") == 404:
        return _probe_error(404, "model_not_found", "服务商未找到指定模型或接口。")
    if isinstance(error, InvalidRequestException):
        fields = [
            name
            for name in ("maxOutputTokens", "thinkingConfig", "safetySettings", "tools")
            if name in str(error) or name in str(getattr(error, "details", {}))
        ]
        return _probe_error(
            422,
            "provider_request_invalid",
            "服务商拒绝了请求参数，请检查模型参数及服务商限制。"
            + (f" 涉及字段：{', '.join(fields)}。" if fields else ""),
        )
    for types, status_code, code, message in _PROBE_ERRORS:
        if isinstance(error, types):
            return _probe_error(status_code, code, message)
    return _probe_error(
        502, "provider_request_failed", "服务商请求失败，请查看后台脱敏日志。"
    )


def _saved_provider(
    name: str | None, expected_revision: str | None
) -> ProviderConfig | None:
    """查找已保存的服务商，提供修订号时读取配置文件并校验修订号

    参数:
        name: 服务商名称
        expected_revision: 前端持有的配置修订号

    返回:
        ProviderConfig | None: 已保存的服务商
    """
    if expected_revision is None:
        config = get_llm_config()
    else:
        content = read_config()
        if config_revision(content) != expected_revision:
            raise HTTPException(
                status_code=409,
                detail={
                    "code": "configuration_revision_conflict",
                    "message": "AI 配置已变化，请重新加载后再测试。",
                },
            )
        config = validate_ai(load_config(content)["AI"], strict_references=False)
    if not name:
        return None
    return next(
        (item for item in config.providers if item.name.casefold() == name.casefold()),
        None,
    )


async def _probe_target(
    *,
    provider_name: str | None,
    saved_provider_name: str | None,
    preset: str | None,
    protocol: str | None,
    base_url: str | None,
    api_key: str | None,
    expected_revision: str | None,
    timeout: int | None = None,
) -> tuple[ProviderConfig, list[str], str | None]:
    """确定探测使用的服务商与候选密钥

    已保存的密钥只会发往该服务商保存的协议和地址；使用已保存密钥时
    可用密钥优先，否则只使用临时密钥。

    参数:
        provider_name: 草稿中的服务商名称
        saved_provider_name: 已保存的服务商名称
        preset: 草稿中的厂商预设
        protocol: 草稿中的接口协议
        base_url: 草稿中的接口地址
        api_key: 临时密钥
        expected_revision: 前端持有的配置修订号
        timeout: 请求超时

    返回:
        tuple[ProviderConfig, list[str], str | None]: 服务商、候选密钥，
            以及使用已保存密钥时用于记录健康状态的服务商名称
    """
    saved = _saved_provider(saved_provider_name or provider_name, expected_revision)
    if preset is None and saved is not None:
        preset = saved.preset
    if base_url is None:
        base_url = saved.base_url if saved else ""
    try:
        provider = ProviderConfig(
            name=str(provider_name or (saved.name if saved else "draft-provider")),
            preset=preset,
            protocol=protocol or (saved.protocol if saved else ""),
            base_url=base_url,
            headers=saved.headers if saved else {},
            timeout=int(timeout or (saved.timeout if saved else _DISCOVERY_TIMEOUT)),
        )
    except ValueError as error:
        raise _probe_error(
            422, "provider_protocol_mismatch", "所选预设、协议或地址无效。"
        ) from error
    temporary_key = str(api_key or "").strip()
    scope_matches = bool(
        saved is not None
        and provider.protocol == saved.protocol
        and provider.base_url == saved.base_url
    )
    if saved is not None and not temporary_key and not scope_matches:
        raise _probe_error(
            422,
            "provider_credentials_scope_mismatch",
            "修改协议或接口地址后，请填写临时 API Key 再测试。",
        )
    saved_keys = (
        clean_keys(saved.api_keys) if saved is not None and scope_matches else []
    )
    selected_key = temporary_key or next(iter(saved_keys), "")
    if not provider.base_url or not selected_key:
        raise _probe_error(
            422,
            "provider_credentials_incomplete",
            "请填写接口地址和有效 API Key。",
        )
    if saved is None or selected_key not in saved_keys:
        return provider, [selected_key], None
    available = health_manager.available_keys(saved.name, saved_keys, set())
    keys = [*available, *(key for key in saved_keys if key not in available)]
    if temporary_key:
        keys = [temporary_key, *(key for key in keys if key != temporary_key)]
    return provider, keys, saved.name


async def _try_keys(
    keys: list[str], health_name: str | None, call: Any
) -> tuple[str, Any]:
    """依次用候选密钥发起请求，凭据被拒绝时换下一个

    参数:
        keys: 候选密钥
        health_name: 记录密钥健康状态的服务商名称，为空时不记录
        call: 接收密钥并发起请求的协程函数

    返回:
        tuple[str, Any]: 成功的密钥与请求结果
    """
    settings = get_llm_config().client_settings
    cooldowns = {"rate": settings.rate_limit_cooldown, "quota": settings.quota_cooldown}
    rejected: AuthenticationException | None = None
    for key in keys:
        try:
            return key, await call(key)
        except LLMException as error:
            if health_name:
                health_manager.record_key_failure(health_name, key, error, cooldowns)
            if not isinstance(error, AuthenticationException):
                raise
            rejected = error
    assert rejected is not None
    raise rejected


def _draft_route(
    provider: ProviderConfig,
    model_name: str,
    model_settings: dict[str, Any] | None,
    task: str,
) -> Route:
    """按草稿中的模型配置生成线路

    参数:
        provider: 服务商配置
        model_name: 模型名称
        model_settings: 草稿中的模型配置
        task: 测试的任务类型

    返回:
        Route: 线路
    """
    settings = {
        key: value
        for key, value in (model_settings or {}).items()
        if key in ModelConfig.model_fields
    }
    model = parse_as(ModelConfig, {**settings, "name": model_name})
    route = Route(provider, model, draft_profile(provider, model))
    if not route.profile.supports_task(task):
        raise _probe_error(
            422,
            "model_task_unsupported",
            f"该模型的能力档案未包含 {task} 任务，可在模型能力中调整。",
        )
    return route


async def _model_target(
    payload: ModelTestRequest,
) -> tuple[Route, list[str], str | None]:
    """校验付费确认与模型名称，确定测试使用的线路与候选密钥

    参数:
        payload: 模型测试请求

    返回:
        tuple[Route, list[str], str | None]: 线路、候选密钥与记录健康状态的服务商名称
    """
    if not payload.confirmed_paid_request:
        raise HTTPException(
            status_code=409,
            detail={
                "code": "paid_test_confirmation_required",
                "message": "模型测试可能产生费用，请确认后重试。",
            },
        )
    if "/" not in payload.model:
        raise HTTPException(
            status_code=422,
            detail={
                "code": "model_name_invalid",
                "message": "模型名称必须包含服务商前缀。",
            },
        )
    model_provider_name, model_name = payload.model.split("/", 1)
    provider, keys, health_name = await _probe_target(
        provider_name=payload.provider_name or model_provider_name,
        saved_provider_name=payload.saved_provider_name,
        preset=payload.preset,
        protocol=payload.protocol,
        base_url=payload.base_url,
        api_key=payload.api_key,
        expected_revision=payload.expected_revision,
        timeout=min(payload.timeout or _TEST_TIMEOUT, _TEST_TIMEOUT),
    )
    route = _draft_route(provider, model_name, payload.model_settings, payload.task)
    return route, keys, health_name


async def _run_model_probe(
    payload: ModelTestRequest, label: str, call: Any
) -> tuple[Route, Any, float]:
    """依次用候选密钥执行模型测试，记录密钥与线路健康状态

    参数:
        payload: 模型测试请求
        label: 日志中的操作名称
        call: 接收线路与密钥并发起请求的协程函数

    返回:
        tuple[Route, Any, float]: 线路、请求结果与耗时 (毫秒)
    """
    route, keys, health_name = await _model_target(payload)
    route_name = f"{health_name}/{route.model.name}"
    started = time.monotonic()
    try:
        key, result = await _try_keys(keys, health_name, lambda key: call(route, key))
    except HTTPException:
        raise
    except Exception as error:
        logger.warning(
            f"AI 模型{label}失败 | task={payload.task} | "
            f"error={error.__class__.__name__}",
            "WebUi",
        )
        if health_name and isinstance(error, LLMException | httpx.HTTPError):
            health_manager.record_route_failure(
                route_name, error, get_llm_config().client_settings.route_cooldown
            )
        raise _map_probe_error(error) from error
    latency_ms = (time.monotonic() - started) * 1000
    if health_name:
        health_manager.record_key_success(health_name, key)
        health_manager.record_route_success(route_name)
    return route, result, latency_ms


@router.post(
    "/providers/discover",
    dependencies=[authentication()],
    response_model=Result,
    response_class=JSONResponse,
)
async def discover_models(payload: ProviderDiscoveryRequest) -> Result:
    provider, keys, health_name = await _probe_target(
        provider_name=payload.provider_name,
        saved_provider_name=payload.saved_provider_name,
        preset=payload.preset,
        protocol=payload.protocol,
        base_url=payload.base_url,
        api_key=payload.api_key,
        expected_revision=payload.expected_revision,
    )
    started = time.monotonic()
    try:
        key, models = await _try_keys(
            keys, health_name, lambda key: fetch_models(provider, key)
        )
    except HTTPException:
        raise
    except (LLMException, httpx.HTTPError) as error:
        logger.warning(
            "AI Provider 探测失败 | "
            f"protocol={provider.protocol} | error={error.__class__.__name__}",
            "WebUi",
        )
        raise _map_probe_error(error) from error
    latency = round((time.monotonic() - started) * 1000)
    if health_name:
        health_manager.record_key_success(health_name, key)
    saved = _saved_provider(payload.saved_provider_name or payload.provider_name, None)
    if saved is None or (saved.protocol, saved.base_url) == (
        provider.protocol,
        provider.base_url,
    ):
        save_snapshots(provider, models)
        clear_all_cache()
    logger.info(
        "AI Provider 探测成功 | "
        f"protocol={provider.protocol} | models={len(models)} | latency={latency}ms",
        "WebUi",
    )
    return Result.ok(
        {
            "models": sorted(models),
            "profiles": {
                name: discovered_profile(provider, name, snapshot)
                for name, snapshot in models.items()
            },
            "latency_ms": latency,
        }
    )


@router.post(
    "/catalog/refresh",
    dependencies=[authentication()],
    response_model=Result,
    response_class=JSONResponse,
)
async def update_catalog() -> Result:
    try:
        status = await refresh_catalog()
    except Exception as error:
        logger.warning(f"模型能力库更新失败: {error}", "WebUi")
        raise HTTPException(
            status_code=502,
            detail={
                "code": "catalog_refresh_failed",
                "message": f"能力库下载失败，请检查网络代理或能力库地址: {error}",
            },
        ) from error
    clear_all_cache()
    return Result.ok(status)


@router.post(
    "/models/test",
    dependencies=[authentication()],
    response_model=Result,
    response_class=JSONResponse,
)
async def test_model(payload: ModelTestRequest) -> Result:
    async def call(route: Route, key: str) -> None:
        await probe_once(
            route, payload.task, minimal_request(payload.task, route.protocol), key
        )

    _, _, latency_ms = await _run_model_probe(payload, "测试", call)
    return Result.ok(
        {"ok": True, "latency_ms": round(latency_ms), "task": payload.task}
    )


@router.post(
    "/models/check",
    dependencies=[authentication()],
    response_model=Result,
    response_class=JSONResponse,
)
async def check_model(payload: ModelCheckRequest) -> Result:
    items = [item for item in payload.items if item in CHECK_ITEMS]
    route, results, latency_ms = await _run_model_probe(
        payload, "核验", lambda route, key: run_check(route, key, items)
    )
    return Result.ok(
        {
            "latency_ms": round(latency_ms),
            "results": results,
            "check": check_status(route.provider, route.model),
            "resolved_profile": jsonable_encoder(
                model_dump(draft_profile(route.provider, route.model))
            ),
        }
    )
