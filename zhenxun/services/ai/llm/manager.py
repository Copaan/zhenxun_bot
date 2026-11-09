"""
LLM 模型管理器

负责模型名称与路由组解析、能力档案查询、密钥状态管理与启动关闭，
并在后台定期更新能力库和各提供商的模型列表、核验从未核验过的模型。
"""

import asyncio
import time
from typing import Any

from zhenxun.services.ai.config import ModelConfig, ProviderConfig, get_llm_config
from zhenxun.services.ai.core.exceptions import ConfigurationException
from zhenxun.services.ai.core.models import ModelProfile
from zhenxun.services.ai.utils.logger import log_llm as logger
from zhenxun.utils.manager.priority_manager import PriorityLifecycle
from zhenxun.utils.pydantic_compat import model_dump

from .capability import check_unverified_models, get_observation
from .client import Route
from .discovery import (
    DISCOVERY_MAX_AGE,
    discover_models,
    get_discovery,
    get_snapshot,
    save_snapshots,
)
from .health import health_manager
from .profile import (
    CATALOG_MAX_AGE,
    catalog_status,
    refresh_catalog,
    resolve_profile,
)
from .stats import ModelStats

_group_cache: dict[str, list[str]] = {}
_profile_cache: dict[str, ModelProfile] = {}


def parse_provider_model_string(name: str | None) -> tuple[str | None, str | None]:
    """解析 "提供商/模型" 格式的名称

    参数:
        name: 模型全名

    返回:
        tuple[str | None, str | None]: 提供商名称与模型名称，格式无效时均为 None
    """
    if not name or "/" not in name:
        return None, None
    provider, model = (part.strip() for part in name.split("/", 1))
    return (provider, model) if provider and model else (None, None)


def get_configured_providers() -> list[ProviderConfig]:
    """获取已配置的提供商列表"""
    return get_llm_config().providers


def find_model_config(
    provider_name: str, model_name: str
) -> tuple[ProviderConfig, ModelConfig] | None:
    """查找提供商与模型配置，名称不区分大小写"""
    for provider in get_configured_providers():
        if provider.name.lower() == provider_name.lower():
            model = provider.get_model(model_name)
            return (provider, model) if model else None
    return None


def _resolve_group(name: str, visited: frozenset[str] = frozenset()) -> list[str]:
    if name in visited:
        logger.warning(f"模型路由组嵌套循环: {name}，已跳过")
        return []
    groups = get_llm_config().model_groups
    if name not in groups:
        return []
    result: list[str] = []
    for item in (item.strip() for item in groups[name]):
        if "/" not in item:
            members = _resolve_group(item, visited | {name})
        elif all(parse_provider_model_string(item)) and find_model_config(
            *parse_provider_model_string(item)  # type: ignore[misc]
        ):
            members = [item]
        else:
            logger.warning(f"路由组 '{name}' 中的模型 '{item}' 未配置，已忽略")
            members = []
        result.extend(m for m in members if m not in result)
    return result


def resolve_model_names(name: str) -> list[str]:
    """把模型名或路由组名展开为模型全名列表

    参数:
        name: 模型全名或路由组名

    返回:
        list[str]: 模型全名列表
    """
    name = name.strip()
    if "/" in name:
        return [name]
    if name not in _group_cache:
        _group_cache[name] = _resolve_group(name)
    return _group_cache[name]


def get_default_model(task: str = "chat") -> str | None:
    """获取任务的默认模型名称"""
    return getattr(get_llm_config().default_models, task, None) or None


def get_profile(provider: ProviderConfig, model: ModelConfig) -> ModelProfile:
    """获取模型能力档案 (带缓存)"""
    key = f"{provider.name}/{model.name}"
    if key not in _profile_cache:
        _profile_cache[key] = resolve_profile(
            provider,
            model,
            get_snapshot(provider, model.name),
            get_observation(provider, model),
        )
    return _profile_cache[key]


def resolve_routes(name: str | None, task: str = "chat") -> list[Route]:
    """把模型名或路由组名解析为可调用的线路

    参数:
        name: 模型全名或路由组名，为空时使用任务默认模型
        task: 任务类型

    返回:
        list[Route]: 已启用且支持该任务的线路
    """
    name = name or get_default_model(task)
    if not name:
        raise ConfigurationException(f"尚未配置 {task} 默认模型，请选择模型后再调用。")
    routes = []
    for full_name in resolve_model_names(name):
        found = find_model_config(*parse_provider_model_string(full_name))  # type: ignore[arg-type]
        if not found:
            continue
        provider, model = found
        if not model.enabled:
            continue
        profile = get_profile(provider, model)
        if profile.supports_task(task):
            routes.append(Route(provider, model, profile))
        else:
            logger.debug(f"模型 {full_name} 不支持 {task} 任务，已跳过")
    if not routes:
        raise ConfigurationException(f"'{name}' 没有可用于 {task} 任务的已启用模型")
    return routes


def embeds_modality(name: str | None, modality: str) -> bool:
    """判断 embedding 模型能否把指定模态的内容编码进向量

    参数:
        name: 模型全名或路由组名，为空时使用默认 embedding 模型
        modality: 输入模态，如 image

    返回:
        bool: 首条线路的协议与模型档案都支持该模态时为 True
    """
    try:
        route = resolve_routes(name, "embedding")[0]
    except ConfigurationException:
        return False
    native = route.protocol == "gemini" or route.preset.get("embedding_api") == "jina"
    return native and route.profile.accepts_input(modality)


def check_default_models() -> list[str]:
    """检查各任务的默认模型和对话总结模型能否解析出可用线路

    vision 默认模型按对话任务解析，并要求至少一条线路接受图片输入。

    返回:
        list[str]: 不可用的模型及原因
    """
    config = get_llm_config()
    problems = []
    for task, name in model_dump(config.default_models).items():
        if not name:
            continue
        try:
            routes = resolve_routes(name, "chat" if task == "vision" else task)
        except ConfigurationException as e:
            problems.append(f"{task}: {e}")
            continue
        if task == "vision" and not any(
            route.profile.accepts_input("image") for route in routes
        ):
            problems.append(f"{task}: '{name}' 没有接受图片输入的已启用模型")
    if summary_model := config.context_settings.llm_summary.summarization_model:
        try:
            resolve_routes(summary_model, "chat")
        except ConfigurationException as e:
            problems.append(f"对话总结模型: {e}")
    return problems


def resolve_model_profile(name: str | None = None, task: str = "chat") -> ModelProfile:
    """获取模型 (路由组取首个线路) 的能力档案"""
    return resolve_routes(name, task)[0].profile


def list_available_models() -> list[dict[str, Any]]:
    """列出全部已配置模型及其能力摘要"""
    result = []
    for provider in get_configured_providers():
        for model in provider.models:
            profile = get_profile(provider, model)
            result.append(
                {
                    "provider_name": provider.name,
                    "model_name": model.name,
                    "full_name": f"{provider.name}/{model.name}",
                    "protocol": model.protocol or provider.protocol,
                    "base_url": model.base_url or provider.base_url,
                    "is_available": model.enabled,
                    "tasks": sorted(profile.tasks),
                    "context_window": profile.context_window,
                    "is_embedding_model": profile.supports_task("embedding"),
                }
            )
    return result


async def get_key_usage_stats() -> dict[str, Any]:
    """获取各提供商的密钥使用统计，以脱敏密钥为键"""
    return {
        provider.name: {
            "total_keys": len(provider.api_keys),
            "key_stats": health_manager.key_stats(provider.name, provider.api_keys),
        }
        for provider in get_configured_providers()
    }


def clear_all_cache() -> None:
    """清空路由组与能力档案缓存"""
    _group_cache.clear()
    _profile_cache.clear()


async def refresh_provider_models(
    provider: ProviderConfig, api_keys: list[str] | None = None
) -> tuple[str, dict[str, dict[str, Any]]]:
    """依次尝试密钥查询提供商的模型列表，成功后保存发现结果

    参数:
        provider: 提供商配置
        api_keys: 尝试的密钥，为空时使用提供商的全部密钥

    返回:
        tuple[str, dict[str, dict[str, Any]]]: 成功的密钥与模型名称 -> 能力字段
    """
    keys = api_keys or provider.api_keys
    if not keys:
        raise ConfigurationException(f"提供商 '{provider.name}' 未配置密钥")
    error: Exception | None = None
    for key in keys:
        try:
            models = await discover_models(provider, key)
        except Exception as e:
            error = e
            continue
        save_snapshots(provider, models)
        clear_all_cache()
        return key, models
    assert error is not None
    raise error


_model_refresh_task: asyncio.Task | None = None


async def _refresh_model_data() -> None:
    """按小时检查能力库和提供商快照，失败后保留下一轮重试。"""
    attempted: dict[str, float] = {}
    warned: set[str] = set()
    checked = False

    def failed(key: str, error: Exception) -> None:
        message = f"模型后台刷新失败（{key}），下轮继续重试：{error}"
        (logger.debug if key in warned else logger.warning)(message)
        warned.add(key)

    def recovered(key: str) -> None:
        if key in warned:
            warned.remove(key)
            logger.info(f"模型后台刷新已恢复：{key}")

    while True:
        try:
            now = time.time()
            providers = [
                p for p in get_configured_providers() if p.base_url and p.api_keys
            ]
            recovered("configuration")
            try:
                catalog_age = now - float(catalog_status()["updated_at"] or 0)
                if providers and catalog_age > CATALOG_MAX_AGE:
                    await refresh_catalog()
                    clear_all_cache()
                    logger.info("模型能力库后台刷新完成")
                recovered("catalog")
            except Exception as error:
                failed("catalog", error)
            for provider in providers:
                key = f"provider:{provider.name}"
                try:
                    last = max(
                        float(get_discovery(provider).get("updated_at", 0) or 0),
                        attempted.get(provider.name, 0),
                    )
                    if now - last < DISCOVERY_MAX_AGE:
                        continue
                    await refresh_provider_models(provider)
                    attempted[provider.name] = now
                    recovered(key)
                    logger.info(f"提供商模型列表后台刷新完成：{provider.name}")
                except Exception as error:
                    failed(key, error)
            if not checked:
                try:
                    await check_unverified_models()
                    checked = True
                    recovered("verification")
                except Exception as error:
                    failed("verification", error)
        except asyncio.CancelledError:
            raise
        except Exception as error:
            failed("configuration", error)
        await asyncio.sleep(3600)


async def _shutdown_ai_runtime() -> None:
    """在共享关闭预算内独立清理 AI 资源，并报告未完成步骤。"""
    from zhenxun.services.lifecycle.deadline import remaining_timeout, shutdown_budget

    async def close_mcp():
        from zhenxun.services.ai.tools.providers.mcp.provider import mcp_provider

        await mcp_provider.shutdown()

    async def close_http():
        from .client import close_http_clients

        await close_http_clients()

    steps = (
        ("mcp", close_mcp),
        ("health", health_manager.shutdown),
        ("statistics", ModelStats.shutdown),
        ("http", close_http),
    )
    completed = []
    failures = []
    first_error = None
    with shutdown_budget(10):
        for index, (name, close) in enumerate(steps):
            remaining = remaining_timeout(10)
            if remaining <= 0:
                unfinished = [step[0] for step in steps[index:]]
                logger.warning(f"AI 关闭预算已耗尽，未完成步骤：{unfinished}")
                if first_error is None:
                    first_error = TimeoutError("ai_shutdown_budget_exhausted")
                break
            cleanup = asyncio.create_task(close())
            try:
                await asyncio.wait_for(cleanup, timeout=remaining)
                completed.append(name)
            except asyncio.CancelledError:
                logger.warning(
                    f"AI 关闭被取消，已完成步骤：{completed}，"
                    f"未完成步骤：{[step[0] for step in steps[index:]]}"
                )
                raise
            except Exception as error:
                failures.append(name)
                if first_error is None:
                    first_error = error
                logger.error(f"AI 资源关闭失败（{name}）：{error}", e=error)
                if remaining_timeout(10) <= 0 or (
                    isinstance(error, asyncio.TimeoutError) and cleanup.cancelled()
                ):
                    logger.warning(
                        "AI 关闭预算已耗尽，未完成步骤："
                        f"{[step[0] for step in steps[index:]]}"
                    )
                    break
    if first_error is not None:
        logger.warning(
            f"AI 关闭未全部完成，已完成步骤：{completed}，失败步骤：{failures}"
        )
        raise first_error


async def _cleanup_chat_history() -> None:
    """独立清理短期对话与模型统计，汇总实际清理数和失败原因。"""
    from zhenxun.models.ai_model_stat import AIModelStat
    from zhenxun.services.ai.context.memory.storage import DatabaseChatContext
    from zhenxun.services.scheduler.lifecycle import core_task_run

    async with core_task_run("ai_chat_message_cleanup") as run:
        failures = []
        chat_deleted = statistics_deleted = 0
        try:
            settings = get_llm_config().context_settings
            chat_deleted = await DatabaseChatContext.cleanup(
                settings.history_retention_days, settings.history_max_messages
            )
        except Exception as error:
            failures.append(f"chat:{type(error).__name__}")
            logger.error(f"清理短期对话记忆失败: {error}", e=error)
        try:
            statistics_deleted = await AIModelStat.cleanup()
        except Exception as error:
            failures.append(f"statistics:{type(error).__name__}")
            logger.error(f"清理模型调用统计失败: {error}", e=error)
        run["processed"] = chat_deleted + statistics_deleted
        run["reason"] = (
            f"chat_deleted={chat_deleted},statistics_deleted={statistics_deleted}"
            + (";" + ",".join(failures) if failures else "")
        )
        if failures:
            run["status"] = "failed"


@PriorityLifecycle.on_startup(
    priority=10,
    stage="warmup",
    timeout=60,
    parallel_safe=True,
    failure_policy="degrade",
    component_id="warmup:ai",
    config_keys=("AI", "LLM", "AI_PROVIDER", "AI_TOOLS", "MCP"),
    pass_context=True,
)
async def _init_llm_config_on_startup(context):
    """启动时加载 LLM 配置与密钥状态，初始化工具提供者并开始后台刷新模型数据"""
    from nonebot_plugin_apscheduler import scheduler

    from zhenxun.services.ai.tools.providers.mcp.provider import mcp_provider

    get_llm_config()
    if problems := check_default_models():
        logger.warning(
            "以下模型不可用，相关功能调用时会失败，"
            "可在 WebUI 的 AI 配置页使用“修复失效引用”或重新选择模型:\n"
            + "\n".join(problems)
        )
    context.add_finalizer(_shutdown_ai_runtime)
    scheduler.add_job(
        _cleanup_chat_history,
        "cron",
        hour=4,
        minute=40,
        id="ai_chat_message_cleanup",
        replace_existing=True,
    )
    await health_manager.initialize()
    ModelStats.start()
    try:
        await mcp_provider.initialize()
    except Exception as e:
        logger.error(f"MCP 工具配置初始化失败: {e}", e=e)
    global _model_refresh_task
    if _model_refresh_task is None or _model_refresh_task.done():
        _model_refresh_task = context.spawn_task(
            _refresh_model_data(), name="ai-model-refresh", degrade_on_error=False
        )


@PriorityLifecycle.on_startup(
    priority=20,
    stage="warmup",
    component_id="warmup:ai_sandbox",
    failure_policy="degrade",
    pass_context=True,
)
async def _start_ai_sandbox(context) -> None:
    from zhenxun.services.ai.sandbox.manager import SandboxManager

    context.add_finalizer(SandboxManager.shutdown)
    if get_llm_config().sandbox.enable_sandbox:
        context.spawn_task(
            SandboxManager.prune(), name="sandbox-prune", degrade_on_error=False
        )
