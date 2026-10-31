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


async def _refresh_model_data() -> None:
    """定期更新能力库，刷新超过一天未查询的提供商模型列表，
    首轮刷新后核验从未核验过的模型"""
    attempted: dict[str, float] = {}
    catalog_warned = False
    checked = False
    while True:
        now = time.time()
        providers = [p for p in get_configured_providers() if p.base_url and p.api_keys]
        catalog_age = now - (catalog_status()["updated_at"] or 0)
        if providers and catalog_age > CATALOG_MAX_AGE:
            try:
                await refresh_catalog()
                clear_all_cache()
            except Exception as e:
                message = f"模型能力库更新失败，可在 AI 设置中更换能力库地址: {e}"
                if catalog_warned:
                    logger.debug(message)
                else:
                    logger.warning(message)
                catalog_warned = True
        for provider in providers:
            last = max(
                get_discovery(provider).get("updated_at", 0),
                attempted.get(provider.name, 0),
            )
            if now - last < DISCOVERY_MAX_AGE:
                continue
            attempted[provider.name] = now
            try:
                await refresh_provider_models(provider)
            except Exception as e:
                logger.debug(f"提供商 '{provider.name}' 的模型列表刷新失败: {e}")
        if not checked:
            checked = True
            try:
                await check_unverified_models()
            except Exception as e:
                logger.warning(f"模型能力核验未完成: {e}")
        await asyncio.sleep(3600)


async def _shutdown_ai_runtime() -> None:
    """关闭 MCP 工具提供者、保存密钥状态并关闭连接池"""
    from zhenxun.services.ai.tools.providers.mcp.provider import mcp_provider

    from .client import close_http_clients

    await mcp_provider.shutdown()
    await health_manager.shutdown()
    await ModelStats.shutdown()
    await close_http_clients()


async def _cleanup_chat_history() -> None:
    """按配置清理过期和超出条数的短期对话记忆，以及过期的模型调用统计"""
    from zhenxun.models.ai_model_stat import AIModelStat
    from zhenxun.services.ai.context.memory.storage import DatabaseChatContext

    settings = get_llm_config().context_settings
    try:
        deleted = await DatabaseChatContext.cleanup(
            settings.history_retention_days, settings.history_max_messages
        )
    except Exception as e:
        logger.error(f"清理短期对话记忆失败: {e}", e=e)
        return
    if deleted:
        logger.info(f"已清理 {deleted} 条短期对话记忆")
    try:
        deleted = await AIModelStat.cleanup()
    except Exception as e:
        logger.error(f"清理模型调用统计失败: {e}", e=e)
        return
    if deleted:
        logger.info(f"已清理 {deleted} 条过期的模型调用统计")


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
    context.spawn_task(
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
