from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
import contextlib
from datetime import datetime
from pathlib import Path
import time
from typing import Literal

import nonebot
from redis.asyncio import Redis
from tortoise import Tortoise

from zhenxun.builtin_plugins.web_ui.api.configure.setup_access import setup_access
from zhenxun.builtin_plugins.web_ui.api.protocol import build_protocol_status
from zhenxun.builtin_plugins.web_ui.security import authenticated_websocket_count
from zhenxun.services.cache import cache_config
from zhenxun.services.cache.config import CacheMode
from zhenxun.services.cache.runtime_cache import health_snapshot
from zhenxun.services.log import logger
from zhenxun.services.message_load import is_db_unhealthy

from ..main.data_source import bot_live
from .model import (
    RuntimeBotStatus,
    RuntimeIssue,
    RuntimeOverview,
    RuntimeProcessStatus,
    RuntimeProtocolStatus,
    RuntimeServiceStatus,
    RuntimeWebSocketStatus,
)

_PROBE_TTL_SECONDS = 30.0
_PROBE_TIMEOUT_SECONDS = 2.0
_STARTED_AT = time.monotonic()
_probe_results: tuple[RuntimeServiceStatus, RuntimeServiceStatus] | None = None
_probe_updated_at = 0.0
_probe_task: asyncio.Task[tuple[RuntimeServiceStatus, RuntimeServiceStatus]] | None = (
    None
)
_probe_lock = asyncio.Lock()


def _version() -> str:
    path = Path("__version__")
    with contextlib.suppress(OSError):
        value = path.read_text(encoding="utf-8").strip()
        return value.replace("__version__:", "").strip() or "unknown"
    return "unknown"


async def _timed_probe(
    probe: Callable[[], Awaitable[None]],
    *,
    label: str,
    ready_code: str,
    error_code: str,
    mode: str | None = None,
) -> RuntimeServiceStatus:
    started_at = time.perf_counter()
    try:
        await asyncio.wait_for(probe(), timeout=_PROBE_TIMEOUT_SECONDS)
    except Exception as error:
        logger.warning(f"WebUI runtime {label} probe failed", "WebUi", e=error)
        return RuntimeServiceStatus(
            status="critical" if label == "数据库" else "warning",
            code=error_code,
            label=label,
            detail=f"{label}连接检查失败。",
            latency_ms=round((time.perf_counter() - started_at) * 1000),
            mode=mode,
        )
    return RuntimeServiceStatus(
        status="ok",
        code=ready_code,
        label=label,
        detail=f"{label}连接正常。",
        latency_ms=round((time.perf_counter() - started_at) * 1000),
        mode=mode,
    )


async def _probe_database() -> RuntimeServiceStatus:
    async def select_one() -> None:
        connection = Tortoise.get_connection("default")
        await connection.execute_query("SELECT 1")

    result = await _timed_probe(
        select_one,
        label="数据库",
        ready_code="database_ready",
        error_code="database_unavailable",
    )
    if is_db_unhealthy() and result.status == "ok":
        return RuntimeServiceStatus(
            status="warning",
            code="database_recovering",
            label="数据库",
            detail="数据库已响应，运行时仍处于恢复观察期。",
            latency_ms=result.latency_ms,
        )
    return result


def _runtime_cache_has_errors() -> bool:
    return any(item.get("last_error") for item in health_snapshot().values())


async def _probe_cache() -> RuntimeServiceStatus:
    mode = str(cache_config.cache_mode or CacheMode.NONE).upper()
    if mode == CacheMode.NONE:
        return RuntimeServiceStatus(
            status="ok",
            code="cache_disabled",
            label="缓存",
            detail="缓存模式为 NONE，运行时直接读取数据库。",
            mode=mode,
        )
    if mode == CacheMode.MEMORY:
        has_errors = _runtime_cache_has_errors()
        return RuntimeServiceStatus(
            status="warning" if has_errors else "ok",
            code="cache_runtime_error" if has_errors else "cache_memory_ready",
            label="缓存",
            detail=(
                "部分运行时缓存最近刷新失败。" if has_errors else "内存缓存运行正常。"
            ),
            mode=mode,
        )
    if not cache_config.redis_host:
        return RuntimeServiceStatus(
            status="warning",
            code="redis_host_missing",
            label="缓存",
            detail="Redis模式已启用，但未配置连接地址。",
            mode=mode,
        )

    client = Redis(
        host=cache_config.redis_host,
        port=cache_config.redis_port or 6379,
        password=cache_config.redis_password or None,
        decode_responses=True,
    )

    async def ping() -> None:
        await client.ping()

    try:
        return await _timed_probe(
            ping,
            label="缓存",
            ready_code="redis_ready",
            error_code="redis_unavailable",
            mode=mode,
        )
    finally:
        with contextlib.suppress(Exception):
            await client.aclose()


async def _run_probes() -> tuple[RuntimeServiceStatus, RuntimeServiceStatus]:
    async def safe_probe(
        probe: Callable[[], Awaitable[RuntimeServiceStatus]],
        *,
        label: str,
        code: str,
        status: Literal["warning", "critical"],
    ) -> RuntimeServiceStatus:
        try:
            return await probe()
        except Exception as error:
            logger.warning(f"WebUI runtime {label} probe failed", "WebUi", e=error)
            return RuntimeServiceStatus(
                status=status,
                code=code,
                label=label,
                detail=f"{label}状态暂时无法检查。",
            )

    database, cache = await asyncio.gather(
        safe_probe(
            _probe_database,
            label="数据库",
            code="database_probe_failed",
            status="critical",
        ),
        safe_probe(
            _probe_cache,
            label="缓存",
            code="cache_probe_failed",
            status="warning",
        ),
    )
    return database, cache


async def _get_probes(
    force: bool,
) -> tuple[RuntimeServiceStatus, RuntimeServiceStatus]:
    global _probe_results, _probe_task, _probe_updated_at
    now = time.monotonic()
    if (
        not force
        and _probe_results is not None
        and now - _probe_updated_at < _PROBE_TTL_SECONDS
    ):
        return _probe_results
    async with _probe_lock:
        now = time.monotonic()
        if (
            not force
            and _probe_results is not None
            and now - _probe_updated_at < _PROBE_TTL_SECONDS
        ):
            return _probe_results
        if _probe_task is None or _probe_task.done():
            _probe_task = asyncio.create_task(_run_probes())
        task = _probe_task
    results = await task
    async with _probe_lock:
        if _probe_task is task:
            _probe_results = results
            _probe_updated_at = time.monotonic()
            _probe_task = None
    return results


def _issue(
    code: str,
    severity: Literal["warning", "critical"],
    title: str,
    detail: str,
    action_label: str,
    action_route: str,
) -> RuntimeIssue:
    return RuntimeIssue(
        code=code,
        severity=severity,
        title=title,
        detail=detail,
        action_label=action_label,
        action_route=action_route,
    )


async def build_runtime_overview(force: bool = False) -> RuntimeOverview:
    database, cache = await _get_probes(force)
    driver_config = nonebot.get_driver().config
    protocol_error = False
    try:
        protocol = build_protocol_status()
    except Exception as error:
        logger.warning("WebUI runtime protocol status probe failed", "WebUi", e=error)
        protocol_error = True
        protocol = None
    bots = [
        RuntimeBotStatus(
            self_id=item.self_id,
            platform=item.platform,
            adapter=item.adapter,
            connect_seconds=max(
                0, int(time.time() - (bot_live.get(item.self_id) or time.time()))
            ),
        )
        for item in (protocol.connections if protocol else [])
    ]
    issues: list[RuntimeIssue] = []
    if database.status != "ok":
        issues.append(
            _issue(
                database.code,
                database.status,
                "数据库需要检查",
                database.detail,
                "查看系统状态",
                "/system",
            )
        )
    if cache.status != "ok":
        issues.append(
            _issue(
                cache.code,
                "warning",
                "缓存状态异常",
                cache.detail,
                "查看系统状态",
                "/system",
            )
        )
    if protocol_error:
        issues.append(
            _issue(
                "protocol_status_unavailable",
                "warning",
                "协议状态暂时不可用",
                "运行中心无法读取协议连接状态。",
                "重新检查",
                "/dashboard",
            )
        )
    elif not bots:
        issues.append(
            _issue(
                "protocol_not_connected",
                "warning",
                "尚无协议端连接",
                "真寻已启动，但当前没有机器人连接。",
                "配置协议端",
                "/protocol",
            )
        )
    elif (
        protocol and protocol.qq_official_enabled and not protocol.qq_official_connected
    ):
        issues.append(
            _issue(
                "qq_official_disconnected",
                "warning",
                "QQ官方机器人未连接",
                "QQ官方适配已启用，但当前未建立连接。",
                "查看协议状态",
                "/protocol",
            )
        )
    restart_pending = setup_access.state() == "restart_pending"
    if restart_pending:
        issues.append(
            _issue(
                "restart_pending",
                "warning",
                "配置等待重启",
                "已保存的运行配置需要重启后生效。",
                "查看系统状态",
                "/system",
            )
        )
    overall_status = "ok"
    if any(item.severity == "critical" for item in issues):
        overall_status = "critical"
    elif issues:
        overall_status = "warning"
    return RuntimeOverview(
        generated_at=datetime.now(),
        overall_status=overall_status,
        process=RuntimeProcessStatus(
            version=_version(),
            uptime_seconds=max(0, int(time.monotonic() - _STARTED_AT)),
            restart_pending=restart_pending,
            listen_host=str(driver_config.host),
            listen_port=int(driver_config.port),
            log_level=str(driver_config.log_level),
        ),
        database=database,
        cache=cache,
        websocket=RuntimeWebSocketStatus(
            status="ok",
            active_connections=authenticated_websocket_count(),
            connection_limit=33,
        ),
        protocols=RuntimeProtocolStatus(
            onebot_v11_connected=bool(protocol and protocol.onebot_v11_connected),
            qq_official_enabled=bool(protocol and protocol.qq_official_enabled),
            qq_official_connected=bool(protocol and protocol.qq_official_connected),
            qq_webhook_mode=(protocol.qq_webhook_mode if protocol else "external"),
            connection_count=len(protocol.connections) if protocol else 0,
        ),
        bots=bots,
        issues=issues,
    )


async def reset_probe_cache_for_tests() -> None:
    global _probe_results, _probe_task, _probe_updated_at
    async with _probe_lock:
        task = _probe_task
        _probe_results = None
        _probe_task = None
        _probe_updated_at = 0.0
    if task is not None and not task.done():
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task


__all__ = ["build_runtime_overview", "reset_probe_cache_for_tests"]
