from __future__ import annotations

import asyncio
import contextlib
import os
from pathlib import Path
import time
from typing import Literal
from urllib.parse import urlsplit

from dotenv import dotenv_values
from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from redis.asyncio import Redis
from tortoise import Tortoise

from zhenxun.services.cache.bounded_ttl import BoundedTTLCache
from zhenxun.services.cache.config import CACHE_KEY_PREFIX
from zhenxun.services.cache.runtime_cache import (
    health_snapshot,
    refresh_all_runtime_caches,
)
from zhenxun.services.data_access import DataAccess
from zhenxun.services.runtime_environment import runtime_environment_manager
from zhenxun.utils._restart_utils import issue_restart_ticket
from zhenxun.utils.pydantic_compat import model_copy

from ....apply_result import (
    apply_result_data,
)
from ....base_model import Result
from ....restart_service import restart_status_data
from ....utils import authentication
from ...configure.data_source import (
    build_database_url,
    log_probe_result,
    probe_cache,
    probe_database,
)
from ...configure.model import CacheConfig, DatabaseConfig
from ...configure.persistence import _write_transaction
from ..system.configuration import _read, _revision, _update_env, _validate_env

router = APIRouter()

_ENV_FILE = Path(".env.dev")
_ENV_TEMPLATE = Path(".env.example")


class RuntimeProbeRequest(BaseModel):
    database: DatabaseConfig | None = None
    cache: CacheConfig | None = None


class RuntimeConfigurationUpdate(BaseModel):
    expected_revision: str
    database: DatabaseConfig
    cache: CacheConfig


class CacheAction(BaseModel):
    scope: Literal["local", "runtime", "redis"]
    confirmation: str = ""


def _env_path() -> Path:
    return _ENV_FILE if _ENV_FILE.exists() else _ENV_TEMPLATE


def _env_values() -> dict[str, str]:
    return {
        key: str(value)
        for key, value in dotenv_values(_env_path()).items()
        if key and value is not None
    }


def _current_database(values: dict[str, str]) -> DatabaseConfig:
    url = values.get("DB_URL", "")
    if not url:
        return DatabaseConfig()
    if url.startswith("sqlite"):
        path = url.split(":", 1)[1].lstrip("/") or "data/db/zhenxun.db"
        return DatabaseConfig(mode="sqlite", path=path)
    parsed = urlsplit(url)
    mode = "mysql" if parsed.scheme.startswith("mysql") else "postgres"
    if parsed.scheme.startswith(("mysql", "postgres")):
        return DatabaseConfig(
            mode=mode,
            host=parsed.hostname or "",
            port=parsed.port,
            username=parsed.username or "",
            password=parsed.password or "",
            database=parsed.path.lstrip("/"),
        )
    return DatabaseConfig(mode="url", url=url)


def _current_cache(values: dict[str, str]) -> CacheConfig:
    mode = values.get("CACHE_MODE", "MEMORY").upper()
    if mode not in {"NONE", "MEMORY", "REDIS"}:
        mode = "MEMORY"
    return CacheConfig(
        mode=mode,
        host=values.get("REDIS_HOST", "127.0.0.1"),
        port=int(values.get("REDIS_PORT", "6379") or 6379),
        password=values.get("REDIS_PASSWORD", ""),
    )


def _database_public(config: DatabaseConfig) -> dict:
    if config.mode == "sqlite":
        return {"mode": "sqlite", "path": config.path}
    if config.mode == "url":
        return {"mode": "url", "url": "", "has_saved_url": bool(config.url)}
    return {
        "mode": config.mode,
        "host": config.host,
        "port": config.port,
        "username": config.username,
        "database": config.database,
        "password": "",
        "has_password": bool(config.password),
    }


def _cache_public(config: CacheConfig) -> dict:
    return {
        "mode": config.mode,
        "host": config.host,
        "port": config.port,
        "password": "",
        "has_password": bool(config.password),
    }


async def _database_status() -> dict:
    started = time.perf_counter()
    try:
        connection = Tortoise.get_connection("default")
        await asyncio.wait_for(connection.execute_query("SELECT 1"), timeout=2)
        return {
            "status": "ok",
            "latency_ms": round((time.perf_counter() - started) * 1000),
        }
    except Exception as error:
        return {
            "status": "error",
            "latency_ms": round((time.perf_counter() - started) * 1000),
            "code": f"database_{error.__class__.__name__.lower()}",
        }


async def _redis_metrics(config: CacheConfig) -> dict:
    if config.mode != "REDIS":
        return {"status": "not_applicable"}
    client = Redis(
        host=config.host,
        port=config.port,
        password=config.password or None,
        socket_connect_timeout=2,
        socket_timeout=2,
        decode_responses=True,
    )
    started = time.perf_counter()
    try:
        ping, dbsize, memory = await asyncio.wait_for(
            asyncio.gather(client.ping(), client.dbsize(), client.info("memory")),
            timeout=2,
        )
        return {
            "status": "ok" if ping else "error",
            "latency_ms": round((time.perf_counter() - started) * 1000),
            "dbsize": int(dbsize),
            "used_memory": int(memory.get("used_memory", 0)),
        }
    except Exception as error:
        return {
            "status": "error",
            "latency_ms": round((time.perf_counter() - started) * 1000),
            "code": f"redis_{error.__class__.__name__.lower()}",
        }
    finally:
        with contextlib.suppress(Exception):
            await client.aclose()


def _runtime_health() -> dict:
    snapshot = health_snapshot()
    return {
        name: {
            "loaded": bool(item.get("loaded")),
            "entry_count": int(item.get("entry_count", 0)),
            "negative_count": int(item.get("negative_count", 0)),
            "last_refresh": item.get("last_refresh", 0),
            "has_error": bool(item.get("last_error")),
        }
        for name, item in snapshot.items()
    }


def _merge_saved_passwords(
    database: DatabaseConfig,
    cache: CacheConfig,
    current_database: DatabaseConfig,
    current_cache: CacheConfig,
) -> tuple[DatabaseConfig, CacheConfig]:
    if not database.password and database.mode == current_database.mode:
        database = model_copy(database, update={"password": current_database.password})
    if not cache.password and cache.mode == "REDIS" and current_cache.mode == "REDIS":
        cache = model_copy(cache, update={"password": current_cache.password})
    if database.mode == "url" and not database.url and current_database.mode == "url":
        database = model_copy(database, update={"url": current_database.url})
    return database, cache


@router.get(
    "/runtime",
    dependencies=[authentication()],
    response_model=Result,
    response_class=JSONResponse,
)
async def database_runtime() -> Result:
    values = _env_values()
    database = _current_database(values)
    cache = _current_cache(values)
    database_status, redis_metrics, bounded_stats = await asyncio.gather(
        _database_status(),
        _redis_metrics(cache),
        BoundedTTLCache.stats_all(),
    )
    return Result.ok(
        {
            "revision": _revision(_read(_env_path())),
            "launcher_managed": bool(os.getenv("ZHENXUN_LAUNCHER_PID")),
            "database": {
                "configuration": _database_public(database),
                "connection": database_status,
            },
            "cache": {
                "configuration": _cache_public(cache),
                "redis": redis_metrics,
                "runtime": _runtime_health(),
                "bounded": bounded_stats,
                "data_access": DataAccess.get_cache_stats(),
            },
        }
    )


@router.post(
    "/probe",
    dependencies=[authentication()],
    response_model=Result,
    response_class=JSONResponse,
)
async def database_probe(payload: RuntimeProbeRequest) -> Result:
    values = _env_values()
    current_database = _current_database(values)
    current_cache = _current_cache(values)
    database = payload.database or current_database
    cache = payload.cache or current_cache
    database, cache = _merge_saved_passwords(
        database, cache, current_database, current_cache
    )
    database_result, cache_result = await asyncio.gather(
        probe_database(database), probe_cache(cache)
    )
    log_probe_result("database", database.mode, database_result)
    log_probe_result("cache", cache.mode, cache_result)
    return Result.ok({"database": database_result, "cache": cache_result})


@router.put(
    "/configuration",
    dependencies=[authentication()],
    response_model=Result,
    response_class=JSONResponse,
)
async def update_database_configuration(
    payload: RuntimeConfigurationUpdate,
) -> Result:
    env_path = _env_path()
    current_text = _read(env_path)
    if _revision(current_text) != payload.expected_revision:
        raise HTTPException(
            status_code=409, detail="环境配置已被外部修改，请重新加载。"
        )
    values = _env_values()
    database, cache = _merge_saved_passwords(
        payload.database,
        payload.cache,
        _current_database(values),
        _current_cache(values),
    )
    database_result, cache_result = await asyncio.gather(
        probe_database(database), probe_cache(cache)
    )
    log_probe_result("database", database.mode, database_result)
    log_probe_result("cache", cache.mode, cache_result)
    if database_result.status == "error" or cache_result.status == "error":
        failure = Result.fail("数据库或缓存检查未通过。", code=422)
        failure.data = {"database": database_result, "cache": cache_result}
        return failure
    fields: dict[str, object] = {
        "DB_URL": build_database_url(database),
        "CACHE_MODE": cache.mode,
    }
    if cache.mode == "REDIS":
        fields.update(
            {
                "REDIS_HOST": cache.host,
                "REDIS_PORT": cache.port,
                "REDIS_PASSWORD": cache.password,
            }
        )
    updated = current_text
    try:
        updated = _update_env(current_text, fields)
        _validate_env(updated)
        if updated != current_text:
            _write_transaction([(_ENV_FILE, updated.encode("utf-8"))])
            operation = await runtime_environment_manager.apply(
                current_text, updated, submit_restart=False
            )
        else:
            operation = None
    except Exception as error:
        if updated != current_text:
            _write_transaction([(_ENV_FILE, current_text.encode("utf-8"))])
        raise HTTPException(
            status_code=500,
            detail=f"数据服务配置保存失败（{error.__class__.__name__}）。",
        ) from error
    changed_keys = operation.changed_keys if operation else []
    restart_required = bool(operation and operation.restart_required)
    reasons = operation.reason_codes if operation else []
    if restart_required and os.getenv("ZHENXUN_LAUNCHER_PID"):
        issue_restart_ticket("webui.settings", ttl_seconds=10 * 60)
    status = restart_status_data()
    apply_mode = operation.apply_mode if operation else "no_change"
    return Result.ok(
        apply_result_data(
            apply_mode=apply_mode,
            changed_keys=changed_keys,
            restart_required=restart_required,
            hot_reloaded=bool(operation and operation.hot_reloaded),
            reason_codes=reasons,
            access_urls=status["access_urls"],
            access_targets=status["access_targets"],
            revision=_revision(updated),
            checks={"database": database_result, "cache": cache_result},
            field_effects=operation.field_effects if operation else {},
            rolled_back=False,
        ),
        info=(
            "数据服务配置已保存，需要重启后生效。"
            if restart_required
            else (
                "缓存服务已重新连接并立即生效。"
                if apply_mode in {"config_reloaded", "hot_reloaded"}
                else "数据服务配置没有需要应用的运行时变化。"
            )
        ),
    )


@router.post(
    "/cache/clear",
    dependencies=[authentication()],
    response_model=Result,
    response_class=JSONResponse,
)
async def clear_database_cache(payload: CacheAction) -> Result:
    if payload.scope == "runtime":
        raise HTTPException(
            status_code=422, detail="运行时权限快照只能刷新，不能清空。"
        )
    if payload.scope == "local":
        cleared = await BoundedTTLCache.clear_all()
        return Result.ok({"cleared": cleared}, info="本地临时缓存已清理。")
    if payload.confirmation != "清理真寻Redis缓存":
        raise HTTPException(status_code=422, detail="请输入指定确认文本。")
    cache = _current_cache(_env_values())
    if cache.mode != "REDIS":
        raise HTTPException(status_code=409, detail="当前未启用 Redis 缓存。")
    client = Redis(
        host=cache.host,
        port=cache.port,
        password=cache.password or None,
        socket_connect_timeout=2,
        socket_timeout=2,
        decode_responses=True,
    )
    deleted = 0
    try:
        batch: list[str] = []
        async for key in client.scan_iter(match=f"{CACHE_KEY_PREFIX}:*", count=200):
            batch.append(key)
            if len(batch) >= 200:
                deleted += int(await client.unlink(*batch))
                batch.clear()
        if batch:
            deleted += int(await client.unlink(*batch))
    except Exception as error:
        raise HTTPException(
            status_code=503,
            detail=f"Redis 命名空间清理失败（{error.__class__.__name__}）。",
        ) from error
    finally:
        with contextlib.suppress(Exception):
            await client.aclose()
    return Result.ok({"deleted": deleted}, info="真寻 Redis 命名空间已清理。")


@router.post(
    "/cache/refresh",
    dependencies=[authentication()],
    response_model=Result,
    response_class=JSONResponse,
)
async def refresh_database_cache() -> Result:
    refreshed = await refresh_all_runtime_caches()
    return Result.ok(
        {"refreshed": refreshed, "health": _runtime_health()},
        info="运行时缓存已刷新。",
    )


__all__ = ["router"]
