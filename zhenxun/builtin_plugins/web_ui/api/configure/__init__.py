from __future__ import annotations

import asyncio
from typing import Annotated

from fastapi import APIRouter, Depends, Header, HTTPException, Request
from fastapi.responses import JSONResponse
import nonebot

from zhenxun.configs.config import Config
from zhenxun.services.startup import startup_coordinator
from zhenxun.utils._restart_utils import issue_restart_ticket
from zhenxun.utils.network import private_ipv4_addresses

from ...base_model import Result
from ...console_access import console_access
from ...passwords import validate_new_password
from ...restart_service import (
    preferred_access_targets,
    preferred_access_urls,
    request_webui_restart,
    restart_status_data,
    transaction_verification_status,
)
from ...utils import public_startup_status, request_authenticated
from .data_source import (
    log_probe_result,
    probe_cache,
    probe_database,
    probe_network,
)
from .model import (
    ApplyRequest,
    CacheProbeRequest,
    DatabaseProbeRequest,
    NetworkProbeRequest,
    RestartRequest,
)
from .persistence import (
    _quote_env as _quote_env,
)
from .persistence import (
    _set_env_value as _set_env_value,
)
from .persistence import (
    apply_configuration,
)
from .setup_access import (
    SetupSession,
    client_ip,
    require_setup_token,
    setup_access,
)

router = APIRouter(prefix="/configure")
driver = nonebot.get_driver()


def _current_listener() -> tuple[str, int]:
    return str(driver.config.host), int(driver.config.port)


@router.get("/status", response_model=Result, response_class=JSONResponse)
async def configure_status(request: Request) -> Result:
    if not request_authenticated(request):
        return Result.ok(
            {
                "state": setup_access.state(),
                "startup": public_startup_status(startup_coordinator.snapshot()),
                "boot_id": console_access.current_boot_id(),
                **transaction_verification_status(),
            }
        )
    return Result.ok(
        {
            "state": setup_access.state(),
            "startup": startup_coordinator.snapshot(),
            **restart_status_data(),
        }
    )


@router.get(
    "/draft",
    response_model=Result,
    response_class=JSONResponse,
    dependencies=[Depends(require_setup_token)],
)
async def configure_draft() -> Result:
    current_host = str(getattr(driver.config, "host", "0.0.0.0"))
    current_port = int(getattr(driver.config, "port", 8080))
    username = str(Config.get_config("web-ui", "username", "admin"))
    return Result.ok(
        {
            "username": username,
            "database": {"mode": "sqlite", "path": "data/db/zhenxun.db"},
            "cache": {"mode": "MEMORY", "host": "127.0.0.1", "port": 6379},
            "network": {"mode": "lan", "host": current_host, "port": current_port},
            "detected_addresses": private_ipv4_addresses(),
        }
    )


@router.post(
    "/probe/database",
    response_model=Result,
    response_class=JSONResponse,
    dependencies=[Depends(require_setup_token)],
)
async def probe_database_route(payload: DatabaseProbeRequest) -> Result:
    result = await probe_database(payload.database)
    log_probe_result("database", payload.database.mode, result)
    return Result.ok(result)


@router.post(
    "/probe/cache",
    response_model=Result,
    response_class=JSONResponse,
    dependencies=[Depends(require_setup_token)],
)
async def probe_cache_route(payload: CacheProbeRequest) -> Result:
    result = await probe_cache(payload.cache)
    log_probe_result("cache", payload.cache.mode, result)
    return Result.ok(result)


@router.post(
    "/probe/network",
    response_model=Result,
    response_class=JSONResponse,
    dependencies=[Depends(require_setup_token)],
)
async def probe_network_route(payload: NetworkProbeRequest) -> Result:
    return Result.ok(
        await probe_network(payload.network, current_listener=_current_listener())
    )


@router.post(
    "/apply",
    response_model=Result,
    response_class=JSONResponse,
)
async def apply_setup(
    payload: ApplyRequest,
    session: Annotated[SetupSession, Depends(require_setup_token)],
) -> Result:
    return await _apply_setup(payload, session)


_setup_lock = asyncio.Lock()


async def _apply_setup(payload: ApplyRequest, session: SetupSession) -> Result:
    if _setup_lock.locked():
        raise HTTPException(409, detail="首次配置正在提交，请稍候。")
    async with _setup_lock:
        if setup_access.state() in {"configured", "restart_pending"}:
            raise HTTPException(409, detail="首次配置已完成，请重启真寻。")
        return await _apply_setup_locked(payload, session)


async def _apply_setup_locked(payload: ApplyRequest, session: SetupSession) -> Result:
    if payload.password != payload.confirm_password:
        raise HTTPException(status_code=422, detail="两次输入的密码不一致。")
    if password_error := validate_new_password(payload.password):
        raise HTTPException(status_code=422, detail=password_error)

    database_result, cache_result, network_result = await asyncio.gather(
        probe_database(payload.database),
        probe_cache(payload.cache),
        probe_network(payload.network, current_listener=_current_listener()),
    )
    results = {
        "database": database_result,
        "cache": cache_result,
        "network": network_result,
    }
    log_probe_result("database", payload.database.mode, database_result)
    log_probe_result("cache", payload.cache.mode, cache_result)
    errors = [result for result in results.values() if result.status == "error"]
    warnings = [result for result in results.values() if result.status == "warning"]
    if errors:
        failure = Result.fail("配置检查未通过。", code=422)
        failure.data = results
        return failure
    if warnings and not payload.accept_warnings:
        failure = Result.fail("请确认警告后再保存。", code=409)
        failure.data = results
        return failure

    try:
        applied = await asyncio.to_thread(apply_configuration, payload)
    except Exception as error:
        raise HTTPException(
            status_code=500,
            detail=f"配置写入失败（{error.__class__.__name__}）。",
        ) from error
    issue_restart_ticket("webui.configure", ttl_seconds=10 * 60)
    receipt = await setup_access.mark_applied(session)
    access_urls = preferred_access_urls(applied["host"], applied["port"])
    access_targets = [
        {"kind": item.label.lower(), "url": item.url}
        for item in preferred_access_targets(applied["host"], applied["port"])
    ]
    return Result.ok(
        {
            "state": "restart_pending",
            "restart_receipt": receipt,
            "access_urls": access_urls,
            "access_targets": access_targets,
            "checks": results,
        },
        info="配置已安全保存，可以重启真寻。",
    )


@router.post("/restart", response_model=Result, response_class=JSONResponse)
async def restart_setup(
    request: Request,
    payload: RestartRequest,
    authorization: Annotated[str | None, Header(alias="Authorization")] = None,
) -> Result:
    session = await setup_access.authorize(
        authorization,
        client_ip(request),
        restart_only=True,
    )
    await setup_access.consume_restart_receipt(session, payload.receipt)
    ok, message, data = await request_webui_restart(
        "webui.configure", require_ticket="webui.configure"
    )
    if not ok:
        return Result.fail(message)
    return Result.ok(data, info=message)
