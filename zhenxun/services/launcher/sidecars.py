"""launcher 附属进程：worker 就绪探测、WebUI HTTP 兼容入口与 QQ HTTPS ingress"""

from __future__ import annotations

import asyncio
from functools import partial
import json
import os
from pathlib import Path
import subprocess
import sys
import time
import urllib.error
import uuid

from .supervisor import launcher_supervisor

WORKER_READY_TIMEOUT = 120.0
WORKER_READY_POLL_INTERVAL = 0.25
HTTP_SIDECAR_RETRY_DELAYS = (1.0, 2.0, 5.0, 15.0, 30.0)
HTTP_SIDECAR_START_TIMEOUT = 7.0

certificate_pin: str | None = None
"""当前 worker HTTPS 证书指纹，探测 worker 时用于固定证书"""


def launcher_log(message: str) -> None:
    sys.stderr.write(f"[zx launcher] {message}\n")
    sys.stderr.flush()


def creationflags() -> int:
    if os.name == "nt":
        return getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)
    return 0


def build_worker_command() -> list[str]:
    return [sys.executable, "-m", "zhenxun.cli", "run-worker"]


def _worker_url(settings, path: str, scheme: str) -> str:
    connect_host = settings.worker_connect_host
    host = f"[{connect_host}]" if ":" in connect_host else connect_host
    return f"{scheme}://{host}:{settings.worker_port}{path}"


def worker_health_url(settings, *, scheme: str = "http") -> str:
    return _worker_url(settings, "/qq/healthz", scheme)


def build_ingress_command(settings, *, upstream_scheme: str = "http") -> list[str]:
    config = settings.config
    connect_host = settings.worker_connect_host
    upstream_host = f"[{connect_host}]" if ":" in connect_host else connect_host
    return [
        sys.executable,
        "-m",
        "zhenxun.cli",
        "run-ingress",
        config.qq_webhook_listen_host,
        str(config.qq_webhook_listen_port),
        config.qq_webhook_tls_certfile,
        config.qq_webhook_tls_keyfile,
        f"{upstream_scheme}://{upstream_host}:{settings.worker_port}",
    ]


def ingress_environment() -> dict[str, str]:
    sensitive_markers = ("TOKEN", "SECRET", "PASSWORD", "API_KEY")
    environment = {
        key: value
        for key, value in os.environ.items()
        if key.upper() != "QQ_BOTS"
        and not any(marker in key.upper() for marker in sensitive_markers)
    }
    environment["ZHENXUN_INGRESS_CHILD"] = "1"
    return environment


def _health_urlopen(url: str):
    from zhenxun.services.listener_health import health_response

    return health_response(url, fingerprint=certificate_pin)


def read_worker_status(settings, *, scheme: str = "http") -> dict[str, object] | None:
    """读取 worker 启动状态接口

    参数:
        settings: QQ launcher 设置（提供 worker 地址）
        scheme: worker 协议

    返回:
        dict[str, object] | None: 启动状态，不可达时为 None
    """
    url = _worker_url(settings, "/zhenxun/api/system/startup/status", scheme)
    try:
        with _health_urlopen(url) as response:
            if response.status != 200:
                return None
            data = json.loads(response.read()).get("data", {})
            return data if isinstance(data, dict) else None
    except (OSError, ValueError, TypeError, urllib.error.URLError):
        return None


def status_is_ready(
    data: dict[str, object] | None, *, require_warmup: bool = False
) -> bool:
    if not data:
        return False
    state = data.get("state")
    if require_warmup:
        stages = data.get("stages")
        warmup = stages.get("warmup", {}) if isinstance(stages, dict) else {}
        warmup_state = warmup.get("state") if isinstance(warmup, dict) else None
        return state in {"warmup_ready", "degraded"} and warmup_state in {
            "completed",
            "failed",
        }
    return state in {"runtime_ready", "warmup_ready", "degraded"}


def bind_worker_status(
    worker: subprocess.Popen, status: dict[str, object] | None
) -> bool:
    if status is None:
        return False
    try:
        return launcher_supervisor.bind_worker_runtime(worker, status)
    except Exception:
        return False


async def wait_worker_ready(
    worker: subprocess.Popen,
    settings,
    *,
    scheme: str = "http",
    require_warmup: bool = False,
) -> bool:
    """等待 worker 进入业务可用状态

    参数:
        worker: worker 进程
        settings: QQ launcher 设置
        scheme: worker 协议
        require_warmup: 是否等待 warmup 阶段结束

    返回:
        bool: 是否就绪；进入管理模式、退出或超时时为 False
    """
    deadline = time.monotonic() + WORKER_READY_TIMEOUT
    while time.monotonic() < deadline:
        if launcher_supervisor.shutdown_deadline is not None:
            return False
        if worker.poll() is not None:
            return False
        status = await asyncio.to_thread(read_worker_status, settings, scheme=scheme)
        bound = bind_worker_status(worker, status)
        if status and status.get("operating_mode") in {
            "management_only",
            "setup_only",
        }:
            return False
        if bound and status_is_ready(status, require_warmup=require_warmup):
            return True
        await asyncio.sleep(WORKER_READY_POLL_INTERVAL)
    return False


async def worker_webui_ready_once(
    worker: subprocess.Popen, settings, *, scheme: str = "http"
) -> bool:
    status = await asyncio.to_thread(read_worker_status, settings, scheme=scheme)
    if (
        status is None
        or not bind_worker_status(worker, status)
        or not status.get("server_bound")
    ):
        return False

    def probe() -> bool:
        url = _worker_url(settings, "/zhenxun/api/configure/status", scheme)
        try:
            with _health_urlopen(url) as response:
                return response.status == 200
        except (OSError, urllib.error.URLError):
            return False

    return await asyncio.to_thread(probe)


async def wait_worker_webui_ready(
    worker: subprocess.Popen, settings, *, scheme: str = "http"
) -> bool:
    deadline = time.monotonic() + WORKER_READY_TIMEOUT
    while time.monotonic() < deadline:
        if (
            worker.poll() is not None
            or launcher_supervisor.shutdown_deadline is not None
        ):
            return False
        if await worker_webui_ready_once(worker, settings, scheme=scheme):
            return True
        await asyncio.sleep(WORKER_READY_POLL_INTERVAL)
    return False


async def stop_process(process: subprocess.Popen, name: str | None = None) -> None:
    if name:
        launcher_log(f"stopping {name} pid={process.pid}")
    await launcher_supervisor.stop_process(process, allow_force=True)


def publish_http_sidecar_state(**changes: object) -> dict[str, object]:
    from zhenxun.services.webui_http_sidecar_state import (
        read_http_sidecar_state,
        write_http_sidecar_state,
    )

    if changes:
        write_http_sidecar_state(
            **{**changes, "launcher_boot_id": os.getenv("ZHENXUN_LAUNCHER_BOOT_ID", "")}
        )
    return read_http_sidecar_state()


def http_sidecar_signature(settings) -> tuple | None:
    from zhenxun.configs.webui_tls import certificate_sha256

    if not settings.http_sidecar_enabled:
        return None
    return (
        settings.effective_http_mode,
        settings.host,
        settings.redirect_port,
        settings.port,
        certificate_sha256(settings.certfile),
    )


def _build_http_sidecar_command(settings) -> list[str]:
    from zhenxun.configs.webui_tls import certificate_sha256
    from zhenxun.utils.network import internal_connect_host

    return [
        sys.executable,
        "-m",
        "zhenxun.cli",
        "run-http-sidecar",
        settings.effective_http_mode,
        settings.host,
        str(settings.redirect_port),
        internal_connect_host(settings.host),
        str(settings.port),
        certificate_sha256(settings.certfile),
    ]


def _child_error(state: dict, startup_id: str) -> dict:
    """取子进程自己发布的错误；不属于本次启动时返回空字典"""
    if state.get("startup_id") != startup_id or not state.get("last_error"):
        return {}
    keys = ("last_error", "error_code", "stage", "errno")
    return {key: state.get(key) for key in keys}


async def start_http_sidecar(settings, cwd: Path) -> subprocess.Popen | None:
    """启动 WebUI HTTP 兼容入口，失败时写入降级状态

    参数:
        settings: WebUI TLS 设置
        cwd: 项目目录

    返回:
        subprocess.Popen | None: 已就绪的进程，失败时为 None
    """
    from zhenxun.services.webui_http_sidecar_state import sidecar_error_details

    startup_id = uuid.uuid4().hex
    failure = "sidecar_startup_failed"

    async def wait_ready(process):
        nonlocal failure
        publish_http_sidecar_state(spawn_pid=process.pid)
        deadline = time.monotonic() + HTTP_SIDECAR_START_TIMEOUT
        while process.poll() is None and time.monotonic() < deadline:
            if launcher_supervisor.shutdown_deadline is not None:
                failure = "sidecar_startup_interrupted"
                raise OSError(failure)
            state = publish_http_sidecar_state()
            if state.get("startup_id") == startup_id:
                if state.get("state") == "ready":
                    return
                if state.get("state") == "degraded":
                    break
            await asyncio.sleep(0.05)
        if process.poll() is None and time.monotonic() >= deadline:
            failure = "startup_timeout"
        raise OSError(failure)

    publish_http_sidecar_state(
        startup_id=startup_id,
        mode=settings.effective_http_mode,
        port=settings.redirect_port,
        pid=None,
        spawn_pid=None,
        state="starting",
        active_connections=0,
        last_error=None,
        error_code=None,
        stage="spawn",
        errno=None,
        retry_in_seconds=0,
        next_retry_at=None,
    )
    spawned = False

    def spawn() -> subprocess.Popen:
        nonlocal spawned
        process = subprocess.Popen(
            _build_http_sidecar_command(settings),
            cwd=str(cwd),
            creationflags=creationflags(),
            env={
                **os.environ,
                "ZHENXUN_HTTP_SIDECAR_CHILD": "1",
                "ZHENXUN_HTTP_SIDECAR_STARTUP_ID": startup_id,
            },
        )
        spawned = True
        return process

    try:
        process = await launcher_supervisor.start_process(
            "http_sidecar", spawn, ready=wait_ready, startup_id=startup_id
        )
    except OSError as error:
        details = _child_error(publish_http_sidecar_state(), startup_id) or (
            sidecar_error_details(
                failure if spawned else error,
                stage="readiness" if spawned else "spawn",
            )
        )
        publish_http_sidecar_state(
            state="degraded", active_connections=0, startup_id=startup_id, **details
        )
        return None
    publish_http_sidecar_state(
        state="ready",
        spawn_pid=process.pid,
        last_error=None,
        error_code=None,
        stage=None,
        errno=None,
    )
    launcher_log(
        "WebUI HTTP compatibility sidecar ready on "
        f"{settings.host}:{settings.redirect_port} "
        f"(mode={settings.effective_http_mode})"
    )
    return process


def http_sidecar_exited(process: subprocess.Popen, retry_index: int) -> float:
    """记录 HTTP 兼容入口意外退出并返回下次重试的等待秒数

    参数:
        process: 已退出的进程
        retry_index: 已重试次数

    返回:
        float: 重试等待秒数
    """
    state = publish_http_sidecar_state()
    launcher_supervisor.release(process, "unexpected_exit")
    delay = HTTP_SIDECAR_RETRY_DELAYS[
        min(retry_index, len(HTTP_SIDECAR_RETRY_DELAYS) - 1)
    ]
    publish_http_sidecar_state(
        state="degraded",
        active_connections=0,
        **(
            _child_error(state, str(state.get("startup_id") or ""))
            or {
                "last_error": "sidecar_unexpected_exit",
                "error_code": "sidecar_unexpected_exit",
                "stage": "serve",
                "errno": None,
            }
        ),
        retry_in_seconds=delay,
        next_retry_at=time.time() + delay,
        retry_count=retry_index + 1,
    )
    launcher_log(
        "WebUI HTTP sidecar exited unexpectedly; "
        f"HTTPS remains available, retrying in {delay:g}s"
    )
    return delay


async def start_qq_ingress(settings, tls, cwd: Path) -> subprocess.Popen:
    """启动 QQ HTTPS ingress 并等待其监听

    参数:
        settings: QQ launcher 设置
        tls: WebUI TLS 设置（决定上游协议）
        cwd: 项目目录

    返回:
        subprocess.Popen: 已就绪的进程
    """
    from zhenxun.services.qq_ingress_state import read_ingress_state

    startup_id = uuid.uuid4().hex

    async def wait_ready(process):
        deadline = time.monotonic() + HTTP_SIDECAR_START_TIMEOUT
        while process.poll() is None and time.monotonic() < deadline:
            if launcher_supervisor.shutdown_deadline is not None:
                raise OSError("ingress_startup_interrupted")
            state = read_ingress_state()
            if state.get("state") == "ready" and state.get("startup_id") == startup_id:
                return
            await asyncio.sleep(0.05)
        raise OSError("ingress_startup_failed")

    return await launcher_supervisor.start_process(
        "qq_ingress",
        partial(
            subprocess.Popen,
            build_ingress_command(settings, upstream_scheme=tls.scheme),
            cwd=str(cwd),
            creationflags=creationflags(),
            env={
                **ingress_environment(),
                "ZHENXUN_QQ_INGRESS_STARTUP_ID": startup_id,
                "ZHENXUN_QQ_UPSTREAM_CERT_SHA256": certificate_pin or "",
            },
        ),
        ready=wait_ready,
        startup_id=startup_id,
    )
