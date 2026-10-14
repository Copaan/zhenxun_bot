"""launcher 附属进程：worker 就绪探测与 QQ HTTPS ingress"""

from __future__ import annotations

import asyncio
from contextlib import contextmanager
from functools import partial
import hashlib
import hmac
import http.client
import json
import os
from pathlib import Path
import ssl
import subprocess
import sys
import time
import urllib.error
from urllib.parse import urlsplit
import uuid

from .supervisor import launcher_log, launcher_supervisor

WORKER_READY_TIMEOUT = 120.0
WORKER_READY_POLL_INTERVAL = 0.25
INGRESS_START_TIMEOUT = 7.0

certificate_pin: str | None = None
"""当前 worker HTTPS 证书指纹，探测 worker 时用于固定证书"""


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


@contextmanager
def _health_urlopen(url: str, *, timeout: float = 1):
    """请求本地健康检查地址，HTTPS 时按 launcher 固定的证书指纹校验

    参数:
        url: 健康检查地址
        timeout: 超时秒数
    """
    target = urlsplit(url)
    if target.scheme == "https":
        if not certificate_pin:
            raise OSError("health_certificate_pin_missing")
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        context.check_hostname = False
        # Local names need not occur in the certificate; authenticate its DER pin
        # before sending any HTTP data instead of using system CA/name validation.
        context.verify_mode = ssl.CERT_NONE
        connection = http.client.HTTPSConnection(
            target.hostname, target.port, timeout=timeout, context=context
        )
    elif target.scheme == "http":
        connection = http.client.HTTPConnection(
            target.hostname, target.port, timeout=timeout
        )
    else:
        raise ValueError("health_scheme_invalid")
    try:
        connection.connect()
        if target.scheme == "https":
            certificate = connection.sock.getpeercert(binary_form=True)
            actual = hashlib.sha256(certificate).hexdigest()
            if not hmac.compare_digest(actual, certificate_pin.lower()):
                raise OSError("health_certificate_pin_mismatch")
        connection.request("GET", target.path or "/")
        with connection.getresponse() as response:
            yield response
    except http.client.HTTPException as error:
        raise OSError("health_response_invalid") from error
    finally:
        connection.close()


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


async def stop_process(process: subprocess.Popen, name: str | None = None) -> None:
    if name:
        launcher_log(f"stopping {name} pid={process.pid}")
    await launcher_supervisor.stop_process(process, allow_force=True)


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
        deadline = time.monotonic() + INGRESS_START_TIMEOUT
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
