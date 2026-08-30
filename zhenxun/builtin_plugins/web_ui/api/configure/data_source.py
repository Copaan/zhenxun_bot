from __future__ import annotations

import asyncio
import contextlib
import importlib
import ipaddress
import os
from pathlib import Path
import socket
import sqlite3
import tempfile
import time
from typing import Any
from urllib.parse import quote

import psutil
from redis.asyncio import Redis
from tortoise.backends.base.config_generator import expand_db_url

from zhenxun.services.log import logger
from zhenxun.utils.network import local_access_urls, private_ipv4_addresses

from .model import CacheConfig, DatabaseConfig, NetworkConfig, ProbeResult


def _result(
    status: str,
    code: str,
    message: str,
    started_at: float,
    **facts: str | int | bool | list[str],
) -> ProbeResult:
    return ProbeResult(
        status=status,
        code=code,
        message=message,
        latency_ms=max(0, round((time.perf_counter() - started_at) * 1000)),
        facts=facts,
    )


def _safe_failure(service: str, error: Exception, started_at: float) -> ProbeResult:
    return _result(
        "error",
        f"{service.lower()}_connection_failed",
        f"{service}连接失败（{error.__class__.__name__}）。",
        started_at,
    )


def log_probe_result(service: str, mode: str, result: ProbeResult) -> None:
    message = (
        f"WebUI {service}测试完成 mode={mode} result={result.code} "
        f"latency_ms={result.latency_ms}"
    )
    if result.status == "error":
        logger.warning(message, "WebUIProbe")
    else:
        logger.info(message, "WebUIProbe")


def resolve_sqlite_path(path_value: str, root: Path | None = None) -> Path:
    root = (root or Path.cwd()).resolve()
    candidate = Path(path_value.strip() or "data/db/zhenxun.db")
    if not candidate.is_absolute():
        candidate = root / candidate
    candidate = candidate.resolve()
    try:
        inside_root = candidate.is_relative_to(root)
    except ValueError:
        inside_root = False
    if not inside_root:
        raise ValueError("sqlite_path_outside_project")
    return candidate


def build_database_url(config: DatabaseConfig, root: Path | None = None) -> str:
    if config.mode == "sqlite":
        path = resolve_sqlite_path(config.path, root)
        project_root = (root or Path.cwd()).resolve()
        relative = path.relative_to(project_root).as_posix()
        return f"sqlite://{relative}"
    if config.mode == "url":
        value = config.url.strip()
        if not value:
            raise ValueError("database_url_empty")
        expand_db_url(value)
        return value

    host = config.host.strip()
    database = config.database.strip()
    if not host or not config.username or not database:
        raise ValueError("database_fields_incomplete")
    scheme = "mysql" if config.mode == "mysql" else "postgres"
    port = config.port or (3306 if scheme == "mysql" else 5432)
    if ":" in host and not host.startswith("["):
        host = f"[{host}]"
    username = quote(config.username, safe="")
    password = quote(config.password, safe="")
    database_name = quote(database, safe="")
    return f"{scheme}://{username}:{password}@{host}:{port}/{database_name}"


async def _probe_sqlite(config: DatabaseConfig, root: Path | None) -> ProbeResult:
    started_at = time.perf_counter()
    try:
        path = resolve_sqlite_path(config.path, root)
    except ValueError:
        return _result(
            "error",
            "sqlite_path_outside_project",
            "SQLite 文件必须位于真寻项目目录内。",
            started_at,
        )

    def _check() -> bool:
        if path.exists():
            uri = f"file:{path.as_posix()}?mode=ro"
            with contextlib.closing(
                sqlite3.connect(uri, uri=True, timeout=5)
            ) as connection:
                connection.execute("SELECT 1").fetchone()
            return True
        path.parent.mkdir(parents=True, exist_ok=True)
        handle, temporary_name = tempfile.mkstemp(
            prefix=".zhenxun-db-probe-", suffix=".db", dir=path.parent
        )
        os.close(handle)
        temporary = Path(temporary_name)
        try:
            with contextlib.closing(
                sqlite3.connect(temporary, timeout=5)
            ) as connection:
                connection.execute("SELECT 1").fetchone()
        finally:
            temporary.unlink(missing_ok=True)
        return False

    try:
        exists = await asyncio.wait_for(asyncio.to_thread(_check), timeout=10)
    except asyncio.TimeoutError:
        return _result("error", "database_timeout", "数据库检查超时。", started_at)
    except Exception as error:
        return _safe_failure("SQLite", error, started_at)
    return _result(
        "ok",
        "sqlite_ready",
        "SQLite 文件可读取。" if exists else "SQLite 目录可创建数据库文件。",
        started_at,
        database_type="sqlite",
        existing_file=exists,
    )


async def probe_database(
    config: DatabaseConfig, root: Path | None = None
) -> ProbeResult:
    if config.mode == "sqlite":
        return await _probe_sqlite(config, root)
    started_at = time.perf_counter()
    client: Any | None = None
    try:
        database_url = build_database_url(config, root)
        db_config = expand_db_url(database_url)
        engine = importlib.import_module(db_config["engine"])
        client = engine.client_class(
            connection_name="webui_configure_test",
            **db_config["credentials"],
        )

        async def _ping() -> None:
            await client.create_connection(with_db=True)
            await client.execute_query("SELECT 1")

        await asyncio.wait_for(_ping(), timeout=10)
        return _result(
            "ok",
            "database_ready",
            "数据库连接和查询均正常。",
            started_at,
            database_type=config.mode,
        )
    except asyncio.TimeoutError:
        return _result("error", "database_timeout", "数据库连接超时。", started_at)
    except ValueError as error:
        code = str(error) if str(error).startswith("database_") else "database_invalid"
        return _result("error", code, "数据库配置不完整或格式无效。", started_at)
    except Exception as error:
        return _safe_failure("数据库", error, started_at)
    finally:
        if client is not None:
            with contextlib.suppress(Exception):
                await client.close()


async def probe_cache(config: CacheConfig) -> ProbeResult:
    started_at = time.perf_counter()
    if config.mode == "MEMORY":
        return _result(
            "ok",
            "memory_cache_ready",
            "将使用进程内有界缓存。",
            started_at,
            cache_mode="MEMORY",
        )
    if config.mode == "NONE":
        return _result(
            "warning",
            "cache_disabled",
            "缓存已关闭，部分高频功能的性能会下降。",
            started_at,
            cache_mode="NONE",
        )
    if not config.host.strip():
        return _result("error", "redis_host_empty", "Redis 地址不能为空。", started_at)
    client = Redis(
        host=config.host.strip(),
        port=config.port,
        password=config.password or None,
        socket_connect_timeout=5,
        socket_timeout=5,
    )
    try:
        await asyncio.wait_for(client.ping(), timeout=5)
        return _result(
            "ok",
            "redis_ready",
            "Redis PING 成功。",
            started_at,
            cache_mode="REDIS",
        )
    except asyncio.TimeoutError:
        return _result("error", "redis_timeout", "Redis 连接超时。", started_at)
    except Exception as error:
        return _safe_failure("Redis", error, started_at)
    finally:
        await client.aclose()


def resolve_network_host(config: NetworkConfig) -> str:
    if config.mode == "local":
        return "127.0.0.1"
    if config.mode == "lan":
        return "0.0.0.0"
    return config.host.strip().strip("[]")


def _listener_owner(host: str, port: int) -> int | None:
    wildcard = host in {"0.0.0.0", "::"}
    with contextlib.suppress(psutil.Error, OSError):
        for connection in psutil.net_connections(kind="inet"):
            if connection.status != psutil.CONN_LISTEN or not connection.laddr:
                continue
            bound_host, bound_port = connection.laddr[:2]
            if bound_port != port:
                continue
            if wildcard or bound_host in {host, "0.0.0.0", "::"}:
                return connection.pid
    return None


def _same_listener(
    requested_host: str,
    requested_port: int,
    current_listener: tuple[str, int] | None,
) -> bool:
    if current_listener is None or requested_port != current_listener[1]:
        return False
    current_host = current_listener[0].strip().strip("[]")
    return requested_host == current_host or current_host in {"0.0.0.0", "::"}


async def probe_network(
    config: NetworkConfig,
    current_listener: tuple[str, int] | None = None,
) -> ProbeResult:
    started_at = time.perf_counter()
    host = resolve_network_host(config)
    if not host:
        return _result("error", "network_host_empty", "监听地址不能为空。", started_at)
    try:
        infos = socket.getaddrinfo(host, config.port, type=socket.SOCK_STREAM)
    except socket.gaierror:
        return _result(
            "error", "network_host_invalid", "监听地址无法解析。", started_at
        )

    if host not in {"0.0.0.0", "::", "127.0.0.1", "::1", "localhost"}:
        local_addresses = set(private_ipv4_addresses())
        for addresses in psutil.net_if_addrs().values():
            for address in addresses:
                if address.family in {socket.AF_INET, socket.AF_INET6}:
                    local_addresses.add(address.address.split("%", 1)[0])
        with contextlib.suppress(ValueError):
            address = ipaddress.ip_address(host)
            if address.is_loopback:
                local_addresses.add(host)
        if host not in local_addresses:
            return _result(
                "error",
                "network_host_not_local",
                "自定义地址不属于当前主机的可用网卡。",
                started_at,
            )

    owner = _listener_owner(host, config.port)
    urls = [item.url for item in local_access_urls(host, config.port)]
    if owner == os.getpid() or _same_listener(host, config.port, current_listener):
        return _result(
            "warning",
            "network_current_worker",
            "该端口正由当前真寻进程监听，重启时可继续使用。",
            started_at,
            access_urls=urls,
            port=config.port,
        )
    if owner is not None:
        return _result(
            "error",
            "network_port_in_use",
            "端口已被其他进程占用。",
            started_at,
            port=config.port,
        )

    family, socktype, protocol, _, address = infos[0]
    try:
        with socket.socket(family, socktype, protocol) as probe:
            probe.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            probe.bind(address)
    except OSError:
        return _result(
            "error",
            "network_bind_failed",
            "监听地址或端口无法绑定。",
            started_at,
            port=config.port,
        )
    return _result(
        "ok",
        "network_ready",
        "监听地址和端口可用。",
        started_at,
        access_urls=urls,
        port=config.port,
    )


async def test_db_connection(db_url: str) -> bool | str:
    result = await probe_database(DatabaseConfig(mode="url", url=db_url))
    return True if result.status == "ok" else result.message


async def test_redis_connection(host: str, port: int, password: str = "") -> bool | str:
    result = await probe_cache(
        CacheConfig(mode="REDIS", host=host, port=port, password=password)
    )
    return True if result.status == "ok" else result.message


__all__ = [
    "build_database_url",
    "probe_cache",
    "probe_database",
    "probe_network",
    "resolve_network_host",
    "resolve_sqlite_path",
    "test_db_connection",
    "test_redis_connection",
]
