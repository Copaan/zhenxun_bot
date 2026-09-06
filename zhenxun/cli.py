"""zx CLI — 绪山真寻 Bot 命令行工具

用法:
    zx run          启动 launcher
    zx run-worker   启动 worker（由 launcher 调用）
    zx version      显示版本信息

进程环境变量:
    ZHENXUN_STARTUP_BANNER=0  禁用启动图案
    NO_COLOR                 禁用启动图案颜色
"""

from __future__ import annotations

import asyncio
import contextlib
from functools import partial
import importlib.metadata
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time
import urllib.error
import urllib.request

GRACEFUL_SHUTDOWN_TIMEOUT = 15
WORKER_CONNECTION_LIMIT = 512
WORKER_BACKLOG = 2048
WORKER_KEEP_ALIVE_TIMEOUT = 15
WORKER_POLL_INTERVAL = 0.1
RESTART_POLL_INTERVAL = 0.5
WORKER_SOFT_EXIT_TIMEOUT = 15.0
WORKER_TERMINATE_TIMEOUT = 5.0
WORKER_KILL_TIMEOUT = 5.0
WORKER_READY_TIMEOUT = 120.0
WORKER_READY_POLL_INTERVAL = 0.25
HTTP_SIDECAR_RETRY_DELAYS = (1.0, 2.0, 5.0, 15.0, 30.0)
HTTP_SIDECAR_START_TIMEOUT = 7.0
ENV_EXAMPLE_FILE = ".env.example"
ENV_DEV_FILE = ".env.dev"


def _env_assignment_key(line: str, *, include_commented: bool = False) -> str | None:
    stripped = line.strip()
    if include_commented and stripped.startswith("#"):
        stripped = stripped[1:].lstrip()
    if not stripped or stripped.startswith("#") or "=" not in stripped:
        return None
    key = stripped.split("=", 1)[0].strip()
    return key if key.replace("_", "").isalnum() else None


def _env_key(line: str) -> str | None:
    return _env_assignment_key(line)


def _env_block_key(block: list[str]) -> str | None:
    for line in block:
        if key := _env_key(line):
            return key
    return None


def _env_block_anchor_key(block: list[str]) -> str | None:
    for line in block:
        if key := _env_assignment_key(line, include_commented=True):
            return key
    return None


def _split_env_blocks(lines: list[str]) -> list[tuple[int, list[str]]]:
    blocks: list[tuple[int, list[str]]] = []
    current: list[str] = []
    start_index = 0
    for index, line in enumerate(lines):
        if line.strip():
            if not current:
                start_index = index
            current.append(line)
        elif current:
            blocks.append((start_index, current))
            current = []

    if current:
        blocks.append((start_index, current))
    return blocks


def _find_env_block_start(lines: list[str], key: str) -> int | None:
    for start_index, block in _split_env_blocks(lines):
        if _env_block_anchor_key(block) == key:
            return start_index
    return None


def _insert_env_block_before(
    lines: list[str],
    index: int,
    block: list[str],
) -> list[str]:
    insert_block = block.copy()
    if index > 0 and lines[index - 1].strip():
        insert_block.insert(0, "\n")
    if index < len(lines) and insert_block and insert_block[-1].strip():
        insert_block.append("\n")
    return lines[:index] + insert_block + lines[index:]


def _sync_env_missing_items(project_root: Path) -> None:
    """Copy missing .env keys from .env.example without touching existing values."""
    example_path = project_root / ENV_EXAMPLE_FILE
    env_path = project_root / ENV_DEV_FILE
    if not example_path.exists():
        return
    if not env_path.exists():
        env_path.write_text(example_path.read_text(encoding="utf-8"), encoding="utf-8")
        _launcher_log("已根据 .env.example 生成 .env.dev")
        return

    example_lines = example_path.read_text(encoding="utf-8").splitlines(keepends=True)
    env_lines = env_path.read_text(encoding="utf-8").splitlines(keepends=True)
    example_blocks = _split_env_blocks(example_lines)
    existing_keys = {key for line in env_lines if (key := _env_key(line))}
    existing_values = {
        key: line.split("=", 1)[1].strip()
        for line in env_lines
        if (key := _env_key(line)) and "=" in line
    }
    missing_blocks: list[tuple[int, list[str]]] = []

    for block_index, (_, block) in enumerate(example_blocks):
        key = _env_block_key(block)
        if key and key not in existing_keys:
            if key == "WEBUI_HTTP_MODE" and str(
                existing_values.get("WEBUI_HTTP_REDIRECT_ENABLED", "")
            ).casefold() in {"1", "true", "yes", "on"}:
                block = [
                    "# 已从旧 HTTP 重定向配置迁移\n",
                    "WEBUI_HTTP_MODE=redirect\n",
                ]
            missing_blocks.append((block_index, block))

    if not missing_blocks:
        return

    updated_lines = env_lines
    added_keys: list[str] = []
    for block_index, block in missing_blocks:
        key = _env_block_key(block)
        if not key:
            continue
        anchor_index = len(updated_lines)
        for _, next_block in example_blocks[block_index + 1 :]:
            next_key = _env_block_anchor_key(next_block)
            if not next_key:
                continue
            if (found := _find_env_block_start(updated_lines, next_key)) is not None:
                anchor_index = found
                break
        updated_lines = _insert_env_block_before(updated_lines, anchor_index, block)
        existing_keys.add(key)
        added_keys.append(key)

    env_path.write_text("".join(updated_lines), encoding="utf-8")
    _launcher_log(f"已补齐 .env.dev 缺失配置: {', '.join(added_keys)}")


def _launcher_log(message: str) -> None:
    sys.stderr.write(f"[zx launcher] {message}\n")
    sys.stderr.flush()


def _print_version() -> None:
    try:
        ver = importlib.metadata.version("zhenxun-bot")
    except importlib.metadata.PackageNotFoundError:
        ver = "unknown"
    sys.stdout.write(f"zhenxun-bot {ver}\n")


def _ensure_project_root() -> Path:
    cwd = Path.cwd()
    if not (cwd / "zhenxun").is_dir():
        sys.stderr.write("错误: 当前目录不是 zhenxun_bot 项目目录。\n")
        sys.stderr.write("请在项目根目录（包含 zhenxun/ 目录的位置）执行 zx run。\n")
        sys.exit(1)

    cwd_str = str(cwd)
    if cwd_str not in sys.path:
        sys.path.insert(0, cwd_str)
    return cwd


def _run_worker() -> None:
    """启动 Bot worker（必须在项目目录下执行）"""
    worker_started = time.monotonic()
    project_root = _ensure_project_root()
    from zhenxun.update_service import apply_pending_update

    if not os.environ.get("ZHENXUN_LAUNCHER_PID") and apply_pending_update(
        project_root
    ):
        os.execv(sys.executable, [sys.executable, "-m", "zhenxun.cli", "run-worker"])
    _sync_env_missing_items(project_root)

    from zhenxun.nonebot_store.runtime import activate_current_generation

    activate_current_generation()

    import contextlib
    import platform

    import nonebot

    htmlrender_browser_channel = None
    system = platform.system()

    if system == "Windows":
        import winreg

        paths = {
            "chrome": r"SOFTWARE\Clients\StartMenuInternet\Google Chrome\DefaultIcon",
            "msedge": r"SOFTWARE\Clients\StartMenuInternet\Microsoft Edge\DefaultIcon",
        }
        for name, path in paths.items():
            with contextlib.suppress(FileNotFoundError):
                winreg.OpenKey(winreg.HKEY_LOCAL_MACHINE, path)
                htmlrender_browser_channel = name
                break

    elif system == "Darwin":
        mac_paths = {
            "chrome": "/Applications/Google Chrome.app",
            "msedge": "/Applications/Microsoft Edge.app",
        }
        for name, path in mac_paths.items():
            if Path(path).exists():
                htmlrender_browser_channel = name
                break

    if htmlrender_browser_channel:
        nonebot.logger.info(
            f"使用 {htmlrender_browser_channel} 作为 htmlrender 驱动启动..."
        )

    nonebot.init(
        _env_file=ENV_DEV_FILE,
        htmlrender_browser_channel=htmlrender_browser_channel,
        render_backend="playwright",
        render_playwright={"channel": htmlrender_browser_channel},
    )

    from zhenxun.services.lifecycle import hook_kernel
    from zhenxun.services.runtime_bootstrap import register_runtime_bootstrap
    from zhenxun.services.runtime_reload import plugin_runtime_manager

    hook_kernel.install(plugin_runtime_manager)

    # Core library plugins are process infrastructure, not side effects of
    # importing zhenxun.services. HTMLRender is loaded lazily by the renderer.
    for library_plugin in (
        "nonebot_plugin_apscheduler",
        "nonebot_plugin_alconna",
        "nonebot_plugin_session",
        "nonebot_plugin_uninfo",
        "nonebot_plugin_waiter",
    ):
        nonebot.require(library_plugin)
    plugin_runtime_manager.activate_loaded_incarnations()
    register_runtime_bootstrap(nonebot.get_driver())

    from zhenxun.services.startup import startup_coordinator

    startup_coordinator.record_operation(
        "worker:bootstrap_imports",
        "worker",
        "completed",
        (time.monotonic() - worker_started) * 1000,
    )

    from nonebot.adapters.onebot.v11 import Adapter as OneBotV11Adapter

    from zhenxun.configs.config import BotConfig

    driver = nonebot.get_driver()
    driver.register_adapter(OneBotV11Adapter)
    enabled_adapters = ["OneBot V11"]

    if BotConfig.qq_adapter_load:
        try:
            from zhenxun.adapters.qq_official.config import (
                validate_qq_official_config,
            )

            qq_config = validate_qq_official_config()
            if (
                qq_config.has_webhook_bots
                and qq_config.qq_webhook_mode == "builtin_https"
                and not os.environ.get("ZHENXUN_LAUNCHER_PID")
            ):
                raise RuntimeError(
                    "QQ_WEBHOOK_MODE=builtin_https 必须通过 `zx run` 启动"
                )
            from zhenxun.adapters.qq_official.adapter import ZhenxunQQAdapter
        except ImportError as e:
            raise RuntimeError(
                "QQ_ADAPTER_LOAD=True 但未安装 nonebot-adapter-qq，"
                "请安装后再开启 QQ 官方适配器。"
            ) from e
        driver.register_adapter(ZhenxunQQAdapter)
        enabled_adapters.append("<c>QQ_Official</c>")

    nonebot.logger.opt(colors=True).info(f"已启用适配器: {', '.join(enabled_adapters)}")

    from zhenxun.services.startup_load import startup_load_planner

    phase_started = time.monotonic()
    for model_file in sorted(Path("zhenxun/models").glob("*.py")):
        if model_file.name.startswith("_"):
            continue
        importlib.import_module(f"zhenxun.models.{model_file.stem}")
    startup_coordinator.record_operation(
        "worker:load_core_models",
        "worker",
        "completed",
        (time.monotonic() - phase_started) * 1000,
    )

    source_roots: list[tuple[str, Path]] = [
        ("builtin", Path("zhenxun/builtin_plugins")),
        ("source", Path("zhenxun/plugins")),
    ]
    source_roots.extend(
        ("external", Path(ext.strip())) for ext in BotConfig.ext_path if ext.strip()
    )
    phase_started = time.monotonic()
    startup_load_planner.prepare(source_roots)
    startup_coordinator.record_operation(
        "worker:plan_plugin_load",
        "worker",
        "completed",
        (time.monotonic() - phase_started) * 1000,
        details=startup_load_planner.summary(),
    )
    startup_load_planner.prepare_library_plugins()
    phase_started = time.monotonic()
    startup_load_planner.load_critical()
    plugin_runtime_manager.activate_loaded_incarnations()
    startup_coordinator.record_operation(
        "worker:load_critical_plugins",
        "worker",
        "completed",
        (time.monotonic() - phase_started) * 1000,
    )

    from zhenxun.nonebot_store.runtime import load_managed_plugins

    phase_started = time.monotonic()
    managed_status = load_managed_plugins()
    plugin_runtime_manager.activate_loaded_incarnations()
    startup_coordinator.record_operation(
        "worker:load_managed_plugins",
        "worker",
        "completed",
        (time.monotonic() - phase_started) * 1000,
    )
    if managed_status["failed"]:
        nonebot.logger.error(
            "部分 WebUI 托管的 NoneBot 插件加载失败，已隔离: {}",
            ", ".join(item["store_key"] for item in managed_status["failed"]),
        )

    startup_load_planner.instrument_prebind_hooks(driver)

    from zhenxun.configs.webui_tls import (
        load_webui_tls_settings,
        validate_webui_tls_settings,
    )

    webui_tls = load_webui_tls_settings(project_root)
    validate_webui_tls_settings(
        webui_tls,
        launcher_managed=bool(os.environ.get("ZHENXUN_LAUNCHER_PID")),
    )
    tls_options = (
        {
            "ssl_certfile": webui_tls.certfile,
            "ssl_keyfile": webui_tls.keyfile,
        }
        if webui_tls.enabled
        else {}
    )
    from zhenxun.services.webui_transport import transport_runtime

    transport_runtime.install_uvicorn_signal_bridge()
    proxy_options: dict[str, object] = {}
    if os.environ.get("ZHENXUN_LAUNCHER_PID") and webui_tls.http_sidecar_enabled:
        import ipaddress

        trusted_proxy_ips = ["127.0.0.1", "::1"]
        normalized_host = webui_tls.host.strip().strip("[]")
        with contextlib.suppress(ValueError):
            bound_address = ipaddress.ip_address(normalized_host)
            if not bound_address.is_unspecified:
                trusted_proxy_ips.append(str(bound_address))
        proxy_options = {
            "proxy_headers": True,
            "forwarded_allow_ips": ",".join(dict.fromkeys(trusted_proxy_ips)),
        }
    try:
        nonebot.run(
            workers=1,
            access_log=False,
            limit_concurrency=WORKER_CONNECTION_LIMIT,
            backlog=WORKER_BACKLOG,
            timeout_keep_alive=WORKER_KEEP_ALIVE_TIMEOUT,
            timeout_graceful_shutdown=GRACEFUL_SHUTDOWN_TIMEOUT,
            **proxy_options,
            **tls_options,
        )
    finally:
        transport_runtime.restore_uvicorn_signal_bridge()
        transport_runtime.restore()
        from zhenxun.services.runtime_bootstrap import finalize_runtime_executor

        finalize_runtime_executor()


def _build_worker_command() -> list[str]:
    return [sys.executable, "-m", "zhenxun.cli", "run-worker"]


def _build_ingress_command(settings, *, upstream_scheme: str = "http") -> list[str]:
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


def _build_redirect_command(settings) -> list[str]:
    return [
        sys.executable,
        "-m",
        "zhenxun.cli",
        "run-http-redirect",
        settings.host,
        str(settings.redirect_port),
        str(settings.port),
    ]


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


def _safe_redirect_hostname(raw_host: str) -> str:
    import ipaddress

    value = raw_host.strip()
    if not value or any(ord(char) > 127 for char in value):
        return "localhost"
    if value.startswith("["):
        closing = value.find("]")
        if closing < 0:
            return "localhost"
        hostname = value[1:closing]
        suffix = value[closing + 1 :]
        if suffix and (not suffix.startswith(":") or not suffix[1:].isdigit()):
            return "localhost"
        try:
            ipaddress.IPv6Address(hostname)
        except ValueError:
            return "localhost"
        return f"[{hostname}]"

    hostname, separator, port = value.rpartition(":")
    if separator and port.isdigit() and ":" not in hostname:
        value = hostname
    try:
        address = ipaddress.ip_address(value)
    except ValueError:
        labels = value.rstrip(".").split(".")
        if value != "localhost" and not all(
            label
            and len(label) <= 63
            and label[0].isalnum()
            and label[-1].isalnum()
            and all(char.isalnum() or char == "-" for char in label)
            for label in labels
        ):
            return "localhost"
        return value.rstrip(".")
    if address.version == 6:
        return f"[{address.compressed}]"
    return str(address)


def _ingress_environment() -> dict[str, str]:
    sensitive_markers = ("TOKEN", "SECRET", "PASSWORD", "API_KEY")
    environment = {
        key: value
        for key, value in os.environ.items()
        if key.upper() != "QQ_BOTS"
        and not any(marker in key.upper() for marker in sensitive_markers)
    }
    environment["ZHENXUN_INGRESS_CHILD"] = "1"
    return environment


def _worker_health_url(settings, *, scheme: str = "http") -> str:
    connect_host = settings.worker_connect_host
    host = f"[{connect_host}]" if ":" in connect_host else connect_host
    return f"{scheme}://{host}:{settings.worker_port}/qq/healthz"


def _worker_webui_health_url(settings, *, scheme: str = "http") -> str:
    connect_host = settings.worker_connect_host
    host = f"[{connect_host}]" if ":" in connect_host else connect_host
    return f"{scheme}://{host}:{settings.worker_port}/zhenxun/api/configure/status"


def _worker_runtime_status_url(settings, *, scheme: str = "http") -> str:
    connect_host = settings.worker_connect_host
    host = f"[{connect_host}]" if ":" in connect_host else connect_host
    return (
        f"{scheme}://{host}:{settings.worker_port}" "/zhenxun/api/system/startup/status"
    )


def _health_urlopen(url: str):
    if url.startswith("https://"):
        import ssl

        return urllib.request.urlopen(
            url,
            timeout=1.0,
            context=ssl._create_unverified_context(),
        )
    return urllib.request.urlopen(url, timeout=1.0)


def _worker_is_ready(
    settings, *, scheme: str = "http", require_warmup: bool = False
) -> bool:
    data = _read_worker_runtime_status(settings, scheme=scheme)
    return _runtime_status_is_ready(data, require_warmup=require_warmup)


def _read_worker_runtime_status(
    settings, *, scheme: str = "http"
) -> dict[str, object] | None:
    try:
        with _health_urlopen(
            _worker_runtime_status_url(settings, scheme=scheme)
        ) as response:
            if response.status != 200:
                return None
            payload = json.loads(response.read())
            data = payload.get("data", {})
            return data if isinstance(data, dict) else None
    except (OSError, ValueError, TypeError, urllib.error.URLError):
        return None


def _runtime_status_is_ready(
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


def _bind_worker_runtime_status(
    worker: subprocess.Popen, status: dict[str, object] | None
) -> None:
    if status is None:
        return
    with contextlib.suppress(Exception):
        from zhenxun.services.lifecycle.launcher import bind_launcher_worker_runtime

        bind_launcher_worker_runtime(worker, status)


async def _wait_worker_ready_async(
    worker: subprocess.Popen,
    settings,
    *,
    scheme: str = "http",
    require_warmup: bool = False,
) -> bool:
    from zhenxun.services.lifecycle.launcher import launcher_supervisor

    deadline = time.monotonic() + WORKER_READY_TIMEOUT
    while time.monotonic() < deadline:
        if launcher_supervisor.shutdown_deadline is not None:
            return False
        if worker.poll() is not None:
            return False
        status = await asyncio.to_thread(
            _read_worker_runtime_status, settings, scheme=scheme
        )
        _bind_worker_runtime_status(worker, status)
        if _runtime_status_is_ready(status, require_warmup=require_warmup):
            return True
        if status and status.get("operating_mode") in {
            "management_only",
            "setup_only",
        }:
            return False
        await asyncio.sleep(WORKER_READY_POLL_INTERVAL)
    return False


async def _wait_worker_webui_ready_async(
    worker: subprocess.Popen, settings, *, scheme: str = "http"
) -> bool:
    from zhenxun.services.lifecycle.launcher import launcher_supervisor

    deadline = time.monotonic() + WORKER_READY_TIMEOUT
    while time.monotonic() < deadline:
        if (
            worker.poll() is not None
            or launcher_supervisor.shutdown_deadline is not None
        ):
            return False
        if await asyncio.to_thread(
            _worker_webui_is_ready_once, settings, scheme=scheme
        ):
            return True
        await asyncio.sleep(WORKER_READY_POLL_INTERVAL)
    return False


def _worker_webui_is_ready_once(settings, *, scheme: str = "http") -> bool:
    try:
        with _health_urlopen(
            _worker_webui_health_url(settings, scheme=scheme)
        ) as response:
            return response.status == 200
    except (OSError, urllib.error.URLError):
        return False


def _run_ingress(args: list[str]) -> None:
    if len(args) != 5 or os.environ.get("ZHENXUN_INGRESS_CHILD") != "1":
        raise RuntimeError("run-ingress 参数无效；该命令只能由 zx launcher 调用")
    from zhenxun.adapters.qq_official.ingress import IngressSettings, run_ingress

    run_ingress(
        IngressSettings(
            listen_host=args[0],
            listen_port=int(args[1]),
            certfile=args[2],
            keyfile=args[3],
            upstream_url=args[4],
        )
    )


def _run_http_redirect(args: list[str]) -> None:
    if len(args) != 3 or os.environ.get("ZHENXUN_REDIRECT_CHILD") != "1":
        raise RuntimeError("run-http-redirect 参数无效；该命令只能由 zx launcher 调用")
    from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
    import socket

    listen_host, redirect_port_text, https_port_text = args
    redirect_port = int(redirect_port_text)
    https_port = int(https_port_text)

    class RedirectHandler(BaseHTTPRequestHandler):
        def _redirect(self) -> None:
            hostname = _safe_redirect_hostname(self.headers.get("Host", ""))
            location = f"https://{hostname}:{https_port}{self.path}"
            self.send_response(308)
            self.send_header("Location", location)
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", "0")
            self.end_headers()

        do_GET = _redirect
        do_HEAD = _redirect
        do_POST = _redirect
        do_PUT = _redirect
        do_DELETE = _redirect
        do_OPTIONS = _redirect

        def log_message(self, _format: str, *_args: object) -> None:
            return

    class RedirectServer(ThreadingHTTPServer):
        address_family = socket.AF_INET6 if ":" in listen_host else socket.AF_INET

    server = RedirectServer((listen_host, redirect_port), RedirectHandler)
    try:
        server.serve_forever()
    finally:
        server.server_close()


def _run_http_sidecar(args: list[str]) -> None:
    if len(args) != 6 or os.environ.get("ZHENXUN_HTTP_SIDECAR_CHILD") != "1":
        raise RuntimeError("run-http-sidecar 参数无效；该命令只能由 zx launcher 调用")
    from zhenxun.services.webui_http_sidecar import (
        HttpSidecarSettings,
        run_http_sidecar,
    )

    settings = HttpSidecarSettings(
        mode=args[0],
        listen_host=args[1],
        listen_port=int(args[2]),
        upstream_host=args[3],
        upstream_port=int(args[4]),
        certificate_sha256=args[5],
    )
    try:
        run_http_sidecar(settings)
    except (OSError, RuntimeError, ValueError):
        raise SystemExit(2) from None


def _get_worker_creationflags() -> int:
    if os.name == "nt":
        return getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)
    return 0


def _wait_worker_exit(proc: subprocess.Popen, timeout_seconds: float) -> bool:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            _record_launcher_process_exit(proc, "process_exit")
            return True
        time.sleep(WORKER_POLL_INTERVAL)
    exited = proc.poll() is not None
    if exited:
        _record_launcher_process_exit(proc, "process_exit")
    return exited


def _record_launcher_process_start(role: str, proc: subprocess.Popen) -> None:
    with contextlib.suppress(Exception):
        from zhenxun.services.lifecycle.launcher import observe_launcher_process

        observe_launcher_process(role, proc)


def _record_launcher_process_exit(proc: subprocess.Popen, reason: str) -> None:
    with contextlib.suppress(Exception):
        from zhenxun.services.lifecycle.launcher import release_launcher_process

        release_launcher_process(proc, reason)


def _publish_http_sidecar_state(**changes: object) -> dict[str, object]:
    from zhenxun.services.lifecycle.launcher import update_launcher_metadata
    from zhenxun.services.webui_http_sidecar_state import (
        read_http_sidecar_state,
        write_http_sidecar_state,
    )

    if changes:
        write_http_sidecar_state(**changes)
    state = read_http_sidecar_state()
    update_launcher_metadata(http_sidecar=state)
    return state


def _http_sidecar_process_matches(
    state: dict, process: subprocess.Popen, startup_id: str
) -> bool:
    if state.get("startup_id") != startup_id:
        return False
    runtime_pid = state.get("pid")
    if runtime_pid == process.pid:
        return True
    if os.name != "nt" or not isinstance(runtime_pid, int):
        return False
    import psutil

    # Windows venv python.exe may delegate execution to a child interpreter.
    try:
        return any(
            parent.pid == process.pid
            for parent in psutil.Process(runtime_pid).parents()
        )
    except psutil.Error:
        return False


async def _start_http_sidecar_async(settings, cwd: Path) -> subprocess.Popen | None:
    from uuid import uuid4

    from zhenxun.services.webui_http_sidecar_state import sanitized_sidecar_error

    startup_id = uuid4().hex

    async def wait_ready(process):
        from zhenxun.services.lifecycle.launcher import launcher_supervisor

        deadline = time.monotonic() + HTTP_SIDECAR_START_TIMEOUT
        while process.poll() is None and time.monotonic() < deadline:
            if launcher_supervisor.shutdown_deadline is not None:
                raise OSError("sidecar_startup_interrupted")
            child_state = _publish_http_sidecar_state()
            if _http_sidecar_process_matches(child_state, process, startup_id):
                if child_state.get("state") == "ready":
                    return
                if child_state.get("state") == "degraded":
                    break
            await asyncio.sleep(0.05)
        raise OSError("sidecar_startup_failed")

    _publish_http_sidecar_state(
        startup_id=startup_id,
        mode=settings.effective_http_mode,
        port=settings.redirect_port,
        pid=None,
        state="starting",
        active_connections=0,
        last_error=None,
    )
    try:
        process = await _spawn_launcher_process(
            "http_sidecar",
            partial(
                subprocess.Popen,
                _build_http_sidecar_command(settings),
                cwd=str(cwd),
                creationflags=_get_worker_creationflags(),
                env={
                    **os.environ,
                    "ZHENXUN_HTTP_SIDECAR_CHILD": "1",
                    "ZHENXUN_HTTP_SIDECAR_STARTUP_ID": startup_id,
                },
            ),
            ready=wait_ready,
        )
    except OSError as error:
        _publish_http_sidecar_state(
            state="degraded",
            pid=None,
            last_error=sanitized_sidecar_error(error),
        )
        return None
    _record_launcher_process_start("http_sidecar", process)
    _publish_http_sidecar_state(state="ready", spawn_pid=process.pid)
    _launcher_log(
        "WebUI HTTP compatibility sidecar ready on "
        f"{settings.host}:{settings.redirect_port} "
        f"(mode={settings.effective_http_mode})"
    )
    return process


def _terminate_worker(proc: subprocess.Popen) -> None:
    asyncio.run(_terminate_worker_async(proc))


def _terminate_named_process(proc: subprocess.Popen, name: str) -> None:
    asyncio.run(_terminate_named_process_async(proc, name))


async def _terminate_worker_async(proc: subprocess.Popen) -> None:
    from zhenxun.services.lifecycle.launcher import launcher_supervisor

    await launcher_supervisor.stop_process(proc)


async def _terminate_named_process_async(proc: subprocess.Popen, name: str) -> None:
    _launcher_log(f"stopping {name} pid={proc.pid}")
    await _terminate_worker_async(proc)


async def _spawn_launcher_process(
    role: str, factory, *, ready=None
) -> subprocess.Popen:
    from zhenxun.services.lifecycle.launcher import launcher_supervisor

    return await launcher_supervisor.start_process(role, factory, ready=ready)


async def _run_launcher_command(command, *, cwd):
    from zhenxun.services.lifecycle.launcher import launcher_supervisor

    process = await _spawn_launcher_process(
        "dependency_sync",
        partial(
            subprocess.Popen,
            command,
            cwd=cwd,
            creationflags=_get_worker_creationflags(),
        ),
    )
    try:
        while process.poll() is None:
            if launcher_supervisor.shutdown_deadline is not None:
                raise asyncio.CancelledError()
            await asyncio.sleep(WORKER_POLL_INTERVAL)
        if process.returncode:
            raise subprocess.CalledProcessError(process.returncode, command)
    finally:
        await launcher_supervisor.stop_process(process)


def _start_http_sidecar(settings, cwd: Path) -> subprocess.Popen | None:
    return asyncio.run(_start_http_sidecar_async(settings, cwd))


def _wait_worker_ready(*args, **kwargs) -> bool:
    return asyncio.run(_wait_worker_ready_async(*args, **kwargs))


def _wait_worker_webui_ready(*args, **kwargs) -> bool:
    return asyncio.run(_wait_worker_webui_ready_async(*args, **kwargs))


def _run_launcher() -> None:
    asyncio.run(_launcher_entry())


async def _launcher_entry() -> None:
    from zhenxun.services.lifecycle.launcher import launcher_supervisor

    signals = [signal.SIGINT, signal.SIGTERM]
    if hasattr(signal, "SIGBREAK"):
        signals.append(signal.SIGBREAK)
    original = {sig: signal.getsignal(sig) for sig in signals}
    try:
        await _run_launcher_async()
    finally:
        try:
            await launcher_supervisor.shutdown()
        finally:
            for sig, handler in original.items():
                signal.signal(sig, handler)


async def _run_launcher_async() -> None:
    launcher_started_at = time.time()
    cwd = _ensure_project_root()
    from zhenxun.services.lifecycle.launcher import initialize_launcher_lifecycle
    from zhenxun.update_service import (
        applied_update_pending,
        apply_pending_update,
        finalize_applied_update,
        rollback_applied_update,
    )

    launcher_boot_id = initialize_launcher_lifecycle()
    from zhenxun.services.lifecycle.launcher import launcher_supervisor

    pending_bot_verification = await launcher_supervisor.run_recovery(
        lambda: apply_pending_update(cwd) or applied_update_pending()
    )
    _sync_env_missing_items(cwd)
    from zhenxun.adapters.qq_official.config import (
        load_qq_launcher_settings,
        validate_builtin_ingress,
        validate_qq_config_data,
    )
    from zhenxun.configs.webui_tls import (
        certificate_sha256,
        load_webui_tls_settings,
        validate_webui_tls_settings,
    )
    from zhenxun.services.lifecycle.launcher import (
        begin_launcher_commit,
        transition_launcher_commit,
    )
    from zhenxun.utils.restart_state import (
        clear_launcher_restart_signal,
        consume_launcher_action,
    )

    clear_launcher_restart_signal()
    current_worker: subprocess.Popen | None = None
    ingress: subprocess.Popen | None = None
    ingress_signature: tuple[str, int, str, str, str] | None = None
    http_sidecar: subprocess.Popen | None = None
    http_sidecar_signature: tuple[str, str, int, int, str] | None = None
    http_sidecar_retry_index = 0
    next_http_sidecar_retry = 0.0
    stop_requested = False
    stop_signal: int | None = None

    def _handle_launcher_signal(signum, _frame) -> None:
        nonlocal stop_requested, stop_signal
        if stop_requested:
            return
        from zhenxun.services.lifecycle.launcher import launcher_supervisor

        launcher_supervisor.begin_shutdown()
        stop_requested = True
        stop_signal = int(signum)
        _launcher_log(f"received signal {signum}, scheduling worker shutdown")

    handled_signals = [signal.SIGINT]
    if hasattr(signal, "SIGTERM"):
        handled_signals.append(signal.SIGTERM)
    if hasattr(signal, "SIGBREAK"):
        handled_signals.append(signal.SIGBREAK)
    for sig in handled_signals:
        try:
            signal.signal(sig, _handle_launcher_signal)
        except Exception:
            pass

    while True:
        if stop_requested:
            raise SystemExit(128 + int(stop_signal or signal.SIGINT))
        from zhenxun.nonebot_store.runtime import (
            apply_pending_transaction as apply_pending_nonebot_transaction,
        )
        from zhenxun.nonebot_store.runtime import (
            finalize_pending_transaction as finalize_nonebot_transaction,
        )
        from zhenxun.nonebot_store.runtime import (
            rollback_pending_transaction as rollback_nonebot_transaction,
        )
        from zhenxun.nonebot_store.runtime import (
            startup_verification as verify_nonebot_startup,
        )
        from zhenxun.nonebot_store.storage import load_manifest as load_nonebot_manifest
        from zhenxun.nonebot_store.storage import (
            pending_transaction as pending_nonebot_transaction,
        )
        from zhenxun.plugin_store_transaction import (
            apply_pending_transaction as apply_pending_source_transaction,
        )
        from zhenxun.plugin_store_transaction import (
            finalize_pending_transaction as finalize_source_transaction,
        )
        from zhenxun.plugin_store_transaction import (
            pending_transaction as pending_source_transaction,
        )
        from zhenxun.plugin_store_transaction import prepare_dependency_transaction
        from zhenxun.plugin_store_transaction import (
            rollback_pending_transaction as rollback_source_transaction,
        )
        from zhenxun.plugin_store_transaction import (
            startup_verification as verify_source_startup,
        )
        from zhenxun.utils.restart_state import clear_pending_restart_state

        source_before_apply = pending_source_transaction()
        nonebot_before_apply = pending_nonebot_transaction()
        commit_targets = [
            *(
                ["source_plugins"]
                if source_before_apply
                and source_before_apply.get("state")
                in {"pending_restart", "verification_pending"}
                else []
            ),
            *(
                ["nonebot_generation"]
                if nonebot_before_apply
                and nonebot_before_apply.get("state")
                in {"pending_restart", "verification_pending"}
                else []
            ),
        ]
        if commit_targets:
            begin_launcher_commit(commit_targets)
        dependencies_ready = prepare_dependency_transaction()
        nonebot_applied = False
        if dependencies_ready:
            nonebot_applied = apply_pending_nonebot_transaction()
        nonebot_after_apply = pending_nonebot_transaction()
        nonebot_apply_failed = bool(
            nonebot_before_apply
            and nonebot_before_apply.get("state") == "pending_restart"
            and nonebot_after_apply
            and nonebot_after_apply.get("state") in {"failed", "migration_blocked"}
        )
        source_applied = (
            apply_pending_source_transaction()
            if dependencies_ready and not nonebot_apply_failed
            else False
        )
        source_after_apply = pending_source_transaction()
        source_apply_failed = bool(
            source_before_apply
            and source_before_apply.get("state") == "pending_restart"
            and source_after_apply
            and source_after_apply.get("state") == "failed"
        )
        if nonebot_applied or source_applied:
            with contextlib.suppress(Exception):
                transition_launcher_commit("applied")
        if source_applied:
            _launcher_log("真寻插件源码事务已应用，等待 worker 启动验证")
        if source_apply_failed and nonebot_applied:
            with contextlib.suppress(Exception):
                transition_launcher_commit("rolling_back")
            rollback_nonebot_transaction()
            nonebot_applied = False
            _launcher_log("真寻插件源码事务应用失败，已回滚依赖 generation")
            with contextlib.suppress(Exception):
                transition_launcher_commit("rolled_back")
        if nonebot_apply_failed or source_apply_failed:
            with contextlib.suppress(Exception):
                transition_launcher_commit("rolling_back")
                transition_launcher_commit("rolled_back")
        if nonebot_applied:
            _launcher_log("NoneBot 插件依赖层已构建，等待 worker 启动验证")
        source_pending = pending_source_transaction()
        verify_source_transaction = bool(
            source_pending and source_pending.get("state") == "verification_pending"
        )
        verify_nonebot_generation = bool(
            load_nonebot_manifest().get("pending_verification")
        )
        qq_settings = load_qq_launcher_settings(cwd)
        webui_tls = load_webui_tls_settings(cwd)
        builtin_ingress = bool(
            qq_settings.enabled
            and qq_settings.config.has_webhook_bots
            and qq_settings.config.qq_webhook_mode == "builtin_https"
        )
        validate_webui_tls_settings(
            webui_tls,
            qq_https_port=(
                qq_settings.config.qq_webhook_listen_port if builtin_ingress else None
            ),
            launcher_managed=True,
            check_redirect_port=http_sidecar is None,
        )
        desired_http_sidecar_signature = (
            (
                webui_tls.effective_http_mode,
                webui_tls.host,
                webui_tls.redirect_port,
                webui_tls.port,
                certificate_sha256(webui_tls.certfile),
            )
            if webui_tls.http_sidecar_enabled
            else None
        )
        if (
            http_sidecar is not None
            and http_sidecar_signature != desired_http_sidecar_signature
        ):
            await _terminate_named_process_async(http_sidecar, "WebUI HTTP sidecar")
            http_sidecar = None
            http_sidecar_signature = None
        if not webui_tls.http_sidecar_enabled:
            _publish_http_sidecar_state(
                mode="disabled",
                port=None,
                pid=None,
                state="disabled",
                active_connections=0,
                last_error=None,
            )
        desired_ingress_signature = (
            (
                qq_settings.config.qq_webhook_listen_host,
                qq_settings.config.qq_webhook_listen_port,
                qq_settings.config.qq_webhook_tls_certfile,
                qq_settings.config.qq_webhook_tls_keyfile,
                _worker_health_url(qq_settings, scheme=webui_tls.scheme),
            )
            if builtin_ingress
            else None
        )
        if ingress is not None and ingress_signature != desired_ingress_signature:
            await _terminate_named_process_async(ingress, "QQ HTTPS ingress")
            ingress = None
            ingress_signature = None
        if qq_settings.enabled:
            validate_qq_config_data(qq_settings.config)
        if builtin_ingress:
            validate_builtin_ingress(qq_settings, check_port=ingress is None)
        worker_env = os.environ.copy()
        worker_env["ZHENXUN_LAUNCHER_PID"] = str(os.getpid())
        worker_env["ZHENXUN_LAUNCHER_STARTED_AT"] = str(launcher_started_at)
        worker_env["ZHENXUN_WORKER_SPAWNED_AT"] = str(time.time())
        worker_env["ZHENXUN_LAUNCHER_BOOT_ID"] = launcher_boot_id
        worker = await _spawn_launcher_process(
            "worker",
            partial(
                subprocess.Popen,
                _build_worker_command(),
                cwd=str(cwd),
                creationflags=_get_worker_creationflags(),
                env=worker_env,
            ),
        )
        _record_launcher_process_start("worker", worker)
        current_worker = worker
        if verify_nonebot_generation or verify_source_transaction:
            with contextlib.suppress(Exception):
                transition_launcher_commit("verifying")
            _launcher_log("waiting for managed plugin transaction verification")
            if not await _wait_worker_ready_async(
                worker,
                qq_settings,
                scheme=webui_tls.scheme,
                require_warmup=True,
            ):
                await _terminate_worker_async(worker)
                current_worker = None
                _launcher_log("managed plugin worker failed, rolling back transaction")
                with contextlib.suppress(Exception):
                    transition_launcher_commit("rolling_back")
                if verify_nonebot_generation:
                    rollback_nonebot_transaction()
                if verify_source_transaction:
                    rollback_source_transaction(
                        [{"code": "worker_health_check_failed"}]
                    )
                with contextlib.suppress(Exception):
                    transition_launcher_commit("rolled_back")
                continue
            nonebot_verified, nonebot_verification = (
                verify_nonebot_startup()
                if verify_nonebot_generation
                else (True, {"failed": []})
            )
            source_verified, source_verification = (
                verify_source_startup()
                if verify_source_transaction
                else (True, {"failed": []})
            )
            if not nonebot_verified or not source_verified:
                await _terminate_worker_async(worker)
                current_worker = None
                failed = [
                    *(nonebot_verification.get("failed") or []),
                    *(source_verification.get("failed") or []),
                ]
                _launcher_log(
                    "managed plugin verification failed, rolling back transaction: "
                    + ", ".join(
                        str(item.get("store_key", "unknown")) for item in failed
                    )
                )
                with contextlib.suppress(Exception):
                    transition_launcher_commit("rolling_back")
                if verify_nonebot_generation:
                    rollback_nonebot_transaction()
                if verify_source_transaction:
                    rollback_source_transaction(failed)
                with contextlib.suppress(Exception):
                    transition_launcher_commit("rolled_back")
                continue
            if verify_nonebot_generation:
                finalize_nonebot_transaction()
                clear_pending_restart_state("webui.nonebot-store")
            if verify_source_transaction:
                finalize_source_transaction()
                for operation in (source_pending or {}).get("operations", []):
                    clear_pending_restart_state(
                        f"webui.plugin:{operation.get('store_key', '')}"
                    )
            _launcher_log("managed plugin transaction passed startup verification")
            with contextlib.suppress(Exception):
                transition_launcher_commit("committed")
        if pending_bot_verification:
            _launcher_log("waiting for updated worker health verification")
            if not await _wait_worker_ready_async(
                worker, qq_settings, scheme=webui_tls.scheme
            ):
                await _terminate_worker_async(worker)
                current_worker = None
                _launcher_log("updated worker failed health check, rolling back")
                rollback_applied_update()
                pending_bot_verification = False
                continue
            finalize_applied_update()
            pending_bot_verification = False
            _launcher_log("updated worker passed health verification")
        if builtin_ingress and ingress is None:
            _launcher_log(
                "waiting for QQ worker readiness before opening HTTPS ingress"
            )
            runtime_available = await _wait_worker_ready_async(
                worker, qq_settings, scheme=webui_tls.scheme
            )
            if not runtime_available:
                status = await asyncio.to_thread(
                    _read_worker_runtime_status, qq_settings, scheme=webui_tls.scheme
                )
                _bind_worker_runtime_status(worker, status)
                if status and status.get("operating_mode") in {
                    "management_only",
                    "setup_only",
                }:
                    _launcher_log(
                        "worker entered management/setup-only mode; "
                        "QQ ingress remains closed"
                    )
                else:
                    await _terminate_worker_async(worker)
                    raise RuntimeError(
                        "QQ worker 未在规定时间内就绪，HTTPS Ingress 未启动"
                    )
            if runtime_available:
                ingress = await _spawn_launcher_process(
                    "qq_ingress",
                    partial(
                        subprocess.Popen,
                        _build_ingress_command(
                            qq_settings, upstream_scheme=webui_tls.scheme
                        ),
                        cwd=str(cwd),
                        creationflags=_get_worker_creationflags(),
                        env=_ingress_environment(),
                    ),
                )
                _record_launcher_process_start("qq_ingress", ingress)
                ingress_signature = desired_ingress_signature
                await asyncio.sleep(0.25)
                if ingress.poll() is not None:
                    code = ingress.returncode
                    _record_launcher_process_exit(ingress, "unexpected_exit")
                    ingress = None
                    await _terminate_worker_async(worker)
                    raise SystemExit(code or 1)
                _launcher_log(
                    "QQ HTTPS ingress ready on "
                    f"{qq_settings.config.qq_webhook_listen_host}:"
                    f"{qq_settings.config.qq_webhook_listen_port}"
                )
        if webui_tls.http_sidecar_enabled and http_sidecar is None:
            if not await _wait_worker_webui_ready_async(
                worker, qq_settings, scheme=webui_tls.scheme
            ):
                _publish_http_sidecar_state(
                    mode=webui_tls.effective_http_mode,
                    port=webui_tls.redirect_port,
                    pid=None,
                    state="degraded",
                    active_connections=0,
                    last_error="https_worker_not_ready",
                )
                next_http_sidecar_retry = time.monotonic() + 1.0
                _launcher_log(
                    "WebUI HTTP sidecar is degraded: HTTPS worker is not ready"
                )
            else:
                http_sidecar = await _start_http_sidecar_async(webui_tls, cwd)
                if http_sidecar is not None:
                    http_sidecar_signature = desired_http_sidecar_signature
                    http_sidecar_retry_index = 0
                else:
                    next_http_sidecar_retry = time.monotonic() + 1.0
        restart_requested = False
        restart_action: tuple[str, list[str]] | None = None
        return_code: int | None = None
        next_restart_check = 0.0
        try:
            while True:
                return_code = worker.poll()
                if return_code is not None:
                    break
                if ingress is not None and ingress.poll() is not None:
                    ingress_code = ingress.returncode
                    _record_launcher_process_exit(ingress, "unexpected_exit")
                    ingress = None
                    _launcher_log("QQ HTTPS ingress exited unexpectedly")
                    await _terminate_worker_async(worker)
                    raise SystemExit(ingress_code or 1)
                if http_sidecar is not None and http_sidecar.poll() is not None:
                    _record_launcher_process_exit(http_sidecar, "unexpected_exit")
                    http_sidecar = None
                    http_sidecar_signature = None
                    delay = HTTP_SIDECAR_RETRY_DELAYS[
                        min(
                            http_sidecar_retry_index,
                            len(HTTP_SIDECAR_RETRY_DELAYS) - 1,
                        )
                    ]
                    http_sidecar_retry_index += 1
                    next_http_sidecar_retry = time.monotonic() + delay
                    _publish_http_sidecar_state(
                        state="degraded",
                        pid=None,
                        active_connections=0,
                        last_error="sidecar_unexpected_exit",
                        retry_in_seconds=delay,
                    )
                    _launcher_log(
                        "WebUI HTTP sidecar exited unexpectedly; "
                        f"HTTPS remains available, retrying in {delay:g}s"
                    )
                if stop_requested:
                    clear_launcher_restart_signal()
                    if ingress is not None:
                        await _terminate_named_process_async(
                            ingress, "QQ HTTPS ingress"
                        )
                        ingress = None
                    if http_sidecar is not None:
                        await _terminate_named_process_async(
                            http_sidecar, "WebUI HTTP sidecar"
                        )
                        http_sidecar = None
                    await _terminate_worker_async(worker)
                    raise SystemExit(128 + int(stop_signal or signal.SIGINT))
                now = time.monotonic()
                if (
                    webui_tls.http_sidecar_enabled
                    and http_sidecar is None
                    and now >= next_http_sidecar_retry
                ):
                    if await asyncio.to_thread(
                        _worker_webui_is_ready_once,
                        qq_settings,
                        scheme=webui_tls.scheme,
                    ):
                        http_sidecar = await _start_http_sidecar_async(webui_tls, cwd)
                    if http_sidecar is not None:
                        http_sidecar_signature = desired_http_sidecar_signature
                        http_sidecar_retry_index = 0
                        next_http_sidecar_retry = 0.0
                    else:
                        delay = HTTP_SIDECAR_RETRY_DELAYS[
                            min(
                                http_sidecar_retry_index,
                                len(HTTP_SIDECAR_RETRY_DELAYS) - 1,
                            )
                        ]
                        http_sidecar_retry_index += 1
                        next_http_sidecar_retry = now + delay
                        _publish_http_sidecar_state(
                            state="degraded",
                            pid=None,
                            active_connections=0,
                            retry_in_seconds=delay,
                        )
                if now >= next_restart_check:
                    next_restart_check = now + RESTART_POLL_INTERVAL
                    status = await asyncio.to_thread(
                        _read_worker_runtime_status,
                        qq_settings,
                        scheme=webui_tls.scheme,
                    )
                    _bind_worker_runtime_status(worker, status)
                    if action := consume_launcher_action():
                        from zhenxun.services.lifecycle.launcher import (
                            launcher_supervisor,
                        )

                        launcher_supervisor.begin_shutdown()
                        restart_requested = True
                        restart_action = action
                        _launcher_log(
                            "detected restart request, stopping current worker"
                        )
                        if ingress is not None:
                            await _terminate_named_process_async(
                                ingress, "QQ HTTPS ingress"
                            )
                            ingress = None
                            ingress_signature = None
                        if http_sidecar is not None:
                            await _terminate_named_process_async(
                                http_sidecar, "WebUI HTTP sidecar"
                            )
                            http_sidecar = None
                            http_sidecar_signature = None
                        await _terminate_worker_async(worker)
                        return_code = worker.poll()
                        break
                await asyncio.sleep(WORKER_POLL_INTERVAL)
        except KeyboardInterrupt:
            clear_launcher_restart_signal()
            if ingress is not None:
                await _terminate_named_process_async(ingress, "QQ HTTPS ingress")
                ingress = None
            if http_sidecar is not None:
                await _terminate_named_process_async(http_sidecar, "WebUI HTTP sidecar")
                http_sidecar = None
            await _terminate_worker_async(worker)
            return
        finally:
            if ingress is not None:
                await _terminate_named_process_async(ingress, "QQ HTTPS ingress")
                ingress = None
                ingress_signature = None
            if http_sidecar is not None:
                await _terminate_named_process_async(http_sidecar, "WebUI HTTP sidecar")
                http_sidecar = None
                http_sidecar_signature = None
            if webui_tls.http_sidecar_enabled:
                _publish_http_sidecar_state(
                    state="stopped",
                    pid=None,
                    active_connections=0,
                    retry_in_seconds=None,
                )
            _record_launcher_process_exit(
                worker,
                "restart" if restart_requested else "worker_exit",
            )
            if current_worker is worker:
                current_worker = None

        if restart_requested or (restart_action := consume_launcher_action()):
            if restart_action and restart_action[0] == "sync_dependencies_restart":
                dependency_paths = restart_action[1]
                from zhenxun.nonebot_store.storage import (
                    save_dependency_sync_status,
                )

                save_dependency_sync_status(
                    {"status": "running", "paths": dependency_paths}
                )
                try:
                    if any(
                        path in {"pyproject.toml", "uv.lock"}
                        for path in dependency_paths
                    ):
                        from zhenxun.update_service import _sync_dependencies

                        _sync_dependencies(preserve_extras=True)
                    for dependency_path in dependency_paths:
                        if Path(dependency_path).name not in {
                            "requirement.txt",
                            "requirements.txt",
                        }:
                            continue
                        await _run_launcher_command(
                            ["uv", "pip", "install", "-r", dependency_path],
                            cwd=str(cwd),
                        )
                except Exception as error:
                    _launcher_log(
                        "dependency sync failed; restarting worker with the "
                        f"current environment ({type(error).__name__})"
                    )
                    save_dependency_sync_status(
                        {
                            "status": "failed",
                            "code": getattr(error, "code", "dependency_sync_failed"),
                            "paths": dependency_paths,
                        }
                    )
                else:
                    save_dependency_sync_status(
                        {"status": "succeeded", "paths": dependency_paths}
                    )
            pending_bot_verification = await launcher_supervisor.run_recovery(
                lambda: apply_pending_update(cwd)
            )
            continue
        if ingress is not None:
            await _terminate_named_process_async(ingress, "QQ HTTPS ingress")
            ingress = None
        if http_sidecar is not None:
            await _terminate_named_process_async(http_sidecar, "WebUI HTTP sidecar")
            http_sidecar = None
        raise SystemExit(return_code if return_code is not None else 1)


def main() -> None:
    args = sys.argv[1:]

    if not args or args[0] == "run":
        from zhenxun.startup_banner import show_startup_banner

        show_startup_banner()
        _run_launcher()
    elif args[0] == "run-worker":
        if not os.environ.get("ZHENXUN_LAUNCHER_PID"):
            from zhenxun.startup_banner import show_startup_banner

            show_startup_banner(role="worker")
        _run_worker()
    elif args[0] == "run-ingress":
        _run_ingress(args[1:])
    elif args[0] == "run-http-redirect":
        _run_http_redirect(args[1:])
    elif args[0] == "run-http-sidecar":
        _run_http_sidecar(args[1:])
    elif args[0] == "version":
        _print_version()
    elif args[0] in ("-h", "--help", "help"):
        sys.stdout.write((__doc__ or "") + "\n")
    else:
        sys.stderr.write(f"未知命令: {args[0]}\n")
        sys.stderr.write((__doc__ or "") + "\n")
        sys.exit(1)


if __name__ == "__main__":
    main()
