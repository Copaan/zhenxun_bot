"""zx CLI — 绪山真寻 Bot 命令行工具

用法:
    zx run          启动 launcher
    zx run-worker   启动 worker（由 launcher 调用）
    zx version      显示版本信息
"""

from __future__ import annotations

import atexit
import importlib.metadata
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
    missing_blocks: list[tuple[int, list[str]]] = []

    for block_index, (_, block) in enumerate(example_blocks):
        key = _env_block_key(block)
        if key and key not in existing_keys:
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
    project_root = _ensure_project_root()
    from zhenxun.update_service import apply_pending_update

    if apply_pending_update(project_root):
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
        htmlrender_browser_channel=htmlrender_browser_channel,
        render_backend="playwright",
        render_playwright={"channel": htmlrender_browser_channel},
    )

    from zhenxun.services.runtime_reload import plugin_runtime_manager

    plugin_runtime_manager.install()

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

    nonebot.load_plugins("zhenxun/builtin_plugins")
    nonebot.load_plugins("zhenxun/plugins")

    for ext in BotConfig.ext_path:
        ext = ext.strip()
        if ext:
            nonebot.logger.info(f"加载第三方插件目录: {ext}")
            nonebot.load_plugins(ext)

    from zhenxun.nonebot_store.runtime import load_managed_plugins

    managed_status = load_managed_plugins()
    if managed_status["failed"]:
        nonebot.logger.error(
            "部分 WebUI 托管的 NoneBot 插件加载失败，已隔离: {}",
            ", ".join(item["store_key"] for item in managed_status["failed"]),
        )

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
    nonebot.run(
        workers=1,
        access_log=False,
        limit_concurrency=WORKER_CONNECTION_LIMIT,
        backlog=WORKER_BACKLOG,
        timeout_keep_alive=WORKER_KEEP_ALIVE_TIMEOUT,
        timeout_graceful_shutdown=GRACEFUL_SHUTDOWN_TIMEOUT,
        **tls_options,
    )


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


def _health_urlopen(url: str):
    if url.startswith("https://"):
        import ssl

        return urllib.request.urlopen(
            url,
            timeout=1.0,
            context=ssl._create_unverified_context(),
        )
    return urllib.request.urlopen(url, timeout=1.0)


def _worker_is_ready(settings, *, scheme: str = "http") -> bool:
    try:
        with _health_urlopen(_worker_health_url(settings, scheme=scheme)) as response:
            return response.status == 200 and b'"status":"ready"' in response.read(256)
    except (OSError, urllib.error.URLError):
        return False


def _wait_worker_ready(
    worker: subprocess.Popen, settings, *, scheme: str = "http"
) -> bool:
    deadline = time.monotonic() + WORKER_READY_TIMEOUT
    while time.monotonic() < deadline:
        if worker.poll() is not None:
            return False
        if _worker_is_ready(settings, scheme=scheme):
            return True
        time.sleep(WORKER_READY_POLL_INTERVAL)
    return False


def _wait_worker_webui_ready(
    worker: subprocess.Popen, settings, *, scheme: str = "http"
) -> bool:
    deadline = time.monotonic() + WORKER_READY_TIMEOUT
    while time.monotonic() < deadline:
        if worker.poll() is not None:
            return False
        try:
            with _health_urlopen(
                _worker_webui_health_url(settings, scheme=scheme)
            ) as response:
                if response.status == 200:
                    return True
        except (OSError, urllib.error.URLError):
            pass
        time.sleep(WORKER_READY_POLL_INTERVAL)
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


def _get_worker_creationflags() -> int:
    if os.name == "nt":
        return getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)
    return 0


def _wait_worker_exit(proc: subprocess.Popen, timeout_seconds: float) -> bool:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            return True
        time.sleep(WORKER_POLL_INTERVAL)
    return proc.poll() is not None


def _terminate_worker(proc: subprocess.Popen) -> None:
    if proc.poll() is not None:
        return
    _launcher_log(f"stopping worker pid={proc.pid}")
    if os.name == "nt":
        ctrl_break_event = getattr(signal, "CTRL_BREAK_EVENT", None)
        if ctrl_break_event is not None:
            try:
                _launcher_log(f"sending CTRL_BREAK_EVENT to worker pid={proc.pid}")
                proc.send_signal(ctrl_break_event)
            except Exception as e:
                _launcher_log(f"failed to send CTRL_BREAK_EVENT: {e!r}")
            else:
                if _wait_worker_exit(proc, WORKER_SOFT_EXIT_TIMEOUT):
                    _launcher_log(
                        f"worker pid={proc.pid} exited after CTRL_BREAK_EVENT "
                        f"with code {proc.returncode}"
                    )
                    return
                _launcher_log(
                    f"worker pid={proc.pid} did not exit after "
                    f"{WORKER_SOFT_EXIT_TIMEOUT:.0f}s"
                )
    if _wait_worker_exit(proc, 1.0):
        return
    try:
        _launcher_log(f"terminating worker pid={proc.pid}")
        proc.terminate()
    except Exception as e:
        _launcher_log(f"failed to terminate worker: {e!r}")
    else:
        if _wait_worker_exit(proc, WORKER_TERMINATE_TIMEOUT):
            _launcher_log(
                f"worker pid={proc.pid} exited after terminate with code "
                f"{proc.returncode}"
            )
            return
        _launcher_log(f"worker pid={proc.pid} did not exit after terminate timeout")
    _launcher_log(f"killing worker pid={proc.pid}")
    proc.kill()
    proc.wait(timeout=WORKER_KILL_TIMEOUT)


def _terminate_named_process(proc: subprocess.Popen, name: str) -> None:
    if proc.poll() is not None:
        return
    _launcher_log(f"stopping {name} pid={proc.pid}")
    if os.name == "nt" and hasattr(signal, "CTRL_BREAK_EVENT"):
        try:
            proc.send_signal(signal.CTRL_BREAK_EVENT)
        except OSError:
            pass
        else:
            if _wait_worker_exit(proc, WORKER_SOFT_EXIT_TIMEOUT):
                return
    proc.terminate()
    if _wait_worker_exit(proc, WORKER_TERMINATE_TIMEOUT):
        return
    proc.kill()
    proc.wait(timeout=WORKER_KILL_TIMEOUT)


def _run_launcher() -> None:
    cwd = _ensure_project_root()
    from zhenxun.update_service import (
        applied_update_pending,
        apply_pending_update,
        finalize_applied_update,
        rollback_applied_update,
    )

    pending_bot_verification = apply_pending_update(cwd) or applied_update_pending()
    _sync_env_missing_items(cwd)
    from zhenxun.adapters.qq_official.config import (
        load_qq_launcher_settings,
        validate_builtin_ingress,
        validate_qq_config_data,
    )
    from zhenxun.configs.webui_tls import (
        load_webui_tls_settings,
        validate_webui_tls_settings,
    )
    from zhenxun.utils.restart_state import (
        clear_launcher_restart_signal,
        consume_launcher_action,
    )

    clear_launcher_restart_signal()
    current_worker: subprocess.Popen | None = None
    ingress: subprocess.Popen | None = None
    ingress_signature: tuple[str, int, str, str, str] | None = None
    redirect: subprocess.Popen | None = None
    redirect_signature: tuple[str, int, int] | None = None
    stop_requested = False
    stop_signal: int | None = None

    def _cleanup_current_worker() -> None:
        if ingress is not None:
            _terminate_named_process(ingress, "QQ HTTPS ingress")
        if redirect is not None:
            _terminate_named_process(redirect, "WebUI HTTP redirect")
        if current_worker is not None:
            _terminate_worker(current_worker)

    atexit.register(_cleanup_current_worker)

    def _handle_launcher_signal(signum, _frame) -> None:
        nonlocal stop_requested, stop_signal
        if stop_requested:
            _launcher_log(f"received signal {signum} while stopping, exiting launcher")
            raise SystemExit(128 + int(signum))
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

        if apply_pending_nonebot_transaction():
            _launcher_log("NoneBot 插件依赖层已构建，等待 worker 启动验证")
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
            check_redirect_port=redirect is None,
        )
        desired_redirect_signature = (
            (webui_tls.host, webui_tls.redirect_port, webui_tls.port)
            if webui_tls.redirect_enabled
            else None
        )
        if redirect is not None and redirect_signature != desired_redirect_signature:
            _terminate_named_process(redirect, "WebUI HTTP redirect")
            redirect = None
            redirect_signature = None
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
            _terminate_named_process(ingress, "QQ HTTPS ingress")
            ingress = None
            ingress_signature = None
        if qq_settings.enabled:
            validate_qq_config_data(qq_settings.config)
        if builtin_ingress:
            validate_builtin_ingress(qq_settings, check_port=ingress is None)
        worker_env = os.environ.copy()
        worker_env["ZHENXUN_LAUNCHER_PID"] = str(os.getpid())
        worker = subprocess.Popen(
            _build_worker_command(),
            cwd=str(cwd),
            creationflags=_get_worker_creationflags(),
            env=worker_env,
        )
        current_worker = worker
        if verify_nonebot_generation:
            _launcher_log("waiting for managed NoneBot plugin verification")
            if not _wait_worker_webui_ready(
                worker, qq_settings, scheme=webui_tls.scheme
            ):
                _terminate_worker(worker)
                current_worker = None
                _launcher_log("managed plugin worker failed, rolling back generation")
                rollback_nonebot_transaction()
                continue
            verified, verification = verify_nonebot_startup()
            if not verified:
                _terminate_worker(worker)
                current_worker = None
                failed = verification.get("failed") or []
                _launcher_log(
                    "managed plugin verification failed, rolling back generation: "
                    + ", ".join(
                        str(item.get("store_key", "unknown")) for item in failed
                    )
                )
                rollback_nonebot_transaction()
                continue
            finalize_nonebot_transaction()
            _launcher_log("managed NoneBot plugins passed startup verification")
        if pending_bot_verification:
            _launcher_log("waiting for updated worker health verification")
            if not _wait_worker_webui_ready(
                worker, qq_settings, scheme=webui_tls.scheme
            ):
                _terminate_worker(worker)
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
            if not _wait_worker_ready(worker, qq_settings, scheme=webui_tls.scheme):
                _terminate_worker(worker)
                raise RuntimeError("QQ worker 未在规定时间内就绪，HTTPS Ingress 未启动")
            ingress = subprocess.Popen(
                _build_ingress_command(qq_settings, upstream_scheme=webui_tls.scheme),
                cwd=str(cwd),
                creationflags=_get_worker_creationflags(),
                env=_ingress_environment(),
            )
            ingress_signature = desired_ingress_signature
            time.sleep(0.25)
            if ingress.poll() is not None:
                code = ingress.returncode
                ingress = None
                _terminate_worker(worker)
                raise SystemExit(code or 1)
            _launcher_log(
                "QQ HTTPS ingress ready on "
                f"{qq_settings.config.qq_webhook_listen_host}:"
                f"{qq_settings.config.qq_webhook_listen_port}"
            )
        if webui_tls.redirect_enabled and redirect is None:
            if not _wait_worker_webui_ready(
                worker, qq_settings, scheme=webui_tls.scheme
            ):
                _terminate_worker(worker)
                raise RuntimeError("WebUI worker 未就绪，HTTP 重定向服务未启动")
            redirect = subprocess.Popen(
                _build_redirect_command(webui_tls),
                cwd=str(cwd),
                creationflags=_get_worker_creationflags(),
                env={**os.environ, "ZHENXUN_REDIRECT_CHILD": "1"},
            )
            redirect_signature = desired_redirect_signature
            time.sleep(0.25)
            if redirect.poll() is not None:
                code = redirect.returncode
                redirect = None
                _terminate_worker(worker)
                raise SystemExit(code or 1)
            _launcher_log(
                "WebUI HTTP redirect ready on "
                f"{webui_tls.host}:{webui_tls.redirect_port}"
            )
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
                    ingress = None
                    _launcher_log("QQ HTTPS ingress exited unexpectedly")
                    _terminate_worker(worker)
                    raise SystemExit(ingress_code or 1)
                if redirect is not None and redirect.poll() is not None:
                    redirect_code = redirect.returncode
                    redirect = None
                    _launcher_log("WebUI HTTP redirect exited unexpectedly")
                    _terminate_worker(worker)
                    raise SystemExit(redirect_code or 1)
                if stop_requested:
                    clear_launcher_restart_signal()
                    if ingress is not None:
                        _terminate_named_process(ingress, "QQ HTTPS ingress")
                        ingress = None
                    if redirect is not None:
                        _terminate_named_process(redirect, "WebUI HTTP redirect")
                        redirect = None
                    _terminate_worker(worker)
                    raise SystemExit(128 + int(stop_signal or signal.SIGINT))
                now = time.monotonic()
                if now >= next_restart_check:
                    next_restart_check = now + RESTART_POLL_INTERVAL
                    if action := consume_launcher_action():
                        restart_requested = True
                        restart_action = action
                        _launcher_log(
                            "detected restart request, stopping current worker"
                        )
                        _terminate_worker(worker)
                        return_code = worker.poll()
                        break
                time.sleep(WORKER_POLL_INTERVAL)
        except KeyboardInterrupt:
            clear_launcher_restart_signal()
            if ingress is not None:
                _terminate_named_process(ingress, "QQ HTTPS ingress")
                ingress = None
            if redirect is not None:
                _terminate_named_process(redirect, "WebUI HTTP redirect")
                redirect = None
            _terminate_worker(worker)
            return
        finally:
            if current_worker is worker:
                current_worker = None

        if restart_requested or (restart_action := consume_launcher_action()):
            if restart_action and restart_action[0] == "sync_dependencies_restart":
                dependency_paths = restart_action[1]
                if any(
                    path in {"pyproject.toml", "uv.lock"} for path in dependency_paths
                ):
                    from zhenxun.update_service import _sync_dependencies

                    _sync_dependencies()
                for dependency_path in dependency_paths:
                    if Path(dependency_path).name not in {
                        "requirement.txt",
                        "requirements.txt",
                    }:
                        continue
                    subprocess.run(
                        ["uv", "pip", "install", "-r", dependency_path],
                        cwd=str(cwd),
                        check=True,
                    )
            pending_bot_verification = apply_pending_update(cwd)
            continue
        if ingress is not None:
            _terminate_named_process(ingress, "QQ HTTPS ingress")
            ingress = None
        if redirect is not None:
            _terminate_named_process(redirect, "WebUI HTTP redirect")
            redirect = None
        raise SystemExit(return_code if return_code is not None else 1)


def main() -> None:
    args = sys.argv[1:]

    if not args or args[0] == "run":
        _run_launcher()
    elif args[0] == "run-worker":
        _run_worker()
    elif args[0] == "run-ingress":
        _run_ingress(args[1:])
    elif args[0] == "run-http-redirect":
        _run_http_redirect(args[1:])
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
