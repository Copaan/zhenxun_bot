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

    nonebot.run(
        workers=1,
        access_log=False,
        limit_concurrency=WORKER_CONNECTION_LIMIT,
        backlog=WORKER_BACKLOG,
        timeout_keep_alive=WORKER_KEEP_ALIVE_TIMEOUT,
        timeout_graceful_shutdown=GRACEFUL_SHUTDOWN_TIMEOUT,
    )


def _build_worker_command() -> list[str]:
    return [sys.executable, "-m", "zhenxun.cli", "run-worker"]


def _build_ingress_command(settings) -> list[str]:
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
        f"http://{upstream_host}:{settings.worker_port}",
    ]


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


def _worker_health_url(settings) -> str:
    connect_host = settings.worker_connect_host
    host = f"[{connect_host}]" if ":" in connect_host else connect_host
    return f"http://{host}:{settings.worker_port}/qq/healthz"


def _worker_webui_health_url(settings) -> str:
    connect_host = settings.worker_connect_host
    host = f"[{connect_host}]" if ":" in connect_host else connect_host
    return f"http://{host}:{settings.worker_port}/zhenxun/api/configure/status"


def _worker_is_ready(settings) -> bool:
    try:
        with urllib.request.urlopen(
            _worker_health_url(settings), timeout=1.0
        ) as response:
            return response.status == 200 and b'"status":"ready"' in response.read(256)
    except (OSError, urllib.error.URLError):
        return False


def _wait_worker_ready(worker: subprocess.Popen, settings) -> bool:
    deadline = time.monotonic() + WORKER_READY_TIMEOUT
    while time.monotonic() < deadline:
        if worker.poll() is not None:
            return False
        if _worker_is_ready(settings):
            return True
        time.sleep(WORKER_READY_POLL_INTERVAL)
    return False


def _wait_worker_webui_ready(worker: subprocess.Popen, settings) -> bool:
    deadline = time.monotonic() + WORKER_READY_TIMEOUT
    while time.monotonic() < deadline:
        if worker.poll() is not None:
            return False
        try:
            with urllib.request.urlopen(
                _worker_webui_health_url(settings), timeout=1.0
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
    from zhenxun.utils.restart_state import (
        clear_launcher_restart_signal,
        consume_launcher_restart_signal,
    )

    clear_launcher_restart_signal()
    current_worker: subprocess.Popen | None = None
    ingress: subprocess.Popen | None = None
    ingress_signature: tuple[str, int, str, str, str] | None = None
    stop_requested = False
    stop_signal: int | None = None

    def _cleanup_current_worker() -> None:
        if ingress is not None:
            _terminate_named_process(ingress, "QQ HTTPS ingress")
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
        qq_settings = load_qq_launcher_settings(cwd)
        builtin_ingress = bool(
            qq_settings.enabled
            and qq_settings.config.has_webhook_bots
            and qq_settings.config.qq_webhook_mode == "builtin_https"
        )
        desired_ingress_signature = (
            (
                qq_settings.config.qq_webhook_listen_host,
                qq_settings.config.qq_webhook_listen_port,
                qq_settings.config.qq_webhook_tls_certfile,
                qq_settings.config.qq_webhook_tls_keyfile,
                _worker_health_url(qq_settings),
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
        if pending_bot_verification:
            _launcher_log("waiting for updated worker health verification")
            if not _wait_worker_webui_ready(worker, qq_settings):
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
            if not _wait_worker_ready(worker, qq_settings):
                _terminate_worker(worker)
                raise RuntimeError("QQ worker 未在规定时间内就绪，HTTPS Ingress 未启动")
            ingress = subprocess.Popen(
                _build_ingress_command(qq_settings),
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
        restart_requested = False
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
                if stop_requested:
                    clear_launcher_restart_signal()
                    if ingress is not None:
                        _terminate_named_process(ingress, "QQ HTTPS ingress")
                        ingress = None
                    _terminate_worker(worker)
                    raise SystemExit(128 + int(stop_signal or signal.SIGINT))
                now = time.monotonic()
                if now >= next_restart_check:
                    next_restart_check = now + RESTART_POLL_INTERVAL
                    if consume_launcher_restart_signal():
                        restart_requested = True
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
            _terminate_worker(worker)
            return
        finally:
            if current_worker is worker:
                current_worker = None

        if restart_requested or consume_launcher_restart_signal():
            pending_bot_verification = apply_pending_update(cwd)
            continue
        if ingress is not None:
            _terminate_named_process(ingress, "QQ HTTPS ingress")
            ingress = None
        raise SystemExit(return_code if return_code is not None else 1)


def main() -> None:
    args = sys.argv[1:]

    if not args or args[0] == "run":
        _run_launcher()
    elif args[0] == "run-worker":
        _run_worker()
    elif args[0] == "run-ingress":
        _run_ingress(args[1:])
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
