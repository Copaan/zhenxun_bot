"""Bot worker 进程：初始化 NoneBot、加载插件并运行 HTTP 服务"""

from __future__ import annotations

import contextlib
import importlib
import json
import os
from pathlib import Path
import platform
import signal
import time

from zhenxun.configs.environment import sync_env_missing_items


class WorkerReexec(Exception):
    pass


def record_process_crash(error: BaseException, *, role: str) -> None:
    """Flush a last-chance process failure through the normal error sink."""
    try:
        from zhenxun.services.log import logger

        logger.error(
            f"进程崩溃 | role={role} | code=process_crash",
            "Process",
            e=error if isinstance(error, Exception) else Exception(str(error)),
        )
    except Exception:
        # Startup can fail before the logging module is available. Keep the
        # original exception and let the process supervisor handle it.
        pass


def _playwright_chromium_installed() -> bool:
    """Playwright 自带的 Chromium 无头浏览器是否已安装

    返回:
        bool: 当前 Playwright 版本对应的 chromium-headless-shell 已安装完成
    """
    try:
        import playwright

        manifest = (
            Path(playwright.__file__).parent / "driver" / "package" / "browsers.json"
        )
        revision = next(
            item["revision"]
            for item in json.loads(manifest.read_text("utf-8"))["browsers"]
            if item["name"] == "chromium-headless-shell"
        )
    except Exception:
        return False
    configured = os.environ.get("PLAYWRIGHT_BROWSERS_PATH")
    if configured == "0":
        root = manifest.parent / ".local-browsers"
    elif configured:
        root = Path(configured)
    elif platform.system() == "Windows":
        local = os.environ.get("LOCALAPPDATA")
        root = (Path(local) if local else Path.home() / "AppData/Local") / (
            "ms-playwright"
        )
    elif platform.system() == "Darwin":
        root = Path.home() / "Library/Caches/ms-playwright"
    else:
        cache = os.environ.get("XDG_CACHE_HOME")
        root = (Path(cache) if cache else Path.home() / ".cache") / "ms-playwright"
    marker = root / f"chromium_headless_shell-{revision}" / "INSTALLATION_COMPLETE"
    return marker.exists()


def run_worker(project_root: Path) -> None:
    """启动 Bot worker

    参数:
        project_root: 项目目录
    """
    worker_started = time.monotonic()
    from zhenxun.services.update.update_service import apply_pending_update

    if not os.environ.get("ZHENXUN_LAUNCHER_PID") and apply_pending_update(
        project_root
    ):
        raise WorkerReexec
    sync_env_missing_items(project_root)

    from zhenxun.services.nonebot_store.runtime import (
        activate_current_generation,
        recovery_required,
    )

    store_recovery = recovery_required()
    if not store_recovery:
        activate_current_generation()

    import nonebot

    htmlrender_browser_channel = None
    system = None if _playwright_chromium_installed() else platform.system()

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

    from zhenxun.services import proxy_clients

    proxy_clients.install()
    from zhenxun.configs.environment import environment_file, migrate_orm_data_dir

    nonebot.init(
        _env_file=environment_file(),
        htmlrender_browser_channel=htmlrender_browser_channel,
        render_backend="playwright",
        render_playwright={"channel": htmlrender_browser_channel},
    )
    migrate_orm_data_dir(project_root)

    from zhenxun.adapters.onebot_managed import ManagedOneBotAdapter as OneBotV11Adapter
    from zhenxun.configs.config import BotConfig

    driver = nonebot.get_driver()
    driver.register_adapter(OneBotV11Adapter)
    enabled_adapters = ["OneBot V11"]
    if BotConfig.qq_adapter_load:
        from zhenxun.adapters.qq_official.config import validate_qq_official_config

        qq_config = validate_qq_official_config()
        if (
            qq_config.has_webhook_bots
            and qq_config.qq_webhook_mode == "builtin_https"
            and not os.environ.get("ZHENXUN_LAUNCHER_PID")
        ):
            raise RuntimeError("QQ_WEBHOOK_MODE=builtin_https 必须通过 `zx run` 启动")
        from zhenxun.adapters.qq_official.adapter import ZhenxunQQAdapter

        driver.register_adapter(ZhenxunQQAdapter)
        enabled_adapters.append("<c>QQ_Official</c>")
    if BotConfig.telegram_adapter_load:
        try:
            from nonebot.adapters.telegram import Adapter as TelegramAdapter
        except ModuleNotFoundError as e:
            raise RuntimeError(
                "TELEGRAM_ADAPTER_LOAD=True 需要先安装可选依赖: "
                "uv sync --locked --inexact --extra telegram"
            ) from e
        driver.register_adapter(TelegramAdapter)
        enabled_adapters.append("Telegram")

    from zhenxun.services.runtime_bootstrap import register_runtime_bootstrap

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
    from nonebot_plugin_uninfo.adapters import alter_get_fetcher

    for adapter in nonebot.get_adapters().values():
        # An embedding application may have imported Uninfo before its adapters.
        from nonebot_plugin_uninfo.adapters import INFO_FETCHER_MAPPING

        name = adapter.get_name()
        if name not in INFO_FETCHER_MAPPING:
            alter_get_fetcher(name)
    register_runtime_bootstrap(nonebot.get_driver())

    from zhenxun.services.startup import startup_coordinator

    startup_coordinator.record_operation(
        "worker:bootstrap_imports",
        "worker",
        "completed",
        (time.monotonic() - worker_started) * 1000,
    )

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
    if not store_recovery:
        startup_load_planner.prepare_library_plugins()
    phase_started = time.monotonic()
    startup_load_planner.load_critical(management_only=store_recovery)
    startup_coordinator.record_operation(
        "worker:load_critical_plugins",
        "worker",
        "completed",
        (time.monotonic() - phase_started) * 1000,
    )

    from zhenxun.services.nonebot_store.runtime import load_managed_plugins

    phase_started = time.monotonic()
    managed_status = (
        {"failed": [], "loaded": []} if store_recovery else load_managed_plugins()
    )
    startup_coordinator.record_operation(
        "worker:load_managed_plugins",
        "worker",
        "completed",
        (time.monotonic() - phase_started) * 1000,
    )
    if managed_status["failed"]:
        from zhenxun.services.nonebot_store.storage import load_manifest

        nonebot.logger.error(
            "部分 WebUI 托管的 NoneBot 插件加载失败，已跳过加载{}: {}",
            "，等待启动验证决定保留或回滚"
            if load_manifest().get("pending_verification")
            else "",
            ", ".join(item["store_key"] for item in managed_status["failed"]),
        )

    from zhenxun.configs.webui_tls import (
        bind_runtime_webui_settings,
        load_webui_tls_settings,
        validate_webui_tls_settings,
    )

    webui_tls = load_webui_tls_settings(project_root)
    validate_webui_tls_settings(webui_tls)
    from dataclasses import replace

    webui_tls = replace(
        webui_tls, host=str(driver.config.host), port=int(driver.config.port)
    )
    bind_runtime_webui_settings(webui_tls)
    tls_options = (
        {
            "ssl_certfile": webui_tls.certfile,
            "ssl_keyfile": webui_tls.keyfile,
        }
        if webui_tls.enabled
        else {}
    )
    from zhenxun.services.webui_transport import (
        GRACEFUL_SHUTDOWN_TIMEOUT,
        SERVER_OPTIONS,
        install_uvicorn_shutdown_hook,
        install_websocket_drain_lock,
    )

    install_websocket_drain_lock()
    install_uvicorn_shutdown_hook()
    from zhenxun.services.lifecycle import lifecycle_kernel

    def after_server_signal(signum, frame):
        # Uvicorn replays captured signals after lifespan shutdown. SIG_DFL on
        # Windows SIGBREAK exits in the C runtime, bypassing Python finally.
        if not lifecycle_kernel.shutdown_requested:
            signal.default_int_handler(signum, frame)

    worker_signals = [signal.SIGINT, signal.SIGTERM]
    if hasattr(signal, "SIGBREAK"):
        worker_signals.append(signal.SIGBREAK)
    original_signals = {
        sig: signal.signal(sig, after_server_signal) for sig in worker_signals
    }
    try:
        from zhenxun.services.runtime_bootstrap import install_runtime_worker_teardown

        install_runtime_worker_teardown(nonebot.get_app())
        nonebot.run(
            workers=1,
            timeout_graceful_shutdown=GRACEFUL_SHUTDOWN_TIMEOUT,
            **SERVER_OPTIONS,
            **tls_options,
        )
    finally:
        proxy_clients.restore()
        from zhenxun.services.runtime_bootstrap import finalize_runtime_executor

        finalize_runtime_executor()
        for sig, handler in original_signals.items():
            signal.signal(sig, handler)
