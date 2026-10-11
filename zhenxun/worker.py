"""Bot worker 进程：初始化 NoneBot、加载插件并运行 HTTP 服务"""

from __future__ import annotations

import contextlib
import importlib
import os
from pathlib import Path
import platform
import signal
import time

from zhenxun.configs.environment import sync_env_missing_items

GRACEFUL_SHUTDOWN_TIMEOUT = 15
WORKER_CONNECTION_LIMIT = 512
WORKER_BACKLOG = 2048
WORKER_KEEP_ALIVE_TIMEOUT = 15


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

    from zhenxun.services.migration.validation import validation_gate
    from zhenxun.services.nonebot_store.runtime import (
        activate_current_generation,
        recovery_required,
    )

    store_recovery = recovery_required()

    if validation_gate.task_id:
        from zhenxun.services.migration.generation import activate_candidate

        activate_candidate(project_root, validation_gate.task_id)
    elif not store_recovery:
        activate_current_generation()

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

    from zhenxun.services import proxy_clients

    proxy_clients.install()
    from zhenxun.configs.environment import environment_file

    nonebot.init(
        _env_file=environment_file(),
        htmlrender_browser_channel=htmlrender_browser_channel,
        render_backend="playwright",
        render_playwright={"channel": htmlrender_browser_channel},
    )

    from zhenxun.adapters.onebot_managed import ManagedOneBotAdapter as OneBotV11Adapter
    from zhenxun.configs.config import BotConfig

    driver = nonebot.get_driver()
    from zhenxun.services.migration.validation_worker import (
        install as install_validation,
    )

    install_validation(driver)
    driver.register_adapter(OneBotV11Adapter)
    from zhenxun.services.onebot_endpoint import OneBotHandshakeDiagnostics

    nonebot.get_app().add_middleware(OneBotHandshakeDiagnostics)
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

    from zhenxun.services.message_execution import install_matcher_execution
    from zhenxun.services.runtime_bootstrap import register_runtime_bootstrap

    install_matcher_execution()

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
        nonebot.logger.error(
            "部分 WebUI 托管的 NoneBot 插件加载失败，已隔离: {}",
            ", ".join(item["store_key"] for item in managed_status["failed"]),
        )

    from zhenxun.services.migration.validation_worker import (
        register as register_validation,
    )

    register_validation(driver, managed_status=managed_status)

    from zhenxun.configs.webui_tls import (
        bind_runtime_webui_settings,
        load_webui_tls_settings,
        validate_webui_tls_settings,
    )

    webui_tls = load_webui_tls_settings(project_root)
    validate_webui_tls_settings(
        webui_tls,
        launcher_managed=bool(os.environ.get("ZHENXUN_LAUNCHER_PID")),
    )
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
    from zhenxun.services.lifecycle import lifecycle_kernel

    exit_signals: list[int] = []

    def after_server_signal(signum, frame):
        # Uvicorn replays captured signals after lifespan shutdown. SIG_DFL on
        # Windows SIGBREAK exits in the C runtime, bypassing Python finally.
        exit_signals.append(signum)
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
        proxy_clients.restore()
        from zhenxun.services.runtime_bootstrap import finalize_runtime_executor

        finalize_runtime_executor()
        try:
            lifecycle_kernel.finalize_terminal_receipt(exit_signals=exit_signals)
        finally:
            for sig, handler in original_signals.items():
                signal.signal(sig, handler)
