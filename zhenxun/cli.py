"""zx CLI — 绪山真寻 Bot 命令行工具

用法:
    zx run          启动 launcher
    zx run --webui-dev [--webui-source PATH]  在原入口使用前端开发热更新
    zx run-worker   启动 worker（由 launcher 调用）
    zx version      显示版本信息

进程环境变量:
    ZHENXUN_STARTUP_BANNER=0  禁用启动图案
    NO_COLOR                 禁用启动图案颜色
"""

from __future__ import annotations

import importlib.metadata
import os
from pathlib import Path
import sys


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


def main() -> None:
    args = sys.argv[1:]

    if not args or args[0] == "run":
        import argparse

        from zhenxun.services.launcher.lease import InstanceLease, LeaseError
        from zhenxun.services.launcher.main import run_launcher
        from zhenxun.services.startup_banner import show_startup_banner

        project = _ensure_project_root()
        parser = argparse.ArgumentParser(prog="zx run")
        parser.add_argument(
            "--webui-dev",
            action="store_true",
            help="Serve live WebUI source through the Bot address",
        )
        parser.add_argument(
            "--webui-source",
            type=Path,
            help="WebUI source directory (default: sibling zhenxun_bot_webui)",
        )
        options = parser.parse_args(args[1:] if args else [])
        if options.webui_source and not options.webui_dev:
            parser.error("--webui-source requires --webui-dev")
        webui_dev_source = (
            (options.webui_source or project.parent / "zhenxun_bot_webui").resolve()
            if options.webui_dev
            else None
        )
        show_startup_banner()
        try:
            with InstanceLease(project, role="launcher") as lease:
                previous = os.environ.get("ZHENXUN_INSTANCE_LEASE_ID")
                os.environ["ZHENXUN_INSTANCE_LEASE_ID"] = lease.identity
                try:
                    run_launcher(project, webui_dev_source)
                finally:
                    if previous is None:
                        os.environ.pop("ZHENXUN_INSTANCE_LEASE_ID", None)
                    else:
                        os.environ["ZHENXUN_INSTANCE_LEASE_ID"] = previous
        except LeaseError as error:
            sys.stderr.write(f"错误: {error}\n")
            sys.exit(1)
    elif args[0] == "run-worker":
        from zhenxun.services.launcher.lease import (
            InstanceLease,
            LeaseError,
            validate_delegation,
        )
        from zhenxun.worker import WorkerReexec, record_process_crash, run_worker

        project = _ensure_project_root()
        launcher = os.environ.get("ZHENXUN_LAUNCHER_PID")
        if launcher:
            identity = os.environ.get("ZHENXUN_INSTANCE_LEASE_ID")
            if not launcher.isdecimal() or not identity:
                raise LeaseError("launcher_identity_mismatch")
            validate_delegation(project, int(launcher), identity)
            try:
                run_worker(project)
            except Exception as error:
                record_process_crash(error, role="worker")
                raise
        else:
            from zhenxun.services.startup_banner import show_startup_banner

            show_startup_banner(role="worker")
            try:
                with InstanceLease(project, role="worker"):
                    try:
                        run_worker(project)
                    except Exception as error:
                        record_process_crash(error, role="worker")
                        raise
            except WorkerReexec:
                # Release the old process lease before the Windows exec handoff.
                # The replacement must acquire it anew before touching runtime.
                os.execv(
                    sys.executable, [sys.executable, "-m", "zhenxun.cli", "run-worker"]
                )
    elif args[0] == "run-ingress":
        _run_ingress(args[1:])
    elif args[0] == "run-http-sidecar":
        _run_http_sidecar(args[1:])
    elif args[0] == "run-maintenance" and len(args) == 2:
        from zhenxun.services.launcher.maintenance import execute

        execute(args[1])
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
