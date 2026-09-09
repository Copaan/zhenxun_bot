from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
import os
from pathlib import Path
import signal
import sys
import time
import uuid

from .control import request_control
from .errors import MigrationError
from .lease import validate_delegation
from .maintenance_app import ManagementSnapshot, create_maintenance_app
from .tasks import TaskStore


def main():
    import uvicorn

    from zhenxun.configs.webui_tls import WebUITLSSettings
    from zhenxun.services.lifecycle.kernel import LifecycleKernel
    from zhenxun.services.lifecycle.models import ComponentSpec

    root, startup = Path.cwd(), sys.argv[1]
    validate_delegation(
        root,
        int(os.environ["ZHENXUN_LAUNCHER_PID"]),
        identity=os.environ["ZHENXUN_INSTANCE_LEASE_ID"],
    )
    # The child can reach IPC before its parent has registered its ProcessHandle.
    deadline = time.monotonic() + 10
    while True:
        try:
            request = request_control(
                root, "maintenance_input", {"startup_id": startup}
            )
            break
        except MigrationError as error:
            if (
                error.code != "migration_management_identity_unconfirmed"
                or time.monotonic() >= deadline
            ):
                raise
            time.sleep(0.02)
    settings = WebUITLSSettings(**request["network"])
    snapshot = ManagementSnapshot.parse(request.pop("management"))
    identity = {
        "boot_id": uuid.uuid4().hex,
        "startup_id": startup,
        "launcher_boot_id": request["launcher_boot_id"],
        "pid": os.getpid(),
        "task_id": request["job_id"],
    }
    state_path = (
        TaskStore(root).path("jobs", request["job_id"]).parent
        / f"management-{startup}.json"
    )
    kernel = LifecycleKernel(state_path)
    kernel.set_process_metadata(**identity, shutdown_id=startup)
    kernel.register(
        ComponentSpec("migration:management", stage="management"), lambda: None
    )
    server = None

    async def watchdog():
        until = time.monotonic() + request["remaining_seconds"]
        while request.get("keep_available") is True or time.monotonic() < until:
            await asyncio.sleep(1)
            try:
                validate_delegation(
                    root,
                    int(os.environ["ZHENXUN_LAUNCHER_PID"]),
                    identity=os.environ["ZHENXUN_INSTANCE_LEASE_ID"],
                )
            except MigrationError:
                break
        server.should_exit = True

    @asynccontextmanager
    async def lifespan(app):
        await kernel.start_components({"migration:management"})
        guard = asyncio.create_task(watchdog(), name="migration-management-watchdog")
        try:
            yield
        finally:
            guard.cancel()
            await asyncio.gather(guard, return_exceptions=True)
            await kernel.stop_all(timeout=15)

    app = create_maintenance_app(root, snapshot, identity, lifespan=lifespan)
    server = uvicorn.Server(
        uvicorn.Config(
            app,
            host=settings.host,
            port=settings.port,
            access_log=False,
            proxy_headers=False,
            timeout_graceful_shutdown=15,
            ssl_certfile=settings.certfile if settings.enabled else None,
            ssl_keyfile=settings.keyfile if settings.enabled else None,
        )
    )
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: None)
    if hasattr(signal, "SIGBREAK"):
        signal.signal(signal.SIGBREAK, lambda *_: None)
    try:
        server.run()
    finally:
        kernel.finalize_terminal_receipt()


if __name__ == "__main__":
    main()
