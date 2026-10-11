from __future__ import annotations

import asyncio
from functools import wraps
import inspect
from pathlib import Path
from weakref import WeakSet

from .errors import MigrationError


class ValidationGate:
    """Process-local business admission; initialization is a separate state."""

    def __init__(self):
        self.task_id: str | None = None
        self.startup_id: str | None = None
        self.opened = False
        self.stopping = False
        self._promotion = asyncio.Lock()
        self._ready = []
        self._started: set[int] = set()
        self._patches = []
        self._schedulers = WeakSet()

    @property
    def business_allowed(self) -> bool:
        return self.task_id is None or (self.opened and not self.stopping)

    def configure(self, task_id: str, startup_id: str) -> None:
        if self.task_id is not None:
            raise MigrationError("migration_validation_already_configured")
        self.task_id, self.startup_id = task_id, startup_id

    def install(self, driver) -> None:
        from apscheduler.schedulers.asyncio import AsyncIOScheduler
        from nonebot.adapters import Adapter

        if self.task_id is None:
            raise MigrationError("migration_validation_authorization_required")
        if self._patches:
            raise MigrationError("migration_validation_already_installed")
        lifespan = getattr(driver, "_lifespan", None)
        if not (
            isinstance(getattr(lifespan, "_ready_funcs", None), list)
            and callable(getattr(Adapter, "on_ready", None))
            and callable(getattr(AsyncIOScheduler, "start", None))
            and callable(getattr(AsyncIOScheduler, "resume", None))
        ):
            raise MigrationError("migration_validation_interfaces_unavailable")
        original_ready = Adapter.on_ready
        original_start = AsyncIOScheduler.start
        original_resume = AsyncIOScheduler.resume
        gate = self

        @wraps(original_ready)
        def on_ready(adapter, function):
            @wraps(function)
            async def deferred():
                if not gate.business_allowed:
                    if function not in gate._ready:
                        gate._ready.append(function)
                    return
                result = function()
                if inspect.isawaitable(result):
                    await result

            original_ready(adapter, deferred)
            return function

        @wraps(original_start)
        def start(scheduler, paused=False):
            if not gate.business_allowed:
                if not paused:
                    gate._schedulers.add(scheduler)
                paused = True
            return original_start(scheduler, paused=paused)

        @wraps(original_resume)
        def resume(scheduler):
            if gate.business_allowed:
                return original_resume(scheduler)

        try:
            for owner, name, replacement in (
                (Adapter, "on_ready", on_ready),
                (AsyncIOScheduler, "start", start),
                (AsyncIOScheduler, "resume", resume),
            ):
                original = getattr(owner, name)
                setattr(owner, name, replacement)
                self._patches.append((owner, name, original, replacement))
        except BaseException:
            self.uninstall()
            raise

    async def promote(self, decision: dict) -> None:
        async with self._promotion:
            if (
                self.task_id is None
                or self.stopping
                or decision.get("job_id") != self.task_id
                or decision.get("validation", {}).get("startup_id") != self.startup_id
                or decision.get("mode") != "online"
                or decision.get("decision") != "commit"
            ):
                raise MigrationError("migration_promotion_authorization_invalid")
            # The launcher must persist its irreversible decision before invoking us.
            self.opened = True
            for index, function in enumerate(self._ready):
                if index in self._started:
                    continue
                # Do not repeat a callback whose external outcome is uncertain.
                self._started.add(index)
                try:
                    result = function()
                    if inspect.isawaitable(result):
                        await result
                except BaseException:
                    self.stopping = True
                    raise
            for scheduler in self._schedulers:
                if scheduler.running:
                    scheduler.resume()
            self._schedulers.clear()

    def stop(self) -> None:
        self.stopping = True

    def uninstall(self) -> None:
        for owner, name, original, replacement in reversed(self._patches):
            if getattr(owner, name) is replacement:
                setattr(owner, name, original)
        self._patches.clear()


validation_gate = ValidationGate()


class ValidationIngress:
    """Serve original management authentication while business is closed."""

    def __init__(self, app, *, management, gate=validation_gate):
        self.app, self.management, self.gate = app, management, gate

    async def __call__(self, scope, receive, send):
        if self.gate.business_allowed or scope["type"] == "lifespan":
            return await self.app(scope, receive, send)
        if scope["type"] == "websocket":
            await send({"type": "websocket.close", "code": 1013})
            return
        await self.management(scope, receive, send)


def authorized_validation(project: Path, identity: str) -> dict:
    """Environment selects an IPC request, never grants validation authority."""
    import os

    from .control import request_control
    from .lease import validate_delegation
    from .tasks import TaskStore

    validate_delegation(
        project,
        int(os.environ.get("ZHENXUN_LAUNCHER_PID", "0")),
        identity=os.environ.get("ZHENXUN_INSTANCE_LEASE_ID"),
    )
    request = request_control(
        project,
        "validation_input",
        {"task_id": identity, "startup_id": os.getenv("ZHENXUN_WORKER_STARTUP_ID")},
    )
    job = TaskStore(project).active()
    if (
        not job
        or job["id"] != identity
        or job["action"] != "restore"
        or job["stage"] not in {"verifying", "committed"}
        or request.get("job_id") != identity
        or request.get("startup_id") != os.getenv("ZHENXUN_WORKER_STARTUP_ID")
        or request.get("launcher_boot_id") != os.getenv("ZHENXUN_LAUNCHER_BOOT_ID")
    ):
        raise MigrationError("migration_validation_authorization_invalid")
    if job["stage"] == "committed":
        from .committed_recovery import revalidation_authorized

        revalidation_authorized(
            TaskStore(project),
            identity,
            {**request, "lease_id": os.getenv("ZHENXUN_INSTANCE_LEASE_ID")},
        )
    return request
