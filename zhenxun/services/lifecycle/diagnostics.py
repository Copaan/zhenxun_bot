from __future__ import annotations

import asyncio
from collections.abc import Callable
from concurrent.futures import Future, ThreadPoolExecutor
from contextvars import Context
from datetime import datetime, timezone
import logging
from pathlib import Path
import threading
import time
from typing import Any

from zhenxun.utils.atomic_json import write_json_locked

from .deadline import remaining_timeout

_logger = logging.getLogger(__name__)


def value_snapshot(value: Any) -> Any:
    """Detach JSON values without copying runtime objects or invoking their hooks."""
    if value is None or isinstance(value, str | bool | int | float):
        return value
    if isinstance(value, dict):
        if not all(isinstance(key, str) for key in value):
            raise TypeError("diagnostic_snapshot_non_string_key")
        return {key: value_snapshot(item) for key, item in value.items()}
    if isinstance(value, list | tuple):
        return [value_snapshot(item) for item in value]
    raise TypeError("diagnostic_snapshot_non_value")


class DiagnosticWorker:
    """A bounded, explicitly owned worker; cancellation never implies thread exit."""

    def __init__(self, name: str) -> None:
        self.threads: set[threading.Thread] = set()
        self.pending: Future[Any] | None = None
        self.closed = False
        self._executor = ThreadPoolExecutor(
            max_workers=1,
            thread_name_prefix=name,
            initializer=lambda: self.threads.add(threading.current_thread()),
        )

    def submit(self, callback: Callable[..., Any], *args: Any) -> asyncio.Future[Any]:
        if self.closed or (self.pending is not None and not self.pending.done()):
            raise RuntimeError("diagnostic_worker_unavailable")
        self.pending = Context().run(self._executor.submit, callback, *args)
        future = asyncio.wrap_future(self.pending)
        future.add_done_callback(
            lambda done: None if done.cancelled() else done.exception()
        )
        return future

    async def close(self, timeout: float) -> bool:
        self.closed = True
        self._executor.shutdown(wait=False, cancel_futures=True)
        deadline = time.monotonic() + remaining_timeout(timeout)
        while not self.released:
            if time.monotonic() >= deadline:
                return False
            await asyncio.sleep(min(0.01, max(0, deadline - time.monotonic())))
        return True

    @property
    def released(self) -> bool:
        return (
            self.closed
            and (self.pending is None or self.pending.done())
            and not any(thread.is_alive() for thread in self.threads)
        )


class LifecycleStateWriter:
    def __init__(self, path: Path, snapshot: Callable[[], dict[str, Any]]) -> None:
        self.path = path
        self.snapshot = snapshot
        self.worker = DiagnosticWorker("zhenxun-lifecycle-state")
        self._loop = asyncio.get_running_loop()
        self.task: asyncio.Task[None] | None = None
        self._wake = asyncio.Event()
        self._revision = 0
        self._written_revision = 0
        self._closing = False
        self._closed = False
        self._next_write = 0.0
        self._last_warning = float("-inf")
        self.last_success_at: str | None = None
        self.error_code: str | None = None
        self.duration_ms: float | None = None
        self.shutdown_timed_out = False
        self._deadline: float | None = None

    def mark_dirty(self) -> None:
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = None
        if loop is not self._loop:
            if not self._loop.is_closed():
                self._loop.call_soon_threadsafe(self.mark_dirty, context=Context())
            return
        self._revision += 1
        if self._closed:
            return
        self._wake.set()
        if self.task is None:
            # The Kernel owns this task, never the plugin triggering a state update.
            self.task = Context().run(
                asyncio.create_task, self._run(), name="lifecycle-state-writer"
            )

    def status(self) -> dict[str, Any]:
        return {
            "pending": self._revision != self._written_revision,
            "last_success_at": self.last_success_at,
            "error_code": self.error_code,
            "write_duration_ms": self.duration_ms,
            "shutdown_timed_out": self.shutdown_timed_out,
            "worker_released": self.worker.released,
            "writer_task_active": self.task is not None and not self.task.done(),
        }

    def _failed(self, code: str) -> None:
        self.error_code = code
        now = time.monotonic()
        if now - self._last_warning >= 30:
            self._last_warning = now
            _logger.warning("lifecycle_persistence:%s", code)

    async def _run(self) -> None:
        while True:
            await self._wake.wait()
            self._wake.clear()
            if self._revision == self._written_revision:
                if self._closing:
                    return
                continue
            delay = self._next_write - time.monotonic()
            if delay > 0 and not self._closing:
                try:
                    await asyncio.wait_for(self._wake.wait(), timeout=delay)
                except asyncio.TimeoutError:
                    pass
                if not self._closing and time.monotonic() < self._next_write:
                    self._wake.set()
                    continue
            self._wake.clear()
            revision = self._revision
            started = time.monotonic()
            if self._deadline is not None and started >= self._deadline:
                self.shutdown_timed_out = True
                self._failed("state_flush_timeout")
                return
            try:
                snapshot = value_snapshot(self.snapshot())
                await asyncio.shield(
                    self.worker.submit(write_json_locked, self.path, snapshot)
                )
            except Exception:
                self._failed("state_write_failed")
            else:
                self._written_revision = revision
                self.last_success_at = datetime.now(timezone.utc).isoformat()
                self.error_code = None
            self.duration_ms = round((time.monotonic() - started) * 1000, 2)
            self._next_write = time.monotonic() + 1.0
            if self._closing:
                if revision == self._revision:
                    return
            if self._revision != self._written_revision:
                self._wake.set()

    async def close(self, timeout: float) -> bool:
        if self._closed:
            return self.worker.released and self._revision == self._written_revision
        deadline = time.monotonic() + remaining_timeout(timeout)
        self._deadline = (
            min(deadline, self._deadline) if self._deadline is not None else deadline
        )
        deadline = self._deadline
        self._closing = True
        if not self._closed:
            self.mark_dirty()
        try:
            if self.task is not None:
                done, _ = await asyncio.wait(
                    {self.task}, timeout=max(0, deadline - time.monotonic())
                )
                if done:
                    self.task.result()
                else:
                    self.shutdown_timed_out = True
                    self._failed("state_flush_timeout")
        except asyncio.CancelledError:
            self._failed("state_flush_cancelled")
            raise
        finally:
            self._closed = True
            if self.task is not None and not self.task.done():
                self.task.cancel()
                self.task.add_done_callback(
                    lambda done: None if done.cancelled() else done.exception()
                )
            released = await self.worker.close(max(0, deadline - time.monotonic()))
            if not released:
                self.shutdown_timed_out = True
                self._failed("state_worker_shutdown_timeout")
        return released and self._revision == self._written_revision
