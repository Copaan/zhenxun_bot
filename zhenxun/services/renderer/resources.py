from __future__ import annotations

import asyncio
from collections import deque
from dataclasses import dataclass
import time
from typing import Any

from zhenxun.services.lifecycle.deadline import remaining_timeout, shutdown_budget
from zhenxun.services.log import logger


@dataclass
class ContextResource:
    context: Any
    browser: Any
    owner: str
    generation: int | None
    cleanup: asyncio.Task | None = None
    failed: bool = False


class BrowserResources:
    """Own browser contexts and cleanup work until their release is confirmed."""

    def __init__(self) -> None:
        self.contexts: dict[Any, ContextResource] = {}
        self.tasks: set[asyncio.Task] = set()
        self.failures: deque[dict[str, Any]] = deque(maxlen=128)

    def spawn(self, coroutine, *, name: str) -> asyncio.Task:
        task = asyncio.create_task(coroutine, name=name)
        self.tasks.add(task)

        def finished(done: asyncio.Task) -> None:
            self.tasks.discard(done)
            if not done.cancelled():
                done.exception()

        task.add_done_callback(finished)
        return task

    def record_failure(self, resource: Any, phase: str, error: BaseException) -> None:
        record = self.contexts.get(resource)
        if record is not None:
            record.failed = True
        self.failures.append(
            {
                "resource_id": id(resource),
                "generation": record.generation if record else None,
                "owner": record.owner if record else "renderer",
                "phase": phase,
                "error_type": type(error).__name__,
                "at": time.time(),
                "released_at": None,
            }
        )
        logger.warning(
            f"browser resource cleanup: phase={phase} error={type(error).__name__}",
            "RendererResources",
        )

    async def create_context(
        self, browser: Any, *, owner: str, generation: int | None = None, **options
    ) -> Any:
        async def create():
            context = await browser.new_context(**options)
            self.contexts[context] = ContextResource(
                context, browser, owner, generation
            )
            return context

        task = self.spawn(create(), name="renderer-context-create")
        try:
            return await asyncio.shield(task)
        except asyncio.CancelledError:

            async def release_late_context():
                context = await asyncio.shield(task)
                await self.close_context(context)

            self.spawn(release_late_context(), name="renderer-context-abandoned")
            raise

    def confirmed(self, context: Any) -> None:
        self.contexts.pop(context, None)
        for failure in self.failures:
            if failure["resource_id"] == id(context) and not failure["released_at"]:
                failure["released_at"] = time.time()

    async def close_context(self, context: Any) -> None:
        record = self.contexts.get(context)
        if record is None:
            return
        if remaining_timeout(10.0) <= 0:
            error = TimeoutError("renderer_cleanup_budget_exhausted")
            self.record_failure(context, "context_close_timeout", error)
            raise error
        task = record.cleanup
        if task is None or (task.done() and record.failed):

            async def close():
                try:
                    await context.close()
                except BaseException as error:
                    self.record_failure(context, "context_close", error)
                    raise
                else:
                    self.confirmed(context)

            task = record.cleanup = self.spawn(close(), name="renderer-context-close")
        try:
            await asyncio.wait_for(asyncio.shield(task), remaining_timeout(10.0))
        except asyncio.TimeoutError as error:
            self.record_failure(context, "context_close_timeout", error)
            raise

    async def finish_page(self, page: Any, context: Any, *, retain: bool) -> bool:
        """Return whether a pooled context can be reused after closing its page."""
        reusable = retain
        cancelled = None
        with shutdown_budget(10.0):
            if page is not None:
                try:
                    await asyncio.wait_for(page.close(), remaining_timeout(10.0))
                except BaseException as error:
                    self.record_failure(context, "page_close", error)
                    reusable = False
                    if not isinstance(error, Exception):
                        cancelled = error
            if not reusable:
                try:
                    await self.close_context(context)
                finally:
                    if cancelled is not None:
                        raise cancelled
        return reusable

    async def cleanup(self, coroutine, *, name: str) -> Any:
        task = self.spawn(coroutine, name=name)
        return await asyncio.wait_for(asyncio.shield(task), remaining_timeout(10.0))

    async def drain(self) -> None:
        pending = [task for task in self.tasks if task is not asyncio.current_task()]
        if pending:
            await asyncio.wait_for(
                asyncio.shield(asyncio.gather(*pending, return_exceptions=True)),
                remaining_timeout(10.0),
            )

    async def close_all(self) -> None:
        with shutdown_budget(10.0):
            results = await asyncio.gather(
                *(self.close_context(context) for context in tuple(self.contexts)),
                return_exceptions=True,
            )
        if any(isinstance(result, BaseException) for result in results):
            raise RuntimeError("renderer_context_cleanup_failed")

    async def retry_failed(self) -> None:
        with shutdown_budget(10.0):
            results = await asyncio.gather(
                *(
                    self.close_context(context)
                    for context, record in tuple(self.contexts.items())
                    if record.failed
                ),
                return_exceptions=True,
            )
        if any(isinstance(result, BaseException) for result in results):
            raise RuntimeError("renderer_context_cleanup_failed")

    def confirm_browser_closed(self, browser: Any) -> None:
        for context, record in tuple(self.contexts.items()):
            if record.browser is browser:
                self.confirmed(context)

    def snapshot(self) -> dict[str, Any]:
        return {
            "owned_context_count": len(self.contexts),
            "pending_context_count": sum(r.failed for r in self.contexts.values()),
            "cleanup_task_count": sum(not task.done() for task in self.tasks),
            "cleanup_failures": list(self.failures),
        }


browser_resources = BrowserResources()
