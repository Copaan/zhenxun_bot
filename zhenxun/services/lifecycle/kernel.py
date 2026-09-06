from __future__ import annotations

import asyncio
from collections import defaultdict
from collections.abc import Callable, Coroutine
import contextlib
from contextlib import AbstractAsyncContextManager, AsyncExitStack, asynccontextmanager
from datetime import datetime, timezone
import inspect
import os
from pathlib import Path
from threading import RLock
import time
from typing import Any
import uuid

from zhenxun.utils.atomic_json import write_json_locked

from .deadline import check_budget, remaining_timeout, shutdown_budget
from .diagnostics import LifecycleStateWriter
from .models import (
    SCOPE_DEPTH,
    ComponentRuntime,
    ComponentSpec,
    ComponentState,
    CompositeHandle,
    LifecycleOperationResult,
    LifecycleStage,
    ResourceReceipt,
    RuntimeHandle,
    ScopeRecord,
    ScopeState,
)

StartCallback = Callable[..., Any]
StopCallback = Callable[..., Any]
HealthCallback = Callable[..., Any]
LifecycleObserver = Callable[[dict[str, Any]], Any]

_DEFAULT_STATE_PATH = Path("data/runtime/lifecycle-state-v2.json")


class LifecycleError(RuntimeError):
    pass


_WORKER_RECOVERY_ERRORS = {
    "component_drain_timeout",
    "component_finalizer_timeout",
    "component_task_cancel_timeout",
}


def _error_code(error: BaseException) -> str:
    if isinstance(error, LifecycleError) and str(error):
        return str(error)
    return type(error).__name__


class LifecycleContext:
    def __init__(
        self,
        kernel: LifecycleKernel,
        spec: ComponentSpec,
        *,
        scope_record: ScopeRecord | None = None,
    ) -> None:
        self.kernel = kernel
        self.spec = spec
        self.scope_record = scope_record
        self._stack = AsyncExitStack()
        self._finalizers: list[
            tuple[Callable[..., Any], tuple[Any, ...], dict[str, Any]]
        ] = []
        self._tasks: set[asyncio.Task[Any]] = set()
        self._task_receipts: dict[asyncio.Task[Any], ResourceReceipt] = {}
        self._task_cancel_callbacks: dict[asyncio.Task[Any], Callable[[], None]] = {}
        self._in_flight = 0
        self._drained = asyncio.Event()
        self._drained.set()
        self.accepting = True
        self.closed = False
        self.value: Any = None
        self.resources: list[ResourceReceipt] = []
        self._release_checks: dict[str, Callable[[], bool]] = {}
        self._children: list[LifecycleContext] = []
        self._parent: LifecycleContext | None = None
        self._parent_receipt: ResourceReceipt | None = None
        self._close_task: asyncio.Task[None] | None = None

    def _check_accepting(self) -> None:
        if not self.accepting or self.closed:
            raise LifecycleError("component_scope_revoked")

    @property
    def scope_id(self) -> str:
        return (
            self.scope_record.scope_id
            if self.scope_record is not None
            else f"component:{self.spec.component_id}"
        )

    @property
    def scope_type(self) -> str:
        return self.scope_record.scope if self.scope_record else self.spec.scope

    async def enter_async_context(
        self, manager: AbstractAsyncContextManager[Any]
    ) -> Any:
        self._check_accepting()
        return await self._stack.enter_async_context(manager)

    def add_finalizer(
        self, callback: Callable[..., Any], *args: Any, **kwargs: Any
    ) -> None:
        self._check_accepting()
        self._finalizers.append((callback, args, kwargs))

    def spawn_task(
        self,
        coroutine: Coroutine[Any, Any, Any],
        *,
        name: str | None = None,
        persistent: bool = True,
        cancel: Callable[[], None] | None = None,
    ) -> asyncio.Task[Any]:
        if not self.accepting:
            coroutine.close()
            raise LifecycleError("component_scope_revoked")
        task = asyncio.create_task(coroutine, name=name)
        receipt = ResourceReceipt(
            receipt_id=f"task:{id(task)}",
            provider="asyncio",
            resource_type="task",
            owner_id=self.scope_id,
            detail={"name": task.get_name(), "persistent": persistent},
        )
        self._tasks.add(task)
        self._task_receipts[task] = receipt
        if cancel is not None:
            self._task_cancel_callbacks[task] = cancel
        self.resources.append(receipt)
        task.add_done_callback(
            lambda completed: self.kernel._task_completed(
                self, completed, persistent=persistent
            )
        )
        return task

    def own_resource(
        self,
        *,
        receipt_id: str,
        provider: str,
        resource_type: str,
        ownership: str = "exclusive",
        reversible: bool = True,
        detail: dict[str, Any] | None = None,
        release_check: Callable[[], bool] | None = None,
    ) -> ResourceReceipt:
        self._check_accepting()
        receipt = ResourceReceipt(
            receipt_id=receipt_id,
            provider=provider,
            resource_type=resource_type,
            owner_id=self.scope_id,
            ownership=ownership,
            reversible=reversible,
            detail=dict(detail or {}),
        )
        self.resources.append(receipt)
        if release_check is not None:
            self._release_checks[receipt_id] = release_check
        return receipt

    def provide(self, capability: str, value: Any) -> None:
        self._check_accepting()
        self.kernel.provide(self.spec.component_id, capability, value)

    def create_child_scope(self, scope: str, scope_id: str) -> LifecycleContext:
        child = self.kernel._create_scope_context(self, scope, scope_id)
        self._children.append(child)
        child._parent = self
        child._parent_receipt = self.own_resource(
            receipt_id=f"scope:{scope_id}",
            provider="lifecycle",
            resource_type=scope,
        )
        return child

    @asynccontextmanager
    async def child_scope(self, scope: str, scope_id: str):
        child = self.create_child_scope(scope, scope_id)
        try:
            yield child
        except BaseException as error:
            self.kernel._mark_scope_failed(child, error)
            raise
        finally:
            await child.close()

    def spawn_detached(
        self,
        coroutine: Coroutine[Any, Any, Any],
        *,
        scope_id: str,
        scope: str = "task",
        name: str | None = None,
        persistent: bool = False,
    ) -> asyncio.Task[Any]:
        child = self.create_child_scope(scope, scope_id)
        try:
            task = child.spawn_task(coroutine, name=name, persistent=persistent)
        except BaseException:
            self._children.remove(child)
            raise
        task.add_done_callback(lambda _task: self.kernel._schedule_scope_close(child))
        return task

    @asynccontextmanager
    async def activity(self):
        if not self.accepting:
            raise LifecycleError("component_scope_revoked")
        self._in_flight += 1
        self.kernel._set_active_activities(self, self._in_flight)
        self._drained.clear()
        try:
            yield
        finally:
            self._in_flight = max(0, self._in_flight - 1)
            self.kernel._set_active_activities(self, self._in_flight)
            if not self._in_flight:
                self._drained.set()

    async def quiesce(self, timeout: float | None = None) -> None:
        self.accepting = False
        self.kernel._set_scope_state(self, ScopeState.QUIESCING)
        if self._in_flight:
            try:
                await asyncio.wait_for(
                    self._drained.wait(),
                    timeout=remaining_timeout(
                        self.spec.drain_timeout if timeout is None else timeout
                    ),
                )
            except asyncio.TimeoutError as error:
                raise LifecycleError("component_drain_timeout") from error

    async def close(self) -> None:
        if self._close_task is None:
            self.accepting = False
            self._close_task = asyncio.create_task(
                self._close_once(), name=f"lifecycle-close-once:{self.scope_id}"
            )
            self.kernel._scope_cleanup_tasks.add(self._close_task)
            self._close_task.add_done_callback(
                lambda task: self.kernel._scope_cleanup_done(self, task)
            )
        await asyncio.shield(self._close_task)

    async def _close_once(self) -> None:
        self.accepting = False
        self.kernel._set_scope_state(self, ScopeState.CLOSING)
        error: BaseException | None = None
        try:
            if self.scope_record is not None:
                try:
                    await self.quiesce()
                except BaseException as caught:
                    error = caught
            for child in reversed(tuple(self._children)):
                try:
                    await child.close()
                except BaseException as caught:
                    error = error or caught
            current_task = asyncio.current_task()
            tasks = [
                task
                for task in self._tasks
                if not task.done() and task is not current_task
            ]
            for task in tasks:
                cancel = self._task_cancel_callbacks.get(task)
                if cancel is not None:
                    cancel()
                else:
                    task.cancel()
            if tasks:
                _, pending = await asyncio.wait(
                    tasks, timeout=remaining_timeout(self.spec.cancel_timeout)
                )
                if pending:
                    for task in pending:
                        receipt = self._task_receipts.get(task)
                        if receipt is not None:
                            receipt.state = "leaked"
                            receipt.error_code = "task_cancel_timeout"
                    error = LifecycleError("component_task_cancel_timeout")
            finalizer_errors: list[BaseException] = []
            for callback, args, kwargs in reversed(self._finalizers):
                try:
                    check_budget()
                    result = callback(*args, **kwargs)
                    if inspect.isawaitable(result):
                        await self.kernel._run_cleanup(
                            result,
                            owner=self.spec.component_id,
                            stage="finalizer",
                            timeout=self.spec.finalizer_timeout,
                            grace=self.spec.cancel_timeout,
                        )
                except BaseException as caught:
                    finalizer_errors.append(caught)
            self._finalizers.clear()
            try:
                await self.kernel._run_cleanup(
                    self._stack.aclose(),
                    owner=self.spec.component_id,
                    stage="exit_stack",
                    timeout=self.spec.finalizer_timeout,
                    grace=self.spec.cancel_timeout,
                )
            except BaseException as caught:
                error = error or caught
            if finalizer_errors:
                error = error or finalizer_errors[0]
        finally:
            if self._in_flight:
                error = error or LifecycleError("component_drain_timeout")
            for receipt in self.resources:
                if receipt.state not in {
                    "active",
                    "unresolved",
                    "leaked",
                } or receipt.detail.get("composite_handle"):
                    continue
                check = self._release_checks.get(receipt.receipt_id)
                try:
                    released = check is not None and check() is True
                except BaseException as caught:
                    released = False
                    error = error or caught
                receipt.state = (
                    "inactive"
                    if released and receipt.detail.get("lease_deactivation")
                    else "released"
                    if released
                    else "leaked"
                    if receipt.state == "leaked"
                    else "unresolved"
                )
                if released:
                    receipt.completed_at = datetime.now(timezone.utc).isoformat()
                    self._release_checks.pop(receipt.receipt_id, None)
                else:
                    error = error or LifecycleError("resource_release_unconfirmed")
            if error is None:
                self._release_checks.clear()
            self.closed = True
            self.kernel._scope_closed(self, error)
        if error is not None:
            raise error


class _Registration:
    def __init__(
        self,
        spec: ComponentSpec,
        start: StartCallback,
        stop: StopCallback | None,
        health: HealthCallback | None,
        *,
        pass_context: bool,
    ) -> None:
        self.spec = spec
        self.start = start
        self.stop = stop
        self.health = health
        self.pass_context = pass_context
        self.runtime = ComponentRuntime(spec)
        self.context: LifecycleContext | None = None
        self.value: Any = None
        self.controller: CompositeHandle | None = None
        self.stop_after: set[str] = set()


class LifecycleKernel:
    def __init__(self, state_path: Path | None = None) -> None:
        self._registrations: dict[str, _Registration] = {}
        self._capabilities: dict[str, tuple[str, Any]] = {}
        self._start_order: list[str] = []
        self._metadata_lock = RLock()
        self._operation_lock = asyncio.Lock()
        self._generation = 0
        self._current_operations: dict[str, dict[str, Any]] = {}
        self._observers: list[LifecycleObserver] = []
        self._state_path = state_path
        self._process_metadata: dict[str, Any] = {}
        self._scopes: dict[str, ScopeRecord] = {}
        self._scope_contexts: dict[str, LifecycleContext] = {}
        self._scope_sequence = 0
        self._scope_cleanup_tasks: set[asyncio.Task[Any]] = set()
        self._cleanup_tasks: dict[asyncio.Future[Any], dict[str, Any]] = {}
        self._recovery_required: set[str] = set()
        self._plugin_scope_contexts: dict[str, LifecycleContext] = {}
        self._state_writer: LifecycleStateWriter | None = None
        self._rebuilding: set[str] = set()

    async def _run_cleanup(
        self,
        awaitable: Any,
        *,
        owner: str,
        stage: str,
        timeout: float,
        grace: float,
    ) -> Any:
        if remaining_timeout(timeout) <= 0:
            if inspect.iscoroutine(awaitable):
                awaitable.close()
                raise LifecycleError("shutdown_budget_exhausted")
        task = asyncio.ensure_future(awaitable)
        detail = {"owner": owner, "stage": stage, "state": "running"}
        self._cleanup_tasks[task] = detail

        def completed(future: asyncio.Future[Any]) -> None:
            self._cleanup_tasks.pop(future, None)
            if not future.cancelled():
                future.exception()

        task.add_done_callback(completed)
        try:
            if remaining_timeout(timeout) <= 0:
                raise LifecycleError("shutdown_budget_exhausted")
            done, _ = await asyncio.wait({task}, timeout=remaining_timeout(timeout))
            if done:
                return task.result()
            raise LifecycleError("component_finalizer_timeout")
        except BaseException:
            task.cancel()
            _, pending = await asyncio.wait(
                {task}, timeout=remaining_timeout(min(grace, 0.05))
            )
            if pending:
                detail["state"] = "leaked"
                self._recovery_required.add(owner)
            raise

    @property
    def runtime_generation(self) -> int:
        return self._generation

    def register(
        self,
        spec: ComponentSpec,
        start: StartCallback,
        *,
        stop: StopCallback | None = None,
        health: HealthCallback | None = None,
        pass_context: bool = False,
        replace: bool = False,
    ) -> None:
        with self._metadata_lock:
            existing = self._registrations.get(spec.component_id)
            if existing and not replace:
                if existing.start is start and existing.stop is stop:
                    return
                raise LifecycleError(f"component_duplicate:{spec.component_id}")
            if existing and existing.runtime.state in {
                ComponentState.STARTING,
                ComponentState.READY,
                ComponentState.DEGRADED,
                ComponentState.QUIESCING,
                ComponentState.STOPPING,
            }:
                raise LifecycleError(f"component_active:{spec.component_id}")
            self._registrations[spec.component_id] = _Registration(
                spec,
                start,
                stop,
                health,
                pass_context=pass_context,
            )

    def add_observer(self, observer: LifecycleObserver) -> None:
        with self._metadata_lock:
            if observer not in self._observers:
                self._observers.append(observer)

    def remove_observer(self, observer: LifecycleObserver) -> None:
        with self._metadata_lock:
            if observer in self._observers:
                self._observers.remove(observer)

    def set_process_metadata(self, **metadata: Any) -> None:
        with self._metadata_lock:
            self._process_metadata.update(metadata)
        self._persist()

    def _create_scope_context(
        self,
        parent: LifecycleContext,
        scope: str,
        scope_id: str,
    ) -> LifecycleContext:
        parent._check_accepting()
        if scope not in SCOPE_DEPTH:
            raise LifecycleError("component_scope_invalid")
        if SCOPE_DEPTH[scope] <= SCOPE_DEPTH[parent.scope_type]:
            raise LifecycleError("child_scope_must_be_narrower")
        normalized_id = scope_id.strip()
        if not normalized_id or "/" in normalized_id:
            raise LifecycleError("scope_id_invalid")
        full_id = f"{parent.scope_id}/{normalized_id}"
        if full_id in self._scope_contexts:
            raise LifecycleError(f"scope_duplicate:{full_id}")
        existing = self._scopes.get(full_id)
        if existing and existing.state not in {ScopeState.CLOSED, ScopeState.FAILED}:
            raise LifecycleError(f"scope_duplicate:{full_id}")
        self._scope_sequence += 1
        record = ScopeRecord(
            scope_id=full_id,
            scope=scope,  # type: ignore[arg-type]
            parent_id=parent.scope_id,
            owner_id=parent.spec.component_id,
            generation=self._generation,
            created_order=self._scope_sequence,
        )
        context = LifecycleContext(self, parent.spec, scope_record=record)
        record.resources = context.resources
        self._scopes[full_id] = record
        self._scope_contexts[full_id] = context
        self._emit("scope_open", full_id)
        self._persist()
        return context

    def _set_scope_state(self, context: LifecycleContext, state: ScopeState) -> None:
        if context.scope_record is None:
            return
        context.scope_record.state = state
        self._persist()

    def _set_active_activities(self, context: LifecycleContext, value: int) -> None:
        if context.scope_record is not None:
            context.scope_record.active_activities = value
            return
        registration = self._registrations.get(context.spec.component_id)
        if registration is not None:
            registration.runtime.active_activities = value

    def _mark_scope_failed(
        self, context: LifecycleContext, error: BaseException
    ) -> None:
        if context.scope_record is None:
            return
        context.scope_record.state = ScopeState.FAILED
        context.scope_record.error_code = _error_code(error)
        self._persist()

    def _scope_closed(
        self, context: LifecycleContext, error: BaseException | None
    ) -> None:
        record = context.scope_record
        if record is None:
            return
        record.state = ScopeState.FAILED if error else ScopeState.CLOSED
        record.error_code = _error_code(error) if error else None
        record.closed_at = datetime.now(timezone.utc).isoformat()
        record.active_activities = context._in_flight
        if error is None:
            if self._scope_contexts.get(record.scope_id) is context:
                self._scope_contexts.pop(record.scope_id, None)
            parent = context._parent
            if parent is not None:
                if context in parent._children:
                    parent._children.remove(context)
                receipt = context._parent_receipt
                parent.resources[:] = [
                    item for item in parent.resources if item is not receipt
                ]
                context._parent = None
                context._parent_receipt = None
        else:
            self._recovery_required.add(context.spec.component_id)
        self._emit("scope_failed" if error else "scope_closed", record.scope_id)
        self._prune_scopes()
        self._persist()

    def _schedule_scope_close(self, context: LifecycleContext) -> None:
        if context.closed:
            return
        task = asyncio.create_task(
            context.close(), name=f"lifecycle-close:{context.scope_id}"
        )
        self._scope_cleanup_tasks.add(task)
        task.add_done_callback(
            lambda completed: self._scope_cleanup_done(context, completed)
        )

    def _scope_cleanup_done(
        self,
        context: LifecycleContext,
        task: asyncio.Task[Any],
    ) -> None:
        self._scope_cleanup_tasks.discard(task)
        try:
            error = task.exception()
        except asyncio.CancelledError:
            error = LifecycleError("scope_cleanup_cancelled")
        if error is None:
            return
        error_code = _error_code(error)
        if error_code in _WORKER_RECOVERY_ERRORS or "timeout" in error_code:
            self._recovery_required.add(context.spec.component_id)
        self._persist()

    async def drain_scope_cleanups(self) -> None:
        while self._scope_cleanup_tasks:
            tasks = list(self._scope_cleanup_tasks)
            _, pending = await asyncio.wait(tasks, timeout=remaining_timeout(10.0))
            if pending:
                raise LifecycleError("scope_cleanup_timeout")

    def _prune_scopes(self, keep: int = 200) -> None:
        closed = sorted(
            (
                record
                for record in self._scopes.values()
                if record.state is ScopeState.CLOSED
            ),
            key=lambda item: item.created_order,
        )
        for record in closed[:-keep]:
            self._scopes.pop(record.scope_id, None)

    def unregister_declared(self, component_id: str) -> None:
        with self._metadata_lock:
            registration = self._registrations.get(component_id)
            if registration is None:
                return
            if registration.runtime.state not in {
                ComponentState.DECLARED,
                ComponentState.STOPPED,
                ComponentState.FAILED,
            }:
                raise LifecycleError(f"component_active:{component_id}")
            self._registrations.pop(component_id, None)
            self._recovery_required.discard(component_id)

    def unregister_components(self, component_ids: set[str]) -> None:
        for component_id in sorted(component_ids, reverse=True):
            self.unregister_declared(component_id)

    def component_context(self, component_id: str) -> LifecycleContext:
        registration = self._registrations.get(component_id)
        if registration is None or registration.context is None:
            raise LifecycleError(f"component_context_unavailable:{component_id}")
        if not registration.context.accepting:
            raise LifecycleError(f"component_scope_revoked:{component_id}")
        return registration.context

    def create_scope(
        self,
        component_id: str,
        scope: str,
        scope_id: str,
    ) -> LifecycleContext:
        return self.component_context(component_id).create_child_scope(scope, scope_id)

    def observe_external_process(
        self,
        component_id: str,
        *,
        pid: int,
        scope: str = "worker",
        metadata: dict[str, Any] | None = None,
        controller: CompositeHandle | None = None,
    ) -> None:
        registration = self._registrations.get(component_id)
        if registration is None:
            self.register(
                ComponentSpec(
                    component_id,
                    scope=scope,  # type: ignore[arg-type]
                    stage="management",
                    source="launcher_process",
                ),
                lambda: None,
            )
            registration = self._registrations[component_id]
        runtime = registration.runtime
        registration.controller = controller
        self._generation += 1
        runtime.state = ComponentState.READY
        runtime.health = "healthy"
        runtime.error_code = None
        runtime.runtime_generation = self._generation
        runtime.observed_generation = self._generation
        runtime.started_at = datetime.now(timezone.utc).isoformat()
        runtime.stopped_at = None
        runtime.metadata = {"pid": pid, **dict(metadata or {})}
        runtime.resources = [
            ResourceReceipt(
                receipt_id=f"process:{pid}",
                provider="subprocess",
                resource_type="process",
                owner_id=component_id,
                reversible=False,
                detail={"pid": pid},
            )
        ]
        if component_id not in self._start_order:
            self._start_order.append(component_id)
        self._emit("ready", component_id)
        self._persist()

    def release_external_process(
        self,
        pid: int,
        *,
        return_code: int | None,
        reason: str,
    ) -> None:
        for component_id, registration in self._registrations.items():
            if registration.runtime.metadata.get("pid") != pid:
                continue
            runtime = registration.runtime
            if runtime.state is ComponentState.STOPPED:
                return
            for receipt in runtime.resources:
                receipt.state = "released"
                receipt.completed_at = datetime.now(timezone.utc).isoformat()
            runtime.state = ComponentState.STOPPED
            runtime.health = "stopped"
            runtime.stopped_at = datetime.now(timezone.utc).isoformat()
            runtime.metadata.update({"return_code": return_code, "exit_reason": reason})
            registration.controller = None
            while component_id in self._start_order:
                self._start_order.remove(component_id)
            self._emit("stopped", component_id)
            self._persist()
            return

    def observe_plugin_incarnation(
        self,
        plugin_id: str,
        incarnation_id: str,
        *,
        source_digest: str,
        receipts: list[ResourceReceipt],
        classification: str,
        stop: StopCallback | None = None,
        release_checks: dict[str, Callable[[], bool]] | None = None,
        stop_after: set[str] | None = None,
    ) -> None:
        component_id = f"plugin:{plugin_id}"
        registration = self._registrations.get(component_id)
        if registration is None:
            self.register(
                ComponentSpec(
                    component_id,
                    scope="plugin",
                    stage="runtime",
                    failure_policy="degrade",
                    source="plugin_runtime",
                    depends_on=("runtime:plugin_host",)
                    if "runtime:plugin_host" in self._registrations
                    else (),
                ),
                lambda: None,
            )
            registration = self._registrations[component_id]
        runtime = registration.runtime
        if stop is not None:
            registration.stop = stop
        if stop_after is not None:
            registration.stop_after = stop_after - {component_id, "runtime:plugin_host"}
        plugin_scope = self._plugin_scope_contexts.get(plugin_id)
        if plugin_scope is not None and plugin_scope.closed:
            self._plugin_scope_contexts.pop(plugin_id, None)
            plugin_scope = None
        if (
            plugin_scope is not None
            and plugin_scope.scope_record is not None
            and plugin_scope.scope_record.metadata.get("incarnation_id")
            != incarnation_id
        ):
            plugin_scope.accepting = False
            self._schedule_scope_close(plugin_scope)
            plugin_scope = None
        if plugin_scope is None:
            try:
                host = self.component_context("runtime:plugin_host")
            except LifecycleError:
                host = None
            if host is not None:
                scope_plugin_id = plugin_id.replace("/", "_").replace("\\", "_")
                plugin_scope = host.create_child_scope(
                    "plugin", f"{scope_plugin_id}:{incarnation_id[:12]}"
                )
                if plugin_scope.scope_record is not None:
                    plugin_scope.scope_record.metadata.update(
                        {
                            "plugin_id": plugin_id,
                            "incarnation_id": incarnation_id,
                            "classification": classification,
                            "source_digest": source_digest[:12],
                        }
                    )
                self._plugin_scope_contexts[plugin_id] = plugin_scope
        if plugin_scope is not None:
            plugin_scope._release_checks.update(release_checks or {})
            self._merge_scope_resources(plugin_scope, receipts)
            receipts = plugin_scope.resources
            registration.context = plugin_scope
        elif release_checks is not None:
            context = registration.context or LifecycleContext(self, registration.spec)
            context._release_checks.update(release_checks)
            self._merge_scope_resources(context, receipts)
            registration.context = context
            receipts = context.resources
        receipt_ids = {receipt.receipt_id for receipt in receipts}
        if (
            runtime.state in {ComponentState.READY, ComponentState.DEGRADED}
            and runtime.metadata.get("incarnation_id") == incarnation_id
            and {receipt.receipt_id for receipt in runtime.resources} == receipt_ids
            and runtime.metadata.get("classification") == classification
        ):
            return
        self._generation += 1
        runtime.state = ComponentState.READY
        runtime.health = "healthy"
        runtime.runtime_generation = self._generation
        runtime.observed_generation = self._generation
        runtime.started_at = datetime.now(timezone.utc).isoformat()
        runtime.stopped_at = None
        runtime.metadata = {
            "plugin_id": plugin_id,
            "incarnation_id": incarnation_id,
            "source_digest": source_digest[:12],
            "classification": classification,
        }
        runtime.resources = list(receipts)
        if component_id not in self._start_order:
            self._start_order.append(component_id)
        self._emit("ready", component_id)
        self._persist()

    def release_plugin_incarnation(self, plugin_id: str) -> None:
        component_id = f"plugin:{plugin_id}"
        registration = self._registrations.get(component_id)
        if registration is None:
            return
        runtime = registration.runtime
        plugin_scope = self._plugin_scope_contexts.pop(plugin_id, None)
        if plugin_scope is not None:
            plugin_scope.accepting = False
            self._schedule_scope_close(plugin_scope)
        for receipt in runtime.resources:
            if receipt.state in {"active", "unresolved", "leaked"}:
                self._verify_observed_receipt(registration.context, receipt)
        unresolved = any(
            r.state in {"active", "unresolved", "leaked"} for r in runtime.resources
        )
        runtime.state = ComponentState.FAILED if unresolved else ComponentState.STOPPED
        runtime.health = "failed" if unresolved else "stopped"
        if unresolved:
            runtime.error_code = "resource_release_unconfirmed"
            self._recovery_required.add(component_id)
        runtime.stopped_at = datetime.now(timezone.utc).isoformat()
        while component_id in self._start_order:
            self._start_order.remove(component_id)
        self._emit("stopped", component_id)
        self._persist()

    def forget_plugin_incarnation(self, plugin_id: str) -> None:
        self.release_plugin_incarnation(plugin_id)
        self._plugin_scope_contexts.pop(plugin_id, None)
        self.unregister_declared(f"plugin:{plugin_id}")

    @staticmethod
    def _verify_observed_receipt(
        context: LifecycleContext | None, receipt: ResourceReceipt
    ) -> bool:
        check = context._release_checks.get(receipt.receipt_id) if context else None
        try:
            released = check is not None and check() is True
        except Exception:
            released = False
        if released:
            receipt.state = (
                "inactive" if receipt.detail.get("lease_deactivation") else "released"
            )
            receipt.completed_at = datetime.now(timezone.utc).isoformat()
            receipt.error_code = None
            context._release_checks.pop(receipt.receipt_id, None)
        else:
            if receipt.state != "leaked":
                receipt.state = "unresolved"
            receipt.error_code = receipt.error_code or "resource_release_unconfirmed"
        return released

    @staticmethod
    def _merge_scope_resources(
        context: LifecycleContext, resources: list[ResourceReceipt]
    ) -> None:
        existing = {receipt.receipt_id: receipt for receipt in context.resources}
        seen: set[str] = set()
        for resource in resources:
            seen.add(resource.receipt_id)
            resource.owner_id = context.scope_id
            current = existing.get(resource.receipt_id)
            if current is None:
                context.resources.append(resource)
                continue
            current.state = resource.state
            current.completed_at = resource.completed_at
            current.error_code = resource.error_code
            current.detail = dict(resource.detail)
            current.reversible = resource.reversible
        for receipt in context.resources:
            if receipt.receipt_id not in seen and receipt.state == "active":
                LifecycleKernel._verify_observed_receipt(context, receipt)

    def provide(self, component_id: str, capability: str, value: Any) -> None:
        with self._metadata_lock:
            current = self._capabilities.get(capability)
            if current and current[0] != component_id:
                raise LifecycleError(f"capability_duplicate:{capability}")
            self._capabilities[capability] = (component_id, value)

    def require(self, capability: str) -> Any:
        with self._metadata_lock:
            try:
                return self._capabilities[capability][1]
            except KeyError as error:
                raise LifecycleError(f"capability_unavailable:{capability}") from error

    def validate(self) -> None:
        with self._metadata_lock:
            specs = {key: value.spec for key, value in self._registrations.items()}
        for component_id, spec in specs.items():
            missing = set(spec.depends_on) - set(specs)
            if missing:
                raise LifecycleError(
                    f"component_dependency_missing:{component_id}:{','.join(sorted(missing))}"
                )
            invalid_scope = [
                dependency
                for dependency in spec.depends_on
                if SCOPE_DEPTH[spec.scope] < SCOPE_DEPTH[specs[dependency].scope]
            ]
            if invalid_scope:
                raise LifecycleError(
                    "component_scope_dependency_invalid:"
                    f"{component_id}:{','.join(sorted(invalid_scope))}"
                )
        visiting: set[str] = set()
        completed: set[str] = set()

        def visit(component_id: str) -> None:
            if component_id in completed:
                return
            if component_id in visiting:
                raise LifecycleError(f"component_dependency_cycle:{component_id}")
            visiting.add(component_id)
            for dependency in specs[component_id].depends_on:
                visit(dependency)
            visiting.remove(component_id)
            completed.add(component_id)

        for component_id in sorted(specs):
            visit(component_id)

    async def start_stage(self, stage: LifecycleStage) -> None:
        async with self._operation_lock:
            self.validate()
            started: list[str] = []
            try:
                await self._start_subset(
                    {
                        component_id
                        for component_id, registration in self._registrations.items()
                        if registration.spec.stage == stage
                        and registration.runtime.state
                        in {ComponentState.DECLARED, ComponentState.STOPPED}
                    },
                    started,
                )
            except BaseException:
                for component_id in reversed(started):
                    await self._stop_one(component_id, suppress_errors=True)
                raise

    async def start_components(self, component_ids: set[str]) -> None:
        async with self._operation_lock:
            self.validate()
            missing = component_ids - set(self._registrations)
            if missing:
                raise LifecycleError(f"component_unknown:{','.join(sorted(missing))}")
            pending = {
                component_id
                for component_id in component_ids
                if self._registrations[component_id].runtime.state
                in {ComponentState.DECLARED, ComponentState.STOPPED}
            }
            started: list[str] = []
            try:
                await self._start_subset(pending, started)
            except BaseException:
                for component_id in reversed(started):
                    await self._stop_one(component_id, suppress_errors=True)
                raise

    async def stop_components(self, component_ids: set[str]) -> None:
        async with self._operation_lock:
            selected = self._component_stop_order(component_ids)
            error = None
            for component_id in selected:
                try:
                    await self._stop_one(component_id, suppress_errors=False)
                except BaseException as caught:
                    error = error or caught
            if error is not None:
                raise error

    async def _start_subset(self, pending: set[str], started: list[str]) -> None:
        while pending:
            blocked = [
                component_id
                for component_id in pending
                if any(
                    self._registrations[dependency].runtime.state
                    in {ComponentState.DEGRADED, ComponentState.FAILED}
                    for dependency in self._registrations[component_id].spec.depends_on
                )
            ]
            for component_id in sorted(blocked):
                pending.remove(component_id)
                registration = self._registrations[component_id]
                registration.runtime.error_code = "component_dependency_not_ready"
                registration.runtime.health = "blocked"
                registration.runtime.state = (
                    ComponentState.FAILED
                    if registration.spec.failure_policy == "fatal"
                    else ComponentState.DEGRADED
                )
                self._emit("dependency_blocked", component_id)
                if registration.spec.failure_policy == "fatal":
                    raise LifecycleError(
                        f"component_dependency_not_ready:{component_id}"
                    )
            if not pending:
                return
            ready = [
                component_id
                for component_id in pending
                if all(
                    self._registrations[dependency].runtime.state
                    is ComponentState.READY
                    for dependency in self._registrations[component_id].spec.depends_on
                )
            ]
            if not ready:
                raise LifecycleError(
                    f"component_dependencies_not_ready:{','.join(sorted(pending))}"
                )
            min_priority = min(
                self._registrations[component_id].spec.priority
                for component_id in ready
            )
            batch_candidates = sorted(
                component_id
                for component_id in ready
                if self._registrations[component_id].spec.priority == min_priority
            )
            batch: list[str] = []
            groups: set[str] = set()
            for component_id in batch_candidates:
                spec = self._registrations[component_id].spec
                group = spec.resource_group or component_id
                if batch and not spec.parallel_safe:
                    continue
                if batch and not all(
                    self._registrations[item].spec.parallel_safe for item in batch
                ):
                    continue
                if group in groups:
                    continue
                batch.append(component_id)
                groups.add(group)
                if not spec.parallel_safe:
                    break
            results = await asyncio.gather(
                *(self._start_one(component_id) for component_id in batch),
                return_exceptions=True,
            )
            fatal: BaseException | None = None
            for component_id, result in zip(batch, results):
                pending.remove(component_id)
                registration = self._registrations[component_id]
                if isinstance(result, BaseException):
                    if registration.spec.failure_policy == "fatal" and fatal is None:
                        fatal = result
                    continue
                if registration.runtime.state is ComponentState.READY:
                    started.append(component_id)
            if fatal is not None:
                raise fatal

    async def _start_one(self, component_id: str) -> None:
        registration = self._registrations[component_id]
        runtime = registration.runtime
        runtime.state = ComponentState.STARTING
        runtime.error_code = None
        runtime.metadata = {}
        started_at = time.monotonic()
        context = LifecycleContext(self, registration.spec)
        registration.context = context
        registration.controller = None
        self._current_operations[component_id] = {
            "action": "rebuild" if runtime.started_at else "start",
            "component_id": component_id,
            "started_at": datetime.now(timezone.utc).isoformat(),
        }
        self._emit("starting", component_id)
        self._persist()
        try:
            result = (
                registration.start(context)
                if registration.pass_context
                else registration.start()
            )
            if inspect.isawaitable(result):
                if registration.spec.timeout is not None:
                    result = await asyncio.wait_for(
                        result, timeout=registration.spec.timeout
                    )
                else:
                    result = await result
            if isinstance(result, AbstractAsyncContextManager) or (
                hasattr(result, "__aenter__") and hasattr(result, "__aexit__")
            ):
                result = await context.enter_async_context(result)
            if isinstance(result, RuntimeHandle):
                if result.health != "healthy":
                    raise LifecycleError("component_health_failed")
                runtime.metadata = dict(result.metadata)
                registration.controller = result.controller
                result = result.value
            registration.value = context.value = result
            if registration.controller is not None:
                if not await self._refresh_controller(component_id, check_health=True):
                    raise LifecycleError("component_health_failed")
            if registration.health is not None:
                health = registration.health(result)
                if inspect.isawaitable(health):
                    health = await health
                if health is False:
                    registered_callback = registration.start
                    try:
                        registered_callback = inspect.getclosurevars(
                            registration.start
                        ).nonlocals.get("func", registration.start)
                    except TypeError:
                        pass
                    start_module = getattr(registered_callback, "__module__", "unknown")
                    health_module = getattr(
                        registration.health, "__module__", "unknown"
                    )
                    runtime.metadata.update(
                        {
                            "registered_start_callback": (
                                f"{start_module}:"
                                f"{getattr(registered_callback, '__name__', 'unknown')}"
                            ),
                            "pass_context": registration.pass_context,
                            "initial_health_callback": (
                                f"{health_module}:"
                                f"{getattr(registration.health, '__name__', 'unknown')}"
                            ),
                            "captured_resource_count": len(context.resources),
                        }
                    )
                    raise LifecycleError("component_health_failed")
            self._generation += 1
            runtime.state = ComponentState.READY
            runtime.health = "healthy"
            runtime.last_health_checked_at = datetime.now(timezone.utc).isoformat()
            runtime.consecutive_health_failures = 0
            runtime.runtime_generation = self._generation
            runtime.observed_generation = self._generation
            runtime.started_at = datetime.now(timezone.utc).isoformat()
            runtime.stopped_at = None
            runtime.resources = context.resources
            if component_id not in self._start_order:
                self._start_order.append(component_id)
            self._emit("ready", component_id)
        except BaseException as error:
            runtime.state = (
                ComponentState.DEGRADED
                if registration.spec.failure_policy == "degrade"
                else ComponentState.FAILED
            )
            runtime.health = (
                "degraded" if runtime.state is ComponentState.DEGRADED else "failed"
            )
            runtime.error_code = f"component_start_failed:{type(error).__name__}"
            try:
                await context.close()
            except BaseException as cleanup_error:
                runtime.metadata["cleanup_error_code"] = (
                    f"component_start_cleanup_failed:{type(cleanup_error).__name__}"
                )
            runtime.resources = context.resources
            self._release_capabilities(component_id)
            registration.context = None
            registration.value = None
            registration.controller = None
            self._emit(runtime.state.value, component_id)
            if registration.spec.failure_policy == "fatal":
                raise
        finally:
            runtime.duration_ms = round((time.monotonic() - started_at) * 1000, 2)
            self._current_operations.pop(component_id, None)
            self._persist()

    async def stop_all(self, timeout: float = 15.0) -> None:
        with shutdown_budget(timeout):
            try:
                await self._stop_all_with_budget()
            finally:
                if self._state_writer is not None:
                    if not await self._state_writer.close(remaining_timeout(timeout)):
                        self._recovery_required.add("lifecycle:state_writer")

    async def _stop_all_with_budget(self) -> None:
        async with self._operation_lock:
            ordered = self._component_stop_order(set(self._start_order))
            for component_id in ordered:
                context = self._registrations[component_id].context
                if context is not None:
                    context.accepting = False
            for component_id in ordered:
                await self._stop_one(component_id, suppress_errors=True)
            try:
                await self.drain_scope_cleanups()
            except LifecycleError:
                self._recovery_required.update(ordered)

    def _component_stop_order(self, selected: set[str]) -> list[str]:
        remaining = {
            item
            for item in selected
            if item in self._registrations and item in self._start_order
        }
        ordered: list[str] = []
        start_positions = {
            component_id: index for index, component_id in enumerate(self._start_order)
        }
        while remaining:
            ready = [
                component_id
                for component_id in remaining
                if not any(
                    component_id in self._registrations[other].spec.depends_on
                    for other in remaining
                )
                and not (self._registrations[component_id].stop_after & remaining)
            ]
            if not ready:
                ready = list(remaining)
            ready.sort(
                key=lambda item: (
                    -SCOPE_DEPTH[self._registrations[item].spec.scope],
                    self._registrations[item].spec.stop_priority
                    if self._registrations[item].spec.stop_priority is not None
                    else float("inf"),
                    -start_positions.get(item, -1),
                    item,
                )
            )
            selected_item = ready[0]
            remaining.remove(selected_item)
            ordered.append(selected_item)
        return ordered

    async def _stop_one(self, component_id: str, *, suppress_errors: bool) -> None:
        registration = self._registrations.get(component_id)
        if registration is None or registration.runtime.state not in {
            ComponentState.READY,
            ComponentState.DEGRADED,
            ComponentState.FAILED,
        }:
            return
        if (
            registration.runtime.state is ComponentState.FAILED
            and registration.context is None
            and registration.controller is None
            and component_id not in self._start_order
        ):
            registration.runtime.state = ComponentState.STOPPED
            registration.runtime.health = "stopped"
            registration.runtime.stopped_at = datetime.now(timezone.utc).isoformat()
            self._release_capabilities(component_id)
            self._emit("stopped", component_id)
            self._persist()
            return
        runtime = registration.runtime
        runtime.state = ComponentState.QUIESCING
        self._current_operations[component_id] = {
            "action": "stop",
            "operation_id": uuid.uuid4().hex,
            "component_id": component_id,
            "started_at": datetime.now(timezone.utc).isoformat(),
        }
        self._emit("quiescing", component_id)
        error: BaseException | None = None
        stop_errors: list[dict[str, str]] = []
        runtime.metadata["stop_errors"] = stop_errors

        def record_error(stage: str, caught: BaseException) -> None:
            nonlocal error
            error = error or caught
            code = _error_code(caught)
            stop_errors.append({"stage": stage, "error_code": code})
            self._recovery_required.add(component_id)

        try:
            if registration.context is not None:
                try:
                    await registration.context.quiesce()
                except BaseException as caught:
                    record_error("context_drain", caught)
            if registration.controller is not None:
                try:
                    await self._invoke_controller(
                        registration.controller,
                        "quiesce",
                        timeout=registration.spec.drain_timeout,
                    )
                except BaseException as caught:
                    record_error("handle_drain", caught)
            runtime.state = ComponentState.STOPPING
            if registration.stop is not None:
                try:
                    check_budget()
                    result = registration.stop()
                    if inspect.isawaitable(result):
                        await self._run_cleanup(
                            result,
                            owner=component_id,
                            stage="stop",
                            timeout=registration.spec.timeout or 10.0,
                            grace=registration.spec.cancel_timeout,
                        )
                except BaseException as caught:
                    record_error("stop", caught)
            if registration.context is not None:
                try:
                    await registration.context.close()
                except BaseException as caught:
                    record_error("context_close", caught)
            if registration.controller is not None:
                try:
                    await self._invoke_controller(
                        registration.controller,
                        "close",
                        timeout=registration.spec.finalizer_timeout,
                        owner=component_id,
                    )
                    for receipt in runtime.resources:
                        if receipt.detail.get("composite_handle"):
                            receipt.state = "released"
                            receipt.completed_at = datetime.now(
                                timezone.utc
                            ).isoformat()
                except BaseException as caught:
                    record_error("handle_close", caught)
                    for receipt in runtime.resources:
                        if receipt.detail.get("composite_handle"):
                            receipt.state = "leaked"
                            receipt.error_code = (
                                f"handle_close_failed:{_error_code(caught)}"
                            )
            if any(
                r.state in {"active", "unresolved", "leaked"} for r in runtime.resources
            ):
                record_error(
                    "resource_verification",
                    LifecycleError("resource_release_unconfirmed"),
                )
            if error is None:
                registration.context = None
                registration.value = None
                registration.controller = None
            runtime.state = ComponentState.FAILED if error else ComponentState.STOPPED
            runtime.health = "failed" if error else "stopped"
            runtime.stopped_at = datetime.now(timezone.utc).isoformat()
            self._release_capabilities(component_id)
            while component_id in self._start_order:
                self._start_order.remove(component_id)
            if error is not None:
                raise error
            self._emit("stopped", component_id)
        except BaseException as caught:
            error = caught
            runtime.state = ComponentState.FAILED
            runtime.health = "failed"
            stop_error = _error_code(caught)
            runtime.error_code = f"component_stop_failed:{stop_error}"
            resources: dict[str, int] = defaultdict(int)
            for receipt in runtime.resources:
                if receipt.state in {"active", "unresolved", "leaked"}:
                    resources[f"{receipt.provider}:{receipt.resource_type}"] += 1
            runtime.metadata["stop_diagnostic"] = {
                "diagnostic_id": f"diag-{uuid.uuid4().hex[:12]}",
                "occurred_at": datetime.now(timezone.utc).isoformat(),
                "operation_id": self._current_operations[component_id]["operation_id"],
                "stages": list(stop_errors),
                "resources": dict(resources),
            }
            if stop_error in _WORKER_RECOVERY_ERRORS or "timeout" in stop_error:
                self._recovery_required.add(component_id)
            self._emit("failed", component_id)
        finally:
            self._current_operations.pop(component_id, None)
            self._persist()
        if error is not None and not suppress_errors:
            raise error

    async def _invoke_controller(
        self,
        controller: CompositeHandle,
        method_name: str,
        *,
        timeout: float,
        owner: str = "lifecycle",
    ) -> Any:
        method = getattr(controller, method_name, None)
        if not callable(method):
            if method_name == "close":
                raise LifecycleError("handle_close_contract_missing")
            return None
        check_budget()
        result = method()
        if inspect.isawaitable(result):
            if method_name in {"close", "quiesce"}:
                return await self._run_cleanup(
                    result,
                    owner=owner,
                    stage=f"handle_{method_name}",
                    timeout=timeout,
                    grace=0.05,
                )
            return await asyncio.wait_for(result, timeout=remaining_timeout(timeout))
        return result

    async def _refresh_controller(
        self,
        component_id: str,
        *,
        check_health: bool,
    ) -> bool:
        registration = self._registrations[component_id]
        controller = registration.controller
        if controller is None:
            return True
        runtime = registration.runtime
        snapshot = await self._invoke_controller(
            controller,
            "snapshot",
            timeout=registration.spec.timeout or 5,
        )
        if isinstance(snapshot, dict):
            runtime.metadata["handle"] = dict(snapshot)
        resources = await self._invoke_controller(
            controller,
            "resource_snapshot",
            timeout=registration.spec.timeout or 5,
        )
        if isinstance(resources, list):
            self._merge_controller_resources(registration, resources)
        if not check_health:
            return True
        health = await self._invoke_controller(
            controller,
            "health",
            timeout=registration.spec.timeout or 5,
        )
        if isinstance(health, dict):
            runtime.metadata["handle_health"] = dict(health)
            return bool(health.get("healthy", True))
        if health is False:
            return False
        return not (isinstance(health, str) and health in {"failed", "unhealthy"})

    @staticmethod
    def _merge_controller_resources(
        registration: _Registration,
        resources: list[ResourceReceipt],
    ) -> None:
        context = registration.context
        if context is None:
            return
        existing = {receipt.receipt_id: receipt for receipt in context.resources}
        seen: set[str] = set()
        for resource in resources:
            if not isinstance(resource, ResourceReceipt):
                continue
            seen.add(resource.receipt_id)
            resource.owner_id = registration.spec.component_id
            resource.detail = {**resource.detail, "composite_handle": True}
            current = existing.get(resource.receipt_id)
            if current is None:
                context.resources.append(resource)
                continue
            current.state = resource.state
            current.completed_at = resource.completed_at
            current.error_code = resource.error_code
            current.detail = dict(resource.detail)
            current.reversible = resource.reversible
        completed_at = datetime.now(timezone.utc).isoformat()
        for receipt in context.resources:
            if (
                receipt.detail.get("composite_handle")
                and receipt.receipt_id not in seen
                and receipt.state == "active"
            ):
                receipt.state = "released"
                receipt.completed_at = completed_at

    def _release_capabilities(self, component_id: str) -> None:
        with self._metadata_lock:
            for capability, owner in list(self._capabilities.items()):
                if owner[0] == component_id:
                    self._capabilities.pop(capability, None)

    async def restart_for_config(
        self,
        keys: set[str],
        *,
        apply_change: Callable[[], Any],
        rollback_change: Callable[[], Any],
    ) -> LifecycleOperationResult:
        normalized = {key.upper() for key in keys}
        direct = {
            component_id
            for component_id, registration in self._registrations.items()
            if normalized & {key.upper() for key in registration.spec.config_keys}
        }
        if not direct:
            return LifecycleOperationResult(
                "no_change", [], runtime_generation=self._generation
            )
        affected = self._dependent_closure(direct)
        if any(
            self._registrations[item].spec.restart_policy != "component"
            for item in affected
        ):
            return LifecycleOperationResult(
                "restart_pending",
                sorted(affected),
                ["component_not_restartable"],
                runtime_generation=self._generation,
            )
        from zhenxun.services.runtime_mutation import runtime_mutation_coordinator

        async with runtime_mutation_coordinator.operation("component_config"):
            return await self._restart_for_config_locked(
                affected,
                apply_change=apply_change,
                rollback_change=rollback_change,
            )

    async def _restart_for_config_locked(
        self,
        affected: set[str],
        *,
        apply_change: Callable[[], Any],
        rollback_change: Callable[[], Any],
    ) -> LifecycleOperationResult:
        async with self._operation_lock:
            active = [item for item in self._start_order if item in affected]
            self._rebuilding = set(affected)
            change_applied = False
            try:
                for component_id in reversed(active):
                    await self._stop_one(component_id, suppress_errors=False)
                value = apply_change()
                if inspect.isawaitable(value):
                    await value
                change_applied = True
                started: list[str] = []
                await self._start_subset(set(active), started)
                return LifecycleOperationResult(
                    "component_restarted",
                    sorted(affected),
                    runtime_generation=self._generation,
                )
            except BaseException as error:
                error_code = _error_code(error)
                if error_code in _WORKER_RECOVERY_ERRORS:
                    return LifecycleOperationResult(
                        "restart_pending",
                        sorted(affected),
                        [f"component_restart_failed:{error_code}"],
                        "worker_recovery_required",
                        self._generation,
                    )
                for component_id in reversed(started if "started" in locals() else []):
                    await self._stop_one(component_id, suppress_errors=True)
                rollback_error: BaseException | None = None
                try:
                    if change_applied:
                        value = rollback_change()
                        if inspect.isawaitable(value):
                            await value
                    restored: list[str] = []
                    await self._start_subset(set(active), restored)
                except BaseException as caught:
                    rollback_error = caught
                return LifecycleOperationResult(
                    "restart_pending" if rollback_error else "rolled_back",
                    sorted(affected),
                    [
                        f"component_restart_failed:{error_code}",
                        *(
                            [
                                f"component_rollback_failed:{type(rollback_error).__name__}"
                            ]
                            if rollback_error
                            else []
                        ),
                    ],
                    "worker_recovery_required" if rollback_error else "semantic",
                    self._generation,
                )
            finally:
                self._rebuilding.clear()

    def _dependent_closure(self, direct: set[str]) -> set[str]:
        affected = set(direct)
        while True:
            dependents = {
                component_id
                for component_id, registration in self._registrations.items()
                if set(registration.spec.depends_on) & affected
            }
            before = len(affected)
            affected.update(dependents)
            if len(affected) == before:
                return affected

    def status(self) -> dict[str, Any]:
        with self._metadata_lock:
            components = [
                registration.runtime.public_dict()
                for registration in sorted(
                    self._registrations.values(),
                    key=lambda item: item.spec.component_id,
                )
            ]
            dynamic_scopes = [
                record.public_dict()
                for record in sorted(
                    self._scopes.values(), key=lambda item: item.created_order
                )
            ]
        scopes: dict[str, int] = defaultdict(int)
        states: dict[str, int] = defaultdict(int)
        for component in components:
            scopes[str(component["scope"])] += 1
            states[str(component["state"])] += 1
        active_receipts = sum(
            sum(
                int(value)
                for key, value in component["resource_counts"].items()
                if key.endswith(":active")
            )
            for component in components
        )
        unowned = [
            *list(self._process_metadata.get("unowned_zhenxun_tasks") or []),
            *list(self._process_metadata.get("unowned_zhenxun_threads") or []),
        ]
        ownership_total = active_receipts + len(unowned)
        try:
            from zhenxun.services.runtime_mutation import runtime_mutation_coordinator

            mutation = runtime_mutation_coordinator.status()
        except ImportError:
            mutation = None
        return {
            "version": 2,
            "persistence": self._state_writer.status()
            if self._state_writer is not None
            else {"pending": False, "enabled": self._state_path is not None},
            "runtime_generation": self._generation,
            "component_count": len(components),
            "scope_counts": dict(sorted(scopes.items())),
            "state_counts": dict(sorted(states.items())),
            "current_operation": dict(next(iter(self._current_operations.values())))
            if len(self._current_operations) == 1
            else None,
            "current_operations": [
                dict(item) for _, item in sorted(self._current_operations.items())
            ],
            "process": dict(self._process_metadata),
            "current_mutation": mutation,
            "dynamic_scope_count": len(dynamic_scopes),
            "active_scope_count": sum(
                scope["state"] not in {"closed", "failed"} for scope in dynamic_scopes
            ),
            "dynamic_scopes": dynamic_scopes,
            "recovery_required": sorted(self._recovery_required),
            "cleanup_tasks": [dict(item) for item in self._cleanup_tasks.values()],
            "unresolved_resources": [
                {
                    "owner_id": receipt.owner_id,
                    "provider": receipt.provider,
                    "resource_type": receipt.resource_type,
                    "state": receipt.state,
                    "error_code": receipt.error_code,
                }
                for registration in self._registrations.values()
                for receipt in registration.runtime.resources
                if receipt.state in {"unresolved", "leaked"}
            ][:200],
            "ownership": {
                "tracked_resource_count": active_receipts,
                "unowned_resource_count": len(unowned),
                "observation_scope": "registered_receipts_and_known_runtime_resources",
                "complete_coverage_proven": False,
                "unowned_resources": unowned,
                "coverage_percent": round(active_receipts / ownership_total * 100, 2)
                if ownership_total
                else None,
            },
            "components": components,
        }

    def _task_completed(
        self,
        context: LifecycleContext,
        task: asyncio.Task[Any],
        *,
        persistent: bool,
    ) -> None:
        context._tasks.discard(task)
        context._task_cancel_callbacks.pop(task, None)
        receipt = context._task_receipts.pop(task, None)
        if receipt is None:
            return
        receipt.completed_at = datetime.now(timezone.utc).isoformat()
        error: BaseException | None = None
        if task.cancelled():
            receipt.state = "cancelled"
        else:
            try:
                error = task.exception()
            except (asyncio.CancelledError, RuntimeError):
                receipt.state = "cancelled"
            if error is not None:
                receipt.state = "failed"
                receipt.error_code = f"task_failed:{type(error).__name__}"
            elif receipt.state == "active":
                receipt.state = "completed"
        if not context.accepting or not persistent:
            self._persist()
            return
        registration = self._registrations.get(context.spec.component_id)
        if registration is None:
            return
        runtime = registration.runtime
        if error is not None or receipt.state == "completed":
            runtime.consecutive_health_failures += 1
            runtime.last_health_checked_at = receipt.completed_at
            runtime.error_code = receipt.error_code or "task_exited_unexpectedly"
            runtime.health = "degraded"
            runtime.state = (
                ComponentState.FAILED
                if registration.spec.failure_policy == "fatal"
                else ComponentState.DEGRADED
            )
            self._emit("background_task_failed", context.spec.component_id)
        self._persist()

    async def check_health(self) -> dict[str, str]:
        async with self._operation_lock:
            return await self._check_health_locked()

    async def _check_health_locked(self) -> dict[str, str]:
        results: dict[str, str] = {}
        for component_id, registration in list(self._registrations.items()):
            if registration.health is None and registration.controller is None:
                continue
            if registration.runtime.state not in {
                ComponentState.READY,
                ComponentState.DEGRADED,
            }:
                continue
            checked_at = datetime.now(timezone.utc).isoformat()
            try:
                healthy: Any = True
                if registration.health is not None:
                    healthy = registration.health(registration.value)
                    if inspect.isawaitable(healthy):
                        healthy = await asyncio.wait_for(
                            healthy,
                            timeout=registration.spec.timeout or 5,
                        )
                if registration.controller is not None:
                    healthy = healthy is not False and await self._refresh_controller(
                        component_id, check_health=True
                    )
                if healthy is False:
                    raise LifecycleError("component_health_failed")
                registration.runtime.health = "healthy"
                registration.runtime.consecutive_health_failures = 0
                if registration.runtime.state is ComponentState.DEGRADED:
                    registration.runtime.state = ComponentState.READY
                results[component_id] = "healthy"
            except BaseException as error:
                runtime = registration.runtime
                runtime.health = "degraded"
                runtime.consecutive_health_failures += 1
                runtime.error_code = f"component_health_failed:{type(error).__name__}"
                runtime.state = (
                    ComponentState.FAILED
                    if registration.spec.failure_policy == "fatal"
                    else ComponentState.DEGRADED
                )
                results[component_id] = runtime.health
            registration.runtime.last_health_checked_at = checked_at
        self._persist()
        return results

    def component_status(self, component_id: str) -> dict[str, Any] | None:
        registration = self._registrations.get(component_id)
        if registration is None:
            return None
        result = registration.runtime.public_dict()
        result["dynamic_scopes"] = [
            record.public_dict()
            for record in sorted(
                self._scopes.values(), key=lambda item: item.created_order
            )
            if record.owner_id == component_id
        ]
        result["recovery_required"] = component_id in self._recovery_required
        return result

    def native_component_ids_for_stage(self, stage: LifecycleStage) -> set[str]:
        return {
            component_id
            for component_id, registration in self._registrations.items()
            if registration.spec.stage == stage
            and registration.spec.source != "priority_lifecycle"
        }

    def owned_object_ids(self, resource_type: str) -> set[int]:
        receipts = [
            receipt
            for registration in self._registrations.values()
            for receipt in registration.runtime.resources
        ]
        receipts.extend(
            receipt for record in self._scopes.values() for receipt in record.resources
        )
        result: set[int] = set()
        for receipt in receipts:
            if receipt.resource_type != resource_type or receipt.state != "active":
                continue
            _, _, identity = receipt.receipt_id.partition(":")
            with contextlib.suppress(ValueError):
                result.add(int(identity))
        return result

    def owned_task_ids(self) -> set[int]:
        result = self.owned_object_ids("task")
        result.update(self.owned_cleanup_task_ids())
        if self._state_writer is not None and self._state_writer.task is not None:
            result.add(id(self._state_writer.task))
        return result

    def owned_cleanup_task_ids(self) -> set[int]:
        return {id(task) for task in self._cleanup_tasks} | {
            id(task) for task in self._scope_cleanup_tasks
        }

    def owned_thread_ids(self) -> set[int]:
        result = self.owned_object_ids("thread")
        if self._state_writer is not None:
            result.update(id(thread) for thread in self._state_writer.worker.threads)
        return result

    def _persist(self) -> None:
        if self._state_path is None or (
            self._state_path == _DEFAULT_STATE_PATH and os.getenv("PYTEST_CURRENT_TEST")
        ):
            return
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            # Synchronous CLI users do not have an event loop to block.
            if self._state_writer is None:
                try:
                    write_json_locked(self._state_path, self.status())
                except OSError:
                    pass
            else:
                self._state_writer.mark_dirty()
            return
        if self._state_writer is None:
            self._state_writer = LifecycleStateWriter(self._state_path, self.status)
        self._state_writer.mark_dirty()

    def _emit(self, event: str, component_id: str) -> None:
        registration = self._registrations.get(component_id)
        if registration is not None:
            payload = {
                "event": event,
                "component": registration.runtime.public_dict(),
                "runtime_generation": self._generation,
            }
        elif scope := self._scopes.get(component_id):
            payload = {
                "event": event,
                "scope": scope.public_dict(),
                "runtime_generation": self._generation,
            }
        else:
            return
        operation = dict(self._current_operations.get(component_id) or {})
        if not operation:
            operation = {
                "action": "stop"
                if event
                in {"quiescing", "stopping", "stopped", "scope_closed", "scope_failed"}
                else "health_check"
                if event in {"background_task_failed", "failed"}
                else "start"
            }
        if component_id in self._rebuilding:
            operation["phase"] = operation.get("action")
            operation["action"] = "rebuild"
        payload["operation"] = operation
        payload["failure_stage"] = (
            registration.runtime.metadata.get("stop_errors", [{}])[0].get("stage")
            if registration is not None
            and registration.runtime.metadata.get("stop_errors")
            else operation.get("phase", operation.get("action"))
        )
        with self._metadata_lock:
            observers = list(self._observers)
        for observer in observers:
            try:
                observer(payload)
            except Exception:
                continue


def _state_path_from_environment() -> Path:
    value = os.getenv("ZHENXUN_LIFECYCLE_STATE_PATH", "").strip()
    return Path(value) if value else _DEFAULT_STATE_PATH


lifecycle_kernel = LifecycleKernel(_state_path_from_environment())
