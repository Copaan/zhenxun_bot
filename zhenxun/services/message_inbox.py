"""Bounded persistence and dispatch of messages received by managed adapters."""

from __future__ import annotations

import asyncio
from contextlib import suppress
from datetime import datetime, timezone
from hashlib import sha256
import json
from pathlib import Path
import time

from .lifecycle.deadline import remaining_timeout
from .lifecycle.diagnostics import DiagnosticWorker
from .message_execution import (
    MessageExecution,
    MessageExecutionUnavailable,
    current_execution,
)
from .message_store import MessageStore


class MessageInbox:
    def __init__(self, path=Path("data/runtime/message-inbox/inbox.sqlite3")):
        self.store = MessageStore(path)
        self.worker = None
        self.queue = asyncio.Queue(maxsize=256)
        self.mutations = asyncio.Queue(maxsize=256)
        self.mutation_task = None
        self.io_lock = asyncio.Lock()
        self.context = None
        self.accepting = False
        self.active = {}
        self.finalizing = set()
        self.codecs = {}
        self.bots = {}
        self.tasks = []
        self.generation = ""
        self.metrics = {"observed": 0, "staging_rejected": 0, "write_failed": 0}
        self.status = {}
        self.code_revision = None
        self.disk_future = None
        self.waiting_inputs = {}
        self.waiter_bridge = None
        self.execution_slots = asyncio.Semaphore(64)
        self.slot_owners = set()
        self._rate_sample = None
        self.rates = {"received_per_second": 0.0, "executed_per_second": 0.0}
        self.timing = {
            "handler_count": 0,
            "handler_ms": 0.0,
            "handler_max_ms": 0.0,
            "finalize_count": 0,
            "finalize_ms": 0.0,
            "finalize_max_ms": 0.0,
        }

    def release_slot(self, identity):
        if identity in self.slot_owners:
            self.slot_owners.remove(identity)
            self.execution_slots.release()

    def install_waiter_bridge(self):
        from functools import wraps
        import sys

        module = sys.modules.get("nonebot_plugin_waiter")
        if module is None or self.waiter_bridge is not None:
            return
        original = module.Waiter.wait

        @wraps(original)
        async def wait(waiter, *args, **kwargs):
            from .message_execution import current_dispatch_lease

            execution = current_execution.get()
            if execution is None or execution.identity not in self.active:
                return await original(waiter, *args, **kwargs)
            identity = execution.identity
            if identity in self.waiting_inputs:
                raise RuntimeError("concurrent_message_prompts_unsupported")
            if identity not in self.waiting_inputs and len(self.waiting_inputs) >= 64:
                raise RuntimeError("message_interaction_capacity")
            self.waiting_inputs[identity] = self.waiting_inputs.get(identity, 0) + 1
            lease = current_dispatch_lease.get()
            if lease is not None:
                lease()
            canceled = False
            try:
                # A live prompt yields this conversation so its answer can enter
                # the normal adapter and current permission/matcher pipeline.
                await self.io(self.store.waiting_input, identity, True)
                self.release_slot(identity)
                return await original(waiter, *args, **kwargs)
            except asyncio.CancelledError:
                canceled = True
                raise
            finally:
                try:
                    if (
                        not canceled
                        and self.accepting
                        and identity not in self.slot_owners
                    ):
                        await self.execution_slots.acquire()
                        self.slot_owners.add(identity)
                    if (
                        self.accepting
                        and not canceled
                        and lease is not None
                        and hasattr(lease, "reacquire")
                    ):
                        await lease.reacquire()
                finally:
                    self.waiting_inputs[identity] -= 1
                    if not self.waiting_inputs[identity]:
                        del self.waiting_inputs[identity]
                        if self.accepting and not canceled:
                            await self.io(self.store.waiting_input, identity, False)

        module.Waiter.wait = wait
        self.waiter_bridge = module.Waiter, original, wait

    @staticmethod
    def _source_digest(root):
        digest = sha256()
        root = root.resolve()
        for path in sorted(root.rglob("*.py")):
            if "__pycache__" in path.parts or not path.resolve().is_relative_to(root):
                continue
            before = path.stat()
            content = path.read_bytes()
            after = path.stat()
            if (before.st_mtime_ns, before.st_size) != (
                after.st_mtime_ns,
                after.st_size,
            ):
                raise RuntimeError("message_code_revision_unstable")
            digest.update(path.relative_to(root).as_posix().encode())
            digest.update(content)
        return digest.hexdigest()

    async def refresh_generation(self):
        from .runtime_reload import plugin_runtime_manager

        revision = plugin_runtime_manager.generation
        if self.code_revision == revision:
            return
        self.generation = await self.io(self._source_digest, Path.cwd() / "zhenxun")
        self.code_revision = revision

    async def io(self, method, *args):
        async with self.io_lock:
            # A previous caller may have been canceled during a durable commit.
            # Serialize against its actual completion before submitting more work.
            if self.disk_future is not None:
                with suppress(Exception):
                    await asyncio.shield(self.disk_future)
            future = self.worker.submit(method, *args)
            self.disk_future = future
            try:
                return await asyncio.shield(future)
            except asyncio.CancelledError:
                # A disconnected management request doesn't stop ingress. The
                # next operation still waits for this disk future to finish.
                self.metrics["disk_waiter_canceled"] = (
                    self.metrics.get("disk_waiter_canceled", 0) + 1
                )
                raise

    async def start(self, context):
        if self.accepting:
            return
        if self.worker is not None and not self.worker.released:
            raise RuntimeError("message_inbox_previous_worker_unreleased")
        self.context = context
        self.worker = DiagnosticWorker("zhenxun-message-inbox")
        worker = self.worker
        context.own_resource(
            receipt_id="message-inbox:disk",
            provider="lifecycle",
            resource_type="disk_worker",
            release_check=lambda: worker.released,
        )
        try:
            self.status = await self.io(self.store.initialize)
            await self.refresh_generation()
        except BaseException:
            with suppress(Exception):
                await self.io(self.store.close)
            await worker.close(remaining_timeout(15))
            raise
        self.accepting = True
        self.mutation_task = context.spawn_task(
            self._mutation_loop(), name="message-inbox-mutations"
        )
        self.tasks = [
            context.spawn_task(self._write_loop(), name="message-inbox-writer"),
            context.spawn_task(self._dispatch_loop(), name="message-inbox-dispatch"),
            context.spawn_task(self._delivery_loop(), name="message-inbox-delivery"),
            self.mutation_task,
        ]

    async def mutate(self, method, *args):
        if self.mutation_task is None:
            return await self.io(method, *args)
        if self.mutation_task.done():
            raise RuntimeError("inbox_mutation_worker_unavailable")
        future = asyncio.get_running_loop().create_future()
        future.add_done_callback(
            lambda done: None if done.cancelled() else done.exception()
        )
        try:
            self.mutations.put_nowait((method, args, future))
        except asyncio.QueueFull:
            self.metrics["mutation_staging_rejected"] = (
                self.metrics.get("mutation_staging_rejected", 0) + 1
            )
            raise RuntimeError("inbox_mutation_staging_full") from None
        return await asyncio.shield(future)

    async def _mutation_loop(self):
        batch = []
        try:
            while True:
                batch = [await self.mutations.get()]
                while len(batch) < 64 and not self.mutations.empty():
                    batch.append(self.mutations.get_nowait())
                try:
                    results = await self.io(
                        self.store.apply_mutations,
                        [(method, args) for method, args, _ in batch],
                    )
                except Exception as error:
                    self.metrics["mutation_commit_failed"] = (
                        self.metrics.get("mutation_commit_failed", 0) + 1
                    )
                    for _, _, future in batch:
                        if not future.done():
                            future.set_exception(error)
                else:
                    self.metrics["mutation_commits"] = (
                        self.metrics.get("mutation_commits", 0) + 1
                    )
                    self.metrics["mutation_records"] = self.metrics.get(
                        "mutation_records", 0
                    ) + len(batch)
                    for (_, _, future), result in zip(batch, results):
                        if not future.done():
                            future.set_result(result)
                for _ in batch:
                    self.mutations.task_done()
                batch = []
        finally:
            # A canceled disk await may still commit. Its caller gets an
            # unknown outcome, never a false successful acknowledgment.
            while not self.mutations.empty():
                batch.append(self.mutations.get_nowait())
            for _, _, future in batch:
                if not future.done():
                    future.set_exception(RuntimeError("inbox_mutation_unconfirmed"))
                self.mutations.task_done()

    def register_bot(self, bot, decode, dispatch):
        key = f"{bot.adapter.get_name()}:{bot.self_id}"
        self.bots[key] = bot, dispatch
        self.codecs[bot.adapter.get_name()] = decode
        return key

    def observe(self, bot, event, *, decode=None, dispatch=None, raw_payload=None):
        self.metrics["observed"] += 1
        loop = asyncio.get_running_loop()
        future = loop.create_future()
        if not self.accepting:
            future.set_result({"accepted": False, "reason": "inbox_not_ready"})
            return future
        if bot.adapter.get_name() == "OneBot V11":
            from .message_admission import connection_epochs

            if connection_epochs.is_backlog(
                bot, "qq_client", getattr(event, "time", None)
            ):
                self.metrics["source_backlog_skipped"] = (
                    self.metrics.get("source_backlog_skipped", 0) + 1
                )
                future.set_result(
                    {"accepted": False, "reason": "source_backlog_message"}
                )
                return future
        key = self.register_bot(
            bot, decode or bot.adapter.json_to_event, dispatch or bot.handle_event
        )
        payload = json.loads(event.json())
        message_id = payload.get("message_id") or payload.get("id")
        conversation = ":".join(
            str(payload.get(field) or "")
            for field in ("guild_id", "channel_id", "group_id", "group_openid")
        )
        if not conversation.strip(":"):
            conversation = str(event.get_user_id())
        digest_input = (
            (key, conversation, str(message_id))
            if message_id is not None
            else (key, json.dumps(payload, sort_keys=True, ensure_ascii=False))
        )
        envelope = {
            "id": sha256(repr(digest_input).encode()).hexdigest(),
            "bot": key,
            "adapter": bot.adapter.get_name(),
            "conversation": f"{key}:{conversation}",
            "payload": raw_payload if raw_payload is not None else payload,
            "received": time.time(),
            "generation": self.generation,
        }
        try:
            self.queue.put_nowait((envelope, future))
        except asyncio.QueueFull:
            self.metrics["staging_rejected"] += 1
            future.set_result({"accepted": False, "reason": "inbox_staging_full"})
        return future

    async def _write_loop(self):
        try:
            await self._write_batches()
        finally:
            self.accepting = False
            # Staged requests have not reached the disk worker. Resolve their
            # waiters even when shutdown exhausted its shared commit budget.
            while not self.queue.empty():
                _, future = self.queue.get_nowait()
                if not future.done():
                    future.set_result(
                        {"accepted": False, "reason": "inbox_stopped_before_commit"}
                    )
                self.queue.task_done()

    async def _write_batches(self):
        while True:
            batch = [await self.queue.get()]
            while len(batch) < 64 and not self.queue.empty():
                batch.append(self.queue.get_nowait())
            started = time.monotonic()
            try:
                receipts = await self.io(self.store.accept, [item for item, _ in batch])
            except Exception:
                self.metrics["write_failed"] += len(batch)
                receipts = [{"accepted": False, "reason": "inbox_write_failed"}] * len(
                    batch
                )
            except asyncio.CancelledError:
                for _, future in batch:
                    if not future.done():
                        future.set_result(
                            {"accepted": False, "reason": "inbox_commit_unconfirmed"}
                        )
                    self.queue.task_done()
                raise
            self.metrics["last_commit_ms"] = (time.monotonic() - started) * 1000
            for (_, future), receipt in zip(batch, receipts):
                if not future.done():
                    future.set_result(receipt)
                self.queue.task_done()

    def ready(self):
        from .startup import startup_coordinator

        return startup_coordinator.final_business_ready

    async def enqueue_delivery(self, name, record):
        from .message_execution import operation_key

        execution = current_execution.get()
        identity = operation_key(f"delivery:{name}", "")
        payload = json.dumps(
            {
                key: getattr(record, key)
                for key in record._meta.db_fields
                if key != record._meta.pk_attr
            },
            default=lambda value: value.isoformat()
            if hasattr(value, "isoformat")
            else value.value,
        )
        await self.mutate(
            self.store.enqueue_delivery, identity, execution.identity, name, payload
        )
        execution.deliveries[identity] = "persisted"
        return True

    async def _delivery_loop(self):
        from zhenxun.models.asset_operation import AssetOperation

        while True:
            delay = 0.1
            if self.ready():
                retired = await self.io(self.store.retired_receipts)
                if retired:
                    try:
                        await AssetOperation.filter(
                            id__in=retired, kind="delivery"
                        ).delete()
                        await self.io(self.store.cleaned_receipts, retired)
                    except Exception:
                        self.metrics["receipt_cleanup_failed"] = (
                            self.metrics.get("receipt_cleanup_failed", 0) + 1
                        )
                rows = await self.io(self.store.pending_deliveries)
                if rows:
                    try:
                        await self._deliver_batch(rows)
                        delay = 0
                    except Exception as error:
                        if "first_delivery_error" not in self.metrics:
                            self.metrics["first_delivery_error"] = type(error).__name__
                        self.metrics["delivery_failed"] = (
                            self.metrics.get("delivery_failed", 0) + 1
                        )
                        delay = 1
            await asyncio.sleep(delay)

    async def _deliver_batch(self, rows):
        from collections import defaultdict
        from importlib import import_module

        from tortoise.transactions import in_transaction

        from zhenxun.models.asset_operation import AssetOperation

        from .low_priority_writer import _WRITERS

        grouped = defaultdict(list)
        for row in rows:
            try:
                module, name = {
                    "chat_history": "chat_history.ChatHistory",
                    "statistics": "statistics.Statistics",
                }[row["writer"]].split(".")
                model_type = getattr(import_module(f"zhenxun.models.{module}"), name)
                data = json.loads(row["payload"])
                for key, field in model_type._meta.fields_map.items():
                    if getattr(field, "auto_now_add", False) and data.get(key) is None:
                        data[key] = datetime.fromtimestamp(row["created"], timezone.utc)
                record = model_type(
                    **{
                        key: model_type._meta.fields_map[key].to_python_value(value)
                        for key, value in data.items()
                    }
                )
            except (ValueError, TypeError, KeyError):
                await self.io(
                    self.store.block_delivery, row["id"], "invalid_delivery_payload"
                )
                continue
            grouped[row["writer"]].append((row, record))
        for writer, items in grouped.items():
            # The receipt and business rows commit together. If the subsequent
            # spool acknowledgment is lost, a retry reads these receipts first.
            async with in_transaction():
                identities = [row["id"] for row, _ in items]
                existing = set(
                    await AssetOperation.filter(id__in=identities).values_list(
                        "id", flat=True
                    )
                )
                fresh = [
                    (row, record) for row, record in items if row["id"] not in existing
                ]
                if fresh:
                    await _WRITERS[writer].config.write_batch(
                        [record for _, record in fresh], "durable_delivery"
                    )
                    await AssetOperation.bulk_create(
                        [
                            AssetOperation(
                                id=row["id"],
                                user_id="",
                                kind="delivery",
                                state="committed",
                                event_id=row["event_id"],
                                payload={"event": row["event_id"], "writer": writer},
                            )
                            for row, _ in fresh
                        ]
                    )
            await self.mutate(self.store.delivered, identities)

    async def _dispatch_loop(self):
        last_snapshot = 0.0
        next_cleanup = time.monotonic() + 3600
        while True:
            await self.refresh_generation()
            executing = 64 - self.execution_slots._value
            if self.accepting and self.ready() and executing < 64:
                connected = [
                    key
                    for key, (bot, _) in self.bots.items()
                    if (
                        bot.adapter.bots.get(bot.self_id) is bot
                        or getattr(bot.adapter, "_webhook_bots", {}).get(bot.self_id)
                        is bot
                    )
                ]
                rows = await self.io(
                    self.store.claim_history, connected, 64 - executing
                )
                rows += await self.io(
                    self.store.claim,
                    connected,
                    self.generation,
                    64 - executing - len(rows),
                )
                for row in rows:
                    await self.execution_slots.acquire()
                    self.slot_owners.add(row["id"])
                    try:
                        task = self.context.spawn_task(
                            self._execute(row),
                            name=f"inbox-{row['sequence']}",
                            persistent=False,
                        )
                    except BaseException:
                        self.release_slot(row["id"])
                        raise
                    self.active[row["id"]] = task
                    task.add_done_callback(
                        lambda _, key=row["id"]: self.active.pop(key, None)
                    )
                    task.add_done_callback(
                        lambda _, key=row["id"]: self.release_slot(key)
                    )
            if time.monotonic() - last_snapshot >= 1:
                self.status = await self.io(self.store.snapshot)
                now = time.monotonic()
                counts = self.status.get("counters", {})
                sample = (
                    now,
                    counts.get("accepted", 0),
                    sum(v for k, v in counts.items() if k.startswith("terminal_")),
                )
                if self._rate_sample is not None:
                    elapsed = now - self._rate_sample[0]
                    self.rates = {
                        "received_per_second": max(
                            0, (sample[1] - self._rate_sample[1]) / elapsed
                        ),
                        "executed_per_second": max(
                            0, (sample[2] - self._rate_sample[2]) / elapsed
                        ),
                    }
                self._rate_sample = sample
                last_snapshot = time.monotonic()
            if time.monotonic() >= next_cleanup:
                await self.io(self.store.cleanup)
                next_cleanup = time.monotonic() + 3600
            await asyncio.sleep(0.02)

    async def _execute(self, row):
        self.install_waiter_bridge()
        state, reason = "completed", ""
        execution = MessageExecution(row["id"], received_at=row["received"])
        token = current_execution.set(execution)
        handler_started = time.monotonic()
        try:
            bot, dispatch = self.bots[row["bot"]]
            event = self.codecs[row["adapter"]](row["payload"])
            if event is None:
                raise RuntimeError("event_codec_unavailable")
            if row["state"] == "held":
                await self._record_only(bot, event)
                return
            await dispatch(event)
            if execution.errors:
                state, reason = "failed", "matcher_failed"
        except MessageExecutionUnavailable as error:
            state, reason = "held", str(error)
        except asyncio.CancelledError:
            state, reason = "unresolved", "execution_interrupted"
            raise
        except Exception:
            state, reason = "unresolved", "execution_failed_or_reply_unconfirmed"
        finally:
            current_execution.reset(token)
            handler_ms = (time.monotonic() - handler_started) * 1000
            self.timing["handler_count"] += 1
            self.timing["handler_ms"] += handler_ms
            self.timing["handler_max_ms"] = max(
                self.timing["handler_max_ms"], handler_ms
            )
            # Handler capacity represents time spent in the business pipeline.
            # Durable terminal recording has its own bounded mutation queue;
            # keeping the slot while waiting for that queue couples slow disk
            # commits to matcher admission and amplifies burst recovery time.
            # The done callback remains idempotent through release_slot().
            self.finalizing.add(row["id"])
            self.release_slot(row["id"])
            finalize_started = time.monotonic()
            try:
                if row["state"] == "held":
                    await self.mutate(
                        self.store.record_held_result,
                        row["id"],
                        {
                            "history": "persisted_or_skipped"
                            if state == "completed"
                            else "unresolved",
                            "errors": execution.errors,
                            "deliveries": execution.deliveries,
                        },
                    )
                else:
                    await self.mutate(
                        self.store.finish,
                        row["id"],
                        state,
                        reason,
                        {
                            "errors": execution.errors,
                            "deliveries": execution.deliveries,
                        },
                    )
            finally:
                finalize_ms = (time.monotonic() - finalize_started) * 1000
                self.timing["finalize_count"] += 1
                self.timing["finalize_ms"] += finalize_ms
                self.timing["finalize_max_ms"] = max(
                    self.timing["finalize_max_ms"], finalize_ms
                )
                self.finalizing.discard(row["id"])

    async def _record_only(self, bot, event):
        # Expired work is never sent through business matchers. A dedicated
        # history delivery hook applies the original recording configuration.
        from .message_history_delivery import record_expired_message

        await record_expired_message(bot, event)

    def snapshot(self):
        return {
            **self.status,
            **self.metrics,
            "rates": dict(self.rates),
            "accepting": self.accepting,
            "active": len(self.slot_owners),
            "finalizing": len(self.finalizing),
            "timing": dict(self.timing),
            "waiting_input": len(self.waiting_inputs),
            "staging": self.queue.qsize(),
            "mutation_staging": self.mutations.qsize(),
        }

    async def close(self):
        self.accepting = False
        deadline = time.monotonic() + remaining_timeout(15)
        # Stop dispatch before waiting for staged ingress writes.
        if len(self.tasks) > 1:
            self.tasks[1].cancel()
        with suppress(asyncio.TimeoutError):
            await asyncio.wait_for(
                self.queue.join(), max(0, deadline - time.monotonic())
            )
        for task in self.tasks:
            if task is not self.mutation_task:
                task.cancel()
        for task in self.active.values():
            task.cancel()
        pending = (set(self.tasks) - {self.mutation_task}) | set(self.active.values())
        if pending:
            await asyncio.wait(pending, timeout=max(0, deadline - time.monotonic()))
        with suppress(asyncio.TimeoutError):
            await asyncio.wait_for(
                self.mutations.join(), max(0, deadline - time.monotonic())
            )
        if self.mutation_task is not None:
            self.mutation_task.cancel()
            pending.add(self.mutation_task)
            await asyncio.wait(
                {self.mutation_task}, timeout=max(0, deadline - time.monotonic())
            )
        close_error = None
        try:
            await asyncio.wait_for(
                self.io(self.store.close), max(0, deadline - time.monotonic())
            )
        except Exception as error:
            close_error = error
        released = await self.worker.close(max(0, deadline - time.monotonic()))
        if self.waiter_bridge is not None:
            cls, original, wrapper = self.waiter_bridge
            if cls.wait is wrapper:
                cls.wait = original
            self.waiter_bridge = None
        if close_error or not released or any(not task.done() for task in pending):
            raise RuntimeError("message_inbox_shutdown_unresolved")


message_inbox = MessageInbox()
