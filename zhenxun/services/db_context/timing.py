"""Observe ORM client boundaries without retaining queries or changing drivers."""

import asyncio
from contextvars import ContextVar
from functools import wraps
import inspect
import time
import weakref

from zhenxun.services.pipeline_metrics import pipeline_metrics

_query = ContextVar("db_query_timing", default=None)


def _weak_call(method):
    # Instance attributes containing bound methods otherwise create a cycle for
    # every short transaction, retaining its raw connection until cyclic GC.
    if not inspect.ismethod(method):
        return method
    reference = weakref.WeakMethod(method)

    @wraps(method.__func__)
    def call(*args, **kwargs):
        target = reference()
        if target is None:
            raise RuntimeError("database_client_released")
        return target(*args, **kwargs)

    return call


class TimedConnection:
    def __init__(self, context):
        self.context = context

    async def __aenter__(self):
        started = time.perf_counter()
        try:
            return await self.context.__aenter__()
        finally:
            elapsed = time.perf_counter() - started
            pipeline_metrics.observe("connection_wait_ms", elapsed)
            current = _query.get()
            if current is not None and current[0] is asyncio.current_task():
                current[1] += elapsed

    async def __aexit__(self, *args):
        return await self.context.__aexit__(*args)

    def __getattr__(self, name):
        return getattr(self.context, name)


def instrument_client(client):
    """Bind once to this client and its future transaction clients only."""
    if getattr(client, "_zx_pipeline_timing", False):
        return
    client._zx_pipeline_timing = True
    acquire = _weak_call(client.acquire_connection)

    @wraps(acquire)
    def acquire_timed(*args, **kwargs):
        return TimedConnection(acquire(*args, **kwargs))

    client.acquire_connection = acquire_timed

    def wrap_query(method):
        @wraps(method)
        async def timed(*args, **kwargs):
            current = _query.get()
            task = asyncio.current_task()
            if current is not None and current[0] is task:
                return await method(*args, **kwargs)
            record = [task, 0.0]
            token = _query.set(record)
            started = time.perf_counter()
            try:
                return await method(*args, **kwargs)
            finally:
                pipeline_metrics.observe(
                    "execution_ms", time.perf_counter() - started - record[1]
                )
                _query.reset(token)

        return timed

    def wrap_terminal(method, stage):
        @wraps(method)
        async def timed(*args, **kwargs):
            started = time.perf_counter()
            try:
                return await method(*args, **kwargs)
            finally:
                pipeline_metrics.observe(stage, time.perf_counter() - started)

        return timed

    for name in ("commit", "rollback"):
        if method := getattr(client, name, None):
            setattr(client, name, wrap_terminal(_weak_call(method), f"{name}_ms"))

    for name in (
        "execute_query",
        "execute_query_dict",
        "execute_insert",
        "execute_many",
        "execute_script",
    ):
        method = getattr(client, name, None)
        if method is not None:
            setattr(client, name, wrap_query(_weak_call(method)))

    original = _weak_call(client._in_transaction)

    @wraps(original)
    def transaction(*args, **kwargs):
        context = original(*args, **kwargs)
        instrument_client(context.connection)
        return context

    client._in_transaction = transaction
