"""Instance-local APScheduler boundary for plugin lease admission.

Only owned jobs use these runners. APScheduler still owns scheduling, coalescing,
max_instances and executor accounting; ordinary failures retain its event contract.
"""

import asyncio
from concurrent.futures import Future
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from functools import wraps
import sys
from traceback import clear_frames, format_tb

from apscheduler.events import (
    EVENT_JOB_ERROR,
    EVENT_JOB_EXECUTED,
    EVENT_JOB_MISSED,
    JobExecutionEvent,
)
from apscheduler.executors.asyncio import AsyncIOExecutor
from apscheduler.util import iscoroutinefunction_partial

from .ownership import scheduler_context


class JobLeaseRevoked(Exception):
    """Admission was refused; this is neither job success nor a job failure."""


def _missed(job, run_time, events, logger):
    if job.misfire_grace_time is not None:
        difference = datetime.now(timezone.utc) - run_time
        if difference > timedelta(seconds=job.misfire_grace_time):
            events.append(
                JobExecutionEvent(
                    EVENT_JOB_MISSED, job.id, job._jobstore_alias, run_time
                )
            )
            logger.warning('Run time of job "%s" was missed by %s', job, difference)
            return True
    return False


@contextmanager
def _outcome(job, run_time, events, logger):
    result = []
    try:
        yield result
    except JobLeaseRevoked:
        # The manager records owner/generation and the reason at admission.
        pass
    except BaseException as error:
        tb = error.__traceback__
        events.append(
            JobExecutionEvent(
                EVENT_JOB_ERROR,
                job.id,
                job._jobstore_alias,
                run_time,
                exception=error,
                traceback="".join(format_tb(tb)),
            )
        )
        logger.exception('Job "%s" raised an exception', job)
        clear_frames(tb)
    else:
        events.append(
            JobExecutionEvent(
                EVENT_JOB_EXECUTED,
                job.id,
                job._jobstore_alias,
                run_time,
                retval=result[0],
            )
        )
        logger.info('Job "%s" executed successfully', job)


def _run_sync(job, run_times, logger):
    events = []
    for run_time in run_times:
        if not _missed(job, run_time, events, logger):
            with _outcome(job, run_time, events, logger) as result:
                result.append(job.func(*job.args, **job.kwargs))
    return events


async def _run_async(job, run_times, logger):
    events = []
    for run_time in run_times:
        if not _missed(job, run_time, events, logger):
            with _outcome(job, run_time, events, logger) as result:
                result.append(await job.func(*job.args, **job.kwargs))
    return events


class SchedulerBoundary:
    def __init__(self, scheduler, manager):
        self.scheduler = scheduler
        self.manager = manager
        self.originals = []
        self.executors = set()
        self._wrap_shared("wakeup")
        original = scheduler._lookup_executor

        @wraps(original)
        def lookup(alias):
            executor = original(alias)
            self._track_executor(executor)
            return executor

        self._replace(scheduler, "_lookup_executor", lookup)

    def _replace(self, target, name, replacement):
        self.originals.append((target, name, getattr(target, name), replacement))
        setattr(target, name, replacement)

    def _wrap_shared(self, name):
        original = getattr(self.scheduler, name)

        @wraps(original)
        def shared(*args, **kwargs):
            return scheduler_context().run(original, *args, **kwargs)

        self._replace(self.scheduler, name, shared)

    def _track_executor(self, executor):
        if not isinstance(executor, AsyncIOExecutor) or executor in self.executors:
            return
        self.executors.add(executor)
        original = executor._do_submit_job

        def submit(job, run_times):
            lease = getattr(job.func, "__zhenxun_job_lease__", None)
            context = scheduler_context()
            if lease is None:
                return context.run(original, job, run_times)

            def completed(future):
                executor._pending_futures.discard(future)
                try:
                    events = future.result()
                except asyncio.CancelledError:
                    if self.manager._scheduler_job_refused(job.id, *lease):
                        executor._run_job_success(job.id, [])
                    else:
                        executor._run_job_error(job.id, *sys.exc_info()[1:])
                except BaseException:
                    executor._run_job_error(job.id, *sys.exc_info()[1:])
                else:
                    executor._run_job_success(job.id, events)

            if iscoroutinefunction_partial(job.func):
                future = context.run(
                    executor._eventloop.create_task,
                    _run_async(job, run_times, executor._logger),
                )
            else:
                # An asyncio Future can be cancelled while its worker still
                # runs. Keep a separate completion proof for unload/drain.
                completion = Future()
                owner = self.manager._root_owner(lease[0]) or lease[0]
                owned = self.manager._owned_executor_futures[owner]
                owned.add(completion)

                def execute():
                    if not completion.set_running_or_notify_cancel():
                        return []
                    try:
                        return _run_sync(job, run_times, executor._logger)
                    finally:
                        completion.set_result(None)

                def thread_completed(_):
                    if not executor._eventloop.is_closed():
                        executor._eventloop.call_soon_threadsafe(
                            owned.discard, completion
                        )

                completion.add_done_callback(thread_completed)
                try:
                    future = context.run(
                        executor._eventloop.run_in_executor,
                        None,
                        context.copy().run,
                        execute,
                    )
                except BaseException:
                    completion.cancel()
                    raise
                future.add_done_callback(
                    lambda done: completion.cancel() if done.cancelled() else None
                )
            future.add_done_callback(completed)
            executor._pending_futures.add(future)

        self._replace(executor, "_do_submit_job", submit)

    def restore(self):
        for target, name, original, replacement in reversed(self.originals):
            if getattr(target, name) is replacement:
                setattr(target, name, original)
        self.originals.clear()
        self.executors.clear()
