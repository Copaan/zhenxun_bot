from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
import math
import os
from pathlib import Path
import time


class BudgetExpired(TimeoutError):
    pass


@dataclass
class ShutdownBudget:
    deadline: float
    parent: ShutdownBudget | None = None

    @classmethod
    def start(cls, seconds: float) -> ShutdownBudget:
        return cls(time.monotonic() + max(0.0, seconds))

    def remaining(self, maximum: float | None = None) -> float:
        remaining = max(0.0, self.deadline - time.monotonic())
        if self.parent is not None:
            remaining = min(remaining, self.parent.remaining())
        return remaining if maximum is None else min(remaining, max(0.0, maximum))

    def check(self) -> None:
        if not self.remaining():
            raise BudgetExpired("shutdown_budget_exhausted")

    def tighten(self, parent: ShutdownBudget) -> None:
        self.deadline = min(self.deadline, parent.deadline)


current_budget: ContextVar[ShutdownBudget | None] = ContextVar(
    "lifecycle_shutdown_budget", default=None
)


@contextmanager
def shutdown_budget(seconds: float):
    parent = current_budget.get()
    budget = ShutdownBudget.start(seconds)
    if parent is not None:
        budget = ShutdownBudget(min(parent.deadline, budget.deadline), parent=parent)
    token = current_budget.set(budget)
    try:
        yield budget
    finally:
        current_budget.reset(token)


def remaining_timeout(maximum: float) -> float:
    budget = current_budget.get()
    return maximum if budget is None else budget.remaining(maximum)


def check_budget() -> None:
    budget = current_budget.get()
    if budget is not None:
        budget.check()


def received_shutdown_request() -> dict:
    path = os.getenv("ZHENXUN_SHUTDOWN_BUDGET_PATH")
    boot = os.getenv("ZHENXUN_LAUNCHER_BOOT_ID")
    if not path or not boot:
        return {}
    import psutil

    from zhenxun.utils.atomic_json import read_json_locked

    try:
        state = read_json_locked(Path(path), {}, timeout=0, quarantine_corrupt=False)
        identity = psutil.Process(os.getpid()).create_time()
    except (OSError, ValueError, TimeoutError, psutil.Error):
        return {}
    if not isinstance(state, dict) or state.get("launcher_boot_id") != boot:
        return {}
    requests = state.get("requests")
    if not isinstance(requests, dict):
        return {}
    for request in requests.values():
        if not isinstance(request, dict):
            continue
        identities = request.get("identities")
        if (
            not isinstance(identities, dict)
            or identities.get(str(os.getpid())) != identity
        ):
            continue
        timestamp, milliseconds = request.get("requested_at"), request.get("budget_ms")
        if any(
            isinstance(value, bool)
            or not isinstance(value, int | float)
            or not math.isfinite(value)
            for value in (timestamp, milliseconds)
        ):
            continue
        return request
    return {}


def received_shutdown_budget(default: float) -> float:
    request = received_shutdown_request()
    if not request:
        return default
    age = time.time() - request["requested_at"]
    if age < 0:
        return 0.0
    return max(0.0, min(default, request["budget_ms"] / 1000.0 - age))
