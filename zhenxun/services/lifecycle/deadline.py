from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
import time

WORKER_SHUTDOWN_SECONDS = 13.0


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
