"""Fixed-cardinality pipeline timings; no events, plugins or samples are retained."""

from __future__ import annotations

import math

STAGES = (
    "ingress_wait_ms",
    "persistence_wait_ms",
    "route_select_ms",
    "permission_wait_ms",
    "admission_wait_ms",
    "handler_wait_ms",
    "handler_execution_ms",
    "business_queue_wait_ms",
    "business_dispatch_ms",
    "finalization_wait_ms",
    "reply_wait_ms",
    "background_delivery_wait_ms",
    "event_loop_lag_ms",
    "queue_wait_ms",
    "connection_wait_ms",
    "lock_wait_ms",
    "execution_ms",
    "database_operation_ms",
    "transaction_enter_ms",
    "commit_ms",
    "rollback_ms",
)
BUCKETS_MS = (1, 5, 10, 25, 50, 100, 250, 500, 1000, 5000, 30000)


class PipelineMetrics:
    def __init__(self):
        self._values = {}

    def observe(self, stage: str, seconds: float) -> None:
        if stage not in STAGES or not math.isfinite(seconds):
            return
        elapsed = max(0.0, seconds * 1000)
        value = self._values.setdefault(
            stage,
            {
                "count": 0,
                "total_ms": 0.0,
                "max_ms": 0.0,
                "buckets": [0] * (len(BUCKETS_MS) + 1),
            },
        )
        value["count"] += 1
        value["total_ms"] += elapsed
        value["max_ms"] = max(value["max_ms"], elapsed)
        index = next(
            (i for i, upper in enumerate(BUCKETS_MS) if elapsed <= upper),
            len(BUCKETS_MS),
        )
        value["buckets"][index] += 1

    def snapshot(self) -> dict:
        return {
            "bucket_bounds_ms": list(BUCKETS_MS),
            "stages": {
                stage: {**value, "buckets": list(value["buckets"])}
                for stage, value in self._values.items()
            },
            "unobserved": [stage for stage in STAGES if stage not in self._values],
        }


pipeline_metrics = PipelineMetrics()
