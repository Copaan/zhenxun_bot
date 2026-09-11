"""Bounded counters for existing availability fallbacks; no policy changes."""

_availability = dict.fromkeys(
    (
        "prepare_timeout_allow",
        "db_unhealthy_cache_miss",
        "policy_fallback_timeout",
        "cost_timeout_zero",
    ),
    0,
)


def record_availability_fallback(reason: str) -> None:
    if reason in _availability:
        _availability[reason] += 1


def availability_snapshot() -> dict[str, int]:
    return dict(_availability)
