"""Bounded counters for unavailable dependencies and database fallbacks."""

_availability = {"permission_deferred": 0}


def record_availability_fallback(reason: str) -> None:
    if reason in _availability:
        _availability[reason] += 1


def availability_snapshot() -> dict[str, int]:
    return {
        **_availability,
        "cache_read_failures": sum(value[0] for value in _cache_read_failures.values()),
    }


_cache_read_failures: dict[str, tuple[int, float]] = {}


def record_cache_read_failure(cache_type: str, error: Exception) -> None:
    import time

    from zhenxun.services.log import logger

    # Bound diagnostic keys independently of arbitrary plugin cache names.
    key = (
        cache_type
        if cache_type in _cache_read_failures or len(_cache_read_failures) < 128
        else "other"
    )
    count, last = _cache_read_failures.get(key, (0, float("-inf")))
    now = time.monotonic()
    if now - last >= 60:
        logger.warning(
            f"cache read unavailable; database fallback: {key}; count={count + 1}",
            "Cache",
            e=error,
        )
        last = now
    _cache_read_failures[key] = count + 1, last
