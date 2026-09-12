"""Event-local permission cache fencing, separate from side-effect receipts."""

_revision = 0
_mismatches = 0
_KEYS = (
    "auth_snapshots",
    "plugin_cache",
    "auth_cache_misses",
    "bot_data",
    "bot_cache_ready",
    "group",
    "group_cache_ready",
    "group_runtime_virtual",
    "admin_levels",
    "admin_cache_ready",
    "ban_state",
    "module_limits_ready",
    "module_limit_entries",
)


def current_revision():
    return _revision


def advance_revision():
    global _revision
    _revision += 1


def refresh_event_revision(event_cache):
    global _mismatches
    if event_cache is not None:
        previous = event_cache.get("permission_revision")
        if previous != _revision:
            if previous is not None:
                _mismatches += 1
            for key in _KEYS:
                event_cache.pop(key, None)
            event_cache["permission_revision"] = _revision
    return _revision


def snapshot():
    return {"revision": _revision, "permission_revision_mismatch": _mismatches}
