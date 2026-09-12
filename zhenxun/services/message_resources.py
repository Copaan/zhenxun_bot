"""Bound resource classes while preserving native matcher dispatch and quotas."""

import asyncio
import time

RESOURCE_LIMITS = {
    "chat": 16,
    "light_plugin": 16,
    "database_read": 8,
    "database_write": 4,
    "render": 2,
    "external_io": 8,
    "interactive_wait": 64,
    "delivery": 1,
}
_BUILTIN_LANES = {
    "about": "light_plugin",
    "bot_profile": "light_plugin",
    "shop": "database_write",
    "sign_in": "database_write",
    "mahiro_bank": "database_write",
    "info": "database_read",
    "help": "render",
    "statistics": "render",
}


def matcher_resource_lane(matcher):
    module = str(getattr(matcher, "module_name", "") or "")
    if not module:
        module = str(getattr(getattr(matcher, "module", None), "__name__", ""))
    if module.startswith("zhenxun.builtin_plugins."):
        name = module.removeprefix("zhenxun.builtin_plugins.").split(".")[0]
        if name in _BUILTIN_LANES:
            return _BUILTIN_LANES[name]
    plugin = getattr(matcher, "plugin", None)
    extra = getattr(getattr(plugin, "metadata", None), "extra", None)
    lane = extra.get("resource_lane") if isinstance(extra, dict) else None
    if lane in RESOURCE_LIMITS and lane not in {"interactive_wait", "delivery"}:
        return lane
    return "external_io"


class ResourceLanes:
    def __init__(self):
        self.semaphores = {
            lane: asyncio.Semaphore(limit) for lane, limit in RESOURCE_LIMITS.items()
        }
        self.stats = {
            lane: {
                "active": 0,
                "waiting": 0,
                "acquired": 0,
                "wait_ms": 0.0,
                "execution_ms": 0.0,
            }
            for lane in RESOURCE_LIMITS
        }

    async def acquire(self, lane):
        gate, stats = self.semaphores[lane], self.stats[lane]
        released = True
        acquired_at = 0.0

        async def reacquire():
            nonlocal released, acquired_at
            if not released:
                return
            started = time.perf_counter()
            stats["waiting"] += 1
            try:
                await gate.acquire()
            finally:
                stats["waiting"] -= 1
                stats["wait_ms"] += (time.perf_counter() - started) * 1000
            released = False
            acquired_at = time.perf_counter()
            stats["active"] += 1
            stats["acquired"] += 1

        def release():
            nonlocal released
            if not released:
                released = True
                stats["active"] -= 1
                stats["execution_ms"] += (time.perf_counter() - acquired_at) * 1000
                gate.release()

        release.reacquire = reacquire
        await reacquire()
        return release

    def snapshot(self):
        return {
            lane: {**values, "limit": RESOURCE_LIMITS[lane]}
            for lane, values in self.stats.items()
        }


def combined_lease(resource, dispatch):
    def release():
        dispatch()
        resource()

    async def reacquire():
        await resource.reacquire()
        try:
            await dispatch.reacquire()
        except BaseException:
            resource()
            raise

    release.reacquire = reacquire
    return release


resource_lanes = ResourceLanes()
