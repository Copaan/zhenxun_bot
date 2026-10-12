"""matcher 执行的并发通道"""

import asyncio

RESOURCE_LIMITS = {
    "light_plugin": 16,
    "database_read": 8,
    "database_write": 4,
    "render": 2,
    "external_io": 8,
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
    if lane in RESOURCE_LIMITS:
        return lane
    return "external_io"


class ResourceLanes:
    def __init__(self, limits: dict[str, int] | None = None):
        self.limits = dict(RESOURCE_LIMITS if limits is None else limits)
        self.semaphores = {
            lane: asyncio.Semaphore(limit) for lane, limit in self.limits.items()
        }

    def busy(self) -> bool:
        """是否有通道正被占用

        返回:
            bool: 是否有通道正被占用
        """
        return any(
            gate._value < self.limits[lane] for lane, gate in self.semaphores.items()
        )

    async def acquire(self, lane):
        """占用一个资源通道

        参数:
            lane: 资源通道名

        返回:
            可重复调用的释放函数，带 reacquire 重新占用方法
        """
        gate = self.semaphores[lane]
        released = True

        async def reacquire():
            nonlocal released
            if released:
                await gate.acquire()
                released = False

        def release():
            nonlocal released
            if not released:
                released = True
                gate.release()

        setattr(release, "reacquire", reacquire)
        await reacquire()
        return release


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

    setattr(release, "reacquire", reacquire)
    return release


resource_lanes = ResourceLanes()
